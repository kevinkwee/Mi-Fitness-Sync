from __future__ import annotations

import json
import logging
import tempfile
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from mi_fitness_sync.activity.client import MiFitnessActivitiesClient
from mi_fitness_sync.activity.models import Activity
from mi_fitness_sync.auth.state import AuthState
from mi_fitness_sync.export.files import default_export_path
from mi_fitness_sync.export.render import render_export


logger = logging.getLogger(__name__)

ProgressCallback = Callable[["BulkExportRecord"], None]


@dataclass(slots=True, frozen=True)
class BulkExportOptions:
    output_dir: Path
    manifest_path: Path
    file_format: str
    compress: bool
    country_code: str | None
    no_cache: bool
    cache_dir: Path | None
    workers: int
    overwrite: bool
    smooth: bool
    outlier_speed_mps: float | None
    smooth_mode: str
    include_sport_report: bool
    include_recovery_rate: bool = False


@dataclass(slots=True, frozen=True)
class BulkExportRecord:
    activity_id: str
    status: str
    output_path: str | None
    bytes_written: int
    error: str | None
    recorded_at: str


@dataclass(slots=True)
class BulkExportSummary:
    total_seen: int = 0
    exported: int = 0
    skipped_existing: int = 0
    skipped_manifest: int = 0
    failed: int = 0
    output_dir: Path | None = None
    manifest_path: Path | None = None


class BulkExportManifest:
    """Append-only JSONL manifest used to resume large export jobs."""

    def __init__(self, path: Path):
        self._path = path

    def load_latest_records(self) -> dict[str, BulkExportRecord]:
        if not self._path.exists():
            return {}

        latest_records: dict[str, BulkExportRecord] = {}
        with self._path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed bulk export manifest line in %s", self._path)
                    continue

                record = _record_from_payload(payload)
                if record is None:
                    continue
                latest_records[record.activity_id] = record

        return latest_records

    def append(self, record: BulkExportRecord) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(record), sort_keys=True))
            handle.write("\n")


class BulkExporter:
    """Export matching activities with bounded concurrency and resume support."""

    def __init__(self, auth_state: AuthState, options: BulkExportOptions):
        self._auth_state = auth_state
        self._options = options
        self._thread_local = threading.local()

    def run(
        self,
        activities: Iterable[Activity],
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> BulkExportSummary:
        self._options.output_dir.mkdir(parents=True, exist_ok=True)
        self._options.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        manifest = BulkExportManifest(self._options.manifest_path)
        previous_records = manifest.load_latest_records()
        summary = BulkExportSummary(
            output_dir=self._options.output_dir,
            manifest_path=self._options.manifest_path,
        )

        pending: dict[Future[BulkExportRecord], str] = {}
        queue_limit = max(self._options.workers * 2, 1)

        with ThreadPoolExecutor(max_workers=self._options.workers) as executor:
            for activity in activities:
                summary.total_seen += 1
                output_path = default_export_path(
                    self._options.output_dir,
                    activity,
                    self._options.file_format,
                    compress=self._options.compress,
                    include_activity_id=True,
                )

                previous_record = previous_records.get(activity.activity_id)
                if self._should_skip_manifest(previous_record):
                    summary.skipped_manifest += 1
                    if progress_callback is not None and previous_record is not None:
                        progress_callback(
                            BulkExportRecord(
                                activity_id=activity.activity_id,
                                status="skipped_manifest",
                                output_path=previous_record.output_path,
                                bytes_written=previous_record.bytes_written,
                                error=None,
                                recorded_at=_utc_now_iso(),
                            )
                        )
                    continue

                if not self._options.overwrite and output_path.exists():
                    bytes_written = 0
                    try:
                        bytes_written = output_path.stat().st_size
                    except OSError:
                        logger.warning("Failed to read size for existing export %s", output_path)

                    record = BulkExportRecord(
                        activity_id=activity.activity_id,
                        status="skipped_existing",
                        output_path=str(output_path),
                        bytes_written=bytes_written,
                        error=None,
                        recorded_at=_utc_now_iso(),
                    )
                    summary.skipped_existing += 1
                    manifest.append(record)
                    previous_records[activity.activity_id] = record
                    if progress_callback is not None:
                        progress_callback(record)
                    continue

                future = executor.submit(self._export_activity, activity, output_path)
                pending[future] = activity.activity_id

                if len(pending) >= queue_limit:
                    self._drain_next_completed(pending, manifest, previous_records, summary, progress_callback)

            while pending:
                self._drain_next_completed(pending, manifest, previous_records, summary, progress_callback)

        return summary

    def _drain_next_completed(
        self,
        pending: dict[Future[BulkExportRecord], str],
        manifest: BulkExportManifest,
        previous_records: dict[str, BulkExportRecord],
        summary: BulkExportSummary,
        progress_callback: ProgressCallback | None,
    ) -> None:
        done, _ = wait(tuple(pending.keys()), return_when=FIRST_COMPLETED)
        for future in done:
            activity_id = pending.pop(future)
            try:
                record = future.result()
            except Exception as exc:  # pragma: no cover - defensive wrapper around worker failures
                logger.exception("Bulk export worker crashed for %s", activity_id)
                record = BulkExportRecord(
                    activity_id=activity_id,
                    status="failed",
                    output_path=None,
                    bytes_written=0,
                    error=str(exc),
                    recorded_at=_utc_now_iso(),
                )

            manifest.append(record)
            previous_records[record.activity_id] = record

            if record.status == "completed":
                summary.exported += 1
            elif record.status == "failed":
                summary.failed += 1

            if progress_callback is not None:
                progress_callback(record)

    def _export_activity(self, activity: Activity, output_path: Path) -> BulkExportRecord:
        try:
            client = self._worker_client()
            detail = client.get_activity_detail(
                activity,
                include_sport_report=self._options.include_sport_report,
                include_recovery_rate=self._options.include_recovery_rate,
            )
            export = render_export(
                detail,
                self._options.file_format,
                compress=self._options.compress,
                smooth=self._options.smooth,
                outlier_speed_mps=self._options.outlier_speed_mps,
                smooth_mode=self._options.smooth_mode,
            )
            _write_bytes_atomic(output_path, export.payload)
            return BulkExportRecord(
                activity_id=activity.activity_id,
                status="completed",
                output_path=str(output_path),
                bytes_written=len(export.payload),
                error=None,
                recorded_at=_utc_now_iso(),
            )
        except Exception as exc:  # noqa: BLE001 - bulk export should record and continue
            logger.exception("Bulk export failed for %s", activity.activity_id)
            return BulkExportRecord(
                activity_id=activity.activity_id,
                status="failed",
                output_path=None,
                bytes_written=0,
                error=str(exc),
                recorded_at=_utc_now_iso(),
            )

    def _worker_client(self) -> MiFitnessActivitiesClient:
        client = getattr(self._thread_local, "client", None)
        if isinstance(client, MiFitnessActivitiesClient):
            return client

        kwargs: dict[str, object] = {
            "country_code": self._options.country_code,
            "no_cache": self._options.no_cache,
        }
        if self._options.cache_dir is not None:
            kwargs["cache_dir"] = self._options.cache_dir

        client = MiFitnessActivitiesClient(self._auth_state, **kwargs)  # type: ignore[arg-type]
        self._thread_local.client = client
        return client

    def _should_skip_manifest(self, record: BulkExportRecord | None) -> bool:
        if self._options.overwrite or record is None:
            return False
        if record.status not in {"completed", "skipped_existing"}:
            return False
        if record.output_path is None:
            return False
        return Path(record.output_path).exists()


def _record_from_payload(payload: object) -> BulkExportRecord | None:
    if not isinstance(payload, dict):
        return None

    activity_id = payload.get("activity_id")
    status = payload.get("status")
    output_path = payload.get("output_path")
    bytes_written = payload.get("bytes_written")
    error = payload.get("error")
    recorded_at = payload.get("recorded_at")

    if not isinstance(activity_id, str) or not isinstance(status, str) or not isinstance(recorded_at, str):
        return None
    if output_path is not None and not isinstance(output_path, str):
        return None
    if error is not None and not isinstance(error, str):
        return None
    if not isinstance(bytes_written, int):
        return None

    return BulkExportRecord(
        activity_id=activity_id,
        status=status,
        output_path=output_path,
        bytes_written=bytes_written,
        error=error,
        recorded_at=recorded_at,
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_bytes_atomic(output_path: Path, payload: bytes) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=output_path.parent,
            prefix=f"{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(payload)
            temp_path = Path(handle.name)
        temp_path.replace(output_path)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to clean up temporary export file %s", temp_path)
        raise
