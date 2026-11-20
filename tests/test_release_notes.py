"""Tests for the changelog slicing used by the release workflow."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "release_notes.py"

SAMPLE = """# Changelog

## [Unreleased]

## [1.10.0] - 2025-12-15

### Changed

- A later version that starts with the same digits.

## [1.2.0] - 2026-01-02

### Added

- A thing.

## [1.1.0] - 2025-12-01

### Fixed

- A bug.

[Unreleased]: https://example.com/compare/v1.10.0...HEAD
[1.10.0]: https://example.com/compare/v1.2.0...v1.10.0
[1.2.0]: https://example.com/compare/v1.1.0...v1.2.0
[1.1.0]: https://example.com/releases/tag/v1.1.0
"""


def load_module() -> ModuleType:
    """Load the release notes script as a module."""
    spec = importlib.util.spec_from_file_location("release_notes", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def use_changelog(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, text: str, tmp_path: Path
) -> None:
    """Point the script at a changelog fixture instead of the repository file."""
    path = tmp_path / "CHANGELOG.md"
    path.write_text(text, encoding="utf-8")
    monkeypatch.setattr(module, "CHANGELOG_PATH", path)


def test_changelog_path_points_at_the_repository_changelog() -> None:
    module = load_module()

    assert module.CHANGELOG_PATH.name == "CHANGELOG.md"
    assert module.CHANGELOG_PATH.is_file()


class TestExtractSection:
    def test_returns_the_body_of_one_version(self) -> None:
        module = load_module()

        body = module.extract_section("1.2.0", SAMPLE)

        assert body == "### Added\n\n- A thing."

    def test_stops_at_the_next_version(self) -> None:
        module = load_module()

        body = module.extract_section("1.10.0", SAMPLE)

        assert body == "### Changed\n\n- A later version that starts with the same digits."

    def test_does_not_match_a_version_by_prefix(self) -> None:
        module = load_module()

        assert module.extract_section("1.1", SAMPLE) == ""

    def test_stops_at_the_link_block_for_the_oldest_version(self) -> None:
        module = load_module()

        body = module.extract_section("1.1.0", SAMPLE)

        assert body == "### Fixed\n\n- A bug."

    def test_does_not_match_a_version_by_pattern(self) -> None:
        module = load_module()
        sample = "## [14243]\n\n### Added\n\n- Not version 1.2.3.\n"

        assert module.extract_section("1.2.3", sample) == ""

    def test_returns_nothing_for_an_unknown_version(self) -> None:
        module = load_module()

        assert module.extract_section("9.9.9", SAMPLE) == ""

    def test_returns_nothing_for_an_empty_version(self) -> None:
        module = load_module()

        assert module.extract_section("", SAMPLE) == ""

    def test_returns_nothing_for_an_empty_section(self) -> None:
        module = load_module()
        sample = "## [2.0.0] - 2026-01-01\n\n## [1.0.0] - 2025-01-01\n\n- Old.\n"

        assert module.extract_section("2.0.0", sample) == ""

    def test_stops_at_a_reference_definition_inside_the_body(self) -> None:
        module = load_module()
        sample = (
            "## [2.0.0]\n\n### Added\n\n- See the [notes][n].\n"
            "\n[n]: https://example.com\n\n- After.\n"
        )

        assert module.extract_section("2.0.0", sample) == "### Added\n\n- See the [notes][n]."


class TestMain:
    def test_prints_the_section_for_a_tag_with_a_v_prefix(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        module = load_module()
        use_changelog(monkeypatch, module, SAMPLE, tmp_path)

        assert module.main(["release_notes.py", "v1.2.0"]) == 0
        assert capsys.readouterr().out == "### Added\n\n- A thing.\n"

    def test_prints_nothing_for_a_version_missing_from_the_changelog(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        module = load_module()
        use_changelog(monkeypatch, module, SAMPLE, tmp_path)

        assert module.main(["release_notes.py", "v9.9.9"]) == 0
        assert capsys.readouterr().out == ""

    def test_prints_nothing_for_an_empty_section(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        module = load_module()
        use_changelog(monkeypatch, module, "## [2.0.0]\n\n## [1.0.0]\n\n- Old.\n", tmp_path)

        assert module.main(["release_notes.py", "v2.0.0"]) == 0
        assert capsys.readouterr().out == ""

    def test_strips_only_one_v_prefix(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        module = load_module()
        use_changelog(monkeypatch, module, SAMPLE, tmp_path)

        assert module.main(["release_notes.py", "vv1.2.0"]) == 0
        assert capsys.readouterr().out == ""

    def test_requires_a_tag(self, capsys: pytest.CaptureFixture[str]) -> None:
        module = load_module()

        assert module.main(["release_notes.py"]) == 2
        assert "usage" in capsys.readouterr().err
