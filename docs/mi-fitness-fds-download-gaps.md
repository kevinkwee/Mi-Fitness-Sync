# Mi Fitness Sport Detail Data Loading — Decompiled App Reference

How the Mi Fitness Android app loads per-second sport record data, GPS tracks, and recovery rate for an activity's detail view, recovered from decompilation of APK v3.52.0i.

---

## Overview

When a user opens an activity detail screen, the app resolves per-second data through a five-layer chain: UI fragments → view model → data getter interface → FDS data getter → FDS uploader. Every layer ultimately depends on the same `gen_download_url` endpoint. No alternative per-second data API was identified in the reviewed decompiled paths.

---

## Native Detail-Loading Path

### UI → ViewModel

`SportDetailCommonFragment` and `SportDetailGpsFragment` delegate to `SportRecordDetailViewModel`, which calls:

```java
// com.xiaomi.fitness.detail.viewmodel.SportRecordDetailViewModel
public getGPSAndRecordData(SportBasicReport report) {
    String sid = report.getSid();
    FitnessDataGetter getter = FitnessDataExtKt.getInstance(FitnessDataGetter.INSTANCE);

    // Per-second HR, speed, cadence, etc.
    Map recordData = getter.getSportRecordData(sid, report);  // line ~540
    getSportRecordData(recordData, report);

    // GPS tracks
    Map gpsData = getter.getSportGpsData(sid, report);         // line ~623
    // ...dispatch to loadCommonGpsValue / loadTriathlonGpsValue / etc.
}
```

Two additional methods on the view model:

| Method | Purpose |
|---|---|
| `getSportRecoverRateData()` | Recovery rate via `FitnessDataGetter.getRecoverRateData()` |
| `getReportInfoFromNet()` (line ~2206) | Fetches route/course overlay metadata — **not** per-second HR/GPS |

### ViewModel → FDS Data Getter → FDS Uploader

```
SportRecordDetailViewModel.getGPSAndRecordData(report)
  → FitnessDataGetter.getSportRecordData(sid, report)        [interface]
    → FitnessDataGetterImpl.getSportRecordData(sid, report)   [delegates]
      → FitnessFDSDataGetter.getSportRecordData(sid, report)  [impl]
        → getFDSStoreData(sid, dataId)
          → FitnessFileUtils.readFile(local)  // cache check
          → FitnessFDSUploader.downloadFromFDS(sid, dataId)
            → genFdsUrlItem(sid, dataId) → gen_download_url API
            → download(FDSResultValue) → AES decrypt
        → FitnessDataParser.parseSportData(bytes, fileType)
```

### Version Guard

`FitnessFDSDataGetter.getSportRecordData()` and `getSportGpsData()` both check the report version before attempting any FDS download:

```java
if (sportBasicReport.getVersion() <= 0) {
    FitnessLogUtils.i(TAG, "no record file for version = " + sportBasicReport.getVersion());
    return MapsKt.emptyMap();
}
```

Activities with `version <= 0` are skipped entirely — no network call is made.

### FitnessDataId Construction

`FitnessFDSDataGetter` constructs `FitnessDataId` from report fields for each file type:

```java
FitnessDataId fitnessDataIdBuild = new FitnessDataId.Builder()
    .timeStampInSec(sportBasicReport.getTimeStamp())
    .timeZoneIn15Min(sportBasicReport.getTzIn15Min())
    .sportType(sportBasicReport.getProtoType())   // proto_type, NOT sport_type
    .fileType(0)                                   // 0=record, 2=GPS, 3=recovery
    .build();
```

The `version` field is **not** set in the builder for FDS downloads. It is only used in the version guard and for local data IDs.

---

## FDS Download (getFDSStoreData)

From `FitnessFDSDataGetter.getFDSStoreData()`:

```java
// 1. Check local file cache
File fDSDataFile = FitnessFileUtils.getFDSDataFile(context, sid, dataId);
byte[] file = FitnessFileUtils.readFile(fDSDataFile);
if (file != null && file.length > 0) {
    return file;  // local cache hit — no network call
}

// 2. Download from cloud (on cache miss)
byte[] bArr = FitnessFDSUploader.getInstance().downloadFromFDS(sid, dataId, ...);

// 3. Write to local cache on success
if (bArr != null && bArr.length > 0) {
    FitnessFileUtils.writeFile(fDSDataFile, bArr);
}
return bArr;  // null if download failed
```

**No retry logic**: if `downloadFromFDS` returns null (due to null `obj_key` or network error), the method returns null immediately. There is no retry, polling, or fallback to a different endpoint.

**No DB lookup**: `getFDSStoreData` does not consult the local FDS data ID database (`FDSDataIdEntity`). It computes the `FitnessDataId` from the report and attempts download directly.

### Null obj_key Handling

`FitnessFDSUploader.download()` throws when `obj_key` is absent:

```java
if (TextUtils.isEmpty(value.getObjectKey())) {
    throw new Exception("download objectKey is null");
}
```

`downloadFromFDS()` catches this exception and returns null. The caller receives null bytes and renders an empty data set for that file type.

---

## React Native Bridge

`YRNSportModule` (a React Native bridge class) exposes `@ReactMethod getSportRecord(category, startSecStr, endSecStr, callback)`. Internally it delegates to the same `FitnessDataGetter` interface and thus the same `FitnessFDSDataGetter` → `downloadFromFDS` path. For the reviewed sport-detail bridge path, `YRNSportModule` does not implement a separate data-loading mechanism.

---

## Sync Flow After Login

`FitnessDataRemoteImpl.syncWithServer()` (line 433) triggers two processes:

```java
getIncSyncManager().triggerSyncWithServer(forceSync, triggerSource, callback);
getHistorySyncManager().pullRecentLeastOneYearData();
```

### Incremental Sync (IncrementalSyncController)

The incremental sync controller performs both reads and writes:

| Direction | Endpoint | Purpose |
|---|---|---|
| Pull | `data/get_sport_records_by_watermark` | Fetch sport reports since last watermark |
| Push | `data/up_sport_records` | Upload locally-recorded sport reports (batch of 20) |
| Push | `healthapp/service/gen_upload_url` | Get presigned URLs for FDS binary upload |
| Push | CDN PUT | Upload AES-encrypted binary sport data |
| Push | `statistics/report_sports_sync_info` | Report sync completion metadata per sport record |

The `report_sports_sync_info` payload includes diagnostic codes per activity:

| Code | Meaning |
|---|---|
| 0 | No binary data |
| 1 | Recording data included |
| 2 | GPS data included |
| 3 | Both recording + GPS |
| 1001 | GPS upload disabled |

### Historical Sync (FitnessServerHistorySync)

`pullRecentLeastOneYearData()` (line 861) calls two methods:

```java
pullLeastOneYearFitnessData();
pullAllSportReport();
```

**`pullAllSportReport()`** (line 635) launches a coroutine that delegates to `pullAllSportReportFromServer()` → `CrossYearSyncController.pullAllSportReport()`. This fetches all sport report metadata.

**`pullLeastOneYearFitnessData()`** (line 696) launches a coroutine that creates three async `Deferred` tasks (obfuscated as `C7162xaa199a6`, `C7163xc14dc62a`, `C7164x306c8b15`). The mapping from these obfuscated coroutine classes to specific named methods is not directly verifiable — the class names were mangled during compilation. After all three complete, the coroutine sequentially calls `pullAllFitnessDataOfSpecialKeys()` (verified at line ~430 in the `C71611` state machine).

Directly verified delegates from this class:

| Method | Delegate | Verified |
|---|---|---|
| `pullAllSportReportFromServer()` | `CrossYearSyncController.pullAllSportReport()` | Yes — line ~690 |
| `pullOneYearFitnessFromServer()` | `CrossYearSyncController.pullRecentOneYearFitnessData()` | Yes — line ~800 |
| `pullAllFitnessDataOfSpecialKeys()` | `CrossYearSyncController.pullAllFitnessDataByKey([WomenHealthStatus, WomenHealthSymptoms, weight, pai])` | Yes — line ~630 |

Which of these delegates correspond to which of the three async tasks inside `pullLeastOneYearFitnessData()` cannot be determined from the obfuscated coroutine class names alone.

The directly verified delegates listed above are metadata-oriented (sport report JSON summaries, daily fitness aggregates) and do not invoke `FitnessFDSDataGetter` or `downloadFromFDS`. However, the three obfuscated async tasks inside `pullLeastOneYearFitnessData()` have not been fully traced, so whether any of them download FDS binary data cannot be confirmed or ruled out from the decompiled source alone.

### FDS Upload During Sync

FDS binary data is uploaded to the server during incremental sync when the phone has locally-recorded activity data (from a connected watch/band or phone-recorded workout):

```
IncrementalSyncController.pushSportFDSData()
  → FitnessFDSDataDaoUtils.uploadDataToFDS()
    → processLocalPhoneFdsId()
    → uploadNoLocalPhoneDataToFDS()
      → FitnessFDSUploader.uploadToFDS(sid, dataId, bytes)
        → gen_upload_url API → presigned PUT URL + obj_key
        → AES encrypt(bytes, obj_key) → HTTP PUT to CDN
```

After upload, `FitnessFDSDataDaoUtils.recordFDSDataIdToDB()` records the data ID with `upload = true`. Then `SportSyncFinishController` collects uploaded records and reports completion via `report_sports_sync_info`.

---

## API Endpoints Referenced

### FDSApiService

| Annotation | Path | Purpose |
|---|---|---|
| `@gmb("service/gen_download_url")` | `healthapp/service/gen_download_url` | Presigned download URL + `obj_key` for FDS data |
| `@gmb("service/gen_upload_url")` | `healthapp/service/gen_upload_url` | Presigned upload URL + `obj_key` for FDS data |

Base URL: `https://hlth.io.mi.com/healthapp/`

### FitnessApiService (relevant subset)

| Path | Purpose |
|---|---|
| `data/get_sport_records_by_time` | Query sport records within a time range |
| `data/get_sport_records_by_watermark` | Incremental sport record pull (watermark-based cursor) |
| `data/up_sport_records` | Push sport report metadata to server |
| `data/get_fitness_data_by_time` | Query fitness data (e.g. `huami_sport_record`) by time |
| `statistics/report_sports_sync_info` | Report sync completion and binary data state per sport record |

Base URL: `https://hlth.io.mi.com/app/v1/`

---

## Decompilation Gaps

- **APK version v3.52.0i**: Newer versions may implement additional handling for null `obj_key` entries or use different API endpoints.

- **`@gmb` annotation HTTP method**: The decompiled `@gmb` annotation is obfuscated and does not explicitly declare GET or POST. The actual HTTP method is resolved at runtime by a custom Retrofit `CallFactory` or the `CloudInterceptor`.

- **`CloudInterceptor` signing details**: The `@Secret(pathPrefix = "healthapp/")` annotation on `FDSApiService` triggers the `CloudInterceptor` to encrypt request parameters and sign them. The interceptor uses RC4/nonce-based signing similar to the standard Xiaomi cloud API, but the exact parameter transformations for POST vs GET requests differ and cannot be fully reconstructed from the obfuscated interceptor code alone.

- **Server-side FDS data preparation**: The decompiled source does not reveal what server-side processing is required before `gen_download_url` returns a valid `obj_key`. The upload flow (`gen_upload_url` → CDN PUT → `report_sports_sync_info`) establishes that the server receives both the encrypted binary and a sync completion report. Whether any of these steps are prerequisites for `gen_download_url` to return `obj_key` for the same data is not determinable from client code alone.

- **`get_sport_records_by_watermark` vs `get_sport_records_by_time`**: Both APIs return sport record metadata. Whether the server treats these differently with respect to FDS data availability (e.g., activating FDS data for records pulled via the watermark API) is not determinable from client code.

- **JavaScript React Native layer**: The sport detail UI uses `YRNSportModule` as a bridge to native code, but the JavaScript-side rendering logic and any additional data massaging is not visible in the decompiled Java source.
