# Mi Fitness Activity Sync & Upload — Decompiled App Reference

Reference documentation for the activity data upload and cloud synchronization architecture of the Mi Fitness Android app (v3.52.0i), recovered from decompilation of the APK via JADX.

---

## Overview

Mi Fitness uses a layered sync architecture to upload activity data to the cloud backend at `https://hlth.io.mi.com/`. Activity summaries (sport reports) and binary detail files (FDS data) follow separate upload paths but share a common scheduling and orchestration layer. The Android **JobScheduler** triggers periodic sync every 15 minutes, while event-driven triggers can request immediate one-shot syncs. All cloud data operations flow through an IPC Binder service (`FitnessDataRemoteImpl`) that delegates to either incremental or historical sync controllers.

---

## Key Classes

| Class | Package | Role |
|---|---|---|
| `SyncJobService` | `com.xiaomi.fitness.sync` | Android `JobService`; schedules periodic (15 min) and one-shot sync jobs via `JobScheduler` |
| `SyncEngine` | `com.xiaomi.fitness.sync` | Top-level orchestrator; coordinates device sync (BLE, calendar, weather) and delegates cloud sync to `FitnessServerSyncMgr` |
| `FitnessServerSyncMgr` | `com.xiaomi.fit.fitness` | Public sync API; implements `FitnessServerSyncer`; proxies calls over IPC Binder to `FitnessDataRemoteImpl` |
| `FitnessDataRemoteImpl` | `com.xiaomi.fit.fitness.remote` | Binder service (key ID 12, name `mRemoteSyncMgr`); entry point for all cloud data operations |
| `FitnessServerIncSync` | `com.xiaomi.fit.fitness.remote` | Incremental sync orchestrator; launches parallel coroutine jobs for sport, fitness, medical, and project data |
| `FitnessServerHistorySync` | `com.xiaomi.fit.fitness.remote` | Historical sync; full pulls across date ranges using `CrossYearSyncController` |
| `IncrementalSyncController` | `com.xiaomi.fit.fitness.persist.process` | Actual upload/download executor; pushes sport reports and FDS data to the cloud in batches |
| `CrossYearSyncController` | `com.xiaomi.fit.fitness.persist.process` | Multi-year historical data puller; used for initial setup and recovery |
| `CloudSyncBaseRequest` | `com.xiaomi.fit.fitness.persist.process` | Base class for sync operations; implements batched pull-and-save logic with pagination |
| `SportSyncFinishController` | `com.xiaomi.fit.fitness.persist.process` | Post-upload metadata reporter; sends `SportSyncInfo` to `/statistics/report_sports_sync_info` |
| `FitnessFDSDataDaoUtils` | `com.xiaomi.fit.fitness.persist.p112db.utils` | FDS upload coordinator; records pending FDS files via `recordFDSDataIdToDB()` (`upload=false`), records post-upload sport report bookkeeping via `recordSportReportId()` (`upload=true`), and delegates uploads to `FitnessFDSUploader` |
| `SportReportDaoUtils` | `com.xiaomi.fit.fitness.persist.p112db.utils` | Room DAO wrapper for sport report entities; tracks upload state per record |
| `FitnessDBDataUpdater` | `com.xiaomi.fit.fitness.remote` | Database writer; converts `SportBasicReport` to `SportDBKey` + JSON and calls `SportReportDaoUtils.recordSportReportToDB()` |
| `FitnessFDSUploader` | `com.xiaomi.fit.fitness.persist` | Singleton; encrypts and uploads binary FDS data to presigned S3/CDN URLs |
| `FitnessDataPreference` | `com.xiaomi.fit.fitness.export.p111sp` | SharedPreferences wrapper; stores `SYNC_TIME` (last cloud sync timestamp) and related sync state |
| `SettingPreference` | `com.xiaomi.fitness.about.preference` | User preferences; includes `notUploadGpsEnable` flag that suppresses GPS FDS uploads |
| `YRNSportModule` | `com.xiaomi.wearable.yrn.components.sport` | React Native bridge; exposes sport data and sync APIs to the JavaScript UI layer |
| `FitnessServerSyncer` | (interface) | Interface implemented by `FitnessServerSyncMgr`; defines `syncWithServer()`, `pullServerSportReport()`, etc. |

---

## Sync Scheduling

### JobScheduler Configuration

`SyncJobService` uses the Android `JobScheduler` API (not WorkManager or AlarmManager).

| Constant | Value | Description |
|---|---|---|
| `SYNC_JOB_ID` | `9999` | Unique job identifier |
| `sIntervalMillis` | `900000` | Periodic interval in milliseconds (15 minutes) |
| `EXRA_IS_ONCE` | `"isOnce"` | `PersistableBundle` key flagging one-shot jobs |

### Scheduling Methods

| Method | Behavior |
|---|---|
| `schedulePeriodic()` | `setPeriodic(900000L)` — repeating 15-minute job |
| `scheduleOnce()` | `setMinimumLatency(1L)` — immediate one-shot; extras include `{isOnce: 1}` |
| `scheduleRefresh()` | `setMinimumLatency(900000L)` — delayed one-shot (15-minute offset); extras include `{isOnce: 1}` |

`scheduleOnce()` cancels any existing job 9999 before scheduling the new one-shot.

### Job Lifecycle

```java
SyncJobService.onStartJob(params)
    → Extract isOnce flag from params.getExtras()
    → Create SyncEngine(applicationContext, false, 2, null)
    → SyncEngine.syncAll(callback)
    → return true  // job continues in background

SyncJobService.finishJob()
    → if (mIsOnce) → scheduleRefresh()  // re-arm 15-min delayed job
    → jobFinished(mParams, false)        // no reschedule needed
```

After a one-shot sync completes, `finishJob()` calls `scheduleRefresh()` to re-arm the periodic cycle with a 15-minute delay.

---

## Sync Trigger Sources

Defined in `com.xiaomi.fit.data.common.constant.SyncTriggerSource`:

| Constant | String Value | Context |
|---|---|---|
| `AutoSync` | `"auto_sync"` | Periodic 15-minute JobScheduler tick |
| `HomePage` | `"home_page"` | User opens the app home screen |
| `AccountLogin` | `"account_login"` | User completes login |
| `DeviceSync` | `"device_sync"` | BLE device sync event |
| `CloudSyncSwitch` | `"cloud_sync_switch"` | User toggles cloud sync setting |
| `AddOrDelete` | `"add_or_delete"` | Activity added or deleted locally |
| `MigrationFitnessDB` | `"migration_fitness_db"` | Database migration event |
| `MigrationOldHealth` | `"migration_old_health"` | Legacy health data migration |
| `OverseaDataMigration` | `"oversea_data_migration"` | Overseas data migration |
| `MiuiFunctionStep` | `"miui_function_step"` | MIUI step counter callback |
| `MiuiFunctionStand` | `"miui_function_stand"` | MIUI stand reminder callback |
| `MiuiFunctionManualMarkStand` | `"miui_function_stand_manual"` | Manual stand mark |
| `MiuiFunctionMotion` | `"miui_function_motion"` | MIUI motion callback |
| `MiuiFunctionSleep` | `"miui_function_sleep"` | MIUI sleep callback |
| `EcgInterpretedNotification` | `"ecg_interpreted_notification"` | ECG analysis complete |
| `EcgListPage` | `"ecg_list_page"` | User opens ECG list |
| `SleepAnimalGenerate` | `"sleep_animal_generate"` | Sleep animal generation |

---

## Cloud Sync Call Chain

### Top-Level Flow

```
SyncJobService.onStartJob()
  → SyncEngine.syncAll(callback)
    → FitnessServerSyncMgr.syncWithServer(forceSync, triggerSource, callback)
      → [IPC Binder call]
        → FitnessDataRemoteImpl.syncWithServer(forceSync, triggerSource, callback)
          → FitnessServerIncSync.triggerSyncWithServer(forceSync, triggerSource, callback)
            → FitnessServerIncSync.syncDataWithServer(triggerSource, isForeground)
```

### FitnessDataRemoteImpl.syncWithServer

```java
syncWithServer(forceSync, triggerSource, callback) {
    getIncSyncManager().triggerSyncWithServer(forceSync, triggerSource, callback);
    getHistorySyncManager().pullRecentLeastOneYearData();
}
```

The incremental and historical sync managers are lazy-loaded singletons:
- `FitnessServerIncSync` — handles recent changes
- `FitnessServerHistorySync` — handles full historical pulls

### Parallel Coroutine Jobs

`FitnessServerIncSync.syncDataWithServer()` launches four parallel `Deferred` coroutine jobs:

| Job | Method | Data Type |
|---|---|---|
| `sportSyncJob` | `incSyncInstance.syncSportWithServer()` | Sport reports + FDS binary data |
| `dailySyncJob` | `incSyncInstance.syncFitnessDataWithServer()` | Daily aggregated fitness data |
| `medicalSyncJob` | `incSyncInstance.syncMedicalDataWithServer()` | Medical data (ECG, etc.) |
| `projectSyncJob` | `incSyncInstance.syncProjectDataWithServer()` | Training plans and projects |

All four jobs are awaited before `handleSyncResult()` notifies listeners and sets `SYNC_TIME` in preferences.

### Sync Throttling

`FitnessServerIncSync` checks `(currentTime - FitnessDataPreference.getSYNC_TIME()) < cloud_sync_bg_interval` before executing. An `AtomicBoolean isTaskSyncing` prevents concurrent sync runs.

---

## Sport Report Upload

### Database State Tracking

`SportReportDaoUtils.recordSportReportToDB()` inserts a `SportReportEntity` with:

```java
entity.setUpload(false);   // pending upload
entity.setDeleted(false);
```

The `upload` boolean field tracks whether the record has been pushed to the cloud.

### Upload Sequence

```
IncrementalSyncController.syncSportWithServer()
  → pullSportReportByWaterMark(localWatermark)       // pull new server data
  → pushSportReport2Server()                          // push local data
    → pushNewSportReport2Server()
      → SportReportDaoUtils.getSportNotUploadToServer()
      → computeBatchGroupList(list, 20)              // batch into groups of 20
      → for each batch:
          → Convert SportReportEntity → SportReportModel
          → FitnessDataRequest.uploadSportReports(batch)
            → POST /data/up_sport_records
          → SportReportDaoUtils.markSportReportHasUploaded(batch)
    → pushDeletedSportReport2Server()                 // sync deletions
  → pushSportFDSData()                                // upload binary data
  → SportSyncFinishController.uploadSportSyncInfo()   // report metadata
```

### Upload Endpoint

| Path | Method | Request Type |
|---|---|---|
| `data/up_sport_records` | POST | `UploadSportReportParam` |

`UploadSportReportParam` fields:

| JSON Field | Type | Description |
|---|---|---|
| `phone_id` | `String` | Device identifier |
| `sport_records` | `List<SportReportModel>` | Activity records batch (max 20) |

### Batch Limits

| Data Type | Batch Size | Constant |
|---|---|---|
| Sport reports | 20 | `LIMIT_UP_SPORT_REPORT` |
| Fitness data | 60 | `LIMIT_UP_FITNESS_DATA` |
| Sport sync info | 100 | `LIMIT_UP_SPORT_INFO` |
| Step data | 500 | `upLoadStepLimit` |

### Watermark-Based Incremental Pull

Sport report pulls use `getSportRecordsByWM` with a `phone_id` and `watermark` value to fetch only records newer than the last sync point. The watermark is stored locally and updated after each successful pull.

---

## FDS Binary Data Upload

### Upload State Tracking

`FDSDataIdEntity` in the Room database tracks each binary file's upload status:

| Column | Type | Description |
|---|---|---|
| `sid` | `String` | Activity/device SID |
| `dataId` | `String` | Encoded data identifier |
| `fileType` | `int` | File type (0=record, 1=report, 2=GPS, 3=recovery rate) |
| `upload` | `boolean` | `false` = pending upload; `true` = uploaded |

### Upload Sequence

```
IncrementalSyncController.pushSportFDSData()
  → FitnessFDSDataDaoUtils.uploadDataToFDS()
    → Phase 1: processLocalPhoneFdsId()
      → Reconcile local phone SID vs. current phone SID
      → Re-record files if device changed
    → Phase 2: uploadNoLocalPhoneDataToFDS()
      → FdsDataIdDao.getNotUploadIds()                   // query pending files
      → for each FDSDataIdEntity:
          → FitnessFileUtils.readFile(sid, dataId)        // read local binary
          → Skip if dataType=1, fileType=2, and notUploadGps2Server()
          → FitnessFDSUploader.uploadToFDS(sid, dataId, file)
            → FitnessFDSRequest.getFDSUploadUrl(sid, items)
              → POST /healthapp/service/gen_upload_url    // get presigned URL
            → AES-CBC encrypt(data, key, IV)
            → HTTP PUT to presigned S3/CDN URL
          → markFDSDataHasUpload(entity)
```

### GPS Upload Suppression

`SettingPreference.getNotUploadGpsEnable()` returns a user-configurable boolean. When `true`, FDS files with `dataType=1` (sport) and `fileType=2` (GPS) are skipped during upload. The corresponding track point code `TRACK_POINT_NOT_UP_GPS_SWITCH = 1001` is reported via `SportSyncFinishController`.

### FDS Upload Endpoint

| Path | Method | Purpose |
|---|---|---|
| `healthapp/service/gen_upload_url` | POST | Generate presigned upload URLs |

Request payload: `FDSRequestParam` (same structure as download, serialized with `did` for SID).

### FDS Upload Encryption

| Parameter | Value |
|---|---|
| Algorithm | `AES/CBC/PKCS5Padding` |
| IV | `"1234567887654321"` (16 bytes UTF-8) |
| Key | AES key from preferences (128-bit or 256-bit) |
| Encoding | Base64 URL-safe, no padding |

---

## Post-Upload Metadata Reporting

### SportSyncFinishController

After FDS uploads complete, `SportSyncFinishController.uploadSportSyncInfo()` reports metadata to the server:

```
SportSyncFinishController.uploadSportSyncInfo()
  → FitnessFDSDataDaoUtils.getSportReportId(100)    // batch of 100
  → for each sport:
      → getSportSyncInfo(sportType, sportKey, entityList)
      → getSportFileState(sportType, trackCodeMap)
  → POST /statistics/report_sports_sync_info
```

### SportSyncInfo Fields

| JSON Field | Type | Description |
|---|---|---|
| `sport_key` | `FitnessDataKey` | Activity identifier |
| `content_state` | `int` | Upload state/status |
| `track_point` | `List<Integer>` | Track point diagnostic codes (optional) |

### File State Constants

| Constant | Value | Meaning |
|---|---|---|
| `FILE_STATE_NOT_RECORD_NO_GPS` | `0` | No binary data present |
| `FILE_STATE_RECORD` | `1` | Has per-second recording data |
| `FILE_STATE_GPS` | `2` | Has GPS track data |
| `FILE_STATE_RECORD_GPS` | `3` | Has both recording and GPS data |

### Track Point Diagnostic Codes

| Constant | Value | Meaning |
|---|---|---|
| `TRACK_POINT_NOT_UP_GPS_SWITCH` | `1001` | GPS upload disabled by user preference |
| `TRACK_POINT_NO_GPS_FOR_PERMISSION` | `1002` | GPS permission not granted |
| `TRACK_POINT_NO_GPS` | `1003` | GPS data not available |
| `TRACK_POINT_NO_RECORD` | `1004` | Recording data missing |

---

## Cloud Upload Endpoints

All upload endpoints are declared in `FitnessApiService` under `@BaseUrl(host = "https://hlth.io.mi.com/", path = "app/v1/")`:

| Endpoint Path | Purpose | Request Wrapper |
|---|---|---|
| `data/up_sport_records` | Sport report summaries | `UploadSportReportParam` |
| `data/up_fitness_data` | Daily fitness data | `UploadFitnessDataParam` |
| `data/up_medical_data` | Medical data (ECG, etc.) | (medical param) |
| `data/up_aggregated_fitness_data` | Aggregated stats | (aggregation param) |
| `data/up_project_data` | Training plans | (project param) |
| `data/up_huami_raw_data` | HuaMi raw data | (HuaMi param) |
| `data/up_third_raw_data` | Third-party data | (third-party param) |
| `statistics/report_sports_sync_info` | Post-sync metadata | `List<SportSyncInfo>` |

FDS upload URL endpoints are declared in `FDSApiService` under `@BaseUrl(host = "https://hlth.io.mi.com/", path = "healthapp/")`:

| Endpoint Path | Purpose |
|---|---|
| `service/gen_upload_url` | Generate presigned upload URLs for FDS binary data |
| `service/gen_download_url` | Generate presigned download URLs for FDS binary data |
| `algorithm/gen_file_upload_url` | Upload URLs for algorithm/sleep source data |

---

## Activity Recording to Upload Lifecycle

### Step 1 — Local Recording

When a workout completes (on the phone or received from a BLE device), the app records it locally:

```
FitnessDBDataUpdater.recordSportReport(sid, sportBasicReport)
  → Convert SportBasicReport to SportDBKey + JSON value
  → SportReportDaoUtils.recordSportReportToDB(sportKey, json)
    → SportReportEntity created with upload=false
    → SportReportDao.insertAll(entity)
```

FDS binary files (per-second records, GPS tracks) are saved to local storage and registered in the database:

```
FitnessFDSDataDaoUtils.recordFDSDataIdToDB(sid, dataId)
  → FDSDataIdEntity created with upload=false
  → FdsDataIdDao.insert(entity)
```

`recordFDSDataIdToDB()` creates one `FDSDataIdEntity` per binary file with `upload=false`, marking it as pending upload.

### Step 2 — Sync Trigger

One of the following events triggers synchronization:

| Trigger | Timing | Source |
|---|---|---|
| Periodic JobScheduler | Every 15 minutes | `SyncJobService` job 9999 |
| Home page open | On navigation | `"home_page"` trigger |
| Account login | After authentication | `"account_login"` trigger |
| Activity add/delete | On user action | `"add_or_delete"` trigger |
| Cloud sync toggle | On preference change | `"cloud_sync_switch"` trigger |
| BLE device sync | On device connection | `"device_sync"` trigger |

### Step 3 — Upload Execution

The incremental sync controller queries the local database for records with `upload=false` and uploads them in batches:

1. **Sport reports**: batches of 20, POST to `/data/up_sport_records`
2. **FDS binary files**: individually encrypted and PUT to presigned S3 URLs
3. **Metadata**: batches of 100, POST to `/statistics/report_sports_sync_info`

### Step 4 — Completion

Successfully uploaded sport reports are marked with `upload=true` via `SportReportDaoUtils.markSportReportHasUploaded()`. FDS binary records are marked via `markFDSDataHasUpload()`. `FitnessDataPreference.SYNC_TIME` is set to `System.currentTimeMillis()`.

After sport reports are successfully uploaded, `IncrementalSyncController.saveSportReportKey()` calls `FitnessFDSDataDaoUtils.recordSportReportId(sportList)` to create bookkeeping `FDSDataIdEntity` records with `upload=true` and `fileType=1` (report). These records link uploaded sport reports to the FDS tracking table for use by `SportSyncFinishController`; they are not pending-upload entries.

```
IncrementalSyncController.pushSportReport(uploadList)
  → uploadSportReports(batch)           // POST to server
  → markSportReportHasUploaded(list)     // mark sport report upload=true
  → saveSportReportKey(list)
    → FitnessFDSDataDaoUtils.recordSportReportId(list)
      → FDSDataIdEntity created with upload=true, fileType=1
      → FdsDataIdDao.insert(entities)
```

---

## BLE Device Sport Finish Handlers

When a BLE wearable device reports workout completion, the callback interface `ISportDataChangedListener` is invoked:

```java
void onSportFinished(boolean validSport, byte[] ids, FinishSportData finishSportData)
```

Implementations:

| Class | Context |
|---|---|
| `CourseBasePlayerVM` | Course/guided workout tracking |
| `EcoCourseBasePlayerVM` | Eco device guided workouts |
| `ScreenCastHandler` | Screen casting/mirroring of workout data |
| `SportXmsApiImpl` | XMS device sync protocol |

These handlers write the sport data to the local database. The data is then picked up by the next scheduled or triggered sync cycle.

---

## React Native Bridge

`YRNSportModule` (`com.xiaomi.wearable.yrn.components.sport`) exposes sport data operations to the React Native JavaScript UI layer. It holds lazy references to:

- `FitnessServerSyncer` — via `getServerSyncer()` (private) — used for training plan operations (`deleteCurDataByPlanId()`, `syncCurDatePlanInfo()`)
- `FitnessDataGetter` — via `getDataGetter()` (private) — for querying local/cached sport data

`getServerSyncer()` is not called for general sync triggering (e.g., `syncWithServer()`) within the decompiled `YRNSportModule` source.

Exported `@ReactMethod` methods include:

| Method | Purpose |
|---|---|
| `gotoSportPage(sportType)` | Navigate to sport page |
| `getLatestSportReport(callback)` | Fetch latest sport report |
| `getSportRecord(category, start, end, callback)` | Query sport records by time range |
| `terminateTrainingPlan(planId, planType, curProgress, success, withReport)` | Terminate a training plan; calls `getServerSyncer().deleteCurDataByPlanId()` |
| `updateTodayAbilityPlan(planInfo, trainingType, id, dateStr)` | Update daily ability plan; calls `getServerSyncer().syncCurDatePlanInfo()` |

---

## Decompilation Gaps

- **Immediate post-workout sync trigger:** The exact code path that triggers sync immediately after a phone-recorded workout completes is not fully recovered. `SyncJobService.scheduleOnce()` exists for one-shot immediate sync, but direct call sites from sport completion handlers are not visible in the decompiled source.

- **`SyncEngine.syncAll()` internals:** `SyncEngine` coordinates device-level sync (BLE, calendar, weather, etc.) and delegates cloud sync to `FitnessServerSyncMgr`, but the exact branching logic between device sync and cloud sync is partially obfuscated.

- **`FitnessFDSUploader.uploadToFDS()` retry logic:** The upload method includes error handling and retry mechanisms, but intermediate steps between presigned URL generation and actual HTTP PUT are partially obfuscated.

- **AES key source for FDS upload:** The encryption key used by `FitnessFDSUploader` is loaded from preferences, but the mechanism that generates and stores this key (whether server-provided or locally generated) is not recovered.

- **`add_or_delete` trigger call sites:** The string constant `SyncTriggerSource.AddOrDelete = "add_or_delete"` is defined in the `SyncTriggerSource` annotation, but the specific call sites passing this trigger to `syncWithServer()` are not recovered from the decompiled Java source.

- **Phone-recorded sport initial save path:** The entry point that creates `SportReportEntity` and `FDSDataIdEntity` records after a phone-only (no BLE device) workout completes is not fully traced. `FitnessDBDataUpdater.recordSportReport()` is the database writer, but its callers from the sport recording UI are not recovered.

- **`FitnessServerSyncMgr` to `FitnessDataRemoteImpl` IPC:** The Binder proxy uses a key ID of 12 (`KEY_FITNESS_DB_DATA_BINDER`) and `ServiceConnector.getBinder()` to obtain the remote interface. The `ServiceConnector` and its service registration mechanism are partially obfuscated.
