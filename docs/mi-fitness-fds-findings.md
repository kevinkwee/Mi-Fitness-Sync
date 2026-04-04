# Mi Fitness FDS (File Data Service) — Decompiled App Reference

Reference documentation for the FDS cloud storage system, binary sport record format, and sport type encoding in the Mi Fitness Android app, recovered from decompilation of the APK via JADX.

---

## Overview

Mi Fitness stores detailed per-second workout data (heart rate tracks, GPS coordinates, sport reports, recovery rate) as AES-encrypted binary blobs on a cloud **FDS** (File Data Service) layer. The Android app constructs a data identifier suffix from the activity's timestamp, timezone, and protocol type, requests presigned download URLs via `healthapp/service/gen_download_url`, downloads the encrypted payload, decrypts with AES-CBC, and dispatches the raw bytes to a sport-type-specific binary parser.

---

## Key Classes

| Class | Package | Role |
|---|---|---|
| `FitnessFDSDataGetter` | `com.xiaomi.fit.fitness.impl.internal` | Entry point for downloading binary sport data; checks local cache first, falls back to cloud FDS |
| `FitnessFDSUploader` | `com.xiaomi.fit.fitness.persist` | FDS upload/download orchestration; constructs suffix, calls API, decrypts response |
| `FitnessFDSRequest` | `com.xiaomi.fit.fitness.persist.server` | Extends `BaseRequest<FDSApiService>`; serializes `FDSRequestParam` and calls `getFDSDownloadUrl()` / `getFDSUploadUrl()` |
| `FDSItem` | `com.xiaomi.fit.fitness.persist.server.data` | Request item model (`timestamp`, `suffix`) |
| `FDSRequestParam` | `com.xiaomi.fit.fitness.persist.server.data` | Request envelope model (`sid` serialized as `"did"`, `items`) |
| `FDSResultValue` | `com.xiaomi.fit.fitness.persist.server.data` | Response model (`url`, `obj_key`, `obj_name`, `method`, `expires_time`) |
| `FitnessDataId` | `com.xiaomi.fit.data.common.data.p101mi` | Data identifier; encodes timestamp, timezone, data type, sport type, file type, and version into compact byte arrays |
| `FitnessDataParser` | `com.xiaomi.fit.fitness.parser` | Dispatches decrypted binary bytes to sport-type-specific record/report/GPS parsers |
| `FitnessDataValidity` | `com.xiaomi.fit.fitness.parser` | Returns expected `dataValid` byte lengths per sport type and version |
| `FitnessFDSDataDaoUtils` | `com.xiaomi.fit.fitness.persist.db.utils` | Builds `FitnessDataId` from `SportBasicReport` fields — maps `proto_type` to `sportType` |
| `FitnessFileUtils` | `com.xiaomi.fit.fitness.persist.utils` | Local file I/O; resolves cache paths for BLE-synced sport data |
| `HashCoder` | `com.xiaomi.fit.fitness.persist.utils` | SHA-1 hashing utility (`MessageDigest.getInstance("SHA1")`) |
| `AESCoder` | `com.xiaomi.fit.fitness.persist.utils` | AES-CBC encrypt/decrypt for FDS payloads |
| `SportRecordConverter` | `com.xiaomi.fit.fitness.impl.internal.sport` | Converts parsed binary records into Mi Fitness display format |

---

## FDS Download & Decryption Flow

### High-Level Sequence

```
FitnessFDSDataGetter.getSportRecordData(sid, dataId)
  → Check local cache: FitnessFileUtils.getFDSDataFile(context, sid, dataId)
  → Cache miss → FitnessFDSUploader.downloadFromFDS(sid, dataId)
    → Construct FDSItem with computed suffix and timestamp
    → POST to healthapp/service/gen_download_url
    → Receive presigned URL map keyed by suffix_timestamp
    → HTTP GET binary data from presigned URL
    → AES-CBC decrypt with obj_key (IV = "1234567887654321")
  → FitnessDataParser.parse(dataId, decryptedBytes)
    → Dispatch to sport-type-specific parser
```

`FitnessFDSDataGetter` exposes several entry points by file type:

| Method | File Type | Description |
|---|---|---|
| `getSportRecordData()` | 0 | Per-second sport record (HR, speed, etc.) |
| `getSportGpsData()` | 2 | GPS track data |
| `getSportRecoverRateData()` | 3 | Recovery rate data |
| `getEcgRecordData()` | — | ECG data (includes `EcgFilterAlgo` post-processing) |
| `getTemperatureFileData()` | — | Temperature data by start date |

### AES Decryption Parameters

From `AESCoder`:

| Parameter | Value |
|---|---|
| Algorithm | `AES/CBC/PKCS5Padding` |
| Key | `obj_key` from `FDSResultValue`, Base64URL-decoded (flag 8 = `URL_SAFE`) to 16 bytes |
| IV | Fixed: `"1234567887654321"` (16 bytes UTF-8) |

### Additional FDS Paths

`FitnessFDSRequest` also exposes:

| Method | Purpose |
|---|---|
| `getFDSUploadUrl()` | Generates presigned upload URLs |
| `getSleepSrcDataUploadUrl()` | Upload URL for sleep algorithm source data (`GetAlgoFileUrlParam`) |
| `getRouteFDSUrl()` | Download URL for route/track data (separate from sport GPS) |

---

## FDS Suffix Construction

### Suffix Format

```
suffix = base64url_nopad(genDataIdKeyBytes) + "_" + base64url_nopad(SHA1(sid_utf8))
```

The separator is `_` (underscore), sourced from `RegionManagerImpl.LOCALE_REPORTED_SERVER_SEPARATOR = "_"` in `com.xiaomi.fitness.login.region.RegionManagerImpl`.

Base64 flags: `NO_WRAP | NO_PADDING | URL_SAFE` = `2 | 1 | 8` = **11**.

### genDataIdKeyBytes (6 bytes, little-endian)

From `FitnessFDSUploader.genDataIdKeyBytes()`:

```java
ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);
byteBufferOrder.putInt((int) dataId.getTimeStamp());
byteBufferOrder.put((byte) dataId.getTzIn15Min());
byteBufferOrder.put(dataId.genDataTypeByte());
```

Layout: `[timestamp (4 bytes LE)] [tzIn15Min (1 byte)] [genDataTypeByte (1 byte)]`

### Server Key (for response map lookup)

From `FDSItem.toServerKey()`:

```java
return this.suffix + RegionManagerImpl.LOCALE_REPORTED_SERVER_SEPARATOR + this.timeStamp;
```

The separator is `RegionManagerImpl.LOCALE_REPORTED_SERVER_SEPARATOR = "_"`. The FDS response is a map keyed by `suffix_timestamp` (underscore separator), each value being an `FDSResultValue`.

### FDS Request Format

`FDSRequestParam` serializes as:

```json
{
  "did": "<sid>",
  "items": [
    {"timestamp": <unix_sec>, "suffix": "<computed_suffix>"}
  ]
}
```

Note: the Java field is `sid` but it serializes to JSON key `"did"` via `@SerializedName("did")`.

---

## FitnessDataId Encoding

### Class: `com.xiaomi.fit.data.common.data.p101mi.FitnessDataId`

`FitnessDataId` encodes activity metadata into compact byte arrays. The class supports multiple serialization formats for different purposes:

### Local Data ID (7 bytes)

`toByteArray()` — used for local BLE-synced file storage:

```
[timestamp (4 bytes LE)] [tzIn15Min (1 byte)] [version (1 byte)] [genDataTypeByte (1 byte)]
```

### FDS Key (6 bytes)

`genDataIdKeyBytes()` — used for cloud FDS suffix construction:

```
[timestamp (4 bytes LE)] [tzIn15Min (1 byte)] [genDataTypeByte (1 byte)]
```

The FDS key **omits the version byte** compared to the local data ID.

### Server Data ID

`convertToServerDataId()` — a separate format used for server-side data identification:

| Data Type | Bytes | Format |
|---|---|---|
| Daily (`dataType=0`) | 6 | `[timestamp(4 LE)] [tzIn15Min(1)] [version(1)]` — no type byte |
| Sport (`dataType=1`) | 7 | `[timestamp(4 LE)] [tzIn15Min(1)] [version(1)] [sportType(1)]` — raw sportType, not genDataTypeByte |

This format differs from both the local ID and the FDS key.

### genDataTypeByte Encoding

```java
public final byte genDataTypeByte() {
    return (byte) ((this.dataType << 7) + (this.sportType << 2) + (this.dailyType << 2) + this.fileType);
}
```

Bit layout:

| Bits | Field | Range |
|---|---|---|
| 7 | `dataType` | 0–1 |
| 6–2 | `sportType` or `dailyType` (mutually exclusive) | 0–31 |
| 1–0 | `fileType` | 0–3 |

For sport data (`dataType = 1`):

```
genDataTypeByte = (1 << 7) | (sportType << 2) | fileType
               = 128 + (sportType * 4) + fileType
```

Reverse decoding:

```
dataType  = (byte & 0x80) >> 7    // bit 7
sportType = (byte & 0x7C) >> 2    // bits 2–6
fileType  = byte & 0x03           // bits 0–1
```

### File Types

| fileType | Description | Parser / Handler |
|---|---|---|
| 0 | Sport Record (per-second HR, speed, etc.) | `SportRecordBaseParser` subclass via `getSportRecordParserInstance()` |
| 1 | Sport Report (summary) | `SportReportBaseParser` subclass via `getSportReportParserInstance()` |
| 2 | GPS Track | `SportGpsParser` |
| 3 | Recovery Rate | `RecoverRateRecordParser` |

---

## sport_type vs proto_type

The `SportBasicReport` JSON response contains both `sport_type` and `proto_type`. These are **different values** serving different purposes:

| Field | Role | Example |
|---|---|---|
| `sport_type` | Human-readable category identifier | 22 = strength_training |
| `proto_type` | Binary protocol type; used for FDS data identification and parser dispatch | 28 = strength training protocol |

### CRITICAL: sportType in FitnessDataId comes from proto_type

From `FitnessFDSDataDaoUtils.recordSportReportId()`:

```java
FitnessDataId fitnessDataIdBuild = new FitnessDataId.Builder()
    .timeStampInSec(sportBasicReport.getTimeStamp())
    .timeZoneIn15Min(sportBasicReport.getTzIn15Min())
    .sportType(sportBasicReport.getProtoType())  // <-- proto_type, NOT sport_type
    .fileType(1)
    .version(sportBasicReport.getVersion())
    .build();
```

The `sportType` field in `FitnessDataId` is set from `SportBasicReport.getProtoType()`, not `getSportType()`. Confusing the two produces wrong FDS suffixes.

### CRITICAL: timestamp comes from report `time`, not record `time`

`SportBasicReport.getTimeStamp()` has annotation:

```java
@SerializedName(alternate = {"timestamp"}, value = "time")
private long timeStamp;
```

This maps to the JSON field `"time"` (or alternate `"timestamp"`) **inside the report payload** (`SportBasicReport`), NOT the record envelope's `time` field. The `get_sport_records_by_time` API response has two distinct `time` values:

| Source | JSON Path | Description |
|---|---|---|
| Record envelope | `sport_records[].time` | Record-level timestamp (may differ from report timestamp) |
| Report payload | `sport_records[].value` → parsed → `time` | `SportBasicReport.timeStamp` — the value used for FDS suffix |

The bytes 0–3 of the 6-byte FDS key encode the **report-level** `time`. Using the record-level `time` produces wrong suffixes when the two values diverge.

Similarly, `SportBasicReport.getTzIn15Min()` has annotation:

```java
@SerializedName(alternate = {"time_zone"}, value = "timezone")
private int tzIn15Min;
```

This is the `"timezone"` field inside the report payload, in 15-minute increments.

---

## Sport Record Parser Dispatch

### getSportRecordParserInstance(sportType)

`FitnessDataParser.getSportRecordParserInstance()` dispatches on `dataId.getSportType()` (which is `proto_type`):

| proto_type | Record Parser | Sport |
|---|---|---|
| 1, 2, 4, 5, 15 | `OutdoorSportRecordParser` | Outdoor Run, Track Running, Outdoor Walk, Trail Run, Hiking |
| 3 | `IndoorRunRecordParser` | Indoor Run (Treadmill) |
| 6 | `OutdoorBikingRecordParser` | Outdoor Cycling |
| 7 | `IndoorBikingRecordParser` | Indoor Cycling |
| 8 | `FreeTrainingRecordParser` | Free Training |
| 9, 10 | `SwimmingRecordParser` | Pool / Open Water Swimming |
| 11 | `EllipticalMachineRecordParser` | Elliptical |
| 12 | `YogaRecordParser` | Yoga |
| 13 | `RowingMachineRecordParser` | Rowing Machine |
| 14 | `RopeSkippingRecordParser` | Jump Rope |
| 16 | `HITrainingRecordParser` | HIIT |
| 17 | `TriathlonRecordParser` | Triathlon |
| 18 | `OrdinaryBallRecordParser` | Ball Sports |
| 19 | `BasketballRecordParser` | Basketball |
| 20 | `GolfRecordParser` | Golf |
| 21 | `SkiRecordParser` | Skiing |
| 22 | `OutdoorStepRecordParser` | Outdoor Step Sports |
| 23 | `OutdoorNoStepRecordParser` | Outdoor No-Step Sports |
| 24 | `RockClimbingRecordParser` | Rock Climbing |
| 25 | `DivingRecordParser` | Diving |

All 21 parser classes reside in `com.xiaomi.fit.fitness.parser.sport.record` and extend `SportRecordBaseParser`.

`OutdoorSportRecordParser` also implements `TriathlonSubRecordParser`, allowing it to be reused for triathlon sub-legs.

### Sport Report Parser Note

For sport reports (fileType=1), parser dispatch is separate. Notably, proto_type 15 (Hiking) maps to `HikingReportParser` for reports, while its record parser is the shared `OutdoorSportRecordParser`.

### Data Validity Lengths

`FitnessDataValidity.getSportRecordValidityLen()` returns the expected `dataValid` byte count per sport type and version. Its switch statement covers proto_types 1–25, mirroring the parser dispatch table above.

Of note: proto_types 8 (Free Training), 12 (Yoga), and 16 (HIIT) all share `getFreeTrainingRecordValidityLen()` for their validity length computation.

### proto_type 28 (Strength Training) — Not Dispatched

`getSportRecordParserInstance()` does **not** handle proto_type 28 — it falls through to `default → null`. `FitnessDataValidity.getSportRecordValidityLen()` returns `-1` for sport type 28. This decompiled APK version (v3.52.0i) predates the addition of strength training as a dedicated sport type.

**Observed behavior (not verified from this decompiled source):**
- Binary data for proto_type 28 activities successfully decodes using the `FreeTrainingRecordParser` format
- The FreeTraining IT summary (the `it`-prefixed data structure in `SportRecord`, referenced as `ITSportDetailInfo` in `SportRecordConverter` and `getITSummaryDataType()` in `FreeTrainingRecordParser`; exact expansion not spelled out in the decompiled source) (v5+) includes `gymCourseActionTimes`, `gymCourseActionWeight`, `gymCourseActionId` — fields specifically designed for strength/gym workouts
- Strength training is indoor, no GPS, no step counting — structurally identical to Free Training
- A newer APK version likely adds explicit dispatch for proto_type 28

---

## Binary Record Format

### Parser Version Dispatch

Each `SportRecordBaseParser` subclass dispatches on the `version` field from `FitnessDataId`:

| Version Range | Parsing Mode | Description |
|---|---|---|
| v1–v2 | `parseOneDimenData()` | Older format; fewer metric channels |
| v3+ | `parseFourDimenData()` | Newer format; up to 4 data dimensions per sample |

### Per-Second Data Structure

Parsed output consists of `OneSportRecord` objects (`com.xiaomi.fit.fitness.parser.data`), each representing a per-second sample:

| Field | Description |
|---|---|
| `startTime` | Seconds since activity start |
| `endTime` | Seconds since activity start |
| `hr` | Heart rate (BPM) |
| `calories` | Cumulative calories |
| `distance` | Cumulative distance |
| `speed` | Current speed |
| `pace` | Current pace |
| `steps` | Cumulative step count |
| `cadence` | Step frequency |
| `altitude` / `height` | Device altitude |
| `stress` | Stress level |
| `spo2` | Blood oxygen percentage |

Sport-specific metrics vary by parser class and include stroke rate, swing metrics, diving depth, power, resistance, and others.

### GPS Data

GPS data (fileType=2) is parsed by `SportGpsParser` into `GpsRecord` objects:

| Field | Description |
|---|---|
| `latitude` | GPS latitude |
| `longitude` | GPS longitude |
| `altitude` | Altitude (optional) |
| `speed` | GPS speed |
| `hdop` | Horizontal dilution of precision |
| GPS source | Source indicator |

Relevant data classes:

| Class | Package |
|---|---|
| `OneSportRecord` | `com.xiaomi.fit.fitness.parser.data` |
| `GpsRecord` | `com.xiaomi.fit.fitness.parser.data` |
| `SportRecord` | `com.xiaomi.fit.fitness.parser.data` |
| `FitnessParseResult` | `com.xiaomi.fit.fitness.parser.data` |
| `FitnessRecordKey` | `com.xiaomi.fit.fitness.parser.schema` |
| `FitnessGpsKey` | `com.xiaomi.fit.fitness.parser.schema` |

---

## Local File Naming (BLE Sync)

When the watch sends data over BLE, the app saves it locally via `FitnessFileUtils.getFDSDataFile(context, sid, dataId)`:

```
{filesDir}/fitness/d{sid}/sport/p{proto_type}/{timestamp}_{version}_record
```

Example: `d123456789/sport/p28/1700000002_8_record` — SID `123456789`, proto_type `28`, timestamp `1700000002`, version `8`.

The `dataIdFilePathIgnoreVersion` segment from `FitnessDataId` determines the subdirectory structure based on the data type, while `FitnessFileUtils` resolves the full path under the app's private files directory.

---

## FDS API Endpoint

| Path | Namespace | Purpose |
|---|---|---|
| `healthapp/service/gen_download_url` | `healthapp` | Generates presigned download URLs for binary fitness data files |

The endpoint is called through `FitnessFDSRequest.getFDSDownloadUrl()`. The `healthapp` namespace means the request path signs as `/service/gen_download_url` (stripping the `healthapp/` prefix) per `CloudInterceptor.subpath()` behavior. See [mi-fitness-activity-findings.md](mi-fitness-activity-findings.md) for details on request signing and the `@Secret(pathPrefix)` mechanism.

### Request

`FDSRequestParam` serialized to JSON:

| JSON Field | Java Field | Type | Description |
|---|---|---|---|
| `did` | `sid` | `String` | Activity/device SID |
| `items` | `items` | `List<FDSItem>` | List of file items to request |

Each `FDSItem`:

| JSON Field | Type | Description |
|---|---|---|
| `timestamp` | `long` | Activity timestamp (Unix seconds) |
| `suffix` | `String` | Computed data ID suffix |

### Response

Map keyed by `suffix_timestamp` pairs (underscore separator, per `FDSItem.toServerKey()`). Each value is an `FDSResultValue`:

| JSON Field | Java Field | Type | Nullable | Description |
|---|---|---|---|---|
| `url` | `url` | `String` | No | Presigned download URL |
| `obj_name` | `objectName` | `String` | No | Object storage identifier |
| `obj_key` | `objectKey` | `String` | Yes | Base64URL-encoded AES decryption key |
| `method` | `method` | `String` | No | HTTP method for download |
| `expires_time` | `expireTime` | `long` | No | URL expiration timestamp |

---

## Decompilation Gaps

The following areas are partially or incompletely recovered:

- **`FitnessFDSUploader` internal flow:** Key parts of the upload/download orchestration are in obfuscated classes. The suffix construction algorithm and AES decryption parameters are fully recovered, but intermediate error-handling and retry logic may have additional transformations not visible in the decompiled source.

- **Binary parser coverage (proto_types > 25):** `FitnessDataParser.getSportRecordParserInstance()` and `FitnessDataValidity.getSportRecordValidityLen()` switch statements cover proto_types 1–25 only. Newer sport types (e.g., proto_type 28 for strength training) return `null` / `-1`, indicating the decompiled APK version (v3.52.0i) predates their explicit support.

- **FDS file types beyond 0–3:** The primary sport record binary (fileType 0), sport report (fileType 1), GPS data (fileType 2), and recovery rate (fileType 3) paths are documented. The value 8 has been observed in local file naming (e.g., `1700000002_8_record`) but its purpose as a version byte vs. file type indicator and associated parser dispatch path are not fully recovered.

- **Route FDS path:** `FitnessFDSRequest.getRouteFDSUrl()` and `downloadRoute()` exist for a separate route data download flow, but the full request/response format is not recovered.

- **Sleep source data upload:** `getSleepSrcDataUploadUrl()` uses `GetAlgoFileUrlParam`, a separate request structure from `FDSRequestParam`. The full upload flow is partially obfuscated.

- **ECG filter processing:** `FitnessFDSDataGetter.getEcgRecordData()` applies `EcgFilterAlgo` post-processing after FDS download. The filter algorithm's internals are in native code.

- **`FitnessDataId.dataIdFilePathIgnoreVersion`:** Referenced for local file path resolution but the property's exact derivation from the data ID fields is spread across several obfuscated helper methods.
