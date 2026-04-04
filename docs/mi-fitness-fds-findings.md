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
| `sport_type` | Human-readable category identifier | 308 = `STRENGTH_TRAINING` |
| `proto_type` | Binary protocol type; used for FDS data identification and parser dispatch | 8 = Free Training |

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

## Sport Type Constants

### Class: `com.xiaomi.fit.data.common.data.annotation.FitnessSportType`

`FitnessSportType` is a Kotlin annotation (`@Retention(SOURCE)`) whose companion object defines **all** `sport_type` integer constants used by the Mi Fitness API and Android app. It also exposes `getSportRes(sportType)` which maps each constant to a localized display name and icon resource.

`SportBasicReport` carries the `sport_type` value in a field serialized as `"sport_type"` in JSON:

```java
@SerializedName("sport_type")
private int sportType;
```

### Core Sport Types (1–21)

Core sport types have named constants in `FitnessSportType` and correspond 1-to-1 with their `proto_type` counterparts for FDS binary formatting. Every core sport type has a dedicated binary parser and data validity configuration.

> **Note:** Throughout the tables below, the **Label** column contains English shorthand inferred from the Java constant names (e.g., `STRENGTH_TRAINING` → "Strength Training"). These are not recovered display strings — the actual localized names are in Android resource XML files behind obfuscated IDs; see [Sport Name Resolution](#sport-name-resolution) and [Decompilation Gaps](#decompilation-gaps).

| sport_type | Constant | Label |
|---|---|---|
| 0 | `NONE` | No sport / unset |
| 1 | `RUNNING_OUTDOOR` | Outdoor Running |
| 2 | `HEALTH_WALKING_OUTDOOR` | Outdoor Walking |
| 3 | `RUNNING_INDOOR` | Indoor Running (Treadmill) |
| 4 | `MOUNTAINEERING` | Mountaineering |
| 5 | `CROSS_COUNTRY` | Trail Running / Cross Country |
| 6 | `BIKING_OUTDOOR` | Outdoor Cycling |
| 7 | `BIKING_INDOOR` | Indoor Cycling |
| 8 | `SPORT_FREE_TRAINING` | Free Training |
| 9 | `SWIMMING_POOL` | Pool Swimming |
| 10 | `SWIMMING_OPEN_WATER` | Open Water Swimming |
| 11 | `ELLIPTICAL_MACHINE` | Elliptical Machine |
| 12 | `YOGA` | Yoga |
| 13 | `ROWING_MACHINE` | Rowing Machine |
| 14 | `ROPE_SKIPPING` | Jump Rope |
| 15 | `HIKING_OUTDOOR` | Hiking |
| 16 | `HIGH_INTERVAL_TRAINING` | HIIT |
| 17 | `TRIATHLON` | Triathlon |
| 19 | `BASKETBALL` | Basketball |
| 20 | `GOLF` | Golf |
| 21 | `SKI` | Skiing (Downhill) |

Values 18, 22–25 exist in the `proto_type` space (Ball Sports, Outdoor Step Sports, Outdoor No-Step Sports, Rock Climbing, Diving) but have **no named constant** in `FitnessSportType`. `FitnessSportTypeUtils.supportSport()` accepts 1–24 and the somatosensory game types 810 and 812.

### Extended Sport Types — Water Sports (100–118)

| sport_type | Constant | Label |
|---|---|---|
| 100 | `SAIL_BOAT` | Sailing |
| 101 | `PADDLE_BOARD` | Paddle Board |
| 102 | `WATER_POLO` | Water Polo |
| 103 | `WATER_SPORTS` | Water Sports (Generic) |
| 104 | `WATER_SKIING` | Water Skiing |
| 105 | `KAYAKING` | Kayaking |
| 106 | `KAYAK_RAFTING` | Kayak Rafting |
| 107 | `BOATING` | Boating |
| 108 | `MOTOR_BOAT` | Motor Boat |
| 109 | `FIN_SWIMMING` | Fin Swimming |
| 110 | `DIVING` | Diving |
| 111 | `ARTISTIC_SWIMMING` | Artistic/Synchronized Swimming |
| 112 | `SNORKELING` | Snorkeling |
| 113 | `KITE_SURFING` | Kite Surfing |
| 114 | `INDOOR_SURFING` | Indoor Surfing |
| 115 | `DRAGON_BOATS` | Dragon Boats |
| 116 | `FREED_DIVING` | Freediving |
| 117 | `RECREATIONAL_SCUBA_DIVING` | Recreational Scuba Diving |
| 118 | `INSTRUMENT_DIVING` | Instrument Diving |

### Extended Sport Types — Adventure & Outdoor (200–207)

| sport_type | Constant | Label |
|---|---|---|
| 200 | `ROCK_CLIMBING` | Rock Climbing (Generic) |
| 201 | `SKATE_BOARD` | Skateboarding |
| 202 | `ROLLER_SKATING` | Roller Skating |
| 203 | `PARKOUR` | Parkour |
| 204 | `BEACH_BUGGY` | Beach Buggy |
| 205 | `PARAGLIDING` | Paragliding |
| 206 | `BIKE_MOTOCROSS` | BMX / Bike Motocross |
| 207 | `HEEL_TO_TOE_WALK` | Nordic / Heel-to-Toe Walking |

### Extended Sport Types — Indoor Fitness (300–334, 399)

| sport_type | Constant | Label |
|---|---|---|
| 300 | `CLIMBING_MACHINE` | Climbing Machine |
| 301 | `CLIMBING_STAIRS` | Stair Climbing |
| 302 | `STEPPER` | Stepper |
| 303 | `CORE_TRAINING` | Core Training |
| 304 | `FLEXIBILITY_TRAINING` | Flexibility Training |
| 305 | `PILATES` | Pilates |
| 306 | `GYMNASTICS` | Gymnastics |
| 307 | `STRETCH` | Stretching |
| 308 | `STRENGTH_TRAINING` | Strength Training |
| 309 | `CROSS_TRAINING` | Cross Training |
| 310 | `AEROBICS` | Aerobics |
| 311 | `PHYSICAL_TRAINING` | Physical Training |
| 312 | `WALL_BALL` | Wall Ball |
| 313 | `DUMBBELL_TRAINING` | Dumbbell Training |
| 314 | `BARBELL_TRAINING` | Barbell Training |
| 315 | `WEIGHT_LIFTING` | Weight Lifting |
| 316 | `DEAD_LIFT` | Deadlift |
| 317 | `BOBBY_JUMP` | Burpee / Bobby Jump |
| 318 | `SIT_UPS` | Sit-Ups |
| 319 | `FUNCTIONAL_TRAINING` | Functional Training |
| 320 | `UPPER_LIMB_TRAINING` | Upper Limb Training |
| 321 | `LOWER_LIMB_TRAINING` | Lower Limb Training |
| 322 | `WAIST_ABDOMEN_TRAINING` | Waist & Abdomen Training |
| 323 | `BACK_TRAINING` | Back Training |
| 324 | `SPINNING` | Spinning |
| 325 | `STROLLER` | Stroller |
| 326 | `STEP_TRAINING` | Step Training |
| 327 | `HIGH_BAR` | High Bar / Pull-Up Bar |
| 328 | `PARALLEL_BARS` | Parallel Bars |
| 329 | `GROUP_GYMNASTICS` | Group Gymnastics |
| 330 | `KICKBOXING` | Kickboxing |
| 331 | `BATTLE_ROPE` | Battle Rope |
| 332 | `MIXED_AEROBIC` | Mixed Aerobic |
| 333 | `WALK_INDOOR` | Indoor Walking |
| 334 | `ABDWHEEL_TRAINING` | Ab Wheel Training |
| 399 | `INDOOR_FIT` | Indoor Fitness (Generic) |

### Extended Sport Types — Dance (400–412, 499)

| sport_type | Constant | Label |
|---|---|---|
| 400 | `SQUARE_DANCE` | Square Dance |
| 401 | `BELLY_DANCE` | Belly Dance |
| 402 | `BALLET` | Ballet |
| 403 | `STREET_DANCE` | Street Dance |
| 404 | `ZUMBA` | Zumba |
| 405 | `NATIONAL_DANCE` | Folk / National Dance |
| 406 | `JAZZ` | Jazz Dance |
| 407 | `LATIN_DANCE` | Latin Dance |
| 408 | `HIP_HOP_DANCE` | Hip-Hop Dance |
| 409 | `POLE_DANCE` | Pole Dance |
| 410 | `BREAK_DANCE` | Break Dance |
| 411 | `BALLROOM_DANCE` | Ballroom Dance |
| 412 | `MODERN_DANCE` | Modern Dance |
| 499 | `DANCE` | Dance (Generic) |

### Extended Sport Types — Martial Arts (500–511)

| sport_type | Constant | Label |
|---|---|---|
| 500 | `BOXING` | Boxing |
| 501 | `WRESTLING` | Wrestling |
| 502 | `MARTIAL_ARTS` | Martial Arts |
| 503 | `TAICHI` | Tai Chi |
| 504 | `MUAY_THAI` | Muay Thai |
| 505 | `JUDO` | Judo |
| 506 | `TAEKWONDO` | Taekwondo |
| 507 | `KARATE` | Karate |
| 508 | `FREE_SPARRING` | Free Sparring |
| 509 | `KENDO` | Kendo |
| 510 | `FENCING` | Fencing |
| 511 | `JUJITSU` | Jujitsu |

### Extended Sport Types — Ball Sports (600–627)

| sport_type | Constant | Label |
|---|---|---|
| 600 | `FOOTBALL` | Football / Soccer |
| 601 | `HUNDRED_BASKETBALL` | Basketball (Alternative) |
| 602 | `VOLLEYBALL` | Volleyball |
| 603 | `BASEBALL` | Baseball |
| 604 | `SOFTBALL` | Softball |
| 605 | `RUGBY` | Rugby |
| 606 | `HOCKEY` | Hockey |
| 607 | `PING_PONG` | Table Tennis |
| 608 | `BADMINTON` | Badminton |
| 609 | `TENNIS` | Tennis |
| 610 | `CRICKET` | Cricket |
| 611 | `HANDBALL` | Handball |
| 612 | `BOWLING` | Bowling |
| 613 | `SQUASH` | Squash |
| 614 | `BILLIARDS` | Billiards |
| 615 | `SHUTTLECOCK` | Shuttlecock |
| 616 | `BEACH_FOOTBALL` | Beach Football |
| 617 | `BEACH_VOLLEYBALL` | Beach Volleyball |
| 618 | `SEPAKTAKRAW` | Sepak Takraw |
| 619 | `HUNDRED_GOLF` | Golf (Alternative) |
| 620 | `TABLE_FOOTBALL` | Table Football / Foosball |
| 621 | `INDOOR_FOOTBALL` | Indoor Football / Futsal |
| 622 | `SAND_BAG` | Punching Bag |
| 623 | `BOCCE_VOLO` | Bocce |
| 624 | `JAI_BALL` | Jai Alai |
| 625 | `DOOR_KICK` | Door Kick |
| 626 | `DODGE_BALL` | Dodgeball |
| 627 | `SHUFFLE_BALL` | Shuffleboard |

### Extended Sport Types — Snow & Ice (700–710)

| sport_type | Constant | Label |
|---|---|---|
| 700 | `OUTDOOR_SKATING` | Outdoor Skating |
| 701 | `CURLING` | Curling |
| 702 | `SNOW_SPORTS` | Snow Sports (Generic) |
| 703 | `SNOW_MOBILE` | Snowmobile |
| 704 | `PUCK` | Ice Hockey / Puck |
| 705 | `SNOW_CAR` | Snow Car |
| 706 | `SLED` | Sled |
| 707 | `INDOOR_SKATING` | Indoor Skating |
| 708 | `SNOW_BOARD` | Snowboarding |
| 709 | `SKIING` | Skiing (Cross-Country / General) |
| 710 | `CROSS_COUNTRY_SKIING` | Cross-Country Skiing |

### Extended Sport Types — Miscellaneous (800–812)

| sport_type | Constant | Label |
|---|---|---|
| 800 | `ARCHERY` | Archery |
| 801 | `DARTS` | Darts |
| 802 | `HORSE_RIDING` | Horse Riding |
| 803 | `TUG_OF_WAR` | Tug of War |
| 804 | `HULA_HOOP` | Hula Hoop |
| 805 | `FLY_KITE` | Kite Flying |
| 806 | `FISHING` | Fishing |
| 807 | `FRISBEE` | Frisbee |
| 808 | `KICK_SHUTTLECOCK` | Kick Shuttlecock |
| 809 | `SWING` | Swing |
| 810 | `SOMATOSENSORY_GAME` | Somatosensory Game |
| 811 | `E_SPORTS` | E-Sports |
| 812 | `NINTENDO_JUSTDANCE` | Nintendo Just Dance |

### Extended Sport Types — Board Games (900–904)

| sport_type | Constant | Label |
|---|---|---|
| 900 | `INTERNATIONAL_CHESS` | Chess |
| 901 | `CHECKERS` | Checkers |
| 902 | `GO` | Go (Weiqi) |
| 903 | `BRIDGE` | Bridge |
| 904 | `BOARD_GAME` | Board Game |

### Extended Sport Types — Climbing (1000–1001)

| sport_type | Constant | Label |
|---|---|---|
| 1000 | `INDOOR_ROCK_CLIMBING` | Indoor Rock Climbing |
| 1001 | `OUTDOOR_ROCK_CLIMBING` | Outdoor Rock Climbing |

### Extended Sport Types — Special (10000+)

| sport_type | Constant | Label |
|---|---|---|
| 10000 | `EQUESTRIAN` | Equestrian |
| 10001 | `ATHLETICS` | Athletics |
| 10002 | `CAR_RACING` | Car Racing |

### sport_type vs proto_type Mapping

The API returns **both** `sport_type` and `proto_type` as independent fields in the `SportBasicReport` JSON. They are not derived from each other in the app; the server provides both values.

For the core sport types (1–21), the `sport_type` and `proto_type` values often coincide:

| sport_type | proto_type | Label |
|---|---|---|
| 1 | 1 | Outdoor Running |
| 2 | 4 | Outdoor Walking |
| 3 | 3 | Indoor Running |
| 4 | 5 | Mountaineering / Trail |
| 5 | 5 | Cross Country / Trail |
| 6 | 6 | Outdoor Cycling |
| 7 | 7 | Indoor Cycling |
| 8 | 8 | Free Training |
| 9 | 9 | Pool Swimming |
| 10 | 10 | Open Water Swimming |
| 11 | 11 | Elliptical |
| 12 | 12 | Yoga |
| 13 | 13 | Rowing Machine |
| 14 | 14 | Jump Rope |
| 15 | 15 | Hiking |
| 16 | 16 | HIIT |
| 17 | 17 | Triathlon |
| 19 | 19 | Basketball |
| 20 | 20 | Golf |
| 21 | 21 | Skiing |

For extended sport types (100+), the server maps them to one of the core `proto_type` values (1–25) for binary parser dispatch. The exact mapping for each extended type is determined server-side and is not present in the decompiled client code.

### FitnessSportTypeUtils

`FitnessSportTypeUtils` (`com.xiaomi.fit.data.common.data.sport.util`) provides utility methods that classify sport types for display logic. Key methods:

| Method | Input | Logic |
|---|---|---|
| `supportSport(sportType)` | `sport_type` | Returns `true` for core types 1–24, plus 810 and 812 |
| `supportGpsSport(sportType)` | `proto_type` | GPS-capable: 1, 2, 4, 5, 6, 10, 15, 17, 21–25 |
| `supportGpsSport(SportBasicReport)` | Both | Checks `sport_type == 1001` (Outdoor Rock Climbing) OR `commonGpsSport(proto_type)` |
| `supportPaceSport(sportType)` | `sport_type` | Pace display: 1, 2, 3, 207 |
| `supportSpeedSport(sportType)` | `sport_type` | Speed display: 4, 5, 6, 15, 100, 105, 107, 115, 201, 202, 206, 333, 700, 708–710, 10000 |
| `supportSensorSport(sportType)` | `sport_type` | External sensor: 1, 2, 3 |
| `supportStepSensorSport(sportType)` | `sport_type` | Step sensor: 1, 2, 3, 8 |
| `isGroupSport(sportType)` | `sport_type` | Group/set display: 13, 14, 16 |
| `supportsDynamicTrace(sportType)` | `sport_type` | Live route trace: 1, 2, 4, 5, 6, 10, 17 |
| `showSpeedAndDistance(sportType)` | `sport_type` | Speed+distance column: same as `supportSpeedSport` |

Note: `supportGpsSport(int)` operates on `proto_type`, while `supportGpsSport(SportBasicReport)` first checks `sport_type` for value 1001 before falling through to `proto_type` check.

### Sport Summary Protocol (sport_summary2.proto)

The binary sport summary exchanged over BLE uses `TypeMessage` to encode the sport type:

```protobuf
message TypeMessage {
  required uint32 type = 1;     /* sport_type base value */
  optional uint32 subtype = 2;  /* 0=invalid, 1=auto-detect, 2=interval,
                                   3=template, 4=course, 5=PHN */
}
```

The `type` field carries the `sport_type` integer. The `subtype` field indicates the workout mode (standard, auto-detected, interval workout, training template, course-guided, or PHN-initiated).

### Sport Name Resolution

`FitnessSportType.Companion.getSportRes(sportType)` returns a `SportRes` object containing the localized display name string resource ID and the sport icon drawable resource ID. The method has a switch statement covering all 150+ sport types. In `ReportAdapter`:

```java
this.nameView.setText(AppUtil.getApp().getResources()
    .getString(SportUtilsExtKt.getSportTypeStr(report.getSportType())));
this.icon.setImageResource(ly5.m68619b(
    SportUtilsExtKt.convertSportType(report.getSportType())));
```

The localized display names are in Android string resources (not in decompiled Java constants), resolved at runtime via `ResourceExtKt.getString()`. The actual text values are language-dependent and stored in `res/values-*/strings.xml`.

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
{filesDir}/fitness/d{sid}/sport/p{tzIn15Min}/{timestamp}_{proto_type}_record
```

Example: `d123456789/sport/p28/1700000000_8_record` — SID `123456789`, timezone `28` (= UTC+7), timestamp `1700000000`, proto_type `8` (Free Training).

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

- **Binary parser coverage (proto_types > 25):** `FitnessDataParser.getSportRecordParserInstance()` and `FitnessDataValidity.getSportRecordValidityLen()` switch statements cover proto_types 1–25 only. Any proto_type above 25 returns `null` / `-1`, indicating the decompiled APK version (v3.52.0i) does not dispatch them to dedicated parsers.

- **FDS file types beyond 0–3:** The primary sport record binary (fileType 0), sport report (fileType 1), GPS data (fileType 2), and recovery rate (fileType 3) paths are documented. The file type enumeration in the decompiled source does not extend beyond 0–3.

- **Route FDS path:** `FitnessFDSRequest.getRouteFDSUrl()` and `downloadRoute()` exist for a separate route data download flow, but the full request/response format is not recovered.

- **Sleep source data upload:** `getSleepSrcDataUploadUrl()` uses `GetAlgoFileUrlParam`, a separate request structure from `FDSRequestParam`. The full upload flow is partially obfuscated.

- **ECG filter processing:** `FitnessFDSDataGetter.getEcgRecordData()` applies `EcgFilterAlgo` post-processing after FDS download. The filter algorithm's internals are in native code.

- **`FitnessDataId.dataIdFilePathIgnoreVersion`:** Produces the local file path from the data ID fields, excluding the version byte. The resolved format is `d{sid}/sport/p{tzIn15Min}/{timestamp}_{proto_type}_record` (see [Local File Naming](#local-file-naming-ble-sync)). The property's exact derivation is spread across several obfuscated helper methods.

- **Extended sport_type → proto_type mapping:** The API returns both `sport_type` and `proto_type` independently. The server-side mapping from extended sport_type values (100+) to their corresponding proto_type is not present in the decompiled client code. Only empirical observation of API responses can establish the full mapping.

- **Sport display name strings:** `FitnessSportType.Companion.getSportRes()` references obfuscated resource IDs (e.g., `t2k.f113702J1`) for display names. The actual localized strings are in Android resource XML files and cannot be directly correlated to specific English names from the decompiled Java alone.
