# Mi Fitness Activity Data — Decompiled App Reference

Reference documentation for the activity listing, detail retrieval, and FDS file download architecture of the Mi Fitness Android app, recovered from decompilation of the APK via JADX.

---

## Overview

Mi Fitness stores workout activities as **sport reports** on the cloud backend at `https://hlth.io.mi.com/`. The Android app retrieves activity summaries via a time-based or watermark-based cloud pull, caches them in a local Room database, and exposes them to the React Native UI through `YRNSportModule`. Richer per-activity detail — including per-second metrics and GPS tracks — is stored as binary blobs on a separate FDS (File Data Service) layer and downloaded on demand.

---

## Key Classes

| Class | Package | Role |
|---|---|---|
| `FitnessApiService` | `com.xiaomi.fit.fitness.persist.server.service` | Retrofit service interface; declares all cloud data endpoints under `@BaseUrl(host = "https://hlth.io.mi.com/", path = "app/v1/")` with `@Secret(pathPrefix = "")` |
| `FitnessDataRequest` | `com.xiaomi.fit.fitness.persist.server` | Kotlin coroutine wrapper; serializes request objects to JSON and calls `FitnessApiService` methods |
| `CloudInterceptor` | `com.xiaomi.fitness.app` | OkHttp interceptor; performs RC4 encryption/signing of request parameters and decryption of responses |
| `RegionUrlSwitcherImpl` | `com.xiaomi.fitness.login.request` | OkHttp interceptor; rewrites request URLs and injects `region_tag` header based on current region |
| `VerifyToken` | `com.xiaomi.fitness.account.token` | OkHttp interceptor (priority 50); injects authentication cookies and handles 401 retry |
| `AccountServiceCookieImpl` | `com.xiaomi.fitness.login.token` | Plants WebView cookies on health API domains when tokens change |
| `FitnessFDSDataGetter` | `com.xiaomi.fit.fitness.impl.internal` | Entry point for downloading binary sport record data via FDS |
| `FitnessDataParser` | `com.xiaomi.fit.fitness.parser` | Dispatches binary sport record bytes to sport-type-specific parsers |
| `SportRecordConverter` | `com.xiaomi.fit.fitness.impl.internal.sport` | Converts parsed binary records into Mi Fitness display format |
| `SportBasicReport` | `com.xiaomi.fit.data.common.data.sport` | Data class for activity summary payload; approximately 200 fields |
| `FitnessDataId` | `com.xiaomi.fit.data.common.data.p101mi` | Data ID construction for FDS suffix generation (`p101mi` is a JADX-renamed package; original package is likely `mi`) |

---

## Cloud Endpoints

### FitnessApiService Annotations

```java
@BaseUrl(host = "https://hlth.io.mi.com/", path = "app/v1/")
@Secret(pathPrefix = "")
public interface FitnessApiService { ... }
```

Constants:

| Constant | Value |
|---|---|
| `HTTP_HOST` | `https://hlth.io.mi.com/` |
| `HTTP_PATH` | `app/v1/` |

### Activity List Endpoints

| Retrofit Method | Path | Request Type | Response Type |
|---|---|---|---|
| `getSportRecordsByTime` | `data/get_sport_records_by_time` | `GetSportReportByTime` | `SportReportResultByTime` |
| `getSportRecordsByWM` | `data/get_sport_records_by_watermark` | `GetSportReportByWM` | `SportReportResultByWM` |

Both methods accept a single `data` query parameter (annotated `@gij("data")`) containing the JSON-serialized request object.

### Fitness Data Endpoints

| Retrofit Method | Path | Request Type | Response Type |
|---|---|---|---|
| `getFitnessDataByTime` | `data/get_fitness_data_by_time` | `GetFitnessDataByTime` | `FitnessDataResultByTime` |
| `getFitnessDataByWM` | `data/get_fitness_data_by_watermark` | `GetFitnessDataByWM` | `FitnessDataResultByWM` |

### FDS Endpoint

| Path | Namespace | Purpose |
|---|---|---|
| `healthapp/service/gen_download_url` | `healthapp` | Generates presigned download URLs for binary fitness data files |

---

## Request Data Structures

### GetSportReportByTime

6-argument primary constructor; also has a 3-argument secondary constructor `(startTime, endTime, nextKey)` that passes `null` for `category`, `Boolean.TRUE` for `reverse`, and `50` for `limit`.

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `category` | `String` | Yes | Activity category filter |
| `startTime` | `Long` | Yes | Start of time range (seconds) |
| `endTime` | `Long` | Yes | End of time range (seconds) |
| `reverse` | `Boolean` | Yes | Reverse sort order |
| `next_key` | `String` | Yes | Pagination cursor |
| `limit` | `Integer` | Yes | Page size |

All fields use `@SerializedName` for JSON mapping. `startTime` and `endTime` resolve through `SleepAnimalShareActivity.PARAM_ANIMAL_START_TIME` / `PARAM_ANIMAL_END_TIME` constants.

### GetSportReportByWM

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `phone_id` | `String` | No | Device identifier from `FitnessDataRequest.getPhoneId()` |
| `watermark` | `long` | No | Sync watermark value |
| `limit` | `Integer` | Yes | Page size |

### GetFitnessDataByTime

5-argument primary constructor; also has a 3-argument secondary constructor `(startTime, endTime, nextKey)`.

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `key` | `String` | Yes | Data type filter (see HuaMi Key Values below) |
| `startTime` | `Long` | Yes | Start of time range (seconds) |
| `endTime` | `Long` | Yes | End of time range (seconds) |
| `reverse` | `Boolean` | Yes | Reverse sort order |
| `next_key` | `String` | Yes | Pagination cursor |

### FDSRequestParam

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `did` | `String` | No | Activity/device SID |
| `items` | `List<FDSItem>` | No | List of file items to request |

### FDSItem

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `timestamp` | `long` | No | Activity timestamp (seconds) |
| `suffix` | `String` | No | Encoded data ID suffix |

---

## Response Data Structures

### SportReportResultByTime

| JSON Field | Java Type | Description |
|---|---|---|
| `sport_records` | `List<SportReportModel>` | Activity summary list |
| `next_key` | `String` (nullable) | Pagination cursor for next page |
| `has_more` | `boolean` | Whether more records exist |

### SportReportModel

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `sid` | `String` | No | Activity identifier |
| `key` | `String` | No | Data category key |
| `time` | `long` | No | Timestamp |
| `category` | `String` | Yes | Activity category |
| `value` | `String` | Yes | JSON-serialized activity summary payload |
| `zone_offset` | `Integer` | Yes | Timezone offset in seconds |
| `zone_name` | `String` | Yes | Timezone name |
| `deleted` | `Boolean` | Yes | Soft-delete flag |

The `value` field contains a JSON-serialized `SportBasicReport` object.

### FitnessDataResultByTime

| JSON Field | Java Type | Description |
|---|---|---|
| `data_list` | `List<FitnessDataModel>` | Fitness data records |
| `next_key` | `String` (nullable) | Pagination cursor |
| `has_more` | `boolean` | Whether more records exist |

### FitnessDataModel

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `sid` | `String` | No | Record identifier |
| `key` | `String` | No | Data type key (matches HuaMi Key Values) |
| `time` | `long` | No | Timestamp |
| `value` | `String` | Yes | JSON payload |
| `zone_name` | `String` | Yes | Timezone name |
| `zone_offset` | `Integer` | Yes | Timezone offset in seconds |
| `deleted` | `Boolean` | Yes | Soft-delete flag |

### FDSResultValue

| JSON Field | Java Type | Nullable | Description |
|---|---|---|---|
| `url` | `String` | No | Presigned download URL |
| `obj_name` | `String` | No | Object storage name |
| `obj_key` | `String` | Yes | AES decryption key |
| `method` | `String` | No | HTTP method for download |
| `expires_time` | `long` | No | URL expiry timestamp |

The FDS response is a map keyed by `suffix_timestamp` pairs (underscore separator, per `FDSItem.toServerKey()`), each value being an `FDSResultValue`.

---

## SportBasicReport Fields

`SportBasicReport` (`com.xiaomi.fit.data.common.data.sport`) is the activity summary payload deserialized from `SportReportModel.value`. It implements `Serializable` and contains approximately 200 fields. Key fields include:

### Core Activity Fields

| JSON Field | Java Type | Description |
|---|---|---|
| `sport_type` | `int` | Sport type integer |
| `proto_type` | `int` | Protocol/parser type |
| `timestamp` | `long` | Activity timestamp (seconds) |
| `timezone` | `int` | Timezone offset in 15-minute increments (e.g., UTC+8 = 32). Java field `tzIn15Min`; also accepts `@SerializedName(alternate = {"time_zone"})` |
| `start_time` | `long` | Start time |
| `end_time` | `long` | End time |
| `duration` | `int` | Duration |
| `valid_duration` | `int` | Active duration excluding pauses |
| `distance` | `int` | Total distance |
| `corrected_distance` | `int` | GPS-corrected distance |
| `calories` | `int` | Active calories |
| `total_cal` | `int` | Total calories |
| `steps` | `int` | Step count |
| `version` | `int` | Data version |

### Heart Rate

| JSON Field | Java Type |
|---|---|
| `avg_hrm` | `Integer` |
| `max_hrm` | `Integer` |
| `min_hrm` | `Integer` |
| `hr_extreme_duration` | `Integer` |
| `hr_anaerobic_duration` | `Integer` |
| `hr_aerobic_duration` | `Integer` |
| `hr_fat_burning_duration` | `Integer` |
| `hr_warm_up_duration` | `Integer` |
| `hr_smooth_relax_duration` | `Integer` |

### Pace and Speed

| JSON Field | Java Type |
|---|---|
| `avg_pace` | `Integer` |
| `max_pace` | `Integer` |
| `min_pace` | `Integer` |
| `avg_speed` | `Float` |
| `max_speed` | `Float` |
| `avg_cadence` | `Integer` |
| `max_cadence` | `Integer` |
| `min_cadence` | `Integer` |
| `avg_stride` | `Integer` |
| `avg_cycle_cadence` | `Integer` |
| `max_cycle_cadence` | `Integer` |

### Altitude and Climbing

| JSON Field | Java Type |
|---|---|
| `avg_height` | `Float` |
| `max_height` | `Float` |
| `min_height` | `Float` |
| `rise_height` | `Float` |
| `fall_height` | `Float` |
| `total_climbing` | `Integer` |
| `rise_climb_duration` | `Integer` |
| `fall_climb_duration` | `Integer` |

### Training Metrics

| JSON Field | Java Type |
|---|---|
| `train_effect` | `Float` |
| `train_effect_level` | `Integer` |
| `anaerobic_train_effect` | `Float` |
| `anaerobic_train_effect_level` | `Integer` |
| `vo2max` | `Integer` |
| `vo2max_level` | `Integer` |
| `training_load` | `Integer` |
| `training_load_level` | `Integer` |
| `recovery_time` | `Integer` |
| `vitality` | `Integer` |
| `energy_consume` | `Integer` |

### SpO2 and Stress

| JSON Field | Java Type |
|---|---|
| `avg_spo2` | `Integer` |
| `max_spo2` | `Integer` |
| `min_spo2` | `Integer` |
| `avg_stress` | `Integer` |
| `max_stress` | `Integer` |
| `min_stress` | `Integer` |

### Swimming

| JSON Field | Java Type |
|---|---|
| `stroke_count` | `Integer` |
| `main_posture` | `Integer` |
| `avg_stroke_freq` | `Integer` |
| `max_stroke_freq` | `Integer` |
| `turn_count` | `Integer` |
| `avg_swolf` | `Integer` |
| `best_swolf` | `Integer` |
| `pool_width` | `Integer` |

### Power (Cycling)

| JSON Field | Java Type |
|---|---|
| `avg_power` | `Integer` |
| `max_power` | `Integer` |
| `power_active_recovery_duration` | `Integer` |
| `power_endurance_duration` | `Integer` |
| `power_rhythm_duration` | `Integer` |
| `power_lactate_threshold_duration` | `Integer` |
| `power_max_vo2_duration` | `Integer` |
| `power_anaerobic_duration` | `Integer` |
| `power_extreme_duration` | `Integer` |

### Gym/Strength Training

| JSON Field | Java Type |
|---|---|
| `total_rest_duration` | `Integer` |
| `group_count` | `Integer` |
| `gym_training_total_times` | `Integer` |
| `gym_train_weight` | `Integer` |
| `gym_train_action_group` | `Integer` |

### Course/Plan Metadata

| JSON Field | Java Type |
|---|---|
| `sport_course_id` | `String` |
| `cloud_course_id` | `Long` |
| `cloud_course_source` | `Integer` |
| `course_name` | `String` |
| `training_plan_id` | `Integer` |
| `training_plan_time` | `Integer` |

### Triathlon (Nested Reports)

| Field | Java Type | Description |
|---|---|---|
| `openSwimmingReport` | `SportBasicReport` | Nested swim leg report |
| `outdoorRidingReport` | `SportBasicReport` | Nested cycling leg report |
| `outdoorRunningReport` | `SportBasicReport` | Nested run leg report |

Triathlon type constants: `TRIATHLON_TYPE_SWIM = 0`, `TRIATHLON_TYPE_CYCLE = 1`, `TRIATHLON_TYPE_RUN = 2`.

---

## HuaMi Key Values

The `key` parameter in `GetFitnessDataByTime` acts as a data type filter. Known key values (recovered from `HuaMiKey` and related enums):

| Key | Description |
|---|---|
| `huami_sport_record` | Detailed workout/sport records |
| `huami_sport_report` | Sport activity summaries |
| `huami_regular_record` | Daily activity records |
| `huami_manual_heartrate_record` | Manual heart rate recordings |
| `huami_all_day_stress_record` | All-day stress data |
| `huami_single_stress_record` | Manual stress recordings |
| `huami_pai_record` | PAI score data |
| `huami_app_estimate_record` | Calorie stage records |
| `huami_manual_measure_record` | Manual SpO2 measurements |
| `huami_atrial_fibrillation_record` | Atrial fibrillation events |
| `huami_training_load_record` | Training load data |
| `huami_vitality_index` | Activity/vitality index |
| `huami_rest_heart_rate` | Resting heart rate |
| `huami_low_spo2` | Low SpO2 alerts |
| `huami_hl_heart_rate` | Abnormal heart rate alerts |

---

## UI Activity Listing Flow

The Android app's React Native bridge exposes activity listing through `YRNSportModule.getSportRecord(category, startSec, endSec, callback)`.

```
YRNSportModule.getSportRecord(category, startSec, endSec, callback)
  → if category == "sport_stats_all":
      FitnessDataGetter.getSportReport(start, end)
    else:
      FitnessDataGetter.getSportReportByCategory(category, start, end)
  → FitnessDataGetterImpl resolves through SportReportDaoUtils
  → Reads from local Room database
```

The UI reads from the local Room database, which is populated by the cloud sync layer described below.

Relevant decompiled classes:

- `com/xiaomi/wearable/yrn/components/sport/YRNSportModule.java`
- `com/xiaomi/fit/fitness/export/api/FitnessDataGetter.java`
- `com/xiaomi/fit/fitness/impl/FitnessDataGetterImpl.java`
- `com/xiaomi/fit/fitness/persist/db/utils/SportReportDaoUtils.java`

---

## Cloud Sync Paths

Mi Fitness uses two distinct cloud sync strategies to populate the local activity database:

### Time-Based Pull

`FitnessDataRequest.getSportReportsByTime(requestParam)` serializes a `GetSportReportByTime` to JSON and calls `FitnessApiService.getSportRecordsByTime(data)`.

The 3-argument secondary constructor `GetSportReportByTime(startTime, endTime, nextKey)` passes `null` for `category`, `Boolean.TRUE` for `reverse`, `50` for `limit`, and serializes all non-null fields via Gson.

### Watermark-Based Pull

`FitnessDataRequest.getSportReportsByWM(watermark)` uses `data/get_sport_records_by_watermark` for incremental sync. The `phone_id` field comes from `FitnessDataRequest.getPhoneId()`.

---

## Request Signing and Encryption

`CloudInterceptor` (`com.xiaomi.fitness.app`) is an OkHttp interceptor that encrypts request parameters and decrypts responses for all `FitnessApiService` calls.

### Signing Flow

1. **Generate nonce:** `r74.m82490e(timeDiff)` generates `_nonce` from 8 random bytes (via `SecureRandom.nextLong()`) plus the current minute bucket `(System.currentTimeMillis() + timeDiff) / 60000`, Base64-encoded. (Class: `defpackage/w64.java`)

2. **Compute signed nonce:** `Base64(SHA-256(Base64Decode(ssecurity) + Base64Decode(_nonce)))`. (Class: `defpackage/o84.java`, using `MessageDigest.getInstance("SHA-256")`)

3. **Compute `rc4_hash__`:** `Base64(SHA-1("METHOD&ENCODED_PATH&key1=value1&...&signedNonce"))`. The digest algorithm is SHA-1 (`MessageDigest.getInstance("SHA1")`). (Class: `defpackage/w64.java`)

4. **Encrypt parameters:** RC4 using `Base64Decode(signedNonce)` as key, with the cipher primed by consuming 1024 zero bytes (`defpackage/o9k.f95364b`). Both the `data` parameter and `rc4_hash__` are encrypted.

5. **Compute `signature`:** Same SHA-1 digest as step 3, but over the encrypted parameter values.

6. **Send query parameters:** `data` (encrypted), `rc4_hash__` (encrypted), `signature`, `_nonce`.

The response body is Base64-encoded RC4 ciphertext, decrypted using the same signed nonce derivation.

### Relevant Decompiled Classes

| Class | Role |
|---|---|
| `com/xiaomi/fitness/app/CloudInterceptor.java` | Interceptor entry point; dispatches to `handleGetParams` / `handlePostParams` |
| `defpackage/r74.java` | Orchestrates signing: calls nonce generation, RC4 encryption, and digest computation |
| `defpackage/w64.java` | Nonce generation (`m91336a`) and SHA-1 digest computation (`m91338c`) |
| `defpackage/o84.java` | Signed nonce: `SHA-256(ssecurity_bytes + nonce_bytes)` |
| `defpackage/o9k.java` | RC4 cipher with 1024-byte zero prime |
| `defpackage/m9k.java` | RC4 state machine |
| `defpackage/c7c.java` | HMAC-SHA256 helper (used by alternate signing path, not the primary RC4 flow) |

### Path Prefix Stripping

`CloudInterceptor.subpath(pathPrefix, path)` computes the signing path from the request URL:

- If `pathPrefix` is empty (`""`), the method strips everything before the first `/` in the path, returning the full URL path. For `FitnessApiService` with `@Secret(pathPrefix = "")`, the signing path is the full encoded path (e.g., `/app/v1/data/get_sport_records_by_time`).

- If `pathPrefix` is non-empty (e.g., `"healthapp/"`), the method finds that prefix in the path and returns everything after it. For services annotated `@Secret(pathPrefix = "healthapp/")`, the signing path strips `healthapp/` — e.g., request path `/healthapp/service/gen_download_url` signs as `/service/gen_download_url`.

Services observed with `@Secret(pathPrefix = "healthapp/")`:

| Service Class | Package |
|---|---|
| `CourseService` | `com.mi.health.course.api` |
| `AccountService` | `com.xiaomi.fitness.account.api` |
| `AccessService` | `com.xiaomi.fitness.access.request` |
| `GlobalConfigService` | `com.xiaomi.fitness.main.config` |
| `FeedbackService` | `com.xiaomi.fitness.feedback.request` |
| `CheckupdateService` | `com.mi.fitness.checkupdate.net` |
| `LpaActivateApiService` | `com.mi.fitness.lpamanagement.request` |

---

## Request Authentication

Authentication is not carried via an `Authorization` header. The `VerifyToken` interceptor injects cookies via `CookieFetcher` instances resolved by host domain.

For the `miothealth` service against `.hlth.io.mi.com`, the effective request cookies are:

| Cookie | Source |
|---|---|
| `serviceToken` | `MiAccessToken.serviceToken` |
| `cUserId` | `MiAccessToken.cUserId` |

Confirmed from `AccountServiceCookieImpl.plantHealthCookie(...)`: for `.hlth.io.mi.com`, Android plants:

| Cookie | Value |
|---|---|
| `serviceToken` | Service token (bare, not SID-prefixed) |
| `cUserId` | Encrypted user ID |
| `locale` | Current locale string |

The `ssecurity` value is never sent as a cookie to health API endpoints — it is only used locally for the encryption/signature computation described above.

---

## Region Switching

`RegionUrlSwitcherImpl` (`com.xiaomi.fitness.login.request`) rewrites request URLs before they reach the network:

```java
private final String getUrlWithRegionIfNeed(String url, String region) {
    if (Intrinsics.areEqual("cn", region)) {
        return url;  // cn → https://hlth.io.mi.com/
    }
    return StringsKt.replace$default(url, "://", "://" + region + ".", ...);
    // non-cn → https://{region}.hlth.io.mi.com/
}
```

| Region | Resulting Host |
|---|---|
| `cn` | `https://hlth.io.mi.com/` |
| `sg` | `https://sg.hlth.io.mi.com/` |
| `de` | `https://de.hlth.io.mi.com/` |
| `us` | `https://us.hlth.io.mi.com/` |
| `i2` | `https://i2.hlth.io.mi.com/` |
| `ru` | `https://ru.hlth.io.mi.com/` |

The interceptor also injects a `region_tag: <region>` request header unconditionally on every processed request (both `cn` and non-`cn` regions). Only the URL rewrite is conditional on non-`cn`.

Requests that must not be rewritten carry a `RegionTag` header (e.g., `RegionTag:ignore`). The interceptor strips this header before sending, leaving the original URL unchanged.

Region state is managed by `RegionManagerImpl` and persisted in `RegionPreference` (`com.xiaomi.fitness.account.region`).

### Region Lookup Endpoints

| Endpoint | Auth | Purpose |
|---|---|---|
| `https://region.hlth.io.mi.com/app/v1/public/user_region_by_ip` | Fixed cookie `auth_key=rwelJuWBFJxmbMKD`; header `RegionTag:ignore` | IP-based region detection (unauthenticated) |
| `https://hlth.io.mi.com/healthapp/region/user_region` | Authenticated (service token cookies) | Account-bound region lookup |

The IP-based endpoint returns a `CountryRegion` object with `region` and `country` fields.

---

## Country-to-Region Mapping

The APK ships a static country-to-region routing table in:

`assets/server_config/servers.json`

Structure: an array of locale blocks, each containing a list of country entries. Each entry maps a `countryCode` (two-letter ISO code, user-facing) to a `machineCode` (backend region code used for host/header routing).

| Property | Description |
|---|---|
| `countryCode` | Two-letter ISO country code (e.g., `ID`, `GB`, `US`) |
| `machineCode` | Backend region code (e.g., `sg`, `de`, `us`) |

The mapping is identical across all 18 locale blocks. The table contains 247 country code entries.

Observed backend region values:

| Region Code | Example Countries |
|---|---|
| `cn` | CN |
| `de` | GB, DE, FR, and most of Europe |
| `i2` | IN |
| `ru` | RU |
| `sg` | ID, SG, AU, JP, and most of Asia-Pacific |
| `us` | US, CA, BR, and the Americas |

---

## Activity Detail Retrieval

Activity detail data is available through two distinct paths:

### Path 1 — Fitness Data by Time

`FitnessApiService.getFitnessDataByTime(data)` at `data/get_fitness_data_by_time` returns `FitnessDataModel` records, each with a `key` field that identifies the data type (see HuaMi Key Values). For workout detail, the key `huami_sport_record` is associated with rich per-activity data in the HM device sync and fitness-data getter paths.

Relevant decompiled classes:

- `com/xiaomi/fit/fitness/persist/server/service/FitnessApiService.java`
- `com/xiaomi/fit/fitness/persist/server/FitnessDataRequest.java`

### Path 2 — FDS Binary Download

The Android app requests per-activity binary files (per-second heart rate, GPS tracks, sport reports) through a separate **FDS** (File Data Service) cloud layer. The entry point is `FitnessFDSDataGetter`, which constructs a `FitnessDataId` from the activity's `proto_type`, timestamp, and timezone, then downloads and decrypts AES-encrypted binary data via `healthapp/service/gen_download_url`. The decrypted bytes are dispatched to sport-type-specific parsers covering proto_types 1–25.

See [mi-fitness-fds-findings.md](mi-fitness-fds-findings.md) for the complete FDS reference: suffix construction, AES decryption parameters, `FitnessDataId` encoding, sport record parser dispatch table, binary record format, and local file naming conventions.

---

## Decompilation Gaps

The following areas are partially or incompletely recovered:

- **`@Secret` annotation defaults:** The `FitnessApiService` shows `@Secret(pathPrefix = "")` but the default values for `encryptResponse`, `filterSignatureKeys`, and `sid` come from the annotation definition class, which is partially obfuscated. The effective defaults (`encryptResponse = true`, `filterSignatureKeys = {"data"}`, `sid = "miothealth"`) are inferred from observed interceptor behavior but not directly confirmed from the annotation source.

- **`YRNSportModule` internals:** The React Native bridge class `YRNSportModule` is present but parts of its inner callback classes are heavily obfuscated, making the full flow from JS bridge call to `FitnessDataGetter` partially reconstructed.

- **FDS binary parsing and sport type coverage:** See [mi-fitness-fds-findings.md](mi-fitness-fds-findings.md) for FDS-specific decompilation gaps including binary parser coverage, file type handling, and FDS upload/download internals.

- **`CloudSyncBaseRequest` sync orchestration:** The class that orchestrates `pullAndSaveSportReportByTime` and `pullAndSaveSportReportByWM` is referenced in metadata but its implementation is partially obfuscated, making the full sync scheduling logic incomplete.
