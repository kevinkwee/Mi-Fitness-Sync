# Mi Fitness Authentication — Decompiled App Reference

Reference documentation for the authentication architecture of the Mi Fitness Android app, recovered from decompilation of the APK via JADX.

---

## Overview

Mi Fitness does not implement its own credential collection or login UI. It delegates all authentication to the **Xiaomi Passport SDK** (`com.xiaomi.accountsdk`), which handles identity verification against `account.xiaomi.com`. Once Passport returns a session, Mi Fitness exchanges it for a **service token** scoped to the service ID `miothealth`.

---

## Key Classes

| Class | Package | Role |
|---|---|---|
| `LoginComponent` | `com.xiaomi.fitness.login` | App-level init; calls `AccountManager.init("miothealth")`, registers login/logout broadcast receiver |
| `MiAccountInternalManager` | `com.xiaomi.fitness.account.manager` | Wraps `XiaomiAccountManager`; fetches/refreshes service tokens for arbitrary SIDs; uses `SERVICE_ID = "passportapi"` for user profile queries |
| `TokenManagerImpl` | `com.xiaomi.fitness.account.token` | In-memory token cache (`ConcurrentHashMap<String, MiAccessToken>`); dispatches `AccessTokenObserver` callbacks on refresh |
| `VerifyToken` | `com.xiaomi.fitness.account.token` | OkHttp interceptor (priority 50); injects `Cookie: serviceToken=…; cUserId=…` before requests; on HTTP 401, refreshes token and retries up to 3 times (3-minute semaphore timeout) |
| `AccountServiceCookieImpl` | `com.xiaomi.fitness.login.token` | Plants WebView cookies on 8+ domains when tokens change; registers observers for 4 SIDs |
| `SIDs` | `com.xiaomi.fitness.account` | Constants for service IDs |
| `XMPassport` | `com.xiaomi.accountsdk.account` | Core Passport SDK entry point (~2800 lines); implements `loginByPassword`, `loginByPassToken`, `getServiceTokenByStsUrl`, token parsing |
| `PassportLoginRequest.ByPassword` | `com.xiaomi.accountsdk.request` | HTTP request builder for password login; auto-fetches `MetaLoginData` (`_sign`, `qs`, `callback`) if not supplied |
| `URLs` | `com.xiaomi.accountsdk.account` | All Passport endpoint URL constants |
| `Coder` / `CloudCoder` | `com.xiaomi.accountsdk.utils` | Crypto helpers: MD5, SHA1, HMAC-style signing, AES, RC4 |
| `HashedDeviceIdUtil` | `com.xiaomi.accountsdk.hasheddeviceidlib` | Device ID generation and caching |
| `SimpleRequestForAccount` | `com.xiaomi.accountsdk.request` | Base HTTP layer; auto-injects `deviceId`, `fidNonce`, `fidNonceSign` cookies |
| `FidNonce` | `com.xiaomi.accountsdk.utils` | Anti-fraud nonce generation (native signer required) |
| `PasswordEncryptor` | `com.xiaomi.accountsdk.account` | Optional interface for additional password hash encryption (beyond MD5) |
| `LoginAndRegisterController` | `com.xiaomi.passport.ui.utils` | UI controller; bridges `PasswordLoginFragment` → `PhoneLoginController.passwordLogin()` |
| `PhoneLoginController` | `com.xiaomi.passport.uicontroller` | Dispatches password and phone-ticket login calls |
| `PasswordLoginFragment` | `com.xiaomi.passport.ui.page` | Login UI fragment; calls `LoginAndRegisterController.loginIdPassword()` |
| `LoginSetting` | `com.xiaomi.smarthome.setting` | Cookie domain and SID constants for the SmartHome SDK |

---

## Service IDs

Defined in `com.xiaomi.fitness.account.SIDs`:

| Constant | Value | Usage |
|---|---|---|
| `MIOT_HEALTH` | `miothealth` | Production Mi Fitness service scope |
| `MIOT_HEALTH_STAGING` | `miothealth-staging` | Staging variant |
| `MIOT_HEALTH_AT` | `miothealth-onebox` | Onebox/AT variant |
| `PLATO_TO_APP_API` | `miothealth` | Alias used by Plato-to-app API layer |

Additional SIDs observed:

| Source | Constant | Value |
|---|---|---|
| `MiAccountInternalManager` | `SERVICE_ID` | `passportapi` — used for user profile/core info queries |
| `LoginSetting` | `SID_PASSPORT_API` | `passportapi` |
| `LoginSetting` | `SID_XIAOMI_HOME` | `xiaomihome` — `AccountServiceCookieImpl` also registers an observer for this SID |
| `LoginComponent` | `MIOT_SID` | `miothealth` |

`LoginComponent.TOKEN_TIME_OUT` is `86400000` (24 hours), used as an internal token staleness check.

---

## Endpoint URLs

Recovered from `com.xiaomi.accountsdk.account.URLs`. Production values (staging uses `account.preview.n.xiaomi.net` over HTTP):

| Constant | Resolved URL |
|---|---|
| `ACCOUNT_DOMAIN` | `https://account.xiaomi.com` |
| `URL_ACCOUNT_BASE` | `https://account.xiaomi.com/pass` (via `CommonConstants.URL_ONLINE_ACCOUNT_BASE`) |
| `URL_LOGIN` / `URL_LOGIN_HTTPS` | `{URL_ACCOUNT_BASE}/serviceLogin` |
| `URL_LOGIN_AUTH2` / `URL_LOGIN_AUTH2_HTTPS` | `{URL_ACCOUNT_BASE}/serviceLoginAuth2` |
| `URL_LOGIN_AUTH_STEP2` | `{URL_ACCOUNT_BASE}/loginStep2` |
| `URL_REFRESH_PASS_TOKEN` | `{URL_ACCOUNT_BASE}/login/passtoken/refresh` |
| `URL_LOGOUT_SYSTEM_DEVICE` | `{URL_ACCOUNT_BASE}/logoutDeviceWithIdentityAuth` |
| `URL_LOGOUT_LOCAL_ACCOUNT` | `{URL_ACCOUNT_BASE}/logoutApp` |
| `URL_REG_TOKEN` | `{URL_ACCOUNT_BASE}/tokenRegister` |
| `URL_ACOUNT_API_BASE_SECURE` | `https://api.account.xiaomi.com/pass` |
| `URL_ACOUNT_API_BASE_V2_SECURE` | `https://api.account.xiaomi.com/pass/v2` |
| `URL_ACCOUNT_SAFE_API_BASE` | `https://api.account.xiaomi.com/pass/v2/safe` |
| `URL_GET_USER_CORE_INFO` | `{URL_ACCOUNT_SAFE_API_BASE}/user/coreInfo` |
| `URL_SECONDARY_LOGIN` | Observed in Kotlin metadata; exact URL TBD |

Mi Fitness API base (from `ThirdAppService`):

| Annotation | Value |
|---|---|
| `@BaseUrl` | `host = "https://hlth.io.mi.com/", path = "app/v1/"` |
| `@Secret` | `sid = "miothealth"` |
| `APP_HOST` | `https://hlth.io.mi.com/` |

---

## Password Login Flow

The end-to-end password login is fully decompiled. The path through the code is:

```
PasswordLoginFragment.login()
  → LoginAndRegisterController.loginIdPassword(userId, password, countryCode, serviceId, captCode, captIck, callback)
    → PhoneLoginController.passwordLogin(PasswordLoginParams, callback)
      → XMPassport.loginByPassword(PasswordLoginParams)
```

### Step 1 — Fetch Meta Login Data

`XMPassport.getMetaLoginData(userId, serviceId)` calls `loginByPassToken()` with a null pass token, which deliberately fails with `InvalidCredentialException`. The exception carries a `MetaLoginData` object containing:

- `_sign` — CSRF-like signature token
- `qs` — query string parameters
- `callback` — callback URL

If `MetaLoginData` is not pre-supplied to `PassportLoginRequest.ByPassword`, the request builder auto-fetches it via `XMPassport.getMetaLoginData()` before executing the POST.

### Step 2 — Password Hashing

The raw password is hashed before transmission:

```java
String hash = CloudCoder.getMd5DigestUpperCase(password);
// Equivalent to: MD5(password.getBytes()).toHexString().toUpperCase()
```

`Coder.getMd5DigestUpperCase()` computes MD5 over the raw password bytes, converts to lowercase hex, then uppercases.

If a `PasswordEncryptor` is configured (via `XMPassportSettings.getPassWordEncryptor()`), the MD5 hash is further encrypted:

```java
PasswordEncryptor.EncryptedValue encryptedValue = encryptor.getEncryptedValue(md5Hash);
// encryptedValue.encryptedPassword → used as "hash" param
// encryptedValue.encryptedEui → sent as EUI_KEY header
```

If no encryptor is available or it throws `PasswordEncryptorException`, the raw MD5 hash is used directly. The `PasswordEncryptor` interface is abstract — its implementation is not in the decompiled source and may be provided at runtime by the host app or a native library.

### Step 3 — POST to serviceLoginAuth2

`XMPassport.loginByPassword(PasswordLoginParams)` constructs and sends:

**URL:** `URL_LOGIN_AUTH2_HTTPS` → `https://account.xiaomi.com/pass/serviceLoginAuth2`

**POST parameters:**

| Key | Value |
|---|---|
| `hash` | MD5-uppercased (or encrypted) password hash |
| `user` | User ID / email / phone |
| `sid` | Target service ID (e.g. `miothealth`; defaults to `passport` if empty) |
| `captCode` | CAPTCHA code (optional) |
| `cc` | Country code (optional) |
| `_json` | `"true"` — requests JSON response format |
| `_sign` | From `MetaLoginData` (injected by `ByPassword`) |
| `qs` | From `MetaLoginData` |
| `callback` | From `MetaLoginData` |
| Locale params | Injected by `fillCommonParams()` via `XMPassportUtil.getDefaultLocaleParam()` |

**Cookies:**

| Key | Value |
|---|---|
| `deviceId` | Hashed device identifier |
| `ick` | CAPTCHA ICK token (optional) |
| `ticketToken` | Phone ticket token (optional) |

**Headers:**

| Key | Value |
|---|---|
| `vToken` | Verification token (optional) |
| `EUI_KEY` | Encrypted EUI from `PasswordEncryptor` (if present) |

### Step 4 — Response Parsing

`processLoginContent()` strips the `&&&START&&&` safety prefix (`PASSPORT_SAFE_PREFIX`) and parses the JSON body.

**Success path (code 0):**

Calls `parseLoginResult()` which extracts:

| Field | Source | Description |
|---|---|---|
| `passToken` | Response header or JSON (depending on login type) | Passport session token |
| `cUserId` | Response header or JSON | Encrypted/cloud user ID |
| `ssecurity` | JSON field `ssecurity`, or `Extension-Pragma` header | Session security key |
| `psecurity` | JSON field `psecurity`, or `Extension-Pragma` header | Persistent security key |
| `nonce` | JSON field `nonce`, or `Extension-Pragma` header | Numeric nonce for STS signing |
| `location` | JSON field `location` | Auto-login / STS URL |
| `re-pass-token` | Response header | Replacement pass token |
| `pwd` | JSON field `pwd` (1 = has password) | Account has password set |
| `child` | JSON field `child` | Child account flag |
| `haveLocalUpChannel` | Response header | Local upload channel availability |

When `ssecurity` or `psecurity` are missing from the JSON body, the code falls back to the `Extension-Pragma` HTTP response header (parsed as JSON).

**Error paths:**

| Code | Exception | Meaning |
|---|---|---|
| `RESULT_CODE_USERNAME` | `InvalidUserNameException` | Invalid username |
| `RESULT_CODE_APP_NAME_FORBIDDEN` | `PackageNameDeniedException` | App package name blocked |
| `70002` | `InvalidCredentialException` | Wrong password |
| `70016` | `InvalidCredentialException` (with `MetaLoginData` + `captchaUrl`) | Wrong password, CAPTCHA required |
| `RESULT_CODE_VERIFICATION` | `NeedVerificationException` | Step-2 verification required (carries `MetaLoginData` + `step1Token`) |
| `87001` | `NeedCaptchaException` | CAPTCHA required |
| `securityStatus != 0` | `NeedNotificationException` | Notification-based approval required (carries `notificationUrl`) |

### Step 5 — STS Token Exchange

If a service ID is specified (not empty and not `"passport"`), `parseLoginResult()` calls `getServiceTokenByStsUrl()`:

```java
private static AccountInfo getServiceTokenByStsUrl(AccountInfo accountInfo, Long nonce) {
    String clientSign = getClientSign(nonce, accountInfo.security);
    // GET to accountInfo.getAutoLoginUrl() with params:
    //   clientSign=<sign>&_userIdNeedEncrypt=true
}
```

**Client sign computation:**

```java
// In getClientSign():
TreeMap map = new TreeMap();
map.put("nonce", String.valueOf(nonce));
return CloudCoder.generateSignature(null, null, map, security);
```

`Coder.generateSignature(null, null, {"nonce": "<nonce>"}, "<ssecurity>")` builds the string `"nonce=<nonce>&<ssecurity>"` and computes `Base64(SHA1(string.getBytes("UTF-8")))`. This is a plain SHA-1 hash, not HMAC.

**STS response parsing:**

The response headers are read for:

| Header | Fallback | Description |
|---|---|---|
| `{sid}_serviceToken` | `serviceToken` | The service-scoped authentication token |
| `{sid}_slh` | — | Service login hash |
| `{sid}_ph` | — | Service phone hash |

All response cookies are also collected via `getCookieKeys()` and stored in `stsCookies`.

---

## Token Types

| Token | Source | Description |
|---|---|---|
| `passToken` | Login response (header or JSON) | Passport-level session token; can be refreshed via `URL_REFRESH_PASS_TOKEN` (`/login/passtoken/refresh`) |
| `serviceToken` | STS URL response header | Service-scoped token for API authentication |
| `ssecurity` | Login response JSON or `Extension-Pragma` header | Used for STS client sign computation and RC4 request signing |
| `psecurity` | Login response JSON or `Extension-Pragma` header | Persistent security token |
| `cUserId` | Login response (header or JSON) | Encrypted user ID; sent as cookie with API requests |
| `userId` | Login response or Android account manager | Numeric Xiaomi user ID |

### MiAccessToken Data Class

`com.xiaomi.fitness.account.token.MiAccessToken` (Kotlin `@Parcelize` data class) holds the in-memory token state:

| Field | Type | Description |
|---|---|---|
| `userId` | `String` | Xiaomi user ID |
| `cUserId` | `String` | Encrypted/cloud user ID |
| `serviceToken` | `String` | Active service token |
| `security` | `String` | ssecurity value |
| `timeDiff` | `long` | Server-client time offset |
| `updated` | `boolean` | Recently refreshed flag |
| `expireTime` | `long` | Internal expiry timestamp |

`isLogin()` returns `true` when both `serviceToken` and `userId` are non-null.

---

## Token Management

### TokenManagerImpl

Maintains a `ConcurrentHashMap<String, MiAccessToken>` cache. `getToken(sid, forceRefresh, loginPolicy)`:

1. Checks `IMiAccountManager.getMiAccount()` — if null, may broadcast login intent and wait on a semaphore (policy-dependent).
2. If `!forceRefresh`, returns the cached `MiAccessToken` for the requested SID.
3. If `forceRefresh`, calls `IMiAccountManager.getServiceToken(sid, forceRefresh)` and caches the result.
4. Notifies all registered `AccessTokenObserver` instances on token change.

### MiAccountInternalManager

Wraps the Android `XiaomiAccountManager`:

- `getServiceToken(sid, refreshToken)` — calls `XiaomiAccountManager.getServiceToken()`, then optionally `refreshServiceToken()` if requested.
- `getMiAccountCoreInfoSync(forceRefresh)` — builds `XMPassportInfo("passportapi")` and fetches `XiaomiUserCoreInfo` with `BASE_INFO` + `SETTING_INFO` scopes.
- `systemLogin(callback)` — invokes `XiaomiAccountManager.setup(context, true)`.
- `localLogin(callback)` — removes existing account, then `setup(context, false)`.

### VerifyToken (HTTP Interceptor)

OkHttp interceptor at priority 50. Intercepts all Mi Fitness API requests:

- **Before request:** Looks up `SecretData` for the request URL via `ApiHolder`, resolves the matching `CookieFetcher` by host, fetches a `MiAccessToken`, and injects a `Cookie` header with the fetcher's cookie map.
- **On 401 response:** Acquires a fair semaphore (3-minute timeout), force-refreshes the token, and retries the request. Retries up to 3 times (`RETRY_MAX_NUM = 3`). Also calls `adjustTimeDiff()` to sync client time from the response `Date` header.

---

## Device Identification

### HashedDeviceIdUtil

Located at `com.xiaomi.accountsdk.hasheddeviceidlib.HashedDeviceIdUtil`. Generates a persistent device identifier with the following priority chain (under `CACHED_THEN_RUNTIME_THEN_PSEUDO` policy):

1. **Cached ID** — loaded from `SharedPreferences("deviceId")` key `hashedDeviceId`.
2. **Runtime device ID** — hashed via `DeviceIdHasher.hashDeviceInfo()`.
3. **Unified device ID fetcher** — if configured, queries `IUnifiedDeviceIdFetcher`.
4. **OAID** — `PrivacyDataMaster.get(OAID)` → `"oa_" + MD5(oaid)`.
5. **Android ID** — `PrivacyDataMaster.get(ANDROID_ID)` → `"an_" + MD5(androidId)`.
6. **Pseudo ID** — `"android_" + UUID.randomUUID()`.

All IDs are truncated to 128 characters and persisted.

System Xiaomi account apps use `RUNTIME_DEVICE_ID_ONLY` policy instead.

### Cookies Injected by SimpleRequestForAccount

All Passport SDK HTTP requests (`SimpleRequestForAccount.getAsString`, `postAsString`, etc.) auto-inject:

| Cookie | Source | Notes |
|---|---|---|
| `deviceId` | `HashedDeviceIdUtil` | Always added if not already present |
| `fidNonce` | `FidNonce.Builder` | Only added when `deviceId` is present and nonce not already set |
| `fidNonceSign` | `FidNonce.Builder` | Signature over the nonce payload |
| `userSpaceId` | `UserSpaceIdUtil` | Multi-user space identifier (added if non-empty) |

### XMPassport.addDeviceIdInCookies

The `loginByPassword` path uses a separate cookie injection method that adds:

| Cookie | Source |
|---|---|
| `deviceId` | `HashedDeviceIdUtil` or caller-provided value |
| `pass_o` | Raw OAID from `PrivacyDataMaster.get(OAID)` |
| `userSpaceId` | `UserSpaceIdUtil.getNullableUserSpaceIdCookie()` |

### FidNonce Structure

`FidNonce` generates an anti-fraud nonce pair:

- **plain** = `Base64(JSON)` where JSON is `{"tp": "<type>", "nonce": "<generated>", "v": "<version>"}`
  - `tp` = `"n"` for `NATIVE`, `"wb"` for `WEB_VIEW`
  - `nonce` = `NonceCoder.generateNonce(serverTime)` — 12-byte random + time-slot value, Base64-encoded
  - `v` = SDK `BuildConfig.VERSION_NAME`
- **sign** = `Base64(FidSigner.sign(JSON_bytes))`

The `FidSigner` is provided by `FidSigningUtil.getFidSigner()` and relies on a platform-native signing implementation. If the signer is unavailable or `canSign()` returns false, `FidNonce.build()` returns `null` and no nonce cookies are sent.

---

## Cookie Planting

`AccountServiceCookieImpl` registers `AccessTokenObserver` instances for four SIDs: `miothealth`, `miothealth-staging`, `miothealth-onebox`, and `xiaomihome`. On token change, it plants WebView cookies across multiple domains.

### plantHealthCookie(sid, accessToken)

**Group 1 — IoT/Account domains** (SID-prefixed service token):

| Domain | Cookies |
|---|---|
| `.watch.iot.mi.com` | `{sid}_serviceToken=<token>; cUserId=<id>; locale=<locale>` |
| `.st-watch.iot.mi.com` | (same) |
| `.dev.fe.home.mi.com` | (same) |
| `.st.iot.home.mi.com` | (same) |
| `.account.xiaomi.com` | (same) |

**Group 2 — Health API domains** (bare service token):

| Domain | Cookies |
|---|---|
| `.hlth.io.mi.com` | `serviceToken=<token>; cUserId=<id>; locale=<locale>` |
| `.staging-hlth.io.mi.com` | (same) |

### plantPrivateCookie(sid, accessToken)

| Domain | Cookies |
|---|---|
| `.wear.mi.com.internal.yrn.net` | `serviceToken=<token>; cUserId=<id>; ssecurity=<security>; updateTime=<timestamp>` |

Also dispatches a `USER_ACCOUNT_SERVER_INFO_UPDATE` event to React Native.

### plantMiHomeLoginCookie(sid, accessToken)

| Domain | Cookies |
|---|---|
| `.home.mi.com` | `serviceToken=<token>; cUserId=<id>` |

Only called for SID `xiaomihome`.

---

## End-to-End Authentication Sequence

```
1. App startup
   → LoginComponent.initAccount(app)
   → AccountManager.init("miothealth")
   → Check login state

2. Login required
   → PasswordLoginFragment.login()
   → LoginAndRegisterController.loginIdPassword(userId, password, sid, ...)
   → PhoneLoginController.passwordLogin(params, callback)

3. XMPassport.loginByPassword(PasswordLoginParams)
   a. Hash password: MD5(password).toHexString().toUpperCase()
   b. Optional: PasswordEncryptor.getEncryptedValue(hash)
   c. PassportLoginRequest.ByPassword auto-fetches MetaLoginData if needed
      → GET serviceLogin with null passToken → InvalidCredentialException
      → Extract _sign, qs, callback from exception
   d. POST serviceLoginAuth2 with: hash, user, sid, _json=true, _sign, qs, callback
      Cookies: deviceId, pass_o, userSpaceId, [ick, ticketToken]
   e. Strip "&&&START&&&" prefix from response body
   f. Parse JSON: check code field for errors
   g. Extract: passToken, cUserId, ssecurity, psecurity, nonce, location

4. STS token exchange
   → getServiceTokenByStsUrl(accountInfo, nonce)
   a. Compute clientSign = Base64(SHA1("nonce=<nonce>&<ssecurity>"))
   b. GET <location> with: clientSign, _userIdNeedEncrypt=true
   c. Extract {sid}_serviceToken (or serviceToken) from response headers
   d. Also extract {sid}_slh, {sid}_ph, all cookies

5. Token storage
   → MiAccessToken(userId, cUserId, serviceToken, security) cached in TokenManagerImpl
   → AccessTokenObserver notifications dispatched

6. Cookie injection
   → AccountServiceCookieImpl plants cookies on 8+ domains

7. API requests
   → VerifyToken interceptor injects Cookie header
   → On 401: force-refresh token, retry up to 3 times
```

---

## Decompilation Gaps

The following areas are partially or incompletely decompiled:

- **`loginByPassword(PasswordLoginParams)` control flow:** JADX emits a `Code decompiled incorrectly` warning for a few branch regions, but the method is substantially recoverable — parameter construction, POST execution, and response parsing are all intact and used throughout this document. Only some peripheral error-handling branches may be incomplete.

- **`PasswordEncryptor` implementations:** The `PasswordEncryptor` interface is fully decompiled, but no concrete implementation class is present in the APK. The actual encryption applied to the MD5 hash (if any) depends on runtime configuration via `XMPassportSettings.getPassWordEncryptor()`.

- **`FidSigner` native implementation:** `FidSigningUtil.getFidSigner()` returns a native signer whose implementation is in a native library, not in the DEX. The signing algorithm for `fidNonceSign` cannot be determined from decompilation alone.

- **`DeviceIdHasher.hashDeviceInfo()`:** Referenced by `HashedDeviceIdUtil.getRuntimeDeviceIdHashed()` but the hashing implementation is not fully visible.

- **`NeedNotificationException` flow:** Some accounts trigger a security notification flow (`securityStatus != 0`) that redirects to a `notificationUrl`. The full handling of this flow within the Passport UI is partially decompiled.

- **Step-2 verification:** `NeedVerificationException` carries a `step1Token` and `MetaLoginData` for `loginStep2`, but the complete step-2 flow is spread across multiple classes and not all branches are fully recovered.
