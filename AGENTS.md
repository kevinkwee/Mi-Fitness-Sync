# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

Mi Fitness Sync is an unofficial Python CLI (`mi-fitness-sync`) for pulling workouts out of the Xiaomi Mi Fitness cloud and manually syncing the ones that never reached Strava.

- Logs into Xiaomi Passport the way the Mi Fitness Android app does, including captcha, step-2 code, and email-code verification challenges.
- Lists activities and fetches activity detail from the Mi Fitness cloud API, enriched with the binary FDS files (sport records, sport reports, GPS tracks, recovery rate).
- Exports one or many activities to GPX, TCX, or FIT, with optional GPS smoothing.
- Uploads FIT files to Strava using OAuth2, with a duplicate check.

Tech stack: Python 3.12, `requests`, `pycryptodome`, `fit-tool`, setuptools packaging, pytest with pytest-cov. The protocol knowledge comes from a decompiled Mi Fitness APK and is documented in `docs/`.

## Prerequisites

- Python 3.12 or newer (`requires-python = ">=3.12"` in `pyproject.toml`).
- A local virtual environment at `.venv`, with the package installed in editable mode plus the test extras: `python -m pip install -e ".[test]"`. On Windows the interpreter is `.venv\Scripts\python.exe`.
- External services: a Xiaomi account (`account.xiaomi.com`), the Mi Fitness cloud (`hlth.io.mi.com` and its regional hosts), and a Strava API application for the Strava commands. Tests never contact any of them.
- Optional: the decompiled Mi Fitness APK under `mifitness_android/` (gitignored, local only). It is the source of truth for every claim in `docs/`.

## Project Map

| Folder | Purpose | Precedence file |
|--------|---------|-----------------|
| `docs/` | Reverse-engineering reference documents for the Mi Fitness Android app | [`docs/AGENTS.md`](docs/AGENTS.md) |

When working inside a mapped folder, read and follow that folder's `AGENTS.md` in addition to this file. Where they conflict, the folder-level file wins for module-specific rules. This file wins for cross-cutting rules such as runtime version, security, and testing philosophy.

## Core Philosophy

### Trade-off Priority (when conflicts arise)

1. **Correctness**: code does what it should
2. **Simplicity and readability**: code is easy to understand
3. **Testability**: code is easy to test
4. **Performance**: code is fast enough
5. **Abstraction and reuse**: code is DRY

### Ground Rules

- Read and understand existing code before modifying it.
- Treat the project's existing patterns and code as a starting point, not as evidence of correctness. Legacy code can carry bad practices, over-complex code, and code smells. If something looks wrong, speak up and write new code the right way (correct, secure, performant, readable, maintainable) instead of propagating the problem, even if that means departing from the existing code.
- Isolate new code from legacy where possible, and never copy legacy code into new code without reviewing its quality.
- If a requirement is ambiguous, ask before writing code.
- Prefer incremental delivery: core logic first, then edge cases, then refinements.
- Do not overengineer. Build for today's requirements, not hypothetical future ones.
- Do not break foundation code. Changes to widely-depended-on modules ripple into every dependent, so verify dependents before changing a shared field, method, or contract.

## Design Principles

### SOLID

| Principle | Rule | Common Mistake |
|-----------|------|----------------|
| **SRP** (Single Responsibility) | Each class/function/file has ONE reason to change. If you need "and" or "or" to describe it, split it. | Interpreting SRP as "one function per class." SRP means one *axis of change*. |
| **OCP** (Open/Closed) | Add new behavior by writing new code, not modifying existing code. Use polymorphism or strategy patterns where change is expected. | Over-engineering with premature abstractions. Apply OCP where you have *evidence* of changing requirements. |
| **LSP** (Liskov Substitution) | Subclasses must honor the contract of their parent. Prefer composition over inheritance when "is-a" is not strict. | Overriding a method to throw `NotImplementedError` or do nothing. |
| **ISP** (Interface Segregation) | Define small, role-specific interfaces. Clients depend only on methods they use. | Creating one "service" interface stuffed with methods for many unrelated roles. |
| **DIP** (Dependency Inversion) | Depend on abstractions at module boundaries, not concrete implementations. | Confusing DIP with "just use dependency injection." DIP is about inverting the *direction of source-code dependency*. |

### DRY (Don't Repeat Yourself)

- Extract duplicated logic when the copies are the same business rule and the duplication already costs more than a shared implementation would.
- Single source of truth for configuration, constants, and schema definitions.
- **Prefer duplication over wrong abstraction.** Two pieces of code that look similar but serve different business purposes are NOT duplication, because merging them creates accidental coupling.
- "Wrong abstraction" means: premature generalization, unclear purpose, or coupling unrelated concerns.

### KISS (Keep It Simple)

- Choose the simplest implementation that satisfies current requirements.
- Prefer standard library solutions over custom implementations.
- A plain function call beats metaprogramming. A dictionary beats a class when all you need is data grouping.
- Do not add design patterns, abstractions, or frameworks "just in case."

### YAGNI (You Aren't Gonna Need It)

- Implement features only when there is a concrete, current requirement.
- Do not build generic/extensible frameworks before you have at least two concrete use cases.
- Delete speculative code and unused feature flags regularly.
- Three similar lines of code is better than a premature abstraction.

## Code Quality

### Naming

- Use clear, meaningful, intention-revealing names. The name should answer *why* it exists and *what* it does.
- Functions start with a verb that names the action they perform.
- When a framework or convention dictates the name (interface implementations, overrides, event handlers, generated hooks), follow it.
- Classes are nouns: the type is a thing, and its functions are its behavior.
- Booleans read as a yes/no question to the reader. A prefix (`is_`, `has_`, `can_`, `should_`) works, and so does a plain adjective or past participle (`enabled`, `deprecated`) when the declaration or context makes the type clear. Prefer whichever form the surrounding code already uses, so related names stay consistent.
- No abbreviations unless universally understood (`id`, `url`, `api`).
- No generic names: `data`, `result`, `obj`, `thing`, `temp`, `misc`, `utils`.
- The joiners "and" or "or" in a unit's name signal that the unit may hold two responsibilities. Treat them as a reason to re-examine the unit, and usually to split it, not as words that must never appear. When the joined form is genuinely one domain concept, the name is fine.

### Strong Typing

- Use strong typing everywhere. Avoid `any`, `object`, `dynamic`, `Object`.
- Use typed parameters and return types for all public functions.
- Never cast to `any` just to make something compile.

### Immutability

- Default to immutable. Use `const`, `readonly`, `final`, `frozen`, `tuple`, `frozenset`.
- Return new objects from transformation functions instead of mutating inputs.
- Never expose mutable internal collections. Return copies or read-only views.
- Mutable local variables inside a function are fine. The danger is mutable *shared state*.

### Early Returns and Guard Clauses

- Validate preconditions at the top of functions and return/throw early.
- Reduce nesting by inverting conditions and returning early.
- Keep the "happy path" at the lowest indentation level.

### No Magic Values

- Extract repeated numbers and strings to named constants.
- Use descriptive variable names instead of inline literals.

### Comments and Docstrings

- Do not comment obvious code. Prefer self-explanatory code through good naming, and prefer code over comments. A comment that exists only to compensate for a poor name is a failure to express yourself in code.
- Comments explain **WHY**, never **WHAT**. The code already says what it does, so a comment that restates it is noise. Before writing a comment, ask whether removing it leaves the code harder to understand. If not, delete it.
- A docstring is a concise summary of what the unit does or is, not how it is wired. Do not restate mechanics already obvious from the code it documents (signatures, type hints, decorator arguments, configuration keys). Follow the project's docstring style, omit empty sections, and keep a one-liner for obvious helpers.
- Avoid section separator comments (`# ======` banners between functions), since file structure already separates the sections.
- No commented-out code. Use version control.
- No TODO comments without ticket references (the referenced ticket must be findable by a cold reader).
- **Cold-reader oriented.** Every comment and docstring must make sense to a reader with no prior context. Do not reference internal plans, phases, iterations, decision tables, meetings, or documents the reader cannot find. If a design decision needs context, state the constraint itself.
- **No change narration.** Comments and docstrings describe the current code, not how it got there. No "previously", "no longer", "was removed", "was refactored", "moved to", "used to". If a negative statement is genuinely needed for correctness (e.g. a guard that explicitly rejects an input), state the positive contract (what *is* required). Narrate changes in the chat response, not in the code.
- **Describe the unit, not its sibling.** Do not write that code "mirrors", "reflects", "parallels", or "matches" another module or an external contract as a substitute for describing it. That couples two facts and goes stale when the sibling changes. State each fact directly where it lives.
- **No cross-repo file paths** in comments or docstrings, since readers may not have the other repos checked out. State the rule inline instead.
- **No PII-rule restatements.** The rule that PII is never logged lives in [Security](#security) and [Observability](#observability). Do not restate it on every field, method, or module. State what the unit *is*, not what must not be done with it.
- **No unverified performance claims.** Do not write estimated timings ("10-60s", "<100ms") in comments or docstrings unless they were actually measured and the measurement is load-bearing for the design.

### Functions

- Keep functions short with a single level of abstraction.
- One function does one thing. If it does two things, split it.
- Do not use boolean parameters that switch behavior. Split them into two named functions.
- Eliminate dead code and unused imports on every change.

### Performance

- Avoid N+1 queries in loops. Use bulk fetch, batch writes, and the framework's filtering/aggregation primitives instead of per-item queries or count-in-a-loop.

## Architecture

The project is a layered, single-process CLI. `cli/` is the composition root: it parses arguments, prompts the user, and calls the service clients and exporters. Feature packages depend on the shared modules `config`, `exceptions`, `http_retry`, and `paths`, never the other way round. Among the feature packages, imports run in one direction:

- `cli` imports `activity`, `auth`, `export`, and `strava`.
- `export` imports `activity`, `auth`, and `fds`.
- `activity` imports `auth` and `fds`.
- `strava` imports `auth`.
- `auth` and `fds` import only the shared modules.

```
src/mi_fitness_sync/
├── cli/            # argparse entry point, interactive prompts, composition root
├── auth/           # Xiaomi Passport login client, auth state model, and state file store
├── activity/       # Mi Fitness cloud API: signed and encrypted transport, region routing, list and detail normalization
├── fds/            # FDS binary file download, parsing, and on-disk cache
├── export/         # GPX, TCX, and FIT rendering, GPS smoothing, bulk export
├── strava/         # Strava OAuth2, token store, upload client, sport type mapping
├── __main__.py     # entry point for python -m mi_fitness_sync
├── config.py       # User-Agent sent by the Xiaomi Passport and Mi Fitness API clients
├── exceptions.py   # MiFitnessError hierarchy, the errors the CLI reports or resolves
├── http_retry.py   # retryable status codes and backoff delays
└── paths.py        # per-user data directories under ~/.mi_fitness_sync
tests/              # mirrors the package layout, one folder per package
docs/               # decompiled-app reference documents, governed by docs/AGENTS.md
main.py             # runs the CLI from a checkout without installing
```

This structure can be updated as the project evolves. If you add new files or modules, update this diagram and the corresponding sections to reflect the changes.

### Separation of Concerns

- Separate domain, application, and infrastructure concerns.
- Domain/business logic must have zero imports from frameworks, databases, or HTTP layers.
- Keep side effects (I/O, logging, metrics) at the edges. Business logic should be pure.
- Use DTOs or value objects at layer boundaries, and never pass ORM models or HTTP request objects into business logic.

### Layer Rules

| Layer | CAN | CANNOT |
|-------|-----|--------|
| **CLI** (`cli/`) | Parse arguments, prompt the user, call clients and exporters, print results | Contain business logic, protocol or binary parsing, or rendering logic, or call HTTP directly |
| **Orchestrator** (`activity/client.py`, `activity/fds.py`, `export/bulk.py`) | Coordinate clients, parsers, and stores, apply business rules such as filtering and merging | Know about HTTP or transport, such as sessions, requests, and transport errors, or print, prompt, or read CLI arguments |
| **External client** (`auth/client.py`, `activity/transport.py`, `fds/downloader.py`, `strava/client.py`, `strava/auth.py`) | Talk to Xiaomi, Mi Fitness, or Strava over HTTP, map responses to models | Contain business logic, print, or prompt |
| **Store** (`auth/store.py`, `strava/store.py`, `fds/cache.py`) | Read and write local state and cache files | Make business decisions, call external APIs |
| **Parser and renderer** (`activity/crypto.py`, the `parse_*` functions in `fds/`, `export/render.py`, `export/smoothing.py`) | Decrypt, decode, transform, and render bytes and models | Have side effects, do I/O, maintain state |

### Dependency Injection

- Inject dependencies through constructors or method parameters. Make all dependencies explicit.
- Inject I/O boundaries (database, HTTP clients, filesystem, clock) so they are swappable in tests.
- Keep the composition root at the application entry point, separate from business logic.
- Only inject things that have *side effects* or *vary between environments*. Do not inject pure utility functions.

### Dependency Direction

- Base/shared modules are depended upon by feature modules, never the reverse. Foundation code must not import feature code.
- Do not modify vendored, generated, or third-party code, because it is an upstream snapshot. The only exception is a minimal, clearly-marked patch for a confirmed upstream bug.
- `mifitness_android/` holds the decompiled Mi Fitness APK. It is read-only reference material and is never edited.

## File Structure

### Avoid Over-Engineering

- Split when you have clear, reusable responsibilities. Keep together when separation adds complexity without benefit.

### File Naming

- **NEVER** use generic names: `utils`, `helpers`, `misc`, `common`, `shared` as standalone files.
- Name files after what they do, not after a contract nickname or an internal artifact.
- Keep new file names consistent with the project's existing naming scheme.

## Error Handling

- Handle expected errors explicitly. No silent failures.
- Do not use generic exceptions (`Exception`, `Error`, `object`). Use domain-relevant error types.
- Do not swallow exceptions silently. If an upstream or external call fails, log the failure (with approved identifiers, not payloads or secrets) and surface a meaningful error to the caller.
- Return or throw errors with meaningful context (what failed, what input caused it, how to fix it).
- Errors are part of the API contract.
- Validate inputs at system boundaries. Fail fast on invalid data.
- Distinguish between recoverable errors and fatal exceptions.
- Never silently coerce or fix invalid input. Reject it with a clear message.

```python
# BAD
try:
    result = do_something()
except:
    pass

# GOOD
try:
    result = do_something()
except ValidationError as e:
    logger.warning("Validation failed", extra={"error": str(e), "field": e.field})
    raise DomainError(f"Invalid input: {e.field}") from e
```

### Exception Chaining (`raise ... from`)

Always be explicit when re-raising a caught exception. Never rely on implicit chaining (a bare `raise NewError(...)` after an `except ... as e`).

| Form | Use when |
|------|----------|
| `raise NewError(...) from e` | The original exception adds diagnostic value across a layer boundary (e.g. translating an HTTP-client error into a domain error). Preserves `__cause__` and the original traceback. |
| `raise NewError(...) from None` | Converting to a user-facing error (e.g. an HTTP response) where the original cause would leak internal details or add noise. Suppresses the original traceback entirely. |
| `raise NewError(...)` (bare) | Outside an `except` block, a bare raise is fine because there is no caught exception to chain. Inside an `except` block, a bare raise is almost never intended, because implicit chaining surfaces "During handling of the above exception, another exception occurred". Pick `from e` or `from None` deliberately. |

Decision guide (only applies when the `raise` is **inside** an `except ... as e` block):

1. Does the new exception's message already capture everything useful from the cause, and would the cause's traceback leak internals? Use `from None`.
2. Is this an abstraction boundary translating infra errors into domain errors? Use `from e` to preserve the cause, unless the low-level type/traceback exposes internals.
3. Is the new exception a programming/precondition error (e.g. `ValueError` for bad input) where the caught exception is irrelevant noise? Use `from None`.
4. Do not both embed the cause's text in the new message and chain with `from e`, because that prints the cause three times (message, `__cause__` text, traceback). Pick one: chain with `from e` and let the traceback speak for itself, or fold the cause into the message and use `from None`.

## Security

Security is designed in from the start, not added at review time. Each rule applies only where the code touches the concern it names.

### Secure by Design

- Deny by default. Access and network exposure start closed and open only through explicit grants.
- Fail closed. When a security check cannot complete, such as when the auth service is down, a signature cannot be verified, or input cannot be parsed, reject the request instead of letting it through.
- Grant least privilege to users, services, database accounts, tokens, and CI jobs. Give each the narrowest scope and the shortest lifetime that works.
- Layer defenses. Never rely on a single control, such as a client-side check, a firewall, or a network boundary, to stop an attack.
- Keep the attack surface minimal. Do not ship debug endpoints, development-only admin tools, verbose errors, or unused ports and features to production.
- Ship secure defaults. A risky option, such as permissive CORS or debug mode, is off by default and enabled explicitly per environment.
- Threat-model every new trust boundary, such as an endpoint, an integration, a file input, or a privilege level, before building it. Name what an attacker controls, what they could reach, and which control stops them.

### Input Validation and Injection

- Sanitize and validate all user and external inputs at the boundary.
- Never trust data from outside the system boundary.
- Use allowlists, not denylists. Reject by default, accept only known-good patterns.
- Use schema validation libraries, such as Pydantic, zod, or a JSON Schema validator, and do not hand-roll validation for complex structures.
- Never pass untrusted input to a shell, `eval`, or a dynamic import, and never compile it as a template. Start processes with an argument list instead of a shell string.
- Resolve file paths built from input to their canonical form, and reject any path outside the intended root directory.
- Never deserialize untrusted data with a deserializer that can instantiate arbitrary types, such as `pickle`, Java native serialization, .NET `BinaryFormatter`, or an unsafe YAML loader. Use JSON or another schema-bound format.
- Cap request body size, collection and string lengths, and nesting depth, and avoid regexes with catastrophic backtracking on untrusted input, so one request cannot exhaust memory or CPU.

### Secrets Management

- Keep secrets out of code. Load them at runtime from a source such as environment variables or a secret manager.
- No hardcoded API keys, tokens, or passwords.
- Never commit a real secret to version control, whether in code, `.env` files, config, or test fixtures. Keep local secret files in the ignore list, and rotate any secret that was ever committed.
- Scope each credential to the least privilege it needs, and rotate it on a schedule and on suspected exposure.
- Never put secrets in error messages or analytics events.

### Data Protection at Rest

How stored data is protected depends on how the system uses it.

- **Read-back secrets**, such as third-party API keys, refresh tokens from external OAuth providers, and credentials for outbound calls: encrypt them through a vetted library with an authenticated encryption algorithm, such as AES-256-GCM or ChaCha20-Poly1305. Use envelope encryption, where a data key encrypts the value and a key-encryption key held in a KMS, an HSM, or a secret manager encrypts the data key. Never store a plaintext key (a data key or the key-encryption key) in the same database, in code, or in committed config.
- **Sensitive personal data**, such as government ids, payment data, and health data: encrypt it at field level with the same envelope scheme when database or disk encryption alone does not meet the threat model or the applicable regulation.
- Store a key id with each ciphertext, so keys can rotate while older values still decrypt. After rotating the key-encryption key, re-wrap the data keys under it. Re-encrypt the values themselves only when a data key rotates.
- Give backups, exports, and replicas the same protection as the primary store.
- Collect and keep only the sensitive data a feature needs, and delete it when its retention period ends.

### Cryptography

- Use vetted, maintained libraries, such as the platform's standard crypto library or libsodium. Never implement a cryptographic algorithm or protocol yourself.
- Prefer high-level APIs, such as an authenticated `encrypt` call or a password hashing call, over low-level primitives, because they choose safe parameters.
- Never use MD5, DES, 3DES, RC4, or ECB mode for security purposes, and never use SHA-1 for signatures or collision resistance. Hashes for non-security purposes, such as building cache keys or deduplicating data, may use any algorithm.
- Generate tokens, salts, nonces, and keys with a cryptographically secure random generator, such as `secrets` in Python, `crypto.randomBytes` in Node.js, or `RandomNumberGenerator` in .NET. Never use a general-purpose `random` function for them.
- Never reuse a nonce with the same key. Let the library generate nonces when it can.
- Compare secrets, hashes, and signatures with a constant-time function.
- Use current key sizes for new keys: 256-bit symmetric keys, RSA of at least 3072 bits, or elliptic curve keys, such as P-256 or Ed25519 keys.

### Transport Security

- Use TLS 1.2 or newer for all external traffic and for internal traffic that carries credentials or sensitive data.
- Never disable certificate or hostname verification in shipped code. Trust a private CA by configuring it explicitly, not by turning verification off.
- Never put sensitive data or long-lived secrets, such as passwords, API keys, or session tokens, in URLs, because URLs end up in logs, browser history, and referrer headers. A token that must travel in a link, such as a password reset link, an invitation link, or a presigned download link, is single-use or short-lived, and the page that receives it sends `Referrer-Policy: no-referrer`. When a third-party API accepts a key only in the query string, keep those URLs out of logs and error messages.

### Dependencies and Supply Chain

- Pin dependency versions with a lockfile, and commit the lockfile.
- Install packages only from trusted registries, and check the exact package name to avoid typosquatting.
- Before adding a dependency, check its maintenance and license, and prefer the standard library when it covers the need.
- Run the project's vulnerability audit after every dependency change, and do not add or keep a dependency with a known vulnerability that affects the code.
- Never pipe a remote script straight into a shell, in code, docs, or CI. Download it, verify its checksum or signature, then run it.

### Security Tests

- Use fake or anonymized data in tests, never real user data.
- Cover each security control with adversarial tests: unauthenticated and cross-user access, injection payloads, path traversal, oversized input, and tokens that are tampered, expired, or replayed.
- Assert that a rejected request has no side effect, not only that it returns an error.

## Observability

### Logging

- Use structured logging (key-value / JSON), not formatted strings.
- When structured logging is not available (plain-text logs), use the logger's lazy formatting (`logger.info("Order %s created", order.name)`) instead of f-strings or `%` interpolation, so the message is only formatted when actually emitted.
- Log at key decision points and boundaries, not inside tight loops.
- Include: operation name, relevant IDs, outcome (success/failure), duration if relevant.
- Use consistent field names across the entire codebase.

### Log an Exception at Exactly One Layer

A given exception should be logged by exactly **one** handler: the layer that **terminates** its lifecycle (it swallows the exception, converts it to a domain error that callers act on, or translates it into an HTTP response). Every other layer that touches it must either re-raise silently or transform it without logging.

| Situation | Action |
|-----------|--------|
| You **swallow** the exception (catch and continue, return a fallback, return a result object) | **Log it**: no caller will see it, so this is your last chance. |
| You **re-raise** (bare `raise`, or `raise NewError(...) from e`) | **Do not log**: let the next handler own the log. |
| You are at the **top boundary** (HTTP handler, SSE stream, lifespan) and convert to a response | **Log it**: this is the terminal handler. |
| You are an **intermediate layer** translating infra errors into domain errors | **Do not log**: chain with `from e` and let the boundary log. |

- `logger.exception(...)` immediately followed by `raise` is a smell, because it duplicates the traceback at the next handler that also logs. Use it only when you are certain no upstream handler will log (e.g. the top boundary).
- When logging at a re-raise site is unavoidable (e.g. during startup, where the framework will also log), accept the minor duplication. Outside those rare spots, keep the one-log-per-exception rule strict.

### Log Levels

| Level | When to Use |
|-------|-------------|
| **ERROR** | Something is broken and needs human attention |
| **WARN** | Degraded but self-recoverable |
| **INFO** | Significant business events |
| **DEBUG** | Diagnostic detail, off in production |

### PII in Logs (Zero Tolerance)

- **NEVER** log: email addresses, user names, phone numbers, physical addresses, tokens, passwords.
- **Approved identifiers**: only the project's approved identifier set (e.g. `user_id`).
- No `print()` or `console.log()` with user data, since these go to production logs.

## Testing

> **Test code is production code.** It receives the same care, review, and quality standards.

### Core Principles

- Write unit tests for all core logic.
- Write integration tests for all API endpoints and external integrations.
- Distribute tests as a pyramid: many fast unit tests at the base, focused integration tests at service and external boundaries, and only a few end-to-end tests for critical user journeys.
- Follow Arrange-Act-Assert (AAA) structure. ONE act per test, ONE logical assertion per test.
- Tests MUST be independent, deterministic, and not depend on execution order.
- Mock or fake all external dependencies (DB, APIs, filesystem, time, randomness). Never hit real external services in tests.
- Test behavior through public APIs rather than calling private/internal methods directly. Preserve meaningful branch coverage while minimizing coupling to implementation details.
- When a mock must raise, have it raise a concrete type (the domain error the real code would raise), not a bare or generic `Exception`. Likewise, raise-assertions (e.g. `pytest.raises`) must pin the *specific* exception type the production code is expected to raise, so a regression that raises a different (broader) error is caught.
- Use the test commands documented in this file for the touched modules. Do not invent new test commands.

### How to Run Tests

Run from the repo root with the `.venv` environment active, so `python` is `.venv\Scripts\python.exe` on Windows. Test folders mirror the packages, so run the folder of each touched package, then the full suite before finishing.

```bash
# Touched package
python -m pytest tests/auth -q

# Full suite
python -m pytest -q
```

### Test Naming

Every test function MUST start with the project's test prefix (pytest only collects functions named `test_`).

Choose the pattern that matches the test:

| Pattern | When to use | Example |
|---------|-------------|---------|
| `test_[subject]_[aspect]` | **Default.** Name the thing under test, then the scenario or outcome. Most behavior tests. | `test_login_endpoint_invalid_credentials`, `test_provider_matching_case_insensitive` |
| `test_[behavior]` | One descriptive phrase for pure behavior/state checks where splitting into subject + aspect adds no value. | `test_collection_name_generation`, `test_all_expected_routes_exist` |

Rules that apply to all patterns:

- Prefer the split form when there is a clear subject and a distinguishable aspect (an input variant, an error state, a missing field, an empty collection). Use the single-phrase form only when splitting would be artificial.
- Keep names concise and readable, and prefer `_not_found` over `_doesnt_exist`. Do not pad names with `should`/`when` ceremony.
- Do not encode the assertion type or the fixture names in the test name.
- Group tests with a `class TestXxx` whose name matches the unit under test.

### Tests Must Also Challenge the Code, Not Only Confirm It

**Happy path tests are the foundation**, because they validate the code works under normal conditions. Always start with these.

**But happy path tests ALONE are not enough.** You MUST also write adversarial tests that actively try to break the code and find defects:

- Unexpected input types: `None`, `""`, `[]`, `{}`, `0`, `-1`
- Boundary values: max int, max length, exactly at the limit, one past the limit
- Malformed data: missing fields, extra fields, wrong types, invalid formats
- Error states: what happens when dependencies fail?
- What should NOT happen: verify that forbidden states are correctly rejected
- Error messages and types: not just that it fails, but *how* it fails

**Write tests based on REQUIREMENTS/SPEC, not on what the source code currently does.** This is how you catch bugs where the code diverges from expected behavior.

**When a test fails:** first ask if the CODE is wrong, not the test. Do NOT silently change a failing assertion to match the current code without understanding WHY.

### Test File Rules

- Split test files by logical separation, not by an arbitrary line count. One file per unit, module, or service is fine even at 800+ lines, and unrelated behaviors belong in separate files.
- Keep the Arrange part of each test small. When one test's setup grows past ~20 lines, extract it into helpers or factories.

### Coverage

- **Minimum acceptable: 75%.** Below it, the task is not complete.
- Focus on **branch coverage** (both sides of `if/else`, all `catch` blocks), not just line coverage.
- High coverage with no assertions is worthless. Every test MUST have at least one meaningful assertion.
- Coverage must be **run and shown** at the end for ALL created tests.

```bash
# Branch coverage is already enabled in pyproject.toml
python -m pytest tests/auth --cov=mi_fitness_sync.auth --cov-report=term-missing
```

### All Created Tests MUST Pass

- Every test you create or modify MUST pass. Zero failures, zero skips.
- Never disable, skip, or delete a test to hide a failure.
- Never leave a test "to fix later". Fix it now.
- If coverage is below the agreed minimum: write more tests, re-run, repeat until the minimum is met.

### What NOT to Test

- Simple getters, setters, and trivial mappers are not worth testing.
- Implementation details such as method call order or internal state. Test behavior instead.
- Do not inflate coverage with meaningless assertions.

### Anti-Patterns (Forbidden)

| Pattern | Problem |
|---------|---------|
| **The Liar** | Test passes but doesn't verify the behavior it claims to test |
| **The Mirror** | Test reads the source code and asserts exactly what the code does, so it finds zero bugs |
| **The Giant** | 50+ lines of setup, multiple acts, and dozens of assertions. Should be 5+ separate tests |
| **The Mockery** | So many mocks that the test only tests the mock setup |
| **The Inspector** | Coupled to implementation details, breaks on any refactor |
| **The Chain Gang** | Tests depend on execution order or share mutable state |
| **The Flaky** | Sometimes passes, sometimes fails with no code changes |

## Code Review

### Priority (blockers first)

1. **Security & PII**: no PII in logs, and no violation of any [Security](#security) rule
2. **DRY**: no duplicate types, classes, functions, or logic
3. **File Structure**: single responsibility per file, naming respected
4. **Architecture**: single responsibility, proper layer separation
5. **Code Quality**: SOLID, strong typing, error handling
6. **Testing**: both happy path and adversarial tests, coverage met
7. **Observability**: structured logging, no PII

### Review Questions for Tests

1. "Are there BOTH happy path AND adversarial tests?"
2. "Would these tests catch a regression if someone broke the logic?"
3. "Are there edge cases or failure modes that aren't being tested?"
4. "If I remove a line of business logic, will at least one test fail?"

### Legacy Code

- Do NOT prolong bad patterns. Even if the surrounding code is bad, write good code.
- Do NOT copy-paste from legacy code without reviewing quality.
- Isolate new code from legacy where possible.

## Documentation

Update `README.md` when you add new features, endpoints, or significant changes. Update this file (`AGENTS.md`) when you add new modules, classes, or change architecture. Both `README.md` and `AGENTS.md` must be kept in sync with the codebase. When this section holds a `### Docs Setup` subsection, feature and behavior changes go to the docs pages instead, and `README.md` stays a short landing page that links to them.

## Key Concepts

> This section is a reference for AI agents to understand the domain and architecture of the project.
> You should update this section whenever you add or change key concepts, modules, or architecture.
> This section must be kept in sync with the codebase.

### 1. Xiaomi Passport Login

`auth/client.py` replays the Mi Fitness app's Passport login for the `miothealth` service.

- `GET /pass/serviceLogin` returns the `_sign`, `qs`, and `callback` meta values, then `POST /pass/serviceLoginAuth2` sends the MD5 hash of the password.
- Each challenge raises a typed error that `cli/app.py` resolves interactively: `CaptchaRequiredError` (code `87001`), `Step2RequiredError` (code `81003`), and `NotificationRequiredError` (a `notificationUrl` in the response, completed through Xiaomi's email-code identity endpoints). Answering one challenge can raise another, so the CLI resolves them one after another, up to 5 (`MAX_LOGIN_CHALLENGES`).
- `auth/client.py` maps Xiaomi result codes to typed errors, including `VerificationCodeRejectedError` when Xiaomi rejects an emailed code, so `cli/app.py` never branches on raw codes.
- A successful login yields `ssecurity` and `nonce`, then a GET of the STS `location` with `clientSign` (Base64 SHA-1 of `nonce=<nonce>&<ssecurity>`) yields the `serviceToken` cookie. `ssecurity` and `serviceToken` must come from the same login. In the notification branch Xiaomi issues them only after the challenge is completed.

### 2. Auth State

`AuthState` (`auth/state.py`) is persisted by `auth/store.py` as JSON at `~/.mi_fitness_sync/auth/auth.json` (override with `--state-path`), and restricted to owner read and write where the platform supports POSIX modes, which Windows does not. It holds the Passport tokens, `ssecurity`, cookies, and the `device_id` that later logins reuse.

### 3. Mi Fitness Cloud API

`activity/transport.py` signs and encrypts every Mi Fitness data request with `ssecurity` and a fresh nonce through `activity/crypto.py` (SHA-256 signed nonce, RC4 stream cipher, SHA-1 request signature), and decrypts the response the same way. The region comes from an unsigned `user_region_by_ip` lookup, mapped in `activity/region_mapping.py`. Status codes listed in `http_retry.py` are retried with backoff. `activity/client.py` lists and normalizes activities, which are addressed by IDs in the `sid:key:time` format.

### 4. FDS Files

Each activity can carry up to four binary FDS files, fetched through signed URLs from `gen_download_url`: sport record, sport report, GPS track, and recovery rate (`activity/fds.py`). `fds/` decrypts and parses them per sport type and record version, and `FdsCache` caches the downloads under `~/.mi_fitness_sync/cache/fds`. A download URL does not guarantee the object still exists.

### 5. Export and Strava Upload

`export/render.py` renders a normalized `ActivityDetail` (`activity/models.py`) to GPX, TCX, or FIT bytes, with optional smoothing from `export/smoothing.py`, and `export/bulk.py` exports many activities with a manifest. `strava/` runs the OAuth2 flow through a one-shot `localhost` callback server and stores tokens at `~/.mi_fitness_sync/strava/tokens.json`. Before uploading, `cli/app.py` checks Strava for activities within 5 minutes of the start time (skip with `--skip-duplicate-check`), and `strava/client.py` uploads the FIT file and polls until Strava finishes processing it.

### 6. Reverse-Engineering References

`docs/` documents the decompiled Mi Fitness APK, one topic per file. Every claim must be verifiable against `mifitness_android/app/src/main/`, and `docs/AGENTS.md` governs the style and content of those files.

### 7. Sensitive Local Files

The auth state, the Strava tokens, the FDS cache, and any captured HAR file hold live session tokens or personal health data. By default the app writes its own files under `~/.mi_fitness_sync`, outside the repo. Never commit a capture saved inside the repo, such as a HAR file. Per [Secrets Management](#secrets-management), its path goes in `.gitignore` or, for this clone only, in `.git/info/exclude`.

## Rule of Thumb

**When in doubt, choose simplicity. When trade-offs arise, follow the priority order in [Core Philosophy](#core-philosophy).**
**Build for correctness first. Optimize later. Test always.**
