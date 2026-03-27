# Docs Style Guide

Instructions for writing and editing findings documents in `docs/`.

## Purpose & Tone

These documents are **static reverse-engineering reference documentation** for the Mi Fitness Android app, recovered from JADX decompilation of the APK. Write as an impersonal technical reference — not a tutorial, not a development log.

## Title & Opening

- Title format: `# Mi Fitness [Topic] — Decompiled App Reference`
- Follow immediately with a single-line description of what the document covers.

## Structure

- Organize by **topic** (Overview, Key Classes, Constants, Flows, etc.) — never chronologically.
- Separate every major section with a horizontal rule (`---`).
- Use `##` for top-level sections, `###` for subsections within a section.
- End with a `## Decompilation Gaps` section listing areas that are partially or incompletely recovered.

## Formatting

- **Tables** for any structured data: classes and their roles, constants, HTTP parameters, cookies, headers, response fields, error codes.
- **Code blocks** for call chains, code snippets, and computed expressions. Use `java` or appropriate language hints.
- **Bold** for emphasis on key terms on first mention (e.g., SDK names, service IDs).
- **Inline code** for class names, method names, field names, string literals, and file paths when referenced in prose.

## Content Rules

### Include

- Classes, methods, constants, and their packages — with enough context to locate them in the decompiled source.
- Complete flows reconstructed from the code (login, token exchange, data sync, etc.).
- HTTP details: URLs, parameters, headers, cookies, response fields.
- Data structures and their fields.
- Every claim must be **verifiable** against the decompiled source in `mifitness_android/app/src/main/`.

### Do NOT Include

- References to our Python implementation, CLI, or any code in `src/`.
- Development journey, bug investigations, or chronological discovery notes.
- TODO items, implementation plans, or task tracking.
- Opinions, speculation, or recommendations for our sync tool.
- Framing gaps as blockers for our project — describe them as **reverse-engineering gaps** only.

## Reference Document

`docs/mi-fitness-auth-findings.md` is the canonical example of this style.
