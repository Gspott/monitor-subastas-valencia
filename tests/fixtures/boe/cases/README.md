# BOE validation cases

This directory contains the BOE parser validation corpus used by the test suite.

Each case directory contains:
- `manifest.json`: minimal metadata about the case
- `expected.json`: the expected parser contract for that case
- `sanitized.html`: sanitized HTML consumed by tests

## Scope

This directory is a validation corpus for parser behavior.

It is not:
- a production scraping dataset
- a raw capture archive
- a source of truth for runtime logic

The BOE parser must remain unaware of:
- fixtures
- manifests
- sanitization
- test metadata

`src/monitor/sources/boe.py` must stay focused on parsing and domain mapping only.

## Legacy cases

`legacy_sanitized` means the fixture is historical sanitized HTML with no traceable raw source available.

For these cases:
- do not invent retroactive raw provenance
- do not fill `origin_url` with guesses
- do not rewrite them just for cleanliness

Only touch a legacy case if the change improves real coverage, safety, or correctness.

## Test contract

Tests consume `sanitized.html` only.

They do not:
- read raw HTML
- download anything from the network
- depend on capture tooling

`expected.json` is a partial match contract, not a full snapshot.

This means:
- it should include only the fields needed to protect the behavior covered by the case
- it should not try to mirror the full parsed object
- fields must not be added “just in case”

If `expected.json` looks like an automatic dump of the whole object, it is wrong.

## Expected data rules

Keep `expected.json` small and intentional.

Rules:
- include only fields that matter for the behavior under test
- in listing cases, expected items must be identifiable by `external_id`
- avoid special operators and avoid turning `expected.json` into a mini-DSL
- if a special assertion is really needed, prefer making it explicit in the Python test
- do not freeze accidental details that are not part of the contract

A case should be easy to read and answer this question:
what concrete parser behavior does this case protect?

If the answer is unclear, the case is probably too broad or unnecessary.

## Adding or reviewing cases

Before adding a case:
- define the exact behavior the case is meant to protect
- confirm that it adds real coverage, not just more HTML
- keep the case minimal and focused

When adding a case:
1. create a new case directory
2. add `manifest.json`
3. add `expected.json`
4. add `sanitized.html`
5. keep the case intent narrow and explicit
6. update tests only if the new behavior needs a new assertion pattern

When reviewing a case:
- check that the case protects a specific behavior
- check that `expected.json` is partial, not exhaustive
- check that no extra fields were added without a concrete reason
- check that the parser is still decoupled from fixtures and sanitization

If a case does not protect a concrete behavior, it probably should not be added.
