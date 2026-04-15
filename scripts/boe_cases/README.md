# BOE case tooling

This directory contains minimal operational tooling for BOE validation cases.

## Scope

This tooling exists to:
- create a new `sanitized_from_raw` case scaffold
- sanitize a manually captured `raw.html`

It does not:
- web capture
- parser logic
- automatic `expected.json` generation

The parser under `src/monitor/sources/boe.py` must remain unaware of this tooling.

## Required Flow

Follow this order exactly:
1. Save `raw.html` under `tests/fixtures/boe/raw/<case_id>/raw.html`
2. Run `create_case.py`
3. Run `sanitize_case.py`
4. Review `sanitized.html` manually
5. Write `expected.json` manually
6. Run tests

Sanitization is not trusted blindly.

Every new case requires manual review of `sanitized.html` before it becomes part of the corpus.

## Fixture Status

`legacy_sanitized`
- historical sanitized fixture with no traceable raw source
- do not modify it unless there is a real coverage, safety, or correctness reason
- do not invent raw provenance retroactively

`sanitized_from_raw`
- sanitized fixture created from a local `raw.html`
- has operational traceability through `raw_file`, `capture_date`, and `sanitizer_version`
- raw is part of corpus governance, not parser test input

Tests for the parser still consume `sanitized.html` only.

## Before Adding A Case

Answer these questions first:
- What concrete parser behavior does this case protect?
- Does it add real coverage, or is it redundant with an existing case?
- Can the same behavior be protected with a smaller case?
- Is there any PII risk in the raw input?
- Is `expected.json` partial and intentional rather than a snapshot?

If the behavior is unclear, the case probably should not be added.

## Expected Data Rules

`expected.json` is a partial contract, not a full export of the parsed object.

Rules:
- include only the fields needed for the behavior the case protects
- do not add fields "just in case"
- in listing cases, expected items must be identified by `external_id`
- avoid turning `expected.json` into a mini-DSL
- if a special assertion is needed, prefer making it explicit in the Python test

If `expected.json` looks like an automatic dump of the whole object, it is wrong.

## Sanitizer Rules

The sanitizer must stay:
- minimal
- deterministic
- explicit
- easy to review

It must not:
- repair HTML
- pretty-print the document
- normalize formatting for aesthetics
- grow without a new unit test covering the new rule

If it is unclear whether something should be sanitized, do not sanitize it.

## What Not To Do

- Do not add cases just because raw HTML is available.
- Do not put full snapshots into `expected.json`.
- Do not touch legacy cases for symmetry or cleanup alone.
- Do not add sanitizer rules without a new unit test.
- Do not use raw HTML in parser tests.
