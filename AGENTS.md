# AGENTS.md

## Project Goal

`monitor-subastas-valencia` monitors official auction sources in Spain with a practical focus on Valencia/València.

The project is intended to:
- fetch official auction data from approved public sources;
- normalize, deduplicate, score, store, and export results;
- prioritize real estate and other non-vehicle assets in Valencia;
- provide a reliable base for future source adapters and analysis workflows.


## Repository Rules

- Keep the repository simple, modular, and maintainable.
- Prefer explicit and readable logic over clever or opaque abstractions.
- Follow the existing `src/` layout.
- Keep source-specific scraping logic isolated from normalization, deduplication, scoring, storage, and exports.
- Add tests for meaningful behavior changes, especially around parsing, persistence, deduplication, and filtering.
- Do not invent production assumptions that are not backed by fixtures, official docs, or verified behavior.


## Language Conventions

- Code must be written in English.
- Comments inside code must be written in Castilian Spanish.
- User-facing repository documentation may be in English unless a task explicitly requires otherwise.


## Restrictions

- Do not collect, persist, export, or intentionally process personal data.
- Exclude vehicles from the pipeline explicitly.
- Include real estate and other non-vehicle assets only.
- Do not add scraping behavior that extracts personal names, addresses of persons, IDs, phone numbers, emails, or similar sensitive data.
- If a source contains mixed public and personal text, keep only the non-personal subset or discard the field.


## How To Run

Install dependencies:

```bash
pip install -e ".[dev]"
```

Run the monitor:

```bash
python -m monitor.main
```

Run with CSV export:

```bash
python -m monitor.main --export
```


## How To Run Tests

Run the test suite:

```bash
pytest -q
```

If the local virtual environment is used:

```bash
.venv/bin/pytest -q
```


## Definition Of Done

Work is considered done in this repository only when all of the following are true:

- the implementation matches the requested behavior;
- repository rules and restrictions are respected;
- vehicles are excluded explicitly;
- no personal data is introduced into storage or exports;
- code is in English and code comments are in Castilian Spanish;
- tests relevant to the change exist or were updated;
- tests pass locally;
- the main flow still runs without obvious runtime errors;
- any assumptions or limitations are stated clearly, especially for source parsing without real fixtures.


## What Codex Must Not Do

- Do not invent definitive BOE selectors or source structure without real fixtures or verification.
- Do not add or keep logic that stores personal data.
- Do not weaken the vehicle exclusion rule.
- Do not mix English code with Spanish identifiers unless explicitly required by an external API or data source.
- Do not write code comments in English.
- Do not silently change data semantics across `models.py`, `storage.py`, `normalize.py`, `dedupe.py`, `scoring.py`, `exports.py`, and source adapters.
- Do not introduce hidden heuristics when a simple explicit rule is enough.
- Do not overload `main.py` with complex business logic if that logic belongs in a dedicated module.
- Do not remove useful validation or tests without replacing them with equivalent coverage.
- Do not claim a scraper is production-ready if it has not been validated against real source HTML.
