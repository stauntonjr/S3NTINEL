# Testing Support

## Purpose

`libs/testing` contains shared test infrastructure only.

It does not contain:
- actual tests
- regression assertions
- production runtime logic

Those belong in [`tests/`](./../tests/README.md).

## How To Use

Use this package for:
- deterministic sample data
- shared assertions
- shared test-evaluation helpers
- dataset seeding for smoke and integration paths

## Contents

- `data.py`
  - deterministic fixture/dataframe builders
- `assertions.py`
  - shared schema and artifact assertions
- `evaluation.py`
  - reusable test/eval helpers
- `seed.py`
  - dataset seeding helpers

## Notes

- Keep this package small and infrastructure-only.
- If a file starts accumulating real test logic, move that logic back under `tests/`.
