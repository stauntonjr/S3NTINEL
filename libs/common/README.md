# Common

## Purpose

`libs/common` now contains only narrow shared constants and helpers that are genuinely cross-cutting.

## Contents

- `event_types.py`
  - canonical event type labels
- `parameter_datatypes.py`
  - datatype labels and normalization helpers

## Data / Artifacts

This package does not own persisted table schemas. Those live in `libs/io/schemas/`.

## Notes

- Keep this package small.
- Do not move broad row contracts or domain logic back into `common`.
