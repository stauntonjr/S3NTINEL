# Tests

## Purpose

`tests/` contains the actual repo test cases.

It owns:
- behavioral assertions
- integration coverage
- regression coverage
- public API and schema contract checks

It does not own:
- shared fixture factories
- reusable dataframe/schema assertions
- test-only evaluation helpers

Those belong in [`libs/testing/`](./../libs/testing/README.md).

## How To Use

Run focused subsets by purpose:
- `pytest tests/unit/phase`
- `pytest tests/contracts`
- `pytest tests/integration/runner`
- `pytest tests/regression`

For project commands, use the active repo environment:
- `conda run -n sentinel-spark35 pytest ...`

## Suite Layout

- `unit/`
  - object- and method-level behavior
- `contracts/`
  - schema, IO, and public API guarantees
- `integration/`
  - multi-library and stage/runner behavior
- `regression/`
  - bugfix and infrastructure regressions

## Model / Concepts

The suite is organized by intent rather than by historical file placement:
- unit tests prove local semantics
- contract tests prove stable interfaces and artifact shapes
- integration tests prove cross-library and pipeline behavior
- regression tests prevent known failure modes from returning

## Data / Artifacts

Shared data builders and assertion helpers live in:
- `libs/testing/data.py`
- `libs/testing/assertions.py`
- `libs/testing/evaluation.py`
- `libs/testing/seed.py`

Tests should use those helpers where they materially reduce repeated inline Spark or dataframe scaffolding.

## Subject Matter View

The tests validate both engineering and domain expectations:
- telemetry and artifact contract correctness
- simulation realism and truth propagation
- structural model fitting
- anomaly scoring and attribution behavior

## Testing / Validation

Keep this boundary strict:
- `libs/testing` = support code only
- `tests/` = actual test assertions

## Notes / Constraints

- Prefer current package nouns in test names and assertions.
- Do not reintroduce deleted historical naming such as `assembly`, `native`, or `window_x` in new tests unless the test is explicitly checking a backward-compatibility surface.
