---
name: single-model-path
description: Enforce the repository rule that each production modeling concept has exactly one implementation path, owned by the canonical Spark Table/Frame or stage library surface. Use when removing duplicate local/test modeling paths or when adding model logic that could drift across Spark and local code.
---

# Single Model Path

## Rule

For any production modeling concept in this repository:

- there is one production implementation
- that implementation lives in the canonical Spark `Table` / `Frame` / stage library path
- tests exercise that same implementation

Local pandas or in-memory code is allowed only for:

- reporting
- validation
- evaluation
- plotting
- final test assertion materialization after model outputs already exist

Local pandas or in-memory code must not re-implement production model semantics.

## Required Workflow

1. Before editing, search `libs/`, `pipelines/`, and `tests/` for duplicate semantics.
2. If the same model concept exists in Spark and local form, consolidate first.
3. Remove duplicate local/test production paths instead of preserving parity helpers.
4. Rewrite tests to call the canonical Spark owner or stage path directly.
5. Add or update contract tests so the duplicate path cannot silently return.
6. Update the authoritative README/package docs in the same pass.

## Red Flags

Stop and refactor if you see:

- Spark-vs-local parity tests for production semantics
- `artifacts.py` or `rules.py` modules that rebuild production model outputs locally
- package `__init__` files exporting both Spark and local model builders for the same concept
- tests computing expected production results through a second implementation
- hot-path production modules using `toPandas()` or `to_dict(orient="records")` as a semantic fork

## What Good Looks Like

- Stage entrypoints call the canonical Spark `Table` / `Frame` owner.
- Tests assert on outputs from that owner.
- Validation/reporting may materialize bounded outputs locally, but never compute the model there.
- Package READMEs state the canonical owner clearly.
- Contract tests fail fast when a duplicate path reappears.
