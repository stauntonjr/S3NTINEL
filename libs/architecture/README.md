# Architecture

## Purpose

`libs/architecture` owns the deterministic architecture extraction and rendering model for repo-level C4 artifacts.

It does not own:
- low-level AST parsing for generic repo maps
- the checked-in conceptual architecture docs

Those remain in [`tools/`](./../../tools) and [`docs/`](./../../docs).

## How To Use

- Use `tools/architecture_workflow.py` as the canonical CLI surface.
- Use the annotation YAML under `docs/architecture/` as the only manual semantic source.
- Treat generated C4 DSL, metrics, and export artifacts as derived outputs.

## Contents

- `annotations.py`
  - annotation config loading
- `extract.py`
  - repo fact extraction, normalization, and LOC rollups
- `render.py`
  - C4 DSL, metrics, and export rendering
- `ai_review.py`
  - optional AI prompt packaging for review workflows

## Notes

- LOC in this package means AST span LOC derived from syntax node line ranges.
- Build checks should surface skew and drift, not fail on architecture thresholds by default.

