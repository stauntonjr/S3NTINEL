# Layering Rules

Examples in this document are illustrative, not exhaustive. Preserve the distinctions even if exact module names change.

## Top-Level Workflow Modules

Files under `/runs/*`, `/pipelines/*`, or equivalent top-level workflow directories are workflow composition modules.

They may:
- assemble top-level configs and contexts
- choose run modes
- compose major domain stages
- call library stage/domain entrypoints
- coordinate end-to-end execution

They should not:
- implement domain-specific Spark transforms
- define stable domain classes
- own stable payload or persisted artifact modeling
- accumulate many local helpers

## Domain Stage Modules

Files like `libs/*/stage.py`, `libs/*/pipeline.py`, or equivalent domain-stage modules are reusable library implementation modules.

They may:
- implement reusable domain-stage orchestration
- execute Spark/DataFrame transformations for that domain
- construct domain models, artifact groups, frames, tables, and similar typed outputs
- interpret domain-specific config/spec objects

They should not:
- own CLI behavior
- become top-level end-to-end workflow coordinators across unrelated domains

## Scripts

Scripts should:
- parse args
- instantiate config/context
- call library code
- print or log top-level status
- exit

Scripts should not:
- define stable dataclasses or payload objects
- own Spark transformation logic
- duplicate library helpers

## Tests

Tests should:
- use the same noun taxonomy as production code
- validate behavior, contracts, schemas, and public surfaces
- call the real implementation path when possible

Tests should not:
- preserve obsolete naming or object boundaries
- recreate production logic in local helpers
