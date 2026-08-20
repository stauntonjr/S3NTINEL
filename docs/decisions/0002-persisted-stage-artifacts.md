# ADR-0002: Persist Artifacts Between Major Pipeline Stages

- Status: Accepted
- Date: 2026-08-19

## Context

S3NTINEL contains multiple expensive modeling stages with different inputs, validation needs, and replay semantics. Treating the pipeline as one in-memory computation would make failures expensive to recover from, obscure lineage, and couple downstream experimentation to upstream recomputation.

## Decision

Major stages communicate through named persisted artifacts with explicit schemas, manifests, row counts, and lineage. Grouped runners may resume from a prior run only when the first resumed stage declares valid replayable inputs.

Stage entrypoints remain thin orchestration shells; the owning `libs/*` package defines the domain behavior and table abstractions.

## Consequences

Runs are inspectable and replayable, validation can target intermediate representations, and expensive fitting artifacts can be reused. The cost is additional storage, schema governance, migration discipline, and more explicit artifact lifecycle management.

Persisted artifacts are part of the architecture rather than incidental debug outputs. Their names and semantics therefore belong in the glossary and active architecture contract.

## Alternatives Considered

- One monolithic in-memory DAG. Rejected because failure recovery and experiment isolation would be poor.
- Persist only final anomaly outputs. Rejected because model evidence, structural diagnostics, and reference reuse would be lost.
- Let each stage choose ad hoc serialization. Rejected because replay and lineage require a shared artifact contract.

## Revisit When

Revisit individual persistence points when measurement shows that an artifact has no replay, audit, validation, or reuse value and its materialization cost is material.
