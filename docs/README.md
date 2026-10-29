# Documentation

This directory contains cross-package documentation. Start with the root
[README](../README.md) for orientation, then choose the document class that
matches the task. See [documentation conventions](reference/documentation_conventions.md)
for authority and maintenance rules.

## Find The Right Document

- **Get a fast engineering overview:** [10-minute system tour](current/system_tour.md).
- **Run or modify a stage:** [pipelines/README.md](../pipelines/README.md), then the owning package README.
- **Understand the operational motivation:** [A-MATS, AFDX, and CBM+ operational context](design/operational_context.md).
- **Understand the active system contract:** [V2 architecture](current/v2_architecture.md).
- **Understand durable names and schemas:** [glossary](reference/glossary.md) and [IO schemas](../libs/io/schemas/README.md).
- **Understand a durable architectural decision:** [architecture decision records](decisions/README.md).
- **Understand deeper design rationale:** [graph and hierarchy design](design/graph_hierarchy_design.md), [anomaly attribution design](design/anomaly_attribution_design.md), or [artifact replay design](design/artifact_replay_design.md).
- **Work on simulation:** [simulation architecture](simulation/simulation_architecture.md) and [avionics guidance](simulation/avionics_simulation_guidelines.md).
- **Review a proposal:** [plans/README.md](plans/README.md). Plans are non-authoritative.
- **Inspect a generated structural snapshot:** [architecture/README.md](architecture/README.md).

## Directory Roles

- `current/`: active cross-package architecture, workflow, validation, complexity contracts, and system tour.
- `reference/`: stable taxonomy, schema, theory, and documentation conventions.
- `decisions/`: durable Architecture Decision Records (ADRs).
- `design/`: active design rationale, boundaries, and invariants.
- `simulation/`: simulator-specific guidance and diagrams.
- `research/`: exploratory evaluation and research context.
- `plans/`: non-authoritative proposals and sequencing.
- `archive/`: historical source material; not an active implementation contract.
- `architecture/`: generated C4 and repository-map artifacts.

## Notes

Package READMEs, current code, schemas, and tests own current implementation
behavior. `docs/architecture/` is a generated snapshot; regenerate it rather
than manually repairing drift.
