# S3NTINEL

**S3NTINEL** stands for **Structural Streaming Sparse Event Nexus for Telemetry
Inference with Network Envelope Learning**.

S3NTINEL is a Spark-oriented telemetry anomaly-detection and attribution system
with a simulation validation harness. Its operating motivation is fleet-scale
analysis of A-MATS-captured signal feeds from ARINC 664 Part 7 (AFDX) avionics
networks, where a useful result must identify not only an unusual window, but
also its likely system, subsystem, module, parameter, and supporting event
context. The active V2 implementation consumes normalized telemetry; it does not
yet claim a live A-MATS, AFDX, or BLADE integration.

## At A Glance

**What:** a persisted fitting and inference pipeline that turns raw telemetry into
structural models, calibrated anomaly scores, and attribution artifacts.

**Why:** CBM+ needs evidence that helps maintainers and fleet-reliability teams
investigate high-dimensional telemetry, not an opaque collection of point
anomalies. The pipeline recovers phase-aware structural context so users can
reason about where an anomaly belongs and what evidence supports it.

**How:** profile parameters, extract events, adaptively window telemetry, fit a
backbone and relationship graphs, derive a hierarchy and phase model, score
windows, calibrate emissions, and materialize anomaly attribution artifacts.

**Who it is for:** A-MATS and CBM+ maintainers, fleet-reliability analysts, and
sustainment teams who need auditable telemetry evidence. Data, platform, and ML
engineers build, operate, and validate that capability.

For the A-MATS, AFDX, CBM+, and BLADE context, including current integration
boundaries, see [operational context](docs/design/operational_context.md).

## Start Here

1. For a fast engineering overview, read the [10-minute system tour](docs/current/system_tour.md).
2. Create the supported local environment: `conda env create -f environment.spark35.yml`.
3. Run the canonical structural smoke: `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet`.
4. Read the active [architecture contract](docs/current/v2_architecture.md) and
   [pipeline stage index](pipelines/README.md).
5. Use [docs/README.md](docs/README.md) to choose a current contract, reference,
   design rationale, architecture decision, simulator guide, or plan.

The active local baseline is the `sentinel-spark35` conda environment on Python
3.11, Spark 3.5.1, and Delta 3.0.0. For normal local smoke work, use parquet
unless the Spark runtime has Delta JVM jars available.

## Current Workflow

The production path has two grouped runners:

1. Fitting, stages `00` through `60`: `python -m pipelines.97_run_fitting_pipeline`
2. Inference, stages `70` through `95`: `python -m pipelines.98_run_inference_pipeline`

`python -m pipelines.99_run_full_pipeline` runs both under one parent MLflow
run. The simulation-validation-only stage `72_phase_label_centroids.py` runs
between stages `70` and `80` when truth phase labels are available.

For the complete stage-to-artifact mapping, replay behavior, and individual
entrypoints, use [pipelines/README.md](pipelines/README.md). The authoritative
artifact and field vocabulary is the [glossary](docs/reference/glossary.md).

## Architecture And Design

- [10-minute system tour](docs/current/system_tour.md): fast path through architecture, distributed implementation, evidence, and validation.
- [V2 architecture](docs/current/v2_architecture.md): active contracts and artifacts.
- [Architecture Decision Records](docs/decisions/README.md): durable architectural constraints and alternatives considered.
- [Fitting workflow](docs/current/fitting_workflow.md): reusable metadata and structural fitting sequence.
- [Graph and hierarchy design](docs/design/graph_hierarchy_design.md): graph fusion, hierarchy construction, and retained evidence.
- [Anomaly attribution design](docs/design/anomaly_attribution_design.md): score channels, localization, artifacts, and validation.
- [Artifact replay design](docs/design/artifact_replay_design.md): persistence, lineage, and replay invariants.
- [Simulation guidance](docs/simulation/avionics_simulation_guidelines.md): domain constraints for coherent simulation inputs.

## Repository Map

- `conf/`: checked-in runtime defaults.
- `pipelines/`: ordered stage entrypoints and grouped runners.
- `libs/`: reusable Spark-domain libraries and persisted artifact owners.
- `scripts/`: smoke, simulation, validation, and handoff utilities.
- `notebooks/`: exploratory and validation notebooks; see [notebooks/README.md](notebooks/README.md).
- `docs/`: current contracts, reference vocabulary, design rationale, architecture decisions, simulation notes, research notes, and non-authoritative plans.
- `docs/architecture/`: generated C4 and repository-map snapshot; regenerate rather than editing by hand.

## Development Checks

- Markdown contract: `python tools/check_markdown_docs.py`
- Unit and integration tests: `pytest`
- Lint: `ruff check .`
- Format: `ruff format --check .`

The documented package ownership and current entrypoints live next to the code.
Treat `docs/current/`, package READMEs, schemas, and tests as the source of truth
for current behavior; [plans](docs/plans/README.md) describe non-authoritative
next steps only.
