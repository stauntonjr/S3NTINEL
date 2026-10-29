# S3NTINEL: 10-Minute System Tour

This tour is the fastest path through the repository for an engineer evaluating the system design, distributed implementation, and evidence model.

## 1. Start With the Problem

Read the root [README](../../README.md) for the operational goal and current integration boundary. S3NTINEL turns normalized high-dimensional telemetry into structural models, calibrated anomaly scores, and evidence-backed attribution to system, subsystem, module, parameter, telemetry, and event context.

The system is intentionally conservative about integration claims: the active implementation consumes normalized telemetry and does not claim a live A-MATS, AFDX, or BLADE integration.

## 2. See the Pipeline Contract

Read [V2 architecture](v2_architecture.md), then [pipelines/README.md](../../pipelines/README.md).

The critical architectural split is:

- **Fitting:** profile parameters, infer events, fit window policy, materialize windows/features, fit structural backbone/graphs/hierarchy, and fit reference phase structure.
- **Inference:** apply fixed reference artifacts, score windows, calibrate emissions, and attribute anomalies.
- **Validation:** use simulator truth only for evaluation; truth-dependent stages stay outside production inference.

Major stages persist named artifacts so expensive work is replayable, inspectable, and independently testable.

## 3. Inspect the Distributed Boundary

Read [ADR-0001](../decisions/0001-spark-fact-table-boundary.md), then inspect `libs/graph/lag.py`.

The core rule is simple: growing fact-table work remains Spark-native. Driver-side Python is used for bounded metadata, orchestration, configuration, and explicitly bounded numerical work. `toPandas()` is not permitted on growing active-stage fact tables, and `collect()` is restricted to bounded reference artifacts.

The lag graph implementation is a representative example: candidate generation, temporal restriction, nearest-prior selection, band assignment, support aggregation, and graph collapse remain relational Spark operations.

## 4. Follow the Evidence, Not Just the Score

Read [anomaly attribution design](../design/anomaly_attribution_design.md) and the artifact vocabulary in [the glossary](../reference/glossary.md).

The system does not stop at an opaque anomaly number. It retains bounded parameter-level score evidence and emits separate attribution artifacts for windows, telemetry, events, and parameter-candidate evidence. Graph and hierarchy fitting also retain edge evidence so structural assignments can be audited.

This is the core product idea: an anomaly should be investigable through the representations that caused it to be emitted.

## 5. Check the Validation Harness

Read the validation sections of [pipelines/README.md](../../pipelines/README.md), then inspect `scripts.run_sim_pipeline` and the integration tests under `tests/integration/`.

Simulation supplies known phase programs, behavior families, hierarchy shape, coupling signatures, and injected misbehavior. Those truth signals evaluate the production pipeline rather than replacing it. Objective reports combine model quality, engineering metrics, fit parameters, and simulator context so comparable runs can be ranked without losing provenance.

## 6. Inspect One Representative Integration Test

Open `tests/integration/pipelines/test_graph_pipeline.py`.

It constructs Spark fixtures, runs graph-family construction, produces hierarchy artifacts, verifies retained edge evidence, evaluates lag-band behavior and hierarchy sensitivity, and checks execution diagnostics. The goal is to test the artifact contract and structural behavior, not merely whether functions import.

## 7. Understand Why the Boundaries Exist

Read [Architecture Decision Records](../decisions/README.md). The initial ADR set captures four constraints that govern most of the implementation:

1. growing fact tables stay in Spark;
2. major stages communicate through persisted artifacts;
3. reference fitting is separated from target inference;
4. simulation truth never becomes a production inference dependency.

## Suggested Interview Walkthrough

For a five-minute technical walkthrough, use this sequence:

1. architecture overview and fit/inference split;
2. `libs/graph/lag.py` as the distributed implementation example;
3. anomaly attribution artifacts as the operator-facing evidence contract;
4. simulation-backed integration tests as evidence that the architecture is exercised end to end.

For deeper discussion, pivot into replay/lineage, phase-reference fitting, graph fusion and hierarchy evidence, or the Spark/driver scaling boundary depending on the interviewer.
