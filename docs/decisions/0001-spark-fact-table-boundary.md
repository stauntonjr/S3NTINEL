# ADR-0001: Keep Growing Fact-Table Computation in Spark

- Status: Accepted
- Date: 2026-08-19

## Context

S3NTINEL processes telemetry and event relations whose row counts grow with fleet, flight, parameter, event, and window volume. Driver-side collection is convenient for numerical routines and orchestration, but using it on growing fact tables creates memory ceilings and destroys horizontal scalability.

## Decision

Growing fact-table transformations remain Spark-native. `toPandas()` is not permitted on growing active-stage fact tables. `collect()` is limited to explicitly bounded reference or control-plane artifacts, and bounded bridge points should fail fast when configured limits are exceeded.

Small metadata, model coefficients, configuration, and already-pruned result sets may cross to Python when their cardinality is bounded by design rather than by the current test fixture.

## Consequences

The system can scale data-plane work independently of driver memory, but some algorithms require Spark-specific implementations rather than simpler local Python. Query plans, shuffles, joins, broadcast assumptions, and materialization behavior become first-class engineering concerns.

The boundary also clarifies code ownership: vanilla Python may express configuration, domain types, orchestration, and bounded numerical solves while Spark owns distributed relational work.

## Alternatives Considered

- Collect intermediate tables into pandas for each modeling stage. Rejected because the architecture would scale only until driver memory became the bottleneck.
- Implement every operation as a Spark UDF. Rejected because native Spark expressions and relational operators provide better optimization and transparency where applicable.
- Move all logic into JVM-native Spark code. Rejected because it would add implementation cost without improving bounded control-plane logic.

## Revisit When

Revisit if the primary execution engine changes, or if a stage can prove by contract that its complete input cardinality is permanently bounded independent of fleet scale.
