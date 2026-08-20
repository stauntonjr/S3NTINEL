# ADR-0004: Keep Simulation Truth Outside Production Inference

- Status: Accepted
- Date: 2026-08-19

## Context

The simulator provides privileged truth such as known phase labels, injected anomaly programs, and hierarchy/coupling expectations. Those signals are valuable for validation but would create leakage if production inference depended on them.

## Decision

Simulation truth is validation-only. Production stages consume the same observable telemetry, events, windows, and fitted reference artifacts that would be available for real data. Truth-dependent helpers, reports, and stages are explicitly marked and excluded from production grouped runners.

The stage-72 label-centroid comparison is the canonical example: it runs only when truth phase labels exist and does not become an input to production scoring.

## Consequences

Simulation can provide strong quantitative evaluation without making the deployed path dependent on unavailable labels. Validation code must preserve a visible boundary between observable inputs and truth-only diagnostics, and reports must not accidentally promote truth-derived fields into production contracts.

## Alternatives Considered

- Feed simulator labels directly into phase detection or scoring. Rejected because measured performance would not transfer to unlabeled real operations.
- Avoid simulator truth entirely. Rejected because controlled injections and known structure are essential for testing localization, detection, and model recovery.
- Maintain a separate simulation implementation of the pipeline. Rejected because parity is strongest when validation exercises the production path itself.

## Revisit When

Revisit only if an equivalent signal becomes a legitimate production input with an explicit operational source and contract.
