# ADR-0003: Separate Reference Fitting from Target Inference

- Status: Accepted
- Date: 2026-08-19

## Context

An anomaly detector is only meaningful if target observations are judged against a reference learned without contaminating that reference with the target faults being evaluated. Re-fitting structural or phase models on each target run would blur the distinction between nominal behavior and the behavior under investigation.

## Decision

Reference fitting and target inference are separate workflows. Reusable profiling, scaling, backbone, graph, hierarchy, phase-reference, and calibration artifacts are fit on designated reference data and persisted. Target inference applies compatible fixed artifacts without re-fitting them from target observations.

Reference inference may rematerialize target features using the fixed reference scaling profile, then apply the fixed phase reference model and downstream scoring logic.

## Consequences

Evaluation is defensible because target faults cannot silently redefine their own baseline. The system must track artifact compatibility, lineage, model/configuration identity, and fit/apply semantics explicitly. Operational workflows must also decide when a reference is stale enough to warrant an intentional refit.

## Alternatives Considered

- Fit every stage independently on each target flight. Rejected because anomalies could be absorbed into the target-specific baseline.
- Use one immutable global reference forever. Rejected because legitimate fleet, hardware, or configuration drift eventually requires governed refitting.
- Mix reference and target data during fitting. Rejected for the same leakage reason unless the target data is explicitly admitted into a new reference-training cohort.

## Revisit When

Revisit the granularity of reference models when evidence shows that fleet-, tail-, configuration-, or environment-specific references materially improve detection without making governance impractical.
