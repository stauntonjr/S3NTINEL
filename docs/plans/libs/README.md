# Library Plans

This subtree mirrors `libs/` and holds non-authoritative plan artifacts by
library ownership.

Use these docs for:
- next-step work by library area
- deferred library-specific design notes
- library-scoped roadmap consolidation

Do not use them as the source of truth for current behavior. For that, prefer:
- package READMEs under `libs/*`
- [docs/current/](/home/jrs/code/S3NTINEL/sentinel/docs/current)
- current code, contracts, schemas, and validation outputs

## Libraries With Active Plan Docs

- [anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md)
  - primary next-step plan for `libs/anomaly`, with shared scoring and graph decision gates
- [phase.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/phase.md)
  - next phase-simulation plan aligned to `libs/phase`
- [simulation.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/simulation.md)
  - medium-term simulation plan, including behavior-family observability work
- [windows.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/windows.md)
  - deferred windows and continuous-representation notes

## Library Map

### `anomaly`

Active plan: [anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md)

### `architecture`

No active dedicated plan artifact.

### `backbone`

No active dedicated plan artifact. Backbone-sensitive changes are currently
tracked through [windows.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/windows.md)
and [anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md).

### `behavior`

Covered by [simulation.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/simulation.md)
because the current behavior-family observability work is simulator-driven.

### `common`

No active dedicated plan artifact.

### `config`

No active dedicated plan artifact.

### `conformal`

No active dedicated plan artifact. Calibration-related changes are currently
tracked under [anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md).

### `events`

No active dedicated plan artifact. Event work is currently folded into the
simulation and anomaly plans.

### `graph`

Shared plan coverage in [anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md)
for hierarchy decision gates and anomaly-localization dependencies.

### `io`

No active dedicated plan artifact.

### `perf`

No active dedicated plan artifact. Current performance work is tracked under
[simulation.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/simulation.md).

### `phase`

Active plan: [phase.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/phase.md)

### `plotting`

No active dedicated plan artifact.

### `profiling`

No active dedicated plan artifact. Profiling-sensitive simulation changes are
currently tracked under [simulation.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/simulation.md).

### `pyspark`

No active dedicated plan artifact.

### `reporting`

No active dedicated plan artifact.

### `scoring`

Shared plan coverage in [anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md)
for score-channel, calibration, and reconstruction-localization next steps.

### `simulation`

Active plan: [simulation.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/simulation.md)

### `spark_sequence`

No active dedicated plan artifact.

### `testing`

No active dedicated plan artifact.

### `tuning`

No active dedicated plan artifact.

### `windows`

Active plan: [windows.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/windows.md)
