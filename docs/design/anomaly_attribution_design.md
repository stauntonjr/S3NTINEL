# Anomaly Attribution Design

This document explains the active scoring-to-attribution path. Current ownership
is [libs/scoring/README.md](../../libs/scoring/README.md) for scores and
[libs/anomaly/README.md](../../libs/anomaly/README.md) for persisted attribution.
The active end-to-end contract is [V2 architecture](../current/v2_architecture.md).

## Purpose

Attribution makes an emitted anomalous window inspectable. It separates the
question "how unusual is this window?" from "which system context, parameters,
telemetry, and events best explain it?" This preserves a stable scoring path while
allowing downstream localization evidence to be materialized at an explicit grain.

## Score Inputs

`WindowScoresRawTable` produces one score row per window from phase and structural
artifacts. The active score-channel contract is:

- `regime_deviation`
- `reconstruction_error`
- `event_discordance`
- `bound_violation`
- `accumulation_violation`
- `response_violation`
- `state_violation`
- `coherence_break`

`WindowScoresCalibratedTable` calibrates raw scores within phase context and emits
the conservative `emit_ready` decision. Production score semantics remain in the
Spark `Table` owners; bounded local materialization is reserved for validation and
reporting after artifacts exist.

## Attribution Construction

Stage `90_anomaly_attribution.py` filters calibrated windows and builds three
artifacts through `AnomalyAttributionPlan`:

- `anomaly_window_attribution`: one row per emitted/scored anomaly window, with
  dominant system/subsystem/module context and panel context;
- `anomaly_telemetry_attribution`: window-local parameter and telemetry evidence;
- `anomaly_event_attribution`: window-local event evidence.

The hierarchy map provides rollup context. Parameter candidates combine window
evidence with score-channel semantics and behavior-profile context where a
mechanism-specific channel needs residual-backed ranking without nearby events.
The output therefore retains dominant localization fields and detailed evidence
instead of reducing every anomaly to a single opaque label.

## Invariants

1. The window-attribution identity is `(tail_id, flight_id, win_id)`.
2. Telemetry and event attribution rows remain children of that window identity.
3. Attribution reads persisted calibrated scores, phase windows, raw telemetry,
   events, and `hierarchy_sensor_map`; it does not reimplement score semantics.
4. `subsystem_scores` is retained in the raw-score schema for compatibility, but
   current localization uses dominant and ranked targets rather than treating an
   empty map as evidence.
5. Stage `90` writes with merge semantics so the window identity is idempotent.

## Validation Semantics

`validate_attribution_against_misbehavior_truth(...)` compares the attribution
artifacts with injected simulator misbehavior truth using window-local strict
overlap semantics. Reports distinguish parameter, subsystem, module, and
reconstruction-localization outcomes so a miss can be diagnosed as missing local
candidates, rollup loss, or insufficient observable evidence.

The simulation validation harness and benchmark-tier reports keep recovery claims
scoped to authored truth windows and their declared recoverability targets. An
anomaly that is parameter-visible-only should not be interpreted as a failed
subsystem-localization case.

## Known Boundaries

Attribution is a downstream explanation layer, not an independent detector. It
cannot recover a structural target when upstream score evidence is absent or when
the fitted hierarchy does not retain a supported relationship. Its quality is
therefore evaluated with score, hierarchy, and simulator evidence together.

## Notes

Localization improvements and benchmark acceptance work are maintained in the
[anomaly plan](../plans/libs/anomaly.md). This document describes the active
artifact and validation boundaries only.
