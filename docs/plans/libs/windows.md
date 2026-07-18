# Windows Library Plan

Status: Plan
Authority: Non-authoritative roadmap. Use package READMEs and `docs/current/` for current behavior.

This note records a likely next-step architectural refinement for the continuous
window-feature layer. It is intentionally not part of the active path yet.

For the current implementation shape, see:
- [libs/windows/README.md](../../../libs/windows/README.md)
- [libs/phase/README.md](../../../libs/phase/README.md)

## Status

Deferred. Keep in mind for the next modeling-focused iteration, especially once:

- the active V2 interfaces are fully stable
- mixed-rate telemetry becomes more realistic in simulation and production
- phase quality and hierarchy quality are the main bottlenecks

## Resume Position

Keep this work deferred during the current resume cycle. The latest composite
replay reaches positive scoring and attribution; its immediate weakness is
candidate subsystem/module mapping, not insufficient continuous window-feature
expressiveness. Revisit this plan only after the anomaly plan's localization
gates show that the remaining loss is feature-level rather than mapping-level.

## Motivation

The current path uses a single continuous end-of-window snapshot inside the
window feature representation and the persisted `window_features` artifact, then adds compact event
and categorical summaries.

That is clean, but it under-represents mixed-rate continuous behavior:

- slow channels are mostly state-like
- medium-rate channels carry trend/range information
- fast channels carry variance/transient information

If all continuous channels are reduced to the same scalar snapshot, phase detection
and continuous coupling estimation can miss important dynamics.

## Proposed V2.1 direction

### Generality rule

If this work is used to improve downstream detection, it should do so through
generic continuous-time structure rather than simulation-specific waveform
recipes.

That means:

- use rate-aware summaries that are broadly meaningful across domains
- do not add summaries keyed to one scenario or one parameter family just
  because they help the current simulator bundle
- validate gains on downstream detection without turning the continuous feature
  layer into a simulator-specific encoding

### 1. Keep event/categorical logic out of the continuous backbone

Do not put raw categorical or sparse event channels into the continuous backbone fit.

Continue using:

- continuous block for backbone fitting and precision-style structure
- categorical block for logic/state summaries
- event block for behavioral and graph summaries

### 2. Use rate-aware continuous summaries

Partition continuous sensors into rate buckets:

- slow: `rate_hz <= 1`
- medium: `1 < rate_hz <= 10`
- fast: `rate_hz > 10`

Compute per-window summaries by bucket:

- slow:
  - `last_value`
  - `delta_from_prev_window`
- medium:
  - `window_mean`
  - `window_slope`
  - `window_range`
- fast:
  - `window_mean`
  - `window_std`
  - `window_range`

More aggressive fast-channel features such as `hf_energy` or oscillation counts are
plausible, but should be deferred until the simpler summaries are proven useful.

### 3. Separate backbone-fit windows from runtime windows

Do not force one window definition to satisfy both:

- runtime scoring / event grouping
- backbone fitting / covariance structure

Recommended split:

- runtime windows:
  - adaptive
  - event-aware
  - used for scoring, phase updates, anomaly emission
- backbone-fit windows:
  - more regular
  - structurally comparable
  - used for window features, energy, `G/H`, and precision structure

This avoids event-density bias in backbone fitting.

## Expected benefits

### Phase detection

Likely the largest direct gain.

Reason:

- phases are often differentiated by dynamics, not just level
- rate-aware summaries preserve state, trend, and variability separately
- `window_s` becomes more behaviorally expressive without contaminating the
  backbone with sparse logic/event channels

### Hierarchy fitting

Potentially helpful, but less automatically so.

Reason:

- shared subsystem behavior often appears as similar dynamics, not just level
- richer continuous summaries may improve continuous coupling
- but only if the feature blocks are normalized and aggregated carefully

## Important unresolved design point

The current backbone and graph code assume:

- one sensor -> one continuous backbone coordinate
- one sensor -> one residual value
- one sensor -> one graph node

Rate-aware feature blocks break that assumption.

Before implementation, choose one of these models explicitly:

1. sensor-level backbone with multi-feature sensor blocks
2. flat feature-level backbone with a later sensor aggregation rule

This choice affects:

- backbone selection
- `G/H` accumulation
- reconstruction residual attribution
- precision-graph construction

## Recommendation

Do not implement V2.1 yet.

When the active V2 contracts are stable enough, start with the minimal version:

1. separate backbone-fit windows from runtime windows
2. add conservative rate-aware summaries for continuous sensors
3. keep categorical/event summaries separate as they are now
4. defer fast-channel specialty features until the simpler version is validated
