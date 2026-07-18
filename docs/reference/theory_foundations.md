# Theory Foundations

This document tracks the mathematical meaning of the active S3NTINEL pipeline objects.

For current code ownership, see the area READMEs:
- [libs/windows/README.md](../../libs/windows/README.md)
- [libs/backbone/README.md](../../libs/backbone/README.md)
- [libs/graph/README.md](../../libs/graph/README.md)
- [libs/phase/README.md](../../libs/phase/README.md)
- [libs/scoring/README.md](../../libs/scoring/README.md)

## Scope

This note covers:

- telemetry normalization
- events
- window features
- backbone fitting
- graph construction
- phase fitting
- score calibration

It does not try to restate the package structure in detail.

## 1. Robust Scaling

The active continuous representation uses robust centering and scaling:

- center by median
- scale by interquartile range (IQR)

Interpretation:

`x_scaled = (x - median_j) / max(iqr_j, eps)`

This coordinate system is used for:

- per-window feature vectors
- drift magnitude
- backbone fitting
- reconstruction residuals

The fitted metadata artifact is:
- `continuous_scaling_profile`

## 2. Window Features

The active many-window artifact is **`window_features`**.

For one window, the semantic unit is the window-level feature row: robust-scaled
continuous end state plus associated categorical and event summaries.

This replaces older implementation-era references to `window_x` as the primary concept.

## 3. Drift Magnitude

The active drift metric is Euclidean distance in robust-scaled window-feature space:

`d_w = ||x_w - x_{w-1}||_2`

This is a pragmatic first-order structural change metric, not yet a phase-conditioned Mahalanobis metric.

## 4. Backbone Fitting

The backbone uses additive sufficient statistics over selected continuous features:

- per-sensor energy:
  - `E(j) = Σ_w x_w[j]^2`
- ridge reconstruction solve:
  - `G = Σ_w x_C^T x_C`
  - `H = Σ_w x_C^T x`
  - `B = (G + λI)^(-1) H`

Interpretation:

- `selected_sensors_c` are the backbone coordinates
- `weights_b` reconstruct the broader continuous state from those coordinates

## 5. Precision Graph

The continuous-coupling graph is built from the precision matrix.

Let:

- `Σ` be covariance over backbone coordinates
- `Θ = (Σ + λI)^(-1)` be the regularized precision matrix

The active normalized edge interpretation is absolute partial correlation:

`ρ_ij = -Θ_ij / sqrt(Θ_ii Θ_jj)`

`precision_weight = |ρ_ij|`

## 6. Event Co-Presence Graph

Same-window event relation is interpreted as association beyond marginals, not as raw overlap size.

The active event graph uses positive normalized PMI:

- `PMI(i,j) = log( p(i,j) / (p(i)p(j)) )`
- `NPMI(i,j) = PMI(i,j) / -log p(i,j)`
- `event_weight = max(0, NPMI(i,j))`

Cooccurrence is a relation, not an event type.

## 7. Lag and Transition Graphs

Lag and transition graphs model temporal relation, not same-window coexistence.

Interpretations:

- lag graph: delayed conditional tendency
- transition graph: adjacent-event or state-sequence tendency

These support:
- structure discovery
- hierarchy discovery
- anomaly interpretation

## 8. Phase Fitting

The phase layer operates on selected structure vectors derived from window features.

Key current nouns:

- `PhaseFeatureConfig`
- `PhaseFeatureFrame`
- `PhaseClusterModel`
- `PhaseDetectionPlan`

The conceptual steps are:

1. select informative phase features
2. cluster ordered window structure
3. assign and smooth phase progression
4. enforce dwell and emit baselines

## 9. Score Calibration

Calibration currently converts raw score outputs into phase-conditioned emit-ready results.

The active implementation is narrower than a full conformal framework, but the mathematical intent is still empirical tail calibration over a phase-conditioned score distribution.

## Notes

- When package READMEs and this file overlap, the README should be treated as the implementation-ownership source of truth and this file as the theory source of truth.
