# Theory Foundations

This document tracks the mathematical and statistical ideas used in the active S3NTINEL V2 pipeline, the primary references behind them, and the exact code paths where they are implemented.

The purpose is not to perform ritual citation. It is to keep the code honest. If an edge weight, score, or artifact has a mathematical interpretation, that interpretation should be written down here and tied to the implementation.

For the intended sequential fitting workflow that should produce datatype, scaling,
and behavior metadata before structural fitting, see [fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/fitting_workflow.md).

## Scope

This document covers the active path:

- telemetry normalization
- event extraction
- `window_x` / `window_s`
- backbone fitting
- graph construction
- phase fitting
- conformal calibration

It does not attempt to be a textbook. It is a map from theory to code.

## Principles

1. A quantity should have one interpretation.
2. If a weight is normalized, the normalization should match the statistical object it claims to measure.
3. Driver-side approximations are acceptable only when their semantics remain correct.
4. Graph edges must mean something specific:
   - conditional dependence
   - association beyond marginals
   - transition probability
   - lagged conditional transition tendency

## 1. Robust Scaling

### Theory

The active continuous representation uses robust centering and scaling:

- center by median
- scale by interquartile range (IQR)

This is preferable to mean/std in early heterogeneous telemetry settings because:

- outliers are common
- many channels are non-Gaussian
- a few excursions should not define the coordinate system

The fitting workflow should persist these scaling statistics as a reusable artifact:

- `continuous_scaling_profile`

This keeps robust scaling from being an implicit side effect scattered across later
stages.

### Code

- [representations.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/representations.py)
  - `build_continuous_robust_scaler(...)`
- [window_x.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/window_x.py)
  - `build_window_x_table(...)`
  - `build_window_x_spark_table(...)`

### Active interpretation

For each continuous sensor `j`:

`x_scaled = (x - median_j) / max(iqr_j, eps)`

This is the coordinate system used for:

- `window_x`
- drift magnitude
- backbone fitting
- reconstruction residuals

The input signal for these calculations is the observed:

- `parameter_value`

not the clean simulator-side:

- `parameter_value_clean`

## 2. Provisional Window Vector `window_x`

### Theory

`window_x` is the provisional continuous representation used before phase structure is added.

Its semantics are:

- end-of-window continuous state
- robust-scaled
- one vector per window

It is intentionally simpler than a full dynamical summary. The active V2.1 note documents a possible future rate-aware extension.

### Code

- [window_x.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/window_x.py)
- [representations.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/representations.py)
  - `build_window_x_row(...)`

### Related deferred note

- [v2_1_notes.md](/home/jrs/code/S3NTINEL/sentinel/docs/v2_1_notes.md)

## 3. Drift Magnitude

### Theory

The active drift metric is Euclidean distance in the robust-scaled `window_x` space:

`d_w = ||x_w - x_{w-1}||_2`

This is a pragmatic first-order measure of structural change. It is not yet Mahalanobis or phase-conditioned.

### Code

- [representations.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/representations.py)
  - `window_vector_drift_magnitude(...)`

## 4. Backbone Fitting

### Theory

The active backbone uses additive sufficient statistics:

- per-sensor energy:
  - `E(j) = Σ_w x_w[j]^2`
- selected backbone sensors:
  - top sensors by energy
- ridge reconstruction solve:
  - `G = Σ_w x_C^T x_C`
  - `H = Σ_w x_C^T x`
  - `B = (G + λI)^(-1) H`

This is the correct map-reduce style formulation for a global linear reconstruction backbone.

It is deliberately simpler than probabilistic latent factor models, and that simplicity is an advantage in this codebase.

### Code

- [fit.py](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/fit.py)
- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/pipeline.py)
- [10_backbone_fit.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/10_backbone_fit.py)

### Interpretation

- `selected_sensors_c` are the observed backbone coordinates
- `weights_b` reconstruct the full continuous state from those coordinates

## 5. Precision Graph

### Theory

The continuous-coupling graph is built from the precision matrix.

Let:

- `Σ` be covariance over backbone coordinates
- `Θ = (Σ + λI)^(-1)` be the regularized precision matrix

For Gaussian graphical models, nonzero off-diagonal precision entries correspond to conditional dependence.

To normalize edge strength, the active path uses absolute partial correlation:

`ρ_ij = -Θ_ij / sqrt(Θ_ii Θ_jj)`

and then:

`precision_weight = |ρ_ij|`

This is the standard interpretation-preserving normalization for precision-derived edges.

### References

- qgraph documentation for weight matrix to partial correlation network:
  - https://rdrr.io/cran/qgraph/man/wi2net.html

### Code

- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
  - `_build_precision_graph_from_covariance(...)`
  - `_build_precision_graph(...)`
  - `build_precision_graph_from_window_x_spark_table(...)`

## 6. Event Co-occurrence Graph

### Theory

Same-window event co-occurrence is not merely overlap. The question is whether two sensors co-occur more often than expected from their marginals.

That is a PMI problem.

The active path now uses positive normalized PMI:

- `PMI(i,j) = log( p(i,j) / (p(i)p(j)) )`
- `NPMI(i,j) = PMI(i,j) / -log p(i,j)`
- `event_weight = max(0, NPMI(i,j))`

Why this is preferable to Jaccard:

- Jaccard measures overlap size
- PMI/NPMI measures association beyond frequency
- high-frequency sensors no longer dominate simply because they appear often

Using the positive part avoids treating negative association as an edge in an undirected support graph.

### References

- Church and Hanks (1990), word association norms and mutual information:
  - https://aclanthology.org/J90-1003/
- Bouma (2009), normalized PMI:
  - https://www.researchgate.net/publication/267306132_Normalized_Pointwise_Mutual_Information_in_Collocation_Extraction

### Code

- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
  - `_build_event_graph(...)`
  - `build_event_graph_spark_table(...)`

### Active interpretation

`event_weight` answers:

> How strongly do these two sensors co-occur within windows, above what their individual window frequencies would predict?

## 7. Transition Graph

### Theory

The transition graph is an immediate adjacency graph:

- if sensor `u` fires and the next event is on `v`
- count `u -> v`

The correct normalization is row-stochastic:

`transition_weight(u,v) = count(u->v) / Σ_k count(u->k)`

That makes each row a transition distribution from source sensor `u`.

This is the natural Markov-style normalization. It is more meaningful than dividing by the global maximum count, which destroys the source-conditioned interpretation.

### References

- standard transition-matrix / Markov-chain semantics:
  - https://www.cs.purdue.edu/homes/dgleich/nmcomp/lectures/lecture-8.html
  - https://snowch.github.io/learn_probability/chapter_16.html

### Code

- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
  - `_build_transition_graph(...)`
  - `build_transition_graph_spark_table(...)`

### Active interpretation

`precedence_weight(u,v)` answers:

> Given that the previous event was on `u`, how often is the immediate next event on `v`?

## 8. Lag Graph

### Theory

The lag graph is broader than immediate precedence:

- maintain a lag buffer up to `τ_max`
- connect prior sensor `u` to later sensor `v`

The active normalization is:

`lag_weight(u,v) = P(v | u within τ_max) * shortness`

where:

- `P(v | u within τ_max)` is the row-normalized lagged transition probability
- `shortness = max(0, 1 - mean_lag / τ_max)`

This keeps the probability semantics while preferring shorter lags.

This is an inference from transition-matrix semantics plus a lag penalty; it is not claimed here as the unique canonical lag-graph formula.

### Code

- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
  - `_build_lag_graph(...)`
  - `build_lag_graph_spark_table(...)`

### Active interpretation

`lag_weight(u,v)` answers:

> How often does `v` follow `u` within the lag horizon, normalized by all lagged successors of `u`, and discounted if that lag is usually long?

## 9. Graph Fusion

### Theory

The active fused graph combines:

- continuous conditional dependence
- same-window event association
- lagged directed association, symmetrized at fusion time

The active formula is:

`fused_weight = α * precision_weight + β * event_weight + γ * lag_weight`

This is not a learned fusion. It is a transparent weighted sum.

Its virtue is interpretability:

- `α` raises or lowers continuous dependence
- `β` raises or lowers same-window event association
- `γ` raises or lowers delayed coupling

### Code

- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
  - `_fuse_graphs(...)`
  - `build_fused_graph_spark_table(...)`
  - `build_graph_fusion_from_tables(...)`
  - `build_graph_fusion_from_component_tables(...)`

## 10. Hierarchy Assignment

### Theory

The current hierarchy assignment is intentionally simple and deterministic.

Step 1:
- threshold fused edges by minimum weight

Step 2:
- keep only mutual top-k neighbors per sensor

Step 3:
- connected components on that local graph define modules

Step 4:
- roll modules up into subsystems via averaged inter-module edge weights

Step 5:
- roll subsystems up into systems via averaged inter-subsystem edge weights

This is not spectral clustering or modularity maximization. It is a baseline graph rollup with explicit local-support constraints.

### Current Spark boundary

In the active graph-fit stage:

- `precision_graph` is built from Spark-aggregated covariance statistics
- `event_graph`, `lag_graph`, and `transition_graph` are built in Spark
- `fused_graph` is built in Spark from those component tables
- only the already-pruned fused edge set and the parameter universe are brought to
  the driver for the final connected-components hierarchy assignment

So the remaining driver-side work is the hierarchy clustering itself, not graph
construction or fusion.

### Code

- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
  - `_assign_hierarchy(...)`
  - `build_hierarchy_from_fused_spark_table(...)`
- [hierarchy.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/hierarchy.py)
  - `_connected_components_from_edges(...)`
  - `assign_hierarchy_from_weighted_edges(...)`

### Active interpretation

- `module_id`: local mutually strong cluster
- `subsystem_id`: rollup of modules with sufficient inter-module support
- `system_id`: rollup of subsystems with sufficient inter-subsystem support

## 11. Phase Fitting

### Theory

The active phase path clusters in `window_s` space after robust scaling by tail.

This matters because:

- different `s_w` dimensions can have very different scales
- clustering on unscaled mixed summary features is geometrically unstable

The active path also uses:

- transition penalty
- minimum dwell
- ordered progression bias

That makes phase fitting a segmented sequence problem, not just independent nearest-centroid assignment.

### Code

- [detect.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/detect.py)
- [pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/pipeline.py)
- [50_phase_fit.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/50_phase_fit.py)

## 12. Conformal Calibration

### Theory

The active calibration layer is a simple empirical tail calibration within `(tail_id, flight_id, phase_id_detected)` slices.

It is not full conformal inference in the strongest exchangeability sense. It is a practical empirical calibration layer that produces:

- `warm`
- `emit_ready`
- `p_value`

### Code

- [build.py](/home/jrs/code/S3NTINEL/sentinel/libs/conformal/build.py)
- [70_window_scores_calibrate.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/70_window_scores_calibrate.py)

## 13. Current Open Questions

These are mathematically important and not yet fully settled.

### 13.1 Lag graph semantics

The current lag weighting is reasonable, but not final. Future alternatives could include:

- explicit lag-bucket distributions
- conditional PMI over lag buckets
- hazard-like models of delayed response

### 13.2 Hierarchy clustering

Connected-components rollup is serviceable but coarse. Future candidates:

- spectral clustering
- modularity-based clustering
- multi-level graph community methods

### 13.3 Rate-aware continuous representation

See [v2_1_notes.md](/home/jrs/code/S3NTINEL/sentinel/docs/v2_1_notes.md).

The current `window_x` is intentionally simple, but a realistic multi-rate hierarchy will likely eventually require per-rate summary blocks.

## 14. Maintenance Rule

If a future change alters:

- a graph weight definition
- a score normalization
- a clustering objective
- a backbone solve

then this document should be updated in the same change.

Otherwise the code will drift faster than the theory, and the repository will become opaque again.
