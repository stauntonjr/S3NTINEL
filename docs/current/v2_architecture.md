# S3NTINEL V2 Architecture

This document describes the active pipeline path at a high level.

For current ownership and implementation names, prefer:
- [pipelines/README.md](../../pipelines/README.md)
- [libs/windows/README.md](../../libs/windows/README.md)
- [libs/phase/README.md](../../libs/phase/README.md)
- [libs/graph/README.md](../../libs/graph/README.md)

For the mathematical/statistical interpretation of the active representations and graph weights, see [theory_foundations.md](../reference/theory_foundations.md).
For the active code/data taxonomy and naming rules, see [glossary.md](../reference/glossary.md).
For the intended one-off fitting sequence for datatype profiling, robust scaling, behavior profiling, and backbone fitting, see [fitting_workflow.md](fitting_workflow.md).
For domain guidance on realistic avionics-system behavior and simulator priors, see [avionics_simulation_guidelines.md](../simulation/avionics_simulation_guidelines.md).
For recommended simulator anomaly families and backbone-fit validation methods, see [anomaly_injection_and_backbone_validation.md](../research/anomaly_injection_and_backbone_validation.md).
For replayable stage artifacts, caches, manifests, and MLflow lineage policy, see [artifact_replay_design.md](../design/artifact_replay_design.md).

For local smoke and developer workflows, the active baseline is `sentinel-spark35` on Python `3.11` with Spark `3.5.1` and Delta `3.0.0`. Prefer `S3NTINEL_TABLE_FORMAT=parquet` unless the Spark runtime already has Delta JVM jars available. The `delta-spark` Python package does not bundle those jars.

Spark/Delta bootstrap controls:

- `S3NTINEL_DELTA_JAR_PATH`: comma-separated Delta jar path(s) to load directly
- `S3NTINEL_SPARK_EXTRA_JARS`: comma-separated extra Spark jar path(s)
- `S3NTINEL_DELTA_ALLOW_MAVEN`: allow Maven package resolution when no local jars are provided (`true` by default)

## Active V2 path

### Fitting

Before structural fitting, the intended one-off metadata workflow is:

1. `parameter_datatype_profile`
2. `continuous_scaling_profile`
3. `parameter_behavior_primitive_profile`
4. `parameter_behavior_profile`

Those artifacts establish stable parameter semantics and should normally be fit once
and reused. Behavior profiling is now a two-layer metadata path:

- Spark derives primitive evidence from raw telemetry into `parameter_behavior_primitive_profile`
- family scoring consumes that artifact to emit `parameter_behavior_profile`

Then the active structural stages are:

1. `pipelines/00_ingest_raw.py`
2. `pipelines/10_parameter_profiles_fit.py`
3. `pipelines/12_behavior_profiles_fit.py`
4. `pipelines/15_event_profiles_fit.py`
5. `pipelines/20_events_extract.py`
6. `pipelines/25_window_policy_profile.py`
7. `pipelines/30_windows_adaptive.py`
8. `pipelines/40_backbone_fit.py`
9. `pipelines/50_build_graph.py`
10. `pipelines/60_fit_hierarchy.py`

### Inference

1. `pipelines/70_phase_fit.py`
2. `pipelines/80_window_scores_raw.py`
3. `pipelines/85_window_scores_calibrate.py`
4. `pipelines/90_anomaly_attribution.py`
5. `pipelines/95_emit_explorer_bundle.py`

`pipelines/72_phase_label_centroids.py` is a simulation-validation extension,
not a production inference stage. It runs after phase fitting only when truth
phase labels are available and produces label-conditioned centroid comparison
artifacts.

## Core representations

- `window_features`
  - persisted many-window feature artifact
  - used for drift, energy, backbone fitting, graph fitting, and phase fitting
  - `continuous_vector_t_end(_scaled)` is the raw telemetry snapshot at `t_end` under ZOH, not a last-event payload overwrite
  - `continuous_event_summary` carries additive run-based continuous-event context per window

- per-window feature row
  - one-window feature representation used to build the persisted artifact

- `window_s`
  - final structure vector
  - currently built from:
    - backbone-selected continuous vector `x_c`
    - compact event summary
    - compact categorical state summary
    - compact scalar window summary

## Active V2 artifacts

- `parameter_datatype_profile`
  - profiled datatype and observed rate metadata

- `continuous_scaling_profile`
  - robust scaling metadata for continuous parameters

- `parameter_behavior_primitive_profile`
  - per-parameter primitive evidence derived from raw telemetry and scaling metadata
  - includes run persistence, reversals, center/bound occupancy, excursion returns, accumulation, oscillation, tracking, and discrete-state evidence

- `parameter_behavior_profile`
  - profiled nominal behavior-family metadata
  - active family taxonomy: `regulated`, `tracking`, `inertial`, `accumulative`, `discrete_state`, `mixed_unknown`

- `backbone`
  - `selected_sensors_c`
  - `all_sensors`
  - `weights_b`
  - `lambda_ridge`

- `window_policy_profile`
  - ranked candidate `max_ms` / `event_threshold` policies fit from the event stream
  - one row is marked `is_selected=true` and consumed by stage `30`
  - stage `25` also emits `reports/stages/25_window_policy_profile_evaluation.json`, a report-only summary of candidate frontier, closure mix, downstream cost proxies, and bounded window-boundary stability for the selected policy

- `backbone_sensor_energy`
  - per-sensor continuous energy plus event-aware selection score used for backbone selection

- `phase_windows`
  - V2 per-window structure and detected phase context

- `phase_baselines`
  - per-tail detected phase baselines in `window_s` space

- `window_scores_raw`
  - raw V2 anomaly scoring outputs
  - current canonical score-channel contract:
    - `regime_deviation`
    - `reconstruction_error`
    - `event_discordance`
    - `bound_violation`
    - `accumulation_violation`
    - `response_violation`
    - `state_violation`
    - `coherence_break`

- `window_scores_calibrated`
  - calibrated window scores used for emission

- `precision_graph`
  - continuous coupling on backbone-selected sensors
  - weight semantics: absolute partial correlation

- `event_graph`
  - same-window sensor cooccurrence graph
  - weight semantics: positive normalized PMI

- `lag_profile`
  - directed per-band nearest-prior lag profile over parameters
  - weight semantics: per-band row-normalized lagged conditional probability with short-lag discount
  - includes `lag_band` and `support_flight_count`

- `lag_graph`
  - directed collapsed lag compatibility graph derived from `lag_profile`
  - weight semantics: weighted sum of band-level lag-profile weights plus count-weighted mean lag

- `transition_graph`
  - directed adjacent-event transition graph
  - weight semantics: row-normalized transition probability

- `fused_graph`
  - first-pass fusion of precision, event, and lag structure

- `graph_parameter_universe`
  - bounded canonical parameter universe persisted between graph build and hierarchy fit

- `hierarchy_sensor_map`
  - first-pass module/subsystem/system assignment from fused graph

- `hierarchy_edge_evidence`
  - retained mutual-top-k edges that formed the module-level hierarchy
  - preserves each endpoint's retained-neighbor rank, all fused weight components,
    assigned hierarchy IDs, and directed lag evidence in both endpoint directions
  - `hierarchy_edge_evidence_summary.json` maps configured simulation coupling
    signatures to these canonical edges without treating absence as a simulator failure

## Current V2 output paths

- `S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH=data/delta/anomaly_window_attribution`
- `S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH=data/delta/anomaly_telemetry_attribution`
- `S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH=data/delta/anomaly_event_attribution`

## Important semantic rules

- Canonical label fields use `*_label`
- Canonical detector fields use `*_detected`
- `cooccur` is not part of the active V2 detector event contract
- graph cooccurrence and precedence are graph artifacts, not canonical detector event types

## Current Spark boundary

- `20_events_extract.py` uses the segmented Spark sequence kernel over `(tail_id, flight_id, parameter_name)` streams.
- `40_backbone_fit.py` keeps fact-table work in Spark and collects only bounded
  sensor-energy and per-flight `G_f/H_f` aggregates for the final solve.
  The solve still depends only on `continuous_vector_t_end_scaled`; event summaries affect sensor ranking, not the ridge system itself.
- `50_build_graph.py` keeps component-graph construction in Spark and collects only
  small backbone metadata and the already-pruned fused edge set for final hierarchy
  assignment. Precision, event, lag-profile, lag-collapse, transition, and fused
  graph construction are all Spark-native.
- `70_phase_fit.py` consumes persisted `window_features` and emits per-flight phase artifacts
  with segmented Spark assignment; the remaining driver-side work is bounded global configuration.
- `80_window_scores_raw.py` keeps the main fact table distributed, but still collects bounded reference artifacts.

Bridge rule:
- do not use `toPandas()` on growing fact tables in active Spark stages
- do not use `collect()` except for explicitly bounded reference artifacts
- bounded bridge stages should fail fast once configured row-count thresholds are exceeded
