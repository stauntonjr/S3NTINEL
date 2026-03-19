# S3NTINEL V2 Architecture

This document describes the active pipeline path at a high level.

For current ownership and implementation names, prefer:
- [pipelines/README.md](/home/jrs/code/S3NTINEL/sentinel/pipelines/README.md)
- [libs/windows/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/windows/README.md)
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)
- [libs/graph/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/graph/README.md)

For the mathematical/statistical interpretation of the active representations and graph weights, see [theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/theory_foundations.md).
For the active code/data taxonomy and naming rules, see [glossary.md](/home/jrs/code/S3NTINEL/sentinel/docs/glossary.md).
For the intended one-off fitting sequence for datatype profiling, robust scaling, behavior profiling, and backbone fitting, see [fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/fitting_workflow.md).
For domain guidance on realistic avionics-system behavior and simulator priors, see [avionics_simulation_guidelines.md](/home/jrs/code/S3NTINEL/sentinel/docs/avionics_simulation_guidelines.md).
For recommended simulator anomaly families and backbone-fit validation methods, see [anomaly_injection_and_backbone_validation.md](/home/jrs/code/S3NTINEL/sentinel/docs/anomaly_injection_and_backbone_validation.md).
For replayable stage artifacts, caches, manifests, and MLflow lineage policy, see [artifact_replay_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/artifact_replay_design.md).

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
3. `parameter_behavior_profile`

Those artifacts establish stable parameter semantics and should normally be fit once
and reused. The current active code already performs datatype/rate profiling and
robust scaling; behavior profiling is the next planned metadata artifact.

Then the active structural stages are:

1. `pipelines/00_ingest_raw.py`
2. `pipelines/05_parameter_profiles_fit.py`
3. `pipelines/10_backbone_fit.py`
4. `pipelines/11_build_graph.py`

### Inference

1. `pipelines/20_events_extract.py`
2. `pipelines/30_windows_adaptive.py`
3. `pipelines/50_phase_fit.py`
4. `pipelines/60_window_scores_raw.py`
5. `pipelines/70_window_scores_calibrate.py`
6. `pipelines/80_anomaly_attribution.py`

## Core representations

- `window_features`
  - persisted many-window feature artifact
  - used for drift, energy, backbone fitting, graph fitting, and phase fitting

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

- `parameter_behavior_profile`
  - profiled nominal behavior-family metadata

- `backbone`
  - `selected_sensors_c`
  - `all_sensors`
  - `weights_b`
  - `lambda_ridge`

- `backbone_sensor_energy`
  - per-sensor energy used for backbone selection

- `phase_windows`
  - V2 per-window structure and detected phase context

- `phase_baselines`
  - per-tail detected phase baselines in `window_s` space

- `window_scores_raw`
  - raw V2 structure/reconstruction scoring outputs

- `window_scores_calibrated`
  - calibrated window scores used for emission

- `precision_graph`
  - continuous coupling on backbone-selected sensors
  - weight semantics: absolute partial correlation

- `event_graph`
  - same-window sensor cooccurrence graph
  - weight semantics: positive normalized PMI

- `lag_graph`
  - directed lagged transition graph
  - weight semantics: row-normalized lagged conditional probability times short-lag discount

- `transition_graph`
  - directed adjacent-event transition graph
  - weight semantics: row-normalized transition probability

- `fused_graph`
  - first-pass fusion of precision, event, and lag structure

- `hierarchy_sensor_map`
  - first-pass module/subsystem/system assignment from fused graph

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
- `10_backbone_fit.py` keeps fact-table work in Spark and collects only bounded
  sensor-energy and per-flight `G_f/H_f` aggregates for the final solve.
- `11_build_graph.py` keeps component-graph construction in Spark and collects only
  small backbone metadata and the already-pruned fused edge set for final hierarchy
  assignment. Precision, event, lag, transition, and fused graph construction are
  all Spark-native.
- `50_phase_fit.py` consumes persisted `window_features` and emits per-flight phase artifacts
  with segmented Spark assignment; the remaining driver-side work is bounded global configuration.
- `60_window_scores_raw.py` keeps the main fact table distributed, but still collects bounded reference artifacts.

Bridge rule:
- do not use `toPandas()` on growing fact tables in active Spark stages
- do not use `collect()` except for explicitly bounded reference artifacts
- bounded bridge stages should fail fast once configured row-count thresholds are exceeded
