# Glossary

This glossary records the active code/data taxonomy for the S3NTINEL V2 path.

It is not a marketing glossary. It is a contract glossary: the names here are meant
to line up with code, tables, schemas, and reports.

Use this document when naming:

- directories
- modules
- functions
- persisted fields
- output tables
- report sections

## 1. Core entities

### tail

An aircraft identifier.

Canonical field:
- `tail_id`

### flight

A single run/session for a tail.

Canonical field:
- `flight_id`

### parameter

A telemetry channel. This is the canonical term used across the active path instead
of the older generic term `sensor`.

Canonical field:
- `parameter_name`

Deprecated/internal fallback only:
- `sensor`

### timestamp

A UTC timestamp associated with a telemetry sample, event, or attributed record.

Canonical field:
- `timestamp_utc`

Deprecated/internal fallback only:
- `ts`

### window

A time segment used for aggregation, phase fitting, scoring, and anomaly attribution.

Canonical field:
- `win_id`

Related fields:
- `t_start`
- `t_end`
- `duration_ms`
- `event_count`

## 2. Labels vs detected outputs

S3NTINEL uses `*_label` for simulator/source labels and `*_detected` for model outputs.

### event label

Detector-truth event label emitted from the simulator path.

Canonical field:
- `event_type_label`

### event detected

Event type emitted by the event detector.

Canonical field:
- `event_type_detected`

### anomaly labels

Simulator/source anomaly labels.

Canonical fields:
- `anomaly_type_label`
- `anomaly_score_label`

### anomaly detected

Reserved detector-side anomaly outputs.

Canonical fields:
- `anomaly_type_detected`
- `anomaly_score_detected`

These may exist as placeholder columns even when not fully implemented.

### phase label

Simulation truth for the phase.

Canonical field:
- `phase_label`

### phase detected

Detected/assigned phase outputs.

Canonical fields:
- `phase_id_detected`
- `phase_state_detected`
- `phase_confidence_detected`
- `distance_to_centroid_detected`

## 3. Datatype taxonomy

### datatype label

Simulator/source datatype for a parameter.

Canonical field:
- `parameter_datatype_label`

### datatype profiled

Profiler-inferred datatype for a parameter.

Canonical field:
- `parameter_datatype_profiled`

### sampling rate

If carried as label/profiled fields, use:

- `sampling_rate_label_hz`
- `sampling_rate_profiled_hz`

These are preferred over unlabeled generic `sampling_rate_hz` in persisted interfaces.

## 4. Behavior taxonomy

### behavior label

Simulation/source behavior-family label for a parameter.

Planned canonical field:
- `behavior_family_label`

Optional future companion:
- `behavior_traits_label`

### behavior profiled

Profiler-inferred behavior-family label for a parameter.

Planned canonical fields:
- `behavior_family_profiled`
- `behavior_profile_confidence`

Optional future companion fields:
- `regulated_score_profiled`
- `inertial_score_profiled`
- `accumulative_score_profiled`
- `discrete_state_score_profiled`

## 5. Misbehavior taxonomy

### misbehavior label

Simulation/source misbehavior-family label.

Planned canonical fields:
- `misbehavior_family_label`
- `misbehavior_detail_label`

### misbehavior detected

Detector/runtime misbehavior-family output.

Planned canonical fields:
- `misbehavior_family_detected`
- `misbehavior_detail_detected`
- `misbehavior_confidence_detected`

## 6. Representations

### window_x

The provisional continuous representation used for backbone fitting and related
continuous-structure computations.

Math shorthand:
- `x_w`

Comment alias:
- provisional window vector

Typical contents:
- robust-scaled continuous end-of-window snapshot
- continuous-only

### window_s

The final structure representation used for phase and scoring logic.

Math shorthand:
- `s_w`

Comment alias:
- structure vector

Typical contents:
- backbone-restricted continuous block
- compact event summaries
- compact categorical summaries
- compact window summary scalars

### continuous vector at window end

Concrete persisted field inside window-like artifacts.

Canonical field:
- `continuous_vector_t_end_scaled`

## 7. Backbone taxonomy

### backbone

The selected continuous sensor subset and associated reconstruction weights.

Canonical table:
- `backbone`

Important fields:
- `selected_sensors_c`
- `all_sensors`
- `weights_b`
- `lambda_ridge`
- `training_window_count`

### backbone sensor energy

Per-parameter energy used for backbone selection diagnostics and reporting.

Canonical table:
- `backbone_sensor_energy`

Important fields:
- `parameter_name`
- `energy`
- `support_count`

## 8. Graph taxonomy

### precision graph

Continuous conditional-dependence graph over backbone-selected parameters.

Canonical table:
- `precision_graph`

Weight semantics:
- absolute partial correlation

### event graph

Same-window event co-occurrence graph over parameters.

Canonical table:
- `event_graph`

Weight semantics:
- positive normalized PMI

### lag graph

Directed lagged event relationship graph over parameters.

Canonical table:
- `lag_graph`

Weight semantics:
- row-normalized lagged conditional probability with short-lag discount

### transition graph

Directed immediate-precedence event graph over parameters.

Canonical table:
- `transition_graph`

Weight semantics:
- row-normalized transition probability

### fused graph

Weighted combination of the active graph components used for hierarchy assignment.

Canonical table:
- `fused_graph`

### hierarchy sensor map

The resolved hierarchy assignment for parameters.

Canonical table:
- `hierarchy_sensor_map`

Important fields:
- `parameter_name`
- `module_id`
- `subsystem_id`
- `system_id`

## 9. Phase artifacts

### phase windows

Window-level rows enriched with detected phase assignments and structure summaries.

Canonical table:
- `phase_windows`

### phase baselines

Per-tail phase baseline/centroid artifacts.

Canonical table:
- `phase_baselines`

## 10. Scoring artifacts

### raw window scores

Pre-calibration window-level scores.

Canonical table:
- `window_scores_raw`

Important fields include:
- `global_score`
- `reconstruction_score`
- `graph_violation_score`
- `subsystem_scores`
- `dominant_subsystem_id`
- `score_component_scores`
- `dominant_score_component`

### calibrated window scores

Post-calibration window-level scores.

Canonical table:
- `window_scores_calibrated`

## 11. Anomaly attribution artifacts

### anomaly window attribution

Primary anomaly-window fact table.

Canonical table:
- `anomaly_window_attribution`

Grain:
- one row per anomalous/scored window

### anomaly telemetry attribution

Telemetry attribution rows associated with an anomaly window.

Canonical table:
- `anomaly_telemetry_attribution`

Grain:
- `(tail_id, flight_id, win_id, timestamp_utc, parameter_name)`

### anomaly event attribution

Event attribution rows associated with an anomaly window.

Canonical table:
- `anomaly_event_attribution`

Grain:
- `(tail_id, flight_id, win_id, timestamp_utc, parameter_name, event_type_detected)`

## 12. Preferred naming rules

### Prefer explicit table names over vague names

Use:
- `window_scores_raw`
- `window_scores_calibrated`
- `anomaly_window_attribution`

Avoid:
- `scores`
- `calibrated`
- `anomalies`

### Prefer `parameter_name` over `sensor`

Use `sensor` only where:

- a legacy fallback must be tolerated, or
- an external dependency already fixes the name

### Prefer `timestamp_utc` over `ts`

Use `ts` only as a local transient variable if necessary.

### Prefer `*_label` and `*_detected`

Avoid:
- `truth_*`
- unlabeled ambiguous fields where the semantics are source-vs-model dependent

### Prefer explicit grain in attribution tables

Primary anomaly fact:
- `anomaly_window_attribution`

Child/detail tables:
- `anomaly_telemetry_attribution`
- `anomaly_event_attribution`

## 13. Deprecated terminology

These terms should not appear in new code or active persisted interfaces:

- `truth_*`
- `sim_event_type`
- bare detector `event_type` in persisted outputs
- `signatures`
- `pivot_block`
- `cur_block`
- `event_block`
- `cat_block`
- `subsystem_map`
- generic `anomalies` as the primary active output table name

## 14. Maintenance rule

When a new field, function, or table is introduced:

1. prefer a name already present in this glossary
2. if a new term is necessary, update this glossary
3. do not introduce parallel aliases unless there is a short-lived migration reason
