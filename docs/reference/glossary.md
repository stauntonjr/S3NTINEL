# Glossary

This is the contract glossary for the active S3NTINEL data flow. Use its
canonical names in code, schemas, persisted artifacts, reports, and cross-package
documentation. It is organized by the order in which telemetry becomes a
validated attribution result, not by pipeline stage ownership.

For stage composition, use [pipelines/README.md](../../pipelines/README.md).
For current artifact contracts, use [V2 architecture](../current/v2_architecture.md).

## 1. Telemetry And Parameter Semantics

### A-MATS, AFDX, CBM+, and BLADE

These are operating-context terms, not persisted S3NTINEL artifact names.

- **A-MATS:** Advanced Maintenance and Troubleshooting Suite, the motivating
  aircraft-side data-collection program for this project.
- **AFDX / ARINC 664 Part 7:** deterministic switched-Ethernet avionics network
  from which the target signal feed is derived; it is not the S3NTINEL telemetry
  schema.
- **CBM+:** condition-based maintenance context that uses equipment condition
  and operational evidence to support maintenance decisions.
- **BLADE:** Basing and Logistics Analytics Data Environment, the broader
  logistics and analytics setting relevant to downstream CBM+ use.

See [operational_context.md](../design/operational_context.md) for source
background and the boundary between this target context and the active V2
implementation.

### tail and flight

An aircraft identity and one telemetry run for that aircraft.

Canonical fields: `tail_id`, `flight_id`.

### parameter

A telemetry channel. `parameter_name` is the canonical persisted field. Use
`sensor` only where an external API or a current compatibility boundary fixes that
spelling.

### parameter value

The observed telemetry value consumed by profilers, detectors, windows, and
structural fitting. Canonical field: `parameter_value`.

`parameter_value_clean` is the simulator-side pre-observation value used for
truth and debugging; it is not a production fitting input.

### timestamp

UTC instant for telemetry, event, and attribution rows. Canonical field:
`timestamp_utc`. `ts` is limited to short-lived local variables.

### coupling ID label

Simulator truth label identifying an authored coupling relationship. Canonical
field: `coupling_id_label`. It supports simulation coupling validation and does
not determine the learned hierarchy directly.

### source labels and detected outputs

`*_label` fields are simulator or source truth; `*_detected` fields are model
outputs. Examples include `event_type_label`, `event_type_detected`,
`phase_label`, `phase_id_detected`, `anomaly_type_label`, and
`anomaly_type_detected`.

## 2. Profiles, Events, Windows, And Phases

### parameter datatype profile

Reusable metadata from stage `10` that classifies a parameter and records its
observed cadence. Canonical artifact: `parameter_datatype_profile`.

### continuous scaling profile

Reusable robust-scaling metadata for continuous parameters. Canonical artifact:
`continuous_scaling_profile`. Principal fields include `parameter_name`,
`scaling_center_median`, and `scaling_iqr`.

### parameter behavior profiles

`parameter_behavior_primitive_profile` contains telemetry-derived primitive
evidence. `parameter_behavior_profile` contains nominal behavior-family metadata,
including `behavior_family_profiled` and `behavior_profile_confidence`.

### event

A detector-emitted occurrence associated with a parameter and timestamp.
Canonical detector field: `event_type_detected`. Canonical artifact: `events`.

### window and window policy profile

A window is a time segment used for aggregation, phase fitting, scoring, and
attribution. Its canonical identity field is `win_id`; common bounds are
`t_start`, `t_end`, and `duration_ms`.

`window_policy_profile` is the fitted ranked candidate policy artifact. The row
where `is_selected=true` supplies the active adaptive-window policy.

### window features and window structure

`window_features` is the persisted many-window representation used for backbone,
graph, and phase fitting. `continuous_vector_t_end_scaled` is its end-of-window
continuous snapshot. `window_s` is the compact structure representation used by
phase and scoring logic.

### phase windows and phase baselines

`phase_windows` enriches windows with detected phase assignments and structure
summaries. `phase_baselines` contains per-tail detected phase baseline/centroid
artifacts. `phase_state_detected` distinguishes `stable` from
`transition_region` without adding a separate primary phase taxonomy.

## 3. Backbone And Graph Artifacts

### backbone and backbone sensor energy

`backbone` records the selected continuous parameter subset and reconstruction
weights. `backbone_sensor_energy` records per-parameter energy and support used
for diagnostics and selection.

### precision graph

`precision_graph` is the continuous conditional-dependence graph over
backbone-selected parameters. Its edge weight is absolute partial correlation.

### event graph

`event_graph` is the same-window event co-occurrence graph. Its edge weight is
positive normalized PMI.

### lag profile

`lag_profile` is the first-class directed, band-aware nearest-prior event
relationship artifact. It retains lag-band and support information, including
`lag_band` and `support_flight_count`. Its weight is a per-band row-normalized
lagged conditional probability with a short-lag discount.

### lag graph

`lag_graph` is the collapsed directed lag representation derived from
`lag_profile`. It supplies a compact lag input to graph fusion; use `lag_profile`
when inspecting band-level lag evidence.

### transition graph

`transition_graph` records directed immediate-precedence event relationships.
Its edge weight is a row-normalized transition probability.

### fused graph

`fused_graph` is the weighted combination of precision, event, lag, and
transition evidence used by hierarchy fitting. It is relationship evidence, not a
hierarchy assignment.

### graph parameter universe

`graph_parameter_universe` is the bounded canonical parameter set persisted
between graph construction and hierarchy fitting. It prevents the hierarchy stage
from inventing or silently dropping graph-domain parameters during replay.

## 4. Hierarchy Artifacts And Evidence

### hierarchy sensor map

`hierarchy_sensor_map` is the resolved parameter-to-module, subsystem, and system
assignment. Important fields include `parameter_name`, `module_id`,
`subsystem_id`, and `system_id`.

### hierarchy edge evidence

`hierarchy_edge_evidence` contains the mutual-top-k fused edges retained for the
module-level hierarchy decision. It records endpoint ranks, component weights,
assigned hierarchy IDs, and directed lag evidence in both directions.

### hierarchy-edge evidence report

`hierarchy_edge_evidence_summary.json` maps configured simulation coupling
signatures to retained canonical hierarchy edges. It is a diagnostic report, not
a persisted graph table and not a claim that all simulated couplings must appear
as hierarchy edges.

## 5. Scoring, Attribution, And Validation

### raw and calibrated window scores

`window_scores_raw` holds pre-calibration window scores and their component
evidence. The active components are `regime_deviation`, `reconstruction_error`,
`event_discordance`, `bound_violation`, `accumulation_violation`,
`response_violation`, `state_violation`, and `coherence_break`.
Its bounded `parameter_score_evidence` payload preserves score-owned residual,
event, profile, and behavior-channel evidence for candidate diagnosis.

`window_scores_calibrated` adds phase-conditioned calibration and the conservative
`emit_ready` decision. `subsystem_scores` remains a schema compatibility field;
the active scorer uses dominant and ranked localization fields instead.

### anomaly window attribution

`anomaly_window_attribution` is the primary anomaly fact table, with one row per
emitted or scored anomaly window. Its identity is `(tail_id, flight_id, win_id)`.

### anomaly telemetry attribution

`anomaly_telemetry_attribution` contains parameter and telemetry evidence local
to an anomaly window. Its grain is `(tail_id, flight_id, win_id, timestamp_utc,
parameter_name)`.

### anomaly event attribution

`anomaly_event_attribution` contains detected-event evidence local to an anomaly
window. Its grain adds `event_type_detected` to the telemetry attribution key.

### anomaly parameter candidate evidence

`anomaly_parameter_candidate_evidence` is the bounded score-to-localization
ledger. Its grain is `(tail_id, flight_id, win_id, parameter_name)`. It records
the score evidence, hierarchy mapping, localization support and rank, and
whether the candidate survived the telemetry and structural cuts.

### phase reference model

`phase_reference_model` is the reusable stage-70 phase fit artifact. Each
reference-flight phase row carries the fitted feature/backbone configuration,
robust feature statistics, centroid and distance scale, and ordered
transition-support band needed to assign phases on another flight. It contains
model state, not simulator truth, and reference inference replaces its source
flight keys with target flight keys before distributed assignment.

### misbehavior truth and recoverability

`misbehavior_family_label` and `misbehavior_detail_label` identify simulator
truth. Validation reports distinguish what is observable at parameter,
module, subsystem, or detection-only scope so a lower-level observable anomaly is
not assessed as a failed higher-level localization claim.

### validation harness

The validation harness joins run configuration, stage manifests, model-validation
metrics, performance information, and simulator context into an experiment-facing
bundle. Its reports are the canonical comparison surface for iterative simulation
evaluation.

## 6. Naming Rules

- Prefer explicit artifact names such as `window_scores_raw` over generic names
  such as `scores`.
- Prefer `parameter_name` over generic `sensor` in persisted interfaces.
- Prefer `timestamp_utc` over `ts` in persisted interfaces.
- Use `*_label` for source truth and `*_detected` for model outputs.
- State the grain of every attribution table.
- Do not create parallel aliases for a durable artifact or field.

## Notes

When a new durable field, function, or artifact crosses package boundaries, add
it here and update its owning schema and package README in the same change.
