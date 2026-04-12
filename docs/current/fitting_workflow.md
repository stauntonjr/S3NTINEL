# Fitting Workflow

This note defines the current fitting workflow at a conceptual level.

For current ownership and implementation details, see:
- [pipelines/README.md](/home/jrs/code/S3NTINEL/sentinel/pipelines/README.md)
- [libs/profiling/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/profiling/README.md)
- [libs/windows/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/windows/README.md)
- [libs/backbone/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/README.md)
- [libs/graph/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/graph/README.md)
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)

## 1. Signal Semantics

Two simulator-side value fields matter:

- `parameter_value_clean`
  - clean simulated value for truth/debugging
- `parameter_value`
  - observed value used by profiling, events, windows, structure fitting, scoring, and attribution

The fitting workflow operates on `parameter_value`, not `parameter_value_clean`, unless a validator explicitly compares against truth.

## 2. Design Rule

Datatype, scaling, and behavior identity are primarily **parameter metadata artifacts**, not per-window anomaly outputs.

The intended sequence is:

1. fit reusable parameter metadata
2. persist it
3. reuse it during structural fitting and inference

## 3. Sequential Fitting Stages

### 3.1 Datatype and sampling profile

Stage:
- `pipelines/10_parameter_profiles_fit.py`

Artifact:
- `parameter_datatype_profile`

Purpose:
- classify parameters
- estimate observed cadence
- gate later continuous-only structure paths

### 3.2 Continuous scaling profile

Artifact:
- `continuous_scaling_profile`

Purpose:
- define robust centering/scaling for continuous parameters
- provide the coordinate system used by window feature extraction and structural fitting

### 3.3 Behavior profile

Artifacts:
- `parameter_behavior_primitive_profile`
- `parameter_behavior_profile`

Purpose:
- derive primitive evidence from telemetry and then infer nominal behavior-family semantics
- support validation, routing, and future richer downstream behavior-aware logic

The current design is two-layer:

1. Spark derives bounded primitive evidence per parameter into `parameter_behavior_primitive_profile`
2. family scoring consumes that artifact to emit `parameter_behavior_profile`

The active family taxonomy is:

- `regulated`
- `tracking`
- `inertial`
- `accumulative`
- `discrete_state`
- `mixed_unknown`

### 3.4 Window feature extraction and backbone fitting

Artifacts:
- `window_features` as the many-window feature artifact
- `backbone`
- `backbone_sensor_energy`

The old `window_x` terminology has been replaced by the persisted `window_features` artifact and its per-window feature representation.

### 3.5 Graph, phase, and scoring stages

These consume upstream artifacts rather than recomputing parameter semantics:

- graph fitting consumes window features, events, and backbone artifacts
- phase fitting consumes window features and backbone context
- scoring consumes phase outputs and hierarchy structure

## 4. Inference-Time Use

Inference should normally reuse:

- `parameter_datatype_profile`
- `continuous_scaling_profile`
- `parameter_behavior_primitive_profile`
- `parameter_behavior_profile`

and should not continuously re-fit those semantics unless a separate adaptation path is intentionally added.

## 5. Notes

- For the mathematical interpretation of robust scaling, backbone fitting, graph weights, and phase fitting, see [theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/reference/theory_foundations.md).
- For current stage ownership and artifact flow, prefer the package READMEs over historical stage-level implementation notes.

## 6. Validation Harness

Grouped simulation runs now emit `reports/validation_harness_report.json` and `reports/validation_harness_report.md`.
Grouped simulation runs also emit `reports/objective_evaluation_report.json` and `reports/objective_evaluation_report.md`.
Grouped full runs also emit `reports/simulation_benchmark_audit_summary.json`, which classifies each truth fault window by observed recoverability (`module_recoverable`, `subsystem_recoverable`, `parameter_visible_only`, `detection_only`, `undetected`) under the current anomaly stack, compares that outcome against the recoverability target declared on the simulator's authored misbehavior windows, and includes `benchmark_phase_scorecards` so detection-, parameter-, module-, and subsystem-tier benchmark windows can be evaluated separately rather than mixed into one headline score.

The harness is intended to be the canonical tuning bundle for iterative model improvement. It joins:
- fitting parameters from the run manifest and per-stage manifests
- validation metrics from the full-run modeling reports
- compute performance from pipeline and per-stage engineering reports
- simulation context from the selected `FlightSpec`, including parameter inventory, behavior-family counts, hierarchy size, phase structure, misbehavior windows, nominal flight length, and stochastic profile/seed metadata

`libs/tuning` evaluates the harness through a mode-aware default `ObjectiveSpec`:
- `profile` mode emphasizes datatype and behavior fidelity
- `structural` mode emphasizes profile, event, and hierarchy quality
- `full` mode emphasizes end-to-end score and attribution quality

The objective report keeps primary terms and compute tie-breaks separate so tuning remains constrained and interpretable rather than collapsing everything into one opaque number too early.

Recommended comparison protocol:
1. Hold `flight_name`, `n_steps`, `dt_seconds`, pipeline mode, stochastic profile, and resolved sim seed constant.
2. Change a small set of stage-local fitting parameters.
3. Compare validation and compute together rather than optimizing them independently.
4. Promote a change only when the validation gain is worth the compute regression, or the compute win preserves model quality.
5. Use `scripts/profile_pipeline_performance.py` after the single-run harness identifies promising variants or bottleneck stages.
