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
- `pipelines/05_parameter_profiles_fit.py`

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

Artifact:
- `parameter_behavior_profile`

Purpose:
- infer nominal behavior-family semantics
- support validation, routing, and future richer downstream behavior-aware logic

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
- `parameter_behavior_profile`

and should not continuously re-fit those semantics unless a separate adaptation path is intentionally added.

## 5. Notes

- For the mathematical interpretation of robust scaling, backbone fitting, graph weights, and phase fitting, see [theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/theory_foundations.md).
- For current stage ownership and artifact flow, prefer the package READMEs over historical stage-level implementation notes.
