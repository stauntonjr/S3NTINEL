# Pipelines

## Purpose

`pipelines/` contains the persisted stage entrypoints and grouped runners for the fitting and inference workflow.

It owns:
- stage ordering
- environment/config loading through `pipelines/common.py` and `libs/config/`
- persisted artifact writes
- stage manifests, MLflow logging, and wall-time logging

It does not own:
- the core domain logic for profiling, windows, graph, phase, scoring, or anomaly attribution
- simulation domain behavior

Those belong in `libs/*`.

## How To Use

Canonical grouped entrypoints:
- `python -m pipelines.99_run_full_pipeline`
- `python -m pipelines.97_run_fitting_pipeline`
- `python -m pipelines.98_run_inference_pipeline`

Canonical stage order:
1. `00_ingest_raw.py`
2. `10_parameter_profiles_fit.py`
3. `20_events_extract.py`
4. `30_windows_adaptive.py`
5. `40_backbone_fit.py`
6. `50_build_graph.py`
7. `60_fit_hierarchy.py`
8. `70_phase_fit.py`
9. `80_window_scores_raw.py`
10. `85_window_scores_calibrate.py`
11. `90_anomaly_attribution.py`
12. `95_emit_explorer_bundle.py`

Grouped execution is coordinated by `pipelines/_pipeline_runner.py`.

## Contents

- `00`, `10`, `20`, `30`, `40`, `50`, `60`, `70`, `80`, `85`, `90`
  - persisted stage entrypoints
- `97`, `98`, `99`
  - grouped pipeline entrypoints
- `common.py`
  - shared pipeline context assembly
- `libs/config/`
  - typed runtime/artifact/tuning config loaders consumed by the pipeline context
- `_pipeline_runner.py`
  - grouped execution, summaries, and run orchestration

## Model / Concepts

The stage model is:
- ingest canonical raw telemetry
- fit reusable structural artifacts
- infer events and windows
- fit phase structure
- compute raw and calibrated scores
- attribute anomalies

Stages are intentionally thin wrappers around `libs/*` code.

`50_build_graph.py` now emits both a first-class `lag_profile` artifact and the
collapsed legacy `lag_graph` compatibility view.

## Data / Artifacts

Stages read and write persisted artifacts defined by [`libs/io/schemas/`](./../libs/io/schemas/README.md) and stage path env vars.

Every stage should emit:
- persisted output tables
- a stage summary JSON
- a stage manifest with input/output artifact inventories and row counts

Grouped runs should emit:
- `reports/pipeline_run_summary.json`
- per-stage manifests under `reports/stages/`
- MLflow metrics where configured
- wall-time logs

## Math / Methods

The math lives in the owning library packages:
- profiling, backbone, graph, phase, scoring, anomaly

This directory is about stage composition and replayable persisted execution, not algorithm ownership.

## Subject Matter View

These pipelines turn telemetry into:
- parameter profiles
- event and window structure
- graph and phase models
- calibrated anomaly scores
- attribution outputs for operators and validation

## Testing / Validation

- Stage and grouped-run behavior is covered under [`tests/integration/pipelines/`](./../tests/integration/pipelines/) and [`tests/integration/runner/`](./../tests/integration/runner/).
- Simulation-backed end-to-end validation uses `scripts.run_sim_pipeline`.

## Notes / Constraints

- Stage logging is expected to be consistent: MLflow plus wall-time decorators.
- Checked-in defaults come from `conf/defaults.yaml`, but the runtime config surface is resolved through `libs/config/pipeline.py`.
- Keep stage files thin; push algorithmic growth into `libs/*`.
