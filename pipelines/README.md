# Pipelines

## Purpose

`pipelines/` contains the persisted stage entrypoints and grouped runners for the fitting and inference workflow.

It owns:
- stage ordering
- environment/config loading through `pipelines/common.py`
- persisted artifact writes
- stage manifests, MLflow logging, and wall-time logging

It does not own:
- the core domain logic for profiling, windows, graph, phase, scoring, or anomaly attribution
- simulation domain behavior

Those belong in `libs/*`.

## How To Use

Canonical grouped entrypoints:
- `python -m pipelines.90_run_full_pipeline`
- `python -m pipelines.91_run_fitting_pipeline`
- `python -m pipelines.92_run_inference_pipeline`

Canonical stage order:
1. `00_ingest_raw.py`
2. `05_parameter_profiles_fit.py`
3. `10_backbone_fit.py`
4. `11_build_graph.py`
5. `12_fit_hierarchy.py`
6. `20_events_extract.py`
7. `30_windows_adaptive.py`
8. `50_phase_fit.py`
9. `60_window_scores_raw.py`
10. `70_window_scores_calibrate.py`
11. `80_anomaly_attribution.py`

Grouped execution is coordinated by `pipelines/_pipeline_runner.py`.

## Contents

- `00`, `05`, `10`, `11`
  - fitting stages
- `20`, `30`, `50`, `60`, `70`, `80`
  - inference stages
- `90`, `91`, `92`
  - grouped pipeline entrypoints
- `common.py`
  - shared context/config setup
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
- Config defaults are loaded from `conf/defaults.yaml` and overridden by environment variables.
- Keep stage files thin; push algorithmic growth into `libs/*`.
