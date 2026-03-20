# Scripts — Canonical Entry Points and Utilities

## Purpose

This directory contains the repo's operational command-line surface:
- canonical entrypoints
- developer utilities
- handoff helpers

It does not own the core domain logic. That lives in `libs/`.

## How To Use

The canonical operational script surface is intentionally small. Prefer the grouped pipeline or simulation runner entrypoints over ad hoc utility scripts.

## Handoff

Full-history bundle:

- local: `bash scripts/export_bundle.sh s3ntinel.bundle --all`
- target clone: `bash scripts/import_bundle.sh s3ntinel.bundle handoff`

Incremental patches:

- local: `bash scripts/export_patches.sh origin/main patches`
- target clone: `bash scripts/apply_patches.sh patches`

## Canonical Simulation

- Run the canonical simulation pipeline into one persisted run bundle:
  - `python -m scripts.run_sim_pipeline --flight-name power_chain --base-dir data/simulation_runs --mode full --format parquet`
  - this emits canonical raw telemetry plus simulation truth metadata, then runs the real persisted fitting and inference stages into one run directory containing:
    - input raw telemetry
    - fitted/profiled tables
    - persisted `window_policy_profile`
    - persisted `window_features`
    - structural graph artifacts, including `lag_profile` and the collapsed compatibility `lag_graph`
    - phase/scoring/attribution outputs
    - local stage summaries and manifests under `reports/`
    - a run manifest at `reports/run_manifest.json`
    - a consolidated console log at `logs/run.log`
- Realistic hierarchy presets are available through the same entrypoint:
  - `power_pressurization_hierarchy_smoke`
  - `power_pressurization_hierarchy_medium`
  - `power_pressurization_hierarchy_composite`
  - the realistic preset family uses a `28` minute authored mission on a `0.5` second internal tick, sparse multi-rate emission, parameter and coupling misbehavior truth, and writes `reports/coupling_validation_summary.json` alongside the existing validation reports
- For larger local full runs on a laptop, set `S3NTINEL_SPARK_PROFILE=laptop_large_sim` before invoking the runner.
  - the profile currently applies `local[4]`, `spark.driver.memory=8g`, `spark.driver.maxResultSize=2g`, `spark.sql.shuffle.partitions=16`, `spark.default.parallelism=8`, adaptive execution, Kryo serialization, and a dedicated `/tmp` spill directory
  - explicit `S3NTINEL_SPARK_*` env vars still override the profile when you need to tune around a specific machine or workload
  - if you also want the benchmark-winning larger sequence segments, use:
    - `S3NTINEL_SPARK_PROFILE=laptop_large_sim_large_segments`
    - this keeps the same Spark runtime settings as `laptop_large_sim` and additionally applies:
      - event segments: `100000` rows / `1800000` ms
      - window segments: `100000` rows / `1800000` ms
      - phase segments: `10000` rows / `3600000` ms
- Profile semantics-preserving performance variants of the canonical simulation pipeline:
  - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles`
  - run a single named variant when you want a targeted check instead of the whole quick sweep:
    - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --variant baseline --variant all_small_segments`
  - the benchmark runner executes the canonical simulation pipeline repeatedly with different sequence-segmentation overrides and writes:
    - per-variant child run bundles under `runs/`
    - `reports/performance_profile_summary.json`
    - `reports/performance_profile_summary.md`
  - variant failures are recorded in the summary by default; the run only exits non-zero if every variant fails
  - pass `--fail-on-variant-error` if you want any failed variant to make the benchmark command fail
  - TODO: this profiler still compares tuning variants on fixed workloads; dataset-size scale sweep is a planned follow-up
  - the built-in quick sweep compares:
    - baseline
    - moderately smaller event/window/phase segments
    - larger event/window/phase segments
  - segment tuning uses these semantics-preserving env overrides:
    - `S3NTINEL_EVENT_SEGMENT_MAX_ROWS`
    - `S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS`
    - `S3NTINEL_WINDOW_SEGMENT_MAX_ROWS`
    - `S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS`
    - `S3NTINEL_PHASE_SEGMENT_MAX_ROWS`
    - `S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS`
- Run the bounded pre-harness continuous-event calibration pass against the current raw/profile artifacts:
  - `python -m scripts.calibrate_continuous_events`
  - optional bounded grid overrides:
    - `--slope-sources ema,raw`
    - `--ema-alphas 0.2,0.35,0.5`
    - `--slope-abs-thresholds 0.0,0.5,1.0`
  - writes:
    - `reports/continuous_event_calibration.json`

## Developer Utilities

These are useful for local development and regression checks, but they are not the canonical production/simulation entrypoint.

### Sample data and smoke

- Generate deterministic sample data:
  - `python -m scripts.generate_sample_data --base-dir data --mode overwrite`
- Run end-to-end smoke:
  - `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --min-warm 1`
  - this runs the active V2 stage sequence:
    - `00_ingest_raw.py`
    - `10_parameter_profiles_fit.py`
    - `20_events_extract.py`
    - `25_window_policy_profile.py`
    - `30_windows_adaptive.py`
    - `40_backbone_fit.py`
    - `50_build_graph.py`
    - `60_fit_hierarchy.py`
    - `70_phase_fit.py`
    - `80_window_scores_raw.py`
    - `85_window_scores_calibrate.py`
    - `90_anomaly_attribution.py`
  - emits:
    - `reports/smoke_quality_report.json`
    - includes phase-detection accuracy/confusion if `phase_labels` are present
    - includes canonical hierarchy recovery metrics if `hierarchy_sensor_map_label` is present, including exact match, pairwise F1, and ARI
- Smoke runs now use the canonical segmented window builder:
  - `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet`
- Sweep graph/hierarchy settings against the smoke pipeline:
  - `python -m scripts.sweep_smoke_graph_hierarchy --base-dir data/smoke_sweep --format parquet --min-warm 1`
  - if your current shell interpreter is not the active repo env, run:
    - `python -m scripts.sweep_smoke_graph_hierarchy --conda-env sentinel-spark35 --base-dir data/smoke_sweep --format parquet --min-warm 1`
  - writes:
    - `graph_hierarchy_sweep_summary.json`

## Persisted Pipelines

Fitting:

- `python -m pipelines.97_run_fitting_pipeline`
- stages:
  - `00_ingest_raw.py`
  - `10_parameter_profiles_fit.py`
  - `20_events_extract.py`
  - `25_window_policy_profile.py`
  - `30_windows_adaptive.py`
  - `40_backbone_fit.py`
  - `50_build_graph.py`
  - `60_fit_hierarchy.py`

Inference:

- `python -m pipelines.98_run_inference_pipeline`
- stages:
  - `70_phase_fit.py`
  - `80_window_scores_raw.py`
  - `85_window_scores_calibrate.py`
  - `90_anomaly_attribution.py`
  - `95_emit_explorer_bundle.py`

## Partition-Manifest Jobs

Run built-in per-flight pipeline stages from a partition manifest:

- `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/smoke/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_jobs --table-format parquet --min-warm 1`

Run grouped fitting + inference from a partition manifest:

- `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/smoke/_partition_manifest --manifest-format parquet --job grouped --jobs-base-dir data/fleet_jobs_grouped --table-format parquet --write-mode overwrite --min-warm 1`

Optional row controls:

- `--tail-id ... --flight-id ... --limit 1 --continue-on-error --dry-run`

## Notes

- Script entrypoints are intended to be run as modules from the repo root.
- Active Spark baseline is `sentinel-spark35`.
- Prefer `S3NTINEL_TABLE_FORMAT=parquet` for local smoke unless Delta JVM jars are available.
- `scripts.run_sim_pipeline` is the canonical simulation entrypoint.
- `scripts.smoke_test_pipeline` and `scripts.sweep_smoke_graph_hierarchy` are developer utilities, not the primary operational path.
- `scripts.window_diagnostics` is a support utility for windowing diagnostics, not a shared library surface.
