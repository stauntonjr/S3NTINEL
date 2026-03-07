# Scripts — Active Workflows and Handoff

This directory now contains only active V2 workflow scripts and repository handoff helpers.

## Handoff

Full-history bundle:

- local: `bash scripts/export_bundle.sh s3ntinel.bundle --all`
- target clone: `bash scripts/import_bundle.sh s3ntinel.bundle handoff`

Incremental patches:

- local: `bash scripts/export_patches.sh origin/main patches`
- target clone: `bash scripts/apply_patches.sh patches`

## Sample data and smoke

- Generate deterministic sample data:
  - `python -m scripts.generate_sample_data --base-dir data --mode overwrite`
- Run end-to-end smoke:
  - `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --min-warm 1`
  - this runs the active V2 stage sequence:
    - `00_ingest_raw.py`
    - `10_backbone_fit.py`
    - `11_graph_fit.py`
    - `20_events_extract.py`
    - `30_windows_adaptive.py`
    - `50_phase_fit.py`
    - `60_window_scores_raw.py`
    - `70_window_scores_calibrate.py`
    - `80_anomaly_attribution.py`
  - emits:
    - `reports/smoke_quality_report.json`
    - includes phase-detection accuracy/confusion if `phase_labels` are present
    - includes hierarchy exact-match diagnostics if `hierarchy_sensor_map_label` is present
- Compare `bucketed` vs `stream_parity` windows during smoke:
  - `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --compare-window-strategies`
- Sweep graph/hierarchy settings against the smoke pipeline:
  - `python -m scripts.sweep_smoke_graph_hierarchy --base-dir data/smoke_sweep --format parquet --min-warm 1`
  - if your current shell interpreter is not the active repo env, run:
    - `python -m scripts.sweep_smoke_graph_hierarchy --conda-env sentinel-spark35 --base-dir data/smoke_sweep --format parquet --min-warm 1`
  - writes:
    - `graph_hierarchy_sweep_summary.json`

## Active V2 pipelines

Fitting:

- `python -m pipelines.91_run_fitting_pipeline`
- stages:
  - `00_ingest_raw.py`
  - `10_backbone_fit.py`
  - `11_graph_fit.py`

Inference:

- `python -m pipelines.92_run_inference_pipeline`
- stages:
  - `20_events_extract.py`
  - `30_windows_adaptive.py`
  - `50_phase_fit.py`
  - `60_window_scores_raw.py`
  - `70_window_scores_calibrate.py`
  - `80_anomaly_attribution.py`

## Partition-manifest runner

Run built-in per-flight pipeline stages from a partition manifest:

- `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/smoke/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_jobs --table-format parquet --min-warm 1`

Run grouped fitting + inference from a partition manifest:

- `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/smoke/_partition_manifest --manifest-format parquet --job grouped --jobs-base-dir data/fleet_jobs_grouped --table-format parquet --write-mode overwrite --min-warm 1`

Optional row controls:

- `--tail-id ... --flight-id ... --limit 1 --continue-on-error --dry-run`

## Simulation evaluation

- Run the simulation/event-detection evaluation harness:
  - `python -m scripts.run_sim_detection_eval --output-json reports/eda/sim_detection_eval_report.json`

## Notes

- Script entrypoints are intended to be run as modules from the repo root.
- Active Spark baseline is `sentinel-spark35`.
- Prefer `S3NTINEL_TABLE_FORMAT=parquet` for local smoke unless Delta JVM jars are available.
