# S3NTINEL Codebase

S3NTINEL stands for Structural Streaming Sparse Event Nexus for Telemetry Inference with Network Envelope Learning.

## Repo layout

- `conf/defaults.yaml`: v1 runtime defaults and threshold settings.
- `pipelines/`: ordered job entrypoints (`00_...` through `80_...`).
- `libs/`: reusable domain modules (`cur`, `events`, `windows`, `signature`, `phase`, `scoring`, `conformal`, `io`).
- `notebooks/`: exploratory and validation notebooks.
- `scripts/`: local-to-AVD handoff helpers (bundle/patch export-import).
- `Structural_Anomaly_System_Architecture.md`: architecture and spec lock.
- `Architecture_Diagrams.md`: technical mermaid diagrams.
- `S3NTINEL_Presentation_Diagrams.md`: stakeholder-friendly diagrams.

## Pipeline order

1. `pipelines/00_ingest_raw.py`
2. `pipelines/10_cur_backbone_fit.py`
3. `pipelines/20_events_extract.py`
4. `pipelines/30_windows_adaptive.py`
5. `pipelines/40_signatures_build.py`
6. `pipelines/50_phase_detect.py`
7. `pipelines/60_anomaly_score.py`
8. `pipelines/70_conformal_calibrate.py`
9. `pipelines/80_emit_anomalies.py`

## Quick start

- Install editable package: `pip install -e .`
- Install dev extras for local checks: `pip install -e .[dev]`
- Run any pipeline stage directly: `python -m pipelines.00_ingest_raw`
- Run unit tests: `pytest`
- Run fitting-phase graph fusion (CUR-proxy + event cooccur + fused graph): `python -m pipelines.10_cur_backbone_fit`
- Run full pipeline under one parent MLflow run: `python -m pipelines.90_run_full_pipeline`
- Pipeline module commands assume default table/input paths exist (for example `data/input/raw_telemetry`) or that `S3NTINEL_*` path environment variables are set.
- First-run example (bash):
	- `export S3NTINEL_RAW_INPUT_PATH=data/input/raw_telemetry`
	- `export S3NTINEL_RAW_TABLE_PATH=data/delta/raw_telemetry`
	- `export S3NTINEL_TABLE_FORMAT=parquet`
- Generate deterministic sample test data: `python -m scripts.generate_sample_data --base-dir data --mode overwrite`
- Run end-to-end smoke test (00->80): `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --min-warm 1`
- Generate bucketed vs stream_parity window diagnostics in smoke: `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --compare-window-strategies`
- Profile telemetry parameters + channel routing: `python -m scripts.profile_telemetry --input-path data/sample/raw_input --input-format parquet --output-dir data/profile`
- Generate synthetic hierarchy artifacts (global/system/subsystem/module/sensor map) for simulation-only correlation injection: `python -m scripts.generate_synthetic_hierarchy_profile --profile-parameter-profile-path data/profile/parameter_profile --profile-format parquet --output-dir data/profile_hierarchy --hierarchy-profile-id HIER_SYNTH_V1 --system-count 3 --subsystems-per-system 2 --modules-per-subsystem 3`
- Generate fleet-scoped profiles (`m` tails x `n` flights) with controlled variance: `python -m scripts.generate_fleet_profiles --base-parameter-profile-path data/profile/parameter_profile --base-categorical-distribution-path data/profile/categorical_distribution --tail-count 3 --flights-per-tail 2 --output-dir data/profile_fleet`
- Add hierarchy-aware correlation offsets during fleet profile generation (optional): append `--hierarchy-sensor-map-path data/profile_hierarchy/sensor_hierarchy_map --hier-correlation-preset medium` and, if needed, override per-knob hierarchy values such as `--hier-mean-system-std-ratio 0.03 --hier-mean-subsystem-std-ratio 0.015 --hier-mean-module-std-ratio 0.01`
- Generate synthetic normal telemetry: `python -m scripts.generate_synthetic_normal --output-path data/synthetic/raw_telemetry`
- Generate synthetic telemetry for an entire fleet manifest: `python -m scripts.generate_synthetic_fleet --fleet-manifest-path data/profile_fleet/fleet_manifest --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-format parquet --output-path data/synthetic/fleet_raw_telemetry`
- Generate partitioned per-flight fleet telemetry + manifest: `python -m scripts.generate_synthetic_fleet --fleet-manifest-path data/profile_fleet/fleet_manifest --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-format parquet --output-path data/synthetic/fleet_partitioned --emit-manifest-partitions`
- Run downstream pipeline/evaluation jobs per partition row: `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/synthetic/fleet_partitioned/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_jobs`
- Safety contract: keep synthetic hierarchy artifacts (`hierarchy_source=synthetic_injected`) separate from discovered hierarchy outputs; never use injected hierarchy artifacts as learned/discovered structure.
- Score hierarchy recovery difficulty across presets (`easy,medium,hard`) for CUR-graph validation proxy: `conda run -n sentinel --no-capture-output python -m scripts.evaluate_hierarchy_recovery --base-parameter-profile-path data/fleet_seed/profile/parameter_profile --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map --profile-format parquet --presets easy,medium,hard --tail-count 2 --flights-per-tail 2 --duration-seconds 180 --max-corr-sensors 32 --output-json reports/hierarchy_recovery_metrics.json`
- Score hierarchy recovery from existing partition telemetry outputs (no inline generation): append `--telemetry-partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --telemetry-manifest-format parquet --telemetry-format parquet`
- Sentinel-validated hierarchy orchestration flow (copy/paste with `conda run -n sentinel`):
	- `conda run -n sentinel --no-capture-output python -m scripts.generate_synthetic_hierarchy_profile --profile-parameter-profile-path data/fleet_seed/profile/parameter_profile --profile-format parquet --output-dir data/fleet_seed/profile_hierarchy_e2e --output-format parquet --hierarchy-profile-id HIER_SYNTH_E2E --system-count 3 --subsystems-per-system 2 --modules-per-subsystem 3 --seed 7`
	- `conda run -n sentinel --no-capture-output python -m scripts.generate_fleet_profiles --base-parameter-profile-path data/fleet_seed/profile/parameter_profile --base-categorical-distribution-path data/fleet_seed/profile/categorical_distribution --input-format parquet --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map --output-dir data/fleet_seed/profile_fleet_hier_e2e --output-format parquet --tail-count 2 --flights-per-tail 2 --seed 11 --hier-mean-system-std-ratio 0.03 --hier-mean-subsystem-std-ratio 0.015 --hier-mean-module-std-ratio 0.01 --hier-std-system-std-ratio 0.02 --hier-std-subsystem-std-ratio 0.01 --hier-std-module-std-ratio 0.005 --hier-rate-system-std-ratio 0.03 --hier-rate-subsystem-std-ratio 0.015 --hier-rate-module-std-ratio 0.01 --hier-missing-system-std 0.02 --hier-missing-subsystem-std 0.01 --hier-missing-module-std 0.005`
	- `conda run -n sentinel --no-capture-output python -m scripts.generate_synthetic_fleet --fleet-manifest-path data/fleet_seed/profile_fleet_hier_e2e/fleet_manifest --profile-parameter-profile-path data/fleet_seed/profile_fleet_hier_e2e/parameter_profile --profile-categorical-distribution-path data/fleet_seed/profile_fleet_hier_e2e/categorical_distribution --profile-format parquet --output-format parquet --output-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e --emit-manifest-partitions --partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --duration-seconds 120 --seed 23`
	- `conda run -n sentinel --no-capture-output python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_seed/fleet_jobs_hier_e2e --table-format parquet --min-warm 1 --limit 4`
	- Note: `scripts.generate_synthetic_fleet` does not accept `--rate-hz`.
- Run generator-based synthetic + EMA/switch/oscillation detector (no Spark): `python -m scripts.stream_synthetic_events_demo --duration-seconds 180 --rate-hz 20`
- Run profile-driven mixed-type stream demo + window cooccurrence: `python -m scripts.stream_synthetic_events_demo --profile-json conf/demo_stream_profile.json --duration-seconds 180 --emit-extrema-events --emit-cooccur-events`
- Run stream demo from profiling outputs directly: `python -m scripts.stream_synthetic_events_demo --profile-parameter-profile-path data/profile_smoke/profile/parameter_profile --profile-categorical-distribution-path data/profile_smoke/profile/categorical_distribution --profile-format parquet --duration-seconds 180 --emit-extrema-events --emit-cooccur-events`
- Run stream demo from fleet-scoped profile tables: `python -m scripts.stream_synthetic_events_demo --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-tail-id FLEET_T001 --profile-flight-id FL001 --profile-format parquet --duration-seconds 180 --emit-extrema-events --emit-cooccur-events`
- Evaluate stream detectors vs synthetic truth (precision/recall): `python -m scripts.evaluate_stream_event_detection --duration-seconds 300 --tolerance-seconds 0.5`
- Evaluate stream detectors from profile JSON/table inputs: `python -m scripts.evaluate_stream_event_detection --profile-json conf/demo_stream_profile.json --duration-seconds 300 --event-types transition,dropped,oscillation,switch`
- Evaluate from fleet-scoped profile tables: `python -m scripts.evaluate_stream_event_detection --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-tail-id FLEET_T001 --profile-flight-id FL001 --profile-format parquet --duration-seconds 300 --event-types transition,dropped,oscillation,switch`
- Use handoff helpers for AVD transfer: see `scripts/README.md`
- Update defaults in `conf/defaults.yaml` and keep versioned changes in source control.
- CI workflow: `.github/workflows/ci.yml` runs tests on push/PR.

## Event taxonomy coverage (legacy -> current)

- Continuous: `extrema` (with payload `legacy_type=max|min`), `threshold`, `slope_pos|slope_neg`, `drift_guard`, `switch`, `oscillation`.
- Categorical: `state_enter`, `state_exit`, `transition`, `dropped`, `dwell_bucket`, `dwell_guard`, `dwell_violation`, `illegal_transition`.
- Co-occurrence: `cooccur` remains available in Spark batch event construction (`libs/events/cooccur.py`).
- Stream evaluation also emits `cooccur` from detected events in a configurable short window for graph population diagnostics.
- Legacy name mapping: `CAT_CHANGE -> transition`, `DWELL_GUARD -> dwell_guard`, `EXTREMA/max/min -> extrema` with payload kind/legacy type.

## Sample DataFrames for testing

- Module: `libs.testing.sample_data`
- Includes builders for `raw_input`, `raw_telemetry`, `events`, `windows`, `signatures`, `phase_windows`, `scores`, and `calibrated`.
- Main helper: `seed_sample_dataset(spark, base_dir="data")` writes all sample datasets for local smoke tests.

## Stage I/O defaults

- `10_cur_backbone_fit` reads normalized telemetry and writes fitting-phase graph artifacts:
	- Reads `S3NTINEL_RAW_TABLE_PATH` (default `data/delta/raw_telemetry`)
	- Writes `S3NTINEL_CUR_NORMALIZATION_TABLE_PATH` (default `data/delta/cur_normalization_profile`)
	- Writes CUR matrix artifacts:
		- `S3NTINEL_CUR_COLUMN_SKETCH_TABLE_PATH` (default `data/delta/cur_column_sketch`)
		- `S3NTINEL_CUR_COLUMN_LEVERAGE_TABLE_PATH` (default `data/delta/cur_column_leverage`)
		- `S3NTINEL_CUR_ROW_SKETCH_TABLE_PATH` (default `data/delta/cur_row_sketch`)
		- `S3NTINEL_CUR_SENSOR_SAMPLE_TABLE_PATH` (default `data/delta/cur_sensor_sample`)
		- `S3NTINEL_CUR_ROW_SAMPLE_TABLE_PATH` (default `data/delta/cur_row_sample`)
		- `S3NTINEL_CUR_C_MATRIX_TABLE_PATH` (default `data/delta/cur_c_matrix`)
		- `S3NTINEL_CUR_R_MATRIX_TABLE_PATH` (default `data/delta/cur_r_matrix`)
		- `S3NTINEL_CUR_W_MATRIX_TABLE_PATH` (default `data/delta/cur_w_matrix`)
		- `S3NTINEL_CUR_U_MATRIX_TABLE_PATH` (default `data/delta/cur_u_matrix`)
		- `S3NTINEL_CUR_PIVOTS_K` (default `cur.pivots_k`)
		- `S3NTINEL_CUR_ROW_SAMPLES_K` (default `cur.row_samples_k`)
		- `S3NTINEL_CUR_SAMPLING_MODE` (default `cur.sampling_mode`, supports `deterministic|weighted`)
		- `S3NTINEL_CUR_SAMPLING_SEED` (default `cur.sampling_seed`)
		- `S3NTINEL_CUR_MAX_CORE_CELLS` (default `cur.max_core_cells`)
		- `S3NTINEL_CUR_MIN_CORE_ROWS` (default `cur.min_core_rows`)
		- `S3NTINEL_CUR_MIN_CORE_COLS` (default `cur.min_core_cols`)
	- Writes `S3NTINEL_CUR_GRAPH_TABLE_PATH` (default `data/delta/cur_sensor_graph`)
	- Writes `S3NTINEL_EVENT_GRAPH_TABLE_PATH` (default `data/delta/event_cooccurrence_graph`)
	- Writes `S3NTINEL_FUSED_GRAPH_TABLE_PATH` (default `data/delta/fused_sensor_graph`)
	- Writes fitting quality report `S3NTINEL_FIT_GRAPH_REPORT_PATH` (default `reports/fitting_graph_report.json`)
	- A/B evaluator: `python -m scripts.evaluate_cur_sampling_ab` compares `deterministic|weighted` sampling across seeds and writes aggregate report metrics.
	- Threshold/fusion controls:
		- `S3NTINEL_CUR_GRAPH_MAX_SENSORS` (default `cur.pivots_k`)
		- `S3NTINEL_CUR_GRAPH_MIN_OVERLAP` (default `graph.min_overlap`)
		- `S3NTINEL_CUR_GRAPH_MIN_ABS_CORR` (default `graph.min_abs_corr`)
		- `S3NTINEL_CUR_NORMALIZATION_MODE` (default `graph.normalization.mode`, supports `none|zscore|robust`)
		- `S3NTINEL_CUR_NORMALIZATION_CLIP_SIGMA` (default `graph.normalization.clip_sigma`)
		- `S3NTINEL_CUR_NORMALIZATION_MIN_POINTS` (default `graph.normalization.min_sensor_points`)
		- If no sensors meet `S3NTINEL_CUR_NORMALIZATION_MIN_POINTS`, stage 10 automatically falls back to `1` point and records this in `reports/fitting_graph_report.json`.
		- `S3NTINEL_EVENT_GRAPH_MIN_COUNT` (default `graph.min_cooccur_count`)
		- `S3NTINEL_GRAPH_FUSE_ALPHA` (default `graph.cur_weight_alpha`)
- `50_phase_detect` writes:
	- `S3NTINEL_PHASE_WINDOWS_TABLE_PATH` (default `data/delta/phase_windows`)
	- `S3NTINEL_PHASES_TABLE_PATH` (default `data/delta/phases`)
- `60_anomaly_score` reads phase windows + signatures and writes:
	- `S3NTINEL_SCORES_TABLE_PATH` (default `data/delta/scores`)
- `70_conformal_calibrate` reads scores and writes:
	- `S3NTINEL_CALIBRATED_TABLE_PATH` (default `data/delta/calibrated`)
	- `S3NTINEL_MIN_WARM` (optional override; defaults to config value)
- `80_emit_anomalies` reads calibrated + phase windows + signatures + windows and writes:
	- `S3NTINEL_ANOMALIES_TABLE_PATH` (default `data/delta/anomalies`)
- `30_windows_adaptive` strategy:
	- `S3NTINEL_WINDOW_STRATEGY` supports `bucketed` (legacy) and `stream_parity` (stateful max_ms/event_threshold parity with stream windower)
	- `S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS` controls timeout-based closure in `stream_parity` mode (default `0` = disabled)

## v1 conventions

- Canonical naming: first `N` is `Nexus`, second `N` is `Network`.
- Output partitioning: `tail_id`, `flight_id`, `date_utc`.
- Anomaly identity key: `tail_id`, `flight_id`, `win_id`.
- Conformal warm-up: hold emissions until warm, then flush backlog with original timestamps.
- Performance annotations: use `@hot_path` from `libs.perf.annotations` for machine-discoverable critical functions.
- Runtime timing logs: use `@log_wall_time` and `get_logger` from `libs.perf` to emit wall-clock execution metrics.

## MLflow integration

- Pipeline `run()` functions are decorated with `@track_mlflow_run(...)` for stage-level run tracking.
- `pipelines/90_run_full_pipeline.py` creates a parent run; each stage appears as a nested child run.
- Wall-time metrics are logged to both logger output and active MLflow runs.
- Parent run summary is logged as `reports/pipeline_run_summary.json` with per-stage status and elapsed time.
- Use helpers in `libs.perf.mlflow` for additional tracking:
	- `log_params_if_active(...)`
	- `log_metric_if_active(...)`
	- `log_dict_artifact_if_active(...)`
	- `log_artifact_if_active(...)`
	- `register_model_if_available(...)`

These helpers no-op when MLflow is not available, allowing local development without Databricks dependencies.
