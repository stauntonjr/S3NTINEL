# S3NTINEL Codebase

S3NTINEL stands for Structural Streaming Sparse Event Nexus for Telemetry Inference with Network Envelope Learning.

## Active architecture

The active path is now the V2 pipeline. Use [v2_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/v2_architecture.md) as the source of truth for current fitting/inference semantics.
Use [theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/theory_foundations.md) for the mathematical/statistical interpretation of the active representations, graph weights, and scoring quantities.
Use [glossary.md](/home/jrs/code/S3NTINEL/sentinel/docs/glossary.md) for the active code/data taxonomy and naming rules.
Use [avionics_simulation_guidelines.md](/home/jrs/code/S3NTINEL/sentinel/docs/avionics_simulation_guidelines.md) for domain guidance on avionics-system behavior, coupling, and simulation inputs.
Use [artifact_replay_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/artifact_replay_design.md) for artifact persistence, replay, cache, and MLflow lineage design.
Use [simulation_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_architecture.md) for the proposed next simulation architecture and extensibility model.
Use [behavior_profiling_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_profiling_design.md) for the behavior-profile artifact and the mirrored profiling design for simulation behavior semantics.
Use [fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/fitting_workflow.md) for the intended one-off fitting sequence for datatype profiling, robust scaling, behavior profiling, and backbone fitting.
Use [behavior_family_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_family_architecture.md) and [behavior_family_skeletons.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_family_skeletons.md) for the per-family file/class layout.
Use [misbehavior_taxonomy.md](/home/jrs/code/S3NTINEL/sentinel/docs/misbehavior_taxonomy.md) for the planned structured deviation/anomaly ontology.
Use [anomaly_injection_and_backbone_validation.md](/home/jrs/code/S3NTINEL/sentinel/docs/anomaly_injection_and_backbone_validation.md) for anomaly-injection design and backbone-fit validation guidance.

### Active fitting path

1. `pipelines/00_ingest_raw.py`
2. `pipelines/05_parameter_profiles_fit.py`
3. `pipelines/10_backbone_fit.py`
4. `pipelines/11_graph_fit.py`

Run with:

- `python -m pipelines.91_run_fitting_pipeline`

### Active inference path

1. `pipelines/20_events_extract.py`
2. `pipelines/30_windows_adaptive.py`
3. `pipelines/50_phase_fit.py`
4. `pipelines/60_window_scores_raw.py`
5. `pipelines/70_window_scores_calibrate.py`
6. `pipelines/80_anomaly_attribution.py`

Run with:

- `python -m pipelines.92_run_inference_pipeline`

### V2 artifacts

- `backbone`
- `backbone_sensor_energy`
- `phase_windows`
- `phase_baselines`
- `window_scores_raw`
- `window_scores_calibrated`
- `precision_graph`
- `event_graph`
- `lag_graph`
- `transition_graph`
- `fused_graph`
- `hierarchy_sensor_map`
  - graph components and `fused_graph` are now built in Spark; only the final
    hierarchy assignment remains driver-side on the pruned fused edge set

## Repo layout

- `conf/defaults.yaml`: runtime defaults and threshold settings.
- `pipelines/`: ordered job entrypoints (`00_...` through `80_...`).
- `libs/`: reusable domain modules (`backbone`, `graph`, `events`, `windows`, `phase`, `scoring`, `conformal`, `io`, `anomaly`).
- `notebooks/`: exploratory and validation notebooks.
- `scripts/`: local-to-AVD handoff helpers (bundle/patch export-import).
- `docs/v2_architecture.md`: architecture and contract source of truth.
- `docs/glossary.md`: active code/data taxonomy and naming conventions.
- `docs/theory_foundations.md`: theory-to-code map for active mathematical/statistical choices.
- `docs/avionics_simulation_guidelines.md`: domain guidance for realistic avionics simulation inputs and couplings.
- `docs/artifact_replay_design.md`: replayable stage artifacts, manifests, caches, and MLflow lineage policy.
- `docs/simulation_architecture.md`: proposed simulation architecture for an open-ended hierarchy.
- `docs/behavior_profiling_design.md`: behavior-profile artifact and mirrored behavior-profiling design for real telemetry.
- `docs/fitting_workflow.md`: intended one-off fitting workflow for datatype, scaling, behavior, and backbone artifacts.
- `docs/behavior_family_architecture.md`: proposed per-family package/file/class layout.
- `docs/behavior_family_skeletons.md`: concrete family skeletons for `regulated` and `inertial`.
- `docs/misbehavior_taxonomy.md`: planned structured misbehavior ontology and pipeline mapping.
- `docs/anomaly_injection_and_backbone_validation.md`: research note for simulator anomaly families and backbone-fit validation.
- `docs/v2_1_notes.md`: deferred notes for a future rate- and type-aware representation layer.

## Pipeline order

1. `pipelines/00_ingest_raw.py`
2. `pipelines/05_parameter_profiles_fit.py`
3. `pipelines/10_backbone_fit.py`
4. `pipelines/11_graph_fit.py`
5. `pipelines/20_events_extract.py`
6. `pipelines/30_windows_adaptive.py`
7. `pipelines/50_phase_fit.py`
8. `pipelines/60_window_scores_raw.py`
9. `pipelines/70_window_scores_calibrate.py`
10. `pipelines/80_anomaly_attribution.py`

## Quick start

- Install editable package: `pip install -e .`
- Install dev extras for local checks: `pip install -e .[dev]`
- Install Spark extras for Spark/Delta pipelines and Spark-backed tests: `pip install -e .[dev,spark]`
- Recommended Spark env spec: `conda env create -f environment.spark35.yml`
- Run any pipeline stage directly: `python -m pipelines.00_ingest_raw`
- Run fitting stages together (00 + 10) under one parent MLflow run: `python -m pipelines.91_run_fitting_pipeline`
- Run inference stages together (20 -> 80) under one parent MLflow run: `python -m pipelines.92_run_inference_pipeline`
- Run fitting: `python -m pipelines.91_run_fitting_pipeline`
- Run inference: `python -m pipelines.92_run_inference_pipeline`
- Run unit tests: `pytest`
- Run parameter profile fitting stage directly: `python -m pipelines.05_parameter_profiles_fit`
- Run V2 backbone fitting stage directly: `python -m pipelines.10_backbone_fit`
- Run V2 graph fitting stage directly: `python -m pipelines.11_graph_fit`
- Run full pipeline under one parent MLflow run: `python -m pipelines.90_run_full_pipeline`
- Pipeline module commands assume default table/input paths exist (for example `data/input/raw_telemetry`) or that `S3NTINEL_*` path environment variables are set.
- First-run example (bash):
	- `export S3NTINEL_RAW_INPUT_PATH=data/input/raw_telemetry`
	- `export S3NTINEL_RAW_TABLE_PATH=data/delta/raw_telemetry`
	- `export S3NTINEL_TABLE_FORMAT=parquet`
- Active Spark baseline: `sentinel-spark35` on Python `3.11` with Spark `3.5.1` and Delta `3.0.0`.
- Local smoke/default recommendation: use `sentinel-spark35` and `S3NTINEL_TABLE_FORMAT=parquet` unless your Spark runtime already has Delta JVM jars available. The `delta-spark` Python package alone is not sufficient for offline Delta writes.
- Spark bootstrap also supports:
	- `S3NTINEL_DELTA_JAR_PATH=/abs/path/to/delta.jar[,more.jar]`
	- `S3NTINEL_SPARK_EXTRA_JARS=/abs/path/to/extra.jar[,more.jar]`
	- `S3NTINEL_DELTA_ALLOW_MAVEN=false` to disable Maven fallback when you want local jars only
- Generate deterministic sample test data: `python -m scripts.generate_sample_data --base-dir data --mode overwrite`
- Run end-to-end smoke test (00->80, including `05_parameter_profiles_fit`): `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --min-warm 1`
- Smoke test now seeds a deterministic `sensor_subsystem_map` and asserts emitted anomaly quality gates: non-empty output, no duplicate `(tail_id, flight_id, win_id)`, at least one non-null `panel_context`, and at least one populated `subsystems[].top_sensors`.
- For stage-80 merge idempotence validation in smoke: `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format delta --min-warm 1 --write-mode merge`
- Merge smoke checks require a Spark runtime with Delta JVM classes available.
- Generate bucketed vs stream_parity window diagnostics in smoke: `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --compare-window-strategies`
- Run grouped fitting+inference per partition row from any manifest: `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/smoke/_partition_manifest --manifest-format parquet --job grouped --jobs-base-dir data/fleet_jobs_grouped --table-format parquet --write-mode overwrite`
- Run the simulation/event-detection evaluation harness: `python -m scripts.run_sim_detection_eval --output-json reports/eda/sim_detection_eval_report.json`
- Use handoff helpers for AVD transfer: see `scripts/README.md`
- Update defaults in `conf/defaults.yaml` and keep versioned changes in source control.
- CI workflow: `.github/workflows/ci.yml` runs tests on push/PR.

## Event taxonomy coverage

- Continuous: `extrema` (with payload `legacy_type=max|min`), `threshold`, `slope_pos|slope_neg`, `drift_guard`, `switch`, `oscillation`.
- Categorical: `state_enter`, `state_exit`, `transition`, `dropped`, `dwell_bucket`, `dwell_guard`, `dwell_violation`, `illegal_transition`.
- Graph artifacts: cooccurrence and precedence belong in graph outputs, not the active V2 detector event contract.
- Legacy name mapping: `CAT_CHANGE -> transition`, `DWELL_GUARD -> dwell_guard`, `EXTREMA/max/min -> extrema` with payload kind/legacy type.

## Simulation notes

- `libs/simulation/experiment_setup.py` simulator outputs now return telemetry + phase labels only (`simulate_fleet_dataset`, `simulate_fleet_dataset_spark`).
- Simulators keep explicit label metadata in telemetry: anomaly labels (`anomaly_type_label`, `anomaly_score_label`) and detector-event label (`event_type_label`).
- Event rows should be derived from telemetry via detector tooling (`pipelines/20_events_extract.py`, or `libs.events` builders).
- For system-level behavior, coupling, and dynamics priors, use [docs/avionics_simulation_guidelines.md](/home/jrs/code/S3NTINEL/sentinel/docs/avionics_simulation_guidelines.md).
- For the next extensible simulator design, use [docs/simulation_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_architecture.md).
- Optional causal delay realism is available in `flight_setup.causal_delay` via:
	- `mode` (`random_pair` default, or `fixed_group` for legacy behavior)
	- `default_lag_sec` (default baseline lag in seconds; defaults to `0`)
	- `random_pair_delay_sec` (`{"min": 0.0, "max": ...}` positive-only extra delay range per sensor)
	- `jitter_sec_std` (Gaussian jitter stddev in seconds; optional)
	- `jitter_cap_steps` (max absolute jitter clamp in sample steps; default `3`)
	- `seed_offset` (optional deterministic offset for per-flight pair-delay sampling)
	- `per_corr_group_sec` (map of `corr_group -> delay_seconds`, used as group baseline and for `fixed_group` mode)
	- `startup_fill` (`hold_first` default, or `hold_current`)

## Sample DataFrames for testing

- Modules: `libs.testing.data` and `libs.testing.seed`
- Includes builders for `raw_input`, `raw_telemetry`, `events`, `windows`, `phase_windows`, `window_scores_raw`, and `window_scores_calibrated`.
- Main helper: `seed_sample_dataset(spark, base_dir="data")` writes all sample datasets for local smoke tests.

## Stage I/O defaults

- `20_events_extract` continuous event typing:
	- `S3NTINEL_EVENT_DELTA_THRESHOLD` controls `threshold` event emission.
	- When `S3NTINEL_EVENT_DELTA_THRESHOLD <= 0`, `threshold` events are disabled and continuous deltas emit only `slope_pos|slope_neg` (plus first-sample null suppression).

- `10_backbone_fit` reads normalized telemetry and adaptive windows and writes backbone artifacts:
	- `S3NTINEL_BACKBONE_TABLE_PATH` (default `data/delta/backbone`)
	- `S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH` (default `data/delta/backbone_sensor_energy`)
	- key controls:
		- `S3NTINEL_BACKBONE_SENSOR_COUNT`
		- `S3NTINEL_BACKBONE_RIDGE_LAMBDA`
	- `11_graph_fit` writes:
		- `S3NTINEL_PRECISION_GRAPH_TABLE_PATH`
		- `S3NTINEL_EVENT_GRAPH_TABLE_PATH`
		- `S3NTINEL_LAG_GRAPH_TABLE_PATH`
		- `S3NTINEL_TRANSITION_GRAPH_TABLE_PATH`
		- `S3NTINEL_FUSED_GRAPH_TABLE_PATH`
		- `S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH`
		- `S3NTINEL_CUR_GRAPH_MIN_ABS_CORR` (default `graph.min_abs_corr`)
		- `S3NTINEL_CUR_NORMALIZATION_MODE` (default `graph.normalization.mode`, supports `none|zscore|robust`)
		- `S3NTINEL_CUR_NORMALIZATION_CLIP_SIGMA` (default `graph.normalization.clip_sigma`)
		- `S3NTINEL_CUR_NORMALIZATION_MIN_POINTS` (default `graph.normalization.min_sensor_points`)
		- If no sensors meet `S3NTINEL_CUR_NORMALIZATION_MIN_POINTS`, stage 10 automatically falls back to `1` point and records this in `reports/fitting_graph_report.json`.
		- `S3NTINEL_EVENT_GRAPH_MIN_COUNT` (default `graph.min_cooccur_count`)
		- `S3NTINEL_PRECISION_GRAPH_MIN_ABS_PARTIAL_CORR` (default `graph.min_abs_partial_corr`)
		- `S3NTINEL_PRECISION_GRAPH_RIDGE_LAMBDA` (default `graph.precision_ridge_lambda`)
		- `S3NTINEL_SUBSYSTEM_MIN_EDGE_WEIGHT` (default `graph.subsystem_min_edge_weight`)
		- Multi-level hierarchy cluster controls (Spark PIC):
			- `S3NTINEL_HIERARCHY_K_SYSTEM` (default `graph.hierarchy_k_system`)
			- `S3NTINEL_HIERARCHY_K_SUBSYSTEM` (default `graph.hierarchy_k_subsystem`)
			- `S3NTINEL_HIERARCHY_K_MODULE` (default `graph.hierarchy_k_module`)
		- `S3NTINEL_GRAPH_FUSE_ALPHA` (default `graph.cur_weight_alpha`)
- `50_phase_fit` writes:
	- `S3NTINEL_PHASE_WINDOWS_TABLE_PATH` (default `data/delta/phase_windows`)
	- `S3NTINEL_PHASE_BASELINES_TABLE_PATH` (default `data/delta/phase_baselines`)
- `60_window_scores_raw` reads phase windows + phase baselines and writes:
	- `S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH` (default `data/delta/window_scores_raw`)
	- Stage 60 also emits `subsystem_scores` (map of subsystem evidence ratios), propagated through calibration and used by stage 80 to populate `anomaly_window_attribution.subsystems`.
	- Severity thresholds are configurable for normalized score scale:
		- `S3NTINEL_SEVERITY_LOW_THRESHOLD` (default `0.25`)
		- `S3NTINEL_SEVERITY_MEDIUM_THRESHOLD` (default `0.75`)
		- `S3NTINEL_SEVERITY_HIGH_THRESHOLD` (default `1.50`)
- `70_window_scores_calibrate` reads raw window scores and writes:
	- `S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH` (default `data/delta/window_scores_calibrated`)
	- `S3NTINEL_MIN_WARM` (optional override; defaults to config value)
- `80_anomaly_attribution` reads calibrated window scores + phase windows + windows and writes:
	- `S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH` (default `data/delta/anomaly_window_attribution`)
	- `S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH` (default `data/delta/anomaly_telemetry_attribution`)
	- `S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH` (default `data/delta/anomaly_event_attribution`)
	- `S3NTINEL_WRITE_MODE` defaults to `merge` for this stage, enforcing upsert semantics on `(tail_id, flight_id, win_id)`.
	- Required inputs for V2 emission:
		- `S3NTINEL_EVENTS_TABLE_PATH`
		- `S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH`
		- `S3NTINEL_RAW_TABLE_PATH`
	- Stage 80 populates `subsystems[].top_sensors` from windowed event evidence joined through `hierarchy_sensor_map`.
	- Stage 80 populates `panel_context` from window-local ASCII/LCD text features.
	- `S3NTINEL_SUBSYSTEM_TOP_SENSORS_K` controls top sensors per subsystem in anomaly payload (default `5`).
- `scripts/smoke_test_pipeline.py` synthetic seed scaling options:
	- `--tail-count` (default `1`)
	- `--flights-per-tail` (default `1`)
	- `--sensor-count` (default `3`)
	- `--timestamp-count` (default `12`)
	- `--step-ms` (default `100`)
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

## Test fixtures

- Shared Spark fixtures live in `tests/conftest.py`:
	- `spark`: standard local SparkSession with pinned `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` and matching Spark configs.
	- `spark_delta`: Delta-enabled SparkSession that auto-skips when Delta JVM classes are unavailable.
- Spark-heavy regression tests should consume these shared fixtures instead of defining per-file SparkSession setup.
