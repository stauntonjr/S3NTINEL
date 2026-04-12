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
    - `reports/simulation_benchmark_audit_summary.json`, which summarizes observed fault-window recoverability for the current simulator bundle and compares it against the recoverability targets declared on the flight's authored misbehavior windows
    - local stage summaries and manifests under `reports/`
    - a run manifest at `reports/run_manifest.json`
    - a consolidated console log at `logs/run.log`
- Realistic hierarchy presets are available through the same entrypoint:
  - `power_pressurization_hierarchy_smoke`
  - `power_pressurization_hierarchy_medium`
  - `power_pressurization_hierarchy_composite`
  - a narrower localization-sanity pack is also available on the smoke topology:
    - `power_pressurization_hierarchy_smoke_localization_focus`
    - `power_pressurization_hierarchy_smoke_localization_focus_bias_drift`
    - `power_pressurization_hierarchy_smoke_localization_focus_saturation`
    - `power_pressurization_hierarchy_smoke_localization_focus_saturation_local`
  - current smoke-benchmark intent:
    - `bias_drift` is the cleaner module-localization family
    - `saturation` is parameter-visible-only
    - `saturation_local` is detection-only
  - filtered benchmark packs over the same authored composite scenario are also available:
    - `power_pressurization_hierarchy_composite_module_localization`
    - `power_pressurization_hierarchy_composite_subsystem_localization`
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
  - replay-aware late-stage benchmarking is also available when you already have a source run bundle:
    - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles --replay-source-run-dir data/simulation_runs/<run_dir> --replay-target-stage 50_build_graph.py`
    - this clones the source run into each benchmark repeat directory, asks the replay planner for the cheapest valid boundary to the target stage, and resumes from there instead of launching a fresh full run
    - you can also specify the minimal downstream closure you want to evaluate:
      - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles --replay-source-run-dir data/simulation_runs/<run_dir> --evaluation-tier structural`
      - supported tiers: `profile`, `event`, `structural`, `phase`, `scoring`, `anomaly`, `full`
      - if `--replay-target-stage` is omitted, the script infers the earliest impacted stage from the changed knobs and then extends the run only as far as the requested evaluation tier requires
    - or drive replay closure from the exact tuning objective instead of a manual tier:
      - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles --replay-source-run-dir data/simulation_runs/<run_dir> --objective-name sim_structural_default_v1`
      - supported objective names currently mirror the built-in default simulation objectives:
        - `sim_profile_default_v1`
        - `sim_event_default_v1`
        - `sim_structural_default_v1`
        - `sim_full_default_v1`
      - when both `--objective-name` and `--evaluation-tier` are provided, they must agree
    - or choose a named repo-level objective preset:
      - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles --replay-source-run-dir data/simulation_runs/<run_dir> --objective-preset event_recall_heavy`
      - current presets are defined in `libs/tuning/presets.py`
      - `--objective-preset` is mutually exclusive with `--objective-name` and `--objective-spec-path`
    - you can also load a custom objective definition from disk:
      - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles --replay-source-run-dir data/simulation_runs/<run_dir> --objective-spec-path reports/custom_objective.json`
      - accepted payloads:
        - a raw `ObjectiveSpec.to_payload()` JSON object
        - an `objective_evaluation_report.json` whose `evaluation.objective_spec` should be reused
      - `--objective-name` and `--objective-spec-path` are mutually exclusive
    - you can also mutate the resolved objective directly from the benchmark CLI and have the exact variant persisted into each repeat bundle:
      - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --base-dir data/performance_profiles --replay-source-run-dir data/simulation_runs/<run_dir> --objective-name sim_event_default_v1 --objective-override name=\"sim_event_recall_heavy_v1\" --objective-override primary_terms.3.weight=2.0`
      - override paths use dot segments over the serialized objective payload, with list indices as numeric segments
      - override values are parsed as JSON scalars when possible, otherwise left as strings
      - the resolved objective variant is written to `reports/resolved_objective_spec.json` inside each benchmark repeat directory
      - objective overrides can now also live on individual benchmark variants in the script, so one sweep can compare different objective policies instead of sharing a single global objective mutation
  - run a single named variant when you want a targeted check instead of the whole quick sweep:
    - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --variant baseline --variant all_small_segments`
  - run the broad one-at-a-time full-parameter sweep against the full power/pressurization simulation:
    - `python -m scripts.profile_pipeline_performance --flight-name power_pressurization_hierarchy_composite --mode full --variant-set full_parameter_sweep --base-dir data/performance_profiles`
    - this variant set keeps the canonical workload fixed and sweeps each benchmark CLI tuning knob individually around the baseline, including event, window, phase, conformal warmup, and backbone settings
  - run a stage-local combinatorial search, starting with the earliest surfaced stage:
    - `python -m scripts.profile_pipeline_performance --flight-name power_chain --mode profile --search-stage profile --search-strategy grid --base-dir data/performance_profiles`
    - current supported search stages:
      - `profile`
      - `event`
      - `windowing`
      - `structure`
      - `phase`
      - `anomaly`
    - current supported strategies:
      - `grid`
      - `random`
    - `profile` search currently sweeps:
      - `profile_numeric_ratio_threshold`
      - `profile_categorical_cardinality_max`
    - `event` search currently sweeps:
      - a narrow detector neighborhood:
        - `slope_threshold_scale`
        - `slope_abs_threshold`
        - `slope_min_persistence_samples`
        - `slope_reemit_ratio`
        - `event_warmup_points`
      - generic morphology-policy gains:
        - `event_low_scale_responsiveness`
        - `event_repeatability_aggressiveness`
        - `event_drift_conservatism`
        - `event_chatter_suppression`
    - `windowing` search currently sweeps:
      - `window_max_ms`
      - `window_event_threshold`
      - `window_min_ms`
      - `window_inactivity_timeout_ms`
    - `windowing` search defaults to objective `sim_windowing_default_v1`, which prioritizes:
      - stage-25 window boundary stability
      - stage-25 selected balance penalty
      - downstream pair-cost proxies
      - downstream hierarchy exact-match metrics as secondary terms
    - the current `windowing` search space is intentionally local to the promoted adaptive baseline:
      - `window_max_ms` in `{5000, 7500, 10000}`
      - `window_event_threshold` in `{8, 10, 12}`
      - `window_min_ms` in `{25, 50}`
      - `window_inactivity_timeout_ms` in `{0, 500}`
    - `windowing` search runs under `--mode structural` because that is the earliest grouped runner mode that includes the window stages
    - `structure` search currently sweeps:
      - args:
        - `backbone_parameter_count`
        - `backbone_ridge_lambda`
        - `backbone_event_prior_alpha`
      - env-backed graph and hierarchy controls:
        - `S3NTINEL_V2_MIN_ABS_PARTIAL_CORR`
        - `S3NTINEL_V2_GRAPH_MIN_FUSED_EDGE_WEIGHT`
        - `S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR`
    - `structure` search also runs under `--mode structural`
    - `phase` search currently sweeps:
      - args:
        - `phase_count`
      - env-backed phase controls:
        - `S3NTINEL_PHASE_DETECT_SENSOR_COUNT`
        - `S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT`
        - `S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT`
        - `S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE`
        - `S3NTINEL_PHASE_SMOOTHING_RADIUS`
        - `S3NTINEL_PHASE_TRANSITION_PENALTY`
        - `S3NTINEL_PHASE_MIN_DWELL_WINDOWS`
    - `phase` search runs under `--mode full` because that is the earliest grouped runner mode that includes `70_phase_fit.py`
    - `anomaly` search currently sweeps:
      - args:
        - `min_warm`
      - env-backed scoring and attribution controls:
        - `S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS`
        - `S3NTINEL_SUBSYSTEM_TOP_SENSORS_K`
    - `anomaly` search also runs under `--mode full`
    - use `--search-budget N` to cap the number of non-baseline combinations and `--search-seed` when using `--search-strategy random`
  - the benchmark runner executes the canonical simulation pipeline repeatedly with different sequence-segmentation overrides and writes:
    - a pre-run resolved benchmark plan:
      - `reports/performance_profile_plan.json`
      - `reports/performance_profile_plan.md`
      - includes per-variant replay target, resolved end stage, and the cheapest inferred replay start stage when the source run bundle is replayable
    - per-variant child run bundles under `runs/`
    - `reports/performance_profile_summary.json`
    - `reports/performance_profile_summary.md`
  - variant failures are recorded in the summary by default; the run only exits non-zero if every variant fails
  - pass `--fail-on-variant-error` if you want any failed variant to make the benchmark command fail
  - TODO: this profiler still compares tuning variants on fixed workloads; dataset-size scale sweep is a planned follow-up
 - Inspect an existing simulation run and list replayable stage boundaries:
   - `python -m scripts.report_sim_replay --latest --base-dir data/simulation_runs`
   - or inspect a specific run:
     - `python -m scripts.report_sim_replay --run-dir data/simulation_runs/<run_dir>`
   - ask for the cheapest valid resume path to a target stage:
     - `python -m scripts.report_sim_replay --run-dir data/simulation_runs/<run_dir> --target-stage 50_build_graph.py`
   - use `--json` for machine-readable output
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
