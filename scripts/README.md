# Scripts — Runtime Workflows and Handoff

This directory provides helper scripts for moving repository changes from local development (Bitbucket-connected) to AVD (GitLab-connected).

## 1) Full history handoff (bundle)

On local machine:

- `bash scripts/export_bundle.sh s3ntinel.bundle --all`

In AVD clone:

- copy `s3ntinel.bundle` into repo directory
- `bash scripts/import_bundle.sh s3ntinel.bundle handoff`
- create/update branch from fetched refs and push to GitLab

## Sample test data generation

- `python -m scripts.generate_sample_data --base-dir data --mode overwrite`

This writes deterministic parquet/Delta test data that aligns with pipeline stage schemas.

## End-to-end smoke test

- `python -m scripts.smoke_test_pipeline --base-dir data/smoke --format parquet --min-warm 1`
- Stream-parity windowing in stage 30: set `S3NTINEL_WINDOW_STRATEGY=stream_parity` before running smoke tests/pipelines.
- Side-by-side window diagnostics report: add `--compare-window-strategies` (writes `data/smoke/reports/window_strategy_compare.json` by default)
- Comparison report includes `close_reason_counts` for both bucketed and stream_parity windows.
- Optional regression guard (fails smoke if deltas exceed limits):
	- `--guard-max-window-count-delta <float>`
	- `--guard-max-event-count-avg-delta <float>`
	- `--guard-max-sensor-count-avg-delta <float>`
	- `--guard-max-close-reason-tv-distance <float>`
	- Presets: `--guard-profile conservative|strict` (includes close-reason TV defaults; explicit threshold flags override profile defaults)
- Smoke output prints effective guard thresholds after profile + overrides are resolved.

This command seeds sample data, runs pipeline stages `00` through `80`, and prints row counts for each output table.

## Fitting-phase graph fusion (same training data as CUR)

- Run: `python -m pipelines.10_cur_backbone_fit`
- Writes graph artifacts for hierarchy construction:
	- `data/delta/cur_normalization_profile` (sensor scaling stats for CUR fitting)
	- `data/delta/cur_column_sketch`, `data/delta/cur_column_leverage`, `data/delta/cur_row_sketch` (batch sketch/intermediate leverage tables)
	- `data/delta/cur_sensor_sample`, `data/delta/cur_row_sample` (selected CUR pivots)
	- `data/delta/cur_c_matrix`, `data/delta/cur_r_matrix`, `data/delta/cur_w_matrix`, `data/delta/cur_u_matrix` (CUR factors/core artifacts)
	- Sampling controls: `S3NTINEL_CUR_SAMPLING_MODE=deterministic|weighted`, `S3NTINEL_CUR_SAMPLING_SEED` (recorded under `cur_matrices.sampling_mode` and `cur_matrices.sampling_seed`)
	- U-core guardrails: `S3NTINEL_CUR_MAX_CORE_CELLS`, `S3NTINEL_CUR_MIN_CORE_ROWS`, `S3NTINEL_CUR_MIN_CORE_COLS` (effective reductions reported under `cur_matrices.u_core`)
	- Hierarchy artifacts (Spark PIC multi-level):
		- `data/delta/sensor_hierarchy_map`
		- `data/delta/hierarchy_nodes`
		- `data/delta/hierarchy_edges`
	- `data/delta/cur_sensor_graph` (continuous CUR-proxy edge weights)
	- `data/delta/event_cooccurrence_graph` (event co-occurrence edge weights)
	- `data/delta/fused_sensor_graph` (weighted fusion via `S3NTINEL_GRAPH_FUSE_ALPHA`)
	- `reports/fitting_graph_report.json` (edge-source mix, weight stats, top fused edges)

### Stage-10 hierarchy smoke check

- Run stage-10 then print hierarchy cardinalities + sample paths:
	- `python -m scripts.smoke_hierarchy_stage10 --table-format parquet --raw-table-path data/fleet_seed/delta/raw_telemetry --output-json reports/hierarchy_smoke_summary.json`
- Inspect pre-existing hierarchy outputs without re-running stage-10:
	- `python -m scripts.smoke_hierarchy_stage10 --skip-run-stage10 --table-format parquet --hierarchy-nodes-path data/fleet_seed/profile_hierarchy_e2e/hierarchy_nodes --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map`
- Compare two smoke summaries side-by-side:
	- `python -m scripts.compare_hierarchy_smoke --left-json reports/hierarchy_smoke_summary.json --right-json reports/hierarchy_smoke_summary_fixture.json --left-label stage10 --right-label fixture --output-json reports/hierarchy_smoke_comparison.json`

## CUR sampling A/B evaluation (deterministic vs weighted)

- Compare sampling quality across modes/seeds using hierarchy labels:
	- `python -m scripts.evaluate_cur_sampling_ab --raw-table-path data/fleet_seed/delta/raw_telemetry --table-format parquet --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map --hierarchy-format parquet --modes deterministic,weighted --seeds 11,23,37 --work-dir data/ab_sampling --output-json reports/cur_sampling_ab_report.json`
- Scale overrides (useful for large sensor spaces, e.g. ~30k sensors):
	- `--ab-pivots-k <int> --ab-row-samples-k <int> --ab-cur-graph-max-sensors <int>`
	- plus existing U-core safety: `--cur-max-core-cells`, `--cur-min-core-rows`, `--cur-min-core-cols`
- Output report includes per-run and aggregated metrics:
	- sampled pair precision for module/subsystem labels,
	- module/subsystem coverage ratios,
	- U-core guardrail usage (`u_guardrail_applied`, `u_effective_core_cells`).

## CUR contraction-mode profiling (core_w vs pivot_restricted_a vs full_a)

- Compare stage-10 runtime and U sparsity across contraction modes:
	- `python -m scripts.profile_cur_contraction_modes --raw-table-path data/fleet_seed/delta/raw_telemetry --table-format parquet --modes core_w,pivot_restricted_a,full_a --repeats 2 --work-dir data/cur_profile --output-json reports/cur_contraction_profile.json`
- Report includes:
	- per-run elapsed time and CUR nnz (`c_nnz`, `r_nnz`, `w_nnz`, `u_nnz`),
	- per-mode aggregate elapsed stats,
	- complexity notes for each contraction strategy.
- Stability flags for long runs:
	- `--disable-broadcast-joins` (sets `spark.sql.autoBroadcastJoinThreshold=-1`)
	- `--driver-memory <value>` (for example `--driver-memory 8g`)
- Render profile JSON into a one-page markdown summary:
	- `python -m scripts.render_cur_profile_report --input-json reports/cur_contraction_profile_stable.json --output-md reports/cur_contraction_profile_stable.md`

## Telemetry profiling and routing

- `python -m scripts.profile_telemetry --input-path data/sample/raw_input --input-format parquet --output-dir data/profile`

## Fleet profile synthesis (m tails x n flights)

- `python -m scripts.generate_fleet_profiles --base-parameter-profile-path data/profile/parameter_profile --base-categorical-distribution-path data/profile/categorical_distribution --tail-count 3 --flights-per-tail 2 --output-dir data/profile_fleet`
- Optional synthetic hierarchy map injection for structured correlation:
	- `python -m scripts.generate_synthetic_hierarchy_profile --profile-parameter-profile-path data/profile/parameter_profile --profile-format parquet --output-dir data/profile_hierarchy --hierarchy-profile-id HIER_SYNTH_V1 --system-count 3 --subsystems-per-system 2 --modules-per-subsystem 3`
	- then pass map into fleet synthesis: `--hierarchy-sensor-map-path data/profile_hierarchy/sensor_hierarchy_map`
- Preset hierarchy correlation difficulty for CUR-graph validation: `--hier-correlation-preset easy|medium|hard` (default `medium`)
- Controlled variance knobs:
	- numeric mean/std/rate/missing spread: `--mean-tail-std-ratio`, `--mean-flight-std-ratio`, `--std-tail-std-ratio`, `--std-flight-std-ratio`, `--rate-tail-std-ratio`, `--rate-flight-std-ratio`, `--missing-tail-std`, `--missing-flight-std`
	- categorical distribution spread: `--cat-logit-tail-std`, `--cat-logit-flight-std`, `--cat-sample-size`
	- hierarchy-level spread (when map supplied; overrides preset values): `--hier-mean-global-std-ratio`, `--hier-mean-system-std-ratio`, `--hier-mean-subsystem-std-ratio`, `--hier-mean-module-std-ratio`, `--hier-std-global-std-ratio`, `--hier-std-system-std-ratio`, `--hier-std-subsystem-std-ratio`, `--hier-std-module-std-ratio`, `--hier-rate-global-std-ratio`, `--hier-rate-system-std-ratio`, `--hier-rate-subsystem-std-ratio`, `--hier-rate-module-std-ratio`, `--hier-missing-global-std`, `--hier-missing-system-std`, `--hier-missing-subsystem-std`, `--hier-missing-module-std`

Preset defaults:
- `easy`: stronger hierarchy signal and common-mode coupling (easier graph recovery)
- `medium`: balanced hierarchy signal with realistic overlap/noise (recommended default)
- `hard`: weaker hierarchy signal and subtler coupling (harder graph recovery)

This writes fleet-scoped tables with `tail_id` + `flight_id` columns:
- `data/profile_fleet/parameter_profile`
- `data/profile_fleet/categorical_distribution` (when categorical base table is provided)
- `data/profile_fleet/fleet_manifest`

When hierarchy map injection is used, fleet `parameter_profile` rows include provenance columns:
- `injected_system_id`, `injected_subsystem_id`, `injected_module_id`
- `injected_hierarchy_profile_id`, `injected_hierarchy_source` (`synthetic_injected`)

Safety rule: keep synthetic hierarchy artifacts separate from discovered hierarchy outputs; do not treat `synthetic_injected` artifacts as learned structure.

Writes:
- `data/profile/parameter_profile`
- `data/profile/categorical_distribution`
- `data/profile/channel_routing`

## Synthetic normal telemetry

- Defaults only:
	- `python -m scripts.generate_synthetic_normal --output-path data/synthetic/raw_telemetry`
- Using profile-derived characteristics:
	- `python -m scripts.generate_synthetic_normal --profile-path data/profile/parameter_profile --profile-format parquet --output-path data/synthetic/raw_telemetry`

## Synthetic fleet telemetry

- Generate all tail/flight streams from fleet profile artifacts:
	- `python -m scripts.generate_synthetic_fleet --fleet-manifest-path data/profile_fleet/fleet_manifest --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-format parquet --output-path data/synthetic/fleet_raw_telemetry`
- Emit one dataset per tail/flight with orchestration manifest:
	- `python -m scripts.generate_synthetic_fleet --fleet-manifest-path data/profile_fleet/fleet_manifest --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-format parquet --output-path data/synthetic/fleet_partitioned --emit-manifest-partitions`
- Partition manifest path (defaults to `<output-path>/_partition_manifest`):
	- `--partition-manifest-path data/synthetic/fleet_partitioned_manifest`
- Optional selectors for one tail/flight:
	- `--tail-id FLEET_T001 --flight-id FL001`
- Optional strict scope validation (error if scoped profile rows are missing):
	- `--strict-profile-scope`

## Partition manifest job runner

- Run built-in per-flight pipeline stages (`00,20,30,40,50,60,70,80`) from partition manifest rows:
	- `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/synthetic/fleet_partitioned/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_jobs --table-format parquet --min-warm 1`
- Run custom evaluation command(s) per manifest row using placeholders `{tail_id}`, `{flight_id}`, `{output_path}`, `{run_dir}`:
	- `python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/synthetic/fleet_partitioned/_partition_manifest --manifest-format parquet --job custom --command "conda run -n sentinel python -m scripts.profile_telemetry --input-path {output_path} --input-format parquet --output-dir {run_dir}/profile --output-format parquet" --command "conda run -n sentinel python -m scripts.evaluate_stream_event_detection --profile-parameter-profile-path {run_dir}/profile/parameter_profile --profile-categorical-distribution-path {run_dir}/profile/categorical_distribution --profile-format parquet --duration-seconds 120 --event-types transition,dropped,oscillation,switch"`
- Optional row controls:
	- `--tail-id ... --flight-id ... --limit 1 --continue-on-error --dry-run`

## Sentinel env: validated hierarchy → partition orchestration flow

Use this exact sequence from repo root (`conda env sentinel`):

1) Generate synthetic hierarchy artifacts
	- `conda run -n sentinel --no-capture-output python -m scripts.generate_synthetic_hierarchy_profile --profile-parameter-profile-path data/fleet_seed/profile/parameter_profile --profile-format parquet --output-dir data/fleet_seed/profile_hierarchy_e2e --output-format parquet --hierarchy-profile-id HIER_SYNTH_E2E --system-count 3 --subsystems-per-system 2 --modules-per-subsystem 3 --seed 7`

2) Generate hierarchy-aware fleet profiles
	- `conda run -n sentinel --no-capture-output python -m scripts.generate_fleet_profiles --base-parameter-profile-path data/fleet_seed/profile/parameter_profile --base-categorical-distribution-path data/fleet_seed/profile/categorical_distribution --input-format parquet --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map --output-dir data/fleet_seed/profile_fleet_hier_e2e --output-format parquet --tail-count 2 --flights-per-tail 2 --seed 11 --hier-mean-system-std-ratio 0.03 --hier-mean-subsystem-std-ratio 0.015 --hier-mean-module-std-ratio 0.01 --hier-std-system-std-ratio 0.02 --hier-std-subsystem-std-ratio 0.01 --hier-std-module-std-ratio 0.005 --hier-rate-system-std-ratio 0.03 --hier-rate-subsystem-std-ratio 0.015 --hier-rate-module-std-ratio 0.01 --hier-missing-system-std 0.02 --hier-missing-subsystem-std 0.01 --hier-missing-module-std 0.005`

3) Generate partitioned synthetic fleet telemetry + manifest
	- `conda run -n sentinel --no-capture-output python -m scripts.generate_synthetic_fleet --fleet-manifest-path data/fleet_seed/profile_fleet_hier_e2e/fleet_manifest --profile-parameter-profile-path data/fleet_seed/profile_fleet_hier_e2e/parameter_profile --profile-categorical-distribution-path data/fleet_seed/profile_fleet_hier_e2e/categorical_distribution --profile-format parquet --output-format parquet --output-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e --emit-manifest-partitions --partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --duration-seconds 120 --seed 23`

4) Dry-run partition jobs
	- `conda run -n sentinel --no-capture-output python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_seed/fleet_jobs_hier_e2e --table-format parquet --min-warm 1 --limit 4 --dry-run`

5) Execute partition jobs
	- `conda run -n sentinel --no-capture-output python -m scripts.run_partition_manifest_jobs --partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --manifest-format parquet --job pipeline --jobs-base-dir data/fleet_seed/fleet_jobs_hier_e2e --table-format parquet --min-warm 1 --limit 4`

Note: `scripts.generate_synthetic_fleet` does not accept `--rate-hz`.

## Hierarchy recovery scoring (easy/medium/hard)

Run a compact CUR-graph proxy validation by scoring correlation-based recovery against injected hierarchy labels:

- `conda run -n sentinel --no-capture-output python -m scripts.evaluate_hierarchy_recovery --base-parameter-profile-path data/fleet_seed/profile/parameter_profile --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map --profile-format parquet --presets easy,medium,hard --tail-count 2 --flights-per-tail 2 --duration-seconds 180 --max-corr-sensors 32 --output-json reports/hierarchy_recovery_metrics.json`

Evaluate directly from already-generated partition telemetry manifests (no inline telemetry generation):

- Single manifest for all presets:
	- `conda run -n sentinel --no-capture-output python -m scripts.evaluate_hierarchy_recovery --base-parameter-profile-path data/fleet_seed/profile/parameter_profile --hierarchy-sensor-map-path data/fleet_seed/profile_hierarchy_e2e/sensor_hierarchy_map --profile-format parquet --presets easy,medium,hard --telemetry-partition-manifest-path data/fleet_seed/synthetic/fleet_partitioned_hier_e2e/_partition_manifest --telemetry-manifest-format parquet --telemetry-format parquet --max-corr-sensors 32 --output-json reports/hierarchy_recovery_metrics_from_manifest.json`

- Preset-specific manifests via template:
	- `--telemetry-partition-manifest-path data/fleet_seed/synthetic/{preset}/_partition_manifest`

Outputs:
- JSON report with per-preset metrics: `auroc_module`, `auroc_subsystem`, `precision_at_k_module`, `precision_at_k_subsystem`, and within/between separation ratios.
- In manifest mode, JSON also includes `preflight_by_preset` with per-flight `constant_sensors`, `dropped_sensors`, `usable_sensors`, and `pairable` status.

Interpretation:
- `easy` should generally show stronger separation/recovery than `medium`, and `medium` stronger than `hard`.
- A metric can be `null` when the selected hierarchy map does not provide both positive and negative pairs at that level (for example, no same-module pairs in the selected sensor set).
- Existing telemetry mode also needs at least two analyzable, non-constant sensor series per flight; otherwise `pair_count` can be `0` and all recovery metrics will be `null`.

## Generator-based stream demo (no Spark materialization)

- `python -m scripts.stream_synthetic_events_demo --duration-seconds 180 --rate-hz 20 --osc-amp 8 --switch-interval 45 --switch-mag 25 --emit-extrema-events --drift-guard-abs-change 80 --drift-guard-max-gap 100`
- Optional co-occurrence output: add `--emit-cooccur-events --cooccur-min-sensors 2`
- `cooccur` is derived from any distinct sensors present in emitted windows (no peer list required)
- Profile-driven mixed sensor demo: `python -m scripts.stream_synthetic_events_demo --profile-json conf/demo_stream_profile.json --duration-seconds 180 --emit-extrema-events --emit-cooccur-events`
- Generate a starter profile file from the script: `python -m scripts.stream_synthetic_events_demo --write-demo-profile conf/demo_stream_profile.json`
- Use profiled telemetry artifacts directly (no JSON conversion): `python -m scripts.stream_synthetic_events_demo --profile-parameter-profile-path data/profile_smoke/profile/parameter_profile --profile-categorical-distribution-path data/profile_smoke/profile/categorical_distribution --profile-format parquet --duration-seconds 180 --emit-extrema-events --emit-cooccur-events`
- Use fleet-scoped profile artifacts by selecting one tail/flight: `python -m scripts.stream_synthetic_events_demo --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-tail-id FLEET_T001 --profile-flight-id FL001 --profile-format parquet --duration-seconds 180 --emit-extrema-events --emit-cooccur-events`
- Window controls: `--window-max-ms 200 --window-min-ms 50 --window-event-threshold 20 --window-inactivity-timeout-ms 0 --windows-jsonl data/smoke/windows.jsonl`

This runs synthetic telemetry as Python generators and applies streaming continuous and categorical detectors without creating large DataFrames.
Detected events feed adaptive stream windows with per-sensor buffers and ZOH snapshots.
When enabled, `cooccur` is generated from sensors that co-occur within each emitted window, independent of explicit peer lists.
Window outputs include `close_reason` (`event_threshold`, `max_ms`, `event_threshold+max_ms`, `inactivity_timeout`, or `end_of_stream`) and the demo prints close-reason counts.

## Stream detector evaluation (precision/recall)

- `python -m scripts.evaluate_stream_event_detection --duration-seconds 300 --tolerance-seconds 0.5`
- Example with explicit scored types: `python -m scripts.evaluate_stream_event_detection --duration-seconds 300 --event-types transition,dropped,oscillation,switch`
- Profile-driven mixed sensor evaluation: `python -m scripts.evaluate_stream_event_detection --profile-json conf/demo_stream_profile.json --duration-seconds 300 --event-types transition,dropped,oscillation,switch`
- Table-driven profile evaluation: `python -m scripts.evaluate_stream_event_detection --profile-parameter-profile-path data/profile_smoke/profile/parameter_profile --profile-categorical-distribution-path data/profile_smoke/profile/categorical_distribution --profile-format parquet --duration-seconds 300 --event-types transition,dropped,oscillation,switch`
- Fleet table evaluation with selector: `python -m scripts.evaluate_stream_event_detection --profile-parameter-profile-path data/profile_fleet/parameter_profile --profile-categorical-distribution-path data/profile_fleet/categorical_distribution --profile-tail-id FLEET_T001 --profile-flight-id FL001 --profile-format parquet --duration-seconds 300 --event-types transition,dropped,oscillation,switch`
- Includes stream-side `cooccur` emission for graph population view; tune with `--cooccur-window-seconds`, `--cooccur-min-sensors`, and `--cooccur-refractory-seconds`.

This runs mixed continuous + categorical generator streams, applies both streaming detectors, and reports precision/recall against generator-provided truth events.

## 2) Incremental handoff (patches)

On local machine:

- `bash scripts/export_patches.sh origin/main patches`

In AVD clone:

- copy `patches/` folder into repo
- `bash scripts/apply_patches.sh patches`
- push resulting branch to GitLab

## Notes

- Scripts use strict bash mode (`set -euo pipefail`).
- Python script entrypoints are documented in module mode (`python -m scripts.<name> ...`) from the repo root.
- If `git am` stops on conflicts, resolve conflicts and continue with `git am --continue`.
- Use bundle mode when you need complete branch context; use patches for lighter incremental sync.
