workspace "S3NTINEL Architecture" "Architecture view of the active pipeline, simulation, core library, and architecture-tooling surfaces." {
  !identifiers hierarchical

  model {
    engineer = person "Engineer" "Runs simulation, fitting, inference, and architecture-generation workflows."
    raw_telemetry_inputs = softwareSystem "Raw Telemetry Inputs" "Real or simulated telemetry inputs consumed by the persisted pipeline."
    mlflow_tracking = softwareSystem "MLflow Tracking" "Optional experiment tracking and metrics sink used by pipeline and performance workflows."
    persisted_artifacts = softwareSystem "Persisted Artifacts" "Parquet or Delta tables, reports, manifests, explorer bundles, and generated architecture docs."
    s3ntinel = softwareSystem "S3NTINEL Architecture" "Architecture view of the active pipeline, simulation, core library, and architecture-tooling surfaces." {
      architecture_tooling = container "Architecture Tooling" "Repo map extraction, architecture normalization, C4 rendering, and AI review packaging." "Python CLI" {
        properties {
          "loc_span" "3912"
          "module_count" "10"
          "source_paths" "libs/architecture/__init__.py,libs/architecture/ai_review.py,libs/architecture/annotations.py,libs/architecture/extract.py,libs/architecture/model.py,libs/architecture/render.py,tools/__init__.py,tools/architecture_workflow.py,tools/module_deps.py,tools/repo_schematic.py"
        }
        repo_maps = component "Repo Mapping Tools" "Existing AST and import-graph tooling used as the source layer for architecture extraction." "Python CLI" {
        properties {
          "loc_span" "1029"
          "module_count" "2"
          "source_paths" "tools/module_deps.py,tools/repo_schematic.py"
        }
        }
        workflow = component "Architecture Workflow" "Deterministic extract, render, check, and AI draft orchestration for architecture artifacts." "Python CLI" {
        properties {
          "loc_span" "2881"
          "module_count" "7"
          "source_paths" "libs/architecture/__init__.py,libs/architecture/ai_review.py,libs/architecture/annotations.py,libs/architecture/extract.py,libs/architecture/model.py,libs/architecture/render.py,tools/architecture_workflow.py"
        }
        }
      }
      core_libraries = container "Core Libraries" "Reusable domain and infrastructure libraries behind the active runtime path." "Python + PySpark" {
        properties {
          "loc_span" "45078"
          "module_count" "189"
          "source_paths" "libs/__init__.py,libs/anomaly/__init__.py,libs/anomaly/frames.py,libs/anomaly/pipeline.py,libs/anomaly/tables.py,libs/anomaly/validator.py,libs/backbone/__init__.py,libs/backbone/artifacts.py,libs/backbone/energy.py,libs/backbone/fit.py,libs/backbone/tables.py,libs/behavior/__init__.py,libs/behavior/accumulative.py,libs/behavior/base.py,libs/behavior/discrete_state.py,libs/behavior/inertial.py,libs/behavior/primitives.py,libs/behavior/registry.py,libs/behavior/regulated.py,libs/behavior/tick.py,libs/behavior/tracking.py,libs/behavior/utils.py,libs/behavior/validation.py,libs/common/__init__.py,libs/common/event_types.py,libs/common/parameter_datatypes.py,libs/common/spark_exprs.py,libs/config/__init__.py,libs/config/pipeline.py,libs/events/__init__.py,libs/events/calibration.py,libs/events/categorical.py,libs/events/continuous.py,libs/events/pipeline.py,libs/events/profiling.py,libs/events/tables.py,libs/events/types.py,libs/events/validator.py,libs/graph/__init__.py,libs/graph/data.py,libs/graph/evaluation.py,libs/graph/event.py,libs/graph/fused.py,libs/graph/hierarchy.py,libs/graph/hierarchy_artifacts.py,libs/graph/lag.py,libs/graph/pipeline.py,libs/graph/precision.py,libs/graph/tables.py,libs/graph/transition.py,libs/graph/validator.py,libs/io/__init__.py,libs/io/contracts.py,libs/io/delta.py,libs/io/pandas_spark.py,libs/io/schemas/__init__.py,libs/io/schemas/anomaly.py,libs/io/schemas/backbone.py,libs/io/schemas/events.py,libs/io/schemas/explorer.py,libs/io/schemas/graph.py,libs/io/schemas/phase.py,libs/io/schemas/profiling.py,libs/io/schemas/scoring.py,libs/io/schemas/telemetry.py,libs/io/schemas/windows.py,libs/io/transforms.py,libs/perf/__init__.py,libs/perf/annotations.py,libs/perf/logger.py,libs/perf/memory.py,libs/perf/mlflow.py,libs/perf/stage_manifest.py,libs/phase/__init__.py,libs/phase/analysis.py,libs/phase/artifacts.py,libs/phase/config_fit.py,libs/phase/decode.py,libs/phase/feature_config.py,libs/phase/fit.py,libs/phase/frames.py,libs/phase/pipeline.py,libs/phase/selectors.py,libs/phase/tables.py,libs/phase/types.py,libs/phase/utils.py,libs/phase/validator.py,libs/plotting/__init__.py,libs/plotting/explorer_bundle.py,libs/plotting/simulation_artifacts.py,libs/profiling/__init__.py,libs/profiling/pipeline.py,libs/profiling/profiles.py,libs/profiling/validator.py,libs/pyspark/__init__.py,libs/pyspark/frame.py,libs/pyspark/schema.py,libs/pyspark/table.py,libs/reporting/__init__.py,libs/reporting/frame.py,libs/scoring/__init__.py,libs/scoring/channels.py,libs/scoring/tables.py,libs/scoring/validator.py,libs/simulation/__init__.py,libs/simulation/aircraft/__init__.py,libs/simulation/aircraft/examples.py,libs/simulation/aircraft/runtime.py,libs/simulation/aircraft/spec.py,libs/simulation/cli.py,libs/simulation/coupling/__init__.py,libs/simulation/coupling/examples.py,libs/simulation/coupling/runtime.py,libs/simulation/coupling/spec.py,libs/simulation/event_truth.py,libs/simulation/fault/__init__.py,libs/simulation/fault/examples.py,libs/simulation/fault/runtime.py,libs/simulation/fault/spec.py,libs/simulation/fleet/__init__.py,libs/simulation/fleet/examples.py,libs/simulation/fleet/runtime.py,libs/simulation/flight/__init__.py,libs/simulation/flight/examples.py,libs/simulation/flight/runtime.py,libs/simulation/flight/spec.py,libs/simulation/full_run_report.py,libs/simulation/module/__init__.py,libs/simulation/module/examples.py,libs/simulation/module/runtime.py,libs/simulation/module/spec.py,libs/simulation/parameter/__init__.py,libs/simulation/parameter/examples.py,libs/simulation/parameter/runtime.py,libs/simulation/parameter/spec.py,libs/simulation/phase/__init__.py,libs/simulation/phase/catalog.py,libs/simulation/phase/examples.py,libs/simulation/phase/runtime.py,libs/simulation/phase/spec.py,libs/simulation/port/__init__.py,libs/simulation/port/examples.py,libs/simulation/port/runtime.py,libs/simulation/port/spec.py,libs/simulation/replay_report.py,libs/simulation/report_tables.py,libs/simulation/reporting.py,libs/simulation/run_bundle.py,libs/simulation/run_cli.py,libs/simulation/run_context.py,libs/simulation/runner.py,libs/simulation/scenarios/__init__.py,libs/simulation/scenarios/power_pressurization.py,libs/simulation/seed_bundle.py,libs/simulation/subsystem/__init__.py,libs/simulation/subsystem/examples.py,libs/simulation/subsystem/runtime.py,libs/simulation/subsystem/spec.py,libs/simulation/system/__init__.py,libs/simulation/system/examples.py,libs/simulation/system/runtime.py,libs/simulation/system/spec.py,libs/simulation/tail/__init__.py,libs/simulation/tail/examples.py,libs/simulation/tail/runtime.py,libs/simulation/validation_harness.py,libs/spark_sequence/__init__.py,libs/spark_sequence/plan.py,libs/tuning/__init__.py,libs/tuning/benchmark_execution.py,libs/tuning/benchmark_invocation.py,libs/tuning/benchmark_planning.py,libs/tuning/benchmark_reporting.py,libs/tuning/benchmark_runner.py,libs/tuning/benchmark_runtime.py,libs/tuning/benchmark_search.py,libs/tuning/benchmark_variants.py,libs/tuning/objectives.py,libs/tuning/presets.py,libs/tuning/reporting.py,libs/tuning/validation_panels.py,libs/windows/__init__.py,libs/windows/buffer.py,libs/windows/coverage.py,libs/windows/features.py,libs/windows/pipeline.py,libs/windows/policy_profile.py,libs/windows/tables.py,libs/windows/window.py"
        }
        anomaly = component "Anomaly" "libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation" "Python + PySpark" {
        properties {
          "loc_span" "2642"
          "module_count" "5"
          "package_name" "libs.anomaly"
          "source_paths" "libs/anomaly/__init__.py,libs/anomaly/frames.py,libs/anomaly/pipeline.py,libs/anomaly/tables.py,libs/anomaly/validator.py"
        }
        }
        backbone = component "Backbone" "libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance" "Python + PySpark" {
        properties {
          "loc_span" "817"
          "module_count" "5"
          "package_name" "libs.backbone"
          "source_paths" "libs/backbone/__init__.py,libs/backbone/artifacts.py,libs/backbone/energy.py,libs/backbone/fit.py,libs/backbone/tables.py"
        }
        }
        behavior = component "Behavior" "libs/behavior owns the parameter behavior families used by simulation" "Python + PySpark" {
        properties {
          "loc_span" "1910"
          "module_count" "12"
          "package_name" "libs.behavior"
          "source_paths" "libs/behavior/__init__.py,libs/behavior/accumulative.py,libs/behavior/base.py,libs/behavior/discrete_state.py,libs/behavior/inertial.py,libs/behavior/primitives.py,libs/behavior/registry.py,libs/behavior/regulated.py,libs/behavior/tick.py,libs/behavior/tracking.py,libs/behavior/utils.py,libs/behavior/validation.py"
        }
        }
        common = component "Common" "libs/common now contains only narrow shared constants and helpers that are genuinely cross-cutting" "Python + PySpark" {
        properties {
          "loc_span" "199"
          "module_count" "4"
          "package_name" "libs.common"
          "source_paths" "libs/common/__init__.py,libs/common/event_types.py,libs/common/parameter_datatypes.py,libs/common/spark_exprs.py"
        }
        }
        config = component "Config" "Owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses" "Python + PySpark" {
        properties {
          "loc_span" "642"
          "module_count" "2"
          "package_name" "libs.config"
          "source_paths" "libs/config/__init__.py,libs/config/pipeline.py"
        }
        }
        events = component "Events" "libs/events owns canonical event detection and event validation" "Python + PySpark" {
        properties {
          "loc_span" "3493"
          "module_count" "9"
          "package_name" "libs.events"
          "source_paths" "libs/events/__init__.py,libs/events/calibration.py,libs/events/categorical.py,libs/events/continuous.py,libs/events/pipeline.py,libs/events/profiling.py,libs/events/tables.py,libs/events/types.py,libs/events/validator.py"
        }
        }
        graph = component "Graph" "libs/graph owns graph-domain models built from telemetry windows and events" "Python + PySpark" {
        properties {
          "loc_span" "3513"
          "module_count" "13"
          "package_name" "libs.graph"
          "source_paths" "libs/graph/__init__.py,libs/graph/data.py,libs/graph/evaluation.py,libs/graph/event.py,libs/graph/fused.py,libs/graph/hierarchy.py,libs/graph/hierarchy_artifacts.py,libs/graph/lag.py,libs/graph/pipeline.py,libs/graph/precision.py,libs/graph/tables.py,libs/graph/transition.py,libs/graph/validator.py"
        }
        }
        io = component "Io" "libs/io owns artifact schemas, row contracts, and persistence/bridge utilities" "Python + PySpark" {
        properties {
          "loc_span" "2073"
          "module_count" "16"
          "package_name" "libs.io"
          "source_paths" "libs/io/__init__.py,libs/io/contracts.py,libs/io/delta.py,libs/io/pandas_spark.py,libs/io/schemas/__init__.py,libs/io/schemas/anomaly.py,libs/io/schemas/backbone.py,libs/io/schemas/events.py,libs/io/schemas/explorer.py,libs/io/schemas/graph.py,libs/io/schemas/phase.py,libs/io/schemas/profiling.py,libs/io/schemas/scoring.py,libs/io/schemas/telemetry.py,libs/io/schemas/windows.py,libs/io/transforms.py"
        }
        }
        perf = component "Perf" "libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation" "Python + PySpark" {
        properties {
          "loc_span" "1088"
          "module_count" "6"
          "package_name" "libs.perf"
          "source_paths" "libs/perf/__init__.py,libs/perf/annotations.py,libs/perf/logger.py,libs/perf/memory.py,libs/perf/mlflow.py,libs/perf/stage_manifest.py"
        }
        }
        phase = component "Phase" "libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation" "Python + PySpark" {
        properties {
          "loc_span" "4332"
          "module_count" "14"
          "package_name" "libs.phase"
          "source_paths" "libs/phase/__init__.py,libs/phase/analysis.py,libs/phase/artifacts.py,libs/phase/config_fit.py,libs/phase/decode.py,libs/phase/feature_config.py,libs/phase/fit.py,libs/phase/frames.py,libs/phase/pipeline.py,libs/phase/selectors.py,libs/phase/tables.py,libs/phase/types.py,libs/phase/utils.py,libs/phase/validator.py"
        }
        }
        profiling = component "Profiling" "libs/profiling owns parameter profiling over canonical telemetry: datatype profiling; continuous scaling/profile statistics; behavior-family profiling" "Python + PySpark" {
        properties {
          "loc_span" "1552"
          "module_count" "4"
          "package_name" "libs.profiling"
          "source_paths" "libs/profiling/__init__.py,libs/profiling/pipeline.py,libs/profiling/profiles.py,libs/profiling/validator.py"
        }
        }
        pyspark = component "Pyspark" "Owns typed Spark frame and table wrappers used at repository boundaries" "Python + PySpark" {
        properties {
          "loc_span" "201"
          "module_count" "4"
          "package_name" "libs.pyspark"
          "source_paths" "libs/pyspark/__init__.py,libs/pyspark/frame.py,libs/pyspark/schema.py,libs/pyspark/table.py"
        }
        }
        reporting = component "Reporting" "Owns thin report payload wrappers used by generated summaries and diagnostics" "Python + PySpark" {
        properties {
          "loc_span" "64"
          "module_count" "2"
          "package_name" "libs.reporting"
          "source_paths" "libs/reporting/__init__.py,libs/reporting/frame.py"
        }
        }
        scoring = component "Scoring" "libs/scoring owns raw and calibrated anomaly scoring over fitted phase and structural artifacts" "Python + PySpark" {
        properties {
          "loc_span" "2123"
          "module_count" "4"
          "package_name" "libs.scoring"
          "source_paths" "libs/scoring/__init__.py,libs/scoring/channels.py,libs/scoring/tables.py,libs/scoring/validator.py"
        }
        }
        simulation = component "Simulation" "Owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles" "Python + PySpark" {
        properties {
          "loc_span" "11674"
          "module_count" "62"
          "package_name" "libs.simulation"
          "source_paths" "libs/simulation/__init__.py,libs/simulation/aircraft/__init__.py,libs/simulation/aircraft/examples.py,libs/simulation/aircraft/runtime.py,libs/simulation/aircraft/spec.py,libs/simulation/cli.py,libs/simulation/coupling/__init__.py,libs/simulation/coupling/examples.py,libs/simulation/coupling/runtime.py,libs/simulation/coupling/spec.py,libs/simulation/event_truth.py,libs/simulation/fault/__init__.py,libs/simulation/fault/examples.py,libs/simulation/fault/runtime.py,libs/simulation/fault/spec.py,libs/simulation/fleet/__init__.py,libs/simulation/fleet/examples.py,libs/simulation/fleet/runtime.py,libs/simulation/flight/__init__.py,libs/simulation/flight/examples.py,libs/simulation/flight/runtime.py,libs/simulation/flight/spec.py,libs/simulation/full_run_report.py,libs/simulation/module/__init__.py,libs/simulation/module/examples.py,libs/simulation/module/runtime.py,libs/simulation/module/spec.py,libs/simulation/parameter/__init__.py,libs/simulation/parameter/examples.py,libs/simulation/parameter/runtime.py,libs/simulation/parameter/spec.py,libs/simulation/phase/__init__.py,libs/simulation/phase/catalog.py,libs/simulation/phase/examples.py,libs/simulation/phase/runtime.py,libs/simulation/phase/spec.py,libs/simulation/port/__init__.py,libs/simulation/port/examples.py,libs/simulation/port/runtime.py,libs/simulation/port/spec.py,libs/simulation/replay_report.py,libs/simulation/report_tables.py,libs/simulation/reporting.py,libs/simulation/run_bundle.py,libs/simulation/run_cli.py,libs/simulation/run_context.py,libs/simulation/runner.py,libs/simulation/scenarios/__init__.py,libs/simulation/scenarios/power_pressurization.py,libs/simulation/seed_bundle.py,libs/simulation/subsystem/__init__.py,libs/simulation/subsystem/examples.py,libs/simulation/subsystem/runtime.py,libs/simulation/subsystem/spec.py,libs/simulation/system/__init__.py,libs/simulation/system/examples.py,libs/simulation/system/runtime.py,libs/simulation/system/spec.py,libs/simulation/tail/__init__.py,libs/simulation/tail/examples.py,libs/simulation/tail/runtime.py,libs/simulation/validation_harness.py"
        }
        }
        spark_sequence = component "Spark Sequence" "Owns deterministic sequence ordering and segmentation policies for long Spark streams" "Python + PySpark" {
        properties {
          "loc_span" "274"
          "module_count" "2"
          "package_name" "libs.spark_sequence"
          "source_paths" "libs/spark_sequence/__init__.py,libs/spark_sequence/plan.py"
        }
        }
        tuning = component "Tuning" "Owns benchmark search, objective specifications, comparable run ranking, and tuning reports" "Python + PySpark" {
        properties {
          "loc_span" "3479"
          "module_count" "13"
          "package_name" "libs.tuning"
          "source_paths" "libs/tuning/__init__.py,libs/tuning/benchmark_execution.py,libs/tuning/benchmark_invocation.py,libs/tuning/benchmark_planning.py,libs/tuning/benchmark_reporting.py,libs/tuning/benchmark_runner.py,libs/tuning/benchmark_runtime.py,libs/tuning/benchmark_search.py,libs/tuning/benchmark_variants.py,libs/tuning/objectives.py,libs/tuning/presets.py,libs/tuning/reporting.py,libs/tuning/validation_panels.py"
        }
        }
        windows = component "Windows" "libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder" "Python + PySpark" {
        properties {
          "loc_span" "3442"
          "module_count" "8"
          "package_name" "libs.windows"
          "source_paths" "libs/windows/__init__.py,libs/windows/buffer.py,libs/windows/coverage.py,libs/windows/features.py,libs/windows/pipeline.py,libs/windows/policy_profile.py,libs/windows/tables.py,libs/windows/window.py"
        }
        }
      }
      pipeline_runtime = container "Pipeline Runtime" "Persisted stage entrypoints and grouped runners for the active fitting and inference path." "Python CLI + PySpark" {
        properties {
          "loc_span" "3786"
          "module_count" "23"
          "source_paths" "pipelines/00_ingest_raw.py,pipelines/10_parameter_profiles_fit.py,pipelines/12_behavior_profiles_fit.py,pipelines/15_event_profiles_fit.py,pipelines/20_events_extract.py,pipelines/25_window_policy_profile.py,pipelines/30_windows_adaptive.py,pipelines/40_backbone_fit.py,pipelines/50_build_graph.py,pipelines/60_fit_hierarchy.py,pipelines/70_phase_fit.py,pipelines/72_phase_label_centroids.py,pipelines/80_window_scores_raw.py,pipelines/85_window_scores_calibrate.py,pipelines/90_anomaly_attribution.py,pipelines/95_emit_explorer_bundle.py,pipelines/97_run_fitting_pipeline.py,pipelines/98_run_inference_pipeline.py,pipelines/99_run_full_pipeline.py,pipelines/__init__.py,pipelines/_pipeline_runner.py,pipelines/common.py,pipelines/plans.py"
        }
        fitting_stages = component "Fitting Stages" "Entry points for ingest, profiling, events, windows, backbone, graph, and hierarchy fitting." "Python CLI + PySpark" {
        properties {
          "loc_span" "2066"
          "module_count" "10"
          "source_paths" "pipelines/00_ingest_raw.py,pipelines/10_parameter_profiles_fit.py,pipelines/12_behavior_profiles_fit.py,pipelines/15_event_profiles_fit.py,pipelines/20_events_extract.py,pipelines/25_window_policy_profile.py,pipelines/30_windows_adaptive.py,pipelines/40_backbone_fit.py,pipelines/50_build_graph.py,pipelines/60_fit_hierarchy.py"
        }
        }
        grouped_runners = component "Grouped Pipeline Runners" "Coordinates grouped fitting, inference, and end-to-end pipeline execution." "Python CLI" {
        properties {
          "loc_span" "565"
          "module_count" "4"
          "source_paths" "pipelines/97_run_fitting_pipeline.py,pipelines/98_run_inference_pipeline.py,pipelines/99_run_full_pipeline.py,pipelines/_pipeline_runner.py"
        }
        }
        inference_stages = component "Inference Stages" "Entry points for phase fitting, scoring, calibration, attribution, and explorer output." "Python CLI + PySpark" {
        properties {
          "loc_span" "989"
          "module_count" "6"
          "source_paths" "pipelines/70_phase_fit.py,pipelines/72_phase_label_centroids.py,pipelines/80_window_scores_raw.py,pipelines/85_window_scores_calibrate.py,pipelines/90_anomaly_attribution.py,pipelines/95_emit_explorer_bundle.py"
        }
        }
        pipeline_common = component "Pipeline Shared Runtime" "Shared pipeline context and configuration assembly used by persisted stage entrypoints." "Python" {
        properties {
          "loc_span" "123"
          "module_count" "2"
          "source_paths" "pipelines/__init__.py,pipelines/common.py"
        }
        }
        stage_00_ingest_raw = component "00 Ingest Raw" "Ingest raw parquet telemetry into normalized Delta bronze/silver tables." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "2"
          "library_layer_summary" "Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Config: Modules grouped under libs.config"
          "library_layers" "Perf, Io, Config"
          "loc_span" "106"
          "module_name" "pipelines.00_ingest_raw"
          "purpose" "Ingest raw parquet telemetry into normalized Delta bronze/silver tables."
          "source_paths" "pipelines/00_ingest_raw.py"
        }
        }
        stage_10_parameter_profiles_fit = component "10 Parameter Profiles Fit" "Fit datatype and scaling profile artifacts from raw telemetry." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "2"
          "library_layer_summary" "Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Profiling: libs/profiling owns parameter profiling over canonical telemetry: datatype profiling; continuous scaling/profile statistics; behavior-family profiling; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Behavior: libs/behavior owns the parameter behavior families used by simulation"
          "library_layers" "Io, Profiling, Perf, Behavior, Config, Pyspark, Common"
          "loc_span" "134"
          "module_name" "pipelines.10_parameter_profiles_fit"
          "purpose" "Fit datatype and scaling profile artifacts from raw telemetry."
          "source_paths" "pipelines/10_parameter_profiles_fit.py"
        }
        }
        stage_12_behavior_profiles_fit = component "12 Behavior Profiles Fit" "Fit behavior primitive and family profile artifacts from raw telemetry." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "2"
          "library_layer_summary" "Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Profiling: libs/profiling owns parameter profiling over canonical telemetry: datatype profiling; continuous scaling/profile statistics; behavior-family profiling; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Behavior: libs/behavior owns the parameter behavior families used by simulation"
          "library_layers" "Io, Profiling, Perf, Behavior, Config, Pyspark, Common"
          "loc_span" "187"
          "module_name" "pipelines.12_behavior_profiles_fit"
          "purpose" "Fit behavior primitive and family profile artifacts from raw telemetry."
          "source_paths" "pipelines/12_behavior_profiles_fit.py"
        }
        }
        stage_15_event_profiles_fit = component "15 Event Profiles Fit" "Fit parameter-level event detector policy profiles from raw telemetry." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "2"
          "library_layer_summary" "Events: libs/events owns canonical event detection and event validation; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder"
          "library_layers" "Events, Io, Perf, Windows, Profiling, Behavior, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "142"
          "module_name" "pipelines.15_event_profiles_fit"
          "purpose" "Fit parameter-level event detector policy profiles from raw telemetry."
          "source_paths" "pipelines/15_event_profiles_fit.py"
        }
        }
        stage_20_events_extract = component "20 Events Extract" "Extract event stream from mixed-rate sensor channels." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Events: libs/events owns canonical event detection and event validation; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder"
          "library_layers" "Events, Io, Perf, Windows, Profiling, Behavior, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "184"
          "module_name" "pipelines.20_events_extract"
          "purpose" "Extract event stream from mixed-rate sensor channels."
          "source_paths" "pipelines/20_events_extract.py"
        }
        }
        stage_25_window_policy_profile = component "25 Window Policy Profile" "Fit a data-driven window policy profile from detected events." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Config: Modules grouped under libs.config"
          "library_layers" "Windows, Io, Perf, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "157"
          "module_name" "pipelines.25_window_policy_profile"
          "purpose" "Fit a data-driven window policy profile from detected events."
          "source_paths" "pipelines/25_window_policy_profile.py"
        }
        }
        stage_30_windows_adaptive = component "30 Windows Adaptive" "Build adaptive windows from event thresholds and max duration." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Config: Modules grouped under libs.config"
          "library_layers" "Windows, Io, Perf, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "164"
          "module_name" "pipelines.30_windows_adaptive"
          "purpose" "Build adaptive windows from event thresholds and max duration."
          "source_paths" "pipelines/30_windows_adaptive.py"
        }
        }
        stage_40_backbone_fit = component "40 Backbone Fit" "Fit backbone artifacts from adaptive windows and raw telemetry." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Backbone: libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance"
          "library_layers" "Windows, Io, Perf, Backbone, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "282"
          "module_name" "pipelines.40_backbone_fit"
          "purpose" "Fit backbone artifacts from adaptive windows and raw telemetry."
          "source_paths" "pipelines/40_backbone_fit.py"
        }
        }
        stage_50_build_graph = component "50 Build Graph" "Build graph component artifacts from backbone, events, and windows." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "7"
          "library_layer_summary" "Graph: libs/graph owns graph-domain models built from telemetry windows and events; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Config: Modules grouped under libs.config"
          "library_layers" "Graph, Io, Perf, Config, Pyspark"
          "loc_span" "547"
          "module_name" "pipelines.50_build_graph"
          "purpose" "Build graph component artifacts from backbone, events, and windows."
          "source_paths" "pipelines/50_build_graph.py"
        }
        }
        stage_60_fit_hierarchy = component "60 Fit Hierarchy" "Fit hierarchy artifacts from fused graph and the persisted graph parameter universe." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "2"
          "library_layer_summary" "Graph: libs/graph owns graph-domain models built from telemetry windows and events; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Config: Modules grouped under libs.config"
          "library_layers" "Graph, Io, Perf, Config, Pyspark"
          "loc_span" "163"
          "module_name" "pipelines.60_fit_hierarchy"
          "purpose" "Fit hierarchy artifacts from fused graph and the persisted graph parameter universe."
          "source_paths" "pipelines/60_fit_hierarchy.py"
        }
        }
        stage_70_phase_fit = component "70 Phase Fit" "Fit phase baselines and assign detected phases to windows." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "2"
          "library_layer_summary" "Phase: libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Backbone: libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance"
          "library_layers" "Phase, Perf, Io, Backbone, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "214"
          "module_name" "pipelines.70_phase_fit"
          "purpose" "Fit phase baselines and assign detected phases to windows."
          "source_paths" "pipelines/70_phase_fit.py"
        }
        }
        stage_72_phase_label_centroids = component "72 Phase Label Centroids" "Build validation-only centroids from truth-labeled phase windows." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Phase: libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Backbone: libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance"
          "library_layers" "Phase, Perf, Io, Backbone, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "142"
          "module_name" "pipelines.72_phase_label_centroids"
          "purpose" "Build validation-only centroids from truth-labeled phase windows."
          "source_paths" "pipelines/72_phase_label_centroids.py"
        }
        }
        stage_80_window_scores_raw = component "80 Window Scores Raw" "Build raw window scores from phase windows and phase baselines." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Phase: libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation; Events: libs/events owns canonical event detection and event validation; Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder; Graph: libs/graph owns graph-domain models built from telemetry windows and events"
          "library_layers" "Phase, Events, Windows, Graph, Scoring, Io, Profiling, Perf, Backbone, Behavior, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "172"
          "module_name" "pipelines.80_window_scores_raw"
          "purpose" "Build raw window scores from phase windows and phase baselines."
          "source_paths" "pipelines/80_window_scores_raw.py"
        }
        }
        stage_85_window_scores_calibrate = component "85 Window Scores Calibrate" "Calibrate raw window scores with phase-conditioned conformal calibration." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Scoring: libs/scoring owns raw and calibrated anomaly scoring over fitted phase and structural artifacts; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Windows: libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder"
          "library_layers" "Scoring, Io, Perf, Windows, Graph, Phase, Profiling, Backbone, Behavior, Events, Config, Spark Sequence, Pyspark, Common"
          "loc_span" "100"
          "module_name" "pipelines.85_window_scores_calibrate"
          "purpose" "Calibrate raw window scores with phase-conditioned conformal calibration."
          "source_paths" "pipelines/85_window_scores_calibrate.py"
        }
        }
        stage_90_anomaly_attribution = component "90 Anomaly Attribution" "Emit anomaly attribution tables for anomalous windows." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Anomaly: libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation; Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Profiling: libs/profiling owns parameter profiling over canonical telemetry: datatype profiling; continuous scaling/profile statistics; behavior-family profiling; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation"
          "library_layers" "Anomaly, Io, Profiling, Perf, Scoring, Behavior, Config, Pyspark, Common"
          "loc_span" "214"
          "module_name" "pipelines.90_anomaly_attribution"
          "purpose" "Emit anomaly attribution tables for anomalous windows."
          "source_paths" "pipelines/90_anomaly_attribution.py"
        }
        }
        stage_95_emit_explorer_bundle = component "95 Emit Explorer Bundle" "Emit a thin explorer-ready bundle for notebook and UI consumers." "Python CLI + PySpark" {
        properties {
          "class_count" "0"
          "function_count" "1"
          "library_layer_summary" "Io: libs/io owns artifact schemas, row contracts, and persistence/bridge utilities; Perf: libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation; Plotting: Plotting library functionality.; Reporting: Modules grouped under libs.reporting"
          "library_layers" "Io, Perf, Plotting, Reporting, Config"
          "loc_span" "147"
          "module_name" "pipelines.95_emit_explorer_bundle"
          "purpose" "Emit a thin explorer-ready bundle for notebook and UI consumers."
          "source_paths" "pipelines/95_emit_explorer_bundle.py"
        }
        }
      }
      simulation_clis = container "Simulation And Utility CLIs" "Canonical simulation runner plus replay, profiling, smoke, and helper scripts." "Python CLI" {
        properties {
          "loc_span" "1829"
          "module_count" "11"
          "source_paths" "scripts/__init__.py,scripts/calibrate_continuous_events.py,scripts/evaluate_sim_event_labels.py,scripts/generate_sample_data.py,scripts/profile_pipeline_performance.py,scripts/report_sim_replay.py,scripts/run_partition_manifest_jobs.py,scripts/run_sim_pipeline.py,scripts/smoke_test_pipeline.py,scripts/sweep_smoke_graph_hierarchy.py,scripts/window_diagnostics.py"
        }
        performance_and_replay = component "Performance And Replay Utilities" "Profiles replayable runs, reports replay boundaries, and runs smoke workflows." "Python CLI" {
        properties {
          "loc_span" "885"
          "module_count" "3"
          "source_paths" "scripts/profile_pipeline_performance.py,scripts/report_sim_replay.py,scripts/smoke_test_pipeline.py"
        }
        }
        simulation_runner = component "Simulation Runner" "Generates simulation telemetry bundles and runs the persisted pipeline over them." "Python CLI" {
        properties {
          "loc_span" "7"
          "module_count" "1"
          "source_paths" "scripts/run_sim_pipeline.py"
        }
        }
      }
    }
    semantics = softwareSystem "Core Library Semantics" "Synthetic semantic views for core-library dataclasses and payload shapes." {
      anomaly_semantics = container "Anomaly Semantics" "libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation" "Python Dataclasses" {
        properties {
          "dataclass_count" "11"
          "source_component" "core_libraries.anomaly"
        }
        truthwindowattributionmatch = component "_TruthWindowAttributionMatch" "Truth Window Attribution Match within anomaly attribution validation against simulator misbehavior truth with fault wrappers. Carries truth_window_id: str, dominant_subsystem_match: bool, dominant_subsystem_mappable: bool, dominant_subsystem_truth: str | None, +10 more." "Dataclass" {
        properties {
          "field_count" "14"
          "module_name" "libs.anomaly.validator"
          "payload_shape" "Carries truth_window_id: str, dominant_subsystem_match: bool, dominant_subsystem_mappable: bool, dominant_subsystem_truth: str | None, +10 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        anomalyartifactset = component "AnomalyArtifactSet" "artifact bundle for Anomaly within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. Carries window_attribution: AnomalyWindowAttributionTable, telemetry_attribution: AnomalyTelemetryAttributionTable, event_attribution: AnomalyEventAttributionTable." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.anomaly.pipeline"
          "payload_shape" "Carries window_attribution: AnomalyWindowAttributionTable, telemetry_attribution: AnomalyTelemetryAttributionTable, event_attribution: AnomalyEventAttributionTable."
          "semantic_kind" "Artifact Bundle"
        }
        }
        detectedlocalizationtruthmap = component "DetectedLocalizationTruthMap" "Detected Localization Truth Map within anomaly attribution validation against simulator misbehavior truth with fault wrappers. Carries detected_to_truth_id: dict[str, str], ambiguous_detected_ids: set[str]." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.anomaly.validator"
          "payload_shape" "Carries detected_to_truth_id: dict[str, str], ambiguous_detected_ids: set[str]."
          "semantic_kind" "Domain Dataclass"
        }
        }
        anomalyattributionplan = component "AnomalyAttributionPlan" "execution plan for Anomaly Attribution within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. Carries top_k_per_subsystem: int = 5." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.anomaly.pipeline"
          "payload_shape" "Carries top_k_per_subsystem: int = 5."
          "semantic_kind" "Execution Plan"
        }
        }
        anomalywindowattributiontable = component "AnomalyWindowAttributionTable" "table artifact for Anomaly Window Attribution within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.anomaly.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        anomalytelemetryattributiontable = component "AnomalyTelemetryAttributionTable" "table artifact for Anomaly Telemetry Attribution within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.anomaly.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        anomalyeventattributiontable = component "AnomalyEventAttributionTable" "table artifact for Anomaly Event Attribution within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.anomaly.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        anomalyparameterlocalizationframe = component "AnomalyParameterLocalizationFrame" "frame artifact for Anomaly Parameter Localization within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.anomaly.frames"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
        anomalypanelcontextframe = component "AnomalyPanelContextFrame" "frame artifact for Anomaly Panel Context within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.anomaly.frames"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
        anomalysubsystemcontextframe = component "AnomalySubsystemContextFrame" "frame artifact for Anomaly Subsystem Context within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.anomaly.frames"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
        anomalyattributioncontextframe = component "AnomalyAttributionContextFrame" "frame artifact for Anomaly Attribution Context within libs/anomaly owns downstream anomaly attribution artifacts and attribution-vs-truth validation. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.anomaly.frames"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
      }
      backbone_semantics = container "Backbone Semantics" "libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance" "Python Dataclasses" {
        properties {
          "dataclass_count" "8"
          "source_component" "core_libraries.backbone"
        }
        backbonesensorenergy = component "BackboneSensorEnergy" "Backbone Sensor Energy within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. Carries parameter_name: str, energy: float, support_count: int, event_prior: float = 0.0, +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.backbone.artifacts"
          "payload_shape" "Carries parameter_name: str, energy: float, support_count: int, event_prior: float = 0.0, +3 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        backbonemodel = component "BackboneModel" "model for Backbone within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. Carries selected_sensors_c: list[str], all_sensors: list[str], weights_b: np.ndarray, lambda_ridge: float, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.backbone.artifacts"
          "payload_shape" "Carries selected_sensors_c: list[str], all_sensors: list[str], weights_b: np.ndarray, lambda_ridge: float, +2 more."
          "semantic_kind" "Model"
        }
        }
        backbonespec = component "BackboneSpec" "specification for Backbone within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. Carries sensor_count: int = 8, ridge_lambda: float = 1.0, event_prior_alpha: float = 0.35, backbone_version: int = 2." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.backbone.artifacts"
          "payload_shape" "Carries sensor_count: int = 8, ridge_lambda: float = 1.0, event_prior_alpha: float = 0.35, backbone_version: int = 2."
          "semantic_kind" "Specification"
        }
        }
        backbonesensorenergytable = component "BackboneSensorEnergyTable" "table artifact for Backbone Sensor Energy within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.backbone.tables"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Table Artifact"
        }
        }
        backbonecrosstermframe = component "BackboneCrossTermFrame" "frame artifact for Backbone Cross Term within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.backbone.tables"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
        backbonegramframe = component "BackboneGramFrame" "frame artifact for Backbone Gram within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.backbone.tables"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
        backboneselectedsensorframe = component "BackboneSelectedSensorFrame" "frame artifact for Backbone Selected Sensor within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.backbone.tables"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Frame Artifact"
        }
        }
        backbonetable = component "BackboneTable" "table artifact for Backbone within libs/backbone owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.backbone.tables"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Table Artifact"
        }
        }
      }
      behavior_semantics = container "Behavior Semantics" "libs/behavior owns the parameter behavior families used by simulation" "Python Dataclasses" {
        properties {
          "dataclass_count" "15"
          "source_component" "core_libraries.behavior"
        }
        behaviorchoicethresholds = component "BehaviorChoiceThresholds" "Behavior Choice Thresholds within shared primitive vocabulary and family scoring for behavior semantics. Carries low_score_threshold: float = 0.38, ambiguous_score_threshold: float = 0.55, ambiguous_margin_threshold: float = 0.03, base_score: float = 0.85, +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.behavior.primitives"
          "payload_shape" "Carries low_score_threshold: float = 0.38, ambiguous_score_threshold: float = 0.55, ambiguous_margin_threshold: float = 0.03, base_score: float = 0.85, +3 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorfamilydefinition = component "BehaviorFamilyDefinition" "Behavior Family Definition within shared primitive vocabulary and family scoring for behavior semantics. Carries family: str, defining_primitives: tuple[str, ...], positive_weights: Mapping[str, float], negative_weights: Mapping[str, float] = field(default_factory=dict), +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.behavior.primitives"
          "payload_shape" "Carries family: str, defining_primitives: tuple[str, ...], positive_weights: Mapping[str, float], negative_weights: Mapping[str, float] = field(default_factory=dict), +3 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        accumulativecontract = component "AccumulativeContract" "Accumulative Contract within accumulative behavior bundle: generator, profiler, validator, and violator. Carries behavior_family: str = 'accumulative', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['accumulative'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['accumulative'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['accumulative'].supported_datatypes, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.accumulative"
          "payload_shape" "Carries behavior_family: str = 'accumulative', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['accumulative'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['accumulative'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['accumulative'].supported_datatypes, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorcontract = component "BehaviorContract" "Behavior Contract within shared protocols and value objects for behavior-local simulation/profiling. Carries behavior_family: str, defining_primitives: tuple[str, ...], expected_traits: tuple[str, ...], supported_datatypes: tuple[str, ...], +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.base"
          "payload_shape" "Carries behavior_family: str, defining_primitives: tuple[str, ...], expected_traits: tuple[str, ...], supported_datatypes: tuple[str, ...], +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorsample = component "BehaviorSample" "Behavior Sample within shared protocols and value objects for behavior-local simulation/profiling. Carries parameter_name: str, parameter_value_clean: object | None, parameter_value: object | None, state: Any = None, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.base"
          "payload_shape" "Carries parameter_name: str, parameter_value_clean: object | None, parameter_value: object | None, state: Any = None, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        discretestatecontract = component "DiscreteStateContract" "Discrete State Contract within discrete-state behavior bundle: generator, profiler, validator, and violator. Carries behavior_family: str = 'discrete_state', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['discrete_state'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['discrete_state'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['discrete_state'].supported_datatypes, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.discrete_state"
          "payload_shape" "Carries behavior_family: str = 'discrete_state', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['discrete_state'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['discrete_state'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['discrete_state'].supported_datatypes, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        inertialcontract = component "InertialContract" "Inertial Contract within inertial behavior bundle: generator, profiler, validator, and violator. Carries behavior_family: str = 'inertial', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['inertial'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['inertial'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['inertial'].supported_datatypes, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.inertial"
          "payload_shape" "Carries behavior_family: str = 'inertial', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['inertial'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['inertial'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['inertial'].supported_datatypes, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        regulatedcontract = component "RegulatedContract" "Regulated Contract within regulated behavior bundle: generator, profiler, validator, and violator. Carries behavior_family: str = 'regulated', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['regulated'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['regulated'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['regulated'].supported_datatypes, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.regulated"
          "payload_shape" "Carries behavior_family: str = 'regulated', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['regulated'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['regulated'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['regulated'].supported_datatypes, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        trackingcontract = component "TrackingContract" "Tracking Contract within tracking behavior bundle: generator, profiler, validator, and violator. Carries behavior_family: str = 'tracking', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['tracking'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['tracking'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['tracking'].supported_datatypes, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.behavior.tracking"
          "payload_shape" "Carries behavior_family: str = 'tracking', defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['tracking'].defining_primitives, expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['tracking'].expected_traits, supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS['tracking'].supported_datatypes, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorprofileresult = component "BehaviorProfileResult" "Behavior Profile Result within shared protocols and value objects for behavior-local simulation/profiling. Carries behavior_family_profiled: str, behavior_profile_confidence: float, score_by_family: Mapping[str, float], profiled_features: Mapping[str, float | str | None]." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.behavior.base"
          "payload_shape" "Carries behavior_family_profiled: str, behavior_profile_confidence: float, score_by_family: Mapping[str, float], profiled_features: Mapping[str, float | str | None]."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorchoicecolumns = component "BehaviorChoiceColumns" "Behavior Choice Columns within shared primitive vocabulary and family scoring for behavior semantics. Carries family: 'Column', confidence: 'Column', mixed_unknown_score: 'Column'." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.behavior.primitives"
          "payload_shape" "Carries family: 'Column', confidence: 'Column', mixed_unknown_score: 'Column'."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorprimitivespec = component "BehaviorPrimitiveSpec" "specification for Behavior Primitive within libs/behavior owns the parameter behavior families used by simulation. Carries name: str, description: str, supported_datatypes: tuple[str, ...] = ()." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.behavior.primitives"
          "payload_shape" "Carries name: str, description: str, supported_datatypes: tuple[str, ...] = ()."
          "semantic_kind" "Specification"
        }
        }
        behaviorstepinput = component "BehaviorStepInput" "Behavior Step Input within shared protocols and value objects for behavior-local simulation/profiling. Carries dt_seconds: float, latent_state: Mapping[str, float], context: Mapping[str, Any] = field(default_factory=dict)." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.behavior.base"
          "payload_shape" "Carries dt_seconds: float, latent_state: Mapping[str, float], context: Mapping[str, Any] = field(default_factory=dict)."
          "semantic_kind" "Domain Dataclass"
        }
        }
        familyvalidator = component "FamilyValidator" "Family Validator within shared validator helpers for behavior-family contracts. Carries expected_family: str." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.behavior.validation"
          "payload_shape" "Carries expected_family: str."
          "semantic_kind" "Domain Dataclass"
        }
        }
        behaviorregistry = component "BehaviorRegistry" "Behavior Registry within registry for behavior-local generator/profiler/validator/violator bundles. Carries _behaviors: dict[str, Behavior] = field(default_factory=dict)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.behavior.registry"
          "payload_shape" "Carries _behaviors: dict[str, Behavior] = field(default_factory=dict)."
          "semantic_kind" "Domain Dataclass"
        }
        }
      }
      config_semantics = container "Config Semantics" "Owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses" "Python Dataclasses" {
        properties {
          "dataclass_count" "17"
          "source_component" "core_libraries.config"
        }
        pipelineartifactpaths = component "PipelineArtifactPaths" "Pipeline Artifact Paths within typed pipeline configuration derived from defaults.yaml plus env overrides. Carries raw_input: str, raw_table: str, parameter_datatype_profile: str, continuous_scaling_profile: str, +27 more." "Dataclass" {
        properties {
          "field_count" "31"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries raw_input: str, raw_table: str, parameter_datatype_profile: str, continuous_scaling_profile: str, +27 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        eventsettings = component "EventSettings" "configuration for Event within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries delta_threshold: float, slope_source: str, ema_alpha: float, slope_threshold_mode: str, +11 more." "Dataclass" {
        properties {
          "field_count" "15"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries delta_threshold: float, slope_source: str, ema_alpha: float, slope_threshold_mode: str, +11 more."
          "semantic_kind" "Configuration"
        }
        }
        pipelinecontextsettings = component "PipelineContextSettings" "configuration for Pipeline Context within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries profiling: ProfilingSettings, events: EventSettings, windowing: WindowingSettings, backbone: BackboneSettings, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries profiling: ProfilingSettings, events: EventSettings, windowing: WindowingSettings, backbone: BackboneSettings, +5 more."
          "semantic_kind" "Configuration"
        }
        }
        profilingsettings = component "ProfilingSettings" "configuration for Profiling within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries numeric_ratio_threshold: float, categorical_cardinality_max: int, behavior_significant_diff_threshold: float, behavior_center_band_width: float, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries numeric_ratio_threshold: float, categorical_cardinality_max: int, behavior_significant_diff_threshold: float, behavior_center_band_width: float, +5 more."
          "semantic_kind" "Configuration"
        }
        }
        graphsettings = component "GraphSettings" "configuration for Graph within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries precision_ridge_lambda: float, min_abs_partial_corr: float, max_sensor_universe: int, event: EventGraphSettings, +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries precision_ridge_lambda: float, min_abs_partial_corr: float, max_sensor_universe: int, event: EventGraphSettings, +3 more."
          "semantic_kind" "Configuration"
        }
        }
        phasesettings = component "PhaseSettings" "configuration for Phase within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries phase_count: int, detect_sensor_count: int, detect_event_type_count: int, detect_categorical_state_count: int, +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries phase_count: int, detect_sensor_count: int, detect_event_type_count: int, detect_categorical_state_count: int, +3 more."
          "semantic_kind" "Configuration"
        }
        }
        windowingsettings = component "WindowingSettings" "configuration for Windowing within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries min_sampling_rate_hz: float, max_ms: int, min_ms: int, event_threshold: int, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries min_sampling_rate_hz: float, max_ms: int, min_ms: int, event_threshold: int, +2 more."
          "semantic_kind" "Configuration"
        }
        }
        laggraphsettings = component "LagGraphSettings" "configuration for Lag Graph within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries tau_max_seconds: float, min_count: int, max_mean_lag_seconds: float | None, top_k_outgoing: int, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries tau_max_seconds: float, min_count: int, max_mean_lag_seconds: float | None, top_k_outgoing: int, +1 more."
          "semantic_kind" "Configuration"
        }
        }
        backbonesettings = component "BackboneSettings" "configuration for Backbone within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries sensor_count: int, ridge_lambda: float, max_sensor_universe: int, event_prior_alpha: float." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries sensor_count: int, ridge_lambda: float, max_sensor_universe: int, event_prior_alpha: float."
          "semantic_kind" "Configuration"
        }
        }
        graphfusionsettings = component "GraphFusionSettings" "configuration for Graph Fusion within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries alpha: float, beta: float, gamma: float, min_fused_edge_weight: float." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries alpha: float, beta: float, gamma: float, min_fused_edge_weight: float."
          "semantic_kind" "Configuration"
        }
        }
        lagbandsettings = component "LagBandSettings" "configuration for Lag Band within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries name: str, lower_seconds: float, upper_seconds: float, combine_weight: float." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries name: str, lower_seconds: float, upper_seconds: float, combine_weight: float."
          "semantic_kind" "Configuration"
        }
        }
        pipelineexecutionsettings = component "PipelineExecutionSettings" "configuration for Pipeline Execution within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries table_format: str, raw_output_format: str, write_mode: str, fit_write_mode: str." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries table_format: str, raw_output_format: str, write_mode: str, fit_write_mode: str."
          "semantic_kind" "Configuration"
        }
        }
        eventgraphsettings = component "EventGraphSettings" "configuration for Event Graph within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries min_count: int, min_npmi: float, top_k_per_parameter_name: int." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries min_count: int, min_npmi: float, top_k_per_parameter_name: int."
          "semantic_kind" "Configuration"
        }
        }
        hierarchysettings = component "HierarchySettings" "configuration for Hierarchy within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries top_k_per_parameter_name: int, subsystem_min_edge_weight: float | None, system_min_edge_weight: float | None." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries top_k_per_parameter_name: int, subsystem_min_edge_weight: float | None, system_min_edge_weight: float | None."
          "semantic_kind" "Configuration"
        }
        }
        scoringsettings = component "ScoringSettings" "configuration for Scoring within owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses. Carries max_bridge_reference_rows: int, min_warm: int." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.config.pipeline"
          "payload_shape" "Carries max_bridge_reference_rows: int, min_warm: int."
          "semantic_kind" "Configuration"
        }
        }
        additional_dataclasses = component "Additional Dataclasses" "2 more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"
      }
      events_semantics = container "Events Semantics" "libs/events owns canonical event detection and event validation" "Python Dataclasses" {
        properties {
          "dataclass_count" "35"
          "source_component" "core_libraries.events"
        }
        continuousdetectorconfig = component "ContinuousDetectorConfig" "configuration for Continuous Detector within libs/events owns canonical event detection and event validation. Carries delta_threshold: float = 0.0, ema_alpha: float = 0.35, slope_source: str = 'ema', slope_threshold_mode: str = 'fixed', +33 more." "Dataclass" {
        properties {
          "field_count" "37"
          "module_name" "libs.events.continuous"
          "payload_shape" "Carries delta_threshold: float = 0.0, ema_alpha: float = 0.35, slope_source: str = 'ema', slope_threshold_mode: str = 'fixed', +33 more."
          "semantic_kind" "Configuration"
        }
        }
        eventprofileconfig = component "EventProfileConfig" "Base detector settings and generic morphology-policy gains. Carries slope_source: str = 'ema', slope_threshold_mode: str = 'fixed', slope_threshold_quantile: float = 0.75, slope_threshold_scale: float = 0.35, +9 more." "Dataclass" {
        properties {
          "field_count" "13"
          "module_name" "libs.events.profiling"
          "payload_shape" "Carries slope_source: str = 'ema', slope_threshold_mode: str = 'fixed', slope_threshold_quantile: float = 0.75, slope_threshold_scale: float = 0.35, +9 more."
          "semantic_kind" "Configuration"
        }
        }
        continuoussequencestatelayout = component "ContinuousSequenceStateLayout" "Continuous Sequence State Layout within continuous-channel event detection over spark dataframes. Carries last_switch_index: str = 'last_switch_index', last_oscillation_index: str = 'last_oscillation_index', last_drift_guard_index: str = 'last_drift_guard_index', drift_guard_cum_abs: str = 'drift_guard_cum_abs', +6 more." "Dataclass" {
        properties {
          "field_count" "10"
          "module_name" "libs.events.continuous"
          "payload_shape" "Carries last_switch_index: str = 'last_switch_index', last_oscillation_index: str = 'last_oscillation_index', last_drift_guard_index: str = 'last_drift_guard_index', drift_guard_cum_abs: str = 'drift_guard_cum_abs', +6 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        continuouseventcalibrationspec = component "ContinuousEventCalibrationSpec" "specification for Continuous Event Calibration within libs/events owns canonical event detection and event validation. Carries slope_sources: tuple[str, ...] = ('ema', 'raw'), ema_alphas: tuple[float, ...] = (0.2, 0.35, 0.5), slope_abs_thresholds: tuple[float, ...] = (0.0, 0.5, 1.0), delta_threshold: float = 0.0, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.events.calibration"
          "payload_shape" "Carries slope_sources: tuple[str, ...] = ('ema', 'raw'), ema_alphas: tuple[float, ...] = (0.2, 0.35, 0.5), slope_abs_thresholds: tuple[float, ...] = (0.0, 0.5, 1.0), delta_threshold: float = 0.0, +5 more."
          "semantic_kind" "Specification"
        }
        }
        labeledsloperun = component "_LabeledSlopeRun" "Labeled Slope Run within streaming and summary validators for detector outputs against simulator labels. Carries event_key: tuple[str, str, str], family_name: str, row_indexes: tuple[int, ...], label_row_indexes: tuple[int, ...], +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.events.validator"
          "payload_shape" "Carries event_key: tuple[str, str, str], family_name: str, row_indexes: tuple[int, ...], label_row_indexes: tuple[int, ...], +3 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        categoricaldetectorconfig = component "CategoricalDetectorConfig" "configuration for Categorical Detector within libs/events owns canonical event detection and event validation. Carries min_dwell_seconds: float = 0.0, max_dwell_seconds: float = 0.0, emit_state_enter: bool = True, emit_state_exit: bool = True, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.events.categorical"
          "payload_shape" "Carries min_dwell_seconds: float = 0.0, max_dwell_seconds: float = 0.0, emit_state_enter: bool = True, emit_state_exit: bool = True, +2 more."
          "semantic_kind" "Configuration"
        }
        }
        categoricalsequencestatelayout = component "CategoricalSequenceStateLayout" "Categorical Sequence State Layout within categorical transition and missing/dropped event detection. Carries last_state: str = 'last_state', last_state_ts: str = 'last_state_ts', last_dwell_guard_ts: str = 'last_dwell_guard_ts', missing: str = 'missing', +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.events.categorical"
          "payload_shape" "Carries last_state: str = 'last_state', last_state_ts: str = 'last_state_ts', last_dwell_guard_ts: str = 'last_dwell_guard_ts', missing: str = 'missing', +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        eventmatchresult = component "EventMatchResult" "Event Match Result within streaming and summary validators for detector outputs against simulator labels. Carries matched_label_ids: frozenset[int], matched_det_ids: frozenset[int], matched_deltas_seconds: tuple[float, ...], nearest_label_delta_by_id: dict[int, float], +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.events.validator"
          "payload_shape" "Carries matched_label_ids: frozenset[int], matched_det_ids: frozenset[int], matched_deltas_seconds: tuple[float, ...], nearest_label_delta_by_id: dict[int, float], +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        continuouseventdetector = component "ContinuousEventDetector" "Continuous Event Detector within continuous-channel event detection over spark dataframes. Carries config: ContinuousDetectorConfig = field(default_factory=ContinuousDetectorConfig), state_layout: ContinuousSequenceStateLayout = field(default_factory=ContinuousSequenceStateLayout), sequence_plan: SegmentedSequencePlan = field(default_factory=lambda: SegmentedSequencePlan(ordering=SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id', 'parameter_name'), order_columns=('sample_seq_id',), timestamp_column='timestamp_utc', row_number_column='sample_seq_id'), policy=_default_event_segment_policy()))." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.events.continuous"
          "payload_shape" "Carries config: ContinuousDetectorConfig = field(default_factory=ContinuousDetectorConfig), state_layout: ContinuousSequenceStateLayout = field(default_factory=ContinuousSequenceStateLayout), sequence_plan: SegmentedSequencePlan = field(default_factory=lambda: SegmentedSequencePlan(ordering=SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id', 'parameter_name'), order_columns=('sample_seq_id',), timestamp_column='timestamp_utc', row_number_column='sample_seq_id'), policy=_default_event_segment_policy()))."
          "semantic_kind" "Domain Dataclass"
        }
        }
        categoricaleventdetector = component "CategoricalEventDetector" "Categorical Event Detector within categorical transition and missing/dropped event detection. Carries config: CategoricalDetectorConfig = field(default_factory=CategoricalDetectorConfig), state_layout: CategoricalSequenceStateLayout = field(default_factory=CategoricalSequenceStateLayout), sequence_plan: SegmentedSequencePlan = field(default_factory=lambda: SegmentedSequencePlan(ordering=SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id', 'parameter_name'), order_columns=('sample_seq_id',), timestamp_column='timestamp_utc', row_number_column='sample_seq_id'), policy=_default_event_segment_policy()))." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.events.categorical"
          "payload_shape" "Carries config: CategoricalDetectorConfig = field(default_factory=CategoricalDetectorConfig), state_layout: CategoricalSequenceStateLayout = field(default_factory=CategoricalSequenceStateLayout), sequence_plan: SegmentedSequencePlan = field(default_factory=lambda: SegmentedSequencePlan(ordering=SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id', 'parameter_name'), order_columns=('sample_seq_id',), timestamp_column='timestamp_utc', row_number_column='sample_seq_id'), policy=_default_event_segment_policy()))."
          "semantic_kind" "Domain Dataclass"
        }
        }
        eventsourceframe = component "EventSourceFrame" "frame artifact for Event Source within libs/events owns canonical event detection and event validation. Carries numeric_df: 'DataFrame', categorical_df: 'DataFrame', ordering: EventOrderingPolicy = field(default_factory=EventOrderingPolicy)." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.events.pipeline"
          "payload_shape" "Carries numeric_df: 'DataFrame', categorical_df: 'DataFrame', ordering: EventOrderingPolicy = field(default_factory=EventOrderingPolicy)."
          "semantic_kind" "Frame Artifact"
        }
        }
        eventdetectionplan = component "EventDetectionPlan" "execution plan for Event Detection within libs/events owns canonical event detection and event validation. Carries continuous_detector: ContinuousEventDetector, categorical_detector: CategoricalEventDetector = field(default_factory=CategoricalEventDetector), ordering: EventOrderingPolicy = field(default_factory=EventOrderingPolicy)." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.events.pipeline"
          "payload_shape" "Carries continuous_detector: ContinuousEventDetector, categorical_detector: CategoricalEventDetector = field(default_factory=CategoricalEventDetector), ordering: EventOrderingPolicy = field(default_factory=EventOrderingPolicy)."
          "semantic_kind" "Execution Plan"
        }
        }
        eventorderingpolicy = component "EventOrderingPolicy" "policy for Event Ordering within libs/events owns canonical event detection and event validation. Carries source_ordering: SequenceOrderingPolicy = field(default_factory=lambda: SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id', 'parameter_name'), order_columns=('timestamp_utc', 'parameter_value', 'value_num'), timestamp_column='timestamp_utc', row_number_column='sample_seq_id')), event_ordering: SequenceOrderingPolicy = field(default_factory=lambda: SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id'), order_columns=('timestamp_utc', 'parameter_name', 'event_type_detected', 'payload_json'), timestamp_column='timestamp_utc', row_number_column='event_seq_id'))." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.events.pipeline"
          "payload_shape" "Carries source_ordering: SequenceOrderingPolicy = field(default_factory=lambda: SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id', 'parameter_name'), order_columns=('timestamp_utc', 'parameter_value', 'value_num'), timestamp_column='timestamp_utc', row_number_column='sample_seq_id')), event_ordering: SequenceOrderingPolicy = field(default_factory=lambda: SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id'), order_columns=('timestamp_utc', 'parameter_name', 'event_type_detected', 'payload_json'), timestamp_column='timestamp_utc', row_number_column='event_seq_id'))."
          "semantic_kind" "Policy"
        }
        }
        eventartifactset = component "EventArtifactSet" "artifact bundle for Event within libs/events owns canonical event detection and event validation. Carries source_frame: EventSourceFrame, events: EventsTable." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.events.pipeline"
          "payload_shape" "Carries source_frame: EventSourceFrame, events: EventsTable."
          "semantic_kind" "Artifact Bundle"
        }
        }
        sloperunsummary = component "_SlopeRunSummary" "Slope Run Summary within streaming and summary validators for detector outputs against simulator labels. Carries family_name: str, row_indexes: tuple[int, ...]." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.events.validator"
          "payload_shape" "Carries family_name: str, row_indexes: tuple[int, ...]."
          "semantic_kind" "Domain Dataclass"
        }
        }
        additional_dataclasses = component "Additional Dataclasses" "20 more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"
      }
      graph_semantics = container "Graph Semantics" "libs/graph owns graph-domain models built from telemetry windows and events" "Python Dataclasses" {
        properties {
          "dataclass_count" "24"
          "source_component" "core_libraries.graph"
        }
        hierarchyspec = component "HierarchySpec" "specification for Hierarchy within libs/graph owns graph-domain models built from telemetry windows and events. Carries min_edge_weight: float = 0.05, top_k_per_parameter_name: int = 3, subsystem_min_edge_weight: float | None = None, system_min_edge_weight: float | None = None." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.graph.hierarchy_artifacts"
          "payload_shape" "Carries min_edge_weight: float = 0.05, top_k_per_parameter_name: int = 3, subsystem_min_edge_weight: float | None = None, system_min_edge_weight: float | None = None."
          "semantic_kind" "Specification"
        }
        }
        lagbandspec = component "LagBandSpec" "specification for Lag Band within libs/graph owns graph-domain models built from telemetry windows and events. Carries name: str, lower_seconds: float, upper_seconds: float, combine_weight: float." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.graph.lag"
          "payload_shape" "Carries name: str, lower_seconds: float, upper_seconds: float, combine_weight: float."
          "semantic_kind" "Specification"
        }
        }
        eventgraphspec = component "EventGraphSpec" "specification for Event Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries min_count: int = 1, min_npmi: float = 0.0, top_k_per_parameter_name: int = 8." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.graph.event"
          "payload_shape" "Carries min_count: int = 1, min_npmi: float = 0.0, top_k_per_parameter_name: int = 8."
          "semantic_kind" "Specification"
        }
        }
        fusedgraphspec = component "FusedGraphSpec" "specification for Fused Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries alpha: float = 1.0, beta: float = 1.0, gamma: float = 1.0." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.graph.fused"
          "payload_shape" "Carries alpha: float = 1.0, beta: float = 1.0, gamma: float = 1.0."
          "semantic_kind" "Specification"
        }
        }
        graphbuildstepdiagnostics = component "GraphBuildStepDiagnostics" "Graph Build Step Diagnostics within graph artifact builders for spark fitting stages. Carries step_name: str, row_count: int, timing_ms: float." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.graph.pipeline"
          "payload_shape" "Carries step_name: str, row_count: int, timing_ms: float."
          "semantic_kind" "Domain Dataclass"
        }
        }
        graphstageevaluationspec = component "GraphStageEvaluationSpec" "specification for Graph Stage Evaluation within libs/graph owns graph-domain models built from telemetry windows and events. Carries stability_sample_fraction: float = 0.8, stability_sample_count: int = 2, stability_hash_modulus: int = 10." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.graph.evaluation"
          "payload_shape" "Carries stability_sample_fraction: float = 0.8, stability_sample_count: int = 2, stability_hash_modulus: int = 10."
          "semantic_kind" "Specification"
        }
        }
        precisiongraphspec = component "PrecisionGraphSpec" "specification for Precision Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries selected_sensors: tuple[str, ...], ridge_lambda: float = 1.0, min_abs_partial_corr: float = 0.05." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.graph.precision"
          "payload_shape" "Carries selected_sensors: tuple[str, ...], ridge_lambda: float = 1.0, min_abs_partial_corr: float = 0.05."
          "semantic_kind" "Specification"
        }
        }
        graphhierarchy = component "GraphHierarchy" "Graph Hierarchy within libs/graph owns graph-domain models built from telemetry windows and events. Carries spec: HierarchySpec, rows: pd.DataFrame." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.hierarchy_artifacts"
          "payload_shape" "Carries spec: HierarchySpec, rows: pd.DataFrame."
          "semantic_kind" "Domain Dataclass"
        }
        }
        eventgraph = component "EventGraph" "Event Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries spec: EventGraphSpec, edges: pd.DataFrame." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.event"
          "payload_shape" "Carries spec: EventGraphSpec, edges: pd.DataFrame."
          "semantic_kind" "Domain Dataclass"
        }
        }
        precisiongraph = component "PrecisionGraph" "Precision Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries spec: PrecisionGraphSpec, edges: pd.DataFrame." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.precision"
          "payload_shape" "Carries spec: PrecisionGraphSpec, edges: pd.DataFrame."
          "semantic_kind" "Domain Dataclass"
        }
        }
        transitiongraph = component "TransitionGraph" "Transition Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries spec: TransitionGraphSpec, edges: pd.DataFrame." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.transition"
          "payload_shape" "Carries spec: TransitionGraphSpec, edges: pd.DataFrame."
          "semantic_kind" "Domain Dataclass"
        }
        }
        fusedgraph = component "FusedGraph" "Fused Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries spec: FusedGraphSpec, edges: pd.DataFrame." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.fused"
          "payload_shape" "Carries spec: FusedGraphSpec, edges: pd.DataFrame."
          "semantic_kind" "Domain Dataclass"
        }
        }
        graphbuilddiagnostics = component "GraphBuildDiagnostics" "Graph Build Diagnostics within graph artifact builders for spark fitting stages. Carries steps: list[GraphBuildStepDiagnostics], total_timing_ms: float." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.pipeline"
          "payload_shape" "Carries steps: list[GraphBuildStepDiagnostics], total_timing_ms: float."
          "semantic_kind" "Domain Dataclass"
        }
        }
        modulecompatibilityprofile = component "ModuleCompatibilityProfile" "profile for Module Compatibility within libs/graph owns graph-domain models built from telemetry windows and events. Carries datatype: str | None = None, behavior_family: str | None = None." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.graph.hierarchy_artifacts"
          "payload_shape" "Carries datatype: str | None = None, behavior_family: str | None = None."
          "semantic_kind" "Profile"
        }
        }
        transitiongraphspec = component "TransitionGraphSpec" "specification for Transition Graph within libs/graph owns graph-domain models built from telemetry windows and events. Carries min_count: int = 1." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.graph.transition"
          "payload_shape" "Carries min_count: int = 1."
          "semantic_kind" "Specification"
        }
        }
        additional_dataclasses = component "Additional Dataclasses" "9 more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"
      }
      perf_semantics = container "Perf Semantics" "libs/perf owns operational instrumentation helpers: MLflow integration; wall-time logging; memory observability snapshots; stage-manifest generation" "Python Dataclasses" {
        properties {
          "dataclass_count" "2"
          "source_component" "core_libraries.perf"
        }
        stagemanifest = component "StageManifest" "Stage Manifest within stage artifact manifest helpers for replayable v2 pipeline stages. Carries stage_name: str, config: dict[str, Any], input_artifacts: dict[str, dict[str, Any]], output_artifacts: dict[str, dict[str, Any]], +6 more." "Dataclass" {
        properties {
          "field_count" "10"
          "module_name" "libs.perf.stage_manifest"
          "payload_shape" "Carries stage_name: str, config: dict[str, Any], input_artifacts: dict[str, dict[str, Any]], output_artifacts: dict[str, dict[str, Any]], +6 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        artifactmanifest = component "ArtifactManifest" "Artifact Manifest within stage artifact manifest helpers for replayable v2 pipeline stages. Carries path: str, schema_hash: str, schema: dict[str, Any], row_count: int | None = None, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.perf.stage_manifest"
          "payload_shape" "Carries path: str, schema_hash: str, schema: dict[str, Any], row_count: int | None = None, +2 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
      }
      phase_semantics = container "Phase Semantics" "libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation" "Python Dataclasses" {
        properties {
          "dataclass_count" "16"
          "source_component" "core_libraries.phase"
        }
        phaseplanconfig = component "PhasePlanConfig" "configuration for Phase Plan within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries phase_count: int, phase_stable_drift_quantile: float = 0.35, phase_transition_penalty: float = 1.5, phase_min_dwell_windows: int = 8, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries phase_count: int, phase_stable_drift_quantile: float = 0.35, phase_transition_penalty: float = 1.5, phase_min_dwell_windows: int = 8, +5 more."
          "semantic_kind" "Configuration"
        }
        }
        phaseclustermodel = component "PhaseClusterModel" "model for Phase Cluster within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries feature_stats_df: 'DataFrame', centroids_df: 'DataFrame', distance_scales_df: 'DataFrame', transition_model: PhaseTransitionModel, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries feature_stats_df: 'DataFrame', centroids_df: 'DataFrame', distance_scales_df: 'DataFrame', transition_model: PhaseTransitionModel, +2 more."
          "semantic_kind" "Model"
        }
        }
        phasefeatureconfig = component "PhaseFeatureConfig" "configuration for Phase Feature within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries backbone_model: BackboneModel, phase_selected_sensors: list[str], phase_selected_event_types: list[str], phase_selected_categorical_state_pairs: list[tuple[str, str]], +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.phase.feature_config"
          "payload_shape" "Carries backbone_model: BackboneModel, phase_selected_sensors: list[str], phase_selected_event_types: list[str], phase_selected_categorical_state_pairs: list[tuple[str, str]], +1 more."
          "semantic_kind" "Configuration"
        }
        }
        phasefeatureselectiondiagnostics = component "PhaseFeatureSelectionDiagnostics" "Phase Feature Selection Diagnostics within phase artifact and plan dataclasses. Carries sensors: PhaseSelectorDiagnostics, event_types: PhaseSelectorDiagnostics, categorical_state_pairs: PhaseSelectorDiagnostics, selected_event_types: list[str], +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries sensors: PhaseSelectorDiagnostics, event_types: PhaseSelectorDiagnostics, categorical_state_pairs: PhaseSelectorDiagnostics, selected_event_types: list[str], +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        phasetransitionmodel = component "PhaseTransitionModel" "model for Phase Transition within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries support_df: 'DataFrame', policy_name: str = 'monotone_progress_band', canonical_order_source: str = 'seed_bucket', progress_support_source: str = 'seed_progress_mass_position_span', +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries support_df: 'DataFrame', policy_name: str = 'monotone_progress_band', canonical_order_source: str = 'seed_bucket', progress_support_source: str = 'seed_progress_mass_position_span', +1 more."
          "semantic_kind" "Model"
        }
        }
        phaseartifactset = component "PhaseArtifactSet" "artifact bundle for Phase within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries phase_windows: PhaseWindowsTable, phase_baselines: PhaseBaselinesTable, phase_config: 'PhaseFeatureConfig', feature_frame: 'PhaseFeatureFrame | None' = None, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries phase_windows: PhaseWindowsTable, phase_baselines: PhaseBaselinesTable, phase_config: 'PhaseFeatureConfig', feature_frame: 'PhaseFeatureFrame | None' = None, +1 more."
          "semantic_kind" "Artifact Bundle"
        }
        }
        phasedetectionrun = component "PhaseDetectionRun" "Phase Detection Run within phase artifact and plan dataclasses. Carries phase_config: 'PhaseFeatureConfig', feature_frame: 'PhaseFeatureFrame', cluster_model: PhaseClusterModel, phase_windows: PhaseWindowsTable, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries phase_config: 'PhaseFeatureConfig', feature_frame: 'PhaseFeatureFrame', cluster_model: PhaseClusterModel, phase_windows: PhaseWindowsTable, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        phaseselectordiagnostics = component "PhaseSelectorDiagnostics" "Phase Selector Diagnostics within phase artifact and plan dataclasses. Carries selector_name: str, selected_count: int, timing_ms: float, candidate_count: int | None = None, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries selector_name: str, selected_count: int, timing_ms: float, candidate_count: int | None = None, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        phasesequencestate = component "PhaseSequenceState" "runtime state for Phase Sequence within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries score_column: str = 'phase_scores', path_column: str = 'phase_paths', initialized_column: str = 'initialized'." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.phase.decode"
          "payload_shape" "Carries score_column: str = 'phase_scores', path_column: str = 'phase_paths', initialized_column: str = 'initialized'."
          "semantic_kind" "Runtime State"
        }
        }
        phasefeatureselectionpolicy = component "PhaseFeatureSelectionPolicy" "policy for Phase Feature Selection within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries sensor_count: int = 8, event_type_count: int = 6, categorical_state_count: int = 6." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.phase.types"
          "payload_shape" "Carries sensor_count: int = 8, event_type_count: int = 6, categorical_state_count: int = 6."
          "semantic_kind" "Policy"
        }
        }
        phasefeatureframe = component "PhaseFeatureFrame" "frame artifact for Phase Feature within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries feature_names: list[str]." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.phase.frames"
          "payload_shape" "Carries feature_names: list[str]."
          "semantic_kind" "Frame Artifact"
        }
        }
        phasebaselinestable = component "PhaseBaselinesTable" "table artifact for Phase Baselines within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.phase.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        phaselabelcentroidstable = component "PhaseLabelCentroidsTable" "table artifact for Phase Label Centroids within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.phase.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        phasewindowstable = component "PhaseWindowsTable" "table artifact for Phase Windows within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.phase.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        phasedetectionplan = component "PhaseDetectionPlan" "execution plan for Phase Detection within libs/phase owns phase feature selection, phase detection, phase analysis, and phase validation. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.phase.pipeline"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Execution Plan"
        }
        }
        additional_dataclasses = component "Additional Dataclasses" "1 more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"
      }
      profiling_semantics = container "Profiling Semantics" "libs/profiling owns parameter profiling over canonical telemetry: datatype profiling; continuous scaling/profile statistics; behavior-family profiling" "Python Dataclasses" {
        properties {
          "dataclass_count" "9"
          "source_component" "core_libraries.profiling"
        }
        telemetryprofilingplan = component "TelemetryProfilingPlan" "execution plan for Telemetry Profiling within libs/profiling owns parameter profiling over canonical telemetry: datatype profiling; continuous scaling/profile statistics; behavior-family profiling. Carries source: TelemetryProfileSource, numeric_ratio_threshold: float = 0.8, categorical_cardinality_max: int = 200, behavior_significant_diff_threshold: float = ParameterBehaviorPrimitiveProfile.NUMERIC_SIGNIFICANT_DIFF_THRESHOLD, +6 more." "Dataclass" {
        properties {
          "field_count" "10"
          "module_name" "libs.profiling.pipeline"
          "payload_shape" "Carries source: TelemetryProfileSource, numeric_ratio_threshold: float = 0.8, categorical_cardinality_max: int = 200, behavior_significant_diff_threshold: float = ParameterBehaviorPrimitiveProfile.NUMERIC_SIGNIFICANT_DIFF_THRESHOLD, +6 more."
          "semantic_kind" "Execution Plan"
        }
        }
        telemetryprofilingartifacts = component "TelemetryProfilingArtifacts" "Telemetry Profiling Artifacts within class-oriented profiling artifact builders for the active spark path. Carries datatype_profile: ParameterDatatypeProfile, scaling_profile: ContinuousScalingProfile, primitive_profile: ParameterBehaviorPrimitiveProfile, behavior_profile: ParameterBehaviorProfile." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.profiling.pipeline"
          "payload_shape" "Carries datatype_profile: ParameterDatatypeProfile, scaling_profile: ContinuousScalingProfile, primitive_profile: ParameterBehaviorPrimitiveProfile, behavior_profile: ParameterBehaviorProfile."
          "semantic_kind" "Artifact Bundle"
        }
        }
        telemetryprofilesource = component "TelemetryProfileSource" "Canonical raw-telemetry view used by production profiling builders. Carries raw_input_df: 'DataFrame'." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "Carries raw_input_df: 'DataFrame'."
          "semantic_kind" "Domain Dataclass"
        }
        }
        parameterbehaviorprimitiveprofile = component "ParameterBehaviorPrimitiveProfile" "Shared primitive evidence profile derived directly from raw telemetry. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Profile"
        }
        }
        parameterprofile = component "ParameterProfile" "Observed telemetry statistics used to derive profiling artifacts. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Profile"
        }
        }
        parameterbehaviorprofile = component "ParameterBehaviorProfile" "Canonical behavior-family profile artifact. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Profile"
        }
        }
        continuousscalingprofile = component "ContinuousScalingProfile" "Robust scaling metadata for continuous parameters. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Profile"
        }
        }
        parameterdatatypeprofile = component "ParameterDatatypeProfile" "Canonical datatype profile artifact. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Profile"
        }
        }
        categoricaldistribution = component "CategoricalDistribution" "Top observed categorical values per parameter. No extracted dataclass fields." "Dataclass" {
        properties {
          "field_count" "0"
          "module_name" "libs.profiling.profiles"
          "payload_shape" "No extracted dataclass fields."
          "semantic_kind" "Domain Dataclass"
        }
        }
      }
      pyspark_semantics = container "Pyspark Semantics" "Owns typed Spark frame and table wrappers used at repository boundaries" "Python Dataclasses" {
        properties {
          "dataclass_count" "2"
          "source_component" "core_libraries.pyspark"
        }
        table = component "Table" "table artifact for Table within owns typed spark frame and table wrappers used at repository boundaries. Carries path: str = '', format: str = 'delta', partition_by: tuple[str, ...] = ()." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.pyspark.table"
          "payload_shape" "Carries path: str = '', format: str = 'delta', partition_by: tuple[str, ...] = ()."
          "semantic_kind" "Table Artifact"
        }
        }
        frame = component "Frame" "frame artifact for Frame within owns typed spark frame and table wrappers used at repository boundaries. Carries dataframe: 'DataFrame'." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.pyspark.frame"
          "payload_shape" "Carries dataframe: 'DataFrame'."
          "semantic_kind" "Frame Artifact"
        }
        }
      }
      reporting_semantics = container "Reporting Semantics" "Owns thin report payload wrappers used by generated summaries and diagnostics" "Python Dataclasses" {
        properties {
          "dataclass_count" "1"
          "source_component" "core_libraries.reporting"
        }
        reportframe = component "ReportFrame" "frame artifact for Report within owns thin report payload wrappers used by generated summaries and diagnostics. Carries dataframe: pd.DataFrame." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.reporting.frame"
          "payload_shape" "Carries dataframe: pd.DataFrame."
          "semantic_kind" "Frame Artifact"
        }
        }
      }
      scoring_semantics = container "Scoring Semantics" "libs/scoring owns raw and calibrated anomaly scoring over fitted phase and structural artifacts" "Python Dataclasses" {
        properties {
          "dataclass_count" "3"
          "source_component" "core_libraries.scoring"
        }
        localizedhierarchysupportframes = component "_LocalizedHierarchySupportFrames" "Localized Hierarchy Support Frames within typed spark tables for scoring artifacts. Carries module_ranked_df: 'DataFrame', dominant_modules_df: 'DataFrame', subsystem_ranked_df: 'DataFrame', dominant_subsystems_df: 'DataFrame'." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.scoring.tables"
          "payload_shape" "Carries module_ranked_df: 'DataFrame', dominant_modules_df: 'DataFrame', subsystem_ranked_df: 'DataFrame', dominant_subsystems_df: 'DataFrame'."
          "semantic_kind" "Domain Dataclass"
        }
        }
        windowscoresrawtable = component "WindowScoresRawTable" "table artifact for Window Scores Raw within libs/scoring owns raw and calibrated anomaly scoring over fitted phase and structural artifacts. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.scoring.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
        windowscorescalibratedtable = component "WindowScoresCalibratedTable" "table artifact for Window Scores Calibrated within libs/scoring owns raw and calibrated anomaly scoring over fitted phase and structural artifacts. Carries partition_by: tuple[str, ...] = ('tail_id',)." "Dataclass" {
        properties {
          "field_count" "1"
          "module_name" "libs.scoring.tables"
          "payload_shape" "Carries partition_by: tuple[str, ...] = ('tail_id',)."
          "semantic_kind" "Table Artifact"
        }
        }
      }
      simulation_semantics = container "Simulation Semantics" "Owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles" "Python Dataclasses" {
        properties {
          "dataclass_count" "62"
          "source_component" "core_libraries.simulation"
        }
        pipelinerunconfig = component "PipelineRunConfig" "configuration for Pipeline Run within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries flight_name: str, tail_id: str, flight_id: str, n_steps: int, +43 more." "Dataclass" {
        properties {
          "field_count" "47"
          "module_name" "libs.simulation.run_context"
          "payload_shape" "Carries flight_name: str, tail_id: str, flight_id: str, n_steps: int, +43 more."
          "semantic_kind" "Configuration"
        }
        }
        coupling = component "Coupling" "Coupling within live coupling runtime objects. Carries source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str, +14 more." "Dataclass" {
        properties {
          "field_count" "18"
          "module_name" "libs.simulation.coupling.runtime"
          "payload_shape" "Carries source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str, +14 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        couplingspec = component "CouplingSpec" "specification for Coupling within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str, +14 more." "Dataclass" {
        properties {
          "field_count" "18"
          "module_name" "libs.simulation.coupling.spec"
          "payload_shape" "Carries source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str, +14 more."
          "semantic_kind" "Specification"
        }
        }
        parameterspec = component "ParameterSpec" "specification for Parameter within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries parameter_name: str, system_id: str, subsystem_id: str, module_id: str, +13 more." "Dataclass" {
        properties {
          "field_count" "17"
          "module_name" "libs.simulation.parameter.spec"
          "payload_shape" "Carries parameter_name: str, system_id: str, subsystem_id: str, module_id: str, +13 more."
          "semantic_kind" "Specification"
        }
        }
        parameter = component "Parameter" "Parameter within live parameter runtime objects. Carries name: str, system_id: str, subsystem_id: str, module_id: str, +12 more." "Dataclass" {
        properties {
          "field_count" "16"
          "module_name" "libs.simulation.parameter.runtime"
          "payload_shape" "Carries name: str, system_id: str, subsystem_id: str, module_id: str, +12 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        delayedtransferkey = component "DelayedTransferKey" "Delayed Transfer Key within live coupling runtime objects. Carries source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str, +9 more." "Dataclass" {
        properties {
          "field_count" "13"
          "module_name" "libs.simulation.coupling.runtime"
          "payload_shape" "Carries source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str, +9 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        module = component "Module" "Module within live module runtime objects. Carries id: str, system_id: str, subsystem_id: str, family: str | None, +8 more." "Dataclass" {
        properties {
          "field_count" "12"
          "module_name" "libs.simulation.module.runtime"
          "payload_shape" "Carries id: str, system_id: str, subsystem_id: str, family: str | None, +8 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        modulespec = component "ModuleSpec" "specification for Module within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries module_id: str, subsystem_id: str, system_id: str, module_family: str | None = None, +8 more." "Dataclass" {
        properties {
          "field_count" "12"
          "module_name" "libs.simulation.module.spec"
          "payload_shape" "Carries module_id: str, subsystem_id: str, system_id: str, module_family: str | None = None, +8 more."
          "semantic_kind" "Specification"
        }
        }
        flight = component "Flight" "Flight within live flight runtime objects. Carries spec: FlightSpec, tail: Tail, flight_id: str, start_timestamp_utc: datetime, +7 more." "Dataclass" {
        properties {
          "field_count" "11"
          "module_name" "libs.simulation.flight.runtime"
          "payload_shape" "Carries spec: FlightSpec, tail: Tail, flight_id: str, start_timestamp_utc: datetime, +7 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        validationharnessreport = component "ValidationHarnessReport" "Validation Harness Report within unified validation harness report for iterative simulation tuning runs. Carries report_version: str, status: str | None, run_dir: str, source_artifacts: dict[str, str], +7 more." "Dataclass" {
        properties {
          "field_count" "11"
          "module_name" "libs.simulation.validation_harness"
          "payload_shape" "Carries report_version: str, status: str | None, run_dir: str, source_artifacts: dict[str, str], +7 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        latentupdate = component "LatentUpdate" "Latent Update within live module runtime objects. Carries latent_name: str, source_name: str, source_kind: str = 'input_port', gain: float = 1.0, +6 more." "Dataclass" {
        properties {
          "field_count" "10"
          "module_name" "libs.simulation.module.runtime"
          "payload_shape" "Carries latent_name: str, source_name: str, source_kind: str = 'input_port', gain: float = 1.0, +6 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        latentupdatespec = component "LatentUpdateSpec" "specification for Latent Update within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries latent_name: str, source_name: str, source_kind: LatentSourceKind = 'input_port', gain: float = 1.0, +6 more." "Dataclass" {
        properties {
          "field_count" "10"
          "module_name" "libs.simulation.module.spec"
          "payload_shape" "Carries latent_name: str, source_name: str, source_kind: LatentSourceKind = 'input_port', gain: float = 1.0, +6 more."
          "semantic_kind" "Specification"
        }
        }
        structuralrolespec = component "StructuralRoleSpec" "specification for Structural Role within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries role_name: str, role_kind: str, system_id: str, subsystem_id: str, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.simulation.scenarios.power_pressurization"
          "payload_shape" "Carries role_name: str, role_kind: str, system_id: str, subsystem_id: str, +5 more."
          "semantic_kind" "Specification"
        }
        }
        flighttick = component "FlightTick" "Flight Tick within live flight runtime objects. Carries tail_id: str, flight_id: str, step_index: int, timestamp_utc: datetime, +4 more." "Dataclass" {
        properties {
          "field_count" "8"
          "module_name" "libs.simulation.flight.runtime"
          "payload_shape" "Carries tail_id: str, flight_id: str, step_index: int, timestamp_utc: datetime, +4 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        scenariostochasticspec = component "ScenarioStochasticSpec" "specification for Scenario Stochastic within owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles. Carries seed: int, profile_name: str = 'seeded_nominal_v1', profile_version: str = 'v1', enabled_channels: tuple[str, ...] = ('nominal_observation_noise', 'role_profile_offsets', 'probabilistic_parameter_misbehavior', 'coupling_lag_jitter'), +4 more." "Dataclass" {
        properties {
          "field_count" "8"
          "module_name" "libs.simulation.scenarios.power_pressurization"
          "payload_shape" "Carries seed: int, profile_name: str = 'seeded_nominal_v1', profile_version: str = 'v1', enabled_channels: tuple[str, ...] = ('nominal_observation_noise', 'role_profile_offsets', 'probabilistic_parameter_misbehavior', 'coupling_lag_jitter'), +4 more."
          "semantic_kind" "Specification"
        }
        }
        additional_dataclasses = component "Additional Dataclasses" "47 more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"
      }
      spark_sequence_semantics = container "Spark Sequence Semantics" "Owns deterministic sequence ordering and segmentation policies for long Spark streams" "Python Dataclasses" {
        properties {
          "dataclass_count" "7"
          "source_component" "core_libraries.spark_sequence"
        }
        sequenceorderingpolicy = component "SequenceOrderingPolicy" "Deterministic ordering contract for one logical sequence family. Carries key_columns: tuple[str, ...], order_columns: tuple[str, ...], timestamp_column: str | None = None, row_number_column: str = 'sequence_row_number', +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries key_columns: tuple[str, ...], order_columns: tuple[str, ...], timestamp_column: str | None = None, row_number_column: str = 'sequence_row_number', +2 more."
          "semantic_kind" "Policy"
        }
        }
        sequencesegment = component "SequenceSegment" "Sequence Segment within owns deterministic sequence ordering and segmentation policies for long spark streams. Carries key: SequenceKey, flight_segment_id: int, segment_row_count: int, t_start: object | None = None, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries key: SequenceKey, flight_segment_id: int, segment_row_count: int, t_start: object | None = None, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        segmentedsequenceframe = component "SegmentedSequenceFrame" "frame artifact for Segmented Sequence within owns deterministic sequence ordering and segmentation policies for long spark streams. Carries rows_df: 'DataFrame', segments_df: 'DataFrame', segment_steps_df: 'DataFrame | None' = None." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries rows_df: 'DataFrame', segments_df: 'DataFrame', segment_steps_df: 'DataFrame | None' = None."
          "semantic_kind" "Frame Artifact"
        }
        }
        sequencecarryframe = component "SequenceCarryFrame" "frame artifact for Sequence Carry within owns deterministic sequence ordering and segmentation policies for long spark streams. Carries dataframe: 'DataFrame', key_columns: tuple[str, ...], segment_id_column: str = 'flight_segment_id'." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries dataframe: 'DataFrame', key_columns: tuple[str, ...], segment_id_column: str = 'flight_segment_id'."
          "semantic_kind" "Frame Artifact"
        }
        }
        segmentedsequenceplan = component "SegmentedSequencePlan" "Shared segmentation/orchestration utilities for bounded Spark sequence kernels. Carries ordering: SequenceOrderingPolicy = field(default_factory=lambda: SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id'), order_columns=('timestamp_utc',), timestamp_column='timestamp_utc')), policy: SequenceSegmentPolicy = field(default_factory=SequenceSegmentPolicy)." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries ordering: SequenceOrderingPolicy = field(default_factory=lambda: SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id'), order_columns=('timestamp_utc',), timestamp_column='timestamp_utc')), policy: SequenceSegmentPolicy = field(default_factory=SequenceSegmentPolicy)."
          "semantic_kind" "Execution Plan"
        }
        }
        sequencesegmentpolicy = component "SequenceSegmentPolicy" "Deterministic physical segmentation policy for long ordered streams. Carries max_rows_per_segment: int = 50000, max_span_ms: int = 0." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries max_rows_per_segment: int = 50000, max_span_ms: int = 0."
          "semantic_kind" "Policy"
        }
        }
        sequencekey = component "SequenceKey" "Sequence Key within owns deterministic sequence ordering and segmentation policies for long spark streams. Carries columns: tuple[str, ...], values: tuple[object, ...]." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.spark_sequence.plan"
          "payload_shape" "Carries columns: tuple[str, ...], values: tuple[object, ...]."
          "semantic_kind" "Domain Dataclass"
        }
        }
      }
      tuning_semantics = container "Tuning Semantics" "Owns benchmark search, objective specifications, comparable run ranking, and tuning reports" "Python Dataclasses" {
        properties {
          "dataclass_count" "13"
          "source_component" "core_libraries.tuning"
        }
        benchmarkresult = component "BenchmarkResult" "Benchmark Result within benchmark reporting models and summary builders for tuning workflows. Carries name: str, description: str, repeat_index: int, status: str, +25 more." "Dataclass" {
        properties {
          "field_count" "29"
          "module_name" "libs.tuning.benchmark_reporting"
          "payload_shape" "Carries name: str, description: str, repeat_index: int, status: str, +25 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectiveevaluation = component "ObjectiveEvaluation" "Objective Evaluation within objective specifications and evaluation over validation harness reports. Carries spec: ObjectiveSpec, harness_status: str | None, comparison_signature: dict[str, Any], comparable: bool, +11 more." "Dataclass" {
        properties {
          "field_count" "15"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries spec: ObjectiveSpec, harness_status: str | None, comparison_signature: dict[str, Any], comparable: bool, +11 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectivetermevaluation = component "ObjectiveTermEvaluation" "Objective Term Evaluation within objective specifications and evaluation over validation harness reports. Carries label: str, metric: ObjectiveMetricRef, direction: ObjectiveDirection, weight: float, +7 more." "Dataclass" {
        properties {
          "field_count" "11"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries label: str, metric: ObjectiveMetricRef, direction: ObjectiveDirection, weight: float, +7 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectiveconstraintevaluation = component "ObjectiveConstraintEvaluation" "Objective Constraint Evaluation within objective specifications and evaluation over validation harness reports. Carries label: str, metric: ObjectiveMetricRef, op: ConstraintOperator, threshold: float, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries label: str, metric: ObjectiveMetricRef, op: ConstraintOperator, threshold: float, +5 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectivespec = component "ObjectiveSpec" "specification for Objective within owns benchmark search, objective specifications, comparable run ranking, and tuning reports. Carries name: str, primary_terms: tuple[ObjectiveTerm, ...], constraints: tuple[ObjectiveConstraint, ...] = (), tie_break_terms: tuple[ObjectiveTerm, ...] = (), +4 more." "Dataclass" {
        properties {
          "field_count" "8"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries name: str, primary_terms: tuple[ObjectiveTerm, ...], constraints: tuple[ObjectiveConstraint, ...] = (), tie_break_terms: tuple[ObjectiveTerm, ...] = (), +4 more."
          "semantic_kind" "Specification"
        }
        }
        objectiveterm = component "ObjectiveTerm" "Objective Term within objective specifications and evaluation over validation harness reports. Carries metric: ObjectiveMetricRef, direction: ObjectiveDirection, weight: float = 1.0, required: bool = True, +3 more." "Dataclass" {
        properties {
          "field_count" "7"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries metric: ObjectiveMetricRef, direction: ObjectiveDirection, weight: float = 1.0, required: bool = True, +3 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectiveevaluationreport = component "ObjectiveEvaluationReport" "Objective Evaluation Report within objective-evaluation report writing over validation harness payloads. Carries report_version: str, status: str, run_dir: str, source_artifacts: dict[str, str], +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.tuning.reporting"
          "payload_shape" "Carries report_version: str, status: str, run_dir: str, source_artifacts: dict[str, str], +2 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        benchmarkvariant = component "BenchmarkVariant" "Benchmark Variant within benchmark variant policy for pipeline performance profiling. Carries name: str, description: str, env_overrides: dict[str, str], arg_overrides: dict[str, Any] | None = None, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.tuning.benchmark_variants"
          "payload_shape" "Carries name: str, description: str, env_overrides: dict[str, str], arg_overrides: dict[str, Any] | None = None, +2 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectiveconstraint = component "ObjectiveConstraint" "Objective Constraint within objective specifications and evaluation over validation harness reports. Carries metric: ObjectiveMetricRef, op: ConstraintOperator, threshold: float, required: bool = True, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries metric: ObjectiveMetricRef, op: ConstraintOperator, threshold: float, required: bool = True, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectivepreset = component "ObjectivePreset" "Objective Preset within named objective presets for tuning workflows. Carries name: str, description: str, objective_name: str | None = None, objective_spec_path: str | None = None, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.tuning.presets"
          "payload_shape" "Carries name: str, description: str, objective_name: str | None = None, objective_spec_path: str | None = None, +1 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        objectivemetricref = component "ObjectiveMetricRef" "Objective Metric Ref within objective specifications and evaluation over validation harness reports. Carries category: MetricCategory, scope_name: str, subscope_name: str, metric_path: str." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.tuning.objectives"
          "payload_shape" "Carries category: MetricCategory, scope_name: str, subscope_name: str, metric_path: str."
          "semantic_kind" "Domain Dataclass"
        }
        }
        benchmarksearchspec = component "BenchmarkSearchSpec" "specification for Benchmark Search within owns benchmark search, objective specifications, comparable run ranking, and tuning reports. Carries stage: str, mode: str, description: str, dimensions: tuple[BenchmarkSearchDimension, ...]." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.tuning.benchmark_search"
          "payload_shape" "Carries stage: str, mode: str, description: str, dimensions: tuple[BenchmarkSearchDimension, ...]."
          "semantic_kind" "Specification"
        }
        }
        benchmarksearchdimension = component "BenchmarkSearchDimension" "Benchmark Search Dimension within stage-local benchmark search spaces and variant generation. Carries name: str, values: tuple[Any, ...], kind: Literal['arg', 'env'] = 'arg'." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.tuning.benchmark_search"
          "payload_shape" "Carries name: str, values: tuple[Any, ...], kind: Literal['arg', 'env'] = 'arg'."
          "semantic_kind" "Domain Dataclass"
        }
        }
      }
      windows_semantics = container "Windows Semantics" "libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented Spark window builder" "Python Dataclasses" {
        properties {
          "dataclass_count" "23"
          "source_component" "core_libraries.windows"
        }
        windowmetricssummary = component "WindowMetricsSummary" "Window Metrics Summary within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries window_count: int = 0, event_threshold_rate: float = 0.0, budget_threshold_rate: float = 0.0, end_of_stream_rate: float = 0.0, +14 more." "Dataclass" {
        properties {
          "field_count" "18"
          "module_name" "libs.windows.policy_profile"
          "payload_shape" "Carries window_count: int = 0, event_threshold_rate: float = 0.0, budget_threshold_rate: float = 0.0, end_of_stream_rate: float = 0.0, +14 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        windowpolicyprofilespec = component "WindowPolicyProfileSpec" "specification for Window Policy Profile within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries min_sampling_rate_hz: float, configured_max_ms: int, configured_event_threshold: int, min_ms: int, +5 more." "Dataclass" {
        properties {
          "field_count" "9"
          "module_name" "libs.windows.policy_profile"
          "payload_shape" "Carries min_sampling_rate_hz: float, configured_max_ms: int, configured_event_threshold: int, min_ms: int, +5 more."
          "semantic_kind" "Specification"
        }
        }
        window = component "Window" "Window within window domain objects and closure policy. Carries t_start: datetime, t_end: datetime, event_count: int = 0, sensor_buffer: WindowSensorBuffer = field(default_factory=WindowSensorBuffer), +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.windows.window"
          "payload_shape" "Carries t_start: datetime, t_end: datetime, event_count: int = 0, sensor_buffer: WindowSensorBuffer = field(default_factory=WindowSensorBuffer), +2 more."
          "semantic_kind" "Domain Dataclass"
        }
        }
        openwindowstate = component "OpenWindowState" "runtime state for Open Window within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries win_id: str = 'open_win_id', t_start: str = 'open_t_start', t_end: str = 'open_t_end', start_event_seq_id: str = 'open_start_event_seq_id', +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.windows.pipeline"
          "payload_shape" "Carries win_id: str = 'open_win_id', t_start: str = 'open_t_start', t_end: str = 'open_t_end', start_event_seq_id: str = 'open_start_event_seq_id', +2 more."
          "semantic_kind" "Runtime State"
        }
        }
        windowpolicyevaluationspec = component "WindowPolicyEvaluationSpec" "specification for Window Policy Evaluation within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries candidate_frontier_size: int = 5, stability_sample_fraction: float = 0.8, stability_sample_count: int = 2, max_stability_flights: int = 64, +2 more." "Dataclass" {
        properties {
          "field_count" "6"
          "module_name" "libs.windows.policy_profile"
          "payload_shape" "Carries candidate_frontier_size: int = 5, stability_sample_fraction: float = 0.8, stability_sample_count: int = 2, max_stability_flights: int = 64, +2 more."
          "semantic_kind" "Specification"
        }
        }
        adaptivewindowpolicy = component "AdaptiveWindowPolicy" "policy for Adaptive Window within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries max_ms: int, event_threshold: int, min_ms: int, inactivity_timeout_ms: int = 0, +1 more." "Dataclass" {
        properties {
          "field_count" "5"
          "module_name" "libs.windows.pipeline"
          "payload_shape" "Carries max_ms: int, event_threshold: int, min_ms: int, inactivity_timeout_ms: int = 0, +1 more."
          "semantic_kind" "Policy"
        }
        }
        adaptivewindowsegmentstate = component "AdaptiveWindowSegmentState" "runtime state for Adaptive Window Segment within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries next_win_id: str = 'next_win_id', has_open_window: str = 'has_open_window', open_state: OpenWindowState = field(default_factory=OpenWindowState), closed_windows: str = 'closed_windows'." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.windows.pipeline"
          "payload_shape" "Carries next_win_id: str = 'next_win_id', has_open_window: str = 'has_open_window', open_state: OpenWindowState = field(default_factory=OpenWindowState), closed_windows: str = 'closed_windows'."
          "semantic_kind" "Runtime State"
        }
        }
        windowpolicy = component "WindowPolicy" "policy for Window within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries max_ms: int, event_threshold: int, min_ms: int = 50, inactivity_timeout_ms: int = 0." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.windows.window"
          "payload_shape" "Carries max_ms: int, event_threshold: int, min_ms: int = 50, inactivity_timeout_ms: int = 0."
          "semantic_kind" "Policy"
        }
        }
        windowfeaturevectorspec = component "WindowFeatureVectorSpec" "specification for Window Feature Vector within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries timestamp_column: str = 'timestamp_utc', parameter_name_column: str = 'parameter_name', numeric_value_column: str = 'value_num', text_value_column: str = 'parameter_value'." "Dataclass" {
        properties {
          "field_count" "4"
          "module_name" "libs.windows.features"
          "payload_shape" "Carries timestamp_column: str = 'timestamp_utc', parameter_name_column: str = 'parameter_name', numeric_value_column: str = 'value_num', text_value_column: str = 'parameter_value'."
          "semantic_kind" "Specification"
        }
        }
        windowfeaturesdiagnostics = component "WindowFeaturesDiagnostics" "Window Features Diagnostics within class-oriented builders for the canonical window_features artifact. Carries steps: list[WindowFeatureStepDiagnostics], output_row_count: int, total_timing_ms: float." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.windows.features"
          "payload_shape" "Carries steps: list[WindowFeatureStepDiagnostics], output_row_count: int, total_timing_ms: float."
          "semantic_kind" "Domain Dataclass"
        }
        }
        windowfeaturestepdiagnostics = component "WindowFeatureStepDiagnostics" "Window Feature Step Diagnostics within class-oriented builders for the canonical window_features artifact. Carries step_name: str, row_count: int, timing_ms: float." "Dataclass" {
        properties {
          "field_count" "3"
          "module_name" "libs.windows.features"
          "payload_shape" "Carries step_name: str, row_count: int, timing_ms: float."
          "semantic_kind" "Domain Dataclass"
        }
        }
        adaptivewindowplan = component "AdaptiveWindowPlan" "execution plan for Adaptive Window within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries policy: AdaptiveWindowPolicy, sequence_plan: SegmentedSequencePlan = field(default_factory=lambda: SegmentedSequencePlan(ordering=SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id'), order_columns=('window_step_order',), timestamp_column='timestamp_utc'), policy=_default_window_segment_policy()))." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.windows.pipeline"
          "payload_shape" "Carries policy: AdaptiveWindowPolicy, sequence_plan: SegmentedSequencePlan = field(default_factory=lambda: SegmentedSequencePlan(ordering=SequenceOrderingPolicy(key_columns=('tail_id', 'flight_id'), order_columns=('window_step_order',), timestamp_column='timestamp_utc'), policy=_default_window_segment_policy()))."
          "semantic_kind" "Execution Plan"
        }
        }
        adaptivewindowtransition = component "AdaptiveWindowTransition" "Adaptive Window Transition within class-oriented canonical windows-table builder. Carries policy: WindowPolicy, state: AdaptiveWindowSegmentState = field(default_factory=AdaptiveWindowSegmentState)." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.windows.pipeline"
          "payload_shape" "Carries policy: WindowPolicy, state: AdaptiveWindowSegmentState = field(default_factory=AdaptiveWindowSegmentState)."
          "semantic_kind" "Domain Dataclass"
        }
        }
        windowcoveragesampler = component "WindowCoverageSampler" "Window Coverage Sampler within window coverage-sampling object. Carries sample_size_per_flight: int = 32, bins_per_axis: int = 4." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.windows.coverage"
          "payload_shape" "Carries sample_size_per_flight: int = 32, bins_per_axis: int = 4."
          "semantic_kind" "Domain Dataclass"
        }
        }
        windowclosurebudgetpolicy = component "WindowClosureBudgetPolicy" "policy for Window Closure Budget within libs/windows owns: window lifecycle and closure semantics; window-policy profiling and selection; per-window signal buffering; the canonical segmented spark window builder. Carries quiet_horizon_ms: int, event_threshold: int." "Dataclass" {
        properties {
          "field_count" "2"
          "module_name" "libs.windows.window"
          "payload_shape" "Carries quiet_horizon_ms: int, event_threshold: int."
          "semantic_kind" "Policy"
        }
        }
        additional_dataclasses = component "Additional Dataclasses" "8 more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"
      }
    }

    s3ntinel.core_libraries -> s3ntinel.pipeline_runtime "Uses" "Python imports"
    s3ntinel.pipeline_runtime -> s3ntinel.core_libraries "Uses" "Python imports"
    s3ntinel.simulation_clis -> s3ntinel.core_libraries "Uses" "Python imports"
    s3ntinel.simulation_clis -> s3ntinel.pipeline_runtime "Uses" "Python imports"
    s3ntinel.architecture_tooling.workflow -> s3ntinel.architecture_tooling.repo_maps "Uses" "Python imports"
    s3ntinel.core_libraries.anomaly -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.anomaly -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.anomaly -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.anomaly -> s3ntinel.core_libraries.scoring "Uses" "Python imports"
    s3ntinel.core_libraries.backbone -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.backbone -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.common "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.profiling "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.spark_sequence "Uses" "Python imports"
    s3ntinel.core_libraries.events -> s3ntinel.core_libraries.windows "Uses" "Python imports"
    s3ntinel.core_libraries.graph -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.graph -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.graph -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.io -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.phase -> s3ntinel.core_libraries.backbone "Uses" "Python imports"
    s3ntinel.core_libraries.phase -> s3ntinel.core_libraries.common "Uses" "Python imports"
    s3ntinel.core_libraries.phase -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.phase -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.phase -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.phase -> s3ntinel.core_libraries.spark_sequence "Uses" "Python imports"
    s3ntinel.core_libraries.profiling -> s3ntinel.core_libraries.behavior "Uses" "Python imports"
    s3ntinel.core_libraries.profiling -> s3ntinel.core_libraries.common "Uses" "Python imports"
    s3ntinel.core_libraries.profiling -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.profiling -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.profiling -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.pyspark -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.common "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.events "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.graph "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.phase "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.profiling "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.scoring -> s3ntinel.core_libraries.windows "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.anomaly "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.behavior "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.events "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.graph "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.phase "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.profiling "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.reporting "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.scoring "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.tuning "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.core_libraries.windows "Uses" "Python imports"
    s3ntinel.core_libraries.simulation -> s3ntinel.pipeline_runtime.grouped_runners "Uses" "Python imports"
    s3ntinel.core_libraries.tuning -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.tuning -> s3ntinel.core_libraries.simulation "Uses" "Python imports"
    s3ntinel.core_libraries.windows -> s3ntinel.core_libraries.common "Uses" "Python imports"
    s3ntinel.core_libraries.windows -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.core_libraries.windows -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.core_libraries.windows -> s3ntinel.core_libraries.pyspark "Uses" "Python imports"
    s3ntinel.core_libraries.windows -> s3ntinel.core_libraries.spark_sequence "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.backbone "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.events "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.graph "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.profiling "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.core_libraries.windows "Uses" "Python imports"
    s3ntinel.pipeline_runtime.fitting_stages -> s3ntinel.pipeline_runtime.pipeline_common "Uses" "Python imports"
    s3ntinel.pipeline_runtime.grouped_runners -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.anomaly "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.events "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.graph "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.phase "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.profiling "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.scoring "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.core_libraries.windows "Uses" "Python imports"
    s3ntinel.pipeline_runtime.inference_stages -> s3ntinel.pipeline_runtime.pipeline_common "Uses" "Python imports"
    s3ntinel.pipeline_runtime.pipeline_common -> s3ntinel.core_libraries.config "Uses" "Python imports"
    s3ntinel.simulation_clis.performance_and_replay -> s3ntinel.core_libraries.graph "Uses" "Python imports"
    s3ntinel.simulation_clis.performance_and_replay -> s3ntinel.core_libraries.io "Uses" "Python imports"
    s3ntinel.simulation_clis.performance_and_replay -> s3ntinel.core_libraries.perf "Uses" "Python imports"
    s3ntinel.simulation_clis.performance_and_replay -> s3ntinel.core_libraries.phase "Uses" "Python imports"
    s3ntinel.simulation_clis.performance_and_replay -> s3ntinel.core_libraries.simulation "Uses" "Python imports"
    s3ntinel.simulation_clis.performance_and_replay -> s3ntinel.core_libraries.tuning "Uses" "Python imports"
    s3ntinel.simulation_clis.simulation_runner -> s3ntinel.core_libraries.simulation "Uses" "Python imports"
    engineer -> s3ntinel.pipeline_runtime "Runs grouped fitting and inference workflows."
    engineer -> s3ntinel.simulation_clis "Runs simulation, replay, and smoke workflows."
    engineer -> s3ntinel.architecture_tooling "Generates and reviews architecture artifacts."
    s3ntinel.pipeline_runtime -> raw_telemetry_inputs "Consumes canonical telemetry inputs."
    s3ntinel.pipeline_runtime -> persisted_artifacts "Reads and writes persisted stage artifacts and reports."
    s3ntinel.pipeline_runtime -> mlflow_tracking "Emits metrics and run summaries when configured."
    s3ntinel.simulation_clis -> s3ntinel.pipeline_runtime "Invokes persisted stages as part of simulation and replay workflows."
    s3ntinel.simulation_clis -> persisted_artifacts "Seeds run bundles, replay inputs, and local diagnostics."
    s3ntinel.simulation_clis -> raw_telemetry_inputs "Produces or prepares telemetry-shaped inputs for the active pipeline."
    s3ntinel.pipeline_runtime -> s3ntinel.core_libraries "Delegates domain-stage logic to reusable library packages."
    s3ntinel.simulation_clis -> s3ntinel.core_libraries "Uses reusable simulation, replay, validation, and reporting libraries."
    s3ntinel.architecture_tooling -> s3ntinel.pipeline_runtime "Maps persisted stage entrypoints and grouped runner structure."
    s3ntinel.architecture_tooling -> s3ntinel.core_libraries "Maps reusable library boundaries, dependencies, and LOC skew."
    s3ntinel.architecture_tooling -> persisted_artifacts "Writes version-controlled DSL, reports, and export-friendly architecture assets."
    s3ntinel.pipeline_runtime.stage_00_ingest_raw -> s3ntinel.pipeline_runtime.stage_10_parameter_profiles_fit "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_10_parameter_profiles_fit -> s3ntinel.pipeline_runtime.stage_12_behavior_profiles_fit "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_12_behavior_profiles_fit -> s3ntinel.pipeline_runtime.stage_15_event_profiles_fit "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_15_event_profiles_fit -> s3ntinel.pipeline_runtime.stage_20_events_extract "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_20_events_extract -> s3ntinel.pipeline_runtime.stage_25_window_policy_profile "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_25_window_policy_profile -> s3ntinel.pipeline_runtime.stage_30_windows_adaptive "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_30_windows_adaptive -> s3ntinel.pipeline_runtime.stage_40_backbone_fit "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_40_backbone_fit -> s3ntinel.pipeline_runtime.stage_50_build_graph "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_50_build_graph -> s3ntinel.pipeline_runtime.stage_60_fit_hierarchy "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_60_fit_hierarchy -> s3ntinel.pipeline_runtime.stage_70_phase_fit "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_70_phase_fit -> s3ntinel.pipeline_runtime.stage_72_phase_label_centroids "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_72_phase_label_centroids -> s3ntinel.pipeline_runtime.stage_80_window_scores_raw "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_80_window_scores_raw -> s3ntinel.pipeline_runtime.stage_85_window_scores_calibrate "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_85_window_scores_calibrate -> s3ntinel.pipeline_runtime.stage_90_anomaly_attribution "Flows into next pipeline layer" "Pipeline order"
    s3ntinel.pipeline_runtime.stage_90_anomaly_attribution -> s3ntinel.pipeline_runtime.stage_95_emit_explorer_bundle "Flows into next pipeline layer" "Pipeline order"
  }

  views {
    systemContext s3ntinel "system_context" {
      include *
      autoLayout lr
    }

    container s3ntinel "container_view" {
      include *
      autoLayout lr
    }

    component s3ntinel.pipeline_runtime "pipeline_components" {
      include s3ntinel.pipeline_runtime.grouped_runners
      include s3ntinel.pipeline_runtime.fitting_stages
      include s3ntinel.pipeline_runtime.inference_stages
      include s3ntinel.pipeline_runtime.pipeline_common
      autoLayout lr
    }

    component s3ntinel.pipeline_runtime "pipeline_layers" {
      include s3ntinel.pipeline_runtime.stage_00_ingest_raw
      include s3ntinel.pipeline_runtime.stage_10_parameter_profiles_fit
      include s3ntinel.pipeline_runtime.stage_12_behavior_profiles_fit
      include s3ntinel.pipeline_runtime.stage_15_event_profiles_fit
      include s3ntinel.pipeline_runtime.stage_20_events_extract
      include s3ntinel.pipeline_runtime.stage_25_window_policy_profile
      include s3ntinel.pipeline_runtime.stage_30_windows_adaptive
      include s3ntinel.pipeline_runtime.stage_40_backbone_fit
      include s3ntinel.pipeline_runtime.stage_50_build_graph
      include s3ntinel.pipeline_runtime.stage_60_fit_hierarchy
      include s3ntinel.pipeline_runtime.stage_70_phase_fit
      include s3ntinel.pipeline_runtime.stage_72_phase_label_centroids
      include s3ntinel.pipeline_runtime.stage_80_window_scores_raw
      include s3ntinel.pipeline_runtime.stage_85_window_scores_calibrate
      include s3ntinel.pipeline_runtime.stage_90_anomaly_attribution
      include s3ntinel.pipeline_runtime.stage_95_emit_explorer_bundle
      autoLayout tb
    }

    component s3ntinel.core_libraries "core_library_components" {
      include s3ntinel.core_libraries.config
      include s3ntinel.core_libraries.common
      include s3ntinel.core_libraries.profiling
      include s3ntinel.core_libraries.events
      include s3ntinel.core_libraries.windows
      include s3ntinel.core_libraries.backbone
      include s3ntinel.core_libraries.graph
      include s3ntinel.core_libraries.phase
      include s3ntinel.core_libraries.scoring
      include s3ntinel.core_libraries.anomaly
      include s3ntinel.core_libraries.simulation
      include s3ntinel.core_libraries.io
      include s3ntinel.core_libraries.perf
      include s3ntinel.core_libraries.behavior
      autoLayout lr
    }

    component semantics.anomaly_semantics "anomaly_semantics" {
      include *
      autoLayout tb
    }

    component semantics.backbone_semantics "backbone_semantics" {
      include *
      autoLayout tb
    }

    component semantics.behavior_semantics "behavior_semantics" {
      include *
      autoLayout tb
    }

    component semantics.config_semantics "config_semantics" {
      include *
      autoLayout tb
    }

    component semantics.events_semantics "events_semantics" {
      include *
      autoLayout tb
    }

    component semantics.graph_semantics "graph_semantics" {
      include *
      autoLayout tb
    }

    component semantics.perf_semantics "perf_semantics" {
      include *
      autoLayout tb
    }

    component semantics.phase_semantics "phase_semantics" {
      include *
      autoLayout tb
    }

    component semantics.profiling_semantics "profiling_semantics" {
      include *
      autoLayout tb
    }

    component semantics.pyspark_semantics "pyspark_semantics" {
      include *
      autoLayout tb
    }

    component semantics.reporting_semantics "reporting_semantics" {
      include *
      autoLayout tb
    }

    component semantics.scoring_semantics "scoring_semantics" {
      include *
      autoLayout tb
    }

    component semantics.simulation_semantics "simulation_semantics" {
      include *
      autoLayout tb
    }

    component semantics.spark_sequence_semantics "spark_sequence_semantics" {
      include *
      autoLayout tb
    }

    component semantics.tuning_semantics "tuning_semantics" {
      include *
      autoLayout tb
    }

    component semantics.windows_semantics "windows_semantics" {
      include *
      autoLayout tb
    }

    styles {
      element "Software System" {
        background #0b6e4f
        color #ffffff
      }
      element "Container" {
        background #145da0
        color #ffffff
      }
      element "Component" {
        background #f0f4f8
        color #1f2933
      }
      element "Dataclass" {
        background #fff3c4
        color #5d3a00
        shape roundedbox
      }
      element "Person" {
        background #d9e2ec
        color #102a43
        shape person
      }
    }
  }
}
