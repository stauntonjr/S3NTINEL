# Architecture Metrics

- System AST span LOC: `55315`
- Largest module LOC share: `4.08%`
- Largest component LOC share: `21.10%`
- Top three components LOC share: `35.29%`

## Largest Modules

| Module | Path | LOC | Container | Component |
| --- | --- | ---: | --- | --- |
| libs.simulation.scenarios.power_pressurization | libs/simulation/scenarios/power_pressurization.py | 2257 | core_libraries | core_libraries.simulation |
| libs.simulation.aircraft.examples | libs/simulation/aircraft/examples.py | 1472 | core_libraries | core_libraries.simulation |
| libs.architecture.render | libs/architecture/render.py | 1299 | architecture_tooling | architecture_tooling.workflow |
| libs.anomaly.validator | libs/anomaly/validator.py | 1175 | core_libraries | core_libraries.anomaly |
| libs.windows.policy_profile | libs/windows/policy_profile.py | 1161 | core_libraries | core_libraries.windows |
| libs.plotting.simulation_artifacts | libs/plotting/simulation_artifacts.py | 1134 | core_libraries |  |
| libs.scoring.tables | libs/scoring/tables.py | 1072 | core_libraries | core_libraries.scoring |
| libs.simulation.flight.examples | libs/simulation/flight/examples.py | 1066 | core_libraries | core_libraries.simulation |
| libs.tuning.objectives | libs/tuning/objectives.py | 985 | core_libraries | core_libraries.tuning |
| libs.anomaly.frames | libs/anomaly/frames.py | 982 | core_libraries | core_libraries.anomaly |

## Largest Classes

| Qualified Name | Path | LOC |
| --- | --- | ---: |
| libs.windows.features.WindowFeaturesPlan | libs/windows/features.py | 736 |
| libs.windows.policy_profile.WindowPolicyProfile | libs/windows/policy_profile.py | 734 |
| libs.phase.frames.PhaseFeatureFrame | libs/phase/frames.py | 725 |
| libs.scoring.tables.WindowScoresRawTable | libs/scoring/tables.py | 645 |
| libs.profiling.profiles.ParameterBehaviorPrimitiveProfile | libs/profiling/profiles.py | 597 |
| libs.anomaly.frames.AnomalyParameterLocalizationFrame | libs/anomaly/frames.py | 547 |
| libs.events.continuous.ContinuousEventDetector | libs/events/continuous.py | 490 |
| libs.events.profiling.ParameterEventProfile | libs/events/profiling.py | 427 |
| libs.graph.hierarchy_artifacts.GraphHierarchy | libs/graph/hierarchy_artifacts.py | 373 |
| libs.windows.pipeline.AdaptiveWindowPlan | libs/windows/pipeline.py | 357 |

## Largest Functions And Methods

| Qualified Name | Path | LOC |
| --- | --- | ---: |
| libs.simulation.aircraft.examples.build_power_pressurization_hierarchy_composite_aircraft_spec | libs/simulation/aircraft/examples.py | 802 |
| libs.simulation.scenarios.power_pressurization._build_module_templates | libs/simulation/scenarios/power_pressurization.py | 636 |
| libs.scoring.tables.WindowScoresRawTable.from_phase_dataframes | libs/scoring/tables.py | 611 |
| libs.events.profiling.ParameterEventProfile.from_raw_input | libs/events/profiling.py | 419 |
| libs.anomaly.frames.AnomalyParameterLocalizationFrame.from_calibrated_phase_windows_events_and_hierarchy | libs/anomaly/frames.py | 404 |
| pipelines.50_build_graph.run | pipelines/50_build_graph.py | 378 |
| libs.profiling.profiles.ParameterBehaviorPrimitiveProfile._numeric_features_df | libs/profiling/profiles.py | 353 |
| libs.events.continuous.ContinuousEventDetector._feature_frame | libs/events/continuous.py | 289 |
| libs.graph.hierarchy_artifacts.GraphHierarchy.from_fused_spark | libs/graph/hierarchy_artifacts.py | 285 |
| libs.profiling.validator.build_profile_validation_summary | libs/profiling/validator.py | 283 |

## Component Size Table

| Component | Container | LOC | Module Count | System Share |
| --- | --- | ---: | ---: | ---: |
| Simulation | core_libraries | 11674 | 62 | 21.10% |
| Phase | core_libraries | 4332 | 14 | 7.83% |
| Graph | core_libraries | 3513 | 13 | 6.35% |
| Events | core_libraries | 3493 | 9 | 6.31% |
| Tuning | core_libraries | 3479 | 13 | 6.29% |
| Windows | core_libraries | 3442 | 8 | 6.22% |
| Architecture Workflow | architecture_tooling | 2881 | 7 | 5.21% |
| Anomaly | core_libraries | 2642 | 5 | 4.78% |
| Scoring | core_libraries | 2123 | 4 | 3.84% |
| Io | core_libraries | 2073 | 16 | 3.75% |
| Fitting Stages | pipeline_runtime | 2066 | 10 | 3.73% |
| Behavior | core_libraries | 1910 | 12 | 3.45% |
| Profiling | core_libraries | 1552 | 4 | 2.81% |
| Perf | core_libraries | 1088 | 6 | 1.97% |
| Repo Mapping Tools | architecture_tooling | 1029 | 2 | 1.86% |
| Inference Stages | pipeline_runtime | 989 | 6 | 1.79% |
| Performance And Replay Utilities | simulation_clis | 885 | 3 | 1.60% |
| Backbone | core_libraries | 817 | 5 | 1.48% |
| Config | core_libraries | 642 | 2 | 1.16% |
| Grouped Pipeline Runners | pipeline_runtime | 565 | 4 | 1.02% |
| Spark Sequence | core_libraries | 274 | 2 | 0.50% |
| Pyspark | core_libraries | 201 | 4 | 0.36% |
| Common | core_libraries | 199 | 4 | 0.36% |
| Pipeline Shared Runtime | pipeline_runtime | 123 | 2 | 0.22% |
| Reporting | core_libraries | 64 | 2 | 0.12% |
| Simulation Runner | simulation_clis | 7 | 1 | 0.01% |

## Dependency Vs Size

| Component | LOC | Incoming | Outgoing | Combined |
| --- | ---: | ---: | ---: | ---: |
| Simulation | 11674 | 3 | 13 | 16 |
| Io | 2073 | 13 | 1 | 14 |
| Perf | 1088 | 13 | 0 | 13 |
| Events | 3493 | 4 | 7 | 11 |
| Scoring | 2123 | 3 | 8 | 11 |
| Phase | 4332 | 4 | 6 | 10 |
| Windows | 3442 | 5 | 5 | 10 |
| Profiling | 1552 | 5 | 5 | 10 |
| Inference Stages | 989 | 0 | 10 | 10 |
| Pyspark | 201 | 8 | 1 | 9 |
| Graph | 3513 | 5 | 3 | 8 |
| Fitting Stages | 2066 | 0 | 8 | 8 |
| Anomaly | 2642 | 2 | 4 | 6 |
| Performance And Replay Utilities | 885 | 0 | 6 | 6 |
| Common | 199 | 5 | 0 | 5 |
| Tuning | 3479 | 2 | 2 | 4 |
| Backbone | 817 | 2 | 2 | 4 |
| Spark Sequence | 274 | 3 | 0 | 3 |
| Pipeline Shared Runtime | 123 | 2 | 1 | 3 |
| Behavior | 1910 | 2 | 0 | 2 |
| Grouped Pipeline Runners | 565 | 1 | 1 | 2 |
| Architecture Workflow | 2881 | 0 | 1 | 1 |
| Repo Mapping Tools | 1029 | 1 | 0 | 1 |
| Config | 642 | 1 | 0 | 1 |
| Reporting | 64 | 1 | 0 | 1 |
| Simulation Runner | 7 | 0 | 1 | 1 |
