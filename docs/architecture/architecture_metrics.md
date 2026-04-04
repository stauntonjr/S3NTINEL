# Architecture Metrics

- System AST span LOC: `50239`
- Largest module LOC share: `4.49%`
- Largest component LOC share: `22.90%`
- Top three components LOC share: `36.92%`

## Largest Modules

| Module | Path | LOC | Container | Component |
| --- | --- | ---: | --- | --- |
| libs.simulation.scenarios.power_pressurization | libs/simulation/scenarios/power_pressurization.py | 2257 | core_libraries | core_libraries.simulation |
| libs.simulation.aircraft.examples | libs/simulation/aircraft/examples.py | 1472 | core_libraries | core_libraries.simulation |
| libs.architecture.render | libs/architecture/render.py | 1299 | architecture_tooling | architecture_tooling.workflow |
| libs.plotting.simulation_artifacts | libs/plotting/simulation_artifacts.py | 1134 | core_libraries |  |
| libs.simulation.flight.examples | libs/simulation/flight/examples.py | 1066 | core_libraries | core_libraries.simulation |
| libs.tuning.objectives | libs/tuning/objectives.py | 985 | core_libraries | core_libraries.tuning |
| libs.profiling.profiles | libs/profiling/profiles.py | 959 | core_libraries | core_libraries.profiling |
| libs.windows.policy_profile | libs/windows/policy_profile.py | 879 | core_libraries | core_libraries.windows |
| libs.events.continuous | libs/events/continuous.py | 822 | core_libraries | core_libraries.events |
| libs.simulation.validation_harness | libs/simulation/validation_harness.py | 796 | core_libraries | core_libraries.simulation |

## Largest Classes

| Qualified Name | Path | LOC |
| --- | --- | ---: |
| libs.windows.features.WindowFeaturesPlan | libs/windows/features.py | 653 |
| libs.windows.policy_profile.WindowPolicyProfile | libs/windows/policy_profile.py | 650 |
| libs.profiling.profiles.ParameterBehaviorPrimitiveProfile | libs/profiling/profiles.py | 597 |
| libs.events.continuous.ContinuousEventDetector | libs/events/continuous.py | 490 |
| libs.events.profiling.ParameterEventProfile | libs/events/profiling.py | 427 |
| libs.simulation.coupling.runtime.Coupling | libs/simulation/coupling/runtime.py | 338 |
| libs.simulation.module.runtime.Module | libs/simulation/module/runtime.py | 337 |
| libs.phase.frames.PhaseFeatureFrame | libs/phase/frames.py | 316 |
| libs.graph.hierarchy_artifacts.GraphHierarchy | libs/graph/hierarchy_artifacts.py | 314 |
| libs.windows.pipeline.AdaptiveWindowPlan | libs/windows/pipeline.py | 308 |

## Largest Functions And Methods

| Qualified Name | Path | LOC |
| --- | --- | ---: |
| libs.simulation.aircraft.examples.build_power_pressurization_hierarchy_composite_aircraft_spec | libs/simulation/aircraft/examples.py | 802 |
| libs.simulation.scenarios.power_pressurization._build_module_templates | libs/simulation/scenarios/power_pressurization.py | 636 |
| libs.events.profiling.ParameterEventProfile.from_raw_input | libs/events/profiling.py | 419 |
| pipelines.50_build_graph.run | pipelines/50_build_graph.py | 378 |
| libs.profiling.profiles.ParameterBehaviorPrimitiveProfile._numeric_features_df | libs/profiling/profiles.py | 353 |
| libs.events.continuous.ContinuousEventDetector._feature_frame | libs/events/continuous.py | 289 |
| libs.profiling.validator.build_profile_validation_summary | libs/profiling/validator.py | 283 |
| libs.config.pipeline.load_pipeline_context_settings | libs/config/pipeline.py | 275 |
| libs.simulation.scenarios.power_pressurization._role_step_inputs | libs/simulation/scenarios/power_pressurization.py | 265 |
| libs.graph.hierarchy_artifacts.GraphHierarchy.from_fused_spark | libs/graph/hierarchy_artifacts.py | 248 |

## Component Size Table

| Component | Container | LOC | Module Count | System Share |
| --- | --- | ---: | ---: | ---: |
| Simulation | core_libraries | 11507 | 62 | 22.90% |
| Graph | core_libraries | 3550 | 13 | 7.07% |
| Events | core_libraries | 3493 | 9 | 6.95% |
| Tuning | core_libraries | 3479 | 13 | 6.92% |
| Phase | core_libraries | 2950 | 14 | 5.87% |
| Architecture Workflow | architecture_tooling | 2881 | 7 | 5.73% |
| Windows | core_libraries | 2806 | 8 | 5.59% |
| Fitting Stages | pipeline_runtime | 2036 | 10 | 4.05% |
| Io | core_libraries | 2006 | 16 | 3.99% |
| Behavior | core_libraries | 1910 | 12 | 3.80% |
| Profiling | core_libraries | 1552 | 4 | 3.09% |
| Perf | core_libraries | 1088 | 6 | 2.17% |
| Anomaly | core_libraries | 1062 | 5 | 2.11% |
| Repo Mapping Tools | architecture_tooling | 1029 | 2 | 2.05% |
| Scoring | core_libraries | 974 | 5 | 1.94% |
| Inference Stages | pipeline_runtime | 906 | 6 | 1.80% |
| Performance And Replay Utilities | simulation_clis | 885 | 3 | 1.76% |
| Backbone | core_libraries | 817 | 5 | 1.63% |
| Config | core_libraries | 649 | 2 | 1.29% |
| Grouped Pipeline Runners | pipeline_runtime | 565 | 4 | 1.12% |
| Spark Sequence | core_libraries | 274 | 2 | 0.55% |
| Pyspark | core_libraries | 201 | 4 | 0.40% |
| Common | core_libraries | 199 | 4 | 0.40% |
| Pipeline Shared Runtime | pipeline_runtime | 123 | 2 | 0.24% |
| Reporting | core_libraries | 64 | 2 | 0.13% |
| Simulation Runner | simulation_clis | 7 | 1 | 0.01% |

## Dependency Vs Size

| Component | LOC | Incoming | Outgoing | Combined |
| --- | ---: | ---: | ---: | ---: |
| Simulation | 11507 | 3 | 12 | 15 |
| Io | 2006 | 13 | 1 | 14 |
| Perf | 1088 | 13 | 0 | 13 |
| Phase | 2950 | 4 | 6 | 10 |
| Events | 3493 | 2 | 7 | 9 |
| Pyspark | 201 | 8 | 1 | 9 |
| Graph | 3550 | 5 | 3 | 8 |
| Fitting Stages | 2036 | 0 | 8 | 8 |
| Profiling | 1552 | 3 | 5 | 8 |
| Windows | 2806 | 2 | 5 | 7 |
| Scoring | 974 | 3 | 4 | 7 |
| Inference Stages | 906 | 0 | 7 | 7 |
| Anomaly | 1062 | 2 | 4 | 6 |
| Performance And Replay Utilities | 885 | 0 | 6 | 6 |
| Tuning | 3479 | 2 | 2 | 4 |
| Backbone | 817 | 2 | 2 | 4 |
| Common | 199 | 4 | 0 | 4 |
| Spark Sequence | 274 | 3 | 0 | 3 |
| Pipeline Shared Runtime | 123 | 2 | 1 | 3 |
| Behavior | 1910 | 2 | 0 | 2 |
| Grouped Pipeline Runners | 565 | 1 | 1 | 2 |
| Architecture Workflow | 2881 | 0 | 1 | 1 |
| Repo Mapping Tools | 1029 | 1 | 0 | 1 |
| Config | 649 | 1 | 0 | 1 |
| Reporting | 64 | 1 | 0 | 1 |
| Simulation Runner | 7 | 0 | 1 | 1 |
