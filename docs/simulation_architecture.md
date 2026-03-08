# Simulation Architecture

This note proposes the next simulation architecture for S3NTINEL.

The goal is to support a large open-ended hierarchy without hard-coding
module-specific control flow into the simulator core.

For the mirrored profiling layer that can infer the same behavioral semantics from
real telemetry, see [behavior_profiling_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_profiling_design.md).
For the proposed per-family file/class layout, see [behavior_family_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_family_architecture.md) and [behavior_family_skeletons.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_family_skeletons.md).
For the intended one-off fitting sequence that should consume observed telemetry and
produce reusable datatype, scaling, and behavior metadata, see [fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/fitting_workflow.md).
For a monolithic execution diagram of the current simulation seam, see
[simulation_codepath_wire_diagram.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_codepath_wire_diagram.md).
For a focused coupling/latent propagation view, see
[simulation_coupling_wire_diagram.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_coupling_wire_diagram.md).

## 1. Design objective

The simulator should be:

- open-ended with respect to systems, subsystems, and modules
- behaviorally expressive enough to support regulated, inertial, accumulative, and
  discrete state dynamics
- graph-aware enough to generate meaningful cross-parameter structure
- phase-aware enough to drive contextual behavior
- fault-aware enough to support direct, timing, structural, and persistent anomalies

## 2. Core design rule

Use:

- **hierarchy for composition**
- **behavior families for dynamics**
- **latent states and couplings for shared structure**
- **scenario/fault layers for contextual variation**

## 3. Recommended layering

## 3.1 Hierarchy layer

This layer represents the aircraft/system topology.

Suggested objects:

- `AircraftSpec`
- `SystemSpec`
- `SubsystemSpec`
- `ModuleSpec`
- `ParameterSpec`

These should be lightweight data specifications, not behavior-heavy classes.

Responsibilities:

- IDs and names
- parent/child relationships
- metadata
- declared parameter membership

Not responsibilities:

- time stepping
- fault logic
- dynamic equations

## 3.2 Runtime state layer

Separate specs from evolving simulation state.

Suggested objects:

- `AircraftState`
- `ModuleState`
- `ParameterState`
- `LatentStateStore`

Current implemented scaffold:

- `AssemblyRuntime`
- `PortRuntime`
- `ParameterRuntime`
- `ModuleRuntime`
- `ParameterBehaviorBinding`
- `ModuleBehaviorBinding`

Responsibilities:

- current latent values
- current observed values
- current discrete states
- cached delays / history
- persistent tail-level degradation state

Not responsibilities:

- datatype profiling
- robust scaling metadata
- behavior profiling
- downstream confidence or classification state

This lets one `ParameterSpec` be reused across many runs while state remains mutable
and simulation-local.

## 3.3 Behavior layer

This is the correct home for the regulated/inertial/etc distinction.

Suggested protocol:

- `BehaviorModel`
  - `step(...)`
  - `observe(...)`

Suggested behavior families:

- `RegulatedBehavior`
- `InertialBehavior`
- `AccumulativeBehavior`
- `DiscreteStateBehavior`
- `DerivedBehavior`

Examples:

- bus voltage -> regulated
- engine spool speed -> inertial
- battery state of charge -> accumulative
- gear/flap/contactors/modes -> discrete state
- VSI / delta-pressure / derived indicators -> derived from latent state

Attach behaviors to parameters via composition:

- `ParameterRuntime(spec=..., behavior=..., sampler=..., noise_model=...)`

Current implemented binding helper layer:

- `bind_parameter_behavior(...)`
- `bind_module_behaviors(...)`
- `bind_assembly_behaviors(...)`

These resolve `behavior_family_label` against the behavior registry without adding
execution logic to the spec or runtime layers.

Current minimal module execution scaffold:

- `AssemblyRuntime`
- `ModuleStepRequest`
- `AssemblyModuleStepRequest`
- `AssemblyTickRequest`
- `LatentUpdateSpec`
- `iter_module_samples(...)`
- `update_module_latent_state(...)`
- `inject_runtime_latent_state_into_step_inputs(...)`
- `apply_module_sample_to_runtime(...)`
- `apply_inter_module_coupling(...)`
- `propagate_inter_module_couplings(...)`
- `step_module_and_propagate(...)`
- `step_assembly_once(...)`

This is intentionally local to one module. It does not yet introduce inter-module
scheduling or global simulation orchestration. The current cross-module seam is
just port-to-port propagation through declared `InterModuleCouplingSpec` links,
plus a deterministic single-pass assembly tick in a caller-provided module order.
`AssemblyRuntime` is the precomputed execution context that owns:

- bound module behaviors
- mutable module runtimes
- deterministic module order
- preindexed outgoing inter-module couplings

Module and assembly stepping are now explicitly **single-tick orchestration**
layers: each parameter contributes exactly one `BehaviorStepInput` per tick,
while the underlying behavior generators remain stream-capable. Module stepping
also carries parameter state forward implicitly through `ModuleRuntime`: callers
only need to seed first-tick initial state, and later steps will reuse the prior
clean parameter value unless explicitly overridden. Current inter-module
propagation supports `phase_gate` and scoped `source_mode_gate` /
`target_mode_gate`; the older flat `mode_gate` field remains only as a
compatibility fallback. Inter-module propagation now runs after a module's full
local sample set has been applied, so transfer semantics no longer depend on
parameter iteration order. A minimal delayed-transfer queue now supports
`lag_seconds` on inter-module couplings when timestamps are provided at the
assembly tick. Once a lagged transfer has been queued, it will still land when
its due time arrives even if the coupling gate is closed on that later tick.
The delayed queue is scoped by full coupling semantics, not just by endpoint
path, so distinct lagged couplings do not alias each other. The assembly
orchestrator also now synthesizes a default one-tick `BehaviorStepInput` for
any bound parameter that is missing an explicit caller-provided step input. It
still does not implement iterative settling or a full global delay scheduler.
Current `relation_type` semantics are intentionally minimal:

- `drive`: transfer the source value with gain/sign
- `enable`: transfer only when the source signal is active, otherwise clear the target
- `inhibit`: clear the target when the source signal is active

There is now also one concrete native example path exercising this seam end to end:

- small reusable example helpers:
  - `libs/simulation/example_builders.py`
- generic demo example:
  - `libs/simulation/examples.py`
- realistic subsystem-shaped slices:
  - `libs/simulation/subsystem_slices.py`
- `build_native_coupled_module_example(...)`
- `build_native_coupled_module_example_context(...)`
- `simulate_native_coupled_module_example(...)`
- `build_native_multibehavior_example(...)`
- `build_native_multibehavior_example_context(...)`
- `simulate_native_multibehavior_example(...)`
- `build_native_pressurization_example(...)`
- `build_native_pressurization_example_context(...)`
- `simulate_native_pressurization_example(...)`

Those examples now cover:

- one regulated source parameter
- one inertial target parameter
- one discrete-state switch parameter
- one accumulative total parameter
- one inter-module `drive` coupling
- one deterministic ordered assembly tick loop

The richer example is intentionally shaped like a small electrical/load/fuel slice:

- a discrete contactor-like state
- a regulated source voltage and flow output
- an inertial load response
- an accumulative fuel-used total

There is now also a pressurization-shaped slice that demonstrates:

- a discrete pressurization mode state
- an inertial aircraft altitude source
- a regulated outflow-valve controller
- an inertial cabin-altitude response
- a regulated cabin differential-pressure response
- one lagged controller-to-cabin coupling

Current module-local latent handling is also intentionally minimal:

- `LatentUpdateSpec` can derive named latent values from input ports or step context
- runtime latent state is injected into `BehaviorStepInput.latent_state` before generation
- `InertialBehavior` and `RegulatedBehavior` now demonstrate the intended
  consumption pattern by resolving a named latent target from
  `BehaviorStepInput.latent_state` when requested

Native authored slices can now flow through the normal dataset-building layer,
not just the example helpers. The current public bridge is:

- `build_subsystem_slice_hierarchy_df(...)`
- `simulate_fleet_dataset_from_subsystem_slice(...)`
- `simulate_fleet_dataset_spark_from_subsystem_slice(...)`

This means a named native slice such as `power_chain` or `pressurization` can
be turned into `hierarchy_df`, default sensor behavior, telemetry rows, and
phase labels through the same `experiment_setup` surface used elsewhere in the
repo.

There is now also a generic native dataset seam above `AssemblyRuntime` for
direct native execution without going through the legacy fleet bridge:

- `build_native_dataset_context(...)`
- `native_raw_telemetry_to_events_sdf(...)`
- `native_telemetry_to_raw_telemetry_df(...)`
- `native_phase_labels_to_table_df(...)`
- `simulate_native_dataset(...)`
- `simulate_native_dataset_from_assembly(...)`
- `simulate_native_dataset_from_subsystem_slice(...)`

That seam now reaches the first structural learning boundary as well:

- canonical `raw_telemetry`
- canonical `events`
- canonical `windows`
- canonical `window_x`
- canonical `backbone`
- canonical graph artifacts

through:

- `simulate_native_raw_telemetry_from_assembly(...)`
- `simulate_native_raw_telemetry_from_subsystem_slice(...)`
- `simulate_native_event_table_from_assembly(...)`
- `simulate_native_event_table_from_subsystem_slice(...)`
- `simulate_native_window_table_from_assembly(...)`
- `simulate_native_window_table_from_subsystem_slice(...)`
- `simulate_native_window_x_table_from_assembly(...)`
- `simulate_native_window_x_table_from_subsystem_slice(...)`
- `simulate_native_backbone_artifacts_from_assembly(...)`
- `simulate_native_backbone_artifacts_from_subsystem_slice(...)`
- `simulate_native_graph_artifacts_from_assembly(...)`
- `simulate_native_graph_artifacts_from_subsystem_slice(...)`

These wrappers intentionally reuse the active canonical builders:

- `build_events_table(...)`
- `build_windows_table(...)`
- `build_window_x_spark_table(...)`
- `build_backbone_artifacts_from_window_x_table(...)`
- `build_graph_artifacts_from_window_x_table(...)`

so the native simulation seam is exercised against the same structural contracts
used by the main V2 pipeline.

These entrypoints emit:

- telemetry rows with `parameter_name`, `parameter_value_clean`, and `parameter_value`
- phase-label rows keyed by `step_index` / `timestamp_utc`

This is the current public path for native authored assemblies that want a
dataset-facing interface without flattening to the older `hierarchy_df` contract.
It can also now emit canonical raw telemetry and phase-label tables directly,
which means native slices can feed the active fitting-stage profiling path
without going through the legacy fleet simulator bridge.
The same seam can also now emit canonical event tables directly through the
shared stage-20 event builder, so native slices can exercise the active event
extraction path without inventing a parallel detector workflow. It can also now
fit canonical backbone and graph artifacts directly from native slices through
the same backbone and graph builders used on the main structural path.

## 3.4 Coupling layer

This is the most important missing abstraction in the current simulator.

Suggested objects:

- `LatentVariable`
- `CouplingEdge`
- `InterModuleCouplingSpec`
- `HierarchyAssemblySpec`
- `ControllerModel`
- `DelayModel`
- `TransferModel`

Examples:

- commanded thrust -> fuel flow -> spool speeds -> temperatures
- aircraft altitude -> pressurization controller -> outflow valve -> cabin altitude
- command surface target -> actuator response -> body-rate response
- pitot/static pressure state -> IAS / altitude / VSI

Current implemented static scaffold:

- `CouplingSpec` for module-local edges
- `InterModuleCouplingSpec` for cross-module port wiring
- `HierarchyAssemblySpec` for compiled assembly-level module and wiring declarations
- `HierarchyAssemblyBuilder` / `build_hierarchy_assembly_spec(...)` for native V2.1 authoring
- `assembly_spec_from_hierarchy_spec(...)` as a shallow compiler from legacy hierarchy dicts
- `validate_assembly_spec(...)` for early module/port reference checks

### Why this matters

Without an explicit coupling layer, the simulator tends to collapse into:

- phase-specific offsets
- local noise
- independent channels

That is not enough for meaningful graph recovery.

## 3.5 Sampling and observation layer

Suggested objects:

- `Sampler`
- `NoiseModel`
- `Quantizer`
- `DropoutModel`
- `TransportDelayModel`

These should be separate from the underlying latent dynamics.

That separation is necessary because:

- a physically smooth latent state may still be measured sparsely
- two sensors may observe the same latent state at different rates
- timing faults often affect observation, not plant dynamics

## 3.6 Phase / scenario layer

Suggested objects:

- `PhaseSchedule`
- `FlightScenario`
- `CommandProfile`
- `EnvelopeModel`

Responsibilities:

- define phase order and duration distribution
- define phase-conditioned command and envelope changes
- define common-cause contextual changes

Examples:

- thrust schedule
- climb/cruise/descent altitude profile
- cabin schedule
- power-source transitions
- configuration changes (gear/flaps/slats/spoilers)

This should be a first-class layer, not just a dictionary of per-phase modifiers.

## 3.7 Fault layer

Suggested objects:

- `FaultSpec`
- `FaultProcess`
- `FaultScheduler`
- `FaultScope`

Recommended scopes:

- parameter-local
- module-local
- subsystem-local
- cross-parameter edge
- tail-persistent

Recommended families:

- value faults
- timing faults
- structural faults
- persistent degradation faults

This separation keeps nominal behavior and anomalous behavior composable.

## 3.8 Label layer

Suggested objects:

- `EventLabelEmitter`
- `AnomalyLabelEmitter`
- `PhaseLabelEmitter`

Responsibilities:

- emit `event_type_label`
- emit `anomaly_type_label`
- emit `anomaly_score_label`
- emit `phase_label`

These should be attached to the simulator flow itself, not bolted on later.

## 3.9 Constraint / invariant layer

Suggested objects:

- `Invariant`
- `ConstraintSet`
- `ConsistencyCheck`

Examples:

- fuel quantity should not increase without transfer/refuel
- regulated voltage should remain in band absent switching/fault
- cabin pressure schedule should remain plausible for altitude
- actual actuator position should respect rate limits
- impossible discrete transitions should be catchable explicitly

This layer is valuable both for:

- validating nominal simulation
- constructing structural anomalies deliberately

## 4. Open-ended hierarchy strategy

The simulator should be **data-driven**.

That means:

- hierarchy comes from specs, not code branching
- module behavior is assembled from reusable capability blocks
- parameter behavior is declared by metadata and family templates

### Good pattern

- `ecs_pack_left` is a module spec using:
  - flow regulator latent
  - thermal latent
  - valve state machine
  - derived parameters

- `elec_ac_bus_a` is a different module spec using:
  - regulated bus state
  - source topology
  - load transients

Both reuse the same simulator core.

## 5. Suggested spec schema

Each parameter should eventually declare at least:

- `parameter_name`
- `system_id`
- `subsystem_id`
- `module_id`
- `parameter_datatype_label`
- `units`
- `behavior_family`
- `latent_group`
- `sampling_rate_hz`
- `noise_scale`
- `quantization`
- `delay_class`
- `phase_envelope_id`
- `allowed_fault_families`

Each module should declare:

- `module_id`
- `module_family`
- `latent_variables`
- `controllers`
- `coupling_edges`
- `parameters`
- `state_machines`

Each subsystem/system should declare:

- roll-up metadata
- optional shared latent variables
- shared power/pressure/thermal dependencies if applicable

## 6. Recommended package layout

One reasonable next layout under `libs/simulation/` is:

- `specs.py`
  - hierarchy and parameter specs
- `runtime.py`
  - mutable runtime state containers
- `behaviors.py`
  - regulated/inertial/accumulative/discrete/derived behavior models
- `couplings.py`
  - latent variables, coupling edges, transfer functions, delays
- `scenario.py`
  - phase schedule, command profiles, mission envelopes
- `faults.py`
  - fault specs, schedulers, persistent degradation
- `sampling.py`
  - rate control, observation, dropout, quantization
- `labels.py`
  - event/anomaly/phase label emitters
- `constraints.py`
  - invariants and consistency checks
- `engine.py`
  - orchestration / simulation clock
- `builders.py`
  - spec/template construction helpers

This keeps simulation concerns separated by responsibility.

The first implemented step is the static spec layer in:

- [libs/simulation/specs.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/specs.py)

Current concrete spec objects:

- `PortSpec`
- `CouplingSpec`
- `ParameterSpec`
- `ModuleSpec`

Current adapter helpers:

- `parameter_spec_from_legacy_sensor(...)`
- `module_specs_from_hierarchy_spec(...)`
- `flatten_module_specs(...)`

This is intentionally only the static contract layer. Runtime state and module
execution are still separate next steps.

## 7. Minimal interface sketch

### parameter behavior

```python
class BehaviorModel(Protocol):
    def step(self, dt: float, latent: Mapping[str, float], state: ParameterState, context: StepContext) -> ParameterState: ...
    def observe(self, state: ParameterState, context: ObserveContext) -> object: ...
```

### coupling

```python
class CouplingEdge(Protocol):
    def apply(self, latent_store: LatentStateStore, context: StepContext) -> None: ...
```

### fault

```python
class FaultProcess(Protocol):
    def applies(self, context: StepContext) -> bool: ...
    def mutate_latent(self, latent_store: LatentStateStore, context: StepContext) -> None: ...
    def mutate_observation(self, parameter_name: str, observed_value: object, context: ObserveContext) -> object: ...
```

### label emitter

```python
class LabelEmitter(Protocol):
    def emit(self, context: StepContext, observations: Mapping[str, object]) -> list[dict[str, object]]: ...
```

These protocols should stay small and composable.

## 8. Recommended migration path

Do not rewrite the simulator in one step.

### Phase 1

Extract specs and runtime state:

- move hierarchy and parameter metadata into explicit spec objects
- keep current generation logic but stop relying on raw dictionaries everywhere

### Phase 2

Extract behavior families:

- regulated
- inertial
- accumulative
- discrete
- derived

### Phase 3

Extract coupling graph and shared latent states.

This is the largest modeling improvement.

### Phase 4

Separate nominal behavior from fault processes.

### Phase 5

Add invariants and simulation-quality checks.

## 9. Highest-value additions beyond hierarchy base classes

If only a few things are added next, they should be:

1. explicit parameter specs
2. explicit behavior-family objects
3. explicit coupling graph / latent-state layer
4. fault processes separated from nominal behavior
5. invariant checks
6. persistent tail-level degradation state

Those six additions will do more for realism and extensibility than adding more
module-specific logic to the current simulator core.

## 10. Recommendation

Use lightweight classes or dataclasses for hierarchy **only as identity and
containment objects**. Protocols are sufficient for the behavioral and orchestration
boundaries.

Put the real simulation semantics into:

- behavior families
- latent couplings
- observation models
- fault processes
- constraints

That is the architecture most likely to support a large open-ended hierarchy while
keeping the simulator coherent.
