# Simulation Architecture

This note proposes the next simulation architecture for S3NTINEL.

The goal is to support a large open-ended hierarchy without hard-coding
module-specific control flow into the simulator core.

For the mirrored profiling layer that can infer the same behavioral semantics from
real telemetry, see [behavior_profiling_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_profiling_design.md).
For the proposed per-family file/class layout, see [behavior_family_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_family_architecture.md) and [behavior_family_skeletons.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_family_skeletons.md).

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

Responsibilities:

- current latent values
- current observed values
- current discrete states
- cached delays / history
- persistent tail-level degradation state

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

## 3.4 Coupling layer

This is the most important missing abstraction in the current simulator.

Suggested objects:

- `LatentVariable`
- `CouplingEdge`
- `ControllerModel`
- `DelayModel`
- `TransferModel`

Examples:

- commanded thrust -> fuel flow -> spool speeds -> temperatures
- aircraft altitude -> pressurization controller -> outflow valve -> cabin altitude
- command surface target -> actuator response -> body-rate response
- pitot/static pressure state -> IAS / altitude / VSI

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
