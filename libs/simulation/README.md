# Simulation

This package contains the simulation domain model only.

It does not own:
- fitting
- inference
- pipeline stage orchestration
- dataframe-to-artifact bridge code beyond emitting canonical telemetry-shaped rows plus additive simulation metadata

The intended flow is:

1. Author an `AircraftSpec`
2. Author a `FlightSpec` that exercises that aircraft over time
3. Build a live `Flight`
4. Run the flight to produce canonical telemetry-shaped rows plus simulation metadata
5. Feed those rows into the same persisted fitting and inference pipelines used for real telemetry

## Core model

The runtime model is:

- `Aircraft`: the machine
- `Flight`: one run of that machine
- `System`, `Subsystem`, `Module`, `Parameter`, `Port`: the object hierarchy inside the aircraft
- `Coupling`: cross-module signal wiring
- `PhaseProgram`: operating phase schedule and envelopes
- `FaultProgram`: injected misbehavior schedule

The static model mirrors that shape with specs:

- `AircraftSpec`
- `SystemSpec`
- `SubsystemSpec`
- `ModuleSpec`
- `ParameterSpec`
- `PortSpec`
- `CouplingSpec`
- `PhaseProgramSpec`
- `FaultProgramSpec`
- `FlightSpec`

## Package layout

- `aircraft/`
  - `spec.py`: aircraft structure
  - `runtime.py`: live aircraft object
  - `examples.py`: example aircraft specs
- `system/`
  - `spec.py`, `runtime.py`, `examples.py`
- `subsystem/`
  - `spec.py`, `runtime.py`, `examples.py`
- `module/`
  - `spec.py`: module declaration and latent update specs
  - `runtime.py`: live module behavior and stepping
  - `examples.py`: example module specs
- `parameter/`
  - `spec.py`: parameter declaration
  - `runtime.py`: live parameter state and behavior attachment
  - `examples.py`: example parameter specs
- `port/`
  - `spec.py`: declared module interfaces
  - `runtime.py`: live port values
  - `examples.py`: example port specs
- `coupling/`
  - `spec.py`: cross-module wiring declarations
  - `runtime.py`: live coupling behavior, lag, gating, propagation
  - `examples.py`: example coupling specs
- `phase/`
  - `catalog.py`: canonical phase labels
  - `spec.py`: phase schedule and envelope specs
  - `runtime.py`: label resolution and envelope application
  - `examples.py`: example phase programs
- `fault/`
  - `spec.py`: fault-window declarations
  - `runtime.py`: per-step fault resolution
  - `examples.py`: example fault programs
- `flight/`
  - `spec.py`: run program definition
  - `runtime.py`: live flight execution
  - `examples.py`: named example flights

## Responsibility boundaries

### Aircraft

`Aircraft` is a timeless machine.

It owns:
- systems, subsystems, modules
- couplings
- direct machine stepping

It does not own:
- run clock
- step index for a flight
- phase schedule
- phase label resolution
- fault scheduling

Those belong to `Flight`.

### Flight

`Flight` is the run coordinator.

It owns:
- `tail_id`
- `flight_id`
- `start_timestamp_utc`
- `step_index`
- `current_timestamp_utc`
- `current_phase_label`
- one-time initial state application
- phase resolution and phase envelope application
- fault resolution
- the outer run loop

Key runtime methods:
- `Flight.from_spec(...)`
- `Flight.step(...)`
- `Flight.iter_ticks(...)`
- `Flight.simulate_rows(...)`

### Module

`Module` owns local runtime state and local stepping.

That includes:
- parameters
- ports
- latent state
- controller state
- mode state
- delayed input transfers
- latent updates

The main local execution path lives on `Module.step(...)`.

### Parameter

`Parameter` is the live observable quantity.

It owns:
- identity and metadata
- observed value
- clean value
- bound behavior implementation

`Parameter.from_spec(...)` attaches the correct behavior from `libs/behavior`.

### Port

`Port` is the live module interface signal.

Ports are still explicit in the model. They are used as coupling endpoints between modules.

### Coupling

`Coupling` is the live runtime object for inter-module propagation.

It owns:
- source and target endpoint identity
- gating rules
- lag handling
- signal application behavior

There is no second local coupling system. Aircraft-level couplings are the canonical coupling model.

### Phase

Phase is split into:
- canonical labels in `phase/catalog.py`
- static scheduling and envelope definitions in `phase/spec.py`
- runtime resolution and envelope application in `phase/runtime.py`

Phase envelopes can affect:
- module mode state
- latent state
- step-input context

### Fault

`fault` is the injection domain for simulated misbehavior.

Use `fault`, not `anomaly`, for injected simulation-side misbehavior.
`anomaly` is reserved for downstream detection outputs.

`FaultProgram` resolves per-step violation context, which is then passed into the machine step.

## Authoring model

There are two main authoring layers:

- structural authoring: `AircraftSpec`
- run authoring: `FlightSpec`

### AircraftSpec

Use `AircraftSpec` to declare:
- systems
- subsystems
- modules
- parameters
- ports
- couplings

It should answer: "what is this aircraft?"

`AircraftSpec.validate()` is the structural validation entrypoint.

### FlightSpec

Use `FlightSpec` to declare:
- which aircraft is being flown
- input program over time
- initial conditions
- phase program
- fault program

It should answer: "how is this aircraft being exercised in this run?"

`FlightSpec` contains:
- `aircraft_spec`
- `input_program_spec`
- `initial_state_spec`
- `phase_program_spec`
- `fault_program_spec`
- optional metadata

## Named examples

Example builders are intentionally split by domain.

Composite examples build upward:

- parameter examples feed module examples
- module and coupling examples feed aircraft examples
- aircraft, phase, and fault examples feed flight examples

## Data / Artifacts

Simulation rows are emitted in canonical telemetry shape and are intended to feed the same persisted fitting and inference pipeline used for real telemetry.

Important row fields:
- canonical telemetry fields such as `tail_id`, `flight_id`, `timestamp_utc`, `parameter_name`, `parameter_value`, `date_utc`
- additive simulation metadata such as clean values, phase labels, hierarchy context, and fault truth metadata

Downstream structure such as the `WindowFeaturesDataFrame` belongs to `libs/windows`, not to this package.

## Subject Matter View

This package answers:
- what aircraft structure is being simulated
- how a particular flight exercises that structure over time
- what injected faults and phase programs shape the telemetry

## Testing / Validation

- unit tests cover simulation objects and example authoring
- integration tests cover simulation row emission and the persisted `scripts.run_sim_pipeline` path

## Notes / Constraints

- The canonical simulation entrypoint is `python -m scripts.run_sim_pipeline ...`
- `ParameterSpec.sampling_rate_hz` remains a tracked TODO; current behavior is one emitted sample per parameter per tick

The main named public example seam today is:
- `libs.simulation.flight.examples`

Notable helpers there:
- `build_named_flight_spec(...)`
- `list_flight_names()`

## Output shape

Simulation output is meant to be directly consumable by the real telemetry pipelines.

The canonical raw telemetry fields are emitted directly by `FlightTick.telemetry_rows()`:

- `tail_id`
- `flight_id`
- `timestamp_utc`
- `parameter_name`
- `parameter_value`
- `date_utc`

Simulation adds metadata columns on the same rows, including:

- `step_index`
- `phase_label`
- `system_id`
- `subsystem_id`
- `module_id`
- `parameter_value_clean`
- `behavior_family_label`
- `target_source`

This means simulation is not producing a separate bespoke telemetry schema that later has to be translated into the pipeline contract. The rows are already pipeline-shaped.

`FlightTick.phase_row()` emits the associated phase label record for evaluation and validation paths.

## How simulation feeds the main pipelines

Simulation should feed the same persisted pipelines used for real telemetry.

The canonical operational entrypoint is:

- `python -m scripts.run_sim_pipeline --flight-name power_chain --base-dir data/sim_runs --mode full --format parquet`

That runner:

1. resolves a named `FlightSpec`
2. builds a live `Flight`
3. runs the flight
4. writes canonical raw telemetry rows to `input/raw_telemetry`
5. writes phase labels and hierarchy labels
6. invokes the real persisted stage stack

There is no separate simulation-specific fitting or inference pipeline anymore.

The one remaining seam is persistence:
- simulation runs in memory
- the persisted pipeline reads parquet/Spark tables

So the runner still writes the already-canonical rows to disk before `00_ingest_raw.py` reads them.

That is persistence, not schema translation.

## Important current constraints

### Parameter sampling rate

`ParameterSpec.sampling_rate_hz` exists, but true parameter-rate-aware emission is still deferred.

Current behavior:
- one emitted sample per parameter per simulation tick

Follow-up work still needed:
- make parameter emission cadence honor `sampling_rate_hz`
- decide sparse emission vs carry-forward semantics

### Ports

Ports remain explicit for now.

That decision is intentional but not final. The current model keeps them because they make module interfaces and coupling endpoints explicit.

### Behaviors

Behavior implementations still live in `libs/behavior`.

`libs/simulation` does not re-implement those families. It attaches them to parameters and invokes them through the simulation runtime.

## Public root exports

The curated `libs.simulation` root surface exports noun types and phase helpers only.

Examples:
- `Aircraft`, `AircraftSpec`
- `Flight`, `FlightSpec`, `FlightTick`
- `Module`, `ModuleSpec`
- `Parameter`, `ParameterSpec`
- `Port`, `PortSpec`
- `Coupling`, `CouplingSpec`
- `PhaseProgram`, `PhaseProgramSpec`
- `FaultProgramSpec`

The root does not export:
- pipeline bridge helpers
- fitting/scoring helpers
- old assembly-era names
- compatibility wrappers

## Recommended usage

For new code:

- import specs and runtimes from their owning subpackages when possible
- use `Flight.from_spec(...)` as the live run constructor
- use `Flight.simulate_rows(...)` when you need canonical simulation rows
- use `scripts.run_sim_pipeline` when you want a persisted end-to-end run through fitting and inference

Avoid:
- introducing parallel pipeline helpers under `libs/simulation`
- reintroducing compatibility shims for old names
- authoring flights with opaque callback glue when a declarative spec is sufficient

## Mental model

If you only keep one distinction in mind, keep this one:

- `AircraftSpec` says what the machine is
- `FlightSpec` says how that machine is exercised over time
- `Aircraft` is the live machine
- `Flight` is the live run

Everything else in this package exists to support that model cleanly.
