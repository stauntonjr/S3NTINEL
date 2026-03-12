# Simulation Architecture

This document explains the **current** simulation architecture at a conceptual level.

For the package-level ownership and current module layout, start with:
- [libs/simulation/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/README.md)
- [libs/windows/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/windows/README.md)
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)

Use this note for the architectural ideas and relationships, not as the authoritative file inventory.

## 1. Design Objective

The simulator should be:

- open-ended with respect to aircraft, systems, subsystems, modules, and parameters
- behaviorally expressive enough to represent regulated, inertial, accumulative, and discrete-state dynamics
- coupling-aware enough to generate meaningful multivariate structure
- phase-aware enough to drive realistic operating context
- fault-aware enough to inject simulation truth that downstream validators can compare against detection outputs

## 2. Core Design Rule

Use:

- hierarchy for composition
- behavior families for local dynamics
- couplings for cross-module structure
- phase programs for operating context
- fault programs for injected misbehavior

## 3. Current Simulation Model

### 3.1 Structural specs

The static simulation model is expressed through:

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

These specs describe what the aircraft is and how a flight should exercise it.

### 3.2 Runtime objects

The live runtime model is:

- `Aircraft`
- `Flight`
- `System`
- `Subsystem`
- `Module`
- `Parameter`
- `Port`
- `Coupling`
- phase runtime objects in `libs/simulation/phase`
- fault runtime objects in `libs/simulation/fault`

The important boundary is:

- `Aircraft` is the machine
- `Flight` is one run of that machine

`Flight` owns:
- run clock state
- phase progression
- fault scheduling
- row emission over time

`Aircraft` owns:
- object hierarchy
- couplings
- direct stepping of modules and parameters once the run context has been resolved

## 4. Behavior and Fault Semantics

Behavior-family implementations live in:
- [libs/behavior/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/behavior/README.md)

The simulation layer attaches those behaviors to parameters and feeds them:
- local parameter context
- port inputs
- latent state
- phase-conditioned context
- fault/violation context

Simulation-side injected misbehavior should be called **faults**.
Downstream anomaly outputs should be called **anomalies**.

## 5. Simulation Output Contract

The simulation now emits canonical telemetry-shaped rows directly, with additive simulation metadata.

The canonical fields match the real telemetry ingestion contract:

- `tail_id`
- `flight_id`
- `timestamp_utc`
- `parameter_name`
- `parameter_value`
- `date_utc`

Simulation adds metadata on the same rows, including:

- `parameter_value_clean`
- `phase_label`
- `system_id`
- `subsystem_id`
- `module_id`
- behavior/fault truth metadata

This is intentional: simulation should feed the same persisted fitting and inference pipeline that real telemetry feeds.

## 6. Relationship to the Rest of the Stack

The active end-to-end path is:

1. author an `AircraftSpec`
2. author a `FlightSpec`
3. run a `Flight`
4. emit canonical telemetry rows plus simulation truth metadata
5. persist those rows
6. run the normal stage pipeline

The canonical operational entrypoint is:
- `python -m scripts.run_sim_pipeline ...`

See:
- [scripts/README.md](/home/jrs/code/S3NTINEL/sentinel/scripts/README.md)
- [pipelines/README.md](/home/jrs/code/S3NTINEL/sentinel/pipelines/README.md)

## 7. Notes

- Older `assembly` / `native` terminology is obsolete in the active codepath.
- The authoritative implementation shape now lives in the package READMEs close to the code.
