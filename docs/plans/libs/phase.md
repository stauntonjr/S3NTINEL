# Phase Library Plan

Status: Plan
Authority: Non-authoritative roadmap. Use package READMEs and `docs/current/` for current behavior.

This plan covers the next phase-focused simulation work in the current object model.

For current implementation ownership, see:
- [libs/simulation/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/README.md)
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)

**Core idea**
Phases should not be simulated as labels pasted onto telemetry.  
They should be represented as a higher-level operating context that changes:
- targets
- enabled couplings
- controller modes
- noise/variance
- allowed state transitions
- anomaly priors

Then `phase_label` is emitted from that operating context.

**Layered design**

1. `PhaseSchedule`
- sequence of phase segments over a flight
- each segment has:
  - `phase_label`
  - start/end or duration rule
  - transition conditions if needed

2. `PhaseEnvelope`
- what a phase does to the system
- per phase, declare modifiers for:
  - latent targets/setpoints
  - coupling gates
  - mode states
  - noise scales
  - persistence / dynamics
  - anomaly likelihoods

3. `Module/parameter consumption`
- behaviors do not “know phases” directly unless needed
- they consume:
  - setpoints from latent state
  - gated couplings
  - mode states
  - context values injected from the assembly runtime
- phase affects those upstream inputs

4. `Label emission`
- `phase_label` should be emitted from the active phase schedule
- it remains a label, not an inferred property

**What phases should influence**

At minimum:

- `regulated` behaviors
  - target/setpoint changes by phase
- `inertial` behaviors
  - command targets and lag conditions by phase
- `discrete_state` behaviors
  - allowed state set / transition tendencies by phase
- couplings
  - `phase_gate` on inter-module couplings
- latent updates
  - different gains/defaults by phase if needed
- noise
  - low variance in steady phases, higher in transients

**Concrete examples**

- `gate_turnaround`
  - low aircraft altitude proxy
  - APU/electrical supply active
  - pressurization relaxed
  - some couplings disabled
- `taxi_out`
  - low-speed inertial changes
  - configuration states changing
  - moderate vibration/noise
- `takeoff_climb`
  - strong commanded responses
  - fast inertial ramps
  - pressurization/cabin couplings active
  - electrical and ECS loads change
- `cruise`
  - stable regulated targets
  - low drift
  - fewer discrete changes
- `descent/landing`
  - reverse transitions
  - pressurization and control couplings shift again

**How this maps to the current seam**

The current object model already supports the right hooks:

- `Flight` owns phase progression and phase-label emission
- simulation `phase/*` owns schedule and envelope specs/runtime
- `Coupling` supports `phase_gate`
- `Module` and `Parameter` consume phase-conditioned context through the active runtime path

So the next V2.1 phase step should be:

1. keep `Flight` as the owner of phase progression over time
2. keep simulation `phase/spec.py` and `phase/runtime.py` as the phase schedule/envelope seam
3. feed phase-conditioned context into module stepping through the current flight/runtime path

**What not to do**
- do not put all phase behavior inside each family generator
- do not make phase just a telemetry label
- do not hardcode phase logic deep inside module step code

Phase should stay one layer above local behavior, then flow down through context, gates, and modes.

**Short version**
V2.1 phase simulation plan is:

1. define a flight-level phase schedule
2. define per-phase operating envelopes
3. apply phases by modifying latent targets, mode states, and coupling gates
4. emit `phase_label` from the schedule
5. let telemetry behavior emerge from those constraints rather than from explicit phase-specific signal hacks

Current implementation names are now the source of truth:
- `PhaseProgramSpec`
- `Flight`
- simulation `phase/runtime.py`
