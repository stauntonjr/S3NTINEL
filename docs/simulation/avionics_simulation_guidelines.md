# Avionics Simulation Guidelines

This note records practical guidance for choosing simulation inputs for the active
S3NTINEL path.

For the proposed next simulator structure that can support a large open-ended hierarchy,
see [simulation_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation/simulation_architecture.md).

The aim is not to encode one specific aircraft type. The aim is to keep the
simulator physically coherent enough that:

- event extraction is meaningful
- graph structure is not arbitrary
- phase behavior is distinguishable
- anomaly injection stresses the right mechanisms

The sources are mostly FAA handbooks and other primary aviation references, with a
small amount of clearly marked engineering inference where the sources describe system
principles rather than prescribe exact numeric telemetry rates.

## 1. General principles

### 1.1 Simulate latent system state, not just independent sensor traces

The strongest aviation couplings come from shared latent drivers:

- airspeed / altitude / vertical motion
- commanded thrust and engine spool state
- electrical source topology and load switching
- cabin pressure schedule and outflow-valve control
- flight-control command vs surface response

Simulation should therefore proceed from latent states and control schedules first,
then derive sensor channels from those states.

### 1.2 Keep phase dependence explicit

Many parameters are only interpretable relative to flight phase.

Examples:

- high thrust is normal in takeoff/climb and abnormal at gate
- cabin pressure schedule evolves with altitude
- bus-current transients around engine/APU start are contextual, not globally anomalous
- flap, gear, and spoiler states are phase-conditioned

If a signal can only be judged relative to phase, it should be simulated from a
phase-conditioned envelope, not one stationary process.

### 1.3 Distinguish regulated, inertial, and accumulative variables

This is one of the most useful design separations.

#### regulated variables

These are actively held near a target:

- bus voltage
- hydraulic pressure
- cabin differential pressure within schedule bounds
- some temperature-control outputs

They should appear relatively flat in nominal operation, with transient deviations
during switching or actuator demand.

#### inertial variables

These respond with finite lag and smoothness:

- engine spool speed
- airspeed
- pitch/roll/heading state
- cabin altitude

They should not jump arbitrarily unless the underlying driver or sensor itself fails.

#### accumulative variables

These integrate over time:

- fuel quantity
- battery state of charge
- inertial position error / drift
- cumulative degradation state

They should evolve slowly and monotonically or near-monotonically.

## 2. Domain-specific guidance

## 2.1 Air data and pitot-static channels

Relevant sources:

- FAA Pilot's Handbook of Aeronautical Knowledge, Chapter 8  
  https://www.faa.gov/sites/faa.gov/files/FAA-H-8083-25C.pdf
- FAA Instrument Flying Handbook  
  https://www.faa.gov/sites/faa.gov/files/pilots/FAA-H-8083-15B.pdf
- FAA ENR 1.7 Altimeter Errors  
  https://www.faa.gov/air_traffic/publications/atpubs/aip_html/part2_enr_section_1.7.html

These sources support a few strong simulation rules:

- indicated airspeed, pressure altitude, and vertical speed are not independent
- they share the pitot-static system / air data computer chain
- static-source disturbance affects multiple instruments together
- high angle of attack, flap deployment, and landing gear can introduce position/static
  errors
- blocked static conditions produce coupled distortions in altitude, VSI, and airspeed

### Recommended simulation behavior

- derive:
  - `ias`
  - `pressure_altitude`
  - `vertical_speed`
  from a common pressure-state model
- include configuration-dependent static error terms for:
  - flaps
  - gear
  - high angle of attack
- make `vertical_speed` noisier and more transient than altitude
- make `ias` respond faster than altitude, but not instantaneously

### Good anomaly families here

- pitot bias
- static bias
- blocked pitot
- blocked static
- phase-specific position error

These anomalies are valuable because they create coherent multi-channel failures rather
than arbitrary single-sensor noise.

## 2.2 Inertial, AHRS, and navigation channels

Relevant sources:

- FAA AIM 1-1-15 IRU/INS/AHRS  
  https://www.faa.gov/air_traffic/publications/atpubs/aim_html/chap1_section_1.html
- FAA AIM 1-2-1 FMS / coupled navigation inputs  
  https://www.faa.gov/air_traffic/publications/atpubs/aim/aim0102.html
- FAA AIM 1-2-4 GPS jamming/spoofing  
  https://www.faa.gov/air_traffic/publications/atpubs/aim_html/chap1_section_2.html
- Garmin integrated flight deck documentation (search surface used during research)  
  `static.garmin.com` pilot-guide documents

The core guidance is:

- AHRS provides attitude/heading, but not standalone position
- inertial solutions drift over time
- modern navigation solutions are multi-input and can fuse GPS/DME/IRU inputs
- magnetic disturbances can perturb heading on the ground or during taxi
- GPS is vulnerable to jamming/spoofing/interference and should not be treated as
  infallible

### Recommended simulation behavior

- couple:
  - pitch
  - roll
  - yaw/heading
  - body/turn rates
  to one inertial state process
- couple navigation position and groundspeed to:
  - inertial propagation
  - GPS updates
  - optional radio/FMS corrections
- include slow inertial drift when not corrected
- model heading bias or disturbances during taxi due to magnetic effects
- keep groundspeed/course separate from airspeed/heading via wind or bias terms

### Good anomaly families here

- inertial drift growth
- heading bias after taxi
- GPS degradation or dropout
- navigation solution disagreement
- delayed re-alignment / faulty fusion input

## 2.3 Propulsion and engine instrumentation

Relevant sources:

- FAA AMT Powerplant Handbook  
  https://www.faa.gov/regulationspolicies/handbooksmanuals/aviation/faa-h-8083-32b-aviation-maintenance-technician
- FAA-H-8083-32A / powerplant snippets on fuel flow, oil pressure, oil temperature  
  https://www.faa.gov/sites/faa.gov/files/regulations_policies/handbooks_manuals/aviation/FAA-H-8083-32-AMT-Powerplant-Vol-2.pdf
- NASA C-MAPSS engine simulator data  
  https://data.nasa.gov/dataset/c-mapss-aircraft-engine-simulator-data

FAA maintenance references and engine indications strongly suggest this structure:

- commanded power change drives fuel flow first
- spool speeds (`N1`, `N2`, `N3`, `EPR`) follow with lag
- EGT/ITT responds strongly during starts and transients
- oil pressure reacts faster than oil temperature
- starts have recognizable abnormal modes:
  - hot start
  - hung start
  - false start

NASA C-MAPSS supports the longer-horizon view that component degradation is gradual
and can persist across flights rather than appearing only as isolated spikes.

### Recommended simulation behavior

- simulate an engine latent state with:
  - throttle / power command
  - spool state
  - thermal state
  - lubrication state
  - degradation state
- derive:
  - `fuel_flow`
  - `N1/N2/N3`
  - `EGT/ITT`
  - `oil_pressure`
  - `oil_temperature`
from that shared state with realistic lags
- cross-engine channels should share some common phase/command influence but retain
  independent fault states

### Good anomaly families here

- hot-start / hung-start patterns
- slow efficiency degradation
- spool lag increase
- rising EGT at fixed thrust
- oil pressure drop with slower oil temperature response

## 2.4 Electrical power channels

Relevant sources:

- FAA Pilot's Handbook of Aeronautical Knowledge, Chapter 7 Aircraft Systems  
  https://www.faa.gov/sites/faa.gov/files/09_phak_ch7.pdf
- FAA AMT Airframe Handbook Volume 2  
  https://www.faa.gov/sites/faa.gov/files/2022-06/amt_airframe_hb_vol_2.pdf

The electrical-system guidance is straightforward:

- bus voltage is regulated and should be comparatively flat in nominal operation
- current, load, and source topology are more dynamic
- generators/alternators, battery, and APU or external power interact through switching
  logic and bus architecture

### Recommended simulation behavior

- keep `bus_voltage` near regulation with small noise
- allow transient dips or spikes during:
  - engine start
  - APU start
  - bus transfer
  - large actuator/electrical load changes
- treat battery state of charge as slow
- treat charge/discharge current as fast and sign-sensitive
- model generator online/offline and bus-tie states as discrete channels

### Good anomaly families here

- brief undervoltage during load transfer
- stuck contactor / bus-tie logic fault
- battery not charging
- generator dropping offline intermittently
- abnormal current without corresponding load state

## 2.5 Environmental control and pressurization

Relevant sources:

- FAA Pilot's Handbook of Aeronautical Knowledge, Chapter 7 Aircraft Systems  
  https://www.faa.gov/sites/faa.gov/files/09_phak_ch7.pdf
- FAA AMT Airframe Handbook Volume 2  
  https://www.faa.gov/sites/faa.gov/files/2022-06/amt_airframe_hb_vol_2.pdf

The important physical relationships are:

- cabin altitude and differential pressure are constrained by pressurization schedule
- outflow-valve motion affects cabin pressure dynamics
- cabin variables change more slowly than ambient altitude
- pack flow and pack outlet temperature couple to pressurization and cabin conditioning

### Recommended simulation behavior

- drive cabin pressure state from:
  - aircraft altitude
  - pressurization mode
  - outflow valve command/position
  - pack flow availability
- keep:
  - `cabin_altitude`
  - `delta_p`
  - `outflow_valve_position`
  tightly coupled
- make cabin temperature slower than pack outlet temperature
- discrete mode channels matter here:
  - auto/manual
  - pack on/off
  - pressurization mode

### Good anomaly families here

- slow pressurization lag
- valve stuck partially open
- pack underperforming
- cabin altitude rising too fast for aircraft climb schedule
- inconsistent mode/state transition

## 2.6 Hydraulics, flight controls, and actuation

Relevant sources:

- FAA Pilot's Handbook of Aeronautical Knowledge, Chapter 6 Flight Controls  
  https://www.faa.gov/sites/faa.gov/files/FAA-H-8083-25C.pdf
- FAA AMT Airframe Handbook Volume 2  
  https://www.faa.gov/sites/faa.gov/files/2022-06/amt_airframe_hb_vol_2.pdf

The useful abstractions are:

- control commands and surface positions are fast, but not instantaneous
- surface response should obey lag and rate limits
- hydraulic pressure is usually regulated, with transient demand excursions
- flaps, slats, spoilers, and gear are strongly stateful and dwell-heavy

### Recommended simulation behavior

- treat:
  - commanded surface
  - actual surface
  - body-rate response
  as separate but tightly coupled channels
- hydraulic pressure should remain near nominal except during:
  - heavy actuation
  - pump transitions
  - faults
- flap/gear/slat/spoiler states should have explicit dwell and transition timing

### Good anomaly families here

- rate-limited or lagging surface response
- asymmetric or incomplete deployment
- hydraulic pressure sag during actuation
- command/surface mismatch
- impossible surface-state transition

## 2.7 Fuel channels

Relevant sources:

- FAA Pilot's Handbook of Aeronautical Knowledge, Chapter 7 Aircraft Systems  
  https://www.faa.gov/sites/faa.gov/files/09_phak_ch7.pdf
- FAA AMT Powerplant Handbook, fuel and metering chapters  
  https://www.faa.gov/regulationspolicies/handbooksmanuals/aviation/faa-h-8083-32b-chapter-2-engine-fuel-fuel-metering

### Recommended simulation behavior

- fuel quantity should decrease monotonically except when:
  - refueling
  - tank transfer / balancing logic moves usable quantity between tanks
- fuel flow should be tightly coupled to engine power demand
- left/right tank imbalance should evolve slowly, not randomly

### Good anomaly families here

- leakage-like monotone depletion
- transfer logic fault
- flow/quantity inconsistency
- imbalance growth

## 3. Coupling motifs worth preserving

These are the couplings most worth simulating because they are likely to produce
useful graph structure.

### 3.1 common-cause phase couplings

- takeoff power -> fuel flow, spool speed, EGT, electrical load, cabin schedule change
- descent -> throttle reduction, vertical speed, pressurization schedule, configuration changes

### 3.2 regulated-system couplings

- bus voltage with source/load states
- hydraulic pressure with actuator demand
- cabin differential pressure with outflow-valve position

### 3.3 navigation-solution couplings

- attitude/heading from AHRS
- position/velocity from inertial + GPS/FMS fusion
- groundspeed vs airspeed separation due to wind

### 3.4 mechanical/actuation couplings

- control command -> surface position -> body response
- flap/gear configuration -> drag -> airspeed/trim/static error effects

## 4. Dynamic motifs worth preserving

These are more useful than exact OEM-specific numeric rates.

### 4.1 fast channels

Use for:

- body rates
- selected surface positions/commands
- electrical current/load transients
- some hydraulic/pressure transients

Engineering guideline:
- tens of samples per second or higher if you want the channel to carry intra-window
  variance or transition structure

This is an engineering heuristic, not an FAA-prescribed rate.

### 4.2 medium-rate channels

Use for:

- air data
- spool speeds
- fuel flow
- bus power/load summary
- pack flow / valve position

Engineering guideline:
- on the order of 1-10 Hz

### 4.3 slow channels

Use for:

- fuel quantity
- battery state of charge
- cabin temperature
- oil temperature
- degradation states

Engineering guideline:
- sub-Hz to low-Hz

The exact cutoffs should remain configurable and type-aware, not hard-coded as
aircraft-universal truth.

## 5. Noise, lag, and quantization guidance

### noise

- regulated channels: low-amplitude noise
- inertial/derived-rate channels: more visible high-frequency noise
- thermal and accumulative channels: low noise, high persistence

### lag

- actuation and engine channels should show finite lag
- cabin/thermal channels should show stronger lag
- graph-relevant causal links should include small lag jitter, not exact fixed delays

### quantization and dwell

- discrete avionics states should not chatter unless the fault is specifically about
  chatter or unstable logic
- state dwell times should be realistic and phase-conditioned

## 6. Recommended simulation metadata per parameter

Each simulated parameter should ideally carry metadata like:

- `system_id`
- `subsystem_id`
- `module_id`
- `parameter_name`
- `parameter_datatype_label`
- `units`
- nominal range
- phase sensitivity
- coupling group
- nominal lag class
- nominal noise scale
- sampling-rate class
- anomaly families allowed for that parameter

This metadata is more valuable than adding many one-off heuristics in code.

## 7. Practical guidance for the current simulator

For the current S3NTINEL simulator, the next upgrade should prioritize:

1. more realistic mixed-rate classes
2. explicit latent state drivers by subsystem
3. preserved common-cause phase couplings
4. regulated-variable behavior for electrical/hydraulic/pressurization channels
5. persistent degradation state across flights for engine- and subsystem-health channels
6. graph-relevant timing faults, not just value perturbations

## 8. What not to do

- do not simulate every parameter as an independent AR process
- do not give all continuous parameters the same dynamics or sampling rate
- do not inject anomalies only as isolated amplitude spikes
- do not ignore stateful logic channels such as gear, flap, pack, generator, or valve
  modes
- do not make regulated channels wander freely in nominal operation

## 9. References

- FAA Pilot's Handbook of Aeronautical Knowledge, FAA-H-8083-25C  
  https://www.faa.gov/sites/faa.gov/files/FAA-H-8083-25C.pdf
- FAA Instrument Flying Handbook, FAA-H-8083-15B  
  https://www.faa.gov/sites/faa.gov/files/pilots/FAA-H-8083-15B.pdf
- FAA Aeronautical Information Manual, AHRS / INS / IRU and FMS sections  
  https://www.faa.gov/air_traffic/publications/atpubs/aim_html/chap1_section_1.html  
  https://www.faa.gov/air_traffic/publications/atpubs/aim/aim0102.html  
  https://www.faa.gov/air_traffic/publications/atpubs/aim_html/chap1_section_2.html
- FAA Airplane Flying Handbook discussion of pitot-static/static-source failure behavior  
  https://www.faa.gov/sites/faa.gov/files/regulations_policies/handbooks_manuals/aviation/airplane_handbook/19_afh_ch18.pdf
- FAA AMT Airframe Handbook Volume 2  
  https://www.faa.gov/sites/faa.gov/files/2022-06/amt_airframe_hb_vol_2.pdf
- FAA AMT Powerplant Handbook / related powerplant references  
  https://www.faa.gov/regulationspolicies/handbooksmanuals/aviation/faa-h-8083-32b-aviation-maintenance-technician  
  https://www.faa.gov/sites/faa.gov/files/regulations_policies/handbooks_manuals/aviation/FAA-H-8083-32-AMT-Powerplant-Vol-2.pdf
- NASA C-MAPSS aircraft engine simulator data  
  https://data.nasa.gov/dataset/c-mapss-aircraft-engine-simulator-data
