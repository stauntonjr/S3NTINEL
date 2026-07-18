# Computational Complexity Report

This note grounds the active V2 pipeline complexity in the current code and in one concrete checked-in workload.

Use it for:
- stage-by-stage cost reasoning
- current hotspot identification
- scale-driver discussion for parameters, rates, windows, phases, graph density, and anomaly fan-out
- both time-complexity and space-complexity reasoning across the active pipeline

For current stage ownership and entrypoints, see [pipelines/README.md](../../pipelines/README.md).
For current implementation surfaces, the main builder modules are:
- [libs/profiling/profiles.py](../../libs/profiling/profiles.py)
- [libs/events/pipeline.py](../../libs/events/pipeline.py)
- [libs/events/continuous.py](../../libs/events/continuous.py)
- [libs/windows/pipeline.py](../../libs/windows/pipeline.py)
- [libs/windows/features.py](../../libs/windows/features.py)
- [libs/backbone/pipeline.py](../../libs/backbone/pipeline.py)
- [libs/graph/pipeline.py](../../libs/graph/pipeline.py)
- [libs/graph/hierarchy_artifacts.py](../../libs/graph/hierarchy_artifacts.py)
- [libs/phase/pipeline.py](../../libs/phase/pipeline.py)
- [libs/phase/fit.py](../../libs/phase/fit.py)
- [libs/phase/decode.py](../../libs/phase/decode.py)
- [libs/scoring/tables.py](../../libs/scoring/tables.py)
- [libs/anomaly/pipeline.py](../../libs/anomaly/pipeline.py)
- [libs/anomaly/frames.py](../../libs/anomaly/frames.py)
- [libs/anomaly/tables.py](../../libs/anomaly/tables.py)

## Scope And Basis

This report uses two references:

- `data/simulation_runs/20260319T191636Z_power_pressurization_hierarchy_composite`
- `/tmp/s3ntinel_single_path_verify/20260411T163245Z_power_pressurization_hierarchy_composite`

It is grounded in:

- the persisted stage manifests, stage summaries, and parquet artifacts under those run directories
- the current code in `pipelines/` and `libs/*`

Use them differently:

- the checked-in March 19, 2026 bundle is the stable workload-shape reference
- the April 11, 2026 replay under `/tmp` is the current-head timing and artifact-shape reference

The current-head timing snapshot covers stages `00` through `95` successfully.

## Complexity Symbols

| Symbol | Meaning |
| --- | --- |
| `R` | raw telemetry rows |
| `P` | total parameters |
| `P_n` | numeric or constant parameters |
| `E` | detected event rows |
| `W` | windows / `window_features` rows |
| `C` | selected backbone sensors |
| `K_w` | distinct event-bearing parameters inside one window |
| `d_w` | active continuous sensors represented in one sparse window vector |
| `F_phi` | phase feature width |
| `H` | fused graph edges after graph construction |
| `M` | retained `lag_profile` rows after aggregation |
| `A` | emit-ready anomaly windows |
| `A_tel` | anomaly telemetry attribution rows |
| `A_evt` | anomaly event attribution rows |
| `W_phase` | windows in one calibration partition `(tail, flight, phase)` |
| `n_b` | event volume in one lag bucket |
| `I` | phase centroid-refinement iterations |
| `retained_edges` | fused edges retained into hierarchy rollup after neighbor pruning |
| `B_drv` | explicitly collected driver-side rows, vectors, or small matrices |
| `J_{R,W}` | raw-to-window interval join work |
| `J_{E,W}` | event-to-window interval join work |
| `J_{A,R}` | emit-ready-window-to-raw attribution join work |
| `J_{A,E}` | emit-ready-window-to-event attribution join work |

Most current stage costs are dominated by four recurring shapes:

- long ordered per-parameter or per-flight scans
- pair expansion inside windows or lag buckets
- rejoining emit-ready windows back to raw and event evidence
- dense residual materialization or bounded driver-side bridges

## Time And Space Reading Guide

In this report:

- time complexity means the dominant work to produce a stage's outputs
- space complexity means the dominant live state, shuffle/state spill pressure, driver-collection pressure, and persisted output size that the stage creates

Those are related but not identical in Spark.

- sort-dominated stages usually have time proportional to sort plus scan, but their practical space pressure is the largest active partition plus spill
- pair-expansion stages can have moderate final outputs while still hitting large transient local pair buffers
- interval-join stages can have moderate compute but large materialized outputs when many windows overlap the same evidence
- driver-bridge stages can look cheap in wall time until one collected matrix or edge universe exceeds its hard bound

## First-Principles Complexity Model

Before looking at any logged run, the implemented codepaths already force a small set of complexity classes.

### Core Execution Primitives

The active pipeline is built from five recurring primitives:

- partition-local sorts created by `Window.partitionBy(...).orderBy(...)`
- segmented stateful folds built through [libs/spark_sequence/plan.py](../../libs/spark_sequence/plan.py)
- interval joins between windows and raw/event rows
- sparse map explosions and regrouping
- bounded driver bridges for small matrices or pruned graph rollups

Those primitives matter more than the stage wrappers themselves.

In particular:

- any stage dominated by Spark window functions behaves like a per-partition sort plus a linear scan
- any stage dominated by same-window or same-bucket pair generation can become quadratic in local density even if the final output table is small
- any stage that turns sparse per-window maps back into dense all-sensor arrays inherits a hidden `P_n` factor
- any stage with an explicit local collection is safe only while it stays under its configured bound

### Stage Envelopes From Implemented Codepaths

| Stage | Implemented dominant primitives | Time envelope | Space / materialization envelope |
| --- | --- | --- | --- |
| `00` ingest | projection + persisted write | `O(R)` | `O(R)` persisted output, `O(1)` live streaming state |
| `10` profiles | grouped aggs, percentiles, per-parameter ordered lags | `O(R) + O(sum_p R_p log R_p)` | `O(max_p R_p)` partition sort state + `O(P)` outputs |
| `12` behavior profiles | profiling-plan reuse plus primitive/family derivation | `O(R) + O(sum_p R_p log R_p)` | `O(max_p R_p)` ordered state + `O(P)` profile outputs + bounded Python-side family state |
| `15` event profiles | per-parameter detector-policy profiling from raw telemetry | `O(R) + O(sum_p R_p log R_p)` | `O(max_p R_p)` ordered state + `O(P)` output profiles |
| `20` events | per-parameter ordered windows, segmented stateful folds | `O(sum_p R_p log R_p) + O(R)` | `O(max_p R_p)` ordered partition state + `O(E)` persisted events |
| `25` window policy profile | small candidate frontier replayed over ordered events | `O(K_policy * (sum_f E_f log E_f + E))` | `O(max_f E_f)` per-candidate ordered state + `O(K_policy)` profile rows |
| `30` windows | per-flight event ordering, segmented fold, event-to-window assignment | `O(sum_f E_f log E_f) + O(E) + O(J_{E,W})` | `O(max_f E_f)` ordered / segmented state + `O(W)` persisted windows |
| `40` window features + backbone | raw interval build, raw/event interval joins, sparse-map agg, small ridge solve | `O(J_{R,W} + J_{E,W} + W * d_w + W * C^2 + W * C * d_w + C^3)` | `O(W * d_w)` sparse features + `O(C^2 + C * d_w + B_drv)` driver-side matrices |
| `50` graphs | same-window pair expansion, candidate-pruned lag-bucket pair expansion, transition pass, small precision solve | `O(sum_w K_w^2) + O(E) + O(sum_b sum_v n_{b,v} * c_v * (m_{b,u} + m_{b-1,u})) + O(M) + O(W * C^2 + C^3)` | `O(max_w K_w^2)` same-window pair state + `O(max_b sum_v n_{b,v} * c_v * (m_{b,u} + m_{b-1,u}))` lag-pair state + `O(H + M + P + B_drv)` persisted/driver graph state |
| `60` hierarchy | Spark neighbor ranking + bounded driver rollup | `O(H log k) + O(P + retained_edges)` | `O(H + retained_edges)` distributed edge state + `O(P + retained_edges + B_drv)` driver rollup |
| `70` phase | dense residual reconstruction, per-flight scaling, centroid refinement, segmented decode | `O(W * P_n) + O(W * F_phi) + O(I * W * F_phi * phase_count)` | `O(W * (P_n + F_phi + phase_count))` dense residual and decode state |
| `72` phase-label centroids | truth-label assignment plus bounded centroid comparison | `O(W * F_phi)` plus bounded comparison work | `O(W * F_phi + B_drv)` because comparison materializes bounded pandas views |
| `80` raw scores | small broadcast joins, dense residual explode, subsystem regroup | `O(W * F_phi) + O(W * P_n)` | `O(W * (F_phi + P_n))` dense residual state + `O(P)` bridge tables |
| `85` calibration | per-phase partition ordering + window functions | `O(W log W_phase)` | `O(W_phase)` partition state + `O(W)` outputs |
| `90` attribution | emit-ready filter, raw/event interval joins, context regroup | `O(J_{A,R} + J_{A,E})`, with practical amplification from overlapping windows | `O(A_tel + A_evt + A)` persisted attribution outputs + local interval-join fanout state |
| `95` explorer bundle | linear export over already-built artifacts | `O(R + E + A_tel + A_evt + A)` | `O(R + E + A_tel + A_evt + A)` exported explorer materialization |

The important first-principles conclusions are:

- stages `10`, `12`, `15`, `20`, `25`, `30`, and `70` are sort- or replay-dominated
- stages `50` and `90` are the only stages with serious local fan-out risk
- stages `40`, `70`, and `80` hide dense `P_n` work behind otherwise sparse artifacts
- stages `40`, `50`, `60`, `72`, and `80` still rely on bounded driver-side collections or bounded local materialization for at least one subproblem

### Codepath Notes By Stage

#### Ordered-Window Stages

`10`, `20`, `30`, and `70` all use explicit `Window.partitionBy(...).orderBy(...)` clauses in their hot paths.

That means their true first-principles runtime is better thought of as:

- sort cost per logical partition
- then a linear scan over the sorted partition

For those stages, skew in:

- flight length
- per-parameter sample count
- event count per flight

matters more than global row count alone.

#### Pair-Expansion Stages

`50_build_graph` is the only stage where the implemented algorithm can still create a genuinely quadratic local explosion.

Two codepaths matter:

- event graph: upper-triangular same-window parameter pairing, `O(K_w^2)` per window
- lag profile: candidate-pruned nearest-prior pairing across same and adjacent lag buckets, `O(sum_v n_{b,v} * c_v * (m_{b,u} + m_{b-1,u}))` per bucket in the current implementation

The lag-profile path is still the most dangerous part of stage `50` because local density is driven by wall-clock event concentration, not by the window policy. The current code collapses `lag_profile` into a legacy `lag_graph` afterward, but that collapse is linear in the materialized profile rows and is not the asymptotic hotspot.

Before the multi-band redesign, the lag term was better approximated as:

- old single-lag path: `O(sum_b n_b (n_b + n_{b-1}))`

because each current event could pair with every prior event in the same or previous `tau` bucket before nearest-prior aggregation removed duplicates.

After the redesign, the implemented lag term is better approximated as:

- current multi-band path: `O(E) + O(sum_b sum_v n_{b,v} * c_v * (m_{b,u} + m_{b-1,u})) + O(M)`

where:

- `n_{b,v}` is the count of current events for target parameter `v` in bucket `b`
- `c_v` is the number of candidate predecessor parameters allowed for target `v`
- `m_{b,u}` and `m_{b-1,u}` are the counts of prior events for candidate source `u` in the current and previous bucket
- `M` is the number of retained `lag_profile` rows after aggregation

This is the important complexity change in stage `50`:

- the old lag builder expanded over all cross-parameter event pairs inside the bounded `tau` neighborhood
- the new lag builder first restricts to candidate parameter pairs derived from `event_graph` and `transition_graph`
- multiple lag bands do not multiply the self-join cost, because band assignment happens after the single nearest-prior pass

So the new path is strictly better for the intended workload when the candidate graph is sparse, but it is not a proof of subquadratic worst-case behavior. If candidate pruning degenerates and a hot bucket contains many events for many candidate parameters, the local lag join can still approach the old quadratic regime.

#### Interval-Join Stages

The implemented interval joins are the other major first-principles cost driver.

They appear in:

- `40` when building snapshots and events-in-window features
- `90` when reconstructing telemetry and event attribution for emit-ready windows

These joins are not expensive because of arithmetic. They are expensive because the code must re-match one time-indexed stream against many interval records, and overlapping windows can multiply the effective evidence volume.

#### Dense-Vector Stages

Three stages densify sparse data on purpose:

- `40` builds `G` and `H` against the selected backbone sensors
- `70` reconstructs a dense residual map over all numeric sensors
- `80` explodes that dense residual map again for subsystem scoring

So even though `window_features` is sparse, later stages still carry a hidden `P_n` term whenever they touch reconstruction residuals.

#### Bounded Driver Bridges

The codebase currently contains several explicit “small enough to collect” assumptions:

- backbone sensor solve in `40`
- graph parameter universe materialization in `50`
- retained hierarchy rollup edges in `60`
- phase baseline and hierarchy bridge tables in `80`

These are reasonable as long as:

- `C` stays small
- graph pruning stays aggressive
- hierarchy rollup edges stay sparse
- baseline and hierarchy reference tables remain small

They are not fleet-scale-free by first principles; they are fleet-scale-safe only while their bounds hold.

## Current Workload Signature

### Dataset Shape

| Metric | Current value |
| --- | --- |
| Tails | `1` |
| Flights | `1` |
| Flight duration | `1,679.5 s` = `0.4665 h` |
| Raw telemetry rows | `201,600` |
| Parameters | `96` |
| Truth datatypes | `80` numeric, `16` categorical |
| Profiled datatypes | `80` numeric, `8` binary, `8` categorical |
| Truth behaviors | `44` regulated, `28` inertial, `8` accumulative, `16` discrete-state |
| Hierarchy labels | `3` systems, `24` subsystems, `48` modules |
| Events | `92,875` |
| Windows | `4,644` |
| `window_features` rows | `4,644` |
| Phase windows | `4,644` |
| Phase baselines | `3` |
| Backbone selected sensors | `8` |
| Backbone numeric universe | `80` |
| Fused graph edges | `641` |
| Emit-ready windows | `4,644` of `4,644` |

### Sampling Rate Mix

| Rate | Parameters | Raw rows | Share of raw rows |
| --- | --- | --- | --- |
| `0.5 Hz` | `24` | `20,160` | `10%` |
| `1.0 Hz` | `36` | `60,480` | `30%` |
| `2.0 Hz` | `36` | `120,960` | `60%` |

Implication:

- the fastest `37.5%` of parameters generate `60%` of the raw rows
- stages driven by ordered raw telemetry see rate skew more strongly than they see parameter count alone

### Hierarchy Shape

The current simulation workload is perfectly balanced by construction:

- `32` parameters per system
- `4` parameters per subsystem
- `2` parameters per module

That makes the current hierarchy-rollup problem easier than a realistic fleet where one subsystem may have a much larger parameter fan-out than another.

### Window And Feature Shape

Current windows are short and saturated by the `event_threshold=20` policy:

- window duration mean `367 ms`, median `500 ms`, p99 `1,000 ms`, max `2,000 ms`
- window event count mean `19.999`, median `20`, max `20`
- `4,643` of `4,644` windows closed on `event_threshold`
- mean `sensor_count` per window is `19.53`, median `20`, max `20`

Current `window_features` sparsity:

- mean continuous-vector size per window: `36.0` numeric sensors
- max continuous-vector size per window: `64`
- categorical-state size per window: fixed `8`
- event-type count map size mean `2.05`, max `7`

Implication:

- event graph same-window pair generation is currently bounded by short windows and the `20`-event cap
- phase and scoring still expand some data back to dense all-numeric vectors later

### Event Mix

Detected event counts are dominated by continuous slope events:

- `slope_pos`: `54,900`
- `slope_neg`: `37,718`
- everything else combined: `257`

By truth datatype:

- numeric parameters: `92,751` events
- categorical parameters: `124` events

By truth behavior family:

- inertial: `44,307`
- regulated: `41,796`
- accumulative: `6,648`
- discrete-state: `124`

Implication:

- event volume, graph cost, and window count are currently driven by numeric dynamics, not categorical chatter
- current behavior-family differences matter mainly through event density, not through distinct execution paths; most stages do not branch algorithmically by behavior family yet

### Phase Shape

Truth phase durations in the checked-in run:

- `gate_turnaround`: `240 s`
- `takeoff_climb`: `360 s`
- `cruise`: `720 s`
- `descent_approach`: `360 s`

Detected phase distribution:

- phase `0`: `199` windows
- phase `1`: `552` windows
- phase `2`: `3,893` windows

Current phase feature width:

- `F_phi = 24`
- composition: `8` selected backbone sensors + `6` selected event types + `6` selected categorical state pairs + `4` aggregate window-level features

Baseline support is skewed:

- stable windows supporting phase baselines: `55`, `37`, and `2`

Implication:

- compute is still driven by total `W`
- phase support skew matters more for statistical stability than for runtime, but it can keep some phase partitions tiny

## Current Hotspots

Current-head stage timings from the April 11, 2026 verification replay (`20260411T163245Z_power_pressurization_hierarchy_composite`):

| Stage | Elapsed | Share of full run |
| --- | --- | --- |
| `20_events_extract` | `125.2 s` | `18.6%` |
| `50_build_graph` | `119.8 s` | `17.8%` |
| `25_window_policy_profile` | `73.8 s` | `10.9%` |
| `40_backbone_fit` | `69.4 s` | `10.3%` |
| `70_phase_fit` | `66.8 s` | `9.9%` |
| `90_anomaly_attribution` | `54.1 s` | `8.0%` |
| `12_behavior_profiles_fit` | `52.0 s` | `7.7%` |
| all other successful stages combined | `112.8 s` | `16.7%` |

This is a materially different hotspot picture from the older March run:

- `30_windows_adaptive` is no longer the second-largest stage because the current policy profile selects a much smaller emitted-window workload
- `25_window_policy_profile` is now a real end-to-end hotspot
- `90_anomaly_attribution` is still important, but it is no longer dominated by an all-emit-ready path

Inside `50_build_graph`, the current dominant sub-step is no longer lag construction. It is graph evaluation reporting:

- `graph_evaluation_report`: `91,264.0 ms`
- `output_writes`: `6,300.3 ms`
- `parameter_universe_build`: `4,686.0 ms`
- `lag_profile_build`: `3,899.0 ms`
- `event_graph_build`: `3,693.6 ms`
- `output_counts`: `3,276.8 ms`
- `fused_graph_build`: `2,877.8 ms`
- `lag_graph_build`: `2,108.5 ms`

That means the current codebase has two distinct graph-stage stories:

- the asymptotic modeling risk is still pair expansion inside `event_graph` and `lag_profile`
- the current wall-clock hotspot is the validation/reporting side of stage `50`, not the edge builders themselves

This distinction matters when deciding what to optimize:

- for model-scale risk, the lag-profile and same-window pair builders still deserve the most attention
- for current end-to-end runtime, graph evaluation reporting is now the largest single sub-step inside stage `50`

## Current Space Hotspots

Current-head space pressure is concentrated in a different set of places than the raw timing table suggests.

The most important current materialization facts are:

- `window_scores_calibrated`: `307` rows
- `emit_ready = true`: `76` rows
- `anomaly_window_attribution`: `76` rows
- `anomaly_telemetry_attribution`: `25,585` rows
- `anomaly_event_attribution`: `180` rows

So on current head:

- stage `90` is no longer dominated by universal emission, but it still materializes a telemetry-attribution table that is large relative to `A`
- stage `70` and stage `80` remain the main dense-vector stages because they reconstruct or explode all-numeric residual state
- stage `50` remains the main transient pair-expansion risk even though its persisted edge tables are modest
- stage `40`, stage `50`, stage `60`, stage `72`, and stage `80` still have bounded driver-side bridges that are safe only while the current bounds hold

## Empirical Stage-By-Stage Analysis

Unless a section explicitly says otherwise, the artifact counts in this walkthrough still refer to the checked-in March 19 workload bundle. The current-head timing hotspots are summarized above.

### 00 Ingest Raw

Primary code:

- [pipelines/00_ingest_raw.py](../../pipelines/00_ingest_raw.py)

Cost shape:

- `O(R)` scan, normalize, and persisted write

Current observation:

- `201,600` rows written in `5.4 s`

This stage is linear and not a current hotspot.

### 10 Parameter Profiles Fit

Primary code:

- [pipelines/10_parameter_profiles_fit.py](../../pipelines/10_parameter_profiles_fit.py)
- [libs/profiling/profiles.py](../../libs/profiling/profiles.py)

Dominant work:

- grouped datatype/scaling summaries over raw telemetry
- ordered per-parameter lag windows to estimate median interval and to derive behavior features
- approximate quantiles per parameter

Cost shape:

- grouped summaries: roughly `O(R)`
- per-parameter ordered windows: roughly `O(sum_p R_p log R_p)`
- output size: `O(P)` for datatype and behavior, `O(P_n)` for scaling

Current observation:

- input `R = 201,600`
- output `96` datatype rows, `80` scaling rows, `96` behavior rows
- elapsed `20.5 s`

Key scaling note:

- high-rate channels dominate this stage disproportionately because `R_p` changes with sampling rate; a `2 Hz` channel contributes `4x` the ordered-row work of a `0.5 Hz` channel over the same flight duration

### 12 Behavior Profiles Fit

Primary code:

- [pipelines/12_behavior_profiles_fit.py](../../pipelines/12_behavior_profiles_fit.py)
- [libs/profiling/pipeline.py](../../libs/profiling/pipeline.py)
- [libs/profiling/profiles.py](../../libs/profiling/profiles.py)

Dominant work:

- reuse raw telemetry plus datatype/scaling profiles
- build behavior-primitive summaries
- derive behavior-family assignments from those primitive summaries

Time shape:

- the primitive-profile side is still dominated by ordered per-parameter telemetry scans, so practical time stays near `O(R) + O(sum_p R_p log R_p)`

Space shape:

- `O(max_p R_p)` ordered partition state plus `O(P)` primitive/family outputs
- additional Python-side family-state footprint stays bounded per parameter and does not scale with `R` once one parameter partition is in memory

Current-head timing observation:

- elapsed `52.0 s` in the April 11 verification replay

Key scaling note:

- this stage is still rate-skew-sensitive like stage `10`, but it now also pays for Python-side family reasoning after the Spark-side primitive pass

### 15 Event Profiles Fit

Primary code:

- [pipelines/15_event_profiles_fit.py](../../pipelines/15_event_profiles_fit.py)
- [libs/events/profiling.py](../../libs/events/profiling.py)

Dominant work:

- derive per-parameter event detector policy profiles from raw telemetry and datatype profiles

Time shape:

- approximately `O(R) + O(sum_p R_p log R_p)` because it still depends on per-parameter telemetry-order statistics

Space shape:

- `O(max_p R_p)` ordered state plus `O(P)` persisted detector-policy outputs

Current-head timing observation:

- elapsed `7.1 s` in the April 11 verification replay

Key scaling note:

- this stage is not a top hotspot now, but it scales with the same sampling-rate skew as the profiling stages before it

### 20 Events Extract

Primary code:

- [pipelines/20_events_extract.py](../../pipelines/20_events_extract.py)
- [libs/events/pipeline.py](../../libs/events/pipeline.py)
- [libs/events/continuous.py](../../libs/events/continuous.py)
- [libs/events/categorical.py](../../libs/events/categorical.py)

Dominant work:

- ordered per-parameter window functions for lag, lead, EMA-like smoothing, residuals, oscillation features, and extrema features
- segmented state-machine folds for switch and oscillation logic
- categorical transition tracking

Cost shape:

- ordering-heavy continuous detector: roughly `O(sum_p R_p log R_p)`
- segmented stateful folds: linear in the already-ordered rows
- output size: `O(E)`

Current observation:

- `201,600` raw rows became `92,875` event rows
- elapsed `62.5 s`
- `99.7%` of events are `slope_pos` or `slope_neg`

Key scaling note:

- current event volume is controlled much more by continuous numeric dynamics than by categorical states, so any increase in numeric noise, rate, or oscillatory behavior hits this stage first

### 25 Window Policy Profile

Primary code:

- [pipelines/25_window_policy_profile.py](../../pipelines/25_window_policy_profile.py)
- [libs/windows/policy_profile.py](../../libs/windows/policy_profile.py)
- [libs/windows/tables.py](../../libs/windows/tables.py)

Dominant work:

- derive a small candidate policy frontier from event gaps
- replay candidate window policies against the ordered event stream
- score candidates and emit a selected policy plus candidate frontier report

Time shape:

- approximately `O(K_policy * (sum_f E_f log E_f + E))`, where `K_policy` is the candidate frontier size and is intentionally kept small

Space shape:

- `O(max_f E_f)` ordered event state reused across candidate evaluation plus `O(K_policy)` persisted profile rows

Current-head timing observation:

- elapsed `73.8 s` in the April 11 verification replay
- current run evaluated `5` candidates over `91` events and selected `max_ms=5000`, `event_threshold=15`

Key scaling note:

- the current candidate frontier is small by design, so this stage is not asymptotically dangerous; its current runtime comes from replaying the event stream several times plus coverage/reporting work

### 30 Windows Adaptive

Primary code:

- [pipelines/30_windows_adaptive.py](../../pipelines/30_windows_adaptive.py)
- [libs/windows/pipeline.py](../../libs/windows/pipeline.py)

Dominant work:

- per-flight ordered segmentation over `event_seq_id`
- stateful open-window / close-window fold
- event-to-window assignment join
- window-local event-count and snapshot aggregation

Cost shape:

- ordering and segmented fold: roughly `O(sum_f E_f log E_f)`
- assignment join and aggregations: roughly linear in `E` plus a range-join sensitivity to `W_f`
- output size: `O(W)`

Current observation:

- `92,875` events became `4,644` windows
- elapsed `163.1 s`
- `4,643` windows closed on `event_threshold`, so `W` is almost exactly `E / 20`

Key scaling note:

- with the current settings, window count is effectively event-driven, not duration-driven
- because `sensor_count` is capped near `20`, later same-window pair expansions stay bounded

### 40 Backbone Fit

Primary code:

- [pipelines/40_backbone_fit.py](../../pipelines/40_backbone_fit.py)
- [libs/windows/features.py](../../libs/windows/features.py)
- [libs/backbone/pipeline.py](../../libs/backbone/pipeline.py)
- [libs/backbone/fit.py](../../libs/backbone/fit.py)

Dominant work:

- build `window_features`
- explode and aggregate sparse continuous window vectors
- build global `G` and `H`
- solve a small ridge system on the driver

Cost shape:

- `window_features` assembly: dominated by raw-interval and event-in-window joins; practical cost scales with `W`, raw coverage, and event coverage
- energy aggregation: roughly `O(W * d_w)` where `d_w` is active continuous sensors per window
- `G`: roughly `O(W * C^2)`
- `H`: roughly `O(W * C * d_w)`
- local solve: roughly `O(C^3)`

Current observation:

- `W = 4,644`
- mean continuous-vector size `d_w = 36.0`, max `64`
- `C = 8`, `P_n = 80`
- elapsed `34.2 s`

Driver-side bound:

- the local solve and local `H` collection are explicitly bounded by `backbone.max_sensor_universe`
- current run is far below that bound: only `80` numeric sensors are present

### 50 Build Graph

Primary code:

- [pipelines/50_build_graph.py](../../pipelines/50_build_graph.py)
- [libs/graph/pipeline.py](../../libs/graph/pipeline.py)
- [libs/graph/precision.py](../../libs/graph/precision.py)

Dominant work:

- same-window event cooccurrence graph
- lag graph over a `30 s` horizon
- immediate transition graph
- precision graph over selected backbone sensors only
- graph fusion and parameter-universe materialization

Cost shape:

- precision graph: roughly `O(W * C^2 + C^3)`
- event graph: roughly `O(sum_w K_w^2)`
- lag graph: roughly `O(sum_b n_b * (n_b + n_{b-1}))` before dedupe and aggregation, where `n_b` is event volume in one lag bucket
- transition graph: roughly `O(E)`
- fusion and universe build: near-linear in component edge counts

Current observation:

- elapsed `381.6 s`, the largest stage in the pipeline
- output edges: `0` precision, `494` event, `371` lag, `253` transition, `641` fused
- event-graph same-window pair candidates from current window sensor counts: about `843,431`
- lag-graph rough same-bucket-or-previous-bucket pair candidates: about `234,274,963`
- lag-graph buckets: `56`
- average events per `30 s` bucket: `1,658.5`

Key scaling note:

- precision work is intentionally capped by `C = 8`; at most `28` undirected candidate precision edges even before thresholding
- the lag graph is the real quadratic risk in the current codebase, and the stage timings already show that

### 60 Fit Hierarchy

Primary code:

- [pipelines/60_fit_hierarchy.py](../../pipelines/60_fit_hierarchy.py)
- [libs/graph/hierarchy_artifacts.py](../../libs/graph/hierarchy_artifacts.py)

Dominant work:

- Spark-side mutual-top-k pruning on fused edges
- bounded driver-side collection of retained edges and rollup edges
- connected-components rollups from module to subsystem to system

Cost shape:

- distributed pruning: roughly linear in `H`, with ranking cost per local neighbor set
- driver-side rollup: roughly `O(P + retained_edges)`

Current observation:

- `P = 96`
- fused edges `H = 641`
- elapsed `3.3 s`
- output hierarchy rows: `96`

Driver-side bound:

- retained edge rollups are hard-bounded by `S3NTINEL_MAX_HIERARCHY_ROLLUP_EDGE_UNIVERSE`
- current run is far below the default `250,000` cap

### 70 Phase Fit

Primary code:

- [pipelines/70_phase_fit.py](../../pipelines/70_phase_fit.py)
- [libs/phase/pipeline.py](../../libs/phase/pipeline.py)
- [libs/phase/frames.py](../../libs/phase/frames.py)
- [libs/phase/fit.py](../../libs/phase/fit.py)
- [libs/phase/decode.py](../../libs/phase/decode.py)

Dominant work:

- small top-k feature selection from backbone sensors, event types, and categorical states
- per-flight robust scaling over `s_w`
- centroid seeding and refinement
- segmented phase decoding with transition penalty and dwell enforcement
- per-tail phase baseline aggregation

Cost shape:

- feature-frame assembly: roughly `O(W * P_n)` for full residual reconstruction plus `O(W * F_phi)` for selected phase features
- per-flight stats and refinement: roughly `O(I * W * F_phi * phase_count)`
- segmented decoding: roughly `O(W * phase_count)`
- baseline emission: roughly linear in `W`, output size `O(tails * phase_count)`

Current observation:

- `W = 4,644`
- `F_phi = 24`
- detected phase counts: `199`, `552`, `3,893`
- phase baseline rows: `3`
- elapsed `28.0 s`

Key scaling note:

- most of the cost is still tied to window count
- long-flight skew matters here because the dominant partitions are `(tail_id, flight_id)`
- flights-per-tail skew matters later for baseline support because baselines are aggregated per tail, not per flight

### 72 Phase Label Centroids

Primary code:

- [pipelines/72_phase_label_centroids.py](../../pipelines/72_phase_label_centroids.py)
- [libs/phase/tables.py](../../libs/phase/tables.py)
- [libs/phase/validator.py](../../libs/phase/validator.py)

Dominant work:

- build truth-labeled validation centroids from `phase_windows`
- compare detected centroids against truth-labeled centroids

Time shape:

- approximately linear in `W * F_phi` plus bounded centroid-comparison work

Space shape:

- `O(W * F_phi)` for the truth-centroid build plus bounded local pandas materialization for the comparison summary

Current-head timing observation:

- elapsed `5.1 s` in the April 11 verification replay

Key scaling note:

- this stage is validation-only and intentionally bounded, but it is still part of the end-to-end runtime because it materializes local comparison views

### 80 Window Scores Raw

Primary code:

- [pipelines/80_window_scores_raw.py](../../pipelines/80_window_scores_raw.py)
- [libs/scoring/tables.py](../../libs/scoring/tables.py)

Dominant work:

- broadcast join phase windows against small baseline and hierarchy reference tables
- compute structure and reconstruction scores
- explode dense per-window backbone residual maps to produce subsystem scores

Cost shape:

- structure scoring: roughly `O(W * F_phi)`
- residual explode and aggregation: roughly `O(W * P_n)` because phase windows carry a dense residual map over all numeric sensors
- output size: `O(W)`

Current observation:

- phase baselines: `3` rows
- hierarchy map: `96` rows
- residual map size: fixed `80` per window
- elapsed `5.4 s`

Driver-side bound:

- this stage explicitly fails if the broadcast-style bridge tables exceed `10,000` rows

### 85 Window Scores Calibrate

Primary code:

- [pipelines/85_window_scores_calibrate.py](../../pipelines/85_window_scores_calibrate.py)
- [libs/scoring/tables.py](../../libs/scoring/tables.py)

Dominant work:

- per `(tail_id, flight_id, phase_id_detected)` window functions for counts, ordering, and empirical tails

Cost shape:

- roughly linear after partition ordering, or `O(W log W_phase)` if the sort cost is made explicit

Current observation:

- `min_warm = 1`
- `76` of `307` windows became `emit_ready` in the April 11 verification replay
- elapsed `1.0 s`

Key scaling note:

- this stage is cheap now
- the more important downstream effect is how aggressively it controls `A`, because that directly changes stage `90`

### 90 Anomaly Attribution

Primary code:

- [pipelines/90_anomaly_attribution.py](../../pipelines/90_anomaly_attribution.py)
- [libs/anomaly/pipeline.py](../../libs/anomaly/pipeline.py)
- [libs/anomaly/frames.py](../../libs/anomaly/frames.py)
- [libs/anomaly/tables.py](../../libs/anomaly/tables.py)

Dominant work:

- filter to emit-ready windows
- join those windows back to raw telemetry intervals
- join those windows back to event intervals
- aggregate subsystem context and panel context
- write three attribution tables

Cost shape:

- roughly `O(A * raw_rows_in_window + A * event_rows_in_window)` plus context aggregations
- if windows overlap and most windows emit, output amplification can exceed input row counts

Current observation:

- `A = 76` in the April 11 verification replay
- anomaly telemetry attribution rows: `25,585`
- anomaly event attribution rows: `180`
- anomaly window attribution rows: `76`
- elapsed `54.1 s`

Amplification note:

- telemetry attribution output is about `21.8%` of the raw input row count in the current-head replay
- event attribution output is about `1.98x` the event input row count in the current-head replay
- the event side still amplifies because overlapping anomaly windows re-materialize shared event evidence even after emission became selective

### 95 Emit Explorer Bundle

Primary code:

- [pipelines/95_emit_explorer_bundle.py](../../pipelines/95_emit_explorer_bundle.py)

Dominant work:

- thin export of raw telemetry, hierarchy, events, phase intervals, and anomaly outputs into explorer-facing tables

Cost shape:

- approximately linear in the already-materialized raw, event, and anomaly artifact sizes

Current observation:

- the checked-in March 19, 2026 run failed in this stage due an ambiguous `plot_median` reference
- it is not part of the current modeling-path hotspot ranking

## Skew And Scale Implications

### Sampling-Rate Skew

This is already the main skew in the checked-in workload.

- `36` parameters at `2 Hz` produce `60%` of raw rows
- stages `10`, `20`, the raw side of `40`, and the raw join side of `90` all feel this directly
- rate skew also increases lag-graph candidate volume because more raw rows usually mean more numeric events

### Flight-Length Skew

The current bundle has only one flight, so there is no empirical within-run length skew.

Even so, most ordered kernels partition by one of:

- `(tail_id, flight_id, parameter_name)`
- `(tail_id, flight_id)`

That means a single very long flight can dominate:

- profiling order windows
- event detection order windows
- window segmentation
- phase fit and decode

Flight length is therefore a more important compute driver than flight count by itself.

### Flights Per Tail Skew

The checked-in run has exactly one flight for one tail.

In larger runs:

- most heavy stages still scale primarily per flight
- phase baselines are emitted per tail and phase, so baseline row count is still small, but the aggregation work behind those rows grows with all windows accumulated for that tail
- severe flights-per-tail skew therefore shows up more in tail-level baseline support than in raw stage output row counts

### Hierarchy Shape

The current simulated hierarchy is balanced and shallow:

- `2` parameters per module
- `4` per subsystem
- `32` per system

That reduces hub formation and keeps hierarchy rollup cheap.

Real fleets can be harder if:

- one subsystem owns many more parameters than another
- event-bearing parameters concentrate in a few hubs
- fused edge density stays high after pruning

In the current code, hierarchy runtime is driven more by retained fused-edge density than by nominal hierarchy depth.

### Parameter Behaviors

Behavior family currently matters indirectly.

- inertial and regulated numeric parameters generate almost all event volume
- discrete-state parameters are few and sparse in the event stream
- most stages do not yet choose different algorithms by behavior family

So today:

- behavior mix changes runtime mainly by changing `E`, `K_w`, and lag-bucket density
- it does not yet create large independent execution branches

### Phase Support Skew

Detected phase counts are heavily imbalanced in the current run:

- one phase owns `83.8%` of windows
- one phase baseline is supported by only `2` stable windows

This does not dominate runtime, but it matters for:

- phase-baseline stability
- calibration warm-up behavior
- tail-level score interpretation

### Windowing Policy

Current settings keep same-window combinatorics under control:

- `event_threshold = 20`
- `max_ms = 10,000`
- actual windows are usually `500 ms` or shorter

That implies:

- same-window graph work is bounded because `K_w` stays near `20`
- raising `event_threshold` or allowing much longer windows increases same-window pair costs quadratically
- lowering calibration strictness can increase attribution cost sharply because more windows become emit-ready

## Practical Conclusions

For the current codebase:

- the primary asymptotic time-and-space risk is still pair expansion in stage `50`, especially the lag-profile builder when candidate pruning weakens
- the current end-to-end wall-clock hotspots are broader: stages `20`, `25`, `40`, `50`, `70`, and `90` all matter now
- stage `50` needs to be split mentally into two problems:
  - graph construction complexity, which is still dominated by pair expansion
  - graph evaluation/reporting complexity, which is currently the largest measured sub-step inside the stage
- stage `90` is no longer dominated by universal emission, but it still creates large attribution materializations relative to `A`
- sampling-rate skew already dominates raw-row cost, while flight-length skew is the most likely future partition-level hotspot once multi-flight fleet runs become common

If the workload grows materially, the first places to rework are:

- lag graph candidate generation in [libs/graph/pipeline.py](../../libs/graph/pipeline.py)
- event-to-window and raw-to-window interval joins in [libs/windows/pipeline.py](../../libs/windows/pipeline.py), [libs/windows/features.py](../../libs/windows/features.py), [libs/anomaly/frames.py](../../libs/anomaly/frames.py), and [libs/anomaly/tables.py](../../libs/anomaly/tables.py)
- graph evaluation/reporting if end-to-end runtime matters as much as model-stage asymptotics
- any stage that currently relies on bounded driver-side collection, especially backbone solve, graph parameter universe collection, hierarchy rollup, phase-label centroid comparison, and raw-score bridge tables
