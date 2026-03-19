# Computational Complexity Report

This note grounds the active V2 pipeline complexity in the current code and in one concrete checked-in workload.

Use it for:
- stage-by-stage cost reasoning
- current hotspot identification
- scale-driver discussion for parameters, rates, windows, phases, graph density, and anomaly fan-out

For current stage ownership and entrypoints, see [pipelines/README.md](/home/jrs/code/S3NTINEL/sentinel/pipelines/README.md).
For current implementation surfaces, the main builder modules are:
- [libs/profiling/profiles.py](/home/jrs/code/S3NTINEL/sentinel/libs/profiling/profiles.py)
- [libs/events/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/events/pipeline.py)
- [libs/events/continuous.py](/home/jrs/code/S3NTINEL/sentinel/libs/events/continuous.py)
- [libs/windows/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/pipeline.py)
- [libs/windows/features.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/features.py)
- [libs/backbone/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/pipeline.py)
- [libs/graph/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
- [libs/graph/hierarchy_artifacts.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/hierarchy_artifacts.py)
- [libs/phase/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/pipeline.py)
- [libs/phase/fit.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/fit.py)
- [libs/phase/decode.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/decode.py)
- [libs/scoring/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/scoring/pipeline.py)
- [libs/conformal/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/conformal/pipeline.py)
- [libs/anomaly/artifacts.py](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/artifacts.py)

## Scope And Basis

This report uses the checked-in bundle:

- `data/simulation_runs/20260319T191636Z_power_pressurization_hierarchy_composite`

It is grounded in:

- the persisted stage manifests and stage summaries under that run directory
- the persisted parquet artifacts under that run directory
- the current code in `pipelines/` and `libs/*`

The successful timing snapshot covers stages `00` through `90`.

Stage `95_emit_explorer_bundle` is described qualitatively only. In the checked-in March 19, 2026 run it failed with an ambiguous `plot_median` reference, so its elapsed time is not a reliable modeling-stage benchmark.

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
| `A` | emit-ready anomaly windows |
| `W_phase` | windows in one calibration partition `(tail, flight, phase)` |
| `n_b` | event volume in one lag bucket |
| `I` | phase centroid-refinement iterations |
| `J_{R,W}` | raw-to-window interval join work |
| `J_{E,W}` | event-to-window interval join work |
| `J_{A,R}` | emit-ready-window-to-raw attribution join work |
| `J_{A,E}` | emit-ready-window-to-event attribution join work |

Most current stage costs are dominated by one of three shapes:

- long ordered per-parameter or per-flight scans
- pair expansion inside windows or lag buckets
- rejoining emit-ready windows back to raw and event evidence

## First-Principles Complexity Model

Before looking at any logged run, the implemented codepaths already force a small set of complexity classes.

### Core Execution Primitives

The active pipeline is built from five recurring primitives:

- partition-local sorts created by `Window.partitionBy(...).orderBy(...)`
- segmented stateful folds built through [libs/spark_sequence/plan.py](/home/jrs/code/S3NTINEL/sentinel/libs/spark_sequence/plan.py)
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

| Stage | Implemented dominant primitives | First-principles envelope |
| --- | --- | --- |
| `00` ingest | projection + persisted write | `O(R)` |
| `10` profiles | grouped aggs, percentiles, per-parameter ordered lags | `O(R) + O(sum_p R_p log R_p)` |
| `20` events | per-parameter ordered windows, segmented stateful folds | `O(sum_p R_p log R_p) + O(R)` |
| `30` windows | per-flight event ordering, segmented fold, event-to-window assignment | `O(sum_f E_f log E_f) + O(E) + O(J_{E,W})` |
| `40` window features + backbone | raw interval build, raw/event interval joins, sparse-map agg, small ridge solve | `O(J_{R,W} + J_{E,W} + W * d_w + W * C^2 + W * C * d_w + C^3)` |
| `50` graphs | same-window pair expansion, lag-bucket pair expansion, transition pass, small precision solve | `O(sum_w K_w^2) + O(sum_b n_b (n_b + n_{b-1})) + O(E) + O(W * C^2 + C^3)` |
| `60` hierarchy | Spark neighbor ranking + bounded driver rollup | `O(H log k) + O(P + retained_edges)` |
| `70` phase | dense residual reconstruction, per-flight scaling, centroid refinement, segmented decode | `O(W * P_n) + O(W * F_phi) + O(I * W * F_phi * phase_count)` |
| `80` raw scores | small broadcast joins, dense residual explode, subsystem regroup | `O(W * F_phi) + O(W * P_n)` |
| `85` calibration | per-phase partition ordering + window functions | `O(W log W_phase)` |
| `90` attribution | emit-ready filter, raw/event interval joins, context regroup | `O(J_{A,R} + J_{A,E})`, with practical amplification from overlapping windows |
| `95` explorer bundle | linear export over already-built artifacts | `O(R + E + anomaly_telemetry_rows + anomaly_event_rows + anomaly_window_rows)` |

The important first-principles conclusions are:

- stages `10`, `20`, `30`, and `70` are sort-dominated
- stages `50` and `90` are expansion-dominated
- stages `40`, `70`, and `80` hide dense `P_n` work behind otherwise sparse artifacts
- stages `40`, `50`, `60`, and `80` still rely on bounded driver-side collections for at least one subproblem

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
- lag profile: candidate pairing across same and adjacent lag buckets, `O(n_b (n_b + n_{b-1}))` per bucket

The lag-profile path is fundamentally more dangerous because `n_b` is driven by event density in wall-clock time, not by the window policy. The current code collapses `lag_profile` into a legacy `lag_graph` afterward, but that collapse is not the asymptotic hotspot.

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

Baseline stage timings from the March 19, 2026 composite run (`20260319T191636Z_power_pressurization_hierarchy_composite`):

| Stage | Elapsed | Share of full run |
| --- | --- | --- |
| `50_build_graph` | `381.6 s` | `48.2%` |
| `30_windows_adaptive` | `163.1 s` | `20.6%` |
| `90_anomaly_attribution` | `85.6 s` | `10.8%` |
| `20_events_extract` | `62.5 s` | `7.9%` |
| `40_backbone_fit` | `34.2 s` | `4.3%` |
| `70_phase_fit` | `28.0 s` | `3.5%` |
| all other successful stages combined | `36.5 s` | `4.7%` |

Inside `50_build_graph`, the measured baseline's dominant sub-step was `lag_graph_build`:

- `lag_graph_build`: `333,980.8 ms`
- `event_graph_build`: `40,429.6 ms`
- `transition_graph_build`: `782.3 ms`
- `precision_graph_build`: `294.6 ms`

This matches the baseline code path: lag construction was the only stage that could still create very large event-pair candidate sets inside a single flight even when the final edge table stayed small.

Updated measurement from the current multi-band implementation on the same workload family (`20260319T230003Z_power_pressurization_hierarchy_composite`):

- stage `50_build_graph`: `174,398.9 ms` vs `381,614.8 ms` before, about `54.3%` lower
- lag work:
  - old single-output `lag_graph_build`: `333,980.8 ms`
  - new multi-band lag path: `146,581.5 ms` for `lag_profile_build` plus `1,304.6 ms` for `lag_graph_build`
  - combined lag work: `147,886.1 ms`, about `55.7%` lower than the old lag builder
- `event_graph_build`: `17,548.2 ms` vs `40,429.6 ms`
- `transition_graph_build`: `403.7 ms` vs `782.3 ms`

The relevant config changed too:

- old run: single lag output, `lag_tau_max_seconds = 30.0`, no lag bands
- new run: `lag_tau_max_seconds = 120.0` with `quick`, `medium`, `slow`, and `very_slow` bands persisted in `lag_profile`

Important caveat:

- this is not artifact-identical
- old run outputs:
  - `event_edge_count = 494`
  - `lag_edge_count = 371`
  - `transition_edge_count = 253`
  - `fused_edge_count = 641`
- new run outputs:
  - `event_edge_count = 494`
  - `lag_profile_edge_count = 2249`
  - `lag_edge_count = 384`
  - `transition_edge_count = 253`
  - `fused_edge_count = 516`

So the measured wall-time improvement is real, but it comes with a semantic change: stage `50` now materializes a first-class per-band `lag_profile` and the legacy `lag_graph` is only the collapsed compatibility view.

## Empirical Stage-By-Stage Analysis

### 00 Ingest Raw

Primary code:

- [pipelines/00_ingest_raw.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/00_ingest_raw.py)

Cost shape:

- `O(R)` scan, normalize, and persisted write

Current observation:

- `201,600` rows written in `5.4 s`

This stage is linear and not a current hotspot.

### 10 Parameter Profiles Fit

Primary code:

- [pipelines/10_parameter_profiles_fit.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/10_parameter_profiles_fit.py)
- [libs/profiling/profiles.py](/home/jrs/code/S3NTINEL/sentinel/libs/profiling/profiles.py)

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

### 20 Events Extract

Primary code:

- [pipelines/20_events_extract.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/20_events_extract.py)
- [libs/events/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/events/pipeline.py)
- [libs/events/continuous.py](/home/jrs/code/S3NTINEL/sentinel/libs/events/continuous.py)
- [libs/events/categorical.py](/home/jrs/code/S3NTINEL/sentinel/libs/events/categorical.py)

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

### 30 Windows Adaptive

Primary code:

- [pipelines/30_windows_adaptive.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/30_windows_adaptive.py)
- [libs/windows/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/pipeline.py)

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

- [pipelines/40_backbone_fit.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/40_backbone_fit.py)
- [libs/windows/features.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/features.py)
- [libs/backbone/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/pipeline.py)
- [libs/backbone/fit.py](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/fit.py)

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

- [pipelines/50_build_graph.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/50_build_graph.py)
- [libs/graph/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
- [libs/graph/precision.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/precision.py)

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

- [pipelines/60_fit_hierarchy.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/60_fit_hierarchy.py)
- [libs/graph/hierarchy_artifacts.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/hierarchy_artifacts.py)

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

- [pipelines/70_phase_fit.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/70_phase_fit.py)
- [libs/phase/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/pipeline.py)
- [libs/phase/frames.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/frames.py)
- [libs/phase/fit.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/fit.py)
- [libs/phase/decode.py](/home/jrs/code/S3NTINEL/sentinel/libs/phase/decode.py)

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

### 80 Window Scores Raw

Primary code:

- [pipelines/80_window_scores_raw.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/80_window_scores_raw.py)
- [libs/scoring/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/scoring/pipeline.py)

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

- [pipelines/85_window_scores_calibrate.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/85_window_scores_calibrate.py)
- [libs/conformal/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/conformal/pipeline.py)

Dominant work:

- per `(tail_id, flight_id, phase_id_detected)` window functions for counts, ordering, and empirical tails

Cost shape:

- roughly linear after partition ordering, or `O(W log W_phase)` if the sort cost is made explicit

Current observation:

- `min_warm = 1`
- all `4,644` windows became `emit_ready`
- elapsed `0.69 s`

Key scaling note:

- this stage is cheap now
- the more important downstream effect is that `emit_ready = true` for all windows, which makes stage `90` expensive

### 90 Anomaly Attribution

Primary code:

- [pipelines/90_anomaly_attribution.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/90_anomaly_attribution.py)
- [libs/anomaly/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/pipeline.py)
- [libs/anomaly/artifacts.py](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/artifacts.py)
- [libs/anomaly/subsystem_context.py](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/subsystem_context.py)
- [libs/anomaly/panel_context.py](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/panel_context.py)

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

- `A = 4,644` and every window is emit-ready
- anomaly telemetry attribution rows: `509,616`
- anomaly event attribution rows: `235,682`
- anomaly window attribution rows: `4,644`
- elapsed `85.6 s`

Amplification note:

- telemetry attribution output is about `2.53x` the raw input row count
- event attribution output is about `2.54x` the event input row count
- this happens because overlapping anomaly windows re-materialize shared evidence

### 95 Emit Explorer Bundle

Primary code:

- [pipelines/95_emit_explorer_bundle.py](/home/jrs/code/S3NTINEL/sentinel/pipelines/95_emit_explorer_bundle.py)

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

For the current codebase and current checked-in workload:

- the primary scaling risk is the lag graph in stage `50`, not the precision graph or hierarchy rollup
- the next most important runtime drivers are event-volume-derived windowing in stage `30` and emit-ready fan-out in stage `90`
- sampling-rate skew already dominates raw-row cost, while flight-length skew is the most likely future partition-level hotspot once multi-flight fleet runs become common

If the workload grows materially, the first places to rework are:

- lag graph candidate generation in [libs/graph/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/graph/pipeline.py)
- event-to-window and raw-to-window interval joins in [libs/windows/pipeline.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/pipeline.py), [libs/windows/features.py](/home/jrs/code/S3NTINEL/sentinel/libs/windows/features.py), and [libs/anomaly/artifacts.py](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/artifacts.py)
- any stage that currently relies on bounded driver-side collection, especially backbone solve, graph parameter universe collection, hierarchy rollup, and raw-score bridge tables
