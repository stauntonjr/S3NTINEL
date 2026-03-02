# Structural, Phase‑Aware Anomaly Detection — Full Architecture & Workplan

**S3NTINEL:** Structural Streaming Sparse Event Nexus for Telemetry Inference with Network Envelope Learning


> **Final output:** asynchronous emission of anomaly objects as rows in Delta Lake tables.  
> **Scope:** motivation, design, full architecture, layer specs, code structure, modeling workplan (CUR fit; hierarchy discovery from event & precision graphs), and streaming plan.

## Operating Data Context (v1)

- Source environment: DoD CDAO Advana BLADE enclave (IL5).
- Source format: Parquet telemetry files.
- Fleet scope: AC/MC/HC-130J.
- Data scale: ~100 TB corpus; ~5–10 GB per flight hour.
- Sensor breadth: ~33,000 sensors from AFDX/A-MATS network tap.
- Sampling rates: mixed-rate telemetry; mostly high refresh; observed maxima up to ~200 Hz.
- v1 execution posture: historical batch first, optimized for throughput on Databricks job clusters.

---

## 1) Motivation & Design Tenets

**Why this design:** We want a single, time‑invariant structural backbone of the aircraft (global hierarchy), with local, phase‑aware anomaly scoring that runs cheaply in streaming or batch. The backbone is learned once via a **structural decomposition** (QR‑pivoting + CUR) on continuous sensors, while **events** from categorical & continuous channels provide high‑resolution behavioral signals. Anomalies are local deviations from the global structure, not re‑estimates of the structure itself. citeturn1search2

**Tenets**
- Linear, explainable, JVM‑friendly methods; index‑preserving selections (QR/CUR). citeturn1search2
- Time‑invariant **hierarchy** from stabilized covariance/precision, not window‑by‑window graphs. citeturn1search2
- **Event‑adaptive windows**: behavior‑aligned boundaries using event counts, not fixed slicing. citeturn1search2
- **Block‑wise metrics**: Euclidean for continuous structural blocks, discrete metrics (Hamming/Jaccard/Chi‑square/EMD) for dense event blocks. citeturn1search2
- **Phase detection** is implicit from stability plateaus in a structural signature space; **conformal calibration** is phase‑conditioned & streaming. citeturn1search2

---

## 2) End‑to‑End Architecture (Overview)

**Layers**
1. **Data Ingestion** → Delta tables; schema enforcement; timestamp normalization. citeturn1search2
2. **Global Continuous Backbone (CUR)** → select ~200–500 representative sensors; representative rows; compute U core; Louvain on stabilized graph for subsystem hierarchy; freeze backbone. citeturn1search2
3. **Event Extraction** → continuous extrema/threshold/slope; categorical transitions/illegal/dwell; subsystem co‑occurrences. citeturn1search2
4. **Adaptive Windowing** (event‑count or max‑duration) with per‑sensor **ZOH buffers**; assemble window features & event lists. citeturn1search2
5. **Structural Signature** per window (pivot drift, CUR errors, dense event & categorical blocks). citeturn1search2
6. **Phase Detection** (drift magnitude, direction, breadth, persistence accumulator; centroid maintenance). citeturn1search2
7. **Anomaly Scoring** (block‑wise metrics → subsystem fusion → global score). citeturn1search2
8. **Conformal Calibration** (phase‑conditioned buffers; p‑value with warm‑up; transitions/anomalies excluded). citeturn1search2
9. **Anomaly Report** (deterministic hierarchical JSON), emitted **asynchronously** to Delta. citeturn1search2
10. **CUR Maintenance** (column‑sketch Delta; occasional fleet backbone refresh; versioned). citeturn1search2

**Recommended windowing:** max 200 ms or event‑threshold ~20 (adaptive); this balances temporal resolution, stability, and cost and is future‑proof for near‑real‑time. citeturn1search2

---

## 3) Data Contracts & Delta Schemas

> Tables are append-first with schema evolution disabled outside controlled releases; **`anomalies` is MERGE-upsert keyed by `(tail_id, flight_id, win_id)`**. Partition by `(tail_id, flight_id, date_utc)` where applicable. citeturn1search2

### 3.1 Raw Telemetry (Delta)
```
raw_telemetry(
  tail_id STRING,
  flight_id STRING,
  ts TIMESTAMP,
  sensor STRING,
  val DOUBLE,         -- continuous
  state STRING,       -- categorical (nullable)
  unit STRING,
  rate_hz DOUBLE,
  meta MAP<STRING,STRING>
) PARTITIONED BY (tail_id, flight_id)
```

### 3.2 Event Stream (Delta)
```
events(
  tail_id STRING,
  flight_id STRING,
  ts TIMESTAMP,
  sensor STRING,
  subsystem STRING,
  event_type STRING,   -- extrema|threshold|slope_pos|slope_neg|switch|oscillation|drift_guard|state_enter|state_exit|transition|dropped|dwell_bucket|dwell_guard|dwell_violation|illegal_transition|cooccur
  payload MAP<STRING,STRING>
)
```
Events come from per‑sensor buffers: continuous extrema/threshold/slope/switch/oscillation plus categorical transitions/dwell/illegal logic; co‑occurrences can be emitted from event windows.

### 3.3 Adaptive Windows (Delta)
```
windows(
  tail_id STRING,
  flight_id STRING,
  win_id LONG,
  t_start TIMESTAMP,
  t_end TIMESTAMP,
  duration_ms INT,
  event_count INT,
  zoh_version INT,
  meta MAP<STRING,STRING>
)
```
Window closes on **max_duration** or **event_threshold**. ZOH buffers provide last‑seen values/states without upsampling.

### 3.4 Structural Signatures (Delta)
```
signatures(
  tail_id STRING,
  flight_id STRING,
  win_id LONG,
  phase_id INT,
  sig_version INT,
  pivot_block VECTOR<FLOAT>,         -- ranks & magnitudes (k~200–500)
  cur_block VECTOR<FLOAT>,           -- reconstruction & subsystem errors
  event_block SPARSE_VECTOR<FLOAT>,  -- dense event vocabulary (40k–80k effective dims)
  cat_block VECTOR<FLOAT>,           -- illegal/missing, dwell buckets
  breadth FLOAT,
  drift_mag FLOAT,
  drift_dir VECTOR<FLOAT>
)
```
Block structure mirrors metrics used by scoring; event block is sparse & compressed (structural compression). citeturn1search2

### 3.5 Phase Centroids & Calibration Buffers (Delta)
```
phases(
  tail_id STRING,
  phase_id INT,
  name STRING,
  centroid VECTOR<FLOAT>,
  var_envelope VECTOR<FLOAT>,
  version INT
)

calibration_buffers(
  tail_id STRING,
  phase_id INT,
  buf_version INT,
  scores ARRAY<FLOAT>,
  warm BOOLEAN,
  min_size INT
)
```
Centroids update only in stable windows; calibration buffers are **phase‑specific**, accumulate scores only in stable regions, anomalies/transitions excluded, with a warm‑up minimum before p‑values are emitted. citeturn1search2

### 3.6 CUR Backbone & Sketch Tables (Delta)
```
column_sketch(
  tail_id STRING,
  flight_id STRING,
  sketch_cov ARRAY<FLOAT>,  -- compact covariance summary of Y=XΩ
  sketch_norms ARRAY<FLOAT>,
  sample_count LONG,
  ts_range STRING,
  version INT
)

cur_backbone(
  cur_version INT,
  selected_sensors ARRAY<STRING>,   -- C
  selected_rows ARRAY<LONG>,        -- R indices
  U BINARY,                         -- serialized small core matrix
  weights MAP<STRING,ARRAY<FLOAT>>, -- reconstruction weights per sensor
  subsystems MAP<STRING,ARRAY<STRING>> -- Louvain clusters
)
```
Sketch is **append‑only**; CUR refresh aggregates sketches to update leverage scores and resamples columns/rows occasionally; backbone versions are persisted. citeturn1search2

### 3.7 Anomaly Objects (Delta, final async output)
```
anomalies(
  tail_id STRING,
  flight_id STRING,
  win_id LONG,
  ts TIMESTAMP,
  phase_state STRING,        -- stable|entering_phase|leaving_phase|transition_region
  phase_id INT,
  phase_confidence DOUBLE,
  distance_to_centroid DOUBLE,
  drift_magnitude DOUBLE,
  breadth DOUBLE,
  global_score DOUBLE,
  p_value DOUBLE,
  severity STRING,           -- normal|low|medium|high
  dominant_subsystem STRING,
  dominant_block STRING,
  subsystems ARRAY<STRUCT<
      id:STRING,
      name:STRING,
      score:DOUBLE,
      block_contrib:MAP<STRING,DOUBLE>,
      top_sensors:ARRAY<STRUCT<sensor_id:STRING, score:DOUBLE, pivot:DOUBLE, cur:DOUBLE, events:DOUBLE, categorical:DOUBLE, cooccurrence:DOUBLE>>
  >>,
  raw STRUCT<block_scores:MAP<STRING,DOUBLE>, sensor_scores:MAP<STRING,DOUBLE>>,
  versions STRUCT<backbone:int, signature:int, scoring:int, calibration:int>
)
```
Deterministic, hierarchical, includes phase context and block‑wise contributions; emitted **asynchronously** post‑scoring. citeturn1search2

---

## 4) Algorithms & Layer Specs

### 4.1 CUR Backbone (Layer 2)
**Goal:** Time‑invariant structural skeleton from continuous sensors (not events). **Input:** raw continuous matrix X (downsample 10 Hz). **Output:** (C, U, R), reconstruction weights, subsystem clusters. **Never refit per window.** citeturn1search2

**Fitting (out‑of‑core on Databricks)**
1) Column sketching: compute `Y = X Ω` in batches; aggregate sketch covariance; compute column leverage.  
2) Column sampling: choose ~200–500 sensors; extract only those from raw.  
3) Row sketching: leverage scores on reduced matrix; sample ~200–500 rows.  
4) Core: compute pseudoinverses and `U = C⁺ X R⁺` via streamed passes; store weights & clusters; **freeze** backbone. citeturn1search2

**Hierarchy discovery:** Build stabilized covariance/precision on the structural data (e.g., using CUR reconstruction / QR‑selected sensors) → Graphical Lasso → **Louvain** → subsystem clusters. Optionally fuse with event co‑occurrence graph. citeturn1search2

**Why 10 Hz downsample is OK:** CUR learns low‑rank continuous structure; high‑frequency spikes are handled as **events**; the backbone is rank‑limited and benefits from uniform sampling. citeturn1search2

### 4.2 Event Extraction (Layer 3)
Per‑sensor ZOH buffers detect and enqueue events without any upsampling: continuous extrema/threshold/slope, categorical transitions/illegal, dwell buckets, and cross‑subsystem co‑occurrences. citeturn1search2

### 4.3 Adaptive Windowing (Layer 4)
Windows close when **(duration ≥ 200 ms)** or **(events ≥ 20)**; optional min duration (e.g., 50 ms). Event bursts → short windows; quiet → long windows. Persistence is based on **drift evidence**, not counts: `P += d_i * b_i * Δt_i` (reset on drift‑direction reversal). citeturn1search2

### 4.4 Structural Signature (Layer 5)
Concatenate blocks: **pivot** (order/magnitude drift), **CUR** (reconstruction & subsystem error), **event** (counts/co‑occurrence, dense but sparse vector), **categorical logic** (illegal/missing transitions, dwell). Typical size 1–5 KB per window; event block 40k–80k effective dims, sparse & structurally compressed. citeturn1search2

### 4.5 Phase Detection (Layer 6)
Track distances between signature and existing **phase centroids**; compute drift magnitude, direction, breadth; update a persistence accumulator. Stable plateaus → phases; broad persistent drift → transitions; spikes → anomalies. Centroids maintained only in stable windows; hybrid thresholds (fixed minima + adaptive by within‑phase variance). citeturn1search2

### 4.6 Anomaly Scoring (Layer 7)
Use **block‑wise composite metrics**:
- Pivot/CUR: Euclidean (or Mahalanobis) on continuous blocks.  
- Events: Hamming (flags), Jaccard (sparse transitions), Chi‑square (frequencies/co‑occurrence), EMD (dwell histograms).  
Project anomaly vector onto **subsystems** (index sets from Louvain) to get `A_k(t)`; fuse globally `A(t)=Σ α_k A_k(t)`. Provide **per‑sensor contributions** from block decomposition (esp. CUR reconstruction error & event ownership). citeturn1search2

### 4.7 Conformal Calibration (Layer 8)
Maintain **phase‑specific calibration buffers**; update only in stable windows; ignore anomalies & transitions; require **warm‑up** (e.g., ≥100 scores) before p‑values. Reuse buffers upon phase return; compute `p(t) = |{A(τ) ≥ A(t)}| / |B_phase|`. Early‑phase miscalibration is avoided via warm‑start and buffer reuse. citeturn1search2

### 4.8 Anomaly Object (Layer 9)
Deterministic, hierarchical JSON with **phase context** (entering/leaving/transition), global score & p‑value, subsystem breakdown, block contributions, top sensors, raw blocks; emitted **asynchronously** to `anomalies` Delta. citeturn1search2

### 4.9 CUR Maintenance (Layer 10)
Append column sketches per flight; occasionally recompute leverage scores and refresh CUR backbone; publish new **versioned** backbone; downstream scoring references the latest version atomically. citeturn1search2

---

## 5) Code Structure (Repo Sketch)
```
repo/
  conf/
    defaults.yaml                 # thresholds, sizes, weights, versions
  pipelines/
    00_ingest_raw.py
    10_cur_backbone_fit.py        # sketch → leverage → sample → U; writes cur_backbone
    20_events_extract.py          # buffers, extremum, transitions, dwell, cooccurrence
    30_windows_adaptive.py        # event-count or duration; ZOH; writes windows
    40_signatures_build.py        # blocks; writes signatures
    50_phase_detect.py            # centroids, persistence; writes phases
    60_anomaly_score.py           # block-wise metrics → fusion; writes scores
    70_conformal_calibrate.py     # phase buffers; p-values; writes calibrated
    80_emit_anomalies.py          # async sink to anomalies Delta
  libs/
    cur/
      sketch.py, leverage.py, sample.py, core.py
    events/
      buffers.py, extrema.py, categorical.py, cooccur.py
    windows/
      adaptive.py, zoh.py
    signature/
      blocks.py
    phase/
      drift.py, persistence.py, centroids.py
    scoring/
      pivot.py, cur.py, events.py, fuse.py, subsystem.py
    conformal/
      buffers.py, pvalue.py
    io/
      delta.py, schemas.py
  notebooks/
    01_CUR_fit_demo.dbc
    02_Signature_and_Phase.dbc
    03_Anomaly_and_Conformal.dbc
```
Each pipeline step runs in **foreachBatch** (near‑real‑time) or as offline batch replay. All I/O through versioned Delta schemas defined in `io/schemas.py`. citeturn1search2

---

## 6) Pseudocode for Critical Components

### 6.1 Per‑sensor ZOH Buffers & Adaptive Windowing
```python
# on_sample(sensor j, value/state v, time t)
update_buffer(j, v, t)           # last_value, last_state, dwell, extrema, transitions
if event_emitted: event_buf.append(evt)

# window_tick(now)
if (now - win_start >= MAX_MS) or (len(event_buf) >= EVT_THRESH):
    features = zoh_read_all_buffers()
    sig = build_signature(features, event_buf)
    phase = update_phase(sig)
    score = anomaly_score(sig, phase)
    p = conformal_pvalue(phase, score)
    emit_anomaly_async(sig, phase, score, p)
    reset_window()
```
Event bursts shorten windows; quiet extends them; **no upsampling**—just ZOH lookup. citeturn1search2

### 6.2 Phase Detection (drift evidence, persistence)
```python
# inputs: s_t (signature), centroids {c_i}
short = norm(s_t - s_prev)
long  = norm(s_t - s_avg)
B     = breadth(s_t, s_prev)               # fraction of coords shifting > eps
vdir  = direction(s_t - s_prev)
if dot(vdir, vdir_prev) < 0: P = 0         # reset on reversal
P += long * B * dt                          # evidence accumulation
if near_any_centroid(s_t) and persist(k):   # return to phase
    phase = argmin_i(norm(s_t - c_i))
elif P > TAU_PERSIST and stabilized:
    phase = new_phase(); add_centroid(s_t)
else:
    phase = transition_or_anomaly(short, long, B)
```
Plateaus → phase; persistent, broad drift → transition; spikes → anomaly. citeturn1search2

### 6.3 Block‑wise Anomaly Scoring & Subsystem Projection
```python
A_pivot = euclid(sig.pivot_block, phase.centroid.pivot)
A_cur   = euclid(sig.cur_block,   phase.centroid.cur)
A_ev    = hamming(sig.event_flags, phase.ev_flags) + \
          jaccard(sig.event_set,   phase.ev_set)   + \
          chi2(sig.ev_freqs,       phase.ev_freqs) + \
          emd(sig.dwell_hist,      phase.dwell_hist)
# project by subsystem index sets I_k
A_k = {}
for k in subsystems:
    A_k[k] = w_p*A_pivot[I_k] + w_c*A_cur[I_k] + w_e*A_ev[I_k]
A_global = sum(alpha[k] * A_k[k] for k in subsystems)
```
Events use discrete metrics; continuous blocks use Euclidean; subsystem projection localizes anomalies. citeturn1search2

### 6.4 Conformal Calibration (phase‑conditioned)
```python
B = calib_buffer[phase]
if B.size >= MIN_WARM and phase.stable:
    p = (B >= A_global).mean()
else:
    p = None  # hold until warm
if phase.stable and not anomaly:
    B.append(A_global)
```
Buffers per phase; warm‑up; exclude transitions/anomalies; reuse on phase return. citeturn1search2

---

## 7) Modeling Workplan

### 7.1 CUR Fit (Fleet‑level, with Sketch Table)
1. **Assemble training set:** ~9 tails × 9 flights; continuous only; downsample 10 Hz.  
2. **Compute column sketch per flight:** Y = XΩ (batches); write to `column_sketch`.  
3. **Aggregate & sample columns:** leverage from sketches; select 200–500 sensors; extract C.  
4. **Row sketch & sampling:** leverage on C; select 200–500 rows; extract R.  
5. **Compute U & weights:** streamed `U = C⁺ X R⁺`; store weights; build subsystem graph via Louvain; **freeze v1**.  
6. **Validate:** reconstruction error distribution; subsystem interpretability; stability across tails. citeturn1search2

### 7.2 Hierarchy from Event & Precision Graphs (Fusion)
- **Continuous dependency graph:** Graphical Lasso on stabilized covariance (QR/CUR‑based).  
- **Event co‑occurrence graph:** nodes=sensors/states; edges from PMI/Jaccard/χ² on events; pool by subsystem pairs.  
- **Fusion & clustering:** weighted sum of normalized graphs; hierarchical Louvain/Leiden → subsystem tree. citeturn1search2

### 7.3 Streaming & Scoring
- Implement per‑sensor **ZOH buffers**; build event vocabulary + structural compression; **adaptive windows** (200 ms/20 events).  
- Build **structural signature** writers; phase state machine; block‑wise metrics; subsystem projection; **async** anomaly emission.  
- **Conformal buffers** per phase with warm‑up & reuse.  
- Backfill historical flights (offline), then enable near‑real‑time `foreachBatch`. citeturn1search2

---

## 8) Operational Guidance

**Window size:** 200 ms default; adaptive by event count (15–25); min 50 ms.  
**Performance:** single‑driver foreachBatch preferred for determinism; memory footprint <100 MB per tail; throughput 1–3 hrs/flight at large scale; scales linearly.  
**Maintenance:** CUR refresh on sketch‑drift; version all artifacts; avoid per‑window re‑fitting.  
**Partitioning:** anomalies, signatures partitioned by (tail_id, flight_id, date_utc).  
**Governance:** schema contracts & lineage from raw → windows → signatures → anomalies. citeturn1search2

---

## 9) Defaults & Tunables (v1)
- `K_pivots = 300` (200–500); `MAX_MS = 200`; `EVT_THRESH = 20`; `MIN_MS = 50`.  
- Phase thresholds: `tau_near`, `tau_far` hybrid (fixed minima + variance‑scaled).  
- Conformal: `MIN_WARM = 100`, buffers per phase; exclude transitions/anomalies from buffers.  
- Metric weights: per block and per subsystem; start uniform then tune on calibration flights. citeturn1search2

---

## 10) Appendix — Anomaly Object (Deterministic JSON)
A canonical example of the emitted record:
```json
{
  "timestamp": "2026-02-28T00:41:12.345Z",
  "phase_context": {
    "current_phase_id": 3,
    "current_phase_name": "Cruise",
    "phase_state": "entering_phase",
    "phase_state_confidence": 0.82,
    "distance_to_centroid": 0.047,
    "drift_magnitude": 0.012,
    "drift_direction": {"subsystem_weights": {"Hydraulics": 0.41, "Electrical": 0.22}},
    "breadth": 0.34,
    "persistence": 6,
    "is_stable": false,
    "expected_time_to_stability": 4
  },
  "global_anomaly": {
    "score": 0.873,
    "p_value": 0.012,
    "severity": "high",
    "phase_adjusted": true,
    "dominant_subsystem": "Hydraulics",
    "dominant_block": "event_transitions"
  },
  "subsystems": [
    {
      "id": "SYS_12",
      "name": "Hydraulics",
      "score": 0.742,
      "severity": "high",
      "block_contributions": {"pivot": 0.11, "cur_reconstruction": 0.22, "event_transitions": 0.31,
                               "categorical_logic": 0.08, "cooccurrence": 0.02},
      "top_sensors": [
        {"sensor_id": "HYD_PRESS_4", "score": 0.41, "pivot": 0.03, "cur": 0.22, "events": 0.14,
         "categorical": 0.02, "cooccurrence": 0.00},
        {"sensor_id": "HYD_TEMP_2",  "score": 0.19, "pivot": 0.01, "cur": 0.11, "events": 0.06,
         "categorical": 0.01, "cooccurrence": 0.00}
      ]
    }
  ],
  "raw": {
    "block_scores": {"pivot": 0.15, "cur_reconstruction": 0.31, "event_transitions": 0.41,
                      "categorical_logic": 0.12, "cooccurrence": 0.03},
    "sensor_scores": {"HYD_PRESS_4": 0.41, "HYD_TEMP_2": 0.19, "ELEC_VOLT_7": 0.07}
  },
  "versions": {"backbone": 1, "signature": 1, "scoring": 1, "calibration": 1}
}
```
Template is deterministic: same fields/ordering always; ready for async Delta sink. citeturn1search2

---

## 11) v1 Spec Lock (Resolved Decisions)

This section is normative for v1 and overrides prior ambiguity.

### 11.1 Canonical Input Contract
Primary v1 telemetry input is long form:
```
(tail_id STRING,
 flight_id STRING,
 timestamp TIMESTAMP,
 parameter_name STRING,
 parameter_value STRING)
```
`parameter_value` is parsed into continuous/categorical/ASCII features by parameter metadata and extraction rules. Continuous values are cast to `DOUBLE` where valid; categorical and panel text remain string features.

### 11.2 Partitioning & Time Semantics
- `date_utc` is always derived as `to_date(timestamp_utc)`.
- Delta partition convention for v1 analytic outputs (`events`, `windows`, `signatures`, `anomalies`) is `(tail_id, flight_id, date_utc)`.
- Historical replay preserves original event/window timestamps end-to-end.

### 11.3 Processing Mode & Emission Semantics
- v1 is historical/offline first, optimized for processing speed on a 50 DBU Databricks job cluster.
- Anomalies are buffered per phase until conformal warm-up is satisfied (`MIN_WARM`).
- Once warm, buffered anomalies are emitted as a backlog with original event/window timestamps.
- `anomalies` sink semantics: `MERGE` upsert keyed by `(tail_id, flight_id, win_id)`.

### 11.4 Phase, Calibration, and Backbone Scope
- Phase centroids and conformal calibration buffers are maintained **per tail**.
- CUR backbone is **fleet-level** in v1.
- CUR maintenance policy: monthly scheduled refresh + drift-triggered refresh from sketch divergence.
- No per-tail rolling CUR in v1 (deferred to later versions).

### 11.5 Event Vocabulary, Graph Fusion, and Hierarchy
- Event vocabulary is versioned per `cur_backbone.cur_version`.
- Event co-occurrence graph is recomputed in lockstep with backbone versioning.
- Continuous precision graph and event co-occurrence graph are fused before hierarchical Louvain/Leiden subsystem discovery.

### 11.6 Missing/Drop Handling
- Missing or dropped telemetry is represented explicitly as event states/transitions (e.g., `missing`, `dropped`) in the event stream.
- These states participate in categorical/event blocks and anomaly scoring.

### 11.7 Scoring Defaults (v1)
- All four event metrics are mandatory in v1: Hamming, Jaccard, Chi-square, and EMD.
- Block scores are computed per block, robust-normalized within phase (median/MAD), then fused.
- Initial fusion weights are uniform at block level, then modulated by subsystem criticality.
- Subsystem criticality comes from a versioned JSON artifact bootstrapped via NLP over constituent sensor names and then reviewed by SMEs.
- For p-value combination across blocks (when needed), use a dependence-robust method (Cauchy combination) rather than summing p-values.

### 11.8 Threshold Initialization (v1 Defaults)
Initial thresholds are data-adaptive and validated on calibration flights:
- `tau_near`: phase-wise 25th percentile of centroid distances.
- `tau_far`: phase-wise 90th percentile of centroid distances.
- Persistence trigger: phase-wise high quantile of persistence evidence (`P`), initialized at the 95th percentile.
- Reversal reset: reset persistence when drift-direction cosine falls below `-0.2`.

### 11.9 Anomaly Object Extension (LCD Context)
Add LCD/panel context to anomaly payload from ASCII-derived features:
```
panel_context STRUCT<
  text ARRAY<STRING>,
  message_codes ARRAY<STRING>,
  source ARRAY<STRING>
>
```
`panel_context` is optional/null when panel text is unavailable.

### 11.10 Determinism
Deterministic field ordering is required at the logical anomaly-object schema level; physical Delta storage ordering is not required.

## 12) Performance Test Protocol (v1, Historical Batch)

### 12.1 Objective
Validate throughput, cost, and stability for S3NTINEL on Advana BLADE (IL5) Parquet telemetry using a 50 DBU Databricks job cluster before production replay.

### 12.2 Fixed Test Conditions
- Input scope: AC/MC/HC-130J slices representative of low/median/high event rates.
- Data shape: mixed-rate telemetry up to ~200 Hz, ~33k sensors, including sparse categorical and ASCII/LCD features.
- Pipeline mode: full v1 flow (ingest -> events -> windows -> signatures -> phase -> scoring -> conformal -> anomalies).
- Identity semantics: `MERGE` on `(tail_id, flight_id, win_id)`.
- Warm-up policy: hold and backlog anomalies until phase buffer warm.

### 12.3 Run Matrix
Execute each profile at least 3 times and report median + p95.

| Profile | Flight Hours | Expected Input Volume | Purpose |
|---|---:|---:|---|
| P1 Small | 10 h | 50-100 GB | Pipeline sanity + baseline metrics |
| P2 Medium | 50 h | 250-500 GB | Throughput scaling + shuffle behavior |
| P3 Large | 200 h | 1-2 TB | Long-run stability + cost envelope |
| P4 Stress | 500 h | 2.5-5 TB | Spill resilience + SLA boundary |

### 12.4 Required Telemetry (per run)
- Stage durations: p50/p95 for ingest, event extraction, signature build, scoring, calibration, emit.
- Data movement: shuffle read/write bytes, spill bytes, skew indicators.
- Compute/cost: DBU consumed, cost per flight-hour, executor utilization.
- Output quality guards: row counts by table, duplicate `win_id` rate post-merge, null/invalid schema fields.
- Functional guards: warm-up backlog flush count, p-value population rate after warm-up.

### 12.5 Pass/Fail Gates (initial)
- Correctness gates (must pass all):
  - No schema violations in output Delta tables.
  - No duplicate `(tail_id, flight_id, win_id)` in final anomalies.
  - Backlog emission preserves original `ts`/window timestamps.
- Performance gates (target):
  - End-to-end runtime scales sublinearly with input growth from P1 to P3.
  - No sustained spill-dominated stages (>20% of stage time) at P3.
  - p95 stage duration variance across repeats <= 20% for P2/P3.
- Cost gates (target):
  - Stable cost per flight-hour across P2/P3 within +/-15%.

### 12.6 Tuning Order (when gates fail)
1. Repartition by `(tail_id, flight_id, date_utc)` and rebalance skewed keys.
2. Increase target file size toward 256-512 MB and reduce small-file amplification.
3. Raise shuffle partitions proportionally to input bytes; verify spill reduction.
4. Isolate hot sensor families in event extraction micro-stages.
5. Re-run failing profile with identical inputs; compare p95 and cost deltas.

### 12.7 Exit Criteria
v1 is performance-qualified when P1-P3 pass all correctness gates and meet performance/cost targets on 3 repeated runs, with P4 producing no correctness regressions.

---

**References:** All sections reflect the uploaded design discussion (CUR/QR skeleton; adaptive windows; implicit phases; block‑wise metrics; phase‑conditioned conformal; anomaly object schema; Databricks execution; CUR sketch maintenance). citeturn1search2
