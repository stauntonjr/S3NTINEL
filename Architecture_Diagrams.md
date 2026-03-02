# Structural Anomaly System — Architecture Diagrams (v1)

**S3NTINEL:** Structural Streaming Sparse Event Nexus for Telemetry Inference with Network Envelope Learning

**Naming Convention (Canonical):** First `N` = **Nexus**; second `N` = **Network**.

These diagrams reflect the locked v1 spec in `Structural_Anomaly_System_Architecture.md`.

## 0) Operating Context

- Platform: DoD CDAO Advana BLADE enclave (IL5).
- Input format: Parquet telemetry.
- Fleet: AC/MC/HC-130J.
- Scale: ~100 TB total, ~5–10 GB per flight hour.
- Sensor/rate profile: ~33k sensors, mixed rates up to ~200 Hz.

## 1) End-to-End Layered Pipeline

```mermaid
flowchart TD
  A[Raw Telemetry Ingest Parquet Advana BLADE IL5 33k sensors mixed rates up to 200 Hz]
  B[Normalization and Type Parsing continuous categorical ASCII]
  C[Delta raw_telemetry]

  D[Global CUR Backbone Fit sketch leverage sample C and R build U]
  E[Graph Fusion and Hierarchy precision and event graphs to hierarchy]
  F[Delta cur_backbone and column_sketch]

  G[Event Extraction extrema threshold slope transition illegal dwell missing dropped cooccurrence]
  H[Adaptive Windowing close at MAX_MS 200 or EVT_THRESH 20 min 50ms ZOH buffers]
  I[Signature Build pivot CUR event categorical blocks]

  J[Phase Detection per tail centroids and persistence evidence]
  K[Anomaly Scoring block metrics to subsystem projection to global]
  L[Conformal Calibration per tail per phase buffer warmup MIN_WARM 100]

  M{Warm}
  N[Backlog Buffer keep original window timestamps]
  O[Async Anomaly Emit MERGE key tail_id flight_id win_id]

  P[Delta events]
  Q[Delta windows]
  R[Delta signatures]
  S[Delta phases and calibration_buffers]
  T[Delta anomalies]

  A --> B --> C
  C --> D --> E --> F
  C --> G --> P
  P --> H --> Q
  Q --> I --> R
  R --> J --> S
  J --> K --> L --> M
  M -- no --> N --> M
  M -- yes --> O --> T

  F --> I
  F --> K
```

## 2) Runtime Sequence (Per Window)

```mermaid
sequenceDiagram
  participant In as Input Stream
  participant Buf as ZOH/Event Buffers
  participant Win as Adaptive Window
  participant Sig as Signature Builder
  participant Ph as Phase Engine
  participant Sc as Scoring Engine
  participant Cf as Conformal Engine
  participant Out as Anomaly Sink (Delta)

  In->>Buf: sample(tail_id, flight_id, ts, parameter, value)
  Buf->>Buf: update ZOH + detect events
  Buf->>Win: append events

  alt duration>=200ms OR events>=20
    Win->>Sig: finalize window + features
    Sig->>Ph: signature blocks
    Ph->>Sc: phase_id + phase_state + centroid distance
    Sc->>Cf: global_score + subsystem/block contributions

    alt phase buffer warm (>=100 stable scores)
      Cf-->>Out: emit anomaly object (original win ts)
    else not warm
      Cf->>Cf: hold in backlog for phase
      Cf-->>Out: no emission yet
    end
  else keep collecting
    Win->>Buf: continue accumulation
  end
```

## 3) Phase State Machine

```mermaid
stateDiagram-v2
  [*] --> Stable

  Stable --> EnteringPhase: broad persistent drift
  EnteringPhase --> Stable: near centroid + persistence met

  Stable --> TransitionRegion: far drift + high breadth
  TransitionRegion --> Stable: settles near existing/new centroid

  Stable --> LocalAnomaly: short spike / non-persistent deviation
  LocalAnomaly --> Stable: drift decays quickly

  EnteringPhase --> TransitionRegion: persistence continues but not yet stable
  TransitionRegion --> LocalAnomaly: abrupt reversal/spike

  note right of Stable
    Update centroid + calibration only in stable windows
  end note

  note right of TransitionRegion
    Exclude from conformal buffer updates
  end note
```

## 4) Delta Contracts & Relationships (ER)

```mermaid
erDiagram
  RAW_TELEMETRY {
    string tail_id
    string flight_id
    timestamp timestamp_utc
    string parameter_name
    string parameter_value
    date date_utc
  }

  EVENTS {
    string tail_id
    string flight_id
    long win_id
    timestamp ts
    string sensor
    string subsystem
    string event_type
  }

  WINDOWS {
    string tail_id
    string flight_id
    long win_id
    timestamp t_start
    timestamp t_end
    int event_count
    int duration_ms
    date date_utc
  }

  SIGNATURES {
    string tail_id
    string flight_id
    long win_id
    int phase_id
    int sig_version
    vector pivot_block
    vector cur_block
    sparse_vector event_block
    vector cat_block
  }

  PHASES {
    string tail_id
    int phase_id
    vector centroid
    vector var_envelope
    int version
  }

  CALIBRATION_BUFFERS {
    string tail_id
    int phase_id
    int buf_version
    array_float scores
    bool warm
    int min_size
  }

  CUR_BACKBONE {
    int cur_version
    array_string selected_sensors
    array_long selected_rows
    binary U
    map weights
    map subsystems
  }

  COLUMN_SKETCH {
    string tail_id
    string flight_id
    array_float sketch_cov
    array_float sketch_norms
    long sample_count
    int version
  }

  ANOMALIES {
    string tail_id
    string flight_id
    long win_id
    timestamp ts
    int phase_id
    string phase_state
    double global_score
    double p_value
    string severity
    struct panel_context
  }

  RAW_TELEMETRY ||--o{ EVENTS : generates
  EVENTS ||--|| WINDOWS : closes_window
  WINDOWS ||--|| SIGNATURES : produces
  SIGNATURES ||--o{ ANOMALIES : scored_as

  PHASES ||--o{ SIGNATURES : labels
  PHASES ||--o{ CALIBRATION_BUFFERS : conditions
  CALIBRATION_BUFFERS ||--o{ ANOMALIES : calibrates

  COLUMN_SKETCH }o--|| CUR_BACKBONE : refreshes
  CUR_BACKBONE ||--o{ SIGNATURES : backbone_version
  CUR_BACKBONE ||--o{ ANOMALIES : versions_backbone
```

## 5) CUR + Graph Fusion + Hierarchy Discovery

```mermaid
graph TD
  A[Continuous Training Matrix X at 10Hz downsample] --> B[Column Sketching Y equals X times Omega aggregate sketch covariance]
  B --> C[Leverage Scores Columns]
  C --> D[Sample Representative Sensors C K pivots 200 to 500]

  D --> E[Row Sketching and Leverage]
  E --> F[Sample Representative Rows R 200 to 500]
  D --> G[Compute Core U equals C_plus X R_plus]
  F --> G

  G --> H[CUR Backbone Artifact selected_sensors selected_rows U weights]

  H --> I[Structural Dependency Graph covariance precision Graphical Lasso]
  J[Event Cooccurrence Graph PMI Jaccard Chi2 from events] --> K[Graph Normalization]
  I --> K
  K --> L[Weighted Graph Fusion]
  L --> M[Hierarchical Louvain Leiden]
  M --> N[Subsystem Tree and Sensor Index Sets]

  N --> O[Used by scoring projection Ak_t]
  H --> O
```

## 6) System Context (Operational Boundaries)

```mermaid
flowchart LR
  subgraph Aircraft[Aircraft / Tail]
    A[Onboard sensors + buses]
    B[LCD/Panel text source]
  end

  subgraph Ops[Operator Data Platform]
    C[Telemetry landing zone]
    D[Delta Lake bronze/silver]
  end

  subgraph Sentinel[S3NTINEL Analytics]
    E[CUR backbone + hierarchy]
    F[Event + window + signature pipeline]
    G[Phase + scoring + conformal]
    H[Anomaly object builder]
  end

  subgraph Consumers[Downstream Consumers]
    I[Maintenance triage]
    J[Flight ops / reliability]
    K[Model governance + audit]
  end

  A --> C --> D --> F
  B --> C
  D --> E
  E --> G
  F --> G --> H
  H --> I
  H --> J
  H --> K
```

## 7) Throughput & Capacity Planning (50 DBU Batch)

```mermaid
flowchart TD
  A[Input Scale 5 to 10 GB per flight hour up to 200Hz channels about 33k sensors] --> B[Pre shard by tail_id flight_id date_utc]

  B --> C[Bronze to Silver Normalize schema and type parsing mixed rate alignment via ZOH]

  C --> D[Event Extraction Stage CPU bound per sensor family]
  C --> E[CUR Signature Stage vector ops and sparse blocks]

  D --> F[Adaptive Windows MAX_MS 200 EVT_THRESH 20]
  E --> F

  F --> G[Score and Conformal per tail phase buffers]
  G --> H[Anomaly Emit MERGE tail_id flight_id win_id]

  I[Capacity Controls] --> I1[Target file size 256 to 512 MB]
  I --> I2[Partition outputs by tail_id flight_id date_utc]
  I --> I3[Autoscale shuffle partitions by input bytes]
  I --> I4[Bound state by per tail processing slices]

  J[Planning Equations] --> J1[Ingest MB per s equals flight_hours times GB_per_hour times 1024 over 3600]
  J --> J2[Window rate approx equals max events over 20 or time over 0.2s]
  J --> J3[CPU budget dominated by event extraction and sparse distance ops]

  H --> K[Observed KPIs]
  K --> K1[rows per second per stage]
  K --> K2[spill and shuffle read write]
  K --> K3[p95 stage duration]
  K --> K4[cost per flight hour]
```

## Notes
- Partitions for analytic outputs are `(tail_id, flight_id, date_utc)` where `date_utc = to_date(timestamp_utc)`.
- Backlog emissions after warm-up retain original window/event timestamps.
- `panel_context` is optional and populated from ASCII/LCD feature extraction.

## 8) Performance Test Checklist (Execution)

- Use profile matrix from `Structural_Anomaly_System_Architecture.md` section `12) Performance Test Protocol`.
- Run P1/P2/P3/P4 with identical config for 3 repeats each (same input slices).
- Capture p50/p95 stage times, spill/shuffle bytes, DBU, and cost per flight-hour.
- Validate correctness gates: no schema violations, no duplicate merge keys, backlog timestamps preserved.
- Promote config only if P1-P3 pass; use P4 for resilience-only verification.
