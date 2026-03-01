# S3NTINEL — Presentation Diagrams

**S3NTINEL**
Structural Streaming Sparse Event Nexus for Telemetry Inference with Network Envelope Learning

**Naming Convention (Canonical):** First `N` = **Nexus**; second `N` = **Network**.

These visuals are simplified for reviews, roadmap decks, and cross-functional alignment.

## Operating Context

- Advana BLADE enclave (IL5), Parquet telemetry source.
- AC/MC/HC-130J fleet, ~100 TB corpus.
- ~5–10 GB per flight hour; ~33k sensors from AFDX/A-MATS tap.
- Mixed-rate streams with high-refresh channels up to ~200 Hz.

## 1) Executive Pipeline

```mermaid
flowchart LR
  A[Telemetry Ingest] --> B[CUR Structural Backbone]
  B --> C[Event Extraction]
  C --> D[Adaptive Windows]
  D --> E[Structural Signatures]
  E --> F[Phase Detection]
  F --> G[Anomaly Scoring]
  G --> H[Conformal Calibration]
  H --> I[Async Delta Anomalies]
```

## 2) Runtime Decision Flow

```mermaid
flowchart TD
  A[New samples] --> B[Update ZOH + detect events]
  B --> C{Window close?
  MAX_MS or EVT_THRESH}
  C -- No --> B
  C -- Yes --> D[Build signature]
  D --> E[Update phase state]
  E --> F[Compute anomaly score]
  F --> G{Phase warm?}
  G -- No --> H[Buffer anomaly candidate]
  G -- Yes --> I[Emit anomaly record]
  H --> B
  I --> B
```

## 3) Phase Behavior (Simple)

```mermaid
stateDiagram-v2
  [*] --> Stable
  Stable --> Entering: persistent broad drift
  Entering --> Stable: settles near centroid
  Stable --> Transition: prolonged departure
  Transition --> Stable: reaches existing/new plateau
  Stable --> LocalAnomaly: short spike
  LocalAnomaly --> Stable: recovers quickly
```

## 4) Data Products (Minimal Contract View)

```mermaid
flowchart TD
  A[raw_telemetry] --> B[events]
  B --> C[windows]
  C --> D[signatures]
  D --> E[phases]
  E --> F[calibration_buffers]
  D --> G[anomalies]
  F --> G
  H[cur_backbone] --> D
  I[column_sketch] --> H
```

## 5) NEL Concept (Hierarchy + Scoring)

```mermaid
graph TD
  A[Sensor/Event Signals] --> B[Dependency Graphs]
  B --> C[Hierarchical System Graph]
  C --> D[Nested Envelopes by Subsystem]
  D --> E[Local Scores A_k(t)]
  E --> F[Global Risk A(t)]
```

## 6) System Context

```mermaid
flowchart LR
  A[Aircraft Sensors + LCD Text] --> B[Operator Data Platform]
  B --> C[S3NTINEL Analytics]
  C --> D[Anomaly Objects in Delta]
  D --> E[Maintenance + Flight Ops]
  D --> F[Governance + Audit]
```

## 7) Throughput & Cost View

```mermaid
flowchart LR
  A[Parquet Telemetry
  5-10 GB / flight-hour
  mixed rates to 200 Hz] --> B[Batch Normalize + Extract]
  B --> C[Adaptive Window + Score]
  C --> D[Conformal + Emit]
  D --> E[Delta Anomalies]

  F[50 DBU Cluster Controls] --> F1[Partition by tail/flight/date]
  F --> F2[Target 256-512 MB files]
  F --> F3[Monitor p95 stage time + spill]
  F --> F4[Track cost per flight-hour]
```

## Canonical Name

Use this expansion across technical and external materials:

**S3NTINEL: Structural Streaming Sparse Event Nexus for Telemetry Inference with Network Envelope Learning**

This keeps the two `N`s semantically distinct (`Nexus` vs `Network`).
