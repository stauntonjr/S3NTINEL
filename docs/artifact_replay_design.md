# Artifact Replay Design

This note defines how S3NTINEL should persist, version, and replay stage artifacts on
the active V2 path.

The goal is simple:

- expensive stages should not be recomputed unnecessarily
- downstream tuning should reuse stable upstream artifacts
- lineage should be explicit
- replay should be operational, not notebook folklore

This document treats MLflow as a lineage/index layer, not as the sole artifact store.

## 1. Design goals

The replay system should support:

- deterministic stage re-execution from persisted upstream artifacts
- cheap parameter sweeps on late-stage structure
- explicit lineage between stages
- schema/version visibility
- cache invalidation when definitions change

The replay unit should be a **stage artifact set**, not a free-form report JSON.

## 2. Canonical concepts

### canonical artifact

A persisted table or file that is part of the primary pipeline contract.

Examples:

- `backbone`
- `event_graph`
- `phase_baselines`
- `window_scores_raw`
- `anomaly_window_attribution`

### replay cache

A persisted derived artifact whose purpose is to accelerate re-runs of a later stage.

Examples:

- graph component cache before fusion/top-k pruning
- per-flight `G_f` / `H_f` cache for backbone sweeps
- phase feature/config selection cache

Replay caches are useful, but they are not the same thing as canonical stage outputs.

### stage manifest

A compact metadata object describing:

- what a stage consumed
- what it produced
- how it was configured
- how to replay it

## 3. Artifact classes by stage

## 3.0 Parameter metadata fitting

These artifacts should be treated as replayable metadata inputs to later structural
stages.

### canonical outputs

- `parameter_datatype_profile`
- `continuous_scaling_profile`
- `parameter_behavior_profile`

These are the parameter-level artifacts that should normally be fit once from
observed telemetry and reused during backbone, graph, phase, and inference work.

## 3.1 `10_backbone_fit`

### canonical outputs

- `window_x` optional persisted intermediate
- `backbone`
- `backbone_sensor_energy`

### optional replay cache

- `backbone_gh_cache`
  - per-flight `G_f`
  - per-flight `H_f`
  - selected sensor order
  - window counts

This cache is what makes backbone-size or ridge sweeps cheap without rebuilding
`window_x`.

## 3.2 `11_graph_fit`

### canonical outputs

- `precision_graph`
- `event_graph`
- `lag_graph`
- `transition_graph`
- `fused_graph`
- `hierarchy_sensor_map`

In the active implementation:

- `precision_graph`, `event_graph`, `lag_graph`, `transition_graph`, and
  `fused_graph` are built in Spark
- `hierarchy_sensor_map` is derived from the already-pruned fused edge set on the
  driver

### optional replay cache

- `graph_component_cache`
  - `precision_graph` before downstream threshold changes
  - `event_graph` before top-k pruning changes
  - `lag_graph` before top-k pruning changes
  - `transition_graph`
  - `backbone`
  - `hierarchy_labels` when available in simulation/evaluation mode

This is the cache needed for cheap hierarchy-only sweeps.

The practical replay seam is now:

1. reuse component graph tables directly
2. rebuild `fused_graph` cheaply in Spark if fusion weights change
3. rerun only the small hierarchy-assignment step on the pruned fused edge set

## 3.3 `50_phase_fit`

### canonical outputs

- `phase_windows`
- `phase_baselines`

### optional replay cache

- `phase_fit_cache`
  - selected continuous sensors
  - selected event features
  - selected categorical-state features
  - selected cooccurrence features if used
  - stable-window configuration

This cache allows segmentation and clustering sweeps without redoing feature
selection.

## 3.4 `60_window_scores_raw`

### canonical outputs

- `window_scores_raw`

### optional replay cache

- usually unnecessary if `phase_windows`, `phase_baselines`, and
  `hierarchy_sensor_map` are already persisted

This stage should normally replay directly from canonical upstream artifacts.

## 3.5 `70_window_scores_calibrate`

### canonical outputs

- `window_scores_calibrated`

### optional replay cache

- optional calibration buffer snapshots if online calibration is revisited later

## 3.6 `80_anomaly_attribution`

### canonical outputs

- `anomaly_window_attribution`
- `anomaly_telemetry_attribution`
- `anomaly_event_attribution`

### optional replay cache

- usually unnecessary

This stage should replay from canonical upstream tables.

## 4. Stage manifest schema

Every stage run should emit a manifest JSON.

Suggested schema:

```json
{
  "stage_name": "11_graph_fit",
  "stage_version": "v2",
  "run_id": "uuid-or-mlflow-run-id",
  "created_at_utc": "2026-03-07T12:34:56Z",
  "config": {},
  "input_artifacts": {
    "backbone": {
      "path": "...",
      "artifact_version": "BACKBONE_V2",
      "schema_hash": "...",
      "content_hash": "..."
    }
  },
  "output_artifacts": {
    "fused_graph": {
      "path": "...",
      "row_count": 1234,
      "schema_hash": "...",
      "content_hash": "..."
    }
  },
  "replayable_from": [
    "window_x",
    "event_graph",
    "lag_graph",
    "backbone"
  ],
  "cache_artifacts": {
    "graph_component_cache": {
      "path": "...",
      "schema_hash": "...",
      "content_hash": "..."
    }
  },
  "timing": {
    "elapsed_seconds": 12.3
  }
}
```

Minimum required fields:

- `stage_name`
- `stage_version`
- `created_at_utc`
- `config`
- `input_artifacts`
- `output_artifacts`

## 5. MLflow logging policy

MLflow should be used as a **catalog and lineage system**.

It should log:

- stage params
- stage metrics
- manifest JSON
- output artifact paths
- schema hashes
- content hashes
- row counts
- small previews

It should not be the only place the actual replayable data lives.

### recommended MLflow params

- stage config values
- upstream artifact versions
- stage code version or git SHA if available

### recommended MLflow metrics

- elapsed time
- output row counts
- quality metrics when available

### recommended MLflow artifacts

- stage manifest JSON
- schema JSON snapshots
- small sample row previews
- replay cache manifest if applicable

## 6. Filesystem/table storage policy

Persist actual replayable artifacts in stable stage paths.

Examples:

- `data/delta/backbone`
- `data/delta/precision_graph`
- `data/delta/window_scores_raw`

Replay caches should live beside or under stage-specific cache paths.

Examples:

- `data/cache/backbone_gh_cache/...`
- `data/cache/graph_component_cache/...`
- `data/cache/phase_fit_cache/...`

Do not hide important replay inputs only inside MLflow artifacts.

## 7. Cache invalidation rules

Replay caches must be treated as invalid when any of the following changes:

- upstream canonical artifact content
- stage config that affects the cached object
- schema shape of the cached object
- code version of the logic that produced the cached object

At minimum, invalidation keys should include:

- `schema_hash`
- `content_hash`
- `config_hash`
- `code_version`

For example:

### graph component cache invalidates if

- `window_x` definition changes
- event graph normalization changes
- lag graph normalization changes
- backbone changes
- precision graph formula changes

### backbone `G_f/H_f` cache invalidates if

- `window_x` changes
- selected sensors `C` changes
- scaling changes

## 8. Replay entrypoints

Replay should be explicit, not hidden in tuning scripts.

Recommended scripts:

- `scripts/replay_backbone_fit.py`
- `scripts/replay_graph_fit.py`
- `scripts/replay_phase_fit.py`
- `scripts/replay_window_scores.py`

Each should:

- accept artifact paths or run IDs
- accept override knobs
- write outputs to a new destination
- emit a fresh manifest

This is better than forcing every replay through the full pipeline entrypoint.

## 9. Reports vs artifacts

Reports are for inspection.

Artifacts are for replay.

That means:

- report JSONs can summarize
- report JSONs can include top edges, counts, metrics
- report JSONs should not be treated as the primary replay substrate

The current simulation graph sweep exposed exactly this issue:

- report reuse was cheap
- true hierarchy-only replay was not possible from old reports because they only
  contained truncated edge summaries

That is the class of problem this design is meant to prevent.

## 10. Recommended immediate changes

The next implementation steps should be:

1. add a reusable manifest writer/helper
2. add per-stage manifest emission for:
   - `10_backbone_fit`
   - `11_graph_fit`
   - `50_phase_fit`
   - `60_window_scores_raw`
3. promote current graph cache JSON into a formal replay cache artifact with manifest
4. add explicit replay scripts for:
   - graph
   - phase
5. log manifests and artifact paths into MLflow

## 11. Current repo status

The repo already has the start of this pattern:

- canonical V2 stage outputs
- graph-component cache support in the simulation runner
- sweep scripts that can reuse cached reports

What it does not yet have is:

- uniform manifest emission
- explicit replay scripts
- a stable cache invalidation policy
- MLflow as a proper artifact-lineage index

That is the next maturity step.
