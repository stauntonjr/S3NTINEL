# Pipeline Data Flow

```mermaid
flowchart LR
    raw["Raw Telemetry / Simulation Output"]
    stage_1["pipelines/00_ingest_raw.py"]
    raw --> stage_1
    stage_2["pipelines/10_parameter_profiles_fit.py"]
    stage_1 --> stage_2
    stage_3["pipelines/12_behavior_profiles_fit.py"]
    stage_2 --> stage_3
    stage_4["pipelines/15_event_profiles_fit.py"]
    stage_3 --> stage_4
    stage_5["pipelines/20_events_extract.py"]
    stage_4 --> stage_5
    stage_6["pipelines/25_window_policy_profile.py"]
    stage_5 --> stage_6
    stage_7["pipelines/30_windows_adaptive.py"]
    stage_6 --> stage_7
    stage_8["pipelines/40_backbone_fit.py"]
    stage_7 --> stage_8
    stage_9["pipelines/50_build_graph.py"]
    stage_8 --> stage_9
    stage_10["pipelines/60_fit_hierarchy.py"]
    stage_9 --> stage_10
    stage_11["pipelines/70_phase_fit.py"]
    stage_10 --> stage_11
    stage_12["pipelines/72_phase_label_centroids.py"]
    stage_11 --> stage_12
    stage_13["pipelines/80_window_scores_raw.py"]
    stage_12 --> stage_13
    stage_14["pipelines/85_window_scores_calibrate.py"]
    stage_13 --> stage_14
    stage_15["pipelines/90_anomaly_attribution.py"]
    stage_14 --> stage_15
    stage_16["pipelines/95_emit_explorer_bundle.py"]
    stage_15 --> stage_16
    artifacts["Persisted Artifacts / Reports"]
    stage_16 --> artifacts
```
