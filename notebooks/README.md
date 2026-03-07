# Notebooks

Use this directory for exploratory analysis and validation notebooks.

The active architecture is V2. Notebook analysis should follow:

- `window_x` for backbone fit inputs
- `window_s` for phase/scoring structure
- V2 fitting artifacts (`backbone`, `precision_graph`, `event_graph`, `lag_graph`, `transition_graph`, `fused_graph`, `hierarchy_sensor_map`)
- canonical label/detected naming only

Legacy notebooks were removed. Recreate notebooks from the active V2 pipeline artifacts only.

Notebook cleanup rules:

- prefer `parameter_datatype_label` and `parameter_datatype_profiled`
- prefer `event_type_label` and `event_type_detected`
- do not use `truth_*`
- do not treat `cooccur` as part of the active V2 event contract
