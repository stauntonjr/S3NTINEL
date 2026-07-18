# Canonical Label Schema

This repo uses one label/detection nomenclature with no aliases.

Architecture note:

- label/detected naming here applies to both legacy and V2 code paths
- the active pipeline architecture is documented in [v2_architecture.md](../current/v2_architecture.md)

## Canonical fields

Current emitted simulation truth fields:

- `event_type_label`
- `event_misbehavior_label`
- `anomaly_type_label`
- `anomaly_score_label`
- `event_type_detected`
- `anomaly_type_detected`
- `anomaly_score_detected`

## Hard bans

- Any column or payload key starting with `truth_`
- `sim_event_type`
- `truth_event_label_*`
- `truth_anomaly_label_*`
- Bare detector output field `event_type` (use `event_type_detected`)

## Producer/consumer contracts

- Simulation telemetry must emit only canonical label fields (`*_label`).
- Event detector outputs must emit only canonical detector fields (`*_detected`).
- Signal-semantic event validation should match `event_type_label` to `event_type_detected`.
- Misbehavior-driven event-proxy evaluation may separately compare `event_misbehavior_label` to `event_type_detected`.
- No backward-compatibility reads for legacy names.
