"""Canonical phase catalog."""

from __future__ import annotations


def default_phase_definitions() -> list[dict[str, object]]:
    return [
        {
            "phase_id": 0,
            "phase_name": "gate_turnaround",
        },
        {
            "phase_id": 1,
            "phase_name": "taxi_out",
        },
        {
            "phase_id": 2,
            "phase_name": "takeoff_climb",
        },
        {
            "phase_id": 3,
            "phase_name": "cruise",
        },
        {
            "phase_id": 4,
            "phase_name": "descent_approach",
        },
        {
            "phase_id": 5,
            "phase_name": "landing_rollout",
        },
    ]


CANONICAL_PHASE_DEFINITIONS = tuple(default_phase_definitions())
CANONICAL_PHASE_IDS_BY_LABEL = {
    str(item["phase_name"]): int(item["phase_id"])
    for item in CANONICAL_PHASE_DEFINITIONS
}
CANONICAL_PHASE_LABELS = tuple(CANONICAL_PHASE_IDS_BY_LABEL)
