"""Named objective presets for tuning workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ObjectivePreset:
    name: str
    description: str
    objective_name: str | None = None
    objective_spec_path: str | None = None
    objective_overrides: tuple[tuple[str, Any], ...] = ()


OBJECTIVE_PRESET_BY_NAME = {
    "event_recall_heavy": ObjectivePreset(
        name="event_recall_heavy",
        description="Event objective variant that biases recall over precision and overall latency.",
        objective_name="sim_event_default_v1",
        objective_overrides=(
            ("name", "sim_event_recall_heavy_v1"),
            ("primary_terms.0.weight", 2.0),
            ("primary_terms.1.weight", 2.0),
            ("primary_terms.4.weight", 0.25),
        ),
    ),
    "structural_latency_biased": ObjectivePreset(
        name="structural_latency_biased",
        description="Structural objective variant that keeps structural metrics primary but pulls harder on runtime tie-breaks.",
        objective_name="sim_structural_default_v1",
        objective_overrides=(
            ("name", "sim_structural_latency_biased_v1"),
            ("tie_break_terms.0.weight", 0.6),
            ("tie_break_terms.1.weight", 0.25),
        ),
    ),
}


KNOWN_OBJECTIVE_PRESET_NAMES = tuple(sorted(OBJECTIVE_PRESET_BY_NAME))
