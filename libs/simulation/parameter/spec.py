"""Parameter specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ParameterSpec:
    parameter_name: str
    system_id: str
    subsystem_id: str
    module_id: str
    parameter_datatype_label: str
    unit: str = ""
    behavior_family_label: str | None = None
    latent_group: str | None = None
    sampling_rate_hz: float | None = None
    noise_scale: float = 0.0
    quantization: float | None = None
    delay_class: str | None = None
    phase_envelope_id: str | None = None
    allowed_fault_families: tuple[str, ...] = ()
    input_port_names: tuple[str, ...] = ()
    output_port_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
