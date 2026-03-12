"""Inter-object coupling specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class CouplingSpec:
    source_module_id: str
    source_port_name: str
    target_module_id: str
    target_port_name: str
    relation_type: str
    gain: float = 1.0
    sign: int = 1
    lag_seconds: float = 0.0
    time_constant_seconds: float | None = None
    phase_gate: tuple[str, ...] = ()
    mode_gate: tuple[str, ...] = ()
    source_mode_name: str | None = None
    source_mode_gate: tuple[str, ...] = ()
    target_mode_name: str | None = None
    target_mode_gate: tuple[str, ...] = ()
    shared_noise_group: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def drive(
        cls,
        *,
        source_module_id: str,
        source_port_name: str,
        target_module_id: str,
        target_port_name: str,
        gain: float = 1.0,
        sign: int = 1,
        lag_seconds: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> "CouplingSpec":
        return cls(
            source_module_id=source_module_id,
            source_port_name=source_port_name,
            target_module_id=target_module_id,
            target_port_name=target_port_name,
            relation_type="drive",
            gain=float(gain),
            sign=int(sign),
            lag_seconds=float(lag_seconds),
            metadata=dict(metadata or {}),
        )

    @classmethod
    def enable(
        cls,
        *,
        source_module_id: str,
        source_port_name: str,
        target_module_id: str,
        target_port_name: str,
        gain: float = 1.0,
        sign: int = 1,
        lag_seconds: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> "CouplingSpec":
        return cls(
            source_module_id=source_module_id,
            source_port_name=source_port_name,
            target_module_id=target_module_id,
            target_port_name=target_port_name,
            relation_type="enable",
            gain=float(gain),
            sign=int(sign),
            lag_seconds=float(lag_seconds),
            metadata=dict(metadata or {}),
        )

    @classmethod
    def inhibit(
        cls,
        *,
        source_module_id: str,
        source_port_name: str,
        target_module_id: str,
        target_port_name: str,
        gain: float = 1.0,
        sign: int = 1,
        lag_seconds: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> "CouplingSpec":
        return cls(
            source_module_id=source_module_id,
            source_port_name=source_port_name,
            target_module_id=target_module_id,
            target_port_name=target_port_name,
            relation_type="inhibit",
            gain=float(gain),
            sign=int(sign),
            lag_seconds=float(lag_seconds),
            metadata=dict(metadata or {}),
        )
