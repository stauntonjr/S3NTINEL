"""Example coupling specs."""

from __future__ import annotations

from typing import Any

from libs.simulation.coupling.spec import CouplingSpec


def build_drive_coupling_spec(
    *,
    source_module_id: str,
    source_port_name: str,
    target_module_id: str,
    target_port_name: str,
    gain: float = 1.0,
    sign: int = 1,
    lag_seconds: float = 0.0,
    phase_gate: tuple[str, ...] = (),
    source_mode_name: str | None = None,
    source_mode_gate: tuple[str, ...] = (),
    target_mode_name: str | None = None,
    target_mode_gate: tuple[str, ...] = (),
    allowed_misbehavior_families: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> CouplingSpec:
    return CouplingSpec(
        source_module_id=source_module_id,
        source_port_name=source_port_name,
        target_module_id=target_module_id,
        target_port_name=target_port_name,
        relation_type="drive",
        gain=gain,
        sign=sign,
        lag_seconds=lag_seconds,
        phase_gate=tuple(phase_gate),
        source_mode_name=source_mode_name,
        source_mode_gate=tuple(source_mode_gate),
        target_mode_name=target_mode_name,
        target_mode_gate=tuple(target_mode_gate),
        allowed_misbehavior_families=tuple(str(name) for name in allowed_misbehavior_families),
        metadata=dict(metadata or {}),
    )


def build_enable_coupling_spec(
    *,
    source_module_id: str,
    source_port_name: str,
    target_module_id: str,
    target_port_name: str,
    gain: float = 1.0,
    sign: int = 1,
    lag_seconds: float = 0.0,
    phase_gate: tuple[str, ...] = (),
    allowed_misbehavior_families: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> CouplingSpec:
    return CouplingSpec(
        source_module_id=source_module_id,
        source_port_name=source_port_name,
        target_module_id=target_module_id,
        target_port_name=target_port_name,
        relation_type="enable",
        gain=gain,
        sign=sign,
        lag_seconds=lag_seconds,
        phase_gate=tuple(phase_gate),
        allowed_misbehavior_families=tuple(str(name) for name in allowed_misbehavior_families),
        metadata=dict(metadata or {}),
    )


def build_inhibit_coupling_spec(
    *,
    source_module_id: str,
    source_port_name: str,
    target_module_id: str,
    target_port_name: str,
    gain: float = 1.0,
    sign: int = 1,
    lag_seconds: float = 0.0,
    phase_gate: tuple[str, ...] = (),
    allowed_misbehavior_families: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> CouplingSpec:
    return CouplingSpec(
        source_module_id=source_module_id,
        source_port_name=source_port_name,
        target_module_id=target_module_id,
        target_port_name=target_port_name,
        relation_type="inhibit",
        gain=gain,
        sign=sign,
        lag_seconds=lag_seconds,
        phase_gate=tuple(phase_gate),
        allowed_misbehavior_families=tuple(str(name) for name in allowed_misbehavior_families),
        metadata=dict(metadata or {}),
    )
