"""Inter-module port transfer helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Mapping

from libs.simulation.runtime import DelayedPortTransfer, DelayedTransferKey, ModuleRuntime
from libs.simulation.specs import InterModuleCouplingSpec


def _signal_is_active(value: object | None) -> bool:
    if value is None:
        return False
    try:
        return float(value) != 0.0
    except Exception:
        return bool(value)


def _transfer_value(
    value: object | None,
    coupling_spec: InterModuleCouplingSpec,
) -> object | None:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except Exception:
        return value
    return coupling_spec.sign * coupling_spec.gain * numeric_value


def _coupling_key(coupling_spec: InterModuleCouplingSpec) -> DelayedTransferKey:
    return DelayedTransferKey(
        source_module_id=coupling_spec.source_module_id,
        source_port_name=coupling_spec.source_port_name,
        target_module_id=coupling_spec.target_module_id,
        target_port_name=coupling_spec.target_port_name,
        relation_type=coupling_spec.relation_type,
        gain=float(coupling_spec.gain),
        sign=int(coupling_spec.sign),
        lag_seconds=float(coupling_spec.lag_seconds),
        phase_gate=tuple(coupling_spec.phase_gate),
        source_mode_name=coupling_spec.source_mode_name,
        source_mode_gate=tuple(coupling_spec.source_mode_gate),
        target_mode_name=coupling_spec.target_mode_name,
        target_mode_gate=tuple(coupling_spec.target_mode_gate),
    )


def _coupling_is_active(
    source_module_runtime: ModuleRuntime,
    target_module_runtime: ModuleRuntime,
    coupling_spec: InterModuleCouplingSpec,
    *,
    current_phase_label: str | None = None,
) -> bool:
    if coupling_spec.phase_gate:
        if current_phase_label is None or current_phase_label not in coupling_spec.phase_gate:
            return False
    if coupling_spec.source_mode_gate:
        source_mode_value = source_module_runtime.mode_state_by_name.get(
            str(coupling_spec.source_mode_name or "")
        )
        if (
            source_mode_value is None
            or str(source_mode_value) not in coupling_spec.source_mode_gate
        ):
            return False
    if coupling_spec.target_mode_gate:
        target_mode_value = target_module_runtime.mode_state_by_name.get(
            str(coupling_spec.target_mode_name or "")
        )
        if (
            target_mode_value is None
            or str(target_mode_value) not in coupling_spec.target_mode_gate
        ):
            return False
    if (
        coupling_spec.mode_gate
        and not coupling_spec.source_mode_gate
        and not coupling_spec.target_mode_gate
    ):
        active_mode_values = {
            *[str(value) for value in source_module_runtime.mode_state_by_name.values() if value],
            *[str(value) for value in target_module_runtime.mode_state_by_name.values() if value],
        }
        if not active_mode_values.intersection(coupling_spec.mode_gate):
            return False
    return True


def _resolve_transfer_result(
    source_value: object | None,
    coupling_spec: InterModuleCouplingSpec,
) -> tuple[bool, object | None]:
    relation_type = coupling_spec.relation_type
    if relation_type == "drive":
        return True, _transfer_value(source_value, coupling_spec)
    if relation_type == "enable":
        return True, _transfer_value(source_value, coupling_spec) if _signal_is_active(source_value) else None
    if relation_type == "inhibit":
        return (_signal_is_active(source_value), None)
    return True, _transfer_value(source_value, coupling_spec)


def _drain_due_delayed_transfers(
    target_module_runtime: ModuleRuntime,
    coupling_spec: InterModuleCouplingSpec,
    *,
    current_timestamp_utc: datetime,
) -> None:
    queue_key = _coupling_key(coupling_spec)
    queue = target_module_runtime.delayed_input_transfers_by_key.get(queue_key, [])
    if not queue:
        return
    target_port_runtime = target_module_runtime.input_port_runtime(coupling_spec.target_port_name)
    remaining: list[DelayedPortTransfer] = []
    last_due: DelayedPortTransfer | None = None
    for queued_transfer in queue:
        if queued_transfer.effective_timestamp_utc <= current_timestamp_utc:
            last_due = queued_transfer
        else:
            remaining.append(queued_transfer)
    if last_due is not None:
        target_port_runtime.current_value = last_due.value
        target_port_runtime.timestamp_utc = last_due.effective_timestamp_utc
    if remaining:
        target_module_runtime.delayed_input_transfers_by_key[queue_key] = remaining
    else:
        target_module_runtime.delayed_input_transfers_by_key.pop(queue_key, None)


def apply_inter_module_coupling(
    source_module_runtime: ModuleRuntime,
    target_module_runtime: ModuleRuntime,
    coupling_spec: InterModuleCouplingSpec,
    *,
    timestamp_utc: datetime | None = None,
    current_phase_label: str | None = None,
) -> None:
    source_port_runtime = source_module_runtime.output_port_runtime(coupling_spec.source_port_name)
    target_port_runtime = target_module_runtime.input_port_runtime(coupling_spec.target_port_name)
    source_value = source_port_runtime.current_value
    source_timestamp_utc = timestamp_utc if timestamp_utc is not None else source_port_runtime.timestamp_utc

    if coupling_spec.lag_seconds > 0.0 and source_timestamp_utc is not None:
        _drain_due_delayed_transfers(
            target_module_runtime,
            coupling_spec,
            current_timestamp_utc=source_timestamp_utc,
        )

    if not _coupling_is_active(
        source_module_runtime,
        target_module_runtime,
        coupling_spec,
        current_phase_label=current_phase_label,
    ):
        return

    should_write, transferred_value = _resolve_transfer_result(source_value, coupling_spec)
    if coupling_spec.lag_seconds > 0.0 and source_timestamp_utc is not None:
        queue_key = _coupling_key(coupling_spec)
        queue = target_module_runtime.delayed_input_transfers_by_key.setdefault(queue_key, [])
        if should_write:
            queue.append(
                DelayedPortTransfer(
                    effective_timestamp_utc=source_timestamp_utc + timedelta(seconds=float(coupling_spec.lag_seconds)),
                    value=transferred_value,
                )
            )
        return
    if not should_write:
        return
    target_port_runtime.current_value = transferred_value
    target_port_runtime.timestamp_utc = source_timestamp_utc


def propagate_inter_module_couplings(
    module_runtimes_by_id: Mapping[str, ModuleRuntime],
    inter_module_couplings: tuple[InterModuleCouplingSpec, ...],
    *,
    timestamp_utc: datetime | None = None,
    current_phase_label: str | None = None,
) -> None:
    for coupling_spec in inter_module_couplings:
        source_module_runtime = module_runtimes_by_id.get(coupling_spec.source_module_id)
        if source_module_runtime is None:
            raise KeyError(
                f"missing source module runtime for inter-module coupling: {coupling_spec.source_module_id}"
            )
        target_module_runtime = module_runtimes_by_id.get(coupling_spec.target_module_id)
        if target_module_runtime is None:
            raise KeyError(
                f"missing target module runtime for inter-module coupling: {coupling_spec.target_module_id}"
            )
        apply_inter_module_coupling(
            source_module_runtime,
            target_module_runtime,
            coupling_spec,
            timestamp_utc=timestamp_utc,
            current_phase_label=current_phase_label,
        )
