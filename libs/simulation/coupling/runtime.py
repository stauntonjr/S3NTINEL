"""Live coupling runtime objects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, TYPE_CHECKING

from libs.simulation.coupling.spec import CouplingSpec

if TYPE_CHECKING:
    from libs.simulation.module.runtime import Module


def _signal_is_active(value: object | None) -> bool:
    if value is None:
        return False
    try:
        return float(value) != 0.0
    except Exception:
        return bool(value)


@dataclass(slots=True)
class DelayedTransfer:
    effective_timestamp_utc: datetime
    value: object | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DelayedTransferKey:
    source_module_id: str
    source_port_name: str
    target_module_id: str
    target_port_name: str
    relation_type: str
    gain: float
    sign: int
    lag_seconds: float
    phase_gate: tuple[str, ...] = ()
    source_mode_name: str | None = None
    source_mode_gate: tuple[str, ...] = ()
    target_mode_name: str | None = None
    target_mode_gate: tuple[str, ...] = ()


@dataclass(slots=True)
class DelayedTransferQueue:
    transfers: list[DelayedTransfer] = field(default_factory=list)

    def append(self, transfer: DelayedTransfer) -> None:
        self.transfers.append(transfer)

    def drain_due(self, *, current_timestamp_utc: datetime) -> DelayedTransfer | None:
        if not self.transfers:
            return None
        remaining: list[DelayedTransfer] = []
        last_due: DelayedTransfer | None = None
        for transfer in self.transfers:
            if transfer.effective_timestamp_utc <= current_timestamp_utc:
                last_due = transfer
            else:
                remaining.append(transfer)
        self.transfers = remaining
        return last_due

    def __bool__(self) -> bool:
        return bool(self.transfers)


@dataclass(frozen=True, slots=True)
class Coupling:
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
    allowed_misbehavior_families: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_spec(cls, spec: CouplingSpec) -> "Coupling":
        return cls(
            source_module_id=str(spec.source_module_id),
            source_port_name=str(spec.source_port_name),
            target_module_id=str(spec.target_module_id),
            target_port_name=str(spec.target_port_name),
            relation_type=str(spec.relation_type),
            gain=float(spec.gain),
            sign=int(spec.sign),
            lag_seconds=float(spec.lag_seconds),
            time_constant_seconds=(
                None if spec.time_constant_seconds is None else float(spec.time_constant_seconds)
            ),
            phase_gate=tuple(spec.phase_gate),
            mode_gate=tuple(spec.mode_gate),
            source_mode_name=spec.source_mode_name,
            source_mode_gate=tuple(spec.source_mode_gate),
            target_mode_name=spec.target_mode_name,
            target_mode_gate=tuple(spec.target_mode_gate),
            shared_noise_group=spec.shared_noise_group,
            allowed_misbehavior_families=tuple(str(item) for item in spec.allowed_misbehavior_families),
            metadata={**dict(spec.metadata), "coupling_id": spec.coupling_id},
        )

    @classmethod
    def drive(cls, **kwargs: Any) -> "Coupling":
        return cls.from_spec(CouplingSpec.drive(**kwargs))

    @classmethod
    def enable(cls, **kwargs: Any) -> "Coupling":
        return cls.from_spec(CouplingSpec.enable(**kwargs))

    @classmethod
    def inhibit(cls, **kwargs: Any) -> "Coupling":
        return cls.from_spec(CouplingSpec.inhibit(**kwargs))

    @classmethod
    def group_by_source_module(
        cls,
        couplings: tuple["Coupling", ...],
    ) -> dict[str, tuple["Coupling", ...]]:
        grouped: dict[str, list[Coupling]] = {}
        for coupling in couplings:
            grouped.setdefault(str(coupling.source_module_id), []).append(coupling)
        return {module_id: tuple(items) for module_id, items in grouped.items()}

    @property
    def coupling_id(self) -> str:
        explicit = self.metadata.get("coupling_id")
        if explicit:
            return str(explicit)
        return (
            f"{self.source_module_id}:{self.source_port_name}:{self.relation_type}:"
            f"{self.target_module_id}:{self.target_port_name}"
        )

    def key(
        self,
        *,
        gain: float | None = None,
        sign: int | None = None,
        lag_seconds: float | None = None,
    ) -> DelayedTransferKey:
        return DelayedTransferKey(
            source_module_id=self.source_module_id,
            source_port_name=self.source_port_name,
            target_module_id=self.target_module_id,
            target_port_name=self.target_port_name,
            relation_type=self.relation_type,
            gain=self.gain if gain is None else float(gain),
            sign=self.sign if sign is None else int(sign),
            lag_seconds=self.lag_seconds if lag_seconds is None else float(lag_seconds),
            phase_gate=self.phase_gate,
            source_mode_name=self.source_mode_name,
            source_mode_gate=self.source_mode_gate,
            target_mode_name=self.target_mode_name,
            target_mode_gate=self.target_mode_gate,
        )

    def is_active(
        self,
        source_module: "Module",
        target_module: "Module",
        *,
        current_phase_label: str | None = None,
        ignore_phase_gate: bool = False,
    ) -> bool:
        if self.phase_gate and not ignore_phase_gate:
            if current_phase_label is None or current_phase_label not in self.phase_gate:
                return False
        if self.source_mode_gate:
            source_mode_value = source_module.mode_state_by_name.get(str(self.source_mode_name or ""))
            if source_mode_value is None or str(source_mode_value) not in self.source_mode_gate:
                return False
        if self.target_mode_gate:
            target_mode_value = target_module.mode_state_by_name.get(str(self.target_mode_name or ""))
            if target_mode_value is None or str(target_mode_value) not in self.target_mode_gate:
                return False
        if self.mode_gate and not self.source_mode_gate and not self.target_mode_gate:
            active_mode_values = {
                *[str(value) for value in source_module.mode_state_by_name.values() if value],
                *[str(value) for value in target_module.mode_state_by_name.values() if value],
            }
            if not active_mode_values.intersection(self.mode_gate):
                return False
        return True

    def _transfer_value(
        self,
        value: object | None,
        *,
        gain: float | None = None,
        sign: int | None = None,
    ) -> object | None:
        if value is None:
            return None
        try:
            numeric_value = float(value)
        except Exception:
            return value
        effective_sign = self.sign if sign is None else int(sign)
        effective_gain = self.gain if gain is None else float(gain)
        return effective_sign * effective_gain * numeric_value

    def _resolve_transfer_result(
        self,
        source_value: object | None,
        *,
        gain: float | None = None,
        sign: int | None = None,
        break_coupling: bool = False,
    ) -> tuple[bool, object | None]:
        if break_coupling:
            return False, None
        if self.relation_type == "drive":
            return True, self._transfer_value(source_value, gain=gain, sign=sign)
        if self.relation_type == "enable":
            return True, self._transfer_value(source_value, gain=gain, sign=sign) if _signal_is_active(source_value) else None
        if self.relation_type == "inhibit":
            return (_signal_is_active(source_value), None)
        return True, self._transfer_value(source_value, gain=gain, sign=sign)

    def _drain_due_transfers(
        self,
        target_module: "Module",
        *,
        current_timestamp_utc: datetime,
        transfer_key: DelayedTransferKey | None = None,
    ) -> None:
        queue = target_module.delayed_input_transfers_by_key.get(transfer_key or self.key())
        if queue is None:
            return
        last_due = queue.drain_due(current_timestamp_utc=current_timestamp_utc)
        if last_due is not None:
            target_port = target_module.input_port(self.target_port_name)
            target_port.current_value = last_due.value
            target_port.timestamp_utc = last_due.effective_timestamp_utc
            target_port.metadata = dict(last_due.metadata)
        if not queue:
            target_module.delayed_input_transfers_by_key.pop(transfer_key or self.key(), None)

    def _build_transfer_metadata(
        self,
        *,
        misbehavior_context: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        metadata = {
            "coupling_id": self.coupling_id,
            "source_module_id": self.source_module_id,
            "source_port_name": self.source_port_name,
            "target_module_id": self.target_module_id,
            "target_port_name": self.target_port_name,
            "relation_type": self.relation_type,
        }
        if not misbehavior_context:
            return metadata
        metadata.update(
            {
                "misbehavior_active": bool(misbehavior_context.get("misbehavior_active", True)),
                "misbehavior_family_label": str(misbehavior_context.get("misbehavior_family_label", "") or ""),
                "misbehavior_detail_label": str(misbehavior_context.get("misbehavior_detail_label", "") or ""),
                "misbehavior_window_id": str(misbehavior_context.get("misbehavior_window_id", "") or ""),
                "event_misbehavior_label": str(misbehavior_context.get("event_misbehavior_label", "") or ""),
                "anomaly_type_label": str(misbehavior_context.get("anomaly_type_label", "") or ""),
                "anomaly_score_label": misbehavior_context.get("anomaly_score_label"),
                "fault_active": bool(misbehavior_context.get("fault_active", bool(misbehavior_context))),
                "fault_family_label": str(misbehavior_context.get("fault_family_label", "") or ""),
                "fault_type": str(misbehavior_context.get("fault_type", "") or ""),
                "fault_window_id": str(misbehavior_context.get("fault_window_id", "") or ""),
            }
        )
        return metadata

    def _effective_transfer_config(
        self,
        misbehavior_context: Mapping[str, Any] | None,
        *,
        source_timestamp_utc: datetime | None,
    ) -> dict[str, Any]:
        if not misbehavior_context:
            return {
                "gain": self.gain,
                "sign": self.sign,
                "lag_seconds": self.lag_seconds,
                "ignore_phase_gate": False,
                "break_coupling": False,
            }
        violation_type = str(
            misbehavior_context.get("misbehavior_detail_label")
            or misbehavior_context.get("misbehavior_family_label")
            or misbehavior_context.get("violation_type")
            or ""
        )
        effective_gain = float(misbehavior_context.get("gain_override", self.gain))
        effective_sign = int(misbehavior_context.get("sign_override", self.sign))
        effective_lag_seconds = float(
            misbehavior_context.get(
                "lag_seconds_override",
                misbehavior_context.get(
                    "lag_seconds",
                    misbehavior_context.get("extra_lag_seconds", self.lag_seconds),
                ),
            )
        )
        ignore_phase_gate = bool(misbehavior_context.get("ignore_phase_gate", False))
        break_coupling = False

        if violation_type == "coupling_break":
            break_coupling = True
        elif violation_type == "coupling_inversion":
            effective_sign = -abs(self.sign) if self.sign > 0 else abs(self.sign)
        elif violation_type == "timing_lag":
            effective_lag_seconds = self.lag_seconds + float(
                misbehavior_context.get("lag_delta_seconds", misbehavior_context.get("extra_lag_seconds", 0.0))
            )
        elif violation_type == "timing_jitter":
            jitter_seconds = float(misbehavior_context.get("jitter_seconds", 0.0))
            if jitter_seconds == 0.0 and source_timestamp_utc is not None:
                jitter_seconds = float(misbehavior_context.get("jitter_seconds_even", 0.0))
                if int(source_timestamp_utc.timestamp()) % 2:
                    jitter_seconds = float(misbehavior_context.get("jitter_seconds_odd", jitter_seconds))
            effective_lag_seconds = max(self.lag_seconds + jitter_seconds, 0.0)
        elif violation_type == "phase_context_violation":
            ignore_phase_gate = True

        return {
            "gain": effective_gain,
            "sign": effective_sign,
            "lag_seconds": max(effective_lag_seconds, 0.0),
            "ignore_phase_gate": ignore_phase_gate,
            "break_coupling": break_coupling,
        }

    def apply(
        self,
        source_module: "Module",
        target_module: "Module",
        *,
        timestamp_utc: datetime | None = None,
        current_phase_label: str | None = None,
        misbehavior_context: Mapping[str, Any] | None = None,
    ) -> None:
        source_port = source_module.output_port(self.source_port_name)
        target_port = target_module.input_port(self.target_port_name)
        source_value = source_port.current_value
        source_timestamp_utc = timestamp_utc if timestamp_utc is not None else source_port.timestamp_utc
        transfer_config = self._effective_transfer_config(
            misbehavior_context,
            source_timestamp_utc=source_timestamp_utc,
        )
        transfer_key = self.key(
            gain=float(transfer_config["gain"]),
            sign=int(transfer_config["sign"]),
            lag_seconds=float(transfer_config["lag_seconds"]),
        )

        if float(transfer_config["lag_seconds"]) > 0.0 and source_timestamp_utc is not None:
            self._drain_due_transfers(
                target_module,
                current_timestamp_utc=source_timestamp_utc,
                transfer_key=transfer_key,
            )

        if not self.is_active(
            source_module,
            target_module,
            current_phase_label=current_phase_label,
            ignore_phase_gate=bool(transfer_config["ignore_phase_gate"]),
        ):
            return

        should_write, transferred_value = self._resolve_transfer_result(
            source_value,
            gain=float(transfer_config["gain"]),
            sign=int(transfer_config["sign"]),
            break_coupling=bool(transfer_config["break_coupling"]),
        )
        transfer_metadata = self._build_transfer_metadata(misbehavior_context=misbehavior_context)
        if float(transfer_config["lag_seconds"]) > 0.0 and source_timestamp_utc is not None:
            queue = target_module.delayed_input_transfers_by_key.setdefault(
                transfer_key,
                DelayedTransferQueue(),
            )
            if should_write:
                queue.append(
                    DelayedTransfer(
                        effective_timestamp_utc=source_timestamp_utc + timedelta(seconds=float(transfer_config["lag_seconds"])),
                        value=transferred_value,
                        metadata=transfer_metadata,
                    )
                )
            return
        if not should_write:
            return
        target_port.current_value = transferred_value
        target_port.timestamp_utc = source_timestamp_utc
        target_port.metadata = transfer_metadata
