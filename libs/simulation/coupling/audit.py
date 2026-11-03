"""Opt-in execution evidence, separate from transferred-value provenance."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

CouplingAction = Literal["blocked", "suppressed_break", "enqueued", "written", "no_write"]
CouplingGate = Literal["phase", "source_mode", "target_mode", "mode"]


def context_identity_fields(context: Mapping[str, Any] | None) -> dict[str, str]:
    """Copy supplied identities without inferring application from declarations."""
    return {
        key: str((context or {}).get(key, "") or "")
        for key in (
            "misbehavior_window_id",
            "fault_window_id",
            "misbehavior_family_label",
            "misbehavior_detail_label",
            "fault_family_label",
            "fault_type",
        )
    }


@dataclass(frozen=True, slots=True)
class CouplingDelivery:
    """The last due queued transfer actually written, not every drained entry."""

    effective_timestamp_utc: datetime
    misbehavior_window_id: str = ""
    fault_window_id: str = ""
    misbehavior_family_label: str = ""
    misbehavior_detail_label: str = ""
    fault_family_label: str = ""
    fault_type: str = ""
    coupling_id: str = ""


@dataclass(frozen=True, slots=True)
class CouplingExecution:
    """One completed invocation; a prior delivery is independent of its action."""

    coupling_id: str
    source_module_id: str
    source_port_name: str
    target_module_id: str
    target_port_name: str
    relation_type: str
    timestamp_utc: datetime | None
    source_timestamp_utc: datetime | None
    phase_label: str | None
    gain: float
    sign: int
    lag_seconds: float
    ignore_phase_gate: bool
    context_present: bool
    action: CouplingAction
    gate_block_reason: CouplingGate | None = None
    scheduled_for_timestamp_utc: datetime | None = None
    delivered_transfer: CouplingDelivery | None = None
    misbehavior_window_id: str = ""
    fault_window_id: str = ""
    misbehavior_family_label: str = ""
    misbehavior_detail_label: str = ""
    fault_family_label: str = ""
    fault_type: str = ""
