"""Mutable simulation runtime objects built from static simulation specs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.simulation.specs import ModuleSpec, ParameterSpec, PortSpec


@dataclass(slots=True)
class DelayedPortTransfer:
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
class PortRuntime:
    spec: PortSpec
    current_value: object | None = None
    timestamp_utc: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ParameterRuntime:
    spec: ParameterSpec
    parameter_value: object | None = None
    parameter_value_clean: object | None = None
    timestamp_utc: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def behavior_family_label(self) -> str | None:
        return self.spec.behavior_family_label

    def update_observation(
        self,
        *,
        parameter_value: object,
        parameter_value_clean: object | None = None,
        timestamp_utc: datetime | None = None,
    ) -> None:
        self.parameter_value = parameter_value
        self.parameter_value_clean = parameter_value_clean
        self.timestamp_utc = timestamp_utc

@dataclass(slots=True)
class ModuleRuntime:
    spec: ModuleSpec
    parameters: dict[str, ParameterRuntime]
    input_ports: dict[str, PortRuntime]
    output_ports: dict[str, PortRuntime]
    latent_state_by_name: dict[str, float] = field(default_factory=dict)
    controller_state_by_name: dict[str, Any] = field(default_factory=dict)
    mode_state_by_name: dict[str, str] = field(default_factory=dict)
    delayed_input_transfers_by_key: dict[DelayedTransferKey, list[DelayedPortTransfer]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_spec(cls, spec: ModuleSpec) -> "ModuleRuntime":
        return cls(
            spec=spec,
            parameters={parameter_spec.parameter_name: ParameterRuntime(spec=parameter_spec) for parameter_spec in spec.parameters},
            input_ports={port_spec.port_name: PortRuntime(spec=port_spec) for port_spec in spec.input_ports},
            output_ports={port_spec.port_name: PortRuntime(spec=port_spec) for port_spec in spec.output_ports},
            latent_state_by_name={
                latent_name: 0.0
                for latent_name in {
                    *spec.latent_variables,
                    *[latent_update_spec.latent_name for latent_update_spec in spec.latent_update_specs],
                }
            },
            controller_state_by_name={controller_name: None for controller_name in spec.controllers},
            mode_state_by_name={state_name: "" for state_name in spec.state_machines},
        )

    def parameter_runtime(self, parameter_name: str) -> ParameterRuntime:
        return self.parameters[str(parameter_name)]

    def input_port_runtime(self, port_name: str) -> PortRuntime:
        return self.input_ports[str(port_name)]

    def output_port_runtime(self, port_name: str) -> PortRuntime:
        return self.output_ports[str(port_name)]


def module_runtimes_from_specs(module_specs: tuple[ModuleSpec, ...]) -> tuple[ModuleRuntime, ...]:
    return tuple(ModuleRuntime.from_spec(module_spec) for module_spec in module_specs)
