"""Live module runtime objects."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TYPE_CHECKING

from libs.behavior import (
    BehaviorRegistry,
    BehaviorSample,
    BehaviorStepInput,
    build_default_behavior_registry,
    iter_tick_samples,
)
from libs.simulation.coupling.runtime import Coupling, DelayedTransferKey, DelayedTransferQueue
from libs.simulation.module.spec import LatentUpdateSpec, ModuleSpec
from libs.simulation.parameter.runtime import Parameter
from libs.simulation.port.runtime import Port

if TYPE_CHECKING:
    from libs.simulation.module.runtime import Module


@dataclass(frozen=True, slots=True)
class LatentUpdate:
    latent_name: str
    source_name: str
    source_kind: str = "input_port"
    gain: float = 1.0
    sign: int = 1
    offset: float = 0.0
    default_value: float = 0.0
    clamp_min: float | None = None
    clamp_max: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_spec(cls, spec: LatentUpdateSpec) -> "LatentUpdate":
        return cls(
            latent_name=str(spec.latent_name),
            source_name=str(spec.source_name),
            source_kind=str(spec.source_kind),
            gain=float(spec.gain),
            sign=int(spec.sign),
            offset=float(spec.offset),
            default_value=float(spec.default_value),
            clamp_min=(None if spec.clamp_min is None else float(spec.clamp_min)),
            clamp_max=(None if spec.clamp_max is None else float(spec.clamp_max)),
            metadata=dict(spec.metadata),
        )

    @staticmethod
    def _coerce_float(value: object, *, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def resolve_value(
        self,
        module: "Module",
        *,
        context_values: Mapping[str, object],
    ) -> float:
        if self.source_kind == "input_port":
            source_value = (
                module.input_ports.get(self.source_name).current_value
                if self.source_name in module.input_ports
                else None
            )
        else:
            source_value = context_values.get(self.source_name)
        value = self.sign * self.gain * self._coerce_float(source_value, default=self.default_value) + self.offset
        if self.clamp_min is not None:
            value = max(value, self.clamp_min)
        if self.clamp_max is not None:
            value = min(value, self.clamp_max)
        return value

    def apply(
        self,
        module: "Module",
        *,
        context_values: Mapping[str, object],
    ) -> None:
        module.latent_state_by_name[self.latent_name] = self.resolve_value(
            module,
            context_values=context_values,
        )


@dataclass(slots=True)
class Module:
    id: str
    system_id: str
    subsystem_id: str
    family: str | None
    latent_updates: tuple[LatentUpdate, ...]
    parameters: dict[str, Parameter]
    input_ports: dict[str, Port]
    output_ports: dict[str, Port]
    latent_state_by_name: dict[str, float]
    controller_state_by_name: dict[str, Any]
    mode_state_by_name: dict[str, str]
    delayed_input_transfers_by_key: dict[DelayedTransferKey, DelayedTransferQueue]

    @classmethod
    def from_spec(
        cls,
        spec: ModuleSpec,
        *,
        behavior_registry: BehaviorRegistry | None = None,
    ) -> "Module":
        resolved_behavior_registry = behavior_registry or build_default_behavior_registry()
        latent_updates = tuple(LatentUpdate.from_spec(item) for item in spec.latent_update_specs)
        return cls(
            id=str(spec.module_id),
            system_id=str(spec.system_id),
            subsystem_id=str(spec.subsystem_id),
            family=spec.module_family,
            latent_updates=latent_updates,
            parameters={
                parameter_spec.parameter_name: Parameter.from_spec(
                    parameter_spec,
                    behavior_registry=resolved_behavior_registry,
                )
                for parameter_spec in spec.parameters
            },
            input_ports={
                port_spec.port_name: Port.from_spec(port_spec)
                for port_spec in spec.input_ports
            },
            output_ports={
                port_spec.port_name: Port.from_spec(port_spec)
                for port_spec in spec.output_ports
            },
            latent_state_by_name={
                latent_name: 0.0
                for latent_name in {
                    *spec.latent_variables,
                    *[latent_update.latent_name for latent_update in latent_updates],
                }
            },
            controller_state_by_name={controller_name: None for controller_name in spec.controllers},
            mode_state_by_name={state_name: "" for state_name in spec.state_machines},
            delayed_input_transfers_by_key={},
        )

    def parameter(self, parameter_name: str) -> Parameter:
        return self.parameters[str(parameter_name)]

    def input_port(self, port_name: str) -> Port:
        return self.input_ports[str(port_name)]

    def output_port(self, port_name: str) -> Port:
        return self.output_ports[str(port_name)]

    def resolve_step_inputs(
        self,
        raw_step_inputs: Mapping[str, BehaviorStepInput],
    ) -> dict[str, BehaviorStepInput]:
        resolved = {str(parameter_name): step_input for parameter_name, step_input in raw_step_inputs.items()}
        default_dt_seconds = next(iter((step_input.dt_seconds for step_input in resolved.values())), 1.0)
        for parameter_name in self.parameters:
            if parameter_name in resolved:
                continue
            resolved[parameter_name] = BehaviorStepInput(
                dt_seconds=default_dt_seconds,
                latent_state={},
                context={},
            )
        return resolved

    def hydrate_step_inputs_from_ports(
        self,
        step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
    ) -> dict[str, BehaviorStepInput]:
        hydrated: dict[str, BehaviorStepInput] = {}
        for parameter_name, step_input in step_inputs_by_parameter.items():
            parameter = self.parameter(parameter_name)
            injected_context: dict[str, Any] = {}
            for port_name in parameter.input_port_names:
                if port_name not in self.input_ports:
                    continue
                port = self.input_ports[port_name]
                if port.current_value is not None:
                    injected_context[str(port_name)] = port.current_value
            hydrated[str(parameter_name)] = BehaviorStepInput(
                dt_seconds=step_input.dt_seconds,
                latent_state=step_input.latent_state,
                context={**injected_context, **dict(step_input.context)},
            )
        return hydrated

    def _active_coupling_metadata_for_parameter(
        self,
        *,
        parameter: Parameter,
    ) -> dict[str, Any]:
        for port_name in parameter.input_port_names:
            port = self.input_ports.get(str(port_name))
            if port is None:
                continue
            metadata = dict(port.metadata or {})
            if metadata.get("coupling_id") or metadata.get("misbehavior_window_id") or metadata.get("misbehavior_family_label"):
                return metadata
        return {}

    def apply_latent_updates(
        self,
        step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
    ) -> None:
        if not self.latent_updates:
            return
        context_values: dict[str, object] = {}
        for step_input in step_inputs_by_parameter.values():
            context_values.update(dict(step_input.context))
        for latent_update in self.latent_updates:
            latent_update.apply(self, context_values=context_values)

    def hydrate_step_inputs_from_latent_state(
        self,
        step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
    ) -> dict[str, BehaviorStepInput]:
        hydrated: dict[str, BehaviorStepInput] = {}
        runtime_latent_state = dict(self.latent_state_by_name)
        for parameter_name, step_input in step_inputs_by_parameter.items():
            hydrated[str(parameter_name)] = BehaviorStepInput(
                dt_seconds=step_input.dt_seconds,
                latent_state={**runtime_latent_state, **dict(step_input.latent_state)},
                context=dict(step_input.context),
            )
        return hydrated

    def resolve_initial_state(
        self,
        initial_state_by_parameter: Mapping[str, Any],
    ) -> dict[str, Any]:
        resolved: dict[str, Any] = dict(initial_state_by_parameter)
        for parameter_name in self.parameters:
            if parameter_name in resolved:
                continue
            parameter = self.parameter(parameter_name)
            if parameter.parameter_value_clean is not None:
                resolved[parameter_name] = parameter.parameter_value_clean
            elif parameter.parameter_value is not None:
                resolved[parameter_name] = parameter.parameter_value
        return resolved

    def iter_samples(
        self,
        *,
        step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
        initial_state_by_parameter: Mapping[str, Any] | None = None,
        fault_context_by_parameter: Mapping[str, Mapping[str, Any]] | None = None,
        apply_faults: bool = True,
    ) -> Iterator[BehaviorSample]:
        resolved_initial_state_by_parameter = initial_state_by_parameter or {}
        resolved_fault_context_by_parameter = fault_context_by_parameter or {}
        for parameter_name, parameter in self.parameters.items():
            step_input = step_inputs_by_parameter.get(parameter_name)
            if step_input is None or parameter.behavior is None:
                continue
            resolved_step_input = BehaviorStepInput(
                dt_seconds=step_input.dt_seconds,
                latent_state=dict(step_input.latent_state),
                context={**dict(parameter.metadata), **dict(step_input.context)},
            )
            fault_context = dict(resolved_fault_context_by_parameter.get(parameter_name) or {})
            for sample in iter_tick_samples(
                parameter_name=parameter_name,
                generator=parameter.behavior.generator,
                step_input=resolved_step_input,
                initial_state=resolved_initial_state_by_parameter.get(parameter_name),
                violator=parameter.behavior.violator if apply_faults else None,
                violation_context=fault_context,
            ):
                metadata = dict(sample.metadata)
                misbehavior_active = bool(fault_context)
                misbehavior_applied = bool(metadata.get("misbehavior_applied", False))
                misbehavior_family_label = (
                    metadata.get("misbehavior_family_label")
                    or fault_context.get("misbehavior_family_label")
                    or fault_context.get("violation_type")
                )
                misbehavior_detail_label = (
                    metadata.get("misbehavior_detail_label")
                    or fault_context.get("misbehavior_detail_label")
                    or fault_context.get("violation_type")
                    or misbehavior_family_label
                )
                misbehavior_window_id = (
                    metadata.get("misbehavior_window_id")
                    or fault_context.get("misbehavior_window_id")
                    or fault_context.get("fault_window_id")
                    or ""
                )
                metadata.setdefault("system_id", self.system_id)
                metadata.setdefault("subsystem_id", self.subsystem_id)
                metadata.setdefault("module_id", self.id)
                metadata.setdefault("behavior_family_label", parameter.behavior_family_label)
                metadata.setdefault("parameter_datatype_label", parameter.datatype_label)
                metadata.setdefault("unit", parameter.unit)
                metadata.setdefault("rate_hz", parameter.sampling_rate_hz)
                metadata["misbehavior_active"] = misbehavior_active
                metadata["misbehavior_applied"] = misbehavior_applied
                metadata["misbehavior_family_label"] = str(misbehavior_family_label or "")
                metadata["misbehavior_detail_label"] = str(misbehavior_detail_label or "")
                metadata["misbehavior_window_id"] = str(misbehavior_window_id or "")
                metadata["event_misbehavior_label"] = str(
                    (fault_context.get("event_misbehavior_label") or "") if misbehavior_applied else ""
                )
                metadata["anomaly_type_label"] = str((fault_context.get("anomaly_type_label") or "") if misbehavior_applied else "")
                metadata["anomaly_score_label"] = (
                    fault_context.get("anomaly_score_label") if misbehavior_applied else 0.0
                )
                metadata["fault_active"] = misbehavior_active
                metadata["fault_applied"] = misbehavior_applied
                metadata["fault_family_label"] = str(
                    (
                        fault_context.get("fault_family_label")
                        or parameter.behavior_family_label
                    )
                    if fault_context
                    else ""
                )
                metadata["fault_type"] = str(
                    (
                        fault_context.get("fault_type")
                        or fault_context.get("violation_type")
                        or misbehavior_detail_label
                    )
                    if fault_context
                    else ""
                )
                metadata["fault_window_id"] = str(fault_context.get("fault_window_id") if fault_context else "")
                coupling_metadata = self._active_coupling_metadata_for_parameter(parameter=parameter)
                metadata["coupling_id_label"] = str(coupling_metadata.get("coupling_id", "") or "")
                if coupling_metadata and not metadata["misbehavior_applied"]:
                    metadata["misbehavior_active"] = bool(coupling_metadata.get("misbehavior_active", True))
                    metadata["misbehavior_applied"] = bool(coupling_metadata.get("misbehavior_active", True))
                    metadata["misbehavior_family_label"] = str(coupling_metadata.get("misbehavior_family_label", "") or "")
                    metadata["misbehavior_detail_label"] = str(coupling_metadata.get("misbehavior_detail_label", "") or "")
                    metadata["misbehavior_window_id"] = str(coupling_metadata.get("misbehavior_window_id", "") or "")
                    metadata["event_misbehavior_label"] = str(
                        coupling_metadata.get("event_misbehavior_label", "") or ""
                    )
                    metadata["anomaly_type_label"] = str(coupling_metadata.get("anomaly_type_label", "") or "")
                    metadata["anomaly_score_label"] = coupling_metadata.get("anomaly_score_label", 0.0)
                    metadata["fault_active"] = bool(coupling_metadata.get("fault_active", True))
                    metadata["fault_applied"] = bool(coupling_metadata.get("fault_active", True))
                    metadata["fault_family_label"] = str(coupling_metadata.get("fault_family_label", "") or "")
                    metadata["fault_type"] = str(coupling_metadata.get("fault_type", "") or "")
                    metadata["fault_window_id"] = str(coupling_metadata.get("fault_window_id", "") or "")
                yield BehaviorSample(
                    parameter_name=sample.parameter_name,
                    parameter_value_clean=sample.parameter_value_clean,
                    parameter_value=sample.parameter_value,
                    state=sample.state,
                    metadata=metadata,
                )

    def apply_sample(
        self,
        sample: BehaviorSample,
        *,
        timestamp_utc: datetime | None = None,
    ) -> None:
        parameter = self.parameter(sample.parameter_name)
        parameter.step(
            parameter_value=sample.parameter_value,
            parameter_value_clean=sample.parameter_value_clean,
            timestamp_utc=timestamp_utc,
        )
        output_port_name = parameter.output_port_name
        if not output_port_name or output_port_name not in self.output_ports:
            return
        port = self.output_port(output_port_name)
        port.current_value = (
            sample.parameter_value_clean if sample.parameter_value_clean is not None else sample.parameter_value
        )
        port.timestamp_utc = timestamp_utc

    def step(
        self,
        *,
        modules_by_id: Mapping[str, "Module"],
        raw_step_inputs: Mapping[str, BehaviorStepInput],
        outgoing_couplings: tuple[Coupling, ...] = (),
        initial_state_by_parameter: Mapping[str, Any] | None = None,
        fault_context_by_parameter: Mapping[str, Mapping[str, Any]] | None = None,
        coupling_misbehavior_context_by_id: Mapping[str, Mapping[str, Any]] | None = None,
        apply_faults: bool = True,
        timestamp_utc: datetime | None = None,
        current_phase_label: str | None = None,
    ) -> list[BehaviorSample]:
        resolved_step_inputs = self.resolve_step_inputs(raw_step_inputs)
        port_hydrated_step_inputs = self.hydrate_step_inputs_from_ports(resolved_step_inputs)
        self.apply_latent_updates(port_hydrated_step_inputs)
        latent_hydrated_step_inputs = self.hydrate_step_inputs_from_latent_state(port_hydrated_step_inputs)
        resolved_initial_state_by_parameter = self.resolve_initial_state(initial_state_by_parameter or {})
        samples = list(
            self.iter_samples(
                step_inputs_by_parameter=latent_hydrated_step_inputs,
                initial_state_by_parameter=resolved_initial_state_by_parameter,
                fault_context_by_parameter=fault_context_by_parameter,
                apply_faults=apply_faults,
            )
        )
        for sample in samples:
            self.apply_sample(
                sample,
                timestamp_utc=timestamp_utc,
            )
        for coupling in outgoing_couplings:
            source_module = modules_by_id.get(coupling.source_module_id)
            if source_module is None:
                raise KeyError(f"missing source module for coupling: {coupling.source_module_id}")
            target_module = modules_by_id.get(coupling.target_module_id)
            if target_module is None:
                raise KeyError(f"missing target module for coupling: {coupling.target_module_id}")
            coupling.apply(
                source_module,
                target_module,
                timestamp_utc=timestamp_utc,
                current_phase_label=current_phase_label,
                misbehavior_context=(coupling_misbehavior_context_by_id or {}).get(coupling.coupling_id, {}),
            )
        return samples
