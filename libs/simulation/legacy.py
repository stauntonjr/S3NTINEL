"""Compatibility helpers for translating legacy hierarchy inputs into native specs."""

from __future__ import annotations

from typing import Any

from libs.common import SensorDataType, normalize_sensor_datatype
from libs.simulation.specs import ModuleSpec, ParameterSpec


def default_behavior_family_for_datatype(parameter_datatype_label: object) -> str | None:
    dtype = normalize_sensor_datatype(parameter_datatype_label)
    if dtype == SensorDataType.NUMERIC.value:
        return "inertial"
    if dtype in {
        SensorDataType.BINARY.value,
        SensorDataType.CATEGORICAL.value,
        SensorDataType.HIGH_CARDINALITY.value,
    }:
        return "discrete_state"
    if dtype == SensorDataType.CONSTANT.value:
        return "regulated"
    return None


def parameter_spec_from_legacy_sensor(
    *,
    system_id: object,
    subsystem_id: object,
    module_id: object,
    sensor_obj: dict[str, Any],
) -> ParameterSpec:
    parameter_name = str(sensor_obj.get("parameter_name") or sensor_obj.get("sensor") or "").strip()
    if not parameter_name:
        raise ValueError("legacy sensor specification must include 'parameter_name' or 'sensor'")
    datatype = normalize_sensor_datatype(sensor_obj.get("parameter_datatype_label") or sensor_obj.get("datatype"))
    behavior_family_label = sensor_obj.get("behavior_family_label")
    if behavior_family_label is not None:
        behavior_family_label = str(behavior_family_label)
    else:
        behavior_family_label = default_behavior_family_for_datatype(datatype)

    sampling_rate = sensor_obj.get("sampling_rate_hz")
    sampling_rate_hz = float(sampling_rate) if sampling_rate is not None else None
    noise_scale = float(sensor_obj.get("noise_scale", 0.0) or 0.0)
    quantization_value = sensor_obj.get("quantization")
    quantization = float(quantization_value) if quantization_value is not None else None
    allowed_fault_families = tuple(str(item) for item in sensor_obj.get("allowed_fault_families", ()) or ())
    input_port_names = tuple(str(item) for item in sensor_obj.get("input_port_names", ()) or ())
    output_port_name = (
        str(sensor_obj["output_port_name"]) if sensor_obj.get("output_port_name") is not None else None
    )

    metadata = {
        key: value
        for key, value in sensor_obj.items()
        if key
        not in {
            "parameter_name",
            "sensor",
            "parameter_datatype_label",
            "datatype",
            "unit",
            "behavior_family_label",
            "latent_group",
            "sampling_rate_hz",
            "noise_scale",
            "quantization",
            "delay_class",
            "phase_envelope_id",
            "allowed_fault_families",
            "input_port_names",
            "output_port_name",
        }
    }

    return ParameterSpec(
        parameter_name=parameter_name,
        system_id=str(system_id),
        subsystem_id=str(subsystem_id),
        module_id=str(module_id),
        parameter_datatype_label=datatype,
        unit=str(sensor_obj.get("unit", "")),
        behavior_family_label=behavior_family_label,
        latent_group=(str(sensor_obj["latent_group"]) if sensor_obj.get("latent_group") is not None else None),
        sampling_rate_hz=sampling_rate_hz,
        noise_scale=noise_scale,
        quantization=quantization,
        delay_class=(
            str(sensor_obj["delay_class"]) if sensor_obj.get("delay_class") is not None else None
        ),
        phase_envelope_id=(
            str(sensor_obj["phase_envelope_id"]) if sensor_obj.get("phase_envelope_id") is not None else None
        ),
        allowed_fault_families=allowed_fault_families,
        input_port_names=input_port_names,
        output_port_name=output_port_name,
        metadata=metadata,
    )


def module_specs_from_hierarchy_spec(hierarchy_spec: dict[str, Any]) -> tuple[ModuleSpec, ...]:
    module_specs: list[ModuleSpec] = []
    for system_id, system_obj in (hierarchy_spec.get("systems") or {}).items():
        for subsystem_id, subsystem_obj in (system_obj.get("subsystems") or {}).items():
            for module_id, module_sensors in (subsystem_obj.get("modules") or {}).items():
                parameters = tuple(
                    parameter_spec_from_legacy_sensor(
                        system_id=system_id,
                        subsystem_id=subsystem_id,
                        module_id=module_id,
                        sensor_obj=sensor_obj,
                    )
                    for sensor_obj in (module_sensors or [])
                )
                module_specs.append(
                    ModuleSpec(
                        module_id=str(module_id),
                        subsystem_id=str(subsystem_id),
                        system_id=str(system_id),
                        parameters=parameters,
                    )
                )
    return tuple(module_specs)
