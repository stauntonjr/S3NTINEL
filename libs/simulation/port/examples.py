"""Example port specs."""

from __future__ import annotations

from libs.simulation.port.spec import PortSpec


def build_numeric_input_port_spec(*, port_name: str, unit: str = "", description: str = "") -> PortSpec:
    return PortSpec.input(
        port_name=port_name,
        value_datatype_label="numeric",
        unit=unit,
        description=description,
    )


def build_numeric_output_port_spec(*, port_name: str, unit: str = "", description: str = "") -> PortSpec:
    return PortSpec.output(
        port_name=port_name,
        value_datatype_label="numeric",
        unit=unit,
        description=description,
    )


def build_categorical_input_port_spec(*, port_name: str, unit: str = "state", description: str = "") -> PortSpec:
    return PortSpec.input(
        port_name=port_name,
        value_datatype_label="categorical",
        unit=unit,
        description=description,
    )


def build_categorical_output_port_spec(*, port_name: str, unit: str = "state", description: str = "") -> PortSpec:
    return PortSpec.output(
        port_name=port_name,
        value_datatype_label="categorical",
        unit=unit,
        description=description,
    )
