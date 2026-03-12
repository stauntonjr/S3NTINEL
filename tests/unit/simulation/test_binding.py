from __future__ import annotations

import pytest

from libs.behavior import build_default_behavior_registry
from libs.simulation.parameter.runtime import Parameter
from libs.simulation.parameter.spec import ParameterSpec


def test_parameter_from_spec_resolves_behavior_from_registry():
    registry = build_default_behavior_registry()
    parameter = Parameter.from_spec(
        ParameterSpec(
            parameter_name="p_num",
            system_id="sys_a",
            subsystem_id="sub_a",
            module_id="mod_a",
            parameter_datatype_label="numeric",
            behavior_family_label="regulated",
        ),
        behavior_registry=registry,
    )

    assert parameter.behavior is registry.get("regulated")


def test_parameter_from_spec_rejects_unknown_behavior_family():
    registry = build_default_behavior_registry()
    with pytest.raises(ValueError, match="unknown behavior_family_label"):
        Parameter.from_spec(
            ParameterSpec(
                parameter_name="p_num",
                system_id="sys_a",
                subsystem_id="sub_a",
                module_id="mod_a",
                parameter_datatype_label="numeric",
                behavior_family_label="missing_family",
            ),
            behavior_registry=registry,
        )


def test_parameter_from_spec_allows_unbound_static_parameter():
    parameter = Parameter.from_spec(
        ParameterSpec(
            parameter_name="p_num",
            system_id="sys_a",
            subsystem_id="sub_a",
            module_id="mod_a",
            parameter_datatype_label="numeric",
            behavior_family_label=None,
        )
    )

    assert parameter.behavior is None
