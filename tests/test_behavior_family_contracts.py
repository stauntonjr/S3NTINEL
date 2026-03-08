from __future__ import annotations

import pandas as pd

from libs.behavior import (
    AccumulativeBehavior,
    BehaviorStepInput,
    DiscreteStateBehavior,
    InertialBehavior,
    RegulatedBehavior,
    behavior_samples_to_frame,
    build_default_behavior_registry,
)


def test_behavior_registry_contains_initial_behaviors() -> None:
    registry = build_default_behavior_registry()
    assert registry.names() == ("accumulative", "discrete_state", "inertial", "regulated")
    assert registry.get("regulated").contract.behavior_family == "regulated"
    assert registry.get("inertial").contract.behavior_family == "inertial"
    assert registry.get("accumulative").contract.behavior_family == "accumulative"
    assert registry.get("discrete_state").contract.behavior_family == "discrete_state"


def test_regulated_behavior_profiles_stable_band_signal() -> None:
    behavior = RegulatedBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0, "noise_value": noise})
        for noise in (0.0, 0.1, -0.1, 0.0, 0.05, -0.05, 0.0, 0.02)
    ]
    telemetry_pdf = behavior_samples_to_frame(
        behavior.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=step_inputs,
            initial_state=28.0,
        )
    )
    features = behavior.feature_extractor.compute_features(
        parameter_name="bus_voltage",
        telemetry_pdf=telemetry_pdf,
    )
    profile = behavior.profiler.profile(
        parameter_name="bus_voltage",
        features=features,
    )
    validation = behavior.validator.validate_stream(
        parameter_name="bus_voltage",
        generated_stream=behavior.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=step_inputs,
            initial_state=28.0,
        ),
        profile_result=profile,
    )
    assert profile.behavior_family_profiled == "regulated"
    assert profile.behavior_profile_confidence >= 0.5
    assert validation["self_classified"] is True


def test_inertial_behavior_profiles_smooth_persistent_signal() -> None:
    behavior = InertialBehavior()
    targets = (0.0, 0.3, 0.8, 1.2, 1.5, 1.7, 1.8, 1.85)
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": target, "time_constant_seconds": 1.5})
        for target in targets
    ]
    telemetry_pdf = behavior_samples_to_frame(
        behavior.generator.generate_stream(
            parameter_name="spool_speed",
            step_inputs=step_inputs,
            initial_state=0.0,
        )
    )
    features = behavior.feature_extractor.compute_features(
        parameter_name="spool_speed",
        telemetry_pdf=telemetry_pdf,
    )
    profile = behavior.profiler.profile(
        parameter_name="spool_speed",
        features=features,
    )
    expectation = behavior.expectation.evaluate(
        generated_rows=telemetry_pdf,
        profile_result=profile,
    )
    assert profile.behavior_family_profiled == "inertial"
    assert profile.behavior_profile_confidence >= 0.5
    assert expectation["self_classified"] is True


def test_regulated_violator_can_pass_stream_through_unperturbed() -> None:
    behavior = RegulatedBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0})
        for _ in range(4)
    ]
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=step_inputs,
            initial_state=28.0,
        )
    )
    violated_stream = list(
        behavior.violator.violate_stream(
            parameter_name="bus_voltage",
            generated_stream=iter(base_stream),
            context={"bias": 3.0, "anomaly_rate": 0.0, "rng_seed": 7},
        )
    )
    assert [sample.parameter_value for sample in violated_stream] == [sample.parameter_value for sample in base_stream]
    assert all(sample.metadata.get("misbehavior_applied") is False for sample in violated_stream)


def test_inertial_violator_can_apply_stream_lag() -> None:
    behavior = InertialBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": target, "time_constant_seconds": 1.0})
        for target in (0.0, 1.0, 2.0, 3.0, 4.0)
    ]
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="spool_speed",
            step_inputs=step_inputs,
            initial_state=0.0,
        )
    )
    violated_stream = list(
        behavior.violator.violate_stream(
            parameter_name="spool_speed",
            generated_stream=iter(base_stream),
            context={"lag_steps": 1, "anomaly_rate": 1.0},
        )
    )
    assert violated_stream[0].parameter_value == base_stream[0].parameter_value
    assert violated_stream[-1].parameter_value == base_stream[-2].parameter_value
    assert all(sample.metadata.get("misbehavior_applied") is True for sample in violated_stream[1:])


def test_inertial_behavior_can_use_named_latent_target() -> None:
    behavior = InertialBehavior()
    samples = list(
        behavior.generator.generate_stream(
            parameter_name="spool_speed",
            step_inputs=[
                BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={"command_state": 2.0},
                    context={"latent_target_name": "command_state", "time_constant_seconds": 1.0},
                ),
            ],
            initial_state=0.0,
        )
    )

    assert len(samples) == 1
    assert samples[0].parameter_value_clean == 2.0
    assert samples[0].metadata["target_source"] == "latent_state"


def test_regulated_behavior_can_use_named_latent_target() -> None:
    behavior = RegulatedBehavior()
    samples = list(
        behavior.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=[
                BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={"setpoint_state": 28.0},
                    context={"latent_target_name": "setpoint_state", "reversion_rate": 2.0},
                ),
            ],
            initial_state=27.0,
        )
    )

    assert len(samples) == 1
    assert samples[0].parameter_value_clean == 28.0
    assert samples[0].metadata["target_source"] == "latent_state"


def test_accumulative_behavior_profiles_monotone_integrating_signal() -> None:
    behavior = AccumulativeBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"rate_value": rate})
        for rate in (1.0, 1.0, 0.9, 1.1, 1.0, 1.05, 0.95, 1.0)
    ]
    telemetry_pdf = behavior_samples_to_frame(
        behavior.generator.generate_stream(
            parameter_name="fuel_quantity",
            step_inputs=step_inputs,
            initial_state=10.0,
        )
    )
    features = behavior.feature_extractor.compute_features(
        parameter_name="fuel_quantity",
        telemetry_pdf=telemetry_pdf,
    )
    profile = behavior.profiler.profile(
        parameter_name="fuel_quantity",
        features=features,
    )
    expectation = behavior.expectation.evaluate(
        generated_rows=telemetry_pdf,
        profile_result=profile,
    )
    assert profile.behavior_family_profiled == "accumulative"
    assert profile.behavior_profile_confidence >= 0.5
    assert expectation["self_classified"] is True


def test_discrete_state_behavior_profiles_finite_state_signal() -> None:
    behavior = DiscreteStateBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_state": state})
        for state in ("OFF", "OFF", "ON", "ON", "ON", "OFF", "OFF", "OFF")
    ]
    telemetry_pdf = behavior_samples_to_frame(
        behavior.generator.generate_stream(
            parameter_name="contactor_state",
            step_inputs=step_inputs,
            initial_state="OFF",
        )
    )
    features = behavior.feature_extractor.compute_features(
        parameter_name="contactor_state",
        telemetry_pdf=telemetry_pdf,
    )
    profile = behavior.profiler.profile(
        parameter_name="contactor_state",
        features=features,
    )
    validation = behavior.validator.validate_stream(
        parameter_name="contactor_state",
        generated_stream=behavior.generator.generate_stream(
            parameter_name="contactor_state",
            step_inputs=step_inputs,
            initial_state="OFF",
        ),
        profile_result=profile,
    )
    assert profile.behavior_family_profiled == "discrete_state"
    assert profile.behavior_profile_confidence >= 0.5
    assert validation["self_classified"] is True


def test_discrete_state_violator_can_inject_illegal_transition() -> None:
    behavior = DiscreteStateBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_state": state})
        for state in ("OFF", "OFF", "ON")
    ]
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="contactor_state",
            step_inputs=step_inputs,
            initial_state="OFF",
        )
    )
    violated_stream = list(
        behavior.violator.violate_stream(
            parameter_name="contactor_state",
            generated_stream=iter(base_stream),
            context={"violating_state": "ILLEGAL", "anomaly_rate": 1.0, "rng_seed": 3},
        )
    )
    assert all(sample.parameter_value == "ILLEGAL" for sample in violated_stream)
    assert all(sample.metadata.get("misbehavior_family_label") == "illegal_transition" for sample in violated_stream)
