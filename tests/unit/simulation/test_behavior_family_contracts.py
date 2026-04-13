from __future__ import annotations

import pandas as pd
import pytest

from libs.behavior import (
    AccumulativeBehavior,
    BehaviorStepInput,
    DiscreteStateBehavior,
    InertialBehavior,
    RegulatedBehavior,
    TrackingBehavior,
    behavior_samples_to_frame,
    build_default_behavior_registry,
    iter_tick_samples,
)


def test_behavior_registry_contains_initial_behaviors() -> None:
    registry = build_default_behavior_registry()
    assert registry.names() == ("accumulative", "discrete_state", "inertial", "regulated", "tracking")
    assert registry.get("regulated").contract.behavior_family == "regulated"
    assert registry.get("inertial").contract.behavior_family == "inertial"
    assert registry.get("accumulative").contract.behavior_family == "accumulative"
    assert registry.get("discrete_state").contract.behavior_family == "discrete_state"
    assert registry.get("tracking").contract.behavior_family == "tracking"


def test_regulated_behavior_profiles_stable_band_signal() -> None:
    behavior = RegulatedBehavior()
    assert "center_occupancy" in behavior.contract.defining_primitives
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
    assert features["bound_occupancy_profiled"] is not None
    assert profile.score_by_family["regulated"] > profile.score_by_family["inertial"]
    assert profile.score_by_family["regulated"] > profile.score_by_family["accumulative"]
    assert profile.behavior_family_profiled in {"regulated", "tracking", "mixed_unknown"}
    assert profile.behavior_profile_confidence >= 0.4


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
    assert features["lagged_response_score_profiled"] is not None
    assert profile.score_by_family["inertial"] > profile.score_by_family["regulated"]
    assert profile.behavior_family_profiled in {"inertial", "accumulative", "mixed_unknown"}
    assert profile.behavior_profile_confidence >= 0.5
    assert profile.score_by_family["inertial"] >= 0.4


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


def test_regulated_generator_exhibits_more_closed_loop_correction_than_inertial() -> None:
    regulated = RegulatedBehavior()
    inertial = InertialBehavior()
    step_inputs = [
        BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": target, "reversion_rate": 1.5})
        for target in (28.0, 28.0, 28.0, 28.0, 28.0, 28.0, 28.0, 28.0)
    ]
    regulated_pdf = behavior_samples_to_frame(
        regulated.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=step_inputs,
            initial_state=24.0,
        )
    )
    inertial_pdf = behavior_samples_to_frame(
        inertial.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=[
                BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0, "time_constant_seconds": 1.5})
                for _ in range(len(step_inputs))
            ],
            initial_state=24.0,
        )
    )

    regulated_features = regulated.feature_extractor.compute_features(
        parameter_name="bus_voltage",
        telemetry_pdf=regulated_pdf,
    )
    inertial_features = inertial.feature_extractor.compute_features(
        parameter_name="bus_voltage",
        telemetry_pdf=inertial_pdf,
    )

    assert regulated_features["mean_reversion_score_profiled"] > 0.0
    assert regulated_features["mean_reversion_score_profiled"] > float(inertial_features["sign_flip_rate_profiled"] or 0.0)
    assert regulated_pdf["parameter_value_clean"].iloc[-1] == pytest.approx(28.0, abs=0.5)


def test_accumulative_behavior_profiles_monotone_integrating_signal() -> None:
    behavior = AccumulativeBehavior()
    assert "monotone_accumulation" in behavior.contract.defining_primitives
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


def test_tracking_behavior_profiles_target_following_signal() -> None:
    behavior = TrackingBehavior()
    step_inputs = [
        BehaviorStepInput(
            dt_seconds=1.0,
            latent_state={},
            context={"target_value": target, "response_rate": 2.0, "bound_min": 0.0, "bound_max": 100.0},
        )
        for target in (0.0, 0.0, 20.0, 40.0, 60.0, 45.0, 30.0, 30.0)
    ]
    telemetry_pdf = behavior_samples_to_frame(
        behavior.generator.generate_stream(
            parameter_name="outflow_cmd_pct",
            step_inputs=step_inputs,
            initial_state=0.0,
        )
    )
    features = behavior.feature_extractor.compute_features(
        parameter_name="outflow_cmd_pct",
        telemetry_pdf=telemetry_pdf,
    )
    profile = behavior.profiler.profile(
        parameter_name="outflow_cmd_pct",
        features=features,
    )
    assert "tracking_error" in behavior.contract.defining_primitives
    assert features["tracking_recovery_score_profiled"] is not None
    assert profile.score_by_family["tracking"] >= profile.score_by_family["inertial"]
    assert profile.behavior_family_profiled in {"tracking", "regulated", "mixed_unknown"}


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


def test_discrete_state_chatter_survives_single_tick_runtime_calls() -> None:
    behavior = DiscreteStateBehavior()
    values = []
    for step_index in range(4):
        samples = list(
            iter_tick_samples(
                parameter_name="pack_mode_state",
                generator=behavior.generator,
                step_input=BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_state": "LOW", "step_index": step_index},
                ),
                initial_state="LOW",
                violator=behavior.violator,
                violation_context={
                    "violation_type": "state_chatter",
                    "chatter_states": ("LOW", "OFF"),
                    "anomaly_rate": 1.0,
                    "step_index": step_index,
                },
            )
        )
        assert len(samples) == 1
        values.append(samples[0].parameter_value)
    assert values == ["LOW", "OFF", "LOW", "OFF"]


def test_regulated_violator_supports_all_declared_fault_types() -> None:
    behavior = RegulatedBehavior()
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="bus_voltage",
            step_inputs=[BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0}) for _ in range(4)],
            initial_state=28.0,
        )
    )

    for context in (
        {"violation_type": "offset", "offset_value": 3.0, "anomaly_rate": 1.0},
        {"violation_type": "saturation", "saturation_max": 20.0, "anomaly_rate": 1.0},
        {"violation_type": "tracking_degradation", "tracking_scale": 0.5, "anomaly_rate": 1.0},
        {"violation_type": "oscillation", "oscillation_amplitude": 1.5, "oscillation_period_steps": 2, "anomaly_rate": 1.0},
    ):
        violated_stream = list(
            behavior.violator.violate_stream(
                parameter_name="bus_voltage",
                generated_stream=iter(base_stream),
                context=context,
            )
        )
        assert all(sample.metadata.get("misbehavior_family_label") == context["violation_type"] for sample in violated_stream)


def test_inertial_violator_supports_all_declared_fault_types() -> None:
    behavior = InertialBehavior()
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="spool_speed",
            step_inputs=[
                BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": target, "time_constant_seconds": 1.0})
                for target in (0.0, 1.0, 2.0, 3.0, 4.0)
            ],
            initial_state=0.0,
        )
    )

    for context in (
        {"violation_type": "timing_lag", "lag_steps": 1, "anomaly_rate": 1.0},
        {"violation_type": "increased_time_constant", "slowdown_factor": 4.0, "anomaly_rate": 1.0},
        {"violation_type": "stuck_value", "stuck_value": 1.5, "anomaly_rate": 1.0},
        {"violation_type": "ramp_distortion", "slope_scale": 0.4, "anomaly_rate": 1.0},
    ):
        violated_stream = list(
            behavior.violator.violate_stream(
                parameter_name="spool_speed",
                generated_stream=iter(base_stream),
                context=context,
            )
        )
        assert any(sample.metadata.get("misbehavior_applied") is True for sample in violated_stream)
        assert any(sample.metadata.get("misbehavior_family_label") == context["violation_type"] for sample in violated_stream)


def test_accumulative_violator_supports_all_declared_fault_types() -> None:
    behavior = AccumulativeBehavior()
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="fuel_quantity_total",
            step_inputs=[BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"rate_value": 1.0}) for _ in range(4)],
            initial_state=0.0,
        )
    )

    for context in (
        {"violation_type": "reset_drop", "drop_value": 2.0, "anomaly_rate": 1.0},
        {"violation_type": "leak_rate", "leak_rate": 0.2, "anomaly_rate": 1.0},
        {"violation_type": "drift", "drift_rate": 0.5, "anomaly_rate": 1.0},
        {"violation_type": "bias", "bias": 1.0, "anomaly_rate": 1.0},
    ):
        violated_stream = list(
            behavior.violator.violate_stream(
                parameter_name="fuel_quantity_total",
                generated_stream=iter(base_stream),
                context=context,
            )
        )
        assert all(sample.metadata.get("misbehavior_family_label") == context["violation_type"] for sample in violated_stream)


def test_discrete_state_violator_supports_all_declared_fault_types() -> None:
    behavior = DiscreteStateBehavior()
    base_stream = list(
        behavior.generator.generate_stream(
            parameter_name="contactor_state",
            step_inputs=[
                BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_state": state})
                for state in ("OFF", "ON", "ON", "OFF")
            ],
            initial_state="OFF",
        )
    )

    for context in (
        {"violation_type": "illegal_transition", "violating_state": "ILLEGAL", "anomaly_rate": 1.0},
        {"violation_type": "dwell_violation", "extra_dwell_steps": 2, "anomaly_rate": 1.0},
        {"violation_type": "state_chatter", "chatter_states": ("ON", "OFF"), "anomaly_rate": 1.0},
        {"violation_type": "stuck_state", "stuck_state": "ON", "anomaly_rate": 1.0},
    ):
        violated_stream = list(
            behavior.violator.violate_stream(
                parameter_name="contactor_state",
                generated_stream=iter(base_stream),
                context=context,
            )
        )
        assert any(sample.metadata.get("misbehavior_family_label") == context["violation_type"] for sample in violated_stream)
