import copy
import numpy as np
import pandas as pd

from libs.simulation import (
    build_default_parameter_behavior,
    build_fleet_manifest,
    build_mermaid_hierarchy,
    build_subsystem_slice_hierarchy_df,
    build_tail_profiles,
    build_native_multibehavior_example,
    default_phase_definitions,
    flatten_assembly_spec,
    flatten_hierarchy_spec,
    simulate_fleet_dataset,
    simulate_fleet_dataset_from_assembly,
    simulate_fleet_dataset_from_subsystem_slice,
    simulate_fleet_dataset_spark,
    simulate_fleet_dataset_spark_from_assembly,
)


def _sample_hierarchy_spec() -> dict:
    return {
        "systems": {
            "SYS_A": {
                "subsystems": {
                    "SUB_A": {
                        "modules": {
                            "MOD_A": [
                                {"sensor": "s_num", "datatype": "numeric", "unit": "u"},
                                {"sensor": "s_num_2", "datatype": "numeric", "unit": "u"},
                                {"sensor": "s_bin", "datatype": "binary", "unit": "u"},
                            ]
                        }
                    }
                }
            }
        }
    }


def test_flatten_and_mermaid_generation():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    assert len(hierarchy_df) == 3
    assert set(hierarchy_df.columns) >= {"system_id", "subsystem_id", "module_id", "sensor", "parameter_datatype"}

    mermaid = build_mermaid_hierarchy(hierarchy_df, max_sensors=10)
    assert "flowchart TD" in mermaid
    assert "s_num" in mermaid


def test_flatten_assembly_spec_native_example():
    assembly_spec = build_native_multibehavior_example()
    hierarchy_df = flatten_assembly_spec(assembly_spec)

    assert not hierarchy_df.empty
    assert set(hierarchy_df.columns) >= {
        "system_id",
        "subsystem_id",
        "module_id",
        "sensor",
        "parameter_name",
        "parameter_datatype",
        "behavior_family_label",
    }
    assert (hierarchy_df["sensor"] == hierarchy_df["parameter_name"]).all()
    assert set(hierarchy_df["behavior_family_label"].astype(str)) >= {
        "regulated",
        "inertial",
        "accumulative",
        "discrete_state",
    }
    assert set(hierarchy_df.columns) >= {
        "incoming_coupling_count",
        "outgoing_coupling_count",
        "incoming_relation_types",
        "outgoing_relation_types",
        "upstream_module_ids",
        "downstream_module_ids",
    }

    by_parameter = hierarchy_df.set_index("parameter_name")
    assert int(by_parameter.loc["contactor_state", "outgoing_coupling_count"]) == 1
    assert by_parameter.loc["contactor_state", "downstream_module_ids"] == ["MOD_SOURCE"]
    assert int(by_parameter.loc["supply_voltage", "incoming_coupling_count"]) == 1
    assert int(by_parameter.loc["supply_voltage", "outgoing_coupling_count"]) == 1
    assert by_parameter.loc["supply_voltage", "incoming_relation_types"] == ["drive"]
    assert by_parameter.loc["motor_speed", "upstream_module_ids"] == ["MOD_SOURCE"]
    assert int(by_parameter.loc["fuel_used_total", "incoming_coupling_count"]) == 1


def test_build_default_parameter_behavior_uses_coupling_summaries():
    assembly_spec = build_native_multibehavior_example()
    hierarchy_df = flatten_assembly_spec(assembly_spec)

    behavior = build_default_parameter_behavior(hierarchy_df)

    supply_voltage = behavior["supply_voltage"]
    contactor_state = behavior["contactor_state"]
    motor_speed = behavior["motor_speed"]

    assert supply_voltage["parameter_name"] == "supply_voltage"
    assert supply_voltage["incoming_coupling_count"] == 1
    assert supply_voltage["outgoing_coupling_count"] == 1
    assert supply_voltage["incoming_relation_types"] == ["drive"]
    assert supply_voltage["corr_scale"] > 0.4
    assert supply_voltage["noise_sigma"] < 0.1

    assert contactor_state["downstream_module_ids"] == ["MOD_SOURCE"]
    assert contactor_state["latent_gain"] > 0.9
    assert contactor_state["persistence"] > 0.985

    assert motor_speed["upstream_module_ids"] == ["MOD_SOURCE"]
    assert motor_speed["corr_scale"] > 0.4


def test_build_default_parameter_behavior_uses_parameter_behavior_profile_when_confident():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior_base = build_default_parameter_behavior(hierarchy_df)
    behavior_profile_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "s_num",
                "behavior_family_profiled": "regulated",
                "behavior_profile_confidence": 0.92,
            },
            {
                "parameter_name": "s_num_2",
                "behavior_family_profiled": "accumulative",
                "behavior_profile_confidence": 0.89,
            },
            {
                "parameter_name": "s_bin",
                "behavior_family_profiled": "discrete_state",
                "behavior_profile_confidence": 0.95,
            },
        ]
    )

    behavior_profiled = build_default_parameter_behavior(
        hierarchy_df,
        parameter_behavior_profile_df=behavior_profile_df,
    )

    assert behavior_profiled["s_num"]["resolved_behavior_family"] == "regulated"
    assert behavior_profiled["s_num"]["behavior_family_profiled"] == "regulated"
    assert behavior_profiled["s_num"]["corr_scale"] < behavior_base["s_num"]["corr_scale"]
    assert behavior_profiled["s_num"]["noise_sigma"] < behavior_base["s_num"]["noise_sigma"]

    assert behavior_profiled["s_num_2"]["resolved_behavior_family"] == "accumulative"
    assert behavior_profiled["s_num_2"]["osc_amp"] < behavior_base["s_num_2"]["osc_amp"]
    assert behavior_profiled["s_num_2"]["noise_sigma"] < behavior_base["s_num_2"]["noise_sigma"]

    assert behavior_profiled["s_bin"]["resolved_behavior_family"] == "discrete_state"
    assert behavior_profiled["s_bin"]["behavior_family_profiled"] == "discrete_state"
    assert behavior_profiled["s_bin"]["behavior_profile_confidence"] == 0.95


def test_build_default_parameter_behavior_uses_continuous_scaling_profile_when_available():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior_base = build_default_parameter_behavior(hierarchy_df)
    scaling_profile_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "s_num",
                "scaling_center_median": 42.0,
                "scaling_iqr": 8.0,
            },
            {
                "parameter_name": "s_num_2",
                "scaling_center_median": -3.0,
                "scaling_iqr": 0.5,
            },
        ]
    )

    behavior_scaled = build_default_parameter_behavior(
        hierarchy_df,
        continuous_scaling_profile_df=scaling_profile_df,
    )

    assert behavior_scaled["s_num"]["baseline"] == 42.0
    assert behavior_scaled["s_num"]["scaling_center_median"] == 42.0
    assert behavior_scaled["s_num"]["scaling_iqr"] == 8.0
    assert behavior_scaled["s_num"]["osc_amp"] > behavior_base["s_num"]["osc_amp"]
    assert behavior_scaled["s_num"]["noise_sigma"] > behavior_base["s_num"]["noise_sigma"]

    assert behavior_scaled["s_num_2"]["baseline"] == -3.0
    assert behavior_scaled["s_num_2"]["scaling_iqr"] == 0.5


def test_simulation_dataset_shapes():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior = build_default_parameter_behavior(hierarchy_df)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }
    rng = np.random.default_rng(7)
    tails = build_tail_profiles(["SYS_A"], m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_df, phase_labels_df = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert not telemetry_df.empty
    assert not phase_labels_df.empty
    assert set(["tail_id", "flight_id", "sensor", "phase_name"]).issubset(telemetry_df.columns)
    assert "timestamp_utc" in phase_labels_df.columns
    assert "anomaly_type_label" in telemetry_df.columns
    assert "anomaly_score_label" in telemetry_df.columns
    assert "event_type_label" in telemetry_df.columns


def test_simulation_dataset_from_native_assembly_shapes():
    assembly_spec = build_native_multibehavior_example()
    hierarchy_df = flatten_assembly_spec(assembly_spec)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }
    rng = np.random.default_rng(11)
    tails = build_tail_profiles(sorted(set(hierarchy_df["system_id"].astype(str))), m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_df, phase_labels_df = simulate_fleet_dataset_from_assembly(
        assembly_spec=assembly_spec,
        parameter_behavior=build_default_parameter_behavior(hierarchy_df),
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert not telemetry_df.empty
    assert not phase_labels_df.empty
    assert set(["tail_id", "flight_id", "sensor", "parameter_name"]).issubset(telemetry_df.columns)
    assert set(telemetry_df["sensor"].astype(str)) == set(hierarchy_df["sensor"].astype(str))


def test_build_subsystem_slice_hierarchy_df_for_pressurization():
    hierarchy_df = build_subsystem_slice_hierarchy_df("pressurization")

    assert not hierarchy_df.empty
    assert set(hierarchy_df["module_id"].astype(str)) >= {
        "MOD_PRESS_MODE",
        "MOD_AIRCRAFT_ALT",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
    }
    assert "incoming_coupling_count" in hierarchy_df.columns
    assert "outgoing_coupling_count" in hierarchy_df.columns


def test_simulation_dataset_from_named_subsystem_slice_shapes():
    hierarchy_df = build_subsystem_slice_hierarchy_df("pressurization")
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }
    rng = np.random.default_rng(19)
    tails = build_tail_profiles(sorted(set(hierarchy_df["system_id"].astype(str))), m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_df, phase_labels_df = simulate_fleet_dataset_from_subsystem_slice(
        slice_name="pressurization",
        parameter_behavior=build_default_parameter_behavior(hierarchy_df),
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert not telemetry_df.empty
    assert not phase_labels_df.empty
    assert set(["tail_id", "flight_id", "sensor", "parameter_name"]).issubset(telemetry_df.columns)
    assert set(telemetry_df["sensor"].astype(str)).issubset(set(hierarchy_df["sensor"].astype(str)))


def test_simulation_dataset_from_assembly_uses_parameter_behavior_profile_when_parameter_behavior_omitted():
    assembly_spec = build_native_multibehavior_example()
    hierarchy_df = flatten_assembly_spec(assembly_spec)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }
    rng = np.random.default_rng(23)
    tails = build_tail_profiles(sorted(set(hierarchy_df["system_id"].astype(str))), m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)
    behavior_profile_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "fuel_used_total",
                "behavior_family_profiled": "accumulative",
                "behavior_profile_confidence": 0.91,
            }
        ]
    )

    telemetry_df, phase_labels_df = simulate_fleet_dataset_from_assembly(
        assembly_spec=assembly_spec,
        parameter_behavior_profile_df=behavior_profile_df,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert not telemetry_df.empty
    assert not phase_labels_df.empty


def test_simulation_dataset_from_assembly_accepts_behavior_and_scaling_profiles_when_parameter_behavior_omitted():
    assembly_spec = build_native_multibehavior_example()
    hierarchy_df = flatten_assembly_spec(assembly_spec)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }
    rng = np.random.default_rng(29)
    tails = build_tail_profiles(sorted(set(hierarchy_df["system_id"].astype(str))), m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)
    behavior_profile_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "fuel_used_total",
                "behavior_family_profiled": "accumulative",
                "behavior_profile_confidence": 0.91,
            }
        ]
    )
    scaling_profile_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "fuel_used_total",
                "scaling_center_median": 10.0,
                "scaling_iqr": 4.0,
            }
        ]
    )

    telemetry_df, phase_labels_df = simulate_fleet_dataset_from_assembly(
        assembly_spec=assembly_spec,
        parameter_behavior_profile_df=behavior_profile_df,
        continuous_scaling_profile_df=scaling_profile_df,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert not telemetry_df.empty
    assert not phase_labels_df.empty


def test_causal_delay_changes_series_with_same_seed():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior = build_default_parameter_behavior(hierarchy_df)
    phases = default_phase_definitions()[:2]
    base_flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }

    rng = np.random.default_rng(17)
    tails = build_tail_profiles(["SYS_A"], m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    no_delay_df, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=base_flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    with_delay_setup = {
        **base_flight_setup,
        "causal_delay": {
            "per_corr_group_sec": {"SYS_A::SUB_A": 3},
            "startup_fill": "hold_first",
        },
    }
    delayed_df, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=with_delay_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    no_delay_series = (
        no_delay_df.loc[no_delay_df["sensor"] == "s_num", ["timestamp_utc", "parameter_value"]]
        .sort_values("timestamp_utc")
        .pipe(lambda df: pd.to_numeric(df["parameter_value"], errors="coerce"))
        .to_numpy()
    )
    delayed_series = (
        delayed_df.loc[delayed_df["sensor"] == "s_num", ["timestamp_utc", "parameter_value"]]
        .sort_values("timestamp_utc")
        .pipe(lambda df: pd.to_numeric(df["parameter_value"], errors="coerce"))
        .to_numpy()
    )

    assert len(no_delay_series) == len(delayed_series)
    assert not np.allclose(no_delay_series, delayed_series)


def _best_lag(x: np.ndarray, y: np.ndarray, max_lag: int = 60) -> int:
    x_series = np.asarray(x, dtype=float)
    y_series = np.asarray(y, dtype=float)
    best = 0
    best_abs_corr = -1.0
    for lag in range(-max_lag, max_lag + 1):
        shifted = np.roll(y_series, lag)
        if lag > 0:
            shifted[:lag] = np.nan
        elif lag < 0:
            shifted[lag:] = np.nan
        mask = np.isfinite(x_series) & np.isfinite(shifted)
        if mask.sum() < 40:
            continue
        xv = x_series[mask]
        yv = shifted[mask]
        xv = xv - float(np.mean(xv))
        yv = yv - float(np.mean(yv))
        denom = float(np.sqrt(np.sum(xv * xv) * np.sum(yv * yv)))
        corr = float(np.sum(xv * yv) / denom) if denom > 0.0 else float("nan")
        if np.isfinite(corr) and abs(corr) > best_abs_corr:
            best_abs_corr = abs(corr)
            best = lag
    return int(best)


def test_random_pair_delay_yields_nonzero_lag_and_reproducibility():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior = build_default_parameter_behavior(hierarchy_df)
    for sensor_name in ["s_num", "s_num_2"]:
        behavior[sensor_name]["corr_scale"] = 3.0
        behavior[sensor_name]["noise_sigma"] = 0.01
        behavior[sensor_name]["osc_amp"] = 0.0
        behavior[sensor_name]["trend_per_sec"] = 0.0

    phases = default_phase_definitions()[:2]
    base_flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "sample_period_sec": 1.0,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
        "causal_delay": {
            "mode": "random_pair",
            "default_lag_sec": 0.0,
            "random_pair_delay_sec": {"min": 0.0, "max": 8.0},
            "jitter_sec_std": 0.0,
            "seed_offset": 123,
        },
    }

    rng = np.random.default_rng(23)
    tails = build_tail_profiles(["SYS_A"], m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_a, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=base_flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    telemetry_b, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=base_flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    a_left = telemetry_a.loc[telemetry_a["sensor"] == "s_num", ["timestamp_utc", "parameter_value"]].rename(columns={"parameter_value": "x"})
    a_left["x"] = pd.to_numeric(a_left["x"], errors="coerce")
    a_right = telemetry_a.loc[telemetry_a["sensor"] == "s_num_2", ["timestamp_utc", "parameter_value"]].rename(columns={"parameter_value": "y"})
    a_right["y"] = pd.to_numeric(a_right["y"], errors="coerce")
    merged_a = a_left.merge(a_right, on="timestamp_utc", how="inner").sort_values("timestamp_utc")

    b_left = telemetry_b.loc[telemetry_b["sensor"] == "s_num", ["timestamp_utc", "parameter_value"]].rename(columns={"parameter_value": "x"})
    b_left["x"] = pd.to_numeric(b_left["x"], errors="coerce")
    b_right = telemetry_b.loc[telemetry_b["sensor"] == "s_num_2", ["timestamp_utc", "parameter_value"]].rename(columns={"parameter_value": "y"})
    b_right["y"] = pd.to_numeric(b_right["y"], errors="coerce")
    merged_b = b_left.merge(b_right, on="timestamp_utc", how="inner").sort_values("timestamp_utc")

    lag_a = _best_lag(merged_a["x"].to_numpy(), merged_a["y"].to_numpy(), max_lag=60)
    lag_b = _best_lag(merged_b["x"].to_numpy(), merged_b["y"].to_numpy(), max_lag=60)

    assert lag_a != 0
    assert lag_b == lag_a


def test_event_labels_invariant_to_noise_scale_changes():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior_base = build_default_parameter_behavior(hierarchy_df)
    behavior_low_noise = copy.deepcopy(behavior_base)
    behavior_high_noise = copy.deepcopy(behavior_base)

    for sensor_name, spec in behavior_low_noise.items():
        if str(spec.get("datatype")) == "numeric":
            behavior_low_noise[sensor_name]["noise_sigma"] = 0.001
    for sensor_name, spec in behavior_high_noise.items():
        if str(spec.get("datatype")) == "numeric":
            behavior_high_noise[sensor_name]["noise_sigma"] = 1.0

    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }

    rng = np.random.default_rng(41)
    tails = build_tail_profiles(["SYS_A"], m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_low, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior_low_noise,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    telemetry_high, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior_high_noise,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    key_cols = ["tail_id", "flight_id", "timestamp_utc", "sensor"]
    compare_cols = [
        "event_type_label",
        "parameter_value",
        "parameter_value_clean",
    ]

    low_numeric = telemetry_low.loc[telemetry_low["parameter_datatype"] == "numeric", key_cols + compare_cols]
    high_numeric = telemetry_high.loc[telemetry_high["parameter_datatype"] == "numeric", key_cols + compare_cols]

    merged = low_numeric.merge(high_numeric, on=key_cols, suffixes=("_low", "_high"))
    assert not merged.empty

    assert (merged["event_type_label_low"] == merged["event_type_label_high"]).all()

    # Clean signal should be identical while observed values should differ under noise changes.
    low_clean = pd.to_numeric(merged["parameter_value_clean_low"], errors="coerce").to_numpy()
    high_clean = pd.to_numeric(merged["parameter_value_clean_high"], errors="coerce").to_numpy()
    low_obs = pd.to_numeric(merged["parameter_value_low"], errors="coerce").to_numpy()
    high_obs = pd.to_numeric(merged["parameter_value_high"], errors="coerce").to_numpy()
    assert np.allclose(low_clean, high_clean, equal_nan=True)
    assert (np.abs(low_obs - high_obs) > 1e-9).any()


def test_simulation_dataset_spark_schema_and_row_count_parity(spark):
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior = build_default_parameter_behavior(hierarchy_df)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }

    rng = np.random.default_rng(101)
    tails = build_tail_profiles(["SYS_A"], m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_pd, phase_pd = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    telemetry_spark, phase_spark = simulate_fleet_dataset_spark(
        spark=spark,
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert telemetry_spark.count() == len(telemetry_pd)
    assert phase_spark.count() == len(phase_pd)

    assert telemetry_spark.columns == [
        "tail_id",
        "flight_id",
        "timestamp_utc",
        "system_id",
        "subsystem_id",
        "module_id",
        "sensor",
        "parameter_name",
        "parameter_datatype",
        "parameter_value",
        "phase_id_detected",
        "phase_name",
        "anomaly_type_label",
        "anomaly_score_label",
        "event_type_label",
        "date_utc",
    ]
    assert phase_spark.columns == ["tail_id", "flight_id", "timestamp_utc", "phase_id_detected", "phase_name"]


def test_simulation_dataset_spark_from_native_assembly_parity(spark):
    assembly_spec = build_native_multibehavior_example()
    hierarchy_df = flatten_assembly_spec(assembly_spec)
    behavior = build_default_parameter_behavior(hierarchy_df)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }
    rng = np.random.default_rng(103)
    tails = build_tail_profiles(sorted(set(hierarchy_df["system_id"].astype(str))), m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_pd, phase_pd = simulate_fleet_dataset_from_assembly(
        assembly_spec=assembly_spec,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    telemetry_spark, phase_spark = simulate_fleet_dataset_spark_from_assembly(
        spark=spark,
        assembly_spec=assembly_spec,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    assert telemetry_spark.count() == len(telemetry_pd)
    assert phase_spark.count() == len(phase_pd)


def test_simulation_dataset_spark_value_parity_with_pandas(spark):
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior = build_default_parameter_behavior(hierarchy_df)
    phases = default_phase_definitions()[:2]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "anomaly_plan": {
            "base_event_rate_per_min": 0.0,
            "burst_phases": [],
            "burst_multiplier": 1.0,
            "primary_targets": [],
        },
    }

    rng = np.random.default_rng(102)
    tails = build_tail_profiles(["SYS_A"], m_tails=1, rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=1, rng=rng)

    telemetry_pd, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    telemetry_spark, _ = simulate_fleet_dataset_spark(
        spark=spark,
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )

    sort_cols = ["tail_id", "flight_id", "timestamp_utc", "sensor"]

    telemetry_pd_norm = telemetry_pd.copy()
    telemetry_pd_norm["timestamp_utc"] = pd.to_datetime(telemetry_pd_norm["timestamp_utc"], utc=True).dt.tz_localize(None)
    telemetry_pd_norm = telemetry_pd_norm.sort_values(sort_cols).reset_index(drop=True)

    telemetry_spark_pd = telemetry_spark.toPandas()
    telemetry_spark_pd["timestamp_utc"] = pd.to_datetime(telemetry_spark_pd["timestamp_utc"], utc=False)
    telemetry_spark_pd = telemetry_spark_pd.sort_values(sort_cols).reset_index(drop=True)

    assert len(telemetry_pd_norm) == len(telemetry_spark_pd)
    assert telemetry_pd_norm[sort_cols].equals(telemetry_spark_pd[sort_cols])

    float_cols = ["parameter_value", "parameter_value_clean", "anomaly_score_label"]
    for column in float_cols:
        assert np.allclose(
            pd.to_numeric(telemetry_pd_norm[column], errors="coerce").to_numpy(),
            pd.to_numeric(telemetry_spark_pd[column], errors="coerce").to_numpy(),
            equal_nan=True,
        )

    string_cols = [
        "phase_name",
        "anomaly_type_label",
        "event_type_label",
        "parameter_name",
        "parameter_datatype",
        "parameter_value",
        "date_utc",
    ]
    for column in string_cols:
        left = telemetry_pd_norm[column].fillna("<NA>").astype(str)
        right = telemetry_spark_pd[column].fillna("<NA>").astype(str)
        assert left.equals(right)

    int_cols = ["phase_id"]
    for column in int_cols:
        left = pd.to_numeric(telemetry_pd_norm[column], errors="coerce")
        right = pd.to_numeric(telemetry_spark_pd[column], errors="coerce")
        assert np.allclose(left.to_numpy(), right.to_numpy(), equal_nan=True)
