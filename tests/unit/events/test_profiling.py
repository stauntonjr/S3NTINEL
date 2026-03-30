from datetime import datetime, timedelta, timezone

from libs.events import EventProfileConfig, ParameterEventProfile
from libs.profiling import TelemetryProfilingPlan


def test_parameter_event_profile_emits_numeric_recommendations_and_categorical_defaults(spark):
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = []
    for idx, value in enumerate([0.0, 1.0, 2.0, 3.0, 5.0, 8.0]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "numeric_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )
    for idx, value in enumerate(["OFF", "OFF", "ON", "ON", "OFF", "OFF"]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "state_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": value,
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )

    raw_df = spark.createDataFrame(rows)
    profiles = TelemetryProfilingPlan.from_raw_input(raw_df).build()
    event_profile_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=profiles.datatype_profile.to_dataframe(),
    ).to_dataframe()

    by_name = {row["parameter_name"]: row.asDict() for row in event_profile_df.collect()}
    numeric = by_name["numeric_sensor"]
    categorical = by_name["state_sensor"]

    assert numeric["parameter_datatype_profiled"] == "numeric"
    assert numeric["recommended_slope_threshold_mode"] == "fixed"
    assert numeric["recommended_slope_source"] in {"raw", "ema"}
    assert numeric["recommended_slope_archetype"] in {
        "responsive_low_scale",
        "repeatable_low_scale",
        "smooth_midscale",
        "meso_drift",
        "strong_drift",
        "chattery",
    }
    assert numeric["recommended_slope_min_persistence_samples"] >= 2
    assert numeric["recommended_slope_reemit_ratio"] >= 1.5
    assert numeric["sample_count"] > 0
    assert numeric["directionality_ratio_profiled"] is not None
    assert numeric["run_length_p90_profiled"] is not None
    assert numeric["repeatability_score_profiled"] is not None
    assert numeric["drift_score_profiled"] is not None
    assert numeric["chatter_score_profiled"] is not None

    assert categorical["parameter_datatype_profiled"] == "categorical"
    assert categorical["recommended_slope_threshold_mode"] is None
    assert categorical["recommended_emit_switch"] is False
    assert categorical["recommended_emit_oscillation"] is False
    assert categorical["recommended_emit_threshold"] is False


def test_parameter_event_profile_uses_stage_config_as_recommendation_prior(spark):
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = []
    for idx, value in enumerate([0.0, 0.5, 1.0, 1.5, 2.5, 4.0]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "numeric_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )

    raw_df = spark.createDataFrame(rows)
    profiles = TelemetryProfilingPlan.from_raw_input(raw_df).build()
    event_profile_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=profiles.datatype_profile.to_dataframe(),
        config=EventProfileConfig(
            slope_source="ema",
            slope_threshold_mode="fixed",
            slope_threshold_quantile=0.9,
            slope_threshold_scale=0.75,
            slope_threshold_min=0.01,
            slope_abs_threshold=2.0,
            slope_min_persistence_samples=3,
            slope_reemit_ratio=2.0,
            warmup_points=4,
        ),
    ).to_dataframe()

    numeric = event_profile_df.where("parameter_name = 'numeric_sensor'").first().asDict()

    assert numeric["recommended_slope_threshold_mode"] == "fixed"
    assert numeric["recommended_slope_source"] in {"raw", "ema"}
    assert numeric["recommended_slope_threshold_quantile"] in {0.75, 0.9}
    assert numeric["recommended_slope_threshold_scale"] >= 0.525
    assert numeric["recommended_slope_threshold_min"] == 0.01
    assert numeric["recommended_slope_threshold"] >= 0.01
    assert numeric["recommended_slope_min_persistence_samples"] >= 3
    assert numeric["recommended_slope_reemit_ratio"] >= 2.0
    assert numeric["recommended_warmup_points"] >= 4


def test_parameter_event_profile_scales_fixed_threshold_by_parameter_delta_magnitude(spark):
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = []
    for idx, value in enumerate([0.0, 0.1, 0.2, 0.3, 0.45, 0.6]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "small_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )
    for idx, value in enumerate([0.0, 10.0, 25.0, 45.0, 70.0, 100.0]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "large_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )

    raw_df = spark.createDataFrame(rows)
    profiles = TelemetryProfilingPlan.from_raw_input(raw_df).build()
    event_profile_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=profiles.datatype_profile.to_dataframe(),
        config=EventProfileConfig(
            slope_threshold_mode="fixed",
            slope_abs_threshold=1.0,
            slope_threshold_min=0.01,
        ),
    ).to_dataframe()

    by_name = {row["parameter_name"]: row.asDict() for row in event_profile_df.collect()}
    assert by_name["small_sensor"]["recommended_slope_threshold_mode"] == "fixed"
    assert by_name["large_sensor"]["recommended_slope_threshold_mode"] == "fixed"
    assert by_name["large_sensor"]["recommended_slope_threshold"] > by_name["small_sensor"]["recommended_slope_threshold"]


def test_parameter_event_profile_distinguishes_low_scale_and_drift_recommendations(spark):
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = []
    for idx, value in enumerate([0.0, 0.08, 0.16, 0.24, 0.32, 0.28, 0.36, 0.44]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "responsive_low_scale_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )
    for idx, value in enumerate([0.0, 0.1, -0.02, 0.12, -0.01, 0.11, 0.0, 0.13, -0.02, 0.12]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "repeatable_low_scale_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )
    for idx, value in enumerate([0.0, 4.0, 9.0, 15.0, 22.0, 30.0, 39.0, 49.0]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "meso_drift_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )
    for idx, value in enumerate([0.0, 5.0, 15.0, 30.0, 48.0, 68.0, 90.0, 115.0]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "macro_drift_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )

    raw_df = spark.createDataFrame(rows)
    profiles = TelemetryProfilingPlan.from_raw_input(raw_df).build()
    event_profile_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=profiles.datatype_profile.to_dataframe(),
        config=EventProfileConfig(
            slope_source="ema",
            slope_threshold_mode="adaptive_run",
            slope_threshold_quantile=0.75,
            slope_threshold_scale=0.5,
            slope_threshold_min=0.01,
            slope_abs_threshold=1.0,
            slope_min_persistence_samples=2,
            slope_reemit_ratio=1.25,
            warmup_points=2,
        ),
    ).to_dataframe()

    by_name = {row["parameter_name"]: row.asDict() for row in event_profile_df.collect()}
    responsive = by_name["responsive_low_scale_sensor"]
    repeatable = by_name["repeatable_low_scale_sensor"]
    meso = by_name["meso_drift_sensor"]
    macro = by_name["macro_drift_sensor"]

    assert responsive["recommended_slope_archetype"] == "responsive_low_scale"
    assert responsive["recommended_slope_source"] == "raw"
    assert responsive["recommended_warmup_points"] <= repeatable["recommended_warmup_points"]
    assert responsive["recommended_slope_reemit_ratio"] <= repeatable["recommended_slope_reemit_ratio"]
    assert responsive["recommended_slope_threshold"] < repeatable["recommended_slope_threshold"]

    assert repeatable["recommended_slope_archetype"] == "repeatable_low_scale"
    assert repeatable["recommended_slope_source"] == "raw"
    assert repeatable["chatter_score_profiled"] > responsive["chatter_score_profiled"]
    assert repeatable["recommended_warmup_points"] <= macro["recommended_warmup_points"]

    assert meso["recommended_slope_archetype"] in {"meso_drift", "strong_drift"}
    assert meso["recommended_slope_source"] == "ema"
    assert meso["drift_score_profiled"] > responsive["drift_score_profiled"]
    assert meso["recommended_warmup_points"] <= macro["recommended_warmup_points"]
    assert meso["recommended_slope_threshold"] <= macro["recommended_slope_threshold"]
    assert meso["recommended_slope_threshold_scale"] <= macro["recommended_slope_threshold_scale"]

    assert macro["recommended_slope_archetype"] == "strong_drift"
    assert macro["recommended_slope_source"] == "ema"
    assert macro["delta_scale_rank_profiled"] > meso["delta_scale_rank_profiled"]
    assert macro["recommended_warmup_points"] >= responsive["recommended_warmup_points"]
    assert macro["recommended_slope_threshold"] > meso["recommended_slope_threshold"]


def test_parameter_event_profile_applies_generic_morphology_policy_gains(spark):
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = []
    for idx, value in enumerate([0.0, 0.05, 0.12, 0.2, 0.28, 0.34, 0.41, 0.47]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "responsive_low_scale_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )
    for idx, value in enumerate([0.0, 6.0, 14.0, 23.0, 33.0, 45.0, 58.0, 72.0]):
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "parameter_name": "macro_drift_sensor",
                "timestamp_utc": t0 + timedelta(seconds=idx),
                "parameter_value": str(value),
                "date_utc": (t0 + timedelta(seconds=idx)).date(),
            }
        )

    raw_df = spark.createDataFrame(rows)
    profiles = TelemetryProfilingPlan.from_raw_input(raw_df).build()
    baseline_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=profiles.datatype_profile.to_dataframe(),
        config=EventProfileConfig(
            slope_threshold_mode="fixed",
            slope_threshold_scale=0.35,
            slope_abs_threshold=2.0,
            slope_min_persistence_samples=2,
            slope_reemit_ratio=1.5,
            warmup_points=4,
        ),
    ).to_dataframe()
    tuned_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=profiles.datatype_profile.to_dataframe(),
        config=EventProfileConfig(
            slope_threshold_mode="fixed",
            slope_threshold_scale=0.35,
            slope_abs_threshold=2.0,
            slope_min_persistence_samples=2,
            slope_reemit_ratio=1.5,
            warmup_points=4,
            low_scale_responsiveness=1.1,
            drift_conservatism=1.1,
        ),
    ).to_dataframe()

    baseline_by_name = {row["parameter_name"]: row.asDict() for row in baseline_df.collect()}
    tuned_by_name = {row["parameter_name"]: row.asDict() for row in tuned_df.collect()}

    baseline_responsive = baseline_by_name["responsive_low_scale_sensor"]
    tuned_responsive = tuned_by_name["responsive_low_scale_sensor"]
    baseline_macro = baseline_by_name["macro_drift_sensor"]
    tuned_macro = tuned_by_name["macro_drift_sensor"]

    assert baseline_responsive["recommended_slope_archetype"] == "responsive_low_scale"
    assert tuned_responsive["recommended_slope_threshold"] < baseline_responsive["recommended_slope_threshold"]
    assert tuned_responsive["recommended_slope_threshold_scale"] < baseline_responsive["recommended_slope_threshold_scale"]
    assert tuned_responsive["recommended_slope_reemit_ratio"] <= baseline_responsive["recommended_slope_reemit_ratio"]

    assert baseline_macro["recommended_slope_archetype"] in {"meso_drift", "strong_drift"}
    assert tuned_macro["recommended_slope_threshold"] > baseline_macro["recommended_slope_threshold"]
    assert tuned_macro["recommended_slope_threshold_scale"] > baseline_macro["recommended_slope_threshold_scale"]
    assert tuned_macro["recommended_slope_reemit_ratio"] >= baseline_macro["recommended_slope_reemit_ratio"]
