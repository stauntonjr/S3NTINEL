from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from libs.events import EventDetectionPlan
from libs.events.continuous import ContinuousDetectorConfig, ContinuousEventDetector
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import SIMULATION_RAW_INPUT_SCHEMA
from libs.io.transforms import normalize_raw_telemetry
from libs.simulation.flight.examples import build_named_flight_spec
from libs.simulation.flight.runtime import Flight
from libs.windows import WindowFeaturesTable, WindowsTable


def _build_flight(*, flight_name: str, tail_id: str = "TSIM", flight_id: str = "FSIM") -> Flight:
    return Flight.from_spec(
        build_named_flight_spec(flight_name),
        tail_id=tail_id,
        flight_id=flight_id,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


def test_flight_simulate_rows_emits_canonical_raw_rows_and_phase_rows():
    flight = _build_flight(flight_name="power_chain", tail_id="TNAT", flight_id="FNAT")

    raw_rows, phase_rows = flight.simulate_rows(
        n_steps=5,
        dt_seconds=1.0,
        apply_faults=True,
    )

    raw_df = pd.DataFrame.from_records(raw_rows)
    phase_df = pd.DataFrame.from_records(phase_rows)

    assert not raw_df.empty
    assert len(phase_df) == 5
    assert {
        "tail_id",
        "flight_id",
        "timestamp_utc",
        "parameter_name",
        "parameter_value",
        "date_utc",
    }.issubset(raw_df.columns)
    assert {
        "parameter_value_clean",
        "unit",
        "rate_hz",
        "phase_label",
        "system_id",
        "subsystem_id",
        "module_id",
        "behavior_family_label",
        "parameter_datatype_label",
        "misbehavior_active",
        "misbehavior_applied",
        "misbehavior_family_label",
        "misbehavior_detail_label",
        "misbehavior_window_id",
        "coupling_id_label",
        "event_type_label",
        "event_misbehavior_label",
        "anomaly_type_label",
        "anomaly_score_label",
        "fault_active",
        "fault_applied",
        "fault_family_label",
        "fault_type",
        "fault_window_id",
    }.issubset(raw_df.columns)
    assert list(phase_df.columns) == [
        "tail_id",
        "flight_id",
        "step_index",
        "timestamp_utc",
        "phase_label",
        "date_utc",
    ]
    assert set(raw_df["tail_id"].astype(str)) == {"TNAT"}
    assert set(raw_df["flight_id"].astype(str)) == {"FNAT"}


def test_flight_tick_emits_canonical_rows_for_named_flight():
    flight = _build_flight(flight_name="pressurization", tail_id="TPRESS", flight_id="FPRESS")

    tick = flight.step(dt_seconds=1.0, apply_faults=True)
    raw_df = pd.DataFrame.from_records(tick.telemetry_rows())
    phase_row = tick.phase_row()

    assert not raw_df.empty
    assert {
        "tail_id",
        "flight_id",
        "timestamp_utc",
        "parameter_name",
        "parameter_value",
        "date_utc",
    }.issubset(raw_df.columns)
    assert phase_row["tail_id"] == "TPRESS"
    assert phase_row["flight_id"] == "FPRESS"


def test_composite_flight_emits_fault_truth_metadata():
    raw_rows, _phase_rows = _build_flight(
        flight_name="power_pressurization_hierarchy_smoke",
        tail_id="TCOMP",
        flight_id="FCOMP",
    ).simulate_rows(
        n_steps=3360,
        dt_seconds=0.5,
        apply_faults=True,
    )

    raw_df = pd.DataFrame.from_records(raw_rows)
    misbehavior_rows = raw_df[raw_df["misbehavior_active"].fillna(False).astype(bool)]
    misbehavior_applied_rows = raw_df[raw_df["misbehavior_applied"].fillna(False).astype(bool)]
    fault_rows = raw_df[raw_df["fault_active"].fillna(False).astype(bool)]

    assert not raw_df.empty
    assert not misbehavior_rows.empty
    assert not misbehavior_applied_rows.empty
    assert not fault_rows.empty
    assert {
        "misbehavior_active",
        "misbehavior_applied",
        "misbehavior_family_label",
        "misbehavior_detail_label",
        "misbehavior_window_id",
        "fault_active",
        "fault_applied",
        "fault_family_label",
        "fault_type",
        "fault_window_id",
    }.issubset(raw_df.columns)
    applied_detail_labels = set(misbehavior_applied_rows["misbehavior_detail_label"].dropna().astype(str))
    assert {"bias", "saturation", "state_chatter", "illegal_transition"}.issubset(applied_detail_labels)
    assert {"timing_jitter", "coupling_inversion", "coupling_break"}.intersection(applied_detail_labels)
    assert misbehavior_rows["misbehavior_window_id"].dropna().astype(str).nunique() >= 8
    assert {"regulated", "inertial", "discrete_state", "coupling"}.issubset(
        set(fault_rows["fault_family_label"].dropna().astype(str))
    )
    assert fault_rows["fault_window_id"].dropna().astype(str).nunique() >= 8
    assert {"0.5", "1.0", "2.0"}.issubset(set(raw_df["rate_hz"].fillna(0.0).astype(str)))
    assert raw_df["unit"].fillna("").astype(str).str.len().max() > 0
    assert misbehavior_rows["coupling_id_label"].fillna("").astype(str).str.len().max() > 0


def test_canonical_sim_rows_flow_into_events_windows_and_window_features(spark):
    raw_rows, _phase_rows = _build_flight(
        flight_name="pressurization",
        tail_id="TSTRUC",
        flight_id="FSTRUC",
    ).simulate_rows(
        n_steps=6,
        dt_seconds=1.0,
        apply_faults=True,
    )

    raw_sdf = spark.createDataFrame(
        pandas_records_for_spark(pd.DataFrame.from_records(raw_rows)),
        schema=SIMULATION_RAW_INPUT_SCHEMA(),
    )
    normalized_raw_sdf = normalize_raw_telemetry(raw_sdf)
    events_sdf = (
        EventDetectionPlan(
            continuous_detector=ContinuousEventDetector(
                config=ContinuousDetectorConfig(delta_threshold=0.0, slope_source="ema", ema_alpha=0.2)
            )
        )
        .build(normalized_raw_sdf)
        .events.to_dataframe()
    )
    windows_sdf = WindowsTable.from_events(
        events_sdf,
        max_ms=10000,
        event_threshold=20,
        min_ms=50,
        inactivity_timeout_ms=0,
        strategy="segmented",
    ).to_dataframe()
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(
        normalized_raw_sdf,
        events_sdf,
        windows_sdf,
    ).to_dataframe()

    assert events_sdf.count() > 0
    assert windows_sdf.count() > 0
    assert window_features_sdf.count() > 0
    assert {"tail_id", "flight_id", "event_seq_id", "timestamp_utc", "parameter_name", "event_type_detected"}.issubset(events_sdf.columns)
    assert {"tail_id", "flight_id", "win_id", "event_count", "sensor_count", "event_type_counts", "zoh_snapshot", "close_reason"}.issubset(windows_sdf.columns)
    assert {"tail_id", "flight_id", "win_id", "continuous_vector_t_end", "categorical_state_t_end"}.issubset(
        window_features_sdf.columns
    )
