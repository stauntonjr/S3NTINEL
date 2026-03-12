from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from libs.events import build_events_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.transforms import normalize_raw_telemetry
from libs.simulation.flight.examples import build_named_flight_spec
from libs.simulation.flight.runtime import Flight
from libs.windows import build_window_features_spark_dataframe, build_windows_table


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
        "phase_label",
        "system_id",
        "subsystem_id",
        "module_id",
        "behavior_family_label",
        "parameter_datatype_label",
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
        flight_name="power_pressurization_hierarchy_composite",
        tail_id="TCOMP",
        flight_id="FCOMP",
    ).simulate_rows(
        n_steps=32,
        dt_seconds=1.0,
        apply_faults=True,
    )

    raw_df = pd.DataFrame.from_records(raw_rows)
    fault_rows = raw_df[raw_df["fault_active"].fillna(False).astype(bool)]

    assert not raw_df.empty
    assert not fault_rows.empty
    assert {
        "fault_active",
        "fault_applied",
        "fault_family_label",
        "fault_type",
        "fault_window_id",
    }.issubset(raw_df.columns)
    assert {"regulated", "inertial", "accumulative", "discrete_state"}.issubset(
        set(fault_rows["fault_family_label"].dropna().astype(str))
    )
    assert fault_rows["fault_window_id"].dropna().astype(str).nunique() >= 4


def test_canonical_sim_rows_flow_into_events_windows_and_window_x(spark):
    raw_rows, _phase_rows = _build_flight(
        flight_name="pressurization",
        tail_id="TSTRUC",
        flight_id="FSTRUC",
    ).simulate_rows(
        n_steps=6,
        dt_seconds=1.0,
        apply_faults=True,
    )

    raw_sdf = spark.createDataFrame(pandas_records_for_spark(pd.DataFrame.from_records(raw_rows)))
    normalized_raw_sdf = normalize_raw_telemetry(raw_sdf)
    events_sdf = build_events_table(normalized_raw_sdf, delta_threshold=0.0, slope_source="ema", ema_alpha=0.2)
    windows_sdf = build_windows_table(
        events_sdf,
        max_ms=10000,
        event_threshold=20,
        min_ms=50,
        inactivity_timeout_ms=0,
        strategy="bucketed",
    )
    window_x_sdf = build_window_features_spark_dataframe(normalized_raw_sdf, events_sdf, windows_sdf)

    assert events_sdf.count() > 0
    assert windows_sdf.count() > 0
    assert window_x_sdf.count() > 0
    assert {"tail_id", "flight_id", "timestamp_utc", "parameter_name", "event_type_detected"}.issubset(events_sdf.columns)
    assert {"tail_id", "flight_id", "win_id", "event_count", "close_reason"}.issubset(windows_sdf.columns)
    assert {"tail_id", "flight_id", "win_id", "continuous_vector_t_end", "categorical_state_t_end"}.issubset(
        window_x_sdf.columns
    )
