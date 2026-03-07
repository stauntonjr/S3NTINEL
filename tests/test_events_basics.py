from datetime import datetime, timedelta, timezone

from libs.events.categorical import detect_transitions
from libs.events.extrema import ContinuousDetectorConfig, ContinuousSample, classify_continuous_delta_event, detect_continuous_events_stream


def test_detect_transitions_empty():
    assert detect_transitions([]) == []


def test_detect_transitions_changes_only():
    states = ["OFF", "OFF", "ON", "ON", "STBY", "STBY", "ON"]
    assert detect_transitions(states) == ["OFF->ON", "ON->STBY", "STBY->ON"]


def test_classify_continuous_delta_event_threshold_disabled():
    assert classify_continuous_delta_event(10.0, 0.5, 0.0) == "slope_pos"
    assert classify_continuous_delta_event(10.0, -0.5, 0.0) == "slope_neg"


def test_classify_continuous_delta_event_threshold_enabled():
    assert classify_continuous_delta_event(10.0, 2.5, 2.0) == "threshold"
    assert classify_continuous_delta_event(10.0, -1.0, 2.0) == "slope_neg"


def _build_samples(values: list[float]) -> list[ContinuousSample]:
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    return [
        ContinuousSample(
            tail_id="T001",
            flight_id="F001",
            sensor="sensor_numeric",
            ts=t0 + timedelta(seconds=idx),
            value=val,
        )
        for idx, val in enumerate(values)
    ]


def test_detect_continuous_events_stream_slope_source_raw_emits_noisy_slopes():
    samples = _build_samples([0.0, 10.0, 0.0, 10.0, 0.0])
    cfg = ContinuousDetectorConfig(
        slope_source="raw",
        slope_abs_threshold=5.0,
        residual_z_threshold=1e9,
        switch_z_threshold=1e9,
        switch_delta_z_threshold=1e9,
        switch_min_abs_delta=1e9,
        warmup_points=1,
    )
    events = list(detect_continuous_events_stream(samples, config=cfg))
    slope_events = [event for event in events if str(event.get("event_type_detected", "")).startswith("slope_")]
    assert len(slope_events) > 0


def test_detect_continuous_events_stream_slope_source_ema_suppresses_noisy_slopes():
    samples = _build_samples([0.0, 10.0, 0.0, 10.0, 0.0])
    cfg = ContinuousDetectorConfig(
        slope_source="ema",
        ema_alpha=0.2,
        slope_abs_threshold=5.0,
        residual_z_threshold=1e9,
        switch_z_threshold=1e9,
        switch_delta_z_threshold=1e9,
        switch_min_abs_delta=1e9,
        warmup_points=1,
    )
    events = list(detect_continuous_events_stream(samples, config=cfg))
    slope_events = [event for event in events if str(event.get("event_type_detected", "")).startswith("slope_")]
    assert len(slope_events) == 0
