from datetime import datetime, timedelta, timezone

from libs.windows import sample_windows_for_coverage


def _t(offset_s: int) -> datetime:
    return datetime(2026, 3, 1, tzinfo=timezone.utc) + timedelta(seconds=offset_s)


def test_sample_windows_for_coverage_respects_target_size_per_flight():
    windows = []
    for idx in range(1, 21):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": idx,
                "t_end": _t(idx),
                "duration_ms": 500 + (idx * 50),
                "drift_magnitude_profiled": float(idx) * 0.1,
            }
        )

    out = sample_windows_for_coverage(windows, sample_size_per_flight=6, bins_per_axis=3)
    assert len(out) == 6
    assert len({int(item["win_id"]) for item in out}) == 6


def test_sample_windows_for_coverage_samples_each_flight_independently():
    windows = []
    for idx in range(1, 11):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": idx,
                "t_end": _t(idx),
                "duration_ms": 1000 + idx,
                "drift_magnitude_profiled": float(idx),
            }
        )
        windows.append(
            {
                "tail_id": "T2",
                "flight_id": "F2",
                "win_id": idx,
                "t_end": _t(100 + idx),
                "duration_ms": 2000 + idx,
                "drift_magnitude_profiled": float(idx) * 2.0,
            }
        )

    out = sample_windows_for_coverage(windows, sample_size_per_flight=4, bins_per_axis=2)
    by_flight = {}
    for item in out:
        key = (str(item["tail_id"]), str(item["flight_id"]))
        by_flight[key] = by_flight.get(key, 0) + 1
    assert by_flight[("T1", "F1")] == 4
    assert by_flight[("T2", "F2")] == 4
