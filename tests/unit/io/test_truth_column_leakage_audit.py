from __future__ import annotations

from pathlib import Path


_FORBIDDEN_TRUTH_MARKERS = (
    "misbehavior_active",
    "misbehavior_window_id",
    "misbehavior_family_label",
    "misbehavior_detail_label",
    "fault_active",
    "fault_window_id",
    "fault_family_label",
    "fault_type",
)


def test_hot_path_feature_modules_do_not_reference_simulator_truth_columns():
    repo_root = Path(__file__).resolve().parents[3]
    checked_paths = (
        repo_root / "libs/profiling/profiles.py",
        repo_root / "libs/events/pipeline.py",
        repo_root / "libs/windows/pipeline.py",
        repo_root / "libs/anomaly/pipeline.py",
        repo_root / "libs/anomaly/tables.py",
        repo_root / "pipelines/10_parameter_profiles_fit.py",
        repo_root / "pipelines/20_events_extract.py",
        repo_root / "pipelines/90_anomaly_attribution.py",
    )

    for path in checked_paths:
        text = path.read_text()
        for marker in _FORBIDDEN_TRUTH_MARKERS:
            assert marker not in text, f"{path} unexpectedly references simulator truth marker {marker!r}"
