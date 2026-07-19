from pathlib import Path

from scripts.smoke_test_pipeline import SMOKE_STAGE_SCRIPTS, set_env_paths


def test_smoke_stage_sequence_matches_active_validation_path():
    assert SMOKE_STAGE_SCRIPTS == (
        "00_ingest_raw.py",
        "10_parameter_profiles_fit.py",
        "12_behavior_profiles_fit.py",
        "15_event_profiles_fit.py",
        "20_events_extract.py",
        "25_window_policy_profile.py",
        "30_windows_adaptive.py",
        "40_backbone_fit.py",
        "50_build_graph.py",
        "60_fit_hierarchy.py",
        "70_phase_fit.py",
        "72_phase_label_centroids.py",
        "80_window_scores_raw.py",
        "85_window_scores_calibrate.py",
        "90_anomaly_attribution.py",
    )


def test_smoke_artifact_paths_are_scoped_to_base_dir(tmp_path: Path):
    set_env_paths(str(tmp_path / "run"), "parquet", "overwrite", 1)

    import os

    for env_name in (
        "S3NTINEL_PARAMETER_EVENT_PROFILE_TABLE_PATH",
        "S3NTINEL_WINDOW_FEATURES_TABLE_PATH",
        "S3NTINEL_GRAPH_PARAMETER_UNIVERSE_TABLE_PATH",
        "S3NTINEL_PHASE_REFERENCE_MODEL_TABLE_PATH",
        "S3NTINEL_ANOMALY_PARAMETER_CANDIDATE_EVIDENCE_TABLE_PATH",
    ):
        assert str(tmp_path / "run" / "delta") in os.environ[env_name]
