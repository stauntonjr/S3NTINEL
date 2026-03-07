from __future__ import annotations

from pathlib import Path

import numpy as np

from libs.events.detection import detect_events_from_pandas
from libs.simulation import (
    build_default_sensor_behavior,
    build_fleet_manifest,
    build_tail_profiles,
    default_phase_definitions,
    flatten_hierarchy_spec,
    simulate_fleet_dataset,
)
from libs.testing.schema_assertions import (
    REQUIRED_DETECTED_COLUMNS,
    REQUIRED_LABEL_COLUMNS,
    assert_no_banned_columns,
    assert_no_bare_detector_event_type,
    assert_profiler_validator_canonical_columns,
    assert_required_columns,
)
from libs.io.schemas import ACTIVE_V2_TABLES


def _sample_hierarchy_spec() -> dict:
    return {
        "systems": {
            "SYS_A": {
                "subsystems": {
                    "SUB_A": {
                        "modules": {
                            "MOD_A": [
                                {"sensor": "s_num", "datatype": "numeric", "unit": "u"},
                                {"sensor": "s_cat", "datatype": "categorical", "unit": "u"},
                            ]
                        }
                    }
                }
            }
        }
    }


def test_simulation_and_detector_outputs_use_canonical_columns():
    hierarchy_df = flatten_hierarchy_spec(_sample_hierarchy_spec())
    behavior = build_default_sensor_behavior(hierarchy_df)
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

    telemetry_df, _ = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        sensor_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    assert_required_columns(telemetry_df.columns, REQUIRED_LABEL_COLUMNS)
    assert_no_banned_columns(telemetry_df.columns)

    detected_df = detect_events_from_pandas(telemetry_df, include_cooccur=False)
    assert_required_columns(detected_df.columns, REQUIRED_DETECTED_COLUMNS)
    assert_no_bare_detector_event_type(detected_df.columns)
    assert_no_banned_columns(detected_df.columns)


def test_banned_label_identifiers_absent_from_python_and_markdown_sources():
    root = Path(__file__).resolve().parents[1]
    scan_paths = [root / "libs", root / "scripts", root / "tests", root / "pipelines", root / "README.md", root / "scripts/README.md"]
    banned_tokens = [
        "sim_event_type",
        "truth_event_label_",
        "truth_anomaly_label_",
        "truth_",
    ]
    allowlist_files = {
        (root / "docs/canonical_label_schema.md").resolve(),
        (root / "libs/testing/schema_assertions.py").resolve(),
        Path(__file__).resolve(),
    }

    hits: list[str] = []
    for scan_path in scan_paths:
        if not scan_path.exists():
            continue
        files = [scan_path] if scan_path.is_file() else list(scan_path.rglob("*"))
        for file_path in files:
            if file_path.resolve() in allowlist_files:
                continue
            if not file_path.is_file() or file_path.suffix not in {".py", ".md"}:
                continue
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            for token in banned_tokens:
                if token in text:
                    hits.append(f"{file_path.relative_to(root)} contains '{token}'")

    assert not hits, "\n".join(hits)


def test_profiler_validator_schema_rejects_legacy_datatype_names():
    assert_profiler_validator_canonical_columns({"tail_id", "timestamp_utc", "parameter_datatype_label"})
    assert_profiler_validator_canonical_columns({"tail_id", "timestamp_utc", "parameter_datatype_profiled"})


def test_active_v2_schema_contracts_expose_phase_and_score_tables():
    assert "phase_windows" in ACTIVE_V2_TABLES
    assert "phase_baselines" in ACTIVE_V2_TABLES
    assert "window_scores_raw" in ACTIVE_V2_TABLES
    assert "anomaly_window_attribution" in ACTIVE_V2_TABLES
    assert "anomaly_telemetry_attribution" in ACTIVE_V2_TABLES
    assert "anomaly_event_attribution" in ACTIVE_V2_TABLES
    assert "transition_graph" in ACTIVE_V2_TABLES

    phase_windows = set(ACTIVE_V2_TABLES["phase_windows"])
    assert {"tail_id", "flight_id", "win_id", "x_c", "s_w", "backbone_reconstruction_error"}.issubset(phase_windows)

    scores = set(ACTIVE_V2_TABLES["window_scores_raw"])
    assert {"global_score", "dominant_subsystem_id", "dominant_score_component", "score_component_scores"}.issubset(scores)

    anomaly_window_attribution = set(ACTIVE_V2_TABLES["anomaly_window_attribution"])
    assert {"tail_id", "flight_id", "win_id", "global_score", "dominant_subsystem_id", "subsystems", "attribution_context", "artifact_versions"}.issubset(
        anomaly_window_attribution
    )
    telemetry_attribution = set(ACTIVE_V2_TABLES["anomaly_telemetry_attribution"])
    assert {"tail_id", "flight_id", "win_id", "timestamp_utc", "parameter_name", "subsystem_id"}.issubset(
        telemetry_attribution
    )
    event_attribution = set(ACTIVE_V2_TABLES["anomaly_event_attribution"])
    assert {"tail_id", "flight_id", "win_id", "timestamp_utc", "parameter_name", "event_type_detected"}.issubset(
        event_attribution
    )
