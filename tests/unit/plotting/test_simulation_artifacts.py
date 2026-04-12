from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from libs.plotting.explorer_bundle import build_explorer_telemetry_spark_table
from libs.plotting import (
    SensorExplorerState,
    artifact_availability_table,
    build_sensor_explorer_dataset,
    build_sensor_explorer_figure,
    build_sensor_explorer_filter_options,
    discover_latest_run_dir,
    explorer_bundle_manifest_path,
    explorer_bundle_table_paths,
    extract_log_records,
    filter_sensor_explorer_data,
    load_artifact_table,
    load_explorer_bundle,
    load_explorer_filter_options,
    load_explorer_slice,
    load_simulation_run_bundle,
    plot_fleet_structure,
    plot_flight_timelines,
    plot_hierarchy_behavior_map,
    plot_hierarchy_datatype_map,
    plot_hierarchy_structure,
    plot_phase_confidence,
    plot_phase_timelines,
    validation_summary_table,
)


def _write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build_run_bundle(tmp_path: Path, run_name: str, *, include_validations: bool = True) -> Path:
    run_dir = tmp_path / run_name
    reports_dir = run_dir / "reports"
    logs_dir = run_dir / "logs"
    delta_dir = run_dir / "delta"
    reports_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    delta_dir.mkdir(parents=True, exist_ok=True)

    hierarchy_df = pd.DataFrame(
        [
            {"parameter_name": "sensor_a", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_A"},
            {"parameter_name": "sensor_b", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_B"},
        ]
    )
    scores_df = pd.DataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": "W1", "global_score": 0.25, "p_value": 0.8},
            {"tail_id": "T1", "flight_id": "F1", "win_id": "W2", "global_score": 1.2, "p_value": 0.1},
        ]
    )
    telemetry_df = pd.DataFrame(
        [
            {
                "timestamp_utc": "2025-01-01T00:00:00Z",
                "parameter_name": "sensor_a",
                "parameter_value": "1.0",
                "parameter_value_clean": "1.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
                "phase_label": "climb",
            },
            {
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_a",
                "parameter_value": "2.0",
                "parameter_value_clean": "2.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
                "phase_label": "cruise",
            },
            {
                "timestamp_utc": "2025-01-01T00:00:00Z",
                "parameter_name": "sensor_b",
                "parameter_value": "10.0",
                "parameter_value_clean": "10.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
                "phase_label": "climb",
            },
            {
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_b",
                "parameter_value": "11.0",
                "parameter_value_clean": "11.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
                "phase_label": "cruise",
            },
        ]
    )
    events_df = pd.DataFrame(
        [
            {"timestamp_utc": "2025-01-01T00:00:01Z", "parameter_name": "sensor_a", "event_type_detected": "slope_pos"},
            {"timestamp_utc": "2025-01-01T00:00:01Z", "parameter_name": "sensor_b", "event_type_detected": "transition"},
        ]
    )
    anomaly_telemetry_df = pd.DataFrame(
        [
            {
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_b",
                "severity": "high",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
            }
        ]
    )
    anomaly_window_df = pd.DataFrame(
        [
            {"timestamp_utc": "2025-01-01T00:00:01Z", "severity": "high"},
        ]
    )
    phase_windows_df = pd.DataFrame(
        [
            {
                "t_start": "2025-01-01T00:00:00Z",
                "t_end": "2025-01-01T00:00:01Z",
                "phase_state_detected": "climb",
            }
        ]
    )
    hierarchy_df.to_parquet(delta_dir / "hierarchy_sensor_map")
    scores_df.to_parquet(delta_dir / "window_scores_calibrated")
    pd.DataFrame(
        [
            {
                "parameter_name_u": "sensor_a",
                "parameter_name_v": "sensor_b",
                "lag_band": "quick",
                "lag_count": 1,
                "lag_weight": 0.75,
                "mean_lag_seconds": 1.0,
                "support_flight_count": 1,
                "edge_family": "lag_profile_directed",
            }
        ]
    ).to_parquet(delta_dir / "lag_profile")
    telemetry_df.to_parquet(delta_dir / "raw_telemetry")
    events_df.to_parquet(delta_dir / "events")
    anomaly_telemetry_df.to_parquet(delta_dir / "anomaly_telemetry_attribution")
    anomaly_window_df.to_parquet(delta_dir / "anomaly_window_attribution")
    phase_windows_df.to_parquet(delta_dir / "phase_windows")
    explorer_root = delta_dir / "explorer_bundle"
    explorer_root.mkdir(parents=True, exist_ok=True)
    explorer_paths = explorer_bundle_table_paths(explorer_root)
    telemetry_explorer_df = telemetry_df.assign(
        tail_id="T1",
        flight_id="F1",
        unit=None,
        rate_hz=None,
        date_utc=None,
        plot_value_raw=[1.0, 2.0, 10.0, 11.0],
        plot_value_zscore=[-0.7071, 0.7071, -0.7071, 0.7071],
        plot_value_robust=[-0.6745, 0.6745, -0.6745, 0.6745],
        plot_value_default=[-0.6745, 0.6745, -0.6745, 0.6745],
    )[
        [
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "parameter_value",
            "parameter_value_clean",
            "unit",
            "rate_hz",
            "date_utc",
            "system_id",
            "subsystem_id",
            "module_id",
            "plot_value_raw",
            "plot_value_zscore",
            "plot_value_robust",
            "plot_value_default",
        ]
    ]
    parameter_catalog_df = pd.DataFrame(
        [
            {
                "parameter_name": "sensor_a",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
                "unit": None,
                "rate_hz": None,
                "row_count": 2,
                "timestamp_min": "2025-01-01T00:00:00Z",
                "timestamp_max": "2025-01-01T00:00:01Z",
                "plot_mean": 1.5,
                "plot_std": 0.7071,
                "plot_median": 1.5,
                "plot_mad": 0.5,
                "plot_min": 1.0,
                "plot_max": 2.0,
            },
            {
                "parameter_name": "sensor_b",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
                "unit": None,
                "rate_hz": None,
                "row_count": 2,
                "timestamp_min": "2025-01-01T00:00:00Z",
                "timestamp_max": "2025-01-01T00:00:01Z",
                "plot_mean": 10.5,
                "plot_std": 0.7071,
                "plot_median": 10.5,
                "plot_mad": 0.5,
                "plot_min": 10.0,
                "plot_max": 11.0,
            },
        ]
    )
    event_markers_df = pd.DataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_a",
                "event_type_detected": "slope_pos",
                "anomaly_type_detected": None,
                "anomaly_score_detected": None,
                "marker_source": "event",
                "severity": None,
                "window_global_score": None,
                "system_id": None,
                "subsystem_id": None,
                "module_id": None,
                "date_utc": None,
            }
        ]
    )
    anomaly_markers_df = pd.DataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_b",
                "severity": "high",
                "window_global_score": 1.2,
                "parameter_value": "11.0",
                "parameter_datatype_label": None,
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
                "date_utc": None,
            }
        ]
    )
    anomaly_windows_explorer_df = pd.DataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "severity": "high",
                "global_score": 1.2,
                "p_value": 0.1,
                "dominant_subsystem_id": None,
                "dominant_score_component": None,
                "date_utc": None,
            }
        ]
    )
    phase_intervals_df = pd.DataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_start": "2025-01-01T00:00:00Z",
                "timestamp_end": "2025-01-01T00:00:01Z",
                "phase_label": "climb",
                "phase_id_detected": 0,
                "phase_state_detected": "climb",
                "source": "phase_windows",
                "date_utc": None,
            }
        ]
    )
    telemetry_explorer_df.to_parquet(explorer_paths["telemetry"])
    parameter_catalog_df.to_parquet(explorer_paths["parameter_catalog"])
    event_markers_df.to_parquet(explorer_paths["event_markers"])
    anomaly_markers_df.to_parquet(explorer_paths["anomaly_markers"])
    anomaly_windows_explorer_df.to_parquet(explorer_paths["anomaly_windows"])
    phase_intervals_df.to_parquet(explorer_paths["phase_intervals"])
    explorer_bundle_manifest_path(explorer_root).write_text(
        json.dumps(
            {
                "bundle_version": "v1",
                "root_dir": str(explorer_root),
                "tables": {name: str(path) for name, path in explorer_paths.items()},
                "counts": {
                    "telemetry": len(telemetry_explorer_df),
                    "parameter_catalog": len(parameter_catalog_df),
                    "event_markers": len(event_markers_df),
                    "anomaly_markers": len(anomaly_markers_df),
                    "anomaly_windows": len(anomaly_windows_explorer_df),
                    "phase_intervals": len(phase_intervals_df),
                },
            }
        ),
        encoding="utf-8",
    )

    manifest = {
        "status": "success",
        "run_dir": str(run_dir),
        "artifacts": {
            "raw_telemetry": {"path": str(delta_dir / "raw_telemetry"), "exists": True},
            "hierarchy_sensor_map": {"path": str(delta_dir / "hierarchy_sensor_map"), "exists": True},
            "lag_profile": {"path": str(delta_dir / "lag_profile"), "exists": True},
            "window_scores_calibrated": {"path": str(delta_dir / "window_scores_calibrated"), "exists": True},
            "events": {"path": str(delta_dir / "events"), "exists": True},
            "anomaly_telemetry_attribution": {"path": str(delta_dir / "anomaly_telemetry_attribution"), "exists": True},
            "anomaly_window_attribution": {"path": str(delta_dir / "anomaly_window_attribution"), "exists": True},
            "phase_windows": {"path": str(delta_dir / "phase_windows"), "exists": True},
            "explorer_bundle": {"path": str(explorer_root), "exists": True},
        },
    }
    summary = {
        "status": "success",
        "pipeline_mode": "sim_full:v2",
        "completed_stage_count": 10,
        "failed_stage_count": 0,
        "stages": [
            {"stage_script": "00_ingest_raw.py", "status": "success", "elapsed_ms": 1200.0},
            {"stage_script": "50_build_graph.py", "status": "success", "elapsed_ms": 3500.0},
        ],
    }
    _write_json(reports_dir / "run_manifest.json", manifest)
    _write_json(reports_dir / "pipeline_run_summary.json", summary)
    if include_validations:
        _write_json(reports_dir / "profile_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "event_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "label_contract_summary.json", {"status": "ok"})
        _write_json(reports_dir / "phase_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "hierarchy_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "coupling_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "score_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "misbehavior_score_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "misbehavior_window_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "misbehavior_attribution_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "fault_window_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "attribution_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "simulation_benchmark_audit_summary.json", {"status": "ok"})
    (logs_dir / "run.log").write_text(
        "\n".join(
            [
                "2026-03-14T10:00:00 | INFO | s3ntinel.run_sim_pipeline | sim_run_start",
                "2026-03-14T10:00:05 | WARN | __main__ | something_slow",
                "2026-03-14T10:00:10 | INFO | s3ntinel.run_sim_pipeline | sim_run_complete",
            ]
        ),
        encoding="utf-8",
    )
    return run_dir


def test_discover_latest_run_dir_finds_latest_manifest_bundle(tmp_path):
    older = _build_run_bundle(tmp_path, "20260314T100000Z_power_chain")
    newer = _build_run_bundle(tmp_path, "20260314T110000Z_power_chain")

    latest = discover_latest_run_dir(tmp_path)

    assert latest == newer
    assert latest != older


def test_load_simulation_run_bundle_reads_reports_and_inventory(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T120000Z_power_chain")

    bundle = load_simulation_run_bundle(run_dir)

    assert bundle.manifest is not None
    assert bundle.pipeline_summary is not None
    assert bundle.validation_reports["phase_validation_summary.json"] == {"status": "ok"}
    assert bundle.validation_reports["profile_validation_summary.json"] == {"status": "ok"}
    assert bundle.artifact_inventory["hierarchy_sensor_map"]["exists"] is True
    assert bundle.log_text is not None


def test_load_simulation_run_bundle_handles_missing_validations(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T130000Z_power_chain", include_validations=False)

    bundle = load_simulation_run_bundle(run_dir)
    validation_df = validation_summary_table(bundle)

    assert "phase_validation_summary" in set(validation_df["report_name"])
    assert "missing" in set(validation_df["status"])
    assert any("phase_validation_summary.json" in path for path in bundle.missing_files)


def test_artifact_availability_and_table_loading(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T140000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)

    inventory_df = artifact_availability_table(bundle)
    hierarchy_df = load_artifact_table(bundle, "hierarchy_sensor_map")
    scores_df = load_artifact_table(bundle, "window_scores_calibrated", columns=["global_score"], limit=1)

    assert {"artifact_name", "exists", "path"} == set(inventory_df.columns)
    assert set(hierarchy_df.columns) >= {"parameter_name", "system_id", "subsystem_id", "module_id"}
    assert list(scores_df.columns) == ["global_score"]
    assert len(scores_df) == 1


def test_build_explorer_telemetry_spark_table_handles_medians_without_ambiguous_join(spark):
    raw_schema = """
parameter_name string,
tail_id string,
flight_id string,
timestamp_utc timestamp,
parameter_value string,
parameter_value_clean string,
unit string,
rate_hz double,
date_utc date
"""
    hierarchy_schema = """
parameter_name string,
system_id string,
subsystem_id string,
module_id string
"""
    raw_sdf = spark.createDataFrame(
        [
            {
                "parameter_name": "sensor_a",
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "parameter_value": "1.0",
                "parameter_value_clean": "1.0",
                "unit": "psi",
                "rate_hz": 1.0,
                "date_utc": date(2025, 1, 1),
            },
            {
                "parameter_name": "sensor_a",
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
                "parameter_value": "2.0",
                "parameter_value_clean": "2.0",
                "unit": "psi",
                "rate_hz": 1.0,
                "date_utc": date(2025, 1, 1),
            },
            {
                "parameter_name": "sensor_b",
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "parameter_value": "10.0",
                "parameter_value_clean": "10.0",
                "unit": "deg",
                "rate_hz": 2.0,
                "date_utc": date(2025, 1, 1),
            },
            {
                "parameter_name": "sensor_b",
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
                "parameter_value": "11.0",
                "parameter_value_clean": "11.0",
                "unit": "deg",
                "rate_hz": 2.0,
                "date_utc": date(2025, 1, 1),
            },
        ],
        schema=raw_schema,
    )
    hierarchy_sdf = spark.createDataFrame(
        [
            {"parameter_name": "sensor_a", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_A"},
            {"parameter_name": "sensor_b", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_B"},
        ],
        schema=hierarchy_schema,
    )

    telemetry_sdf, catalog_sdf = build_explorer_telemetry_spark_table(raw_sdf, hierarchy_sdf)
    telemetry_pdf = telemetry_sdf.orderBy("parameter_name", "timestamp_utc").toPandas()
    catalog_pdf = catalog_sdf.orderBy("parameter_name").toPandas()

    assert len(telemetry_pdf) == 4
    assert set(telemetry_pdf["parameter_name"]) == {"sensor_a", "sensor_b"}
    catalog = {row["parameter_name"]: row for row in catalog_pdf.to_dict(orient="records")}
    assert set(catalog) == {"sensor_a", "sensor_b"}
    assert float(catalog["sensor_a"]["plot_min"]) <= float(catalog["sensor_a"]["plot_median"]) <= float(catalog["sensor_a"]["plot_max"])
    assert float(catalog["sensor_b"]["plot_min"]) <= float(catalog["sensor_b"]["plot_median"]) <= float(catalog["sensor_b"]["plot_max"])
    assert telemetry_pdf["plot_value_robust"].notna().all()


def test_extract_log_records_parses_log_lines():
    df = extract_log_records(
        "\n".join(
            [
                "2026-03-14T10:00:00 | INFO | logger.name | start",
                "2026-03-14T10:00:01 | ERROR | logger.name | failed",
            ]
        )
    )

    assert list(df["level"]) == ["INFO", "ERROR"]
    assert list(df["message"]) == ["start", "failed"]


def test_sensor_explorer_dataset_and_filter_options(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T150000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)

    dataset = build_sensor_explorer_dataset(bundle)
    options = build_sensor_explorer_filter_options(dataset["telemetry"])

    assert "plot_value" in dataset["telemetry"].columns
    assert options["system_ids"] == ["SYS_A"]
    assert options["module_ids"] == ["MOD_A", "MOD_B"]
    assert options["parameter_names"] == ["sensor_a", "sensor_b"]


def test_explorer_bundle_loaders_and_slice(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T151500Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)

    explorer_bundle = load_explorer_bundle(bundle)
    options = load_explorer_filter_options(explorer_bundle)
    dataset = load_explorer_slice(explorer_bundle, SensorExplorerState(parameter_names=("sensor_a",)))

    assert options["parameter_names"] == ["sensor_a", "sensor_b"]
    assert set(dataset["telemetry"]["parameter_name"]) == {"sensor_a"}
    assert "plot_value_default" in dataset["telemetry"].columns


def test_sensor_explorer_filtering_and_figure_build(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T160000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    dataset = build_sensor_explorer_dataset(bundle)
    state = SensorExplorerState(module_id="MOD_B", scale_mode="normalized", show_events=True, show_anomaly_markers=True)

    filtered = filter_sensor_explorer_data(dataset, state)
    figure = build_sensor_explorer_figure(dataset, state)

    assert set(filtered["telemetry"]["parameter_name"]) == {"sensor_b"}
    assert len(figure.data) >= 2
    assert any(trace.name == "sensor_b" for trace in figure.data)
    assert "normalized" in (figure.layout.title.text or "")


def test_sensor_explorer_dual_axis_mode_builds_plotly_figure(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T170000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    dataset = build_sensor_explorer_dataset(bundle)
    state = SensorExplorerState(scale_mode="dual_axis", parameter_names=("sensor_a", "sensor_b"))

    figure = build_sensor_explorer_figure(dataset, state)

    assert len(figure.data) >= 2
    assert "raw" in (figure.layout.title.text or "")


def test_hierarchy_structure_and_profile_maps_build_plotly_figures(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T171500Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    hierarchy_df = load_artifact_table(bundle, "hierarchy_sensor_map")
    behavior_profile_df = pd.DataFrame(
        [
            {
                "parameter_name": "sensor_a",
                "parameter_datatype_profiled": "continuous_numeric",
                "behavior_family_profiled": "regulated",
                "behavior_profile_confidence": 0.9,
            },
            {
                "parameter_name": "sensor_b",
                "parameter_datatype_profiled": "continuous_numeric",
                "behavior_family_profiled": "inertial",
                "behavior_profile_confidence": 0.8,
            },
        ]
    )

    structure_fig = plot_hierarchy_structure(hierarchy_df)
    behavior_fig = plot_hierarchy_behavior_map(hierarchy_df, behavior_profile_df)
    datatype_fig = plot_hierarchy_datatype_map(hierarchy_df, behavior_profile_df)

    assert structure_fig is not None
    assert behavior_fig is not None
    assert datatype_fig is not None
    assert "Hierarchy structure" in (structure_fig.layout.title.text or "")
    assert "behavior" in (behavior_fig.layout.title.text or "").lower()
    assert "datatype" in (datatype_fig.layout.title.text or "").lower()


def test_fleet_structure_and_timelines_build_plotly_figures(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T172000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    explorer_bundle = load_explorer_bundle(bundle)
    dataset = load_explorer_slice(explorer_bundle, SensorExplorerState())

    structure_fig = plot_fleet_structure(dataset["telemetry"])
    timeline_fig = plot_flight_timelines(dataset["telemetry"])

    assert structure_fig is not None
    assert timeline_fig is not None
    assert "fleet" in (structure_fig.layout.title.text or "").lower()
    assert "timeline" in (timeline_fig.layout.title.text or "").lower()


def test_phase_timelines_and_confidence_build_plotly_figures(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T172500Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    phase_df = load_artifact_table(bundle, "phase_windows")
    phase_df = phase_df.assign(
        tail_id="T1",
        flight_id="F1",
        phase_id_detected=0,
        phase_confidence_detected=0.85,
    )

    timeline_fig = plot_phase_timelines(phase_df)
    confidence_fig = plot_phase_confidence(phase_df)

    assert timeline_fig is not None
    assert confidence_fig is not None
    assert "phase" in (timeline_fig.layout.title.text or "").lower()
    assert "confidence" in (confidence_fig.layout.title.text or "").lower()
