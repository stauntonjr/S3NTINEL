from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pytest

import libs.simulation.runner as runner
from libs.io.delta import get_spark
from libs.simulation.flight.examples import build_named_flight_spec


def _runner_config(tmp_path):
    return runner.PipelineRunConfig(
        flight_name="power_chain",
        tail_id="TSEED",
        flight_id="FSEED",
        n_steps=6,
        dt_seconds=1.0,
        base_dir=str(tmp_path),
        mode="full",
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        delta_threshold=0.0,
        slope_source="raw",
        ema_alpha=0.2,
        slope_threshold_mode="adaptive_run",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.5,
        slope_threshold_min=1e-6,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="segmented",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
        event_warmup_points=1,
    )


def test_sim_runner_uses_grouped_full_stage_scripts(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    monkeypatch.setattr(runner, "resolve_flight", lambda _flight_name: build_named_flight_spec("power_chain"))
    monkeypatch.setattr(runner, "get_spark", lambda _app_name: object())
    monkeypatch.setattr(
        runner,
        "_write_seed_tables",
        lambda **_kwargs: {
            "raw_input_rows": 1,
            "phase_label_rows": 1,
            "hierarchy_label_rows": 1,
            "coupling_misbehavior_window_rows": 0,
        },
    )
    monkeypatch.setattr(runner, "_write_validation_reports", lambda **_kwargs: {})

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(runner, "run_stage_group", fake_run_stage_group)
    result = runner.run_pipeline(_runner_config(tmp_path))

    assert captured["run_name"] == "s3ntinel.sim_full"
    assert captured["pipeline_mode"] == "sim_full:v2"
    assert captured["stage_scripts"] == [
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
        "95_emit_explorer_bundle.py",
    ]
    assert captured["summary_artifact_path"] == "reports/pipeline_run_summary.json"
    assert result.status == "success"


def test_sim_runner_uses_event_stage_scripts(monkeypatch, tmp_path):
    captured: dict[str, object] = {}
    config = _runner_config(tmp_path)
    config = runner.PipelineRunConfig(**{**config.__dict__, "mode": "event"})

    monkeypatch.setattr(runner, "resolve_flight", lambda _flight_name: build_named_flight_spec("power_chain"))
    monkeypatch.setattr(runner, "get_spark", lambda _app_name: object())
    monkeypatch.setattr(
        runner,
        "_write_seed_tables",
        lambda **_kwargs: {
            "raw_input_rows": 1,
            "phase_label_rows": 1,
            "hierarchy_label_rows": 1,
            "coupling_misbehavior_window_rows": 0,
        },
    )
    monkeypatch.setattr(runner, "_write_validation_reports", lambda **_kwargs: {})

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(runner, "run_stage_group", fake_run_stage_group)
    result = runner.run_pipeline(config)

    assert captured["run_name"] == "s3ntinel.sim_event"
    assert captured["pipeline_mode"] == "sim_event:v2"
    assert captured["stage_scripts"] == [
        "00_ingest_raw.py",
        "10_parameter_profiles_fit.py",
        "12_behavior_profiles_fit.py",
        "15_event_profiles_fit.py",
        "20_events_extract.py",
    ]
    assert captured["summary_artifact_path"] == "reports/event_pipeline_run_summary.json"
    assert result.status == "success"


def test_sim_runner_replay_skips_seed_write_and_passes_stage_range(monkeypatch, tmp_path):
    captured: dict[str, object] = {}
    config = runner.PipelineRunConfig(
        **{
            **_runner_config(tmp_path).__dict__,
            "mode": "full",
            "start_stage": "20_events_extract.py",
            "end_stage": "30_windows_adaptive.py",
            "replay_run_dir": str(tmp_path / "existing_run"),
        }
    )
    replay_run_dir = tmp_path / "existing_run"
    (replay_run_dir / "reports").mkdir(parents=True, exist_ok=True)
    (replay_run_dir / "reports" / "run_manifest.json").write_text(
        json.dumps({"seed_counts": {"raw_input_rows": 11, "phase_label_rows": 7}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "resolve_flight", lambda _flight_name: build_named_flight_spec("power_chain"))
    monkeypatch.setattr(runner, "get_spark", lambda _app_name: object())
    monkeypatch.setattr(
        runner,
        "_write_seed_tables",
        lambda **_kwargs: pytest.fail("seed tables should not be written for replay starting after ingest"),
    )
    monkeypatch.setattr(runner, "_write_validation_reports", lambda **_kwargs: {})

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(runner, "run_stage_group", fake_run_stage_group)
    result = runner.run_pipeline(config)

    assert captured["stage_scripts"] == [
        "20_events_extract.py",
        "25_window_policy_profile.py",
        "30_windows_adaptive.py",
    ]
    assert captured["start_stage_script"] == "20_events_extract.py"
    assert captured["end_stage_script"] == "30_windows_adaptive.py"
    assert captured["replay_run_dir"] == str(replay_run_dir)
    assert result.status == "success"
    assert result.seed_counts == {"raw_input_rows": 11, "phase_label_rows": 7}


def test_sim_runner_writes_seed_tables(tmp_path, monkeypatch):
    config = _runner_config(tmp_path)
    run_dir = tmp_path / "sim_seed"
    paths = runner.RunPaths(run_dir=run_dir)
    previous_env = runner._set_run_env(paths, config)
    written_tables: dict[str, dict[str, object]] = {}

    class FakeSparkFrame:
        def __init__(self, records):
            self.records = list(records)
            self.columns = list(self.records[0].keys()) if self.records else []
            self.write = self

        def mode(self, _mode):
            return self

        def parquet(self, path):
            written_tables["raw_input"] = {"path": path, "records": self.records, "columns": self.columns}

    class FakeSpark:
        def createDataFrame(self, records, schema=None):
            return FakeSparkFrame(records)

    def fake_write_table(df, path, mode, fmt, partition_by=None):
        written_tables[str(path)] = {
            "path": path,
            "mode": mode,
            "fmt": fmt,
            "partition_by": list(partition_by or []),
            "records": list(df.records),
            "columns": list(df.columns),
        }

    monkeypatch.setattr(runner, "write_table", fake_write_table)
    try:
        flight = runner.resolve_flight("power_chain")
        counts = runner._write_seed_tables(spark=FakeSpark(), paths=paths, config=config, flight=flight)

        assert counts["raw_input_rows"] > 0
        assert counts["phase_label_rows"] > 0
        assert counts["hierarchy_label_rows"] > 0
        assert "coupling_misbehavior_window_rows" in counts

        raw_input = written_tables["raw_input"]
        phase_labels = written_tables[str(run_dir / "delta" / "phase_labels")]
        hierarchy_labels = written_tables[str(run_dir / "delta" / "hierarchy_sensor_map_label")]

        assert raw_input["path"] == str(run_dir / "input" / "raw_telemetry")
        assert len(raw_input["records"]) == counts["raw_input_rows"]
        assert {"tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value", "date_utc"}.issubset(raw_input["columns"])
        assert {"tail_id", "flight_id", "step_index", "timestamp_utc", "phase_label", "date_utc"}.issubset(phase_labels["columns"])
        assert {"parameter_name", "system_id", "subsystem_id", "module_id"}.issubset(hierarchy_labels["columns"])
    finally:
        runner._restore_env(previous_env)


def test_sim_runner_full_smoke_emits_bundle(monkeypatch, tmp_path):
    try:
        spark = get_spark("s3ntinel.test_sim_full_smoke_preflight")
    except Exception as exc:
        pytest.skip(f"local Spark unavailable in this environment: {exc.__class__.__name__}")
    else:
        spark.stop()

    config = runner.PipelineRunConfig(
        flight_name="power_chain",
        tail_id="T_SIM",
        flight_id="F_SIM",
        n_steps=12,
        dt_seconds=1.0,
        base_dir=str(tmp_path),
        mode="full",
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        delta_threshold=0.0,
        slope_source="raw",
        ema_alpha=0.2,
        slope_threshold_mode="adaptive_run",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.5,
        slope_threshold_min=1e-6,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="segmented",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
        event_warmup_points=1,
    )
    result = runner.run_pipeline(config)
    run_dir = result.paths.run_dir

    manifest = json.loads((run_dir / "reports" / "run_manifest.json").read_text(encoding="utf-8"))
    full_run_report = json.loads((run_dir / "reports" / "full_run_report.json").read_text(encoding="utf-8"))
    stage25_summary = json.loads(
        (run_dir / "reports" / "stages" / "25_window_policy_profile_summary.json").read_text(encoding="utf-8")
    )
    assert manifest["status"] == "success"
    assert full_run_report["status"] == "success"
    assert "modeling_performance" in full_run_report
    assert "window_policy_profile" in full_run_report
    assert "engineering_performance" in full_run_report
    assert full_run_report["window_policy_profile"]["status"] in {"ok", "warning", "skipped"}
    assert (run_dir / "logs" / "run.log").exists()
    assert (run_dir / "reports" / "pipeline_run_summary.json").exists()
    assert (run_dir / "reports" / "full_run_report.md").exists()
    assert (run_dir / "reports" / "validation_harness_report.json").exists()
    assert (run_dir / "reports" / "validation_harness_report.md").exists()
    assert (run_dir / "reports" / "objective_evaluation_report.json").exists()
    assert (run_dir / "reports" / "objective_evaluation_report.md").exists()
    assert (run_dir / "reports" / "stages" / "10_parameter_profiles_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "15_event_profiles_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "25_window_policy_profile_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "25_window_policy_profile_evaluation.json").exists()
    assert stage25_summary["window_policy_profile_evaluation_path"] == "reports/stages/25_window_policy_profile_evaluation.json"
    assert (run_dir / "reports" / "stages" / "40_backbone_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "50_build_graph_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "50_build_graph_evaluation.json").exists()
    assert (run_dir / "reports" / "stages" / "60_fit_hierarchy_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "70_phase_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "72_phase_label_centroids_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "80_window_scores_raw_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "95_emit_explorer_bundle_manifest.json").exists()
    assert (run_dir / "reports" / "phase_validation_summary.json").exists()
    assert (run_dir / "reports" / "hierarchy_validation_summary.json").exists()
    assert (run_dir / "reports" / "coupling_validation_summary.json").exists()
    assert (run_dir / "reports" / "profile_validation_summary.json").exists()
    assert (run_dir / "reports" / "event_validation_summary.json").exists()
    assert (run_dir / "reports" / "label_contract_summary.json").exists()
    assert (run_dir / "reports" / "score_validation_summary.json").exists()
    assert (run_dir / "reports" / "misbehavior_window_validation_summary.json").exists()
    assert (run_dir / "reports" / "fault_window_validation_summary.json").exists()
    assert (run_dir / "reports" / "misbehavior_attribution_validation_summary.json").exists()
    assert (run_dir / "reports" / "attribution_validation_summary.json").exists()

    assert len(pd.read_parquet(run_dir / "delta" / "raw_telemetry")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "parameter_event_profile")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "events")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "window_policy_profile")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "windows")) > 0
    window_features_df = pd.read_parquet(run_dir / "delta" / "window_features")
    assert len(window_features_df) > 0
    assert window_features_df["event_type_counts"].apply(lambda value: bool(value)).any()
    assert len(pd.read_parquet(run_dir / "delta" / "backbone")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "precision_graph")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "lag_profile")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "graph_parameter_universe")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "hierarchy_sensor_map")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "phase_windows")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "phase_label_centroids")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "window_scores_calibrated")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "anomaly_window_attribution")) > 0
    assert (run_dir / "delta" / "explorer_bundle" / "bundle_manifest.json").exists()
    assert len(pd.read_parquet(run_dir / "delta" / "explorer_bundle" / "parameter_catalog")) > 0


def test_full_run_report_surfaces_window_policy_profile_and_skips_when_missing(tmp_path):
    paths = runner.RunPaths(run_dir=tmp_path / "sim_report")
    (paths.run_dir / "reports" / "stages").mkdir(parents=True, exist_ok=True)
    (paths.run_dir / "reports" / "pipeline_run_summary.json").write_text(
        json.dumps({"total_elapsed_ms": 1000.0, "stage_count": 1, "stages": []}),
        encoding="utf-8",
    )
    manifest = {"status": "success", "timing": {}, "environment": {}, "artifacts": {}}

    report_without_eval = runner._write_full_run_report(
        paths=paths,
        manifest=manifest,
        summary_artifact_path="reports/pipeline_run_summary.json",
        validation_payloads={},
    )

    assert report_without_eval["window_policy_profile"]["status"] == "skipped"

    evaluation_payload = {
        "status": "ok",
        "selected_policy": {
            "policy_source": "profile",
            "resolved_policy": {
                "max_ms": 1200,
                "event_threshold": 6,
                "min_ms": 50,
                "inactivity_timeout_ms": 0,
            },
            "configured_policy": {
                "max_ms": 10000,
                "event_threshold": 20,
                "min_ms": 50,
                "inactivity_timeout_ms": 0,
            },
            "profile_row": {
                "candidate_rank": 1,
            },
        },
        "closure_mix": {
            "rates": {
                "event_threshold": 0.7,
                "budget_threshold": 0.25,
                "end_of_stream": 0.05,
            }
        },
        "downstream_cost_proxy": {
            "window_count": 10,
            "pair_cost_proxy": 250.0,
            "same_window_pair_expansion_proxy": 40.0,
            "p95_event_count": 6.0,
            "p95_sensor_count": 2.0,
        },
        "edge_stability": {
            "status": "ok",
            "mean_boundary_jaccard": 0.9,
        },
        "warnings": [],
    }
    (paths.run_dir / "reports" / "stages" / "25_window_policy_profile_evaluation.json").write_text(
        json.dumps(evaluation_payload),
        encoding="utf-8",
    )

    report_with_eval = runner._write_full_run_report(
        paths=paths,
        manifest=manifest,
        summary_artifact_path="reports/pipeline_run_summary.json",
        validation_payloads={},
    )

    assert report_with_eval["window_policy_profile"]["status"] == "ok"
    assert report_with_eval["window_policy_profile"]["selected_max_ms"] == 1200
    assert report_with_eval["window_policy_profile"]["selected_event_threshold"] == 6
    markdown = (paths.run_dir / "reports" / "full_run_report.md").read_text(encoding="utf-8")
    assert "## Window Policy Profile" in markdown


def test_validation_harness_report_bundles_fit_validation_and_compute(tmp_path):
    paths = runner.RunPaths(run_dir=tmp_path / "sim_harness")
    stages_dir = paths.run_dir / "reports" / "stages"
    stages_dir.mkdir(parents=True, exist_ok=True)

    backbone_manifest_path = stages_dir / "40_backbone_fit_manifest.json"
    graph_manifest_path = stages_dir / "50_build_graph_manifest.json"
    backbone_manifest_path.write_text(
        json.dumps(
            {
                "config": {
                    "backbone_sensor_count": 8,
                    "backbone_ridge_lambda": 1.25,
                }
            }
        ),
        encoding="utf-8",
    )
    graph_manifest_path.write_text(
        json.dumps(
            {
                "config": {
                    "min_event_count": 3,
                    "lag_bands": [
                        {
                            "name": "short",
                            "upper_seconds": 2.0,
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    manifest = {
        "status": "success",
        "source": {
            "flight_name": "power_chain",
            "tail_id": "TSEED",
            "flight_id": "FSEED",
        },
        "simulation": {
            "n_steps": 12,
            "dt_seconds": 1.0,
        },
        "pipeline": {
            "mode": "full",
            "backbone_ridge_lambda": 1.0,
            "phase_count": 3,
        },
        "seed_counts": {
            "raw_input_rows": 100,
        },
    }
    full_run_report = {
        "status": "success",
        "modeling_performance": {
            "profile_validation": {
                "behavior_accuracy": 0.9,
            },
            "event_validation": {
                "f1": 0.8,
            },
            "score_validation": {
                "status": "ok",
                "detected_fault_window_rate": 0.5,
                "emit_ready_fault_window_rate": 0.4,
            },
            "hierarchy_validation": {
                "module_exact_match": 0.8,
            },
            "phase_validation": {
                "macro_f1": 0.7,
            },
            "attribution_validation": {
                "dominant_subsystem_match_rate": 0.6,
                "telemetry_parameter_match_rate": 0.8,
                "event_parameter_match_rate": 0.5,
            },
        },
        "engineering_performance": {
            "overall": {
                "pipeline_summary": {
                    "total_elapsed_ms": 1000.0,
                    "stage_count": 2,
                },
                "artifact_disk_bytes_total": 2048,
            },
            "scale_signature": {
                "graph_counts": {
                    "fused_edge_count": 12,
                }
            },
            "stages": [
                {
                    "stage_script": "40_backbone_fit.py",
                    "engineering_performance": {
                        "elapsed_ms": 250.0,
                        "share_of_total_elapsed": 0.25,
                        "summary_path": str(stages_dir / "40_backbone_fit_summary.json"),
                        "manifest_path": str(backbone_manifest_path),
                    },
                    "modeling_performance": {},
                },
                {
                    "stage_script": "50_build_graph.py",
                    "engineering_performance": {
                        "elapsed_ms": 700.0,
                        "share_of_total_elapsed": 0.7,
                        "summary_path": str(stages_dir / "50_build_graph_summary.json"),
                        "manifest_path": str(graph_manifest_path),
                    },
                    "modeling_performance": {
                        "hierarchy_validation": {
                            "module_exact_match": 0.8,
                        }
                    },
                },
            ],
        },
    }

    flight = build_named_flight_spec("power_chain")
    harness = runner._write_validation_harness_report(
        paths=paths,
        manifest=manifest,
        full_run_report=full_run_report,
        flight=flight,
    )

    assert harness["fit_parameters"]["pipeline"]["backbone_ridge_lambda"] == 1.0
    assert harness["fit_parameters"]["by_stage"]["40_backbone_fit.py"]["backbone_ridge_lambda"] == 1.25
    assert harness["simulation_context"]["flight"]["nominal_step_count"] == 12
    assert harness["simulation_context"]["stochasticity"]["profile_name"] == "deterministic"
    assert harness["workload_signature"]["stochasticity"]["seed"] == 0
    assert harness["simulation_context"]["input_program"]["authored_step_count"] > 0
    assert harness["simulation_context"]["parameter_catalog"]["parameter_count"] > 0
    assert harness["simulation_context"]["hierarchy"]["system_count"] > 0
    assert harness["simulation_context"]["phases"]["run_step_count"] == 12
    assert harness["compute_performance"]["bottleneck_stages"][0]["stage_script"] == "50_build_graph.py"
    assert any(
        record["scope_name"] == "50_build_graph.py"
        and record["parameter_path"] == "lag_bands[0].upper_seconds"
        and record["value"] == 2.0
        for record in harness["fit_parameters"]["parameter_records"]
    )
    assert any(
        record["category"] == "validation"
        and record["scope_name"] == "overall"
        and record["subscope_name"] == "score_validation"
        and record["metric_path"] == "detected_fault_window_rate"
        and record["value"] == 0.5
        for record in harness["validation_metrics"]["metric_records"]
    )
    assert any(
        record["category"] == "compute"
        and record["scope_name"] == "50_build_graph.py"
        and record["metric_path"] == "elapsed_ms"
        and record["value"] == 700.0
        for record in harness["compute_performance"]["metric_records"]
    )
    objective_report = runner._write_objective_evaluation_report(
        run_dir=paths.run_dir,
        harness_report=harness,
    )
    assert objective_report["status"] == "ok"
    assert objective_report["evaluation"]["objective_spec"]["name"] == "sim_full_default_v1"
    assert objective_report["evaluation"]["ready_for_search"] is True
    assert (paths.run_dir / "reports" / "validation_harness_report.json").exists()
    assert (paths.run_dir / "reports" / "validation_harness_report.md").exists()
    assert (paths.run_dir / "reports" / "objective_evaluation_report.json").exists()
    assert (paths.run_dir / "reports" / "objective_evaluation_report.md").exists()


def test_validation_harness_event_mode_uses_event_objective(tmp_path):
    paths = runner.RunPaths(run_dir=tmp_path / "sim_event_harness")
    stages_dir = paths.run_dir / "reports" / "stages"
    stages_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "status": "success",
        "source": {
            "flight_name": "power_chain",
            "tail_id": "TSEED",
            "flight_id": "FSEED",
        },
        "simulation": {
            "n_steps": 12,
            "dt_seconds": 1.0,
        },
        "pipeline": {
            "mode": "event",
            "ema_alpha": 0.2,
        },
        "seed_counts": {
            "raw_input_rows": 100,
        },
    }
    full_run_report = {
        "status": "success",
        "modeling_performance": {
            "profile_validation": {
                "datatype_accuracy": 1.0,
                "behavior_accuracy": 0.8,
            },
            "event_validation": {
                "precision": 0.7,
                "recall": 0.75,
                "f1": 0.72,
                "detected_per_label_ratio": 1.4,
                "slope_run_capture_metrics": {
                    "slope_pos": {"run_recall": 0.7},
                    "slope_neg": {"run_recall": 0.65},
                },
                "event_family_metrics": {
                    "slope_pos": {"f1": 0.12},
                    "slope_neg": {"f1": 0.11},
                    "transition": {"f1": 0.2},
                },
            },
        },
        "engineering_performance": {
            "overall": {
                "pipeline_summary": {
                    "total_elapsed_ms": 1000.0,
                    "stage_count": 4,
                },
            },
            "scale_signature": {},
            "stages": [
                {
                    "stage_script": "15_event_profiles_fit.py",
                    "engineering_performance": {
                        "elapsed_ms": 150.0,
                        "share_of_total_elapsed": 0.15,
                        "summary_path": str(stages_dir / "15_event_profiles_fit_summary.json"),
                        "manifest_path": str(stages_dir / "15_event_profiles_fit_manifest.json"),
                    },
                    "modeling_performance": {},
                },
                {
                    "stage_script": "20_events_extract.py",
                    "engineering_performance": {
                        "elapsed_ms": 350.0,
                        "share_of_total_elapsed": 0.35,
                        "summary_path": str(stages_dir / "20_events_extract_summary.json"),
                        "manifest_path": str(stages_dir / "20_events_extract_manifest.json"),
                    },
                    "modeling_performance": {
                        "event_validation": {
                            "precision": 0.7,
                            "recall": 0.75,
                            "f1": 0.72,
                            "slope_run_capture_metrics": {
                                "slope_pos": {"run_recall": 0.7},
                                "slope_neg": {"run_recall": 0.65},
                            },
                            "event_family_metrics": {
                                "slope_pos": {"f1": 0.12},
                                "slope_neg": {"f1": 0.11},
                                "transition": {"f1": 0.2},
                            },
                        }
                    },
                },
            ],
        },
    }
    (stages_dir / "15_event_profiles_fit_manifest.json").write_text(
        json.dumps({"config": {"table_format": "parquet", "write_mode": "overwrite"}}, indent=2),
        encoding="utf-8",
    )
    (stages_dir / "20_events_extract_manifest.json").write_text(
        json.dumps(
            {
                "config": {
                    "ema_alpha": 0.2,
                    "slope_source": "raw",
                    "slope_threshold_mode": "adaptive_run",
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    harness = runner._write_validation_harness_report(
        paths=paths,
        manifest=manifest,
        full_run_report=full_run_report,
        flight=build_named_flight_spec("power_chain"),
    )
    objective_report = runner._write_objective_evaluation_report(
        run_dir=paths.run_dir,
        harness_report=harness,
    )

    assert objective_report["status"] == "ok"
    assert objective_report["evaluation"]["objective_spec"]["name"] == "sim_event_default_v1"
    assert objective_report["evaluation"]["ready_for_search"] is True


def test_manifest_and_harness_capture_seeded_scenario_signature(tmp_path):
    paths = runner.RunPaths(run_dir=tmp_path / "sim_seeded_harness")
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    config = runner.PipelineRunConfig(
        flight_name="power_pressurization_hierarchy_smoke",
        tail_id="TSEED",
        flight_id="FSEED",
        n_steps=0,
        dt_seconds=0.0,
        base_dir=str(tmp_path),
        mode="profile",
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        delta_threshold=0.0,
        slope_source="raw",
        ema_alpha=0.2,
        slope_threshold_mode="adaptive_run",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.5,
        slope_threshold_min=1e-6,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="segmented",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
        event_warmup_points=1,
        sim_seed=123,
    )
    flight = runner.resolve_flight(config.flight_name, sim_seed=config.sim_seed)
    config = config.with_flight_defaults(flight=flight)
    manifest = runner._build_manifest(
        paths=paths,
        config=config,
        flight=flight,
        status="success",
        error_message=None,
        start_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        elapsed_ms=0.0,
        seed_counts={},
    )
    harness = runner._write_validation_harness_report(
        paths=paths,
        manifest=manifest,
        full_run_report={
            "status": "success",
            "modeling_performance": {},
            "engineering_performance": {
                "overall": {},
                "scale_signature": {},
                "stages": [],
            },
        },
        flight=flight,
    )

    assert manifest["stochasticity"]["seed"] == 123
    assert manifest["stochasticity"]["profile_name"] == "seeded_nominal_v1"
    assert manifest["pipeline"]["sim_seed"] == 123
    assert harness["workload_signature"]["stochasticity"]["seed"] == 123
    assert harness["simulation_context"]["stochasticity"]["profile_name"] == "seeded_nominal_v1"


def test_sim_runner_uses_library_validation_reports(monkeypatch, tmp_path):
    config = _runner_config(tmp_path)
    monkeypatch.setattr(runner, "resolve_flight", lambda _flight_name: build_named_flight_spec("power_chain"))
    monkeypatch.setattr(runner, "get_spark", lambda _app_name: object())
    monkeypatch.setattr(
        runner,
        "_write_seed_tables",
        lambda **_kwargs: {
            "raw_input_rows": 1,
            "phase_label_rows": 1,
            "hierarchy_label_rows": 1,
            "coupling_misbehavior_window_rows": 0,
        },
    )
    monkeypatch.setattr(runner, "run_stage_group", lambda **_kwargs: None)
    called: dict[str, object] = {}

    def fake_write_validation_reports(**kwargs):
        called.update(kwargs)
        return {}

    monkeypatch.setattr(runner, "_write_validation_reports", fake_write_validation_reports)

    result = runner.run_pipeline(config)

    assert result.status == "success"
    assert called["table_format"] == config.table_format


def test_runner_fault_wrappers_preserve_extended_validation_metrics():
    misbehavior_score_summary = {
        "status": "ok",
        "misbehavior_window_count": 2,
        "detected_misbehavior_window_count": 1,
        "emit_ready_misbehavior_window_count": 1,
        "detected_misbehavior_window_rate": 0.5,
        "emit_ready_misbehavior_window_rate": 0.5,
        "median_misbehavior_window_score": 3.2,
        "median_detection_latency_seconds": 1.5,
        "median_emit_ready_latency_seconds": 2.5,
        "raw_score_validation": {
            "window_count": 10,
        },
        "calibrated_score_validation": {
            "truth_window_recall_by_top_k_calibrated_rarity": {"any_overlap": {"top_5": 0.5}},
        },
        "emission_validation": {
            "blocked_candidate_window_count_by_p_value_threshold": {"p_le_0p05": 1},
        },
        "score_window_diagnostics": [{"win_id": 7}],
        "misbehavior_windows": [],
    }
    misbehavior_attribution_summary = {
        "status": "ok",
        "misbehavior_window_count": 2,
        "dominant_subsystem_match_count": 1,
        "dominant_subsystem_mappable_count": 2,
        "dominant_subsystem_match_rate": 0.5,
        "dominant_subsystem_mappable_rate": 1.0,
        "dominant_module_match_count": 1,
        "dominant_module_mappable_count": 2,
        "dominant_module_match_rate": 0.5,
        "dominant_module_mappable_rate": 1.0,
        "telemetry_parameter_match_count": 2,
        "event_parameter_match_count": 1,
        "telemetry_parameter_match_rate": 1.0,
        "event_parameter_match_rate": 0.5,
        "telemetry_truth_subsystem_present_rate": 1.0,
        "event_truth_subsystem_present_rate": 0.5,
        "module_localization_validation": {
            "truth_window_count": 2,
            "dominant_module_match_count": 1,
        },
        "channel_localization_validation": {
            "truth_window_count": 2,
            "truth_window_count_by_score_component": {"reconstruction_error": 2},
        },
        "reconstruction_localization_validation": {
            "reconstruction_truth_window_count": 2,
            "reconstruction_failure_count": 1,
            "failure_count_by_bucket": {"shared_source_won": 1},
        },
        "parameter_localization_validation": {
            "truth_window_count": 2,
            "exact_parameter_match_count_by_source": {
                "telemetry": 2,
                "telemetry_selected": 1,
                "event": 1,
                "any": 2,
                "both": 1,
            },
        },
        "misbehavior_windows": [],
    }

    fault_score_summary = runner._build_fault_score_summary_from_misbehavior(misbehavior_score_summary)
    fault_attribution_summary = runner._build_fault_attribution_summary_from_misbehavior(misbehavior_attribution_summary)

    assert fault_score_summary["detected_fault_window_rate"] == 0.5
    assert fault_score_summary["emit_ready_fault_window_rate"] == 0.5
    assert fault_score_summary["median_detection_latency_seconds"] == 1.5
    assert fault_score_summary["median_emit_ready_latency_seconds"] == 2.5
    assert fault_score_summary["raw_score_validation"]["window_count"] == 10
    assert fault_score_summary["calibrated_score_validation"]["truth_window_recall_by_top_k_calibrated_rarity"]["any_overlap"][
        "top_5"
    ] == 0.5
    assert fault_score_summary["emission_validation"]["blocked_candidate_window_count_by_p_value_threshold"]["p_le_0p05"] == 1
    assert fault_score_summary["score_window_diagnostics"] == [{"win_id": 7}]
    assert fault_attribution_summary["dominant_subsystem_match_count"] == 1
    assert fault_attribution_summary["dominant_module_match_count"] == 1
    assert fault_attribution_summary["telemetry_parameter_match_count"] == 2
    assert fault_attribution_summary["event_parameter_match_count"] == 1
    assert fault_attribution_summary["module_localization_validation"]["dominant_module_match_count"] == 1
    assert fault_attribution_summary["channel_localization_validation"]["truth_window_count_by_score_component"] == {
        "reconstruction_error": 2
    }
    assert fault_attribution_summary["reconstruction_localization_validation"]["failure_count_by_bucket"] == {
        "shared_source_won": 1
    }
    assert fault_attribution_summary["parameter_localization_validation"]["exact_parameter_match_count_by_source"] == {
        "telemetry": 2,
        "telemetry_selected": 1,
        "event": 1,
        "any": 2,
        "both": 1,
    }
