from pathlib import Path

from libs.simulation.cli import resolve_flight
from libs.simulation.reference_inference import (
    REFERENCE_ARTIFACT_NAMES,
    nominal_companion_flight,
    reset_spark_between_runs,
    stage_reference_artifacts,
)
from libs.simulation.run_context import MODE_PLAN_BY_NAME, RunPaths


def test_nominal_companion_removes_misbehavior_without_changing_topology():
    faulted = resolve_flight("power_pressurization_hierarchy_composite", sim_seed=17)

    nominal = nominal_companion_flight(faulted)

    assert faulted.misbehavior_program_spec is not None
    assert faulted.misbehavior_program_spec.windows
    assert nominal.misbehavior_program_spec is not None
    assert nominal.misbehavior_program_spec.windows == ()
    assert nominal.aircraft_spec == faulted.aircraft_spec
    assert nominal.input_program_spec == faulted.input_program_spec
    assert nominal.phase_program_spec == faulted.phase_program_spec


def test_reference_artifact_inventory_excludes_observations_and_inference_outputs():
    forbidden = {
        "raw_input",
        "raw_telemetry",
        "events",
        "windows",
        "window_features",
        "phase_windows",
        "window_scores_raw",
        "window_scores_calibrated",
        "anomaly_window_attribution",
        "anomaly_telemetry_attribution",
        "anomaly_event_attribution",
        "anomaly_parameter_candidate_evidence",
    }

    assert not (set(REFERENCE_ARTIFACT_NAMES) & forbidden)
    assert {"continuous_scaling_profile", "backbone", "phase_reference_model"}.issubset(
        REFERENCE_ARTIFACT_NAMES
    )


def test_stage_reference_artifacts_copies_only_declared_model_artifacts(tmp_path: Path):
    reference_paths = RunPaths(tmp_path / "reference")
    target_paths = RunPaths(tmp_path / "target")
    for artifact_name in REFERENCE_ARTIFACT_NAMES:
        source_path = reference_paths.artifact_path(artifact_name)
        source_path.mkdir(parents=True)
        (source_path / "payload.txt").write_text(artifact_name, encoding="utf-8")
        stale_target = target_paths.artifact_path(artifact_name)
        stale_target.mkdir(parents=True)
        (stale_target / "stale.txt").write_text("stale", encoding="utf-8")

    lineage = stage_reference_artifacts(reference_paths=reference_paths, target_paths=target_paths)

    assert [item.artifact_name for item in lineage] == list(REFERENCE_ARTIFACT_NAMES)
    for artifact_name in REFERENCE_ARTIFACT_NAMES:
        target_path = target_paths.artifact_path(artifact_name)
        assert (target_path / "payload.txt").read_text(encoding="utf-8") == artifact_name
        assert not (target_path / "stale.txt").exists()


def test_reference_inference_stage_plan_contains_no_fit_stage():
    stage_scripts = MODE_PLAN_BY_NAME["reference_inference"].stage_scripts

    assert stage_scripts[0] == "20_events_extract.py"
    assert "35_window_features_apply.py" in stage_scripts
    assert "70_phase_fit.py" in stage_scripts
    assert not {
        "10_parameter_profiles_fit.py",
        "12_behavior_profiles_fit.py",
        "15_event_profiles_fit.py",
        "25_window_policy_profile.py",
        "40_backbone_fit.py",
        "50_build_graph.py",
        "60_fit_hierarchy.py",
    }.intersection(stage_scripts)


def test_reset_spark_between_runs_clears_cache_and_stops_session(monkeypatch):
    calls: list[str] = []

    class Catalog:
        @staticmethod
        def clearCache() -> None:
            calls.append("clear_cache")

    class Spark:
        catalog = Catalog()

        @staticmethod
        def stop() -> None:
            calls.append("stop")

    monkeypatch.setattr(
        "libs.simulation.reference_inference.get_spark",
        lambda _app_name: Spark(),
    )

    reset_spark_between_runs()

    assert calls == ["clear_cache", "stop"]
