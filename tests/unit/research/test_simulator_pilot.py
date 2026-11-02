import json
from dataclasses import replace

import pytest

from libs.research import simulator_pilot as pilot
from libs.research.simulator_protocol import PilotProtocol, Threshold, WindowScore
from libs.research.simulator_reporting import build_report


def tiny_protocol():
    return replace(
        PilotProtocol(),
        n_steps=12,
        window_seconds=2.0,
        fit_seeds=(101, 102),
        calibration_seeds=(201,),
        test_seeds=(301,),
    )


def read(path):
    return json.loads(path.read_text())


def test_tiny_real_pilot_freezes_before_test_scores_and_reports_all_arms(tmp_path):
    destination = tmp_path / "pilot"
    result = pilot.run_pilot(destination, tiny_protocol())
    report = read(result)
    manifest = read(destination / "manifest.json")
    assert manifest["status"] == "complete"
    assert report["version"] == 1 and report["qualification"] is False
    assert len(manifest["missions"]) == 5
    assert report["reproducibility"]["same_seed_equal"] and report["reproducibility"]["different_seed_differs"]
    assert len(report["thresholds"]) == 6
    assert len(report["performance"]) == 6
    for item in report["performance"]:
        assert item["nominal_windows"] == 3
        assert item["false_alerts"] <= 3
        assert item["calibration"]["resolution_sufficient"] is False
    assert len(report["paired_window_contrasts"]) == 18
    assert len(report["comparisons"]) == 2
    assert report["coverage"]["stuck_at"]["status"] == "not_covered"
    assert report["coverage"]["drift"]["status"] == "declared_only"
    assert report["coverage"]["temporal"]["common_grid_interval_count"] == 0
    assert len(report["encoding_diagnostics"]) == 5
    sensor_encoding = report["encoding_diagnostics"][0]["sensors"]
    assert all(
        "unknown_rate" in row and "overflow_rate" in row and "observations" in row for row in sensor_encoding.values()
    )
    assert "by_authored_family" in report["performance"][0]
    assert set(report["examples"]) == {"fault_alert", "missed_interval", "nominal_false_alert"}
    assert report["scientific_claim"] == "descriptive_only"
    events = [json.loads(line) for line in (destination / "events.jsonl").read_text().splitlines()]
    frozen = next(i for i, e in enumerate(events) if e["stage"] == "frozen")
    tested = next(i for i, e in enumerate(events) if e.get("role") == "nominal_test")
    truth = next(i for i, e in enumerate(events) if e["stage"] == "truth")
    assert frozen < tested < truth
    assert "hardware.json" in manifest["artifact_hashes"]
    assert "models.json" in manifest["artifact_hashes"]
    assert report["freeze_verified"] is True
    assert (destination / "summary.md").is_file()
    with pytest.raises(FileExistsError):
        pilot.run_pilot(destination, tiny_protocol())


def test_failure_retains_partial_evidence_and_exact_cursor(tmp_path, monkeypatch):
    original = pilot.score_mission

    def fail(model, mission, **kwargs):
        if mission.role == "nominal_test":
            raise ValueError("injected scoring failure")
        return original(model, mission, **kwargs)

    monkeypatch.setattr(pilot, "score_mission", fail)
    destination = tmp_path / "failed"
    with pytest.raises(ValueError, match="injected"):
        pilot.run_pilot(destination, tiny_protocol())
    manifest = read(destination / "manifest.json")
    assert manifest["status"] == "failed"
    assert manifest["cursor"]["role"] == "nominal_test"
    assert manifest["cursor"]["seed"] == 301
    assert manifest["cursor"]["stage"] == "score"
    assert (destination / "freeze.json").exists()
    assert (destination / "missions/nominal_test_301/raw.jsonl").exists()
    assert not (destination / "report.json").exists()


def test_mutating_fitted_counts_during_test_is_a_hard_failure(tmp_path, monkeypatch):
    original = pilot.score_mission

    def mutate(model, mission, **kwargs):
        result = original(model, mission, **kwargs)
        if mission.role == "nominal_test":
            model.marginal[model.order[0]][0] += 1
        return result

    monkeypatch.setattr(pilot, "score_mission", mutate)
    with pytest.raises(ValueError, match="frozen"):
        pilot.run_pilot(tmp_path / "mutated", tiny_protocol())
    assert read(tmp_path / "mutated/manifest.json")["status"] == "failed"


def test_report_comparisons_and_false_alarm_denominators_use_actual_scores():
    scores = [
        WindowScore(role, 301, "lexical", model, step, step + 40, 10, value)
        for role, offset in (("nominal_test", 0), ("faulted_test", 1))
        for model, base in (("marginal", 4), ("temporal", 3), ("conditional", 2))
        for step, value in ((0, base + offset), (40, base + 2 + offset))
    ]
    thresholds = {
        f"lexical/{model}": Threshold(base + 1, 336, 334, 0, 2, True)
        for model, base in (("marginal", 4), ("temporal", 3), ("conditional", 2))
    }
    report = build_report(tiny_protocol(), {"scores": scores, "thresholds": thresholds, "truths": []})
    assert report["comparisons"][0]["conditional_minus_temporal"] == -1
    assert report["comparisons"][0]["conditional_minus_marginal"] == -2
    assert report["performance"][0]["false_alert_rate"] == 0.5
    assert report["performance"][0]["nominal_windows"] == 2
    assert all(row["faulted_minus_nominal"] == 1 for row in report["paired_window_contrasts"])


def test_encoding_report_retains_numerators_and_explicit_rates():
    from libs.research.simulator_reporting import encoding_metrics

    values = encoding_metrics(
        {"a": {"observations": 4, "occupancy": {0: 1, 1: 2, 9: 1}, "underflow": 1, "overflow": 1, "unknown": 0}}
    )
    assert values["a"]["underflow_rate"] == 0.25
    assert values["a"]["overflow_rate"] == 0.25
    assert values["a"]["unknown_rate"] == 0
    assert values["a"]["observations"] == 4


def test_report_aggregates_authored_family_without_losing_intervals():
    scores = [
        WindowScore(role, 301, "lexical", model, 0, 40, 10, 1.0)
        for role in ("nominal_test", "faulted_test")
        for model in ("marginal", "temporal", "conditional")
    ]
    thresholds = {
        f"lexical/{model}": Threshold(2, 336, 334, 0, 2, True) for model in ("marginal", "temporal", "conditional")
    }
    faults = [
        {
            "seed": 301,
            "window_id": detail,
            "arm": "lexical",
            "model": "conditional",
            "family": "regulated",
            "detail": detail,
            "detected": detected,
            "applied_rows": 0,
            "grid_applied_rows": 0,
            "off_grid_only": False,
        }
        for detail, detected in (("bias", True), ("saturation", False))
    ]
    report = build_report(tiny_protocol(), {"scores": scores, "thresholds": thresholds, "truths": [{"faults": faults}]})
    row = next(r for r in report["performance"] if r["model"] == "conditional")
    assert row["by_authored_family"]["regulated"]["declared_intervals"] == 2
    assert row["by_authored_family"]["regulated"]["detected_rate"] == 0.5
