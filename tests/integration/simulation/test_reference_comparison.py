"""Cross-run comparison must align observations, not adaptive window ordinals."""

from datetime import date, datetime, timedelta

import pytest

from libs.simulation.reference_inference import _candidate_comparison, _phase_comparison
from libs.simulation.run_context import RunPaths


@pytest.fixture
def comparison_paths(spark, tmp_path):
    paths = (RunPaths(tmp_path / "self"), RunPaths(tmp_path / "reference"))
    # Same IDs with different intervals must not match; different IDs with
    # identical intervals must match. Only intervals [1, 2] and [2, 3] align.
    branch_rows = (
        [(1, 0, 1, 1, 1, True), (2, 1, 2, 1, 1, True), (3, 2, 3, 1, None, None), (4, 4, 5, 1, 1, True)],
        [(1, 0, 1.5, 1, 1, True), (9, 1, 2, 2, 2, False), (3, 2, 3, 1, None, None), (4, 4, 6, 2, 2, False)],
    )
    epoch = datetime(2026, 1, 1)
    for path, rows in zip(paths, branch_rows):
        windows = [
            (
                "tail",
                "flight",
                win_id,
                date(2026, 1, 1),
                epoch + timedelta(seconds=start),
                epoch + timedelta(seconds=end),
                phase,
            )
            for win_id, start, end, phase, _, _ in rows
        ]
        window_df = spark.createDataFrame(
            windows,
            "tail_id string, flight_id string, win_id int, date_utc date, "
            "t_start timestamp, t_end timestamp, phase_id_detected int",
        )
        window_df.write.parquet(str(path.artifact_path("phase_windows")))
        window_df.drop("phase_id_detected").write.parquet(str(path.artifact_path("windows")))
        candidates = [
            ("tail", "flight", win_id, date(2026, 1, 1), "pressure", rank, retained, retained)
            for win_id, _, _, _, rank, retained in rows
        ]
        # Even identical orphan IDs cannot be compared without boundaries.
        candidates.append(("tail", "flight", 99, date(2026, 1, 1), "pressure", 1, True, True))
        spark.createDataFrame(
            candidates,
            "tail_id string, flight_id string, win_id int, date_utc date, parameter_name string, "
            "parameter_support_rank_in_window int, telemetry_retained boolean, structural_cut_retained boolean",
        ).write.parquet(str(path.artifact_path("anomaly_parameter_candidate_evidence")))
    return paths


def test_phase_comparison_matches_exact_intervals_not_window_ids(spark, comparison_paths):
    own, reference = comparison_paths
    result = _phase_comparison(spark, self_paths=own, reference_inference_paths=reference, fmt="parquet")

    assert result.self_fit_window_count == result.reference_inference_window_count == 4
    assert result.matched_window_count == 2
    assert result.self_fit_unmatched_window_count == result.reference_inference_unmatched_window_count == 2
    assert result.phase_assignment_change_count == 1


def test_candidate_comparison_resolves_each_runs_window_intervals(spark, comparison_paths):
    own, reference = comparison_paths
    result = _candidate_comparison(spark, self_paths=own, reference_inference_paths=reference, fmt="parquet")

    assert result.self_fit_candidate_count == result.reference_inference_candidate_count == 5
    assert result.matched_candidate_count == 2
    assert result.self_fit_unmatched_candidate_count == result.reference_inference_unmatched_candidate_count == 3
    assert result.support_rank_change_count == 1
    assert result.telemetry_cut_change_count == 1
    assert result.structural_cut_change_count == 1


@pytest.mark.parametrize("duplicate_branches", [(0,), (0, 1)])
def test_ambiguous_intervals_are_unmatched_not_cartesian_products(spark, comparison_paths, duplicate_branches):
    from pyspark.sql import functions as F

    own, reference = comparison_paths
    for branch in duplicate_branches:
        path = comparison_paths[branch]
        for artifact in ("windows", "phase_windows", "anomaly_parameter_candidate_evidence"):
            original = spark.read.parquet(str(path.artifact_path(artifact))).localCheckpoint(eager=True)
            # Another valid ordinal with the same [2, 3] interval/parameter.
            duplicate = original.where(F.col("win_id") == 3).withColumn("win_id", F.lit(30))
            original.unionByName(duplicate).write.mode("overwrite").parquet(str(path.artifact_path(artifact)))
            original.unpersist()

    phase = _phase_comparison(spark, self_paths=own, reference_inference_paths=reference, fmt="parquet")
    candidate = _candidate_comparison(spark, self_paths=own, reference_inference_paths=reference, fmt="parquet")

    assert phase.matched_window_count == candidate.matched_candidate_count == 1
    assert phase.self_fit_window_count == 5
    assert phase.reference_inference_window_count == (5 if 1 in duplicate_branches else 4)
    assert phase.self_fit_unmatched_window_count == 4
    assert phase.reference_inference_unmatched_window_count == (4 if 1 in duplicate_branches else 3)
    assert candidate.self_fit_candidate_count == 6
    assert candidate.reference_inference_candidate_count == (6 if 1 in duplicate_branches else 5)
    assert candidate.self_fit_unmatched_candidate_count == 5
    assert candidate.reference_inference_unmatched_candidate_count == (5 if 1 in duplicate_branches else 4)


@pytest.mark.parametrize("missing_bound", ["t_start", "t_end"])
def test_missing_interval_boundaries_are_unmatched(spark, comparison_paths, missing_bound):
    from pyspark.sql import functions as F

    own, reference = comparison_paths
    for path in comparison_paths:
        for artifact in ("windows", "phase_windows"):
            original = spark.read.parquet(str(path.artifact_path(artifact))).localCheckpoint(eager=True)
            original.withColumn(
                missing_bound,
                F.when(F.col("win_id") == 3, F.lit(None).cast("timestamp")).otherwise(F.col(missing_bound)),
            ).write.mode("overwrite").parquet(str(path.artifact_path(artifact)))
            original.unpersist()

    phase = _phase_comparison(spark, self_paths=own, reference_inference_paths=reference, fmt="parquet")
    candidate = _candidate_comparison(spark, self_paths=own, reference_inference_paths=reference, fmt="parquet")

    assert phase.matched_window_count == candidate.matched_candidate_count == 1
    assert phase.self_fit_unmatched_window_count == phase.reference_inference_unmatched_window_count == 3
    assert candidate.self_fit_unmatched_candidate_count == candidate.reference_inference_unmatched_candidate_count == 4
