"""Paired nominal-reference-fit and faulted-inference simulation diagnostics."""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from libs.io.delta import get_spark, read_table
from libs.simulation.fault.examples import build_no_misbehavior_program_spec
from libs.simulation.run_context import PipelineRunConfig, PipelineRunResult, RunPaths
from libs.simulation.runner import run_pipeline


REFERENCE_ARTIFACT_NAMES = (
    "parameter_datatype_profile",
    "continuous_scaling_profile",
    "parameter_behavior_primitive_profile",
    "parameter_behavior_profile",
    "parameter_event_profile",
    "window_policy_profile",
    "backbone",
    "backbone_sensor_energy",
    "precision_graph",
    "event_graph",
    "lag_profile",
    "lag_graph",
    "transition_graph",
    "fused_graph",
    "graph_parameter_universe",
    "hierarchy_edge_evidence",
    "hierarchy_sensor_map",
    "phase_baselines",
    "phase_reference_model",
)


@dataclass(frozen=True)
class ReferenceArtifactLineage:
    artifact_name: str
    source_path: str
    target_path: str
    source_run_dir: str


@dataclass(frozen=True)
class PhaseInferenceComparison:
    self_fit_window_count: int
    reference_inference_window_count: int
    matched_window_count: int
    phase_assignment_change_count: int


@dataclass(frozen=True)
class ScoreInferenceComparison:
    self_fit_window_count: int
    reference_inference_window_count: int
    self_fit_emit_ready_count: int
    reference_inference_emit_ready_count: int


@dataclass(frozen=True)
class CandidateEvidenceComparison:
    self_fit_candidate_count: int
    reference_inference_candidate_count: int
    matched_candidate_count: int
    support_rank_change_count: int
    telemetry_cut_change_count: int
    structural_cut_change_count: int


@dataclass(frozen=True)
class ReferenceInferenceComparisonReport:
    reference_run_dir: str
    self_fit_run_dir: str
    reference_inference_run_dir: str
    artifact_lineage: tuple[ReferenceArtifactLineage, ...]
    phase_comparison: PhaseInferenceComparison
    score_comparison: ScoreInferenceComparison
    candidate_evidence_comparison: CandidateEvidenceComparison
    observed_value_contract: str = "parameter_value_only"
    report_version: int = 1

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


@dataclass(frozen=True)
class PairedReferenceInferenceResult:
    pair_dir: Path
    reference_run: PipelineRunResult
    self_fit_run: PipelineRunResult
    reference_inference_run: PipelineRunResult
    report_path: Path


def nominal_companion_flight(faulted_flight: "FlightSpec") -> "FlightSpec":
    nominal_program = build_no_misbehavior_program_spec()
    metadata = {**dict(faulted_flight.metadata), "reference_role": "nominal_reference"}
    return replace(
        faulted_flight,
        misbehavior_program_spec=nominal_program,
        fault_program_spec=nominal_program,
        metadata=metadata,
    )


def stage_reference_artifacts(
    *,
    reference_paths: RunPaths,
    target_paths: RunPaths,
) -> tuple[ReferenceArtifactLineage, ...]:
    lineage: list[ReferenceArtifactLineage] = []
    for artifact_name in REFERENCE_ARTIFACT_NAMES:
        source_path = reference_paths.artifact_path(artifact_name)
        target_path = target_paths.artifact_path(artifact_name)
        if not source_path.exists():
            raise RuntimeError(f"reference artifact is missing: {artifact_name} at {source_path}")
        if target_path.exists():
            if target_path.is_dir():
                shutil.rmtree(target_path)
            else:
                target_path.unlink()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.is_dir():
            shutil.copytree(source_path, target_path)
        else:
            shutil.copy2(source_path, target_path)
        lineage.append(
            ReferenceArtifactLineage(
                artifact_name=artifact_name,
                source_path=str(source_path),
                target_path=str(target_path),
                source_run_dir=str(reference_paths.run_dir),
            )
        )
    return tuple(lineage)


def reset_spark_between_runs() -> None:
    """Release persisted RDDs and JVM memory before the next paired run."""
    spark = get_spark("s3ntinel.reference_inference_run_boundary")
    spark.catalog.clearCache()
    spark.stop()


def _phase_comparison(spark, *, self_paths: RunPaths, reference_inference_paths: RunPaths, fmt: str):
    from pyspark.sql import functions as F

    keys = ["tail_id", "flight_id", "win_id", "date_utc"]
    self_df = read_table(spark, str(self_paths.artifact_path("phase_windows")), fmt=fmt)
    reference_df = read_table(spark, str(reference_inference_paths.artifact_path("phase_windows")), fmt=fmt)
    matched_df = self_df.select(*keys, F.col("phase_id_detected").alias("self_phase_id")).join(
        reference_df.select(*keys, F.col("phase_id_detected").alias("reference_phase_id")),
        on=keys,
        how="inner",
    )
    return PhaseInferenceComparison(
        self_fit_window_count=int(self_df.count()),
        reference_inference_window_count=int(reference_df.count()),
        matched_window_count=int(matched_df.count()),
        phase_assignment_change_count=int(
            matched_df.where(F.col("self_phase_id") != F.col("reference_phase_id")).count()
        ),
    )


def _score_comparison(spark, *, self_paths: RunPaths, reference_inference_paths: RunPaths, fmt: str):
    from pyspark.sql import functions as F

    self_df = read_table(spark, str(self_paths.artifact_path("window_scores_calibrated")), fmt=fmt)
    reference_df = read_table(
        spark,
        str(reference_inference_paths.artifact_path("window_scores_calibrated")),
        fmt=fmt,
    )
    return ScoreInferenceComparison(
        self_fit_window_count=int(self_df.count()),
        reference_inference_window_count=int(reference_df.count()),
        self_fit_emit_ready_count=int(self_df.where(F.col("emit_ready") == F.lit(True)).count()),
        reference_inference_emit_ready_count=int(
            reference_df.where(F.col("emit_ready") == F.lit(True)).count()
        ),
    )


def _candidate_comparison(spark, *, self_paths: RunPaths, reference_inference_paths: RunPaths, fmt: str):
    from pyspark.sql import functions as F

    keys = ["tail_id", "flight_id", "win_id", "parameter_name", "date_utc"]
    self_df = read_table(
        spark,
        str(self_paths.artifact_path("anomaly_parameter_candidate_evidence")),
        fmt=fmt,
    )
    reference_df = read_table(
        spark,
        str(reference_inference_paths.artifact_path("anomaly_parameter_candidate_evidence")),
        fmt=fmt,
    )
    matched_df = self_df.select(
        *keys,
        F.col("parameter_support_rank_in_window").alias("self_rank"),
        F.col("telemetry_retained").alias("self_telemetry_retained"),
        F.col("structural_cut_retained").alias("self_structural_cut_retained"),
    ).join(
        reference_df.select(
            *keys,
            F.col("parameter_support_rank_in_window").alias("reference_rank"),
            F.col("telemetry_retained").alias("reference_telemetry_retained"),
            F.col("structural_cut_retained").alias("reference_structural_cut_retained"),
        ),
        on=keys,
        how="inner",
    )
    return CandidateEvidenceComparison(
        self_fit_candidate_count=int(self_df.count()),
        reference_inference_candidate_count=int(reference_df.count()),
        matched_candidate_count=int(matched_df.count()),
        support_rank_change_count=int(
            matched_df.where(~F.col("self_rank").eqNullSafe(F.col("reference_rank"))).count()
        ),
        telemetry_cut_change_count=int(
            matched_df.where(
                ~F.col("self_telemetry_retained").eqNullSafe(F.col("reference_telemetry_retained"))
            ).count()
        ),
        structural_cut_change_count=int(
            matched_df.where(
                ~F.col("self_structural_cut_retained").eqNullSafe(
                    F.col("reference_structural_cut_retained")
                )
            ).count()
        ),
    )


def build_reference_inference_report(
    *,
    reference_run: PipelineRunResult,
    self_fit_run: PipelineRunResult,
    reference_inference_run: PipelineRunResult,
    artifact_lineage: tuple[ReferenceArtifactLineage, ...],
    table_format: str,
) -> ReferenceInferenceComparisonReport:
    spark = get_spark("s3ntinel.reference_inference_comparison")
    return ReferenceInferenceComparisonReport(
        reference_run_dir=str(reference_run.paths.run_dir),
        self_fit_run_dir=str(self_fit_run.paths.run_dir),
        reference_inference_run_dir=str(reference_inference_run.paths.run_dir),
        artifact_lineage=artifact_lineage,
        phase_comparison=_phase_comparison(
            spark,
            self_paths=self_fit_run.paths,
            reference_inference_paths=reference_inference_run.paths,
            fmt=table_format,
        ),
        score_comparison=_score_comparison(
            spark,
            self_paths=self_fit_run.paths,
            reference_inference_paths=reference_inference_run.paths,
            fmt=table_format,
        ),
        candidate_evidence_comparison=_candidate_comparison(
            spark,
            self_paths=self_fit_run.paths,
            reference_inference_paths=reference_inference_run.paths,
            fmt=table_format,
        ),
    )


def run_paired_reference_inference(config: PipelineRunConfig) -> PairedReferenceInferenceResult:
    from libs.simulation.cli import resolve_flight

    faulted_flight = resolve_flight(config.flight_name, sim_seed=config.sim_seed)
    nominal_flight = nominal_companion_flight(faulted_flight)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pair_dir = Path(config.base_dir) / f"{timestamp}_{config.flight_name}_reference_comparison"
    reference_config = replace(
        config,
        base_dir=str(pair_dir / "reference"),
        mode="full",
        replay_run_dir=None,
        start_stage=None,
        end_stage=None,
    )
    self_fit_config = replace(
        config,
        base_dir=str(pair_dir / "self_fit"),
        mode="full",
        replay_run_dir=None,
        start_stage=None,
        end_stage=None,
    )
    reference_run = run_pipeline(reference_config, flight_spec=nominal_flight)
    reset_spark_between_runs()
    self_fit_run = run_pipeline(self_fit_config, flight_spec=faulted_flight)
    reset_spark_between_runs()

    reference_inference_dir = pair_dir / "reference_inference" / self_fit_run.paths.run_dir.name
    shutil.copytree(self_fit_run.paths.run_dir, reference_inference_dir)
    reference_inference_paths = RunPaths(reference_inference_dir)
    artifact_lineage = stage_reference_artifacts(
        reference_paths=reference_run.paths,
        target_paths=reference_inference_paths,
    )
    reference_inference_config = replace(
        config,
        mode="reference_inference",
        replay_run_dir=str(reference_inference_dir),
        start_stage=None,
        end_stage=None,
    )
    reference_inference_run = run_pipeline(
        reference_inference_config,
        flight_spec=faulted_flight,
    )
    reset_spark_between_runs()
    report = build_reference_inference_report(
        reference_run=reference_run,
        self_fit_run=self_fit_run,
        reference_inference_run=reference_inference_run,
        artifact_lineage=artifact_lineage,
        table_format=config.table_format,
    )
    report_path = pair_dir / "reports" / "reference_inference_comparison_report.json"
    report.write(report_path)
    return PairedReferenceInferenceResult(
        pair_dir=pair_dir,
        reference_run=reference_run,
        self_fit_run=self_fit_run,
        reference_inference_run=reference_inference_run,
        report_path=report_path,
    )


if TYPE_CHECKING:
    from libs.simulation.flight.spec import FlightSpec
