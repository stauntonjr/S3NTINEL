from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

from libs.graph.fused import FusedGraphSpec
from libs.graph.hierarchy_artifacts import GraphHierarchy, HierarchySpec
from libs.graph.lag import LagBandSpec, resolve_lag_band_specs
from libs.graph.pipeline import (
    build_fused_graph_spark_table,
    build_graph_components_with_diagnostics_spark_table,
    collapse_lag_profile_spark_table,
)
from libs.graph.validator import _cluster_agreement_metrics
from libs.io.schemas import PRECISION_GRAPH_SCHEMA


def _spark_functions():
    from pyspark.sql import functions as F

    return F


@dataclass(frozen=True)
class GraphStageEvaluationSpec:
    stability_sample_fraction: float = 0.8
    stability_sample_count: int = 2
    stability_hash_modulus: int = 10


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _edge_keys(pdf: pd.DataFrame, *, directed: bool) -> set[tuple[str, str]]:
    if pdf.empty:
        return set()
    keys: set[tuple[str, str]] = set()
    for row in pdf[["parameter_name_u", "parameter_name_v"]].to_dict(orient="records"):
        left = str(row["parameter_name_u"])
        right = str(row["parameter_name_v"])
        if directed:
            keys.add((left, right))
        else:
            keys.add(tuple(sorted((left, right))))
    return keys


def _jaccard(left: set[tuple[str, str]], right: set[tuple[str, str]]) -> float | None:
    if not left and not right:
        return None
    union = left | right
    if not union:
        return None
    return float(len(left & right) / len(union))


def _mean(values: list[float | None]) -> float | None:
    usable = [float(item) for item in values if item is not None]
    if not usable:
        return None
    return float(sum(usable) / len(usable))


def _precision_pandas_to_spark_table(spark: Any, precision_df: pd.DataFrame) -> Any:
    if precision_df.empty:
        return spark.createDataFrame([], schema=PRECISION_GRAPH_SCHEMA())
    return spark.createDataFrame(precision_df)


def _compare_detected_hierarchies(reference_df: pd.DataFrame, candidate_df: pd.DataFrame) -> dict[str, Any]:
    if reference_df.empty or candidate_df.empty:
        return {"status": "skipped", "reason": "missing hierarchy rows"}

    joined = reference_df.merge(candidate_df, on="parameter_name", how="inner", suffixes=("_reference", "_candidate"))
    if joined.empty:
        return {"status": "skipped", "reason": "no overlapping parameter names"}

    return {
        "status": "ok",
        "overlap_parameter_count": int(len(joined)),
        "system_partition": _cluster_agreement_metrics(
            joined,
            detected_col="system_id_candidate",
            truth_col="system_id_reference",
        ),
        "subsystem_partition": _cluster_agreement_metrics(
            joined,
            detected_col="subsystem_id_candidate",
            truth_col="subsystem_id_reference",
        ),
        "module_partition": _cluster_agreement_metrics(
            joined,
            detected_col="module_id_candidate",
            truth_col="module_id_reference",
        ),
    }


def _lag_profile_band_skew_report(lag_profile_pdf: pd.DataFrame) -> dict[str, Any]:
    if lag_profile_pdf.empty:
        return {"status": "skipped", "reason": "lag_profile is empty", "bands": []}

    grouped = (
        lag_profile_pdf.groupby("lag_band", dropna=False)
        .agg(
            edge_count=("lag_band", "size"),
            total_lag_count=("lag_count", "sum"),
            total_lag_weight=("lag_weight", "sum"),
            mean_lag_seconds=("mean_lag_seconds", "mean"),
            mean_support_flight_count=("support_flight_count", "mean"),
            median_support_flight_count=("support_flight_count", "median"),
            max_support_flight_count=("support_flight_count", "max"),
        )
        .reset_index()
        .sort_values("lag_band")
    )
    total_edge_count = int(grouped["edge_count"].sum())
    total_lag_count = float(grouped["total_lag_count"].sum())
    total_lag_weight = float(grouped["total_lag_weight"].sum())

    bands: list[dict[str, Any]] = []
    for row in grouped.to_dict(orient="records"):
        edge_count = int(row["edge_count"])
        lag_count = float(row["total_lag_count"] or 0.0)
        lag_weight = float(row["total_lag_weight"] or 0.0)
        bands.append(
            {
                "lag_band": str(row["lag_band"]),
                "edge_count": edge_count,
                "edge_share": float(edge_count / total_edge_count) if total_edge_count else None,
                "total_lag_count": int(round(lag_count)),
                "lag_count_share": float(lag_count / total_lag_count) if total_lag_count > 0.0 else None,
                "total_lag_weight": lag_weight,
                "lag_weight_share": float(lag_weight / total_lag_weight) if total_lag_weight > 0.0 else None,
                "mean_lag_seconds": _safe_float(row["mean_lag_seconds"]),
                "mean_support_flight_count": _safe_float(row["mean_support_flight_count"]),
                "median_support_flight_count": _safe_float(row["median_support_flight_count"]),
                "max_support_flight_count": int(row["max_support_flight_count"] or 0),
            }
        )
    return {
        "status": "ok",
        "band_count": len(bands),
        "total_edge_count": total_edge_count,
        "total_lag_count": int(round(total_lag_count)),
        "bands": bands,
    }


def _weight_scenarios(resolved_bands: tuple[LagBandSpec, ...]) -> list[tuple[str, tuple[LagBandSpec, ...]]]:
    if len(resolved_bands) <= 1:
        return []

    uniform = tuple(
        LagBandSpec(
            name=band.name,
            lower_seconds=band.lower_seconds,
            upper_seconds=band.upper_seconds,
            combine_weight=1.0,
        )
        for band in resolved_bands
    )
    front_loaded = tuple(
        LagBandSpec(
            name=band.name,
            lower_seconds=band.lower_seconds,
            upper_seconds=band.upper_seconds,
            combine_weight=float(1.0 / float(index + 1)),
        )
        for index, band in enumerate(resolved_bands)
    )
    back_loaded = tuple(
        LagBandSpec(
            name=band.name,
            lower_seconds=band.lower_seconds,
            upper_seconds=band.upper_seconds,
            combine_weight=float(index + 1) / float(len(resolved_bands)),
        )
        for index, band in enumerate(resolved_bands)
    )
    return [
        ("uniform", uniform),
        ("front_loaded", front_loaded),
        ("back_loaded", back_loaded),
    ]


def _hierarchy_sensitivity_report(
    *,
    spark: Any,
    precision_pdf: pd.DataFrame,
    event_sdf: Any,
    lag_profile_sdf: Any,
    fused_sdf: Any,
    parameter_names: list[str],
    resolved_bands: tuple[LagBandSpec, ...],
    tau_max_seconds: float,
    lag_min_count: int,
    max_mean_lag_seconds: float | None,
    lag_top_k_outgoing: int,
    fusion_spec: FusedGraphSpec,
    hierarchy_spec: HierarchySpec,
) -> dict[str, Any]:
    if not parameter_names:
        return {"status": "skipped", "reason": "parameter universe is empty", "scenarios": []}

    baseline_hierarchy = GraphHierarchy.from_fused_spark(
        fused_sdf,
        parameter_names=parameter_names,
        spec=hierarchy_spec,
    ).rows
    precision_sdf = _precision_pandas_to_spark_table(spark, precision_pdf)
    baseline_fused_keys = _edge_keys(fused_sdf.toPandas(), directed=False)

    scenarios: list[dict[str, Any]] = []
    for scenario_name, scenario_bands in _weight_scenarios(resolved_bands):
        alt_lag_sdf = collapse_lag_profile_spark_table(
            lag_profile_sdf,
            tau_max_seconds=tau_max_seconds,
            bands=scenario_bands,
            min_count=lag_min_count,
            max_mean_lag_seconds=max_mean_lag_seconds,
            top_k_outgoing=lag_top_k_outgoing,
        )
        alt_fused_sdf = build_fused_graph_spark_table(
            precision_sdf,
            event_sdf,
            alt_lag_sdf,
            alpha=fusion_spec.alpha,
            beta=fusion_spec.beta,
            gamma=fusion_spec.gamma,
        )
        alt_hierarchy = GraphHierarchy.from_fused_spark(
            alt_fused_sdf,
            parameter_names=parameter_names,
            spec=hierarchy_spec,
        ).rows
        alt_fused_pdf = alt_fused_sdf.toPandas()
        scenarios.append(
            {
                "scenario_name": scenario_name,
                "band_weights": {band.name: float(band.combine_weight) for band in scenario_bands},
                "lag_edge_count": int(alt_lag_sdf.count()),
                "fused_edge_count": int(len(alt_fused_pdf)),
                "fused_edge_jaccard_vs_configured": _jaccard(baseline_fused_keys, _edge_keys(alt_fused_pdf, directed=False)),
                "hierarchy_partition_agreement_vs_configured": _compare_detected_hierarchies(
                    baseline_hierarchy,
                    alt_hierarchy,
                ),
            }
        )
    return {
        "status": "ok",
        "configured_band_weights": {band.name: float(band.combine_weight) for band in resolved_bands},
        "scenario_count": len(scenarios),
        "scenarios": scenarios,
    }


def _stability_subset_report(
    *,
    events_df: Any,
    windows_df: Any,
    window_features_df: Any,
    backbone_df: Any,
    full_event_sdf: Any,
    full_lag_sdf: Any,
    full_transition_sdf: Any,
    full_fused_sdf: Any,
    lag_tau_max_seconds: float,
    lag_bands: tuple[LagBandSpec, ...],
    min_event_count: int,
    min_event_npmi: float,
    event_top_k_per_parameter_name: int,
    min_lag_count: int,
    max_mean_lag_seconds: float | None,
    lag_top_k_outgoing: int,
    min_transition_count: int,
    precision_ridge_lambda: float,
    min_abs_partial_corr: float,
    alpha: float,
    beta: float,
    gamma: float,
    max_graph_sensor_universe: int,
    spec: GraphStageEvaluationSpec,
) -> dict[str, Any]:
    F = _spark_functions()

    flight_keys = events_df.select("tail_id", "flight_id").distinct().cache()
    try:
        total_flight_count = int(flight_keys.count())
        if total_flight_count < 2:
            return {"status": "skipped", "reason": "need at least two flights for subset stability", "flight_count": total_flight_count}

        threshold = max(1, min(int(round(spec.stability_hash_modulus * spec.stability_sample_fraction)), spec.stability_hash_modulus - 1))
        full_event_keys = _edge_keys(full_event_sdf.toPandas(), directed=False)
        full_lag_keys = _edge_keys(full_lag_sdf.toPandas(), directed=True)
        full_transition_keys = _edge_keys(full_transition_sdf.toPandas(), directed=True)
        full_fused_keys = _edge_keys(full_fused_sdf.toPandas(), directed=False)

        sample_rows: list[dict[str, Any]] = []
        for sample_index in range(max(int(spec.stability_sample_count), 0)):
            sample_key_df = flight_keys.where(
                F.pmod(F.xxhash64("tail_id", "flight_id", F.lit(f"graph_eval_{sample_index}")), F.lit(spec.stability_hash_modulus))
                < F.lit(threshold)
            )
            sample_flight_count = int(sample_key_df.count())
            if sample_flight_count <= 0 or sample_flight_count >= total_flight_count:
                continue
            sample_events_df = events_df.join(sample_key_df, on=["tail_id", "flight_id"], how="inner")
            sample_windows_df = windows_df.join(sample_key_df, on=["tail_id", "flight_id"], how="inner")
            sample_window_features_df = window_features_df.join(sample_key_df, on=["tail_id", "flight_id"], how="inner")
            if sample_events_df.limit(1).count() == 0 or sample_windows_df.limit(1).count() == 0 or sample_window_features_df.limit(1).count() == 0:
                continue
            _, event_sdf, lag_sdf, transition_sdf, fused_sdf, _, _ = build_graph_components_with_diagnostics_spark_table(
                sample_window_features_df,
                sample_events_df,
                sample_windows_df,
                backbone_df,
                precision_ridge_lambda=precision_ridge_lambda,
                min_abs_partial_corr=min_abs_partial_corr,
                min_event_count=min_event_count,
                min_event_npmi=min_event_npmi,
                event_top_k_per_parameter_name=event_top_k_per_parameter_name,
                lag_tau_max_seconds=lag_tau_max_seconds,
                min_lag_count=min_lag_count,
                max_mean_lag_seconds=max_mean_lag_seconds,
                lag_top_k_outgoing=lag_top_k_outgoing,
                lag_bands=lag_bands,
                min_transition_count=min_transition_count,
                alpha=alpha,
                beta=beta,
                gamma=gamma,
                max_graph_sensor_universe=max_graph_sensor_universe,
            )
            sample_rows.append(
                {
                    "sample_name": f"hash_sample_{sample_index + 1}",
                    "flight_count": sample_flight_count,
                    "event_edge_count": int(event_sdf.count()),
                    "lag_edge_count": int(lag_sdf.count()),
                    "transition_edge_count": int(transition_sdf.count()),
                    "fused_edge_count": int(fused_sdf.count()),
                    "event_jaccard_vs_full": _jaccard(full_event_keys, _edge_keys(event_sdf.toPandas(), directed=False)),
                    "lag_jaccard_vs_full": _jaccard(full_lag_keys, _edge_keys(lag_sdf.toPandas(), directed=True)),
                    "transition_jaccard_vs_full": _jaccard(full_transition_keys, _edge_keys(transition_sdf.toPandas(), directed=True)),
                    "fused_jaccard_vs_full": _jaccard(full_fused_keys, _edge_keys(fused_sdf.toPandas(), directed=False)),
                }
            )

        return {
            "status": "ok" if sample_rows else "skipped",
            "reason": None if sample_rows else "no valid subset samples were produced",
            "flight_count": total_flight_count,
            "sample_fraction": float(spec.stability_sample_fraction),
            "sample_count": len(sample_rows),
            "mean_event_jaccard_vs_full": _mean([row["event_jaccard_vs_full"] for row in sample_rows]),
            "mean_lag_jaccard_vs_full": _mean([row["lag_jaccard_vs_full"] for row in sample_rows]),
            "mean_transition_jaccard_vs_full": _mean([row["transition_jaccard_vs_full"] for row in sample_rows]),
            "mean_fused_jaccard_vs_full": _mean([row["fused_jaccard_vs_full"] for row in sample_rows]),
            "samples": sample_rows,
        }
    finally:
        flight_keys.unpersist()


def build_graph_stage_evaluation_report_spark(
    *,
    spark: Any,
    events_df: Any,
    windows_df: Any,
    window_features_df: Any,
    backbone_df: Any,
    precision_df: pd.DataFrame,
    event_sdf: Any,
    lag_profile_sdf: Any,
    lag_sdf: Any,
    transition_sdf: Any,
    fused_sdf: Any,
    parameter_universe_df: Any,
    precision_ridge_lambda: float,
    min_abs_partial_corr: float,
    min_event_count: int,
    min_event_npmi: float,
    event_top_k_per_parameter_name: int,
    lag_tau_max_seconds: float,
    lag_bands: tuple[LagBandSpec, ...] | None,
    min_lag_count: int,
    max_mean_lag_seconds: float | None,
    lag_top_k_outgoing: int,
    min_transition_count: int,
    alpha: float,
    beta: float,
    gamma: float,
    max_graph_sensor_universe: int,
    hierarchy_spec: HierarchySpec,
    evaluation_spec: GraphStageEvaluationSpec | None = None,
) -> dict[str, Any]:
    eval_spec = evaluation_spec or GraphStageEvaluationSpec()
    resolved_bands = resolve_lag_band_specs(lag_bands, tau_max_seconds=lag_tau_max_seconds)
    parameter_names = [str(row["parameter_name"]) for row in parameter_universe_df.select("parameter_name").collect()]
    lag_profile_pdf = lag_profile_sdf.toPandas()

    return {
        "status": "ok",
        "graph_counts": {
            "precision_edge_count": int(len(precision_df)),
            "event_edge_count": int(event_sdf.count()),
            "lag_profile_edge_count": int(len(lag_profile_pdf)),
            "lag_edge_count": int(lag_sdf.count()),
            "transition_edge_count": int(transition_sdf.count()),
            "fused_edge_count": int(fused_sdf.count()),
            "parameter_universe_count": len(parameter_names),
        },
        "lag_profile_band_skew": _lag_profile_band_skew_report(lag_profile_pdf),
        "edge_stability": _stability_subset_report(
            events_df=events_df,
            windows_df=windows_df,
            window_features_df=window_features_df,
            backbone_df=backbone_df,
            full_event_sdf=event_sdf,
            full_lag_sdf=lag_sdf,
            full_transition_sdf=transition_sdf,
            full_fused_sdf=fused_sdf,
            lag_tau_max_seconds=lag_tau_max_seconds,
            lag_bands=resolved_bands,
            min_event_count=min_event_count,
            min_event_npmi=min_event_npmi,
            event_top_k_per_parameter_name=event_top_k_per_parameter_name,
            min_lag_count=min_lag_count,
            max_mean_lag_seconds=max_mean_lag_seconds,
            lag_top_k_outgoing=lag_top_k_outgoing,
            min_transition_count=min_transition_count,
            precision_ridge_lambda=precision_ridge_lambda,
            min_abs_partial_corr=min_abs_partial_corr,
            alpha=alpha,
            beta=beta,
            gamma=gamma,
            max_graph_sensor_universe=max_graph_sensor_universe,
            spec=eval_spec,
        ),
        "hierarchy_sensitivity": _hierarchy_sensitivity_report(
            spark=spark,
            precision_pdf=precision_df,
            event_sdf=event_sdf,
            lag_profile_sdf=lag_profile_sdf,
            fused_sdf=fused_sdf,
            parameter_names=parameter_names,
            resolved_bands=resolved_bands,
            tau_max_seconds=lag_tau_max_seconds,
            lag_min_count=min_lag_count,
            max_mean_lag_seconds=max_mean_lag_seconds,
            lag_top_k_outgoing=lag_top_k_outgoing,
            fusion_spec=FusedGraphSpec(alpha=alpha, beta=beta, gamma=gamma),
            hierarchy_spec=hierarchy_spec,
        ),
    }


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
