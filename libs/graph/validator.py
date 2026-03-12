"""Graph and hierarchy validation helpers."""

from __future__ import annotations

from math import comb
from typing import Any

import pandas as pd


def _pairwise_partition_metrics(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> dict[str, Any]:
    if joined.empty:
        return {
            "same_cluster_pair_precision": None,
            "same_cluster_pair_recall": None,
            "same_cluster_pair_f1": None,
            "true_positive_pair_count": 0,
            "same_detected_pair_count": 0,
            "same_truth_pair_count": 0,
        }

    def _same_pair_count(series: pd.Series) -> int:
        counts = series.astype(str).value_counts(dropna=False)
        return int(sum(comb(int(count), 2) for count in counts.tolist() if int(count) >= 2))

    tp = int(
        sum(
            comb(int(count), 2)
            for count in joined.groupby([detected_col, truth_col], dropna=False).size().tolist()
            if int(count) >= 2
        )
    )
    same_detected = _same_pair_count(joined[detected_col])
    same_truth = _same_pair_count(joined[truth_col])
    precision = float(tp / same_detected) if same_detected else None
    recall = float(tp / same_truth) if same_truth else None
    if precision is None or recall is None or (precision + recall) <= 0.0:
        f1 = None
    else:
        f1 = float((2.0 * precision * recall) / (precision + recall))
    return {
        "same_cluster_pair_precision": precision,
        "same_cluster_pair_recall": recall,
        "same_cluster_pair_f1": f1,
        "true_positive_pair_count": tp,
        "same_detected_pair_count": same_detected,
        "same_truth_pair_count": same_truth,
    }


def validate_hierarchy_recovery(
    *,
    hierarchy_sensor_map_df: pd.DataFrame,
    hierarchy_label_df: pd.DataFrame,
) -> dict[str, Any]:
    if hierarchy_sensor_map_df is None or hierarchy_label_df is None or hierarchy_sensor_map_df.empty or hierarchy_label_df.empty:
        return {
            "status": "skipped",
            "reason": "missing hierarchy_sensor_map or hierarchy_label rows",
            "sensor_count": 0,
        }

    detected = hierarchy_sensor_map_df[["parameter_name", "system_id", "subsystem_id", "module_id"]].copy()
    labels = hierarchy_label_df[["parameter_name", "system_id", "subsystem_id", "module_id"]].copy()
    joined = detected.merge(labels, on="parameter_name", how="inner", suffixes=("_detected", "_truth"))
    sensor_count = int(len(joined))
    if sensor_count == 0:
        return {
            "status": "skipped",
            "reason": "no overlapping parameter_name rows between detected hierarchy and labels",
            "sensor_count": 0,
        }

    return {
        "status": "ok",
        "sensor_count": sensor_count,
        "system_exact_match": float((joined["system_id_detected"] == joined["system_id_truth"]).mean()),
        "subsystem_exact_match": float((joined["subsystem_id_detected"] == joined["subsystem_id_truth"]).mean()),
        "module_exact_match": float((joined["module_id_detected"] == joined["module_id_truth"]).mean()),
        "system_partition": _pairwise_partition_metrics(
            joined,
            detected_col="system_id_detected",
            truth_col="system_id_truth",
        ),
        "subsystem_partition": _pairwise_partition_metrics(
            joined,
            detected_col="subsystem_id_detected",
            truth_col="subsystem_id_truth",
        ),
        "module_partition": _pairwise_partition_metrics(
            joined,
            detected_col="module_id_detected",
            truth_col="module_id_truth",
        ),
        "truth_system_count": int(labels["system_id"].astype(str).nunique()),
        "truth_subsystem_count": int(labels["subsystem_id"].astype(str).nunique()),
        "truth_module_count": int(labels["module_id"].astype(str).nunique()),
        "detected_system_count": int(detected["system_id"].astype(str).nunique()),
        "detected_subsystem_count": int(detected["subsystem_id"].astype(str).nunique()),
        "detected_module_count": int(detected["module_id"].astype(str).nunique()),
        "detected_nontrivial_system_partition": bool(detected["system_id"].astype(str).nunique() > 1),
        "detected_nontrivial_subsystem_partition": bool(detected["subsystem_id"].astype(str).nunique() > 1),
        "detected_nontrivial_module_partition": bool(detected["module_id"].astype(str).nunique() > 1),
    }


def validate_expected_graph_signatures(
    *,
    lag_graph_df: pd.DataFrame | None = None,
    fused_graph_df: pd.DataFrame | None = None,
    expected_lag_edges: tuple[dict[str, str], ...] = (),
    expected_fused_edges: tuple[dict[str, str], ...] = (),
) -> dict[str, Any]:
    lag_rows = lag_graph_df if lag_graph_df is not None else pd.DataFrame()
    fused_rows = fused_graph_df if fused_graph_df is not None else pd.DataFrame()

    lag_index = {
        (str(row["parameter_name_u"]), str(row["parameter_name_v"])): float(row.get("lag_weight", 0.0) or 0.0)
        for row in lag_rows.to_dict(orient="records")
    }
    fused_index = {
        tuple(sorted((str(row["parameter_name_u"]), str(row["parameter_name_v"])))): float(row.get("fused_weight", 0.0) or 0.0)
        for row in fused_rows.to_dict(orient="records")
    }

    lag_edge_rows = []
    for edge in expected_lag_edges:
        key = (str(edge["parameter_name_u"]), str(edge["parameter_name_v"]))
        reverse_key = (key[1], key[0])
        lag_edge_rows.append(
            {
                "parameter_name_u": key[0],
                "parameter_name_v": key[1],
                "present_forward": key in lag_index,
                "present_reverse": reverse_key in lag_index,
                "present_any_direction": (key in lag_index) or (reverse_key in lag_index),
                "lag_weight": lag_index.get(key),
                "reverse_lag_weight": lag_index.get(reverse_key),
            }
        )

    fused_edge_rows = []
    for edge in expected_fused_edges:
        key = tuple(sorted((str(edge["parameter_name_u"]), str(edge["parameter_name_v"]))))
        fused_edge_rows.append(
            {
                "parameter_name_u": key[0],
                "parameter_name_v": key[1],
                "present": key in fused_index,
                "fused_weight": fused_index.get(key),
            }
        )

    return {
        "status": "ok",
        "lag_expected_edge_count": len(lag_edge_rows),
        "lag_expected_edge_hit_rate": (
            float(sum(1 for row in lag_edge_rows if row["present_any_direction"]) / len(lag_edge_rows))
            if lag_edge_rows
            else None
        ),
        "lag_expected_edge_hit_rate_forward": (
            float(sum(1 for row in lag_edge_rows if row["present_forward"]) / len(lag_edge_rows))
            if lag_edge_rows
            else None
        ),
        "lag_expected_edge_hit_rate_any_direction": (
            float(sum(1 for row in lag_edge_rows if row["present_any_direction"]) / len(lag_edge_rows))
            if lag_edge_rows
            else None
        ),
        "lag_edges": lag_edge_rows,
        "fused_expected_edge_count": len(fused_edge_rows),
        "fused_expected_edge_hit_rate": (
            float(sum(1 for row in fused_edge_rows if row["present"]) / len(fused_edge_rows))
            if fused_edge_rows
            else None
        ),
        "fused_edges": fused_edge_rows,
    }


def build_graph_validation_summary(
    *,
    hierarchy_sensor_map_df: pd.DataFrame,
    hierarchy_label_df: pd.DataFrame,
    lag_graph_df: pd.DataFrame | None = None,
    fused_graph_df: pd.DataFrame | None = None,
    expected_lag_edges: tuple[dict[str, str], ...] = (),
    expected_fused_edges: tuple[dict[str, str], ...] = (),
) -> dict[str, Any]:
    return {
        "hierarchy": validate_hierarchy_recovery(
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            hierarchy_label_df=hierarchy_label_df,
        ),
        "graph_signatures": validate_expected_graph_signatures(
            lag_graph_df=lag_graph_df,
            fused_graph_df=fused_graph_df,
            expected_lag_edges=expected_lag_edges,
            expected_fused_edges=expected_fused_edges,
        ),
    }
