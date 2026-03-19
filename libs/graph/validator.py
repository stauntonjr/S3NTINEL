"""Graph and hierarchy validation helpers."""

from __future__ import annotations

from math import exp
from math import lgamma
from math import log
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


def _entropy(series: pd.Series) -> float:
    counts = series.astype(str).value_counts(dropna=False)
    total = float(counts.sum())
    if total <= 0.0:
        return 0.0
    entropy = 0.0
    for count in counts.tolist():
        probability = float(count) / total
        if probability > 0.0:
            entropy -= probability * log(probability)
    return float(entropy)


def _mutual_information(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> float:
    if joined.empty:
        return 0.0
    total = float(len(joined))
    if total <= 0.0:
        return 0.0

    detected_counts = joined[detected_col].astype(str).value_counts(dropna=False).to_dict()
    truth_counts = joined[truth_col].astype(str).value_counts(dropna=False).to_dict()
    joint_counts = (
        joined.groupby([detected_col, truth_col], dropna=False).size().to_dict()
    )

    mutual_information = 0.0
    for (detected_value, truth_value), joint_count in joint_counts.items():
        p_xy = float(joint_count) / total
        if p_xy <= 0.0:
            continue
        p_x = float(detected_counts[str(detected_value)]) / total
        p_y = float(truth_counts[str(truth_value)]) / total
        if p_x <= 0.0 or p_y <= 0.0:
            continue
        mutual_information += p_xy * log(p_xy / (p_x * p_y))
    return float(mutual_information)


def _contingency_counts(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> tuple[list[int], list[int], list[int], int]:
    if joined.empty:
        return [], [], [], 0

    detected_counts = joined[detected_col].astype(str).value_counts(dropna=False)
    truth_counts = joined[truth_col].astype(str).value_counts(dropna=False)
    joint_counts = joined.groupby([detected_col, truth_col], dropna=False).size()
    return (
        [int(value) for value in detected_counts.tolist()],
        [int(value) for value in truth_counts.tolist()],
        [int(value) for value in joint_counts.tolist()],
        int(len(joined)),
    )


def _adjusted_rand_index(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> float | None:
    detected_marginals, truth_marginals, joint_counts, sample_count = _contingency_counts(
        joined,
        detected_col=detected_col,
        truth_col=truth_col,
    )
    if sample_count <= 1:
        return 1.0 if sample_count == 1 else None

    index = float(sum(comb(count, 2) for count in joint_counts if count >= 2))
    detected_pairs = float(sum(comb(count, 2) for count in detected_marginals if count >= 2))
    truth_pairs = float(sum(comb(count, 2) for count in truth_marginals if count >= 2))
    total_pairs = float(comb(sample_count, 2))
    if total_pairs <= 0.0:
        return None

    expected_index = (detected_pairs * truth_pairs) / total_pairs
    max_index = 0.5 * (detected_pairs + truth_pairs)
    denominator = max_index - expected_index
    if abs(denominator) <= 1e-12:
        return 1.0
    return float((index - expected_index) / denominator)


def _log_comb(n: int, k: int) -> float:
    if k < 0 or k > n:
        return float("-inf")
    return float(lgamma(n + 1) - lgamma(k + 1) - lgamma(n - k + 1))


def _expected_mutual_information(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> float:
    detected_marginals, truth_marginals, _, sample_count = _contingency_counts(
        joined,
        detected_col=detected_col,
        truth_col=truth_col,
    )
    if sample_count <= 0:
        return 0.0

    expected_mutual_information = 0.0
    for detected_count in detected_marginals:
        for truth_count in truth_marginals:
            lower = max(1, detected_count + truth_count - sample_count)
            upper = min(detected_count, truth_count)
            for joint_count in range(lower, upper + 1):
                p_xy = float(joint_count) / float(sample_count)
                if p_xy <= 0.0:
                    continue
                log_probability = (
                    _log_comb(detected_count, joint_count)
                    + _log_comb(sample_count - detected_count, truth_count - joint_count)
                    - _log_comb(sample_count, truth_count)
                )
                probability = exp(log_probability)
                if probability <= 0.0:
                    continue
                expected_mutual_information += probability * p_xy * log(
                    (float(sample_count) * float(joint_count)) / (float(detected_count) * float(truth_count))
                )
    return float(expected_mutual_information)


def _normalized_mutual_information(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> float | None:
    if joined.empty:
        return None
    detected_entropy = _entropy(joined[detected_col])
    truth_entropy = _entropy(joined[truth_col])
    denominator = detected_entropy + truth_entropy
    if denominator <= 0.0:
        return 1.0
    return float((2.0 * _mutual_information(joined, detected_col=detected_col, truth_col=truth_col)) / denominator)


def _adjusted_mutual_information(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> float | None:
    if joined.empty:
        return None
    mutual_information = _mutual_information(joined, detected_col=detected_col, truth_col=truth_col)
    expected_mutual_information = _expected_mutual_information(joined, detected_col=detected_col, truth_col=truth_col)
    denominator = (0.5 * (_entropy(joined[detected_col]) + _entropy(joined[truth_col]))) - expected_mutual_information
    if abs(denominator) <= 1e-12:
        return 1.0
    return float((mutual_information - expected_mutual_information) / denominator)


def _cluster_agreement_metrics(joined: pd.DataFrame, *, detected_col: str, truth_col: str) -> dict[str, Any]:
    return {
        **_pairwise_partition_metrics(joined, detected_col=detected_col, truth_col=truth_col),
        "normalized_mutual_information": _normalized_mutual_information(
            joined,
            detected_col=detected_col,
            truth_col=truth_col,
        ),
        "adjusted_mutual_information": _adjusted_mutual_information(
            joined,
            detected_col=detected_col,
            truth_col=truth_col,
        ),
        "adjusted_rand_index": _adjusted_rand_index(
            joined,
            detected_col=detected_col,
            truth_col=truth_col,
        ),
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
        "system_partition": _cluster_agreement_metrics(
            joined,
            detected_col="system_id_detected",
            truth_col="system_id_truth",
        ),
        "subsystem_partition": _cluster_agreement_metrics(
            joined,
            detected_col="subsystem_id_detected",
            truth_col="subsystem_id_truth",
        ),
        "module_partition": _cluster_agreement_metrics(
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


def build_coupling_validation_summary(
    *,
    coupling_truth_df: pd.DataFrame,
    lag_graph_df: pd.DataFrame | None = None,
    precision_graph_df: pd.DataFrame | None = None,
    fused_graph_df: pd.DataFrame | None = None,
    expected_coupling_signatures: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    if coupling_truth_df is None or coupling_truth_df.empty:
        return {
            "status": "skipped",
            "reason": "missing coupling misbehavior truth rows",
            "coupling_window_count": 0,
        }

    lag_rows = lag_graph_df if lag_graph_df is not None else pd.DataFrame()
    precision_rows = precision_graph_df if precision_graph_df is not None else pd.DataFrame()
    fused_rows = fused_graph_df if fused_graph_df is not None else pd.DataFrame()

    lag_index = {
        (str(row["parameter_name_u"]), str(row["parameter_name_v"])): {
            "lag_weight": float(row.get("lag_weight", 0.0) or 0.0),
            "mean_lag_seconds": row.get("mean_lag_seconds"),
        }
        for row in lag_rows.to_dict(orient="records")
    }
    precision_index = {
        tuple(sorted((str(row["parameter_name_u"]), str(row["parameter_name_v"])))): {
            "partial_corr": row.get("partial_corr"),
            "precision_weight": row.get("precision_weight"),
        }
        for row in precision_rows.to_dict(orient="records")
    }
    fused_index = {
        tuple(sorted((str(row["parameter_name_u"]), str(row["parameter_name_v"])))): float(row.get("fused_weight", 0.0) or 0.0)
        for row in fused_rows.to_dict(orient="records")
    }

    signature_rows = []
    for signature in expected_coupling_signatures:
        key = (str(signature["parameter_name_u"]), str(signature["parameter_name_v"]))
        unordered_key = tuple(sorted(key))
        lag_payload = lag_index.get(key) or lag_index.get((key[1], key[0])) or {}
        precision_payload = precision_index.get(unordered_key) or {}
        fused_weight = fused_index.get(unordered_key)
        signature_type = str(signature.get("signature_type", "edge_present"))
        partial_corr = precision_payload.get("partial_corr")
        if signature_type == "lag_shift":
            hit = bool(lag_payload.get("mean_lag_seconds") is not None)
        elif signature_type == "sign_flip":
            hit = bool(partial_corr is not None and float(partial_corr) < 0.0)
        else:
            hit = bool(lag_payload or precision_payload or fused_weight is not None)
        signature_rows.append(
            {
                "coupling_id": str(signature.get("coupling_id", "")),
                "parameter_name_u": key[0],
                "parameter_name_v": key[1],
                "signature_type": signature_type,
                "hit": hit,
                "lag_weight": lag_payload.get("lag_weight"),
                "mean_lag_seconds": lag_payload.get("mean_lag_seconds"),
                "partial_corr": partial_corr,
                "precision_weight": precision_payload.get("precision_weight"),
                "fused_weight": fused_weight,
            }
        )

    coupling_windows = (
        coupling_truth_df[
            [
                "coupling_id",
                "misbehavior_window_id",
                "misbehavior_family_label",
                "misbehavior_detail_label",
                "start_step",
                "end_step_exclusive",
            ]
        ]
        .drop_duplicates()
        .sort_values(["coupling_id", "start_step", "misbehavior_window_id"])
    )

    return {
        "status": "ok",
        "coupling_window_count": int(len(coupling_windows)),
        "coupling_id_count": int(coupling_windows["coupling_id"].astype(str).nunique()),
        "misbehavior_detail_counts": {
            str(key): int(value)
            for key, value in coupling_windows["misbehavior_detail_label"].astype(str).value_counts().to_dict().items()
        },
        "signature_count": len(signature_rows),
        "signature_hit_rate": (
            float(sum(1 for row in signature_rows if row["hit"]) / len(signature_rows))
            if signature_rows
            else None
        ),
        "signatures": signature_rows,
    }
