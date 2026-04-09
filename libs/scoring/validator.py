"""Score validation helpers against simulator misbehavior truth with fault wrappers."""

from __future__ import annotations

from typing import Any

import pandas as pd

STRICT_WINDOW_COVERAGE_MIN_RATIO = 0.5
# Backward-compatible alias retained for generated-doc references that import this name.
STRICT_TRUTH_COVERAGE_MIN_RATIO = STRICT_WINDOW_COVERAGE_MIN_RATIO
STRICT_MAX_EARLY_LEAD_SECONDS = 0.0
RAW_SCORE_TOP_K_BUDGETS = (1, 5, 10, 25, 50)
CALIBRATED_P_VALUE_THRESHOLDS = (0.01, 0.05, 0.10)
DIAGNOSTIC_WINDOW_SAMPLE_LIMIT = 32


def _to_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _text_series(df: pd.DataFrame, primary: str, fallback: str | None = None) -> pd.Series:
    if primary in df.columns:
        return df[primary].fillna("").astype(str)
    if fallback and fallback in df.columns:
        return df[fallback].fillna("").astype(str)
    return pd.Series("", index=df.index, dtype="object")


def _bool_series(df: pd.DataFrame, primary: str, fallback: str | None = None) -> pd.Series:
    if primary in df.columns:
        return df[primary].fillna(False).astype(bool)
    if fallback and fallback in df.columns:
        return df[fallback].fillna(False).astype(bool)
    return pd.Series(False, index=df.index, dtype="bool")


def _float_series(df: pd.DataFrame, primary: str, fallback: str | None = None) -> pd.Series:
    if primary in df.columns:
        return pd.to_numeric(df[primary], errors="coerce").astype("float64")
    if fallback and fallback in df.columns:
        return pd.to_numeric(df[fallback], errors="coerce").astype("float64")
    return pd.Series(dtype="float64")


def _score_join_keys(left_df: pd.DataFrame, right_df: pd.DataFrame) -> list[str]:
    keys = ["tail_id", "flight_id", "win_id"]
    if "date_utc" in left_df.columns and "date_utc" in right_df.columns:
        keys.append("date_utc")
    return keys


def _threshold_key(value: float) -> str:
    return f"p_le_{str(value).replace('.', 'p')}"


def _top_k_key(value: int) -> str:
    return f"top_{int(value)}"


def _json_timestamp(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(timestamp) else str(timestamp.isoformat())


def _non_empty_strings(values: pd.Series) -> list[str]:
    if values is None or values.empty:
        return []
    cleaned = values.fillna("").astype(str)
    return sorted({value for value in cleaned if value})


def extract_misbehavior_truth_windows(raw_telemetry_df: pd.DataFrame) -> pd.DataFrame:
    if raw_telemetry_df is None or raw_telemetry_df.empty:
        return pd.DataFrame()
    rows = raw_telemetry_df.copy()
    rows["misbehavior_active"] = _bool_series(rows, "misbehavior_active", "fault_active")
    rows["misbehavior_window_id"] = _text_series(rows, "misbehavior_window_id", "fault_window_id")
    rows["misbehavior_family_label"] = _text_series(rows, "misbehavior_family_label")
    rows["misbehavior_detail_label"] = _text_series(rows, "misbehavior_detail_label", "fault_type")
    rows["fault_window_id"] = _text_series(rows, "fault_window_id", "misbehavior_window_id")
    rows["fault_family_label"] = _text_series(rows, "fault_family_label", "behavior_family_label")
    rows["fault_type"] = _text_series(rows, "fault_type", "misbehavior_detail_label")
    rows["timestamp_utc"] = _to_utc(rows["timestamp_utc"])
    active = rows[(rows["misbehavior_active"]) & (rows["misbehavior_window_id"] != "")]
    if active.empty:
        return pd.DataFrame()
    return (
        active.groupby(["tail_id", "flight_id", "misbehavior_window_id"], dropna=False)
        .agg(
            misbehavior_start_timestamp_utc=("timestamp_utc", "min"),
            misbehavior_end_timestamp_utc=("timestamp_utc", "max"),
            misbehavior_family_label=("misbehavior_family_label", "first"),
            misbehavior_detail_label=("misbehavior_detail_label", "first"),
            fault_window_id=("fault_window_id", "first"),
            fault_family_label=("fault_family_label", "first"),
            fault_type=("fault_type", "first"),
            system_id=("system_id", "first"),
            subsystem_id=("subsystem_id", "first"),
            module_id=("module_id", "first"),
            parameter_name=("parameter_name", "first"),
        )
        .reset_index()
    )


def extract_fault_truth_windows(raw_telemetry_df: pd.DataFrame) -> pd.DataFrame:
    misbehavior_df = extract_misbehavior_truth_windows(raw_telemetry_df)
    if misbehavior_df.empty:
        return pd.DataFrame()
    fault_df = misbehavior_df.rename(
        columns={
            "misbehavior_window_id": "fault_window_id",
            "misbehavior_start_timestamp_utc": "fault_start_timestamp_utc",
            "misbehavior_end_timestamp_utc": "fault_end_timestamp_utc",
        }
    ).copy()
    fault_df["fault_family_label"] = fault_df["fault_family_label"].fillna("").astype(str)
    fault_df["fault_type"] = fault_df["fault_type"].fillna("").astype(str)
    return fault_df


def build_truth_window_overlap_table(
    *,
    window_like_df: pd.DataFrame,
    truth_df: pd.DataFrame,
    start_field: str,
    end_field: str,
) -> pd.DataFrame:
    expected_columns = [
        *list(window_like_df.columns),
        *[column for column in truth_df.columns if column not in window_like_df.columns],
        "overlap_seconds",
        "truth_duration_seconds",
        "window_duration_seconds",
        "truth_coverage_ratio",
        "window_coverage_ratio",
        "detection_latency_seconds",
    ]
    if window_like_df.empty or truth_df.empty:
        return pd.DataFrame(columns=expected_columns)
    windows = window_like_df.copy()
    windows["t_start"] = _to_utc(windows["t_start"])
    windows["t_end"] = _to_utc(windows["t_end"])
    rows: list[dict[str, Any]] = []
    for truth in truth_df.to_dict(orient="records"):
        mask = (
            (windows["tail_id"].astype(str) == str(truth["tail_id"]))
            & (windows["flight_id"].astype(str) == str(truth["flight_id"]))
            & (windows["t_end"] >= truth[start_field])
            & (windows["t_start"] <= truth[end_field])
        )
        truth_start = pd.to_datetime(truth[start_field], utc=True, errors="coerce")
        truth_end = pd.to_datetime(truth[end_field], utc=True, errors="coerce")
        truth_duration_seconds = max(float((truth_end - truth_start).total_seconds()), 0.0)
        for window in windows[mask].to_dict(orient="records"):
            window_start = pd.to_datetime(window["t_start"], utc=True)
            window_end = pd.to_datetime(window["t_end"], utc=True)
            overlap_start = max(window_start, truth_start)
            overlap_end = min(window_end, truth_end)
            if truth_duration_seconds <= 0.0 and window_start <= truth_start <= window_end:
                overlap_seconds = 1.0
            else:
                overlap_seconds = max(float((overlap_end - overlap_start).total_seconds()), 0.0)
            if overlap_seconds <= 0.0:
                continue
            window_duration_seconds = max(
                float((window_end - window_start).total_seconds()),
                0.0,
            )
            detection_latency_seconds = float((window_start - truth_start).total_seconds())
            row = dict(window)
            row.update(truth)
            row["overlap_seconds"] = overlap_seconds
            row["truth_duration_seconds"] = truth_duration_seconds
            row["window_duration_seconds"] = window_duration_seconds
            row["truth_coverage_ratio"] = (
                float(overlap_seconds / truth_duration_seconds) if truth_duration_seconds > 0.0 else 1.0
            )
            row["window_coverage_ratio"] = (
                float(overlap_seconds / window_duration_seconds) if window_duration_seconds > 0.0 else 1.0
            )
            row["detection_latency_seconds"] = detection_latency_seconds
            rows.append(row)
    return pd.DataFrame.from_records(rows, columns=expected_columns)


def strict_overlap_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="bool")
    return (
        (df["window_coverage_ratio"].fillna(0.0).astype(float) >= float(STRICT_WINDOW_COVERAGE_MIN_RATIO))
        & (df["detection_latency_seconds"].fillna(float("inf")).astype(float) >= float(-STRICT_MAX_EARLY_LEAD_SECONDS))
    )


_strict_overlap_mask = strict_overlap_mask


def _merge_scored_windows(
    *,
    windows_df: pd.DataFrame,
    raw_scores_df: pd.DataFrame | None,
    calibrated_scores_df: pd.DataFrame,
) -> pd.DataFrame:
    merged = windows_df.copy()

    if raw_scores_df is not None and not raw_scores_df.empty:
        raw_scores = raw_scores_df.copy()
        raw_keep = [
            column
            for column in (
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                "global_score",
                "severity",
                "phase_id_detected",
                "phase_state_detected",
                "phase_confidence_detected",
                "distance_to_centroid_detected",
                "dominant_subsystem_id",
                "dominant_score_component",
            )
            if column in raw_scores.columns
        ]
        raw_scores = raw_scores[raw_keep].rename(
            columns={
                "global_score": "global_score_raw",
                "severity": "severity_raw",
            }
        )
        merged = merged.merge(
            raw_scores,
            on=_score_join_keys(merged, raw_scores),
            how="left",
        )

    calibrated = calibrated_scores_df.copy()
    calibrated_keep = [
        column
        for column in (
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            "global_score",
            "severity",
            "p_value",
            "emit_ready",
            "warm",
            "min_warm",
        )
        if column in calibrated.columns
    ]
    calibrated = calibrated[calibrated_keep].rename(
        columns={
            "global_score": "global_score_calibrated",
            "severity": "severity_calibrated",
        }
    )
    merged = merged.merge(
        calibrated,
        on=_score_join_keys(merged, calibrated),
        how="left",
    )
    merged["global_score_raw"] = _float_series(merged, "global_score_raw", "global_score_calibrated").fillna(0.0)
    merged["global_score"] = merged["global_score_raw"].astype("float64")
    merged["p_value"] = _float_series(merged, "p_value")
    merged["severity"] = _text_series(merged, "severity_calibrated", "severity_raw")
    merged["emit_ready"] = _bool_series(merged, "emit_ready")
    return merged


def _build_overlap_window_summary(
    *,
    scored_windows_df: pd.DataFrame,
    overlaps_df: pd.DataFrame,
    truth_window_id_field: str,
) -> pd.DataFrame:
    windows = scored_windows_df.copy()
    if overlaps_df.empty:
        windows["overlapping_truth_window_count"] = 0
        windows["strict_overlapping_truth_window_count"] = 0
        windows["max_truth_coverage_ratio"] = 0.0
        windows["max_window_coverage_ratio"] = 0.0
        windows["max_overlap_seconds"] = 0.0
        windows["overlapping_truth_window_ids"] = [[] for _ in range(len(windows))]
        windows["truth_overlap_bucket"] = "no_overlap"
        return windows

    key_columns = [column for column in ("tail_id", "flight_id", "win_id") if column in overlaps_df.columns]
    strict_column = "_strict_overlap"
    overlaps = overlaps_df.copy()
    overlaps[strict_column] = _strict_overlap_mask(overlaps)

    aggregated_rows: list[dict[str, Any]] = []
    for key, group in overlaps.groupby(key_columns, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        row: dict[str, Any] = {column: key[idx] for idx, column in enumerate(key_columns)}
        truth_ids = _non_empty_strings(group[truth_window_id_field])
        strict_ids = _non_empty_strings(group.loc[group[strict_column], truth_window_id_field])
        row.update(
            {
                "overlapping_truth_window_count": int(len(truth_ids)),
                "strict_overlapping_truth_window_count": int(len(strict_ids)),
                "max_truth_coverage_ratio": float(
                    pd.to_numeric(group["truth_coverage_ratio"], errors="coerce").fillna(0.0).max()
                ),
                "max_window_coverage_ratio": float(
                    pd.to_numeric(group["window_coverage_ratio"], errors="coerce").fillna(0.0).max()
                ),
                "max_overlap_seconds": float(
                    pd.to_numeric(group["overlap_seconds"], errors="coerce").fillna(0.0).max()
                ),
                "overlapping_truth_window_ids": truth_ids,
            }
        )
        aggregated_rows.append(row)

    overlap_summary = pd.DataFrame.from_records(aggregated_rows)
    windows = windows.merge(overlap_summary, on=key_columns, how="left")
    windows["overlapping_truth_window_count"] = (
        pd.to_numeric(windows["overlapping_truth_window_count"], errors="coerce").fillna(0).astype(int)
    )
    windows["strict_overlapping_truth_window_count"] = (
        pd.to_numeric(windows["strict_overlapping_truth_window_count"], errors="coerce").fillna(0).astype(int)
    )
    windows["max_truth_coverage_ratio"] = _float_series(windows, "max_truth_coverage_ratio").fillna(0.0)
    windows["max_window_coverage_ratio"] = _float_series(windows, "max_window_coverage_ratio").fillna(0.0)
    windows["max_overlap_seconds"] = _float_series(windows, "max_overlap_seconds").fillna(0.0)
    windows["overlapping_truth_window_ids"] = windows["overlapping_truth_window_ids"].apply(
        lambda value: value if isinstance(value, list) else []
    )
    windows["truth_overlap_bucket"] = windows.apply(
        lambda row: (
            "strict_overlap"
            if int(row["strict_overlapping_truth_window_count"]) > 0
            else ("soft_overlap" if int(row["overlapping_truth_window_count"]) > 0 else "no_overlap")
        ),
        axis=1,
    )
    return windows


def _distribution_summary(values: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "p90": None,
            "max": None,
        }
    return {
        "count": int(len(numeric)),
        "mean": float(numeric.mean()),
        "median": float(numeric.median()),
        "p90": float(numeric.quantile(0.9)),
        "max": float(numeric.max()),
    }


def _score_distribution_by_overlap_bucket(
    *,
    scored_windows_df: pd.DataFrame,
    value_field: str,
) -> dict[str, dict[str, Any]]:
    distributions: dict[str, dict[str, Any]] = {}
    for bucket in ("strict_overlap", "soft_overlap", "no_overlap"):
        subset = scored_windows_df[scored_windows_df["truth_overlap_bucket"] == bucket]
        distributions[bucket] = _distribution_summary(subset.get(value_field, pd.Series(dtype="float64")))
    return distributions


def _severity_distribution_by_overlap_bucket(scored_windows_df: pd.DataFrame) -> dict[str, dict[str, int]]:
    distributions: dict[str, dict[str, int]] = {}
    for bucket in ("strict_overlap", "soft_overlap", "no_overlap"):
        subset = scored_windows_df[scored_windows_df["truth_overlap_bucket"] == bucket]
        distributions[bucket] = {
            str(label): int(count)
            for label, count in subset["severity"].fillna("normal").astype(str).value_counts(dropna=False).sort_index().items()
        }
    return distributions


def _truth_window_recall_by_top_k_windows(
    *,
    scored_windows_df: pd.DataFrame,
    overlaps_df: pd.DataFrame,
    truth_df: pd.DataFrame,
    truth_window_id_field: str,
    sort_field: str,
    ascending: bool,
) -> dict[str, dict[str, float]]:
    result = {
        "any_overlap": {_top_k_key(budget): 0.0 for budget in RAW_SCORE_TOP_K_BUDGETS},
        "strict_overlap": {_top_k_key(budget): 0.0 for budget in RAW_SCORE_TOP_K_BUDGETS},
    }
    if truth_df.empty or scored_windows_df.empty:
        return result

    ordered = scored_windows_df.copy()
    ordered[sort_field] = pd.to_numeric(ordered.get(sort_field), errors="coerce")
    fill_value = float("inf") if ascending else float("-inf")
    ordered[sort_field] = ordered[sort_field].fillna(fill_value)
    ordered = ordered.sort_values(
        [sort_field, "tail_id", "flight_id", "win_id"],
        ascending=[ascending, True, True, True],
        kind="mergesort",
    )

    strict_overlaps = overlaps_df[_strict_overlap_mask(overlaps_df)] if not overlaps_df.empty else overlaps_df
    truth_window_count = int(len(truth_df))
    key_columns = [column for column in ("tail_id", "flight_id", "win_id") if column in ordered.columns]

    for budget in RAW_SCORE_TOP_K_BUDGETS:
        selected = ordered.head(int(min(budget, len(ordered))))
        selected_keys = selected[key_columns].drop_duplicates()
        if selected_keys.empty:
            continue
        any_selected = selected_keys.merge(overlaps_df[key_columns + [truth_window_id_field]], on=key_columns, how="inner")
        strict_selected = selected_keys.merge(
            strict_overlaps[key_columns + [truth_window_id_field]],
            on=key_columns,
            how="inner",
        )
        result["any_overlap"][_top_k_key(budget)] = float(
            any_selected[truth_window_id_field].astype(str).nunique() / truth_window_count
        )
        result["strict_overlap"][_top_k_key(budget)] = float(
            strict_selected[truth_window_id_field].astype(str).nunique() / truth_window_count
        )
    return result


def _emit_ready_candidate_summary(scored_windows_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    windows = scored_windows_df.copy()
    windows["p_value"] = pd.to_numeric(windows.get("p_value"), errors="coerce")
    windows["emit_ready"] = _bool_series(windows, "emit_ready")

    candidate_counts: dict[str, int] = {}
    emit_ready_counts: dict[str, int] = {}
    blocked_counts: dict[str, int] = {}
    emit_ready_rates: dict[str, float | None] = {}
    for threshold in CALIBRATED_P_VALUE_THRESHOLDS:
        key = _threshold_key(threshold)
        candidates = windows[windows["p_value"] <= float(threshold)]
        candidate_count = int(len(candidates))
        emit_count = int(candidates["emit_ready"].sum())
        blocked_count = int(candidate_count - emit_count)
        candidate_counts[key] = candidate_count
        emit_ready_counts[key] = emit_count
        blocked_counts[key] = blocked_count
        emit_ready_rates[key] = float(emit_count / candidate_count) if candidate_count > 0 else None
    return {
        "candidate_window_count_by_p_value_threshold": candidate_counts,
        "emit_ready_candidate_window_count_by_p_value_threshold": emit_ready_counts,
        "blocked_candidate_window_count_by_p_value_threshold": blocked_counts,
        "emit_ready_candidate_window_rate_by_p_value_threshold": emit_ready_rates,
    }


def _emit_ready_rate_by_top_k_rarity_windows(scored_windows_df: pd.DataFrame) -> dict[str, float | None]:
    windows = scored_windows_df.copy()
    windows["p_value"] = pd.to_numeric(windows.get("p_value"), errors="coerce").fillna(float("inf"))
    windows["emit_ready"] = _bool_series(windows, "emit_ready")
    windows = windows.sort_values(
        ["p_value", "global_score_raw", "tail_id", "flight_id", "win_id"],
        ascending=[True, False, True, True, True],
        kind="mergesort",
    )
    rates: dict[str, float | None] = {}
    for budget in RAW_SCORE_TOP_K_BUDGETS:
        selected = windows.head(int(min(budget, len(windows))))
        rates[_top_k_key(budget)] = float(selected["emit_ready"].mean()) if not selected.empty else None
    return rates


def _build_score_window_diagnostics(scored_windows_df: pd.DataFrame) -> list[dict[str, Any]]:
    if scored_windows_df.empty:
        return []

    windows = scored_windows_df.copy()
    windows["global_score_raw"] = pd.to_numeric(windows.get("global_score_raw"), errors="coerce").fillna(0.0)
    windows["p_value"] = pd.to_numeric(windows.get("p_value"), errors="coerce")
    windows["raw_score_rank"] = windows["global_score_raw"].rank(method="first", ascending=False)
    windows["calibrated_rarity_rank"] = windows["p_value"].fillna(float("inf")).rank(method="first", ascending=True)

    selected = windows[
        (windows["overlapping_truth_window_count"] > 0)
        | (windows["raw_score_rank"] <= float(DIAGNOSTIC_WINDOW_SAMPLE_LIMIT))
        | (windows["calibrated_rarity_rank"] <= float(DIAGNOSTIC_WINDOW_SAMPLE_LIMIT))
    ].copy()
    selected = selected.sort_values(
        ["max_truth_coverage_ratio", "global_score_raw", "p_value", "win_id"],
        ascending=[False, False, True, True],
        kind="mergesort",
    ).head(DIAGNOSTIC_WINDOW_SAMPLE_LIMIT)

    rows: list[dict[str, Any]] = []
    for row in selected.to_dict(orient="records"):
        rows.append(
            {
                "tail_id": str(row.get("tail_id", "")),
                "flight_id": str(row.get("flight_id", "")),
                "win_id": int(row.get("win_id", 0) or 0),
                "t_start": _json_timestamp(row.get("t_start")),
                "t_end": _json_timestamp(row.get("t_end")),
                "phase_id_detected": None
                if pd.isna(row.get("phase_id_detected"))
                else int(row.get("phase_id_detected", 0) or 0),
                "phase_state_detected": str(row.get("phase_state_detected", "")),
                "event_count": None if pd.isna(row.get("event_count")) else int(row.get("event_count", 0) or 0),
                "real_event_count": None
                if pd.isna(row.get("real_event_count"))
                else int(row.get("real_event_count", 0) or 0),
                "close_reason": str(row.get("close_reason", "") or ""),
                "global_score_raw": float(row.get("global_score_raw", 0.0) or 0.0),
                "p_value": None if pd.isna(row.get("p_value")) else float(row.get("p_value", 0.0) or 0.0),
                "severity": str(row.get("severity", "normal") or "normal"),
                "emit_ready": False if pd.isna(row.get("emit_ready")) else bool(row.get("emit_ready", False)),
                "dominant_score_component": str(row.get("dominant_score_component", "") or ""),
                "dominant_subsystem_id": str(row.get("dominant_subsystem_id", "") or ""),
                "truth_overlap_bucket": str(row.get("truth_overlap_bucket", "no_overlap")),
                "overlapping_truth_window_count": int(row.get("overlapping_truth_window_count", 0) or 0),
                "strict_overlapping_truth_window_count": int(row.get("strict_overlapping_truth_window_count", 0) or 0),
                "max_truth_coverage_ratio": float(row.get("max_truth_coverage_ratio", 0.0) or 0.0),
                "max_window_coverage_ratio": float(row.get("max_window_coverage_ratio", 0.0) or 0.0),
                "raw_score_rank": int(row.get("raw_score_rank", 0) or 0),
                "calibrated_rarity_rank": int(row.get("calibrated_rarity_rank", 0) or 0),
                "overlapping_truth_window_ids": [str(value) for value in row.get("overlapping_truth_window_ids", [])],
            }
        )
    return rows


def _build_raw_score_validation_summary(
    *,
    scored_windows_df: pd.DataFrame,
    overlaps_df: pd.DataFrame,
    truth_df: pd.DataFrame,
    truth_window_id_field: str,
) -> dict[str, Any]:
    return {
        "window_count": int(len(scored_windows_df)),
        "window_count_by_truth_overlap_bucket": {
            bucket: int((scored_windows_df["truth_overlap_bucket"] == bucket).sum())
            for bucket in ("strict_overlap", "soft_overlap", "no_overlap")
        },
        "score_distribution_by_truth_overlap_bucket": _score_distribution_by_overlap_bucket(
            scored_windows_df=scored_windows_df,
            value_field="global_score_raw",
        ),
        "truth_window_recall_by_top_k_raw_score": _truth_window_recall_by_top_k_windows(
            scored_windows_df=scored_windows_df,
            overlaps_df=overlaps_df,
            truth_df=truth_df,
            truth_window_id_field=truth_window_id_field,
            sort_field="global_score_raw",
            ascending=False,
        ),
    }


def _build_calibrated_score_validation_summary(
    *,
    scored_windows_df: pd.DataFrame,
    overlaps_df: pd.DataFrame,
    truth_df: pd.DataFrame,
    truth_window_id_field: str,
) -> dict[str, Any]:
    return {
        "p_value_distribution_by_truth_overlap_bucket": _score_distribution_by_overlap_bucket(
            scored_windows_df=scored_windows_df,
            value_field="p_value",
        ),
        "truth_window_recall_by_top_k_calibrated_rarity": _truth_window_recall_by_top_k_windows(
            scored_windows_df=scored_windows_df,
            overlaps_df=overlaps_df,
            truth_df=truth_df,
            truth_window_id_field=truth_window_id_field,
            sort_field="p_value",
            ascending=True,
        ),
    }


def _build_emission_validation_summary(scored_windows_df: pd.DataFrame) -> dict[str, Any]:
    return {
        **_emit_ready_candidate_summary(scored_windows_df),
        "emit_ready_rate_by_top_k_calibrated_rarity": _emit_ready_rate_by_top_k_rarity_windows(scored_windows_df),
        "severity_distribution_by_truth_overlap_bucket": _severity_distribution_by_overlap_bucket(scored_windows_df),
    }


def validate_scores_against_misbehavior_windows(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    raw_scores_df: pd.DataFrame | None = None,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    truth_df = extract_misbehavior_truth_windows(raw_telemetry_df)
    if truth_df.empty:
        return {
            "status": "ok",
            "misbehavior_window_count": 0,
            "detected_misbehavior_window_count": 0,
            "emit_ready_misbehavior_window_count": 0,
        }

    merged_windows = _merge_scored_windows(
        windows_df=windows_df,
        raw_scores_df=raw_scores_df,
        calibrated_scores_df=calibrated_scores_df,
    )

    overlaps = build_truth_window_overlap_table(
        window_like_df=merged_windows,
        truth_df=truth_df,
        start_field="misbehavior_start_timestamp_utc",
        end_field="misbehavior_end_timestamp_utc",
    )
    scored_windows = _build_overlap_window_summary(
        scored_windows_df=merged_windows,
        overlaps_df=overlaps,
        truth_window_id_field="misbehavior_window_id",
    )
    if overlaps.empty:
        return {
            "status": "ok",
            "misbehavior_window_count": int(len(truth_df)),
            "detected_misbehavior_window_count": 0,
            "emit_ready_misbehavior_window_count": 0,
            "reason": "misbehavior windows did not overlap any calibrated windows",
            "raw_score_validation": _build_raw_score_validation_summary(
                scored_windows_df=scored_windows,
                overlaps_df=overlaps,
                truth_df=truth_df,
                truth_window_id_field="misbehavior_window_id",
            ),
            "calibrated_score_validation": _build_calibrated_score_validation_summary(
                scored_windows_df=scored_windows,
                overlaps_df=overlaps,
                truth_df=truth_df,
                truth_window_id_field="misbehavior_window_id",
            ),
            "emission_validation": _build_emission_validation_summary(scored_windows),
            "score_window_diagnostics": _build_score_window_diagnostics(scored_windows),
        }

    per_window: list[dict[str, Any]] = []
    for (tail_id, flight_id, misbehavior_window_id), group in overlaps.groupby(
        ["tail_id", "flight_id", "misbehavior_window_id"],
        dropna=False,
    ):
        ordered = group.sort_values(["t_start", "win_id"], kind="mergesort")
        strict_overlap = ordered[_strict_overlap_mask(ordered)]
        detected = strict_overlap[strict_overlap["severity"].fillna("normal").astype(str) != "normal"]
        emit_ready_series = (
            strict_overlap["emit_ready"].fillna(False).astype(bool)
            if "emit_ready" in strict_overlap.columns
            else pd.Series(False, index=ordered.index, dtype="bool")
        )
        emit_ready = strict_overlap[emit_ready_series]
        first_detected = detected["t_start"].min() if not detected.empty else pd.NaT
        first_emit_ready = emit_ready["t_start"].min() if not emit_ready.empty else pd.NaT
        misbehavior_start = ordered["misbehavior_start_timestamp_utc"].iloc[0]
        per_window.append(
            {
                "tail_id": str(tail_id),
                "flight_id": str(flight_id),
                "misbehavior_window_id": str(misbehavior_window_id),
                "misbehavior_family_label": str(ordered["misbehavior_family_label"].iloc[0]),
                "misbehavior_detail_label": str(ordered["misbehavior_detail_label"].iloc[0]),
                "fault_window_id": str(ordered["fault_window_id"].iloc[0]),
                "fault_family_label": str(ordered["fault_family_label"].iloc[0]),
                "fault_type": str(ordered["fault_type"].iloc[0]),
                "subsystem_id": str(ordered["subsystem_id"].iloc[0]),
                "parameter_name": str(ordered["parameter_name"].iloc[0]),
                "overlapping_window_count": int(len(ordered)),
                "strict_overlapping_window_count": int(len(strict_overlap)),
                "detected_window_count": int(len(detected)),
                "emit_ready_window_count": int(len(emit_ready)),
                "max_global_score": float(ordered["global_score"].fillna(0.0).max()),
                "median_global_score": float(ordered["global_score"].fillna(0.0).median()),
                "max_truth_coverage_ratio": float(ordered["truth_coverage_ratio"].fillna(0.0).max()),
                "strict_window_coverage_threshold": float(STRICT_WINDOW_COVERAGE_MIN_RATIO),
                "strict_max_early_lead_seconds": float(STRICT_MAX_EARLY_LEAD_SECONDS),
                "detection_latency_seconds": (
                    None if pd.isna(first_detected) else float((first_detected - misbehavior_start).total_seconds())
                ),
                "emit_ready_latency_seconds": (
                    None if pd.isna(first_emit_ready) else float((first_emit_ready - misbehavior_start).total_seconds())
                ),
            }
        )

    detection_latencies = [
        float(row["detection_latency_seconds"])
        for row in per_window
        if row.get("detection_latency_seconds") is not None
    ]
    emit_ready_latencies = [
        float(row["emit_ready_latency_seconds"])
        for row in per_window
        if row.get("emit_ready_latency_seconds") is not None
    ]
    misbehavior_window_count = int(len(truth_df))
    detected_misbehavior_window_count = int(sum(1 for row in per_window if row["detected_window_count"] > 0))
    emit_ready_misbehavior_window_count = int(sum(1 for row in per_window if row["emit_ready_window_count"] > 0))
    return {
        "status": "ok",
        "misbehavior_window_count": misbehavior_window_count,
        "detected_misbehavior_window_count": detected_misbehavior_window_count,
        "emit_ready_misbehavior_window_count": emit_ready_misbehavior_window_count,
        "detected_misbehavior_window_rate": (
            float(detected_misbehavior_window_count / misbehavior_window_count)
            if misbehavior_window_count > 0
            else None
        ),
        "emit_ready_misbehavior_window_rate": (
            float(emit_ready_misbehavior_window_count / misbehavior_window_count)
            if misbehavior_window_count > 0
            else None
        ),
        "median_misbehavior_window_score": (
            float(pd.DataFrame(per_window)["median_global_score"].median()) if per_window else None
        ),
        "median_detection_latency_seconds": (
            float(pd.Series(detection_latencies, dtype="float64").median())
            if detection_latencies
            else None
        ),
        "median_emit_ready_latency_seconds": (
            float(pd.Series(emit_ready_latencies, dtype="float64").median())
            if emit_ready_latencies
            else None
        ),
        "raw_score_validation": _build_raw_score_validation_summary(
            scored_windows_df=scored_windows,
            overlaps_df=overlaps,
            truth_df=truth_df,
            truth_window_id_field="misbehavior_window_id",
        ),
        "calibrated_score_validation": _build_calibrated_score_validation_summary(
            scored_windows_df=scored_windows,
            overlaps_df=overlaps,
            truth_df=truth_df,
            truth_window_id_field="misbehavior_window_id",
        ),
        "emission_validation": _build_emission_validation_summary(scored_windows),
        "score_window_diagnostics": _build_score_window_diagnostics(scored_windows),
        "misbehavior_windows": per_window,
    }


def summarize_misbehavior_window_detection(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    raw_scores_df: pd.DataFrame | None = None,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = validate_scores_against_misbehavior_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        raw_scores_df=raw_scores_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "misbehavior_window_count": summary.get("misbehavior_window_count", 0),
        "detected_misbehavior_window_count": summary.get("detected_misbehavior_window_count", 0),
        "emit_ready_misbehavior_window_count": summary.get("emit_ready_misbehavior_window_count", 0),
        "misbehavior_windows": summary.get("misbehavior_windows", []),
    }


def validate_scores_against_fault_windows(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    raw_scores_df: pd.DataFrame | None = None,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = validate_scores_against_misbehavior_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        raw_scores_df=raw_scores_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_fault_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_fault_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "detected_fault_window_rate": summary.get("detected_misbehavior_window_rate"),
        "emit_ready_fault_window_rate": summary.get("emit_ready_misbehavior_window_rate"),
        "median_fault_window_score": summary.get("median_misbehavior_window_score"),
        "median_detection_latency_seconds": summary.get("median_detection_latency_seconds"),
        "median_emit_ready_latency_seconds": summary.get("median_emit_ready_latency_seconds"),
        "raw_score_validation": summary.get("raw_score_validation"),
        "calibrated_score_validation": summary.get("calibrated_score_validation"),
        "emission_validation": summary.get("emission_validation"),
        "score_window_diagnostics": summary.get("score_window_diagnostics", []),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row["misbehavior_window_id"]),
                "fault_family_label": row["fault_family_label"],
                "fault_type": row["fault_type"],
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }


def summarize_fault_window_detection(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    raw_scores_df: pd.DataFrame | None = None,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = summarize_misbehavior_window_detection(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        raw_scores_df=raw_scores_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_fault_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_fault_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row["misbehavior_window_id"]),
                "fault_family_label": row["fault_family_label"],
                "fault_type": row["fault_type"],
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }
