"""Score validation helpers against injected fault windows."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _to_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def extract_fault_truth_windows(raw_telemetry_df: pd.DataFrame) -> pd.DataFrame:
    if raw_telemetry_df is None or raw_telemetry_df.empty:
        return pd.DataFrame()
    rows = raw_telemetry_df.copy()
    rows["fault_active"] = rows.get("fault_active", False).fillna(False).astype(bool)
    rows["fault_window_id"] = rows.get("fault_window_id", "").fillna("").astype(str)
    rows["timestamp_utc"] = _to_utc(rows["timestamp_utc"])
    active = rows[(rows["fault_active"]) & (rows["fault_window_id"] != "")]
    if active.empty:
        return pd.DataFrame()
    summaries = (
        active.groupby(["tail_id", "flight_id", "fault_window_id"], dropna=False)
        .agg(
            fault_start_timestamp_utc=("timestamp_utc", "min"),
            fault_end_timestamp_utc=("timestamp_utc", "max"),
            fault_family_label=("fault_family_label", "first"),
            fault_type=("fault_type", "first"),
            system_id=("system_id", "first"),
            subsystem_id=("subsystem_id", "first"),
            module_id=("module_id", "first"),
            parameter_name=("parameter_name", "first"),
        )
        .reset_index()
    )
    return summaries


def _join_faults_to_windows(
    *,
    windows_df: pd.DataFrame,
    calibrated_scores_df: pd.DataFrame,
    fault_truth_df: pd.DataFrame,
) -> pd.DataFrame:
    if windows_df.empty or calibrated_scores_df.empty or fault_truth_df.empty:
        return pd.DataFrame()
    windows = windows_df.copy()
    windows["t_start"] = _to_utc(windows["t_start"])
    windows["t_end"] = _to_utc(windows["t_end"])
    scores = calibrated_scores_df.copy()
    merged_windows = windows.merge(
        scores,
        on=["tail_id", "flight_id", "win_id", "date_utc"],
        how="left",
        suffixes=("", "_score"),
    )
    rows: list[dict[str, Any]] = []
    for fault in fault_truth_df.to_dict(orient="records"):
        mask = (
            (merged_windows["tail_id"].astype(str) == str(fault["tail_id"]))
            & (merged_windows["flight_id"].astype(str) == str(fault["flight_id"]))
            & (merged_windows["t_end"] >= fault["fault_start_timestamp_utc"])
            & (merged_windows["t_start"] <= fault["fault_end_timestamp_utc"])
        )
        for window in merged_windows[mask].to_dict(orient="records"):
            row = dict(window)
            row.update(fault)
            rows.append(row)
    return pd.DataFrame.from_records(rows)


def validate_scores_against_fault_windows(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    fault_truth_df = extract_fault_truth_windows(raw_telemetry_df)
    if fault_truth_df.empty:
        return {
            "status": "ok",
            "fault_window_count": 0,
            "detected_fault_window_count": 0,
            "emit_ready_fault_window_count": 0,
        }

    overlaps = _join_faults_to_windows(
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
        fault_truth_df=fault_truth_df,
    )
    if overlaps.empty:
        return {
            "status": "ok",
            "fault_window_count": int(len(fault_truth_df)),
            "detected_fault_window_count": 0,
            "emit_ready_fault_window_count": 0,
            "reason": "fault windows did not overlap any calibrated windows",
        }

    per_fault: list[dict[str, Any]] = []
    for (tail_id, flight_id, fault_window_id), group in overlaps.groupby(["tail_id", "flight_id", "fault_window_id"], dropna=False):
        ordered = group.sort_values(["t_start", "win_id"], kind="mergesort")
        detected = ordered[ordered["severity"].fillna("normal").astype(str) != "normal"]
        emit_ready_series = (
            ordered["emit_ready"].fillna(False).astype(bool)
            if "emit_ready" in ordered.columns
            else pd.Series(False, index=ordered.index, dtype="bool")
        )
        emit_ready = ordered[emit_ready_series]
        first_detected = detected["t_start"].min() if not detected.empty else pd.NaT
        first_emit_ready = emit_ready["t_start"].min() if not emit_ready.empty else pd.NaT
        fault_start = ordered["fault_start_timestamp_utc"].iloc[0]
        per_fault.append(
            {
                "tail_id": str(tail_id),
                "flight_id": str(flight_id),
                "fault_window_id": str(fault_window_id),
                "fault_family_label": str(ordered["fault_family_label"].iloc[0]),
                "fault_type": str(ordered["fault_type"].iloc[0]),
                "subsystem_id": str(ordered["subsystem_id"].iloc[0]),
                "parameter_name": str(ordered["parameter_name"].iloc[0]),
                "overlapping_window_count": int(len(ordered)),
                "detected_window_count": int(len(detected)),
                "emit_ready_window_count": int(len(emit_ready)),
                "max_global_score": float(ordered["global_score"].fillna(0.0).max()),
                "median_global_score": float(ordered["global_score"].fillna(0.0).median()),
                "detection_latency_seconds": (
                    None
                    if pd.isna(first_detected)
                    else float((first_detected - fault_start).total_seconds())
                ),
                "emit_ready_latency_seconds": (
                    None
                    if pd.isna(first_emit_ready)
                    else float((first_emit_ready - fault_start).total_seconds())
                ),
            }
        )

    return {
        "status": "ok",
        "fault_window_count": int(len(fault_truth_df)),
        "detected_fault_window_count": int(sum(1 for row in per_fault if row["detected_window_count"] > 0)),
        "emit_ready_fault_window_count": int(sum(1 for row in per_fault if row["emit_ready_window_count"] > 0)),
        "median_fault_window_score": float(pd.DataFrame(per_fault)["median_global_score"].median()) if per_fault else None,
        "fault_windows": per_fault,
    }


def summarize_fault_window_detection(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = validate_scores_against_fault_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    if summary.get("status") != "ok":
        return summary
    fault_windows = summary.get("fault_windows", [])
    return {
        "status": "ok",
        "fault_window_count": summary.get("fault_window_count", 0),
        "detected_fault_window_count": summary.get("detected_fault_window_count", 0),
        "emit_ready_fault_window_count": summary.get("emit_ready_fault_window_count", 0),
        "fault_windows": fault_windows,
    }
