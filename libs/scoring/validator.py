"""Score validation helpers against simulator misbehavior truth with fault wrappers."""

from __future__ import annotations

from typing import Any

import pandas as pd


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


def _join_truths_to_windows(
    *,
    windows_df: pd.DataFrame,
    calibrated_scores_df: pd.DataFrame,
    truth_df: pd.DataFrame,
    start_field: str,
    end_field: str,
) -> pd.DataFrame:
    if windows_df.empty or calibrated_scores_df.empty or truth_df.empty:
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
    for truth in truth_df.to_dict(orient="records"):
        mask = (
            (merged_windows["tail_id"].astype(str) == str(truth["tail_id"]))
            & (merged_windows["flight_id"].astype(str) == str(truth["flight_id"]))
            & (merged_windows["t_end"] >= truth[start_field])
            & (merged_windows["t_start"] <= truth[end_field])
        )
        for window in merged_windows[mask].to_dict(orient="records"):
            row = dict(window)
            row.update(truth)
            rows.append(row)
    return pd.DataFrame.from_records(rows)


def validate_scores_against_misbehavior_windows(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
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

    overlaps = _join_truths_to_windows(
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
        truth_df=truth_df,
        start_field="misbehavior_start_timestamp_utc",
        end_field="misbehavior_end_timestamp_utc",
    )
    if overlaps.empty:
        return {
            "status": "ok",
            "misbehavior_window_count": int(len(truth_df)),
            "detected_misbehavior_window_count": 0,
            "emit_ready_misbehavior_window_count": 0,
            "reason": "misbehavior windows did not overlap any calibrated windows",
        }

    per_window: list[dict[str, Any]] = []
    for (tail_id, flight_id, misbehavior_window_id), group in overlaps.groupby(
        ["tail_id", "flight_id", "misbehavior_window_id"],
        dropna=False,
    ):
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
                "detected_window_count": int(len(detected)),
                "emit_ready_window_count": int(len(emit_ready)),
                "max_global_score": float(ordered["global_score"].fillna(0.0).max()),
                "median_global_score": float(ordered["global_score"].fillna(0.0).median()),
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
        "misbehavior_windows": per_window,
    }


def summarize_misbehavior_window_detection(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = validate_scores_against_misbehavior_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
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
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = validate_scores_against_misbehavior_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
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
    calibrated_scores_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = summarize_misbehavior_window_detection(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
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
