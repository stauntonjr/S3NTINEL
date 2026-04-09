"""Anomaly attribution validation against simulator misbehavior truth with fault wrappers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.scoring.validator import (
    STRICT_MAX_EARLY_LEAD_SECONDS,
    STRICT_WINDOW_COVERAGE_MIN_RATIO,
    build_truth_window_overlap_table,
    extract_fault_truth_windows,
    extract_misbehavior_truth_windows,
    strict_overlap_mask,
)


def _empty_parameter_localization_validation() -> dict[str, Any]:
    return {
        "status": "ok",
        "truth_window_count": 0,
        "exact_parameter_match_count_by_source": {
            "telemetry": 0,
            "event": 0,
            "any": 0,
            "both": 0,
        },
        "exact_parameter_match_rate_by_source": {
            "telemetry": None,
            "event": None,
            "any": None,
            "both": None,
        },
        "truth_subsystem_present_count_by_source": {
            "telemetry": 0,
            "event": 0,
        },
        "truth_subsystem_present_rate_by_source": {
            "telemetry": None,
            "event": None,
        },
        "parameter_localization_cases": [],
    }


def _sorted_non_empty_string_values(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(
        {
            str(value)
            for value in df[column].fillna("").astype(str).tolist()
            if str(value)
        }
    )


@dataclass(frozen=True)
class DetectedSubsystemTruthMap:
    detected_to_truth_subsystem: dict[str, str]
    ambiguous_detected_subsystems: set[str]

    @classmethod
    def from_hierarchy_frames(
        cls,
        *,
        hierarchy_sensor_map_df: pd.DataFrame | None,
        hierarchy_label_df: pd.DataFrame | None,
    ) -> "DetectedSubsystemTruthMap":
        if (
            hierarchy_sensor_map_df is None
            or hierarchy_label_df is None
            or hierarchy_sensor_map_df.empty
            or hierarchy_label_df.empty
        ):
            return cls(detected_to_truth_subsystem={}, ambiguous_detected_subsystems=set())

        hierarchy_joined = hierarchy_sensor_map_df.merge(
            hierarchy_label_df[["parameter_name", "subsystem_id"]].rename(columns={"subsystem_id": "truth_subsystem_id"}),
            on="parameter_name",
            how="inner",
        )
        detected_to_truth_subsystem: dict[str, str] = {}
        ambiguous_detected_subsystems: set[str] = set()
        if hierarchy_joined.empty:
            return cls(
                detected_to_truth_subsystem=detected_to_truth_subsystem,
                ambiguous_detected_subsystems=ambiguous_detected_subsystems,
            )

        for detected_subsystem_id, group in hierarchy_joined.groupby("subsystem_id", dropna=False):
            counts = group["truth_subsystem_id"].fillna("").astype(str).value_counts()
            if counts.empty:
                continue
            top_truth_subsystem = str(counts.index[0])
            top_count = int(counts.iloc[0])
            second_count = int(counts.iloc[1]) if len(counts) > 1 else -1
            if top_count > second_count:
                detected_to_truth_subsystem[str(detected_subsystem_id)] = top_truth_subsystem
            else:
                ambiguous_detected_subsystems.add(str(detected_subsystem_id))
        return cls(
            detected_to_truth_subsystem=detected_to_truth_subsystem,
            ambiguous_detected_subsystems=ambiguous_detected_subsystems,
        )

    def resolve(self, detected_subsystem_id: str) -> tuple[str | None, bool]:
        detected = str(detected_subsystem_id or "")
        if not detected or detected in self.ambiguous_detected_subsystems:
            return None, False
        return self.detected_to_truth_subsystem.get(detected, detected), True


@dataclass(frozen=True)
class _TruthWindowAttributionMatch:
    truth_window_id: str
    dominant_subsystem_match: bool
    dominant_subsystem_mappable: bool
    dominant_subsystem_truth: str | None
    telemetry_parameter_match: bool
    event_parameter_match: bool
    telemetry_truth_subsystem_present: bool
    event_truth_subsystem_present: bool
    payload: dict[str, Any]

    @classmethod
    def from_truth_record(
        cls,
        *,
        truth: dict[str, Any],
        windows_df: pd.DataFrame,
        anomaly_window_attribution_df: pd.DataFrame,
        anomaly_telemetry_attribution_df: pd.DataFrame,
        anomaly_event_attribution_df: pd.DataFrame,
        subsystem_truth_map: DetectedSubsystemTruthMap,
        truth_parameter_to_subsystem: dict[str, str],
        truth_window_id_field: str,
        truth_start_field: str,
        truth_end_field: str,
    ) -> "_TruthWindowAttributionMatch":
        overlap_df = build_truth_window_overlap_table(
            window_like_df=windows_df,
            truth_df=pd.DataFrame.from_records([truth]),
            start_field=truth_start_field,
            end_field=truth_end_field,
        )
        qualifying_overlap_df = overlap_df[strict_overlap_mask(overlap_df)] if not overlap_df.empty else overlap_df.copy()
        qualifying_overlap_df = qualifying_overlap_df.sort_values(
            ["t_start", "truth_coverage_ratio", "win_id"],
            ascending=[True, False, True],
            kind="mergesort",
        )
        qualifying_win_ids = (
            qualifying_overlap_df["win_id"].dropna().astype(int).drop_duplicates().tolist()
            if not qualifying_overlap_df.empty and "win_id" in qualifying_overlap_df.columns
            else []
        )
        primary_win_id = (
            int(qualifying_win_ids[0])
            if qualifying_win_ids
            else None
        )
        window_hits = anomaly_window_attribution_df[
            (anomaly_window_attribution_df.get("tail_id", pd.Series(dtype="object")).astype(str) == str(truth["tail_id"]))
            & (anomaly_window_attribution_df.get("flight_id", pd.Series(dtype="object")).astype(str) == str(truth["flight_id"]))
            & (anomaly_window_attribution_df.get("win_id", pd.Series(dtype="int")).isin(qualifying_win_ids))
        ] if not anomaly_window_attribution_df.empty else pd.DataFrame()
        telemetry_hits = anomaly_telemetry_attribution_df[
            (anomaly_telemetry_attribution_df.get("tail_id", pd.Series(dtype="object")).astype(str) == str(truth["tail_id"]))
            & (anomaly_telemetry_attribution_df.get("flight_id", pd.Series(dtype="object")).astype(str) == str(truth["flight_id"]))
            & (anomaly_telemetry_attribution_df.get("win_id", pd.Series(dtype="int")).isin(qualifying_win_ids))
        ] if not anomaly_telemetry_attribution_df.empty else pd.DataFrame()
        event_hits = anomaly_event_attribution_df[
            (anomaly_event_attribution_df.get("tail_id", pd.Series(dtype="object")).astype(str) == str(truth["tail_id"]))
            & (anomaly_event_attribution_df.get("flight_id", pd.Series(dtype="object")).astype(str) == str(truth["flight_id"]))
            & (anomaly_event_attribution_df.get("win_id", pd.Series(dtype="int")).isin(qualifying_win_ids))
        ] if not anomaly_event_attribution_df.empty else pd.DataFrame()
        matched_window_ids = (
            window_hits.get("win_id", pd.Series(dtype="int")).dropna().astype(int).drop_duplicates().tolist()
            if not window_hits.empty
            else []
        )
        telemetry_parameter_names = _sorted_non_empty_string_values(telemetry_hits, "parameter_name")
        event_parameter_names = _sorted_non_empty_string_values(event_hits, "parameter_name")

        truth_subsystem = str(truth["subsystem_id"])
        truth_parameter = str(truth["parameter_name"])
        dominant_subsystem_truth = None
        dominant_subsystem_mappable = False
        dominant_subsystem_match = False
        if not window_hits.empty and "dominant_subsystem_id" in window_hits.columns:
            dominant_detected = str(window_hits["dominant_subsystem_id"].fillna("").astype(str).mode().iloc[0] or "")
            dominant_subsystem_truth, dominant_subsystem_mappable = subsystem_truth_map.resolve(dominant_detected)
            dominant_subsystem_match = bool(dominant_subsystem_mappable and dominant_subsystem_truth == truth_subsystem)
        telemetry_parameter_match = bool(truth_parameter in telemetry_parameter_names)
        event_parameter_match = bool(truth_parameter in event_parameter_names)

        telemetry_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in telemetry_parameter_names
        }
        telemetry_truth_subsystems.discard(None)
        event_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in event_parameter_names
        }
        event_truth_subsystems.discard(None)

        payload = {
            "tail_id": str(truth["tail_id"]),
            "flight_id": str(truth["flight_id"]),
            truth_window_id_field: str(truth[truth_window_id_field]),
            "subsystem_id": truth_subsystem,
            "parameter_name": truth_parameter,
            "overlapping_window_count": int(len(qualifying_overlap_df)),
            "primary_win_id": primary_win_id,
            "matched_attribution_window_count": int(len(matched_window_ids)),
            "dominant_subsystem_match": bool(dominant_subsystem_match),
            "dominant_subsystem_mappable": bool(dominant_subsystem_mappable),
            "dominant_subsystem_truth": dominant_subsystem_truth,
            "telemetry_parameter_match": telemetry_parameter_match,
            "event_parameter_match": event_parameter_match,
            "any_parameter_match": bool(telemetry_parameter_match or event_parameter_match),
            "both_sources_parameter_match": bool(telemetry_parameter_match and event_parameter_match),
            "telemetry_truth_subsystem_present": bool(truth_subsystem in telemetry_truth_subsystems),
            "event_truth_subsystem_present": bool(truth_subsystem in event_truth_subsystems),
            "telemetry_attributed_parameter_names": telemetry_parameter_names,
            "event_attributed_parameter_names": event_parameter_names,
            "strict_window_coverage_threshold": float(STRICT_WINDOW_COVERAGE_MIN_RATIO),
            "strict_max_early_lead_seconds": float(STRICT_MAX_EARLY_LEAD_SECONDS),
        }
        if "misbehavior_family_label" in truth:
            payload["misbehavior_family_label"] = str(truth["misbehavior_family_label"])
        if "misbehavior_detail_label" in truth:
            payload["misbehavior_detail_label"] = str(truth["misbehavior_detail_label"])
        if "fault_family_label" in truth:
            payload["fault_family_label"] = str(truth["fault_family_label"])
        if "fault_type" in truth:
            payload["fault_type"] = str(truth["fault_type"])
        if "fault_window_id" in truth:
            payload["fault_window_id"] = str(truth["fault_window_id"])
        return cls(
            truth_window_id=payload[truth_window_id_field],
            dominant_subsystem_match=payload["dominant_subsystem_match"],
            dominant_subsystem_mappable=payload["dominant_subsystem_mappable"],
            dominant_subsystem_truth=payload["dominant_subsystem_truth"],
            telemetry_parameter_match=payload["telemetry_parameter_match"],
            event_parameter_match=payload["event_parameter_match"],
            telemetry_truth_subsystem_present=payload["telemetry_truth_subsystem_present"],
            event_truth_subsystem_present=payload["event_truth_subsystem_present"],
            payload=payload,
        )


def _truth_parameter_to_subsystem_map(hierarchy_label_df: pd.DataFrame | None) -> dict[str, str]:
    if hierarchy_label_df is None or hierarchy_label_df.empty:
        return {}
    return {
        str(row["parameter_name"]): str(row["subsystem_id"])
        for row in hierarchy_label_df[["parameter_name", "subsystem_id"]].dropna().to_dict(orient="records")
    }


def _build_parameter_localization_validation(per_truth_df: pd.DataFrame) -> dict[str, Any]:
    if per_truth_df.empty:
        return _empty_parameter_localization_validation()

    telemetry_match = per_truth_df["telemetry_parameter_match"].fillna(False).astype(bool)
    event_match = per_truth_df["event_parameter_match"].fillna(False).astype(bool)
    any_match = per_truth_df["any_parameter_match"].fillna(False).astype(bool)
    both_match = per_truth_df["both_sources_parameter_match"].fillna(False).astype(bool)
    telemetry_truth_subsystem_present = per_truth_df["telemetry_truth_subsystem_present"].fillna(False).astype(bool)
    event_truth_subsystem_present = per_truth_df["event_truth_subsystem_present"].fillna(False).astype(bool)
    case_columns = [
        "tail_id",
        "flight_id",
        "fault_window_id",
        "misbehavior_window_id",
        "subsystem_id",
        "parameter_name",
        "overlapping_window_count",
        "matched_attribution_window_count",
        "telemetry_parameter_match",
        "event_parameter_match",
        "any_parameter_match",
        "both_sources_parameter_match",
        "telemetry_truth_subsystem_present",
        "event_truth_subsystem_present",
        "telemetry_attributed_parameter_names",
        "event_attributed_parameter_names",
    ]
    available_case_columns = [column for column in case_columns if column in per_truth_df.columns]
    cases = (
        per_truth_df[available_case_columns]
        .sort_values(
            [column for column in ("fault_window_id", "misbehavior_window_id", "parameter_name") if column in available_case_columns],
            kind="mergesort",
        )
        .to_dict(orient="records")
    )
    return {
        "status": "ok",
        "truth_window_count": int(len(per_truth_df)),
        "exact_parameter_match_count_by_source": {
            "telemetry": int(telemetry_match.sum()),
            "event": int(event_match.sum()),
            "any": int(any_match.sum()),
            "both": int(both_match.sum()),
        },
        "exact_parameter_match_rate_by_source": {
            "telemetry": float(telemetry_match.mean()),
            "event": float(event_match.mean()),
            "any": float(any_match.mean()),
            "both": float(both_match.mean()),
        },
        "truth_subsystem_present_count_by_source": {
            "telemetry": int(telemetry_truth_subsystem_present.sum()),
            "event": int(event_truth_subsystem_present.sum()),
        },
        "truth_subsystem_present_rate_by_source": {
            "telemetry": float(telemetry_truth_subsystem_present.mean()),
            "event": float(event_truth_subsystem_present.mean()),
        },
        "parameter_localization_cases": cases,
    }


def build_fault_attribution_summary_from_misbehavior_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "dominant_subsystem_match_count": int(summary.get("dominant_subsystem_match_count", 0)),
        "dominant_subsystem_mappable_count": int(summary.get("dominant_subsystem_mappable_count", 0)),
        "dominant_subsystem_match_rate": summary.get("dominant_subsystem_match_rate"),
        "dominant_subsystem_mappable_rate": summary.get("dominant_subsystem_mappable_rate"),
        "telemetry_parameter_match_count": int(summary.get("telemetry_parameter_match_count", 0)),
        "event_parameter_match_count": int(summary.get("event_parameter_match_count", 0)),
        "telemetry_parameter_match_rate": summary.get("telemetry_parameter_match_rate"),
        "event_parameter_match_rate": summary.get("event_parameter_match_rate"),
        "telemetry_truth_subsystem_present_rate": summary.get("telemetry_truth_subsystem_present_rate"),
        "event_truth_subsystem_present_rate": summary.get("event_truth_subsystem_present_rate"),
        "parameter_localization_validation": summary.get(
            "parameter_localization_validation",
            _empty_parameter_localization_validation(),
        ),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row.get("misbehavior_window_id", "")),
                "fault_family_label": row.get("fault_family_label", ""),
                "fault_type": row.get("fault_type", ""),
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }


def validate_attribution_against_misbehavior_truth(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    anomaly_window_attribution_df: pd.DataFrame,
    anomaly_telemetry_attribution_df: pd.DataFrame,
    anomaly_event_attribution_df: pd.DataFrame,
    hierarchy_sensor_map_df: pd.DataFrame | None = None,
    hierarchy_label_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    truth_df = extract_misbehavior_truth_windows(raw_telemetry_df)
    if truth_df.empty:
        return {
            "status": "ok",
            "misbehavior_window_count": 0,
            "dominant_subsystem_match_count": 0,
            "dominant_subsystem_mappable_count": 0,
            "dominant_subsystem_match_rate": None,
            "dominant_subsystem_mappable_rate": None,
            "telemetry_parameter_match_count": 0,
            "event_parameter_match_count": 0,
            "telemetry_parameter_match_rate": None,
            "event_parameter_match_rate": None,
            "telemetry_truth_subsystem_present_rate": None,
            "event_truth_subsystem_present_rate": None,
            "parameter_localization_validation": _empty_parameter_localization_validation(),
            "misbehavior_windows": [],
        }

    windows = windows_df.copy()
    windows["t_start"] = pd.to_datetime(windows["t_start"], utc=True, errors="coerce")
    windows["t_end"] = pd.to_datetime(windows["t_end"], utc=True, errors="coerce")
    window_attr = anomaly_window_attribution_df.copy() if anomaly_window_attribution_df is not None else pd.DataFrame()
    telemetry_attr = anomaly_telemetry_attribution_df.copy() if anomaly_telemetry_attribution_df is not None else pd.DataFrame()
    event_attr = anomaly_event_attribution_df.copy() if anomaly_event_attribution_df is not None else pd.DataFrame()
    subsystem_truth_map = DetectedSubsystemTruthMap.from_hierarchy_frames(
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
    )
    truth_parameter_to_subsystem = _truth_parameter_to_subsystem_map(hierarchy_label_df)

    matches = [
        _TruthWindowAttributionMatch.from_truth_record(
            truth=truth,
            windows_df=windows,
            anomaly_window_attribution_df=window_attr,
            anomaly_telemetry_attribution_df=telemetry_attr,
            anomaly_event_attribution_df=event_attr,
            subsystem_truth_map=subsystem_truth_map,
            truth_parameter_to_subsystem=truth_parameter_to_subsystem,
            truth_window_id_field="misbehavior_window_id",
            truth_start_field="misbehavior_start_timestamp_utc",
            truth_end_field="misbehavior_end_timestamp_utc",
        )
        for truth in truth_df.to_dict(orient="records")
    ]

    per_truth_df = pd.DataFrame.from_records([match.payload for match in matches])
    mappable = per_truth_df[per_truth_df["dominant_subsystem_mappable"].fillna(False).astype(bool)] if not per_truth_df.empty else pd.DataFrame()
    misbehavior_window_count = int(len(per_truth_df))
    dominant_subsystem_match_count = int(per_truth_df["dominant_subsystem_match"].sum()) if not per_truth_df.empty else 0
    dominant_subsystem_mappable_count = int(per_truth_df["dominant_subsystem_mappable"].sum()) if not per_truth_df.empty else 0
    parameter_localization_validation = _build_parameter_localization_validation(per_truth_df)
    exact_parameter_match_count_by_source = dict(
        parameter_localization_validation.get("exact_parameter_match_count_by_source") or {}
    )
    exact_parameter_match_rate_by_source = dict(
        parameter_localization_validation.get("exact_parameter_match_rate_by_source") or {}
    )
    truth_subsystem_present_rate_by_source = dict(
        parameter_localization_validation.get("truth_subsystem_present_rate_by_source") or {}
    )
    return {
        "status": "ok",
        "misbehavior_window_count": misbehavior_window_count,
        "dominant_subsystem_match_count": dominant_subsystem_match_count,
        "dominant_subsystem_mappable_count": dominant_subsystem_mappable_count,
        "dominant_subsystem_match_rate": (
            float(mappable["dominant_subsystem_match"].mean()) if not mappable.empty else 0.0
        ) if misbehavior_window_count > 0 else None,
        "dominant_subsystem_mappable_rate": float(per_truth_df["dominant_subsystem_mappable"].mean()) if not per_truth_df.empty else None,
        "telemetry_parameter_match_count": int(exact_parameter_match_count_by_source.get("telemetry", 0)),
        "event_parameter_match_count": int(exact_parameter_match_count_by_source.get("event", 0)),
        "telemetry_parameter_match_rate": exact_parameter_match_rate_by_source.get("telemetry"),
        "event_parameter_match_rate": exact_parameter_match_rate_by_source.get("event"),
        "telemetry_truth_subsystem_present_rate": truth_subsystem_present_rate_by_source.get("telemetry"),
        "event_truth_subsystem_present_rate": truth_subsystem_present_rate_by_source.get("event"),
        "parameter_localization_validation": parameter_localization_validation,
        "misbehavior_windows": [match.payload for match in matches],
    }


def validate_attribution_against_fault_truth(
    *,
    raw_telemetry_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    anomaly_window_attribution_df: pd.DataFrame,
    anomaly_telemetry_attribution_df: pd.DataFrame,
    anomaly_event_attribution_df: pd.DataFrame,
    hierarchy_sensor_map_df: pd.DataFrame | None = None,
    hierarchy_label_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    return build_fault_attribution_summary_from_misbehavior_summary(
        validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_attribution_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_attribution_df,
        anomaly_event_attribution_df=anomaly_event_attribution_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
        )
    )
