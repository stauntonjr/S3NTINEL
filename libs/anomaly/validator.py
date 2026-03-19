"""Anomaly attribution validation against simulator misbehavior truth with fault wrappers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.scoring.validator import extract_fault_truth_windows, extract_misbehavior_truth_windows


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
        overlapping_windows = windows_df[
            (windows_df["tail_id"].astype(str) == str(truth["tail_id"]))
            & (windows_df["flight_id"].astype(str) == str(truth["flight_id"]))
            & (windows_df["t_end"] >= truth[truth_start_field])
            & (windows_df["t_start"] <= truth[truth_end_field])
        ]
        overlapping_win_ids = {int(item) for item in overlapping_windows.get("win_id", pd.Series(dtype="int")).tolist()}
        window_hits = anomaly_window_attribution_df[
            (anomaly_window_attribution_df.get("tail_id", pd.Series(dtype="object")).astype(str) == str(truth["tail_id"]))
            & (anomaly_window_attribution_df.get("flight_id", pd.Series(dtype="object")).astype(str) == str(truth["flight_id"]))
            & (anomaly_window_attribution_df.get("win_id", pd.Series(dtype="int")).isin(overlapping_win_ids))
        ] if not anomaly_window_attribution_df.empty else pd.DataFrame()
        telemetry_hits = anomaly_telemetry_attribution_df[
            (anomaly_telemetry_attribution_df.get("tail_id", pd.Series(dtype="object")).astype(str) == str(truth["tail_id"]))
            & (anomaly_telemetry_attribution_df.get("flight_id", pd.Series(dtype="object")).astype(str) == str(truth["flight_id"]))
            & (anomaly_telemetry_attribution_df.get("win_id", pd.Series(dtype="int")).isin(overlapping_win_ids))
        ] if not anomaly_telemetry_attribution_df.empty else pd.DataFrame()
        event_hits = anomaly_event_attribution_df[
            (anomaly_event_attribution_df.get("tail_id", pd.Series(dtype="object")).astype(str) == str(truth["tail_id"]))
            & (anomaly_event_attribution_df.get("flight_id", pd.Series(dtype="object")).astype(str) == str(truth["flight_id"]))
            & (anomaly_event_attribution_df.get("win_id", pd.Series(dtype="int")).isin(overlapping_win_ids))
        ] if not anomaly_event_attribution_df.empty else pd.DataFrame()

        truth_subsystem = str(truth["subsystem_id"])
        truth_parameter = str(truth["parameter_name"])
        dominant_subsystem_truth = None
        dominant_subsystem_mappable = False
        dominant_subsystem_match = False
        if not window_hits.empty and "dominant_subsystem_id" in window_hits.columns:
            dominant_detected = str(window_hits["dominant_subsystem_id"].fillna("").astype(str).mode().iloc[0] or "")
            dominant_subsystem_truth, dominant_subsystem_mappable = subsystem_truth_map.resolve(dominant_detected)
            dominant_subsystem_match = bool(dominant_subsystem_mappable and dominant_subsystem_truth == truth_subsystem)

        telemetry_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in telemetry_hits.get("parameter_name", pd.Series(dtype="object")).fillna("").astype(str).tolist()
        }
        telemetry_truth_subsystems.discard(None)
        event_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in event_hits.get("parameter_name", pd.Series(dtype="object")).fillna("").astype(str).tolist()
        }
        event_truth_subsystems.discard(None)

        payload = {
            "tail_id": str(truth["tail_id"]),
            "flight_id": str(truth["flight_id"]),
            truth_window_id_field: str(truth[truth_window_id_field]),
            "subsystem_id": truth_subsystem,
            "parameter_name": truth_parameter,
            "overlapping_window_count": int(len(overlapping_win_ids)),
            "dominant_subsystem_match": bool(dominant_subsystem_match),
            "dominant_subsystem_mappable": bool(dominant_subsystem_mappable),
            "dominant_subsystem_truth": dominant_subsystem_truth,
            "telemetry_parameter_match": bool(
                not telemetry_hits.empty
                and (telemetry_hits["parameter_name"].fillna("").astype(str) == truth_parameter).any()
            ),
            "event_parameter_match": bool(
                not event_hits.empty
                and (event_hits["parameter_name"].fillna("").astype(str) == truth_parameter).any()
            ),
            "telemetry_truth_subsystem_present": bool(truth_subsystem in telemetry_truth_subsystems),
            "event_truth_subsystem_present": bool(truth_subsystem in event_truth_subsystems),
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
            "dominant_subsystem_match_rate": None,
            "telemetry_parameter_match_rate": None,
            "event_parameter_match_rate": None,
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
    return {
        "status": "ok",
        "misbehavior_window_count": int(len(per_truth_df)),
        "dominant_subsystem_match_rate": float(mappable["dominant_subsystem_match"].mean()) if not mappable.empty else None,
        "dominant_subsystem_mappable_rate": float(per_truth_df["dominant_subsystem_mappable"].mean()) if not per_truth_df.empty else None,
        "telemetry_parameter_match_rate": float(per_truth_df["telemetry_parameter_match"].mean()) if not per_truth_df.empty else None,
        "event_parameter_match_rate": float(per_truth_df["event_parameter_match"].mean()) if not per_truth_df.empty else None,
        "telemetry_truth_subsystem_present_rate": float(per_truth_df["telemetry_truth_subsystem_present"].mean()) if not per_truth_df.empty else None,
        "event_truth_subsystem_present_rate": float(per_truth_df["event_truth_subsystem_present"].mean()) if not per_truth_df.empty else None,
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
    summary = validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_attribution_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_attribution_df,
        anomaly_event_attribution_df=anomaly_event_attribution_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
    )
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "dominant_subsystem_match_rate": summary.get("dominant_subsystem_match_rate"),
        "dominant_subsystem_mappable_rate": summary.get("dominant_subsystem_mappable_rate"),
        "telemetry_parameter_match_rate": summary.get("telemetry_parameter_match_rate"),
        "event_parameter_match_rate": summary.get("event_parameter_match_rate"),
        "telemetry_truth_subsystem_present_rate": summary.get("telemetry_truth_subsystem_present_rate"),
        "event_truth_subsystem_present_rate": summary.get("event_truth_subsystem_present_rate"),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row["misbehavior_window_id"]),
                "fault_family_label": row.get("fault_family_label", ""),
                "fault_type": row.get("fault_type", ""),
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }
