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
            "telemetry_selected": 0,
            "event": 0,
            "any": 0,
            "both": 0,
        },
        "exact_parameter_match_rate_by_source": {
            "telemetry": None,
            "telemetry_selected": None,
            "event": None,
            "any": None,
            "both": None,
        },
        "truth_subsystem_present_count_by_source": {
            "telemetry": 0,
            "telemetry_selected": 0,
            "event": 0,
        },
        "truth_subsystem_present_rate_by_source": {
            "telemetry": None,
            "telemetry_selected": None,
            "event": None,
        },
        "parameter_localization_cases": [],
    }


def _empty_module_localization_validation() -> dict[str, Any]:
    return {
        "status": "ok",
        "truth_window_count": 0,
        "dominant_module_match_count": 0,
        "dominant_module_mappable_count": 0,
        "dominant_module_match_rate": None,
        "dominant_module_mappable_rate": None,
        "top_module_candidate_present_count": 0,
        "top_module_candidate_present_rate": None,
        "truth_module_present_count_by_source": {
            "telemetry": 0,
            "event": 0,
        },
        "truth_module_present_rate_by_source": {
            "telemetry": None,
            "event": None,
        },
        "module_localization_cases": [],
    }


def _empty_channel_localization_validation() -> dict[str, Any]:
    return {
        "status": "ok",
        "truth_window_count": 0,
        "truth_window_count_by_score_component": {},
        "dominant_subsystem_match_rate_by_score_component": {},
        "dominant_module_match_rate_by_score_component": {},
        "top_subsystem_candidate_present_rate_by_score_component": {},
        "top_module_candidate_present_rate_by_score_component": {},
        "telemetry_parameter_match_rate_by_score_component": {},
        "telemetry_selected_parameter_match_rate_by_score_component": {},
        "event_parameter_match_rate_by_score_component": {},
        "channel_localization_cases": [],
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


def _records_with_none_for_missing(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def _sorted_detected_candidate_ids(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []

    candidate_ids: set[str] = set()
    for value in df[column].tolist():
        if value is None:
            continue
        if hasattr(value, "tolist") and not isinstance(value, (list, tuple, dict, str)):
            value = value.tolist()
        if isinstance(value, dict):
            value = [value]
        if not isinstance(value, (list, tuple)):
            continue
        for entry in value:
            if hasattr(entry, "asDict"):
                entry = entry.asDict(recursive=True)
            elif hasattr(entry, "_asdict"):
                entry = entry._asdict()
            if not isinstance(entry, dict):
                continue
            candidate_id = str(entry.get("id") or "")
            if candidate_id:
                candidate_ids.add(candidate_id)
    return sorted(candidate_ids)


def _resolve_truth_candidate_ids(
    detected_candidate_ids: list[str],
    truth_map: "DetectedLocalizationTruthMap",
) -> list[str]:
    resolved_truth_ids: set[str] = set()
    for detected_id in detected_candidate_ids:
        truth_id, mappable = truth_map.resolve(detected_id)
        if mappable and truth_id:
            resolved_truth_ids.add(truth_id)
    return sorted(resolved_truth_ids)


def _rate_by_score_component(
    per_truth_df: pd.DataFrame,
    *,
    value_column: str,
    require_mappable_column: str | None = None,
) -> dict[str, float]:
    if per_truth_df.empty or value_column not in per_truth_df.columns or "dominant_score_component" not in per_truth_df.columns:
        return {}

    working_df = per_truth_df.copy()
    working_df["dominant_score_component"] = working_df["dominant_score_component"].fillna("").astype(str)
    if require_mappable_column is not None and require_mappable_column in working_df.columns:
        working_df = working_df[working_df[require_mappable_column].fillna(False).astype(bool)]
    if working_df.empty:
        return {}

    return {
        str(component): float(group[value_column].fillna(False).astype(bool).mean())
        for component, group in working_df.groupby("dominant_score_component", dropna=False)
        if str(component)
    }


@dataclass(frozen=True)
class DetectedLocalizationTruthMap:
    detected_to_truth_id: dict[str, str]
    ambiguous_detected_ids: set[str]

    @classmethod
    def from_hierarchy_frames(
        cls,
        *,
        hierarchy_sensor_map_df: pd.DataFrame | None,
        hierarchy_label_df: pd.DataFrame | None,
        detected_id_field: str,
        truth_id_field: str,
    ) -> "DetectedLocalizationTruthMap":
        if (
            hierarchy_sensor_map_df is None
            or hierarchy_label_df is None
            or hierarchy_sensor_map_df.empty
            or hierarchy_label_df.empty
        ):
            return cls(detected_to_truth_id={}, ambiguous_detected_ids=set())

        hierarchy_joined = hierarchy_sensor_map_df.merge(
            hierarchy_label_df[["parameter_name", truth_id_field]].rename(columns={truth_id_field: "_truth_id"}),
            on="parameter_name",
            how="inner",
        )
        detected_to_truth_id: dict[str, str] = {}
        ambiguous_detected_ids: set[str] = set()
        if hierarchy_joined.empty:
            return cls(
                detected_to_truth_id=detected_to_truth_id,
                ambiguous_detected_ids=ambiguous_detected_ids,
            )

        for detected_id, group in hierarchy_joined.groupby(detected_id_field, dropna=False):
            counts = group["_truth_id"].fillna("").astype(str).value_counts()
            if counts.empty:
                continue
            top_truth_id = str(counts.index[0])
            top_count = int(counts.iloc[0])
            second_count = int(counts.iloc[1]) if len(counts) > 1 else -1
            if top_count > second_count:
                detected_to_truth_id[str(detected_id)] = top_truth_id
            else:
                ambiguous_detected_ids.add(str(detected_id))
        return cls(
            detected_to_truth_id=detected_to_truth_id,
            ambiguous_detected_ids=ambiguous_detected_ids,
        )

    def resolve(self, detected_id: str) -> tuple[str | None, bool]:
        detected = str(detected_id or "")
        if not detected or detected in self.ambiguous_detected_ids:
            return None, False
        return self.detected_to_truth_id.get(detected, detected), True


@dataclass(frozen=True)
class _TruthWindowAttributionMatch:
    truth_window_id: str
    dominant_subsystem_match: bool
    dominant_subsystem_mappable: bool
    dominant_subsystem_truth: str | None
    dominant_module_match: bool
    dominant_module_mappable: bool
    dominant_module_truth: str | None
    telemetry_parameter_match: bool
    event_parameter_match: bool
    telemetry_truth_subsystem_present: bool
    event_truth_subsystem_present: bool
    telemetry_truth_module_present: bool
    event_truth_module_present: bool
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
        subsystem_truth_map: DetectedLocalizationTruthMap,
        module_truth_map: DetectedLocalizationTruthMap,
        truth_parameter_to_subsystem: dict[str, str],
        truth_parameter_to_module: dict[str, str],
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
        telemetry_selected_mask = (
            telemetry_hits["parameter_localization_selected"].fillna(False).astype(bool)
            if not telemetry_hits.empty and "parameter_localization_selected" in telemetry_hits.columns
            else pd.Series(False, index=telemetry_hits.index, dtype="bool")
        )
        telemetry_selected_hits = telemetry_hits.loc[telemetry_selected_mask] if not telemetry_hits.empty else pd.DataFrame()
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
        telemetry_selected_parameter_names = _sorted_non_empty_string_values(telemetry_selected_hits, "parameter_name")
        event_parameter_names = _sorted_non_empty_string_values(event_hits, "parameter_name")
        dominant_score_component = (
            str(window_hits["dominant_score_component"].fillna("").astype(str).mode().iloc[0] or "")
            if not window_hits.empty and "dominant_score_component" in window_hits.columns
            else ""
        )

        truth_subsystem = str(truth["subsystem_id"])
        truth_parameter = str(truth["parameter_name"])
        truth_module = str(truth_parameter_to_module.get(truth_parameter, "") or "")
        dominant_subsystem_truth = None
        dominant_subsystem_mappable = False
        dominant_subsystem_match = False
        if not window_hits.empty and "dominant_subsystem_id" in window_hits.columns:
            dominant_detected = str(window_hits["dominant_subsystem_id"].fillna("").astype(str).mode().iloc[0] or "")
            dominant_subsystem_truth, dominant_subsystem_mappable = subsystem_truth_map.resolve(dominant_detected)
            dominant_subsystem_match = bool(dominant_subsystem_mappable and dominant_subsystem_truth == truth_subsystem)
        dominant_module_truth = None
        dominant_module_mappable = False
        dominant_module_match = False
        if truth_module and not window_hits.empty and "dominant_module_id" in window_hits.columns:
            dominant_module_detected = str(window_hits["dominant_module_id"].fillna("").astype(str).mode().iloc[0] or "")
            dominant_module_truth, dominant_module_mappable = module_truth_map.resolve(dominant_module_detected)
            dominant_module_match = bool(dominant_module_mappable and dominant_module_truth == truth_module)
        top_subsystem_candidate_ids_detected = _sorted_detected_candidate_ids(window_hits, "top_subsystem_candidates")
        top_module_candidate_ids_detected = _sorted_detected_candidate_ids(window_hits, "top_module_candidates")
        top_subsystem_candidate_truth_ids = _resolve_truth_candidate_ids(
            top_subsystem_candidate_ids_detected,
            subsystem_truth_map,
        )
        top_module_candidate_truth_ids = _resolve_truth_candidate_ids(
            top_module_candidate_ids_detected,
            module_truth_map,
        )
        top_subsystem_candidate_present = bool(truth_subsystem in top_subsystem_candidate_truth_ids)
        top_module_candidate_present = bool(truth_module and truth_module in top_module_candidate_truth_ids)
        telemetry_parameter_match = bool(truth_parameter in telemetry_parameter_names)
        telemetry_selected_parameter_match = bool(truth_parameter in telemetry_selected_parameter_names)
        event_parameter_match = bool(truth_parameter in event_parameter_names)

        telemetry_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in telemetry_parameter_names
        }
        telemetry_truth_subsystems.discard(None)
        telemetry_selected_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in telemetry_selected_parameter_names
        }
        telemetry_selected_truth_subsystems.discard(None)
        event_truth_subsystems = {
            truth_parameter_to_subsystem.get(str(parameter_name))
            for parameter_name in event_parameter_names
        }
        event_truth_subsystems.discard(None)
        telemetry_truth_modules = {
            truth_parameter_to_module.get(str(parameter_name))
            for parameter_name in telemetry_parameter_names
        }
        telemetry_truth_modules.discard(None)
        telemetry_truth_modules.discard("")
        telemetry_selected_truth_modules = {
            truth_parameter_to_module.get(str(parameter_name))
            for parameter_name in telemetry_selected_parameter_names
        }
        telemetry_selected_truth_modules.discard(None)
        telemetry_selected_truth_modules.discard("")
        event_truth_modules = {
            truth_parameter_to_module.get(str(parameter_name))
            for parameter_name in event_parameter_names
        }
        event_truth_modules.discard(None)
        event_truth_modules.discard("")

        payload = {
            "tail_id": str(truth["tail_id"]),
            "flight_id": str(truth["flight_id"]),
            truth_window_id_field: str(truth[truth_window_id_field]),
            "subsystem_id": truth_subsystem,
            "module_id": truth_module,
            "parameter_name": truth_parameter,
            "dominant_score_component": dominant_score_component or None,
            "overlapping_window_count": int(len(qualifying_overlap_df)),
            "primary_win_id": primary_win_id,
            "matched_attribution_window_count": int(len(matched_window_ids)),
            "dominant_subsystem_match": bool(dominant_subsystem_match),
            "dominant_subsystem_mappable": bool(dominant_subsystem_mappable),
            "dominant_subsystem_truth": dominant_subsystem_truth,
            "dominant_module_match": bool(dominant_module_match),
            "dominant_module_mappable": bool(dominant_module_mappable),
            "dominant_module_truth": dominant_module_truth,
            "top_subsystem_candidate_present": top_subsystem_candidate_present,
            "top_module_candidate_present": top_module_candidate_present,
            "top_subsystem_candidate_ids_detected": top_subsystem_candidate_ids_detected,
            "top_module_candidate_ids_detected": top_module_candidate_ids_detected,
            "top_subsystem_candidate_truth_ids": top_subsystem_candidate_truth_ids,
            "top_module_candidate_truth_ids": top_module_candidate_truth_ids,
            "telemetry_parameter_match": telemetry_parameter_match,
            "telemetry_selected_parameter_match": telemetry_selected_parameter_match,
            "event_parameter_match": event_parameter_match,
            "any_parameter_match": bool(telemetry_parameter_match or event_parameter_match),
            "both_sources_parameter_match": bool(telemetry_parameter_match and event_parameter_match),
            "telemetry_truth_subsystem_present": bool(truth_subsystem in telemetry_truth_subsystems),
            "telemetry_selected_truth_subsystem_present": bool(truth_subsystem in telemetry_selected_truth_subsystems),
            "event_truth_subsystem_present": bool(truth_subsystem in event_truth_subsystems),
            "telemetry_truth_module_present": bool(truth_module and truth_module in telemetry_truth_modules),
            "telemetry_selected_truth_module_present": bool(truth_module and truth_module in telemetry_selected_truth_modules),
            "event_truth_module_present": bool(truth_module and truth_module in event_truth_modules),
            "telemetry_attributed_parameter_names": telemetry_parameter_names,
            "telemetry_selected_attributed_parameter_names": telemetry_selected_parameter_names,
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
            dominant_module_match=payload["dominant_module_match"],
            dominant_module_mappable=payload["dominant_module_mappable"],
            dominant_module_truth=payload["dominant_module_truth"],
            telemetry_parameter_match=payload["telemetry_parameter_match"],
            event_parameter_match=payload["event_parameter_match"],
            telemetry_truth_subsystem_present=payload["telemetry_truth_subsystem_present"],
            event_truth_subsystem_present=payload["event_truth_subsystem_present"],
            telemetry_truth_module_present=payload["telemetry_truth_module_present"],
            event_truth_module_present=payload["event_truth_module_present"],
            payload=payload,
        )


def _truth_parameter_to_subsystem_map(hierarchy_label_df: pd.DataFrame | None) -> dict[str, str]:
    if hierarchy_label_df is None or hierarchy_label_df.empty:
        return {}
    return {
        str(row["parameter_name"]): str(row["subsystem_id"])
        for row in hierarchy_label_df[["parameter_name", "subsystem_id"]].dropna().to_dict(orient="records")
    }


def _truth_parameter_to_module_map(hierarchy_label_df: pd.DataFrame | None) -> dict[str, str]:
    if hierarchy_label_df is None or hierarchy_label_df.empty or "module_id" not in hierarchy_label_df.columns:
        return {}
    return {
        str(row["parameter_name"]): str(row["module_id"])
        for row in hierarchy_label_df[["parameter_name", "module_id"]].dropna().to_dict(orient="records")
    }


def _build_parameter_localization_validation(per_truth_df: pd.DataFrame) -> dict[str, Any]:
    if per_truth_df.empty:
        return _empty_parameter_localization_validation()

    telemetry_match = per_truth_df["telemetry_parameter_match"].fillna(False).astype(bool)
    telemetry_selected_match = per_truth_df["telemetry_selected_parameter_match"].fillna(False).astype(bool)
    event_match = per_truth_df["event_parameter_match"].fillna(False).astype(bool)
    any_match = per_truth_df["any_parameter_match"].fillna(False).astype(bool)
    both_match = per_truth_df["both_sources_parameter_match"].fillna(False).astype(bool)
    telemetry_truth_subsystem_present = per_truth_df["telemetry_truth_subsystem_present"].fillna(False).astype(bool)
    telemetry_selected_truth_subsystem_present = (
        per_truth_df["telemetry_selected_truth_subsystem_present"].fillna(False).astype(bool)
    )
    event_truth_subsystem_present = per_truth_df["event_truth_subsystem_present"].fillna(False).astype(bool)
    case_columns = [
        "tail_id",
        "flight_id",
        "fault_window_id",
        "misbehavior_window_id",
        "subsystem_id",
        "module_id",
        "parameter_name",
        "dominant_score_component",
        "overlapping_window_count",
        "matched_attribution_window_count",
        "telemetry_parameter_match",
        "telemetry_selected_parameter_match",
        "event_parameter_match",
        "any_parameter_match",
        "both_sources_parameter_match",
        "telemetry_truth_subsystem_present",
        "telemetry_selected_truth_subsystem_present",
        "event_truth_subsystem_present",
        "telemetry_truth_module_present",
        "telemetry_selected_truth_module_present",
        "event_truth_module_present",
        "telemetry_attributed_parameter_names",
        "telemetry_selected_attributed_parameter_names",
        "event_attributed_parameter_names",
    ]
    available_case_columns = [column for column in case_columns if column in per_truth_df.columns]
    cases = _records_with_none_for_missing(
        per_truth_df[available_case_columns].sort_values(
            [column for column in ("fault_window_id", "misbehavior_window_id", "parameter_name") if column in available_case_columns],
            kind="mergesort",
        )
    )
    return {
        "status": "ok",
        "truth_window_count": int(len(per_truth_df)),
        "exact_parameter_match_count_by_source": {
            "telemetry": int(telemetry_match.sum()),
            "telemetry_selected": int(telemetry_selected_match.sum()),
            "event": int(event_match.sum()),
            "any": int(any_match.sum()),
            "both": int(both_match.sum()),
        },
        "exact_parameter_match_rate_by_source": {
            "telemetry": float(telemetry_match.mean()),
            "telemetry_selected": float(telemetry_selected_match.mean()),
            "event": float(event_match.mean()),
            "any": float(any_match.mean()),
            "both": float(both_match.mean()),
        },
        "truth_subsystem_present_count_by_source": {
            "telemetry": int(telemetry_truth_subsystem_present.sum()),
            "telemetry_selected": int(telemetry_selected_truth_subsystem_present.sum()),
            "event": int(event_truth_subsystem_present.sum()),
        },
        "truth_subsystem_present_rate_by_source": {
            "telemetry": float(telemetry_truth_subsystem_present.mean()),
            "telemetry_selected": float(telemetry_selected_truth_subsystem_present.mean()),
            "event": float(event_truth_subsystem_present.mean()),
        },
        "parameter_localization_cases": cases,
    }


def _build_module_localization_validation(per_truth_df: pd.DataFrame) -> dict[str, Any]:
    if per_truth_df.empty:
        return _empty_module_localization_validation()

    dominant_module_match = per_truth_df["dominant_module_match"].fillna(False).astype(bool)
    dominant_module_mappable = per_truth_df["dominant_module_mappable"].fillna(False).astype(bool)
    top_module_candidate_present = per_truth_df["top_module_candidate_present"].fillna(False).astype(bool)
    telemetry_truth_module_present = per_truth_df["telemetry_truth_module_present"].fillna(False).astype(bool)
    event_truth_module_present = per_truth_df["event_truth_module_present"].fillna(False).astype(bool)
    case_columns = [
        "tail_id",
        "flight_id",
        "fault_window_id",
        "misbehavior_window_id",
        "subsystem_id",
        "module_id",
        "parameter_name",
        "dominant_score_component",
        "matched_attribution_window_count",
        "dominant_module_match",
        "dominant_module_mappable",
        "dominant_module_truth",
        "top_module_candidate_present",
        "top_module_candidate_ids_detected",
        "top_module_candidate_truth_ids",
        "telemetry_truth_module_present",
        "telemetry_selected_truth_module_present",
        "event_truth_module_present",
    ]
    available_case_columns = [column for column in case_columns if column in per_truth_df.columns]
    cases = _records_with_none_for_missing(
        per_truth_df[available_case_columns].sort_values(
            [column for column in ("fault_window_id", "misbehavior_window_id", "parameter_name") if column in available_case_columns],
            kind="mergesort",
        )
    )
    mappable = per_truth_df[dominant_module_mappable]
    return {
        "status": "ok",
        "truth_window_count": int(len(per_truth_df)),
        "dominant_module_match_count": int(dominant_module_match.sum()),
        "dominant_module_mappable_count": int(dominant_module_mappable.sum()),
        "dominant_module_match_rate": float(mappable["dominant_module_match"].mean()) if not mappable.empty else 0.0,
        "dominant_module_mappable_rate": float(dominant_module_mappable.mean()),
        "top_module_candidate_present_count": int(top_module_candidate_present.sum()),
        "top_module_candidate_present_rate": float(top_module_candidate_present.mean()),
        "truth_module_present_count_by_source": {
            "telemetry": int(telemetry_truth_module_present.sum()),
            "event": int(event_truth_module_present.sum()),
        },
        "truth_module_present_rate_by_source": {
            "telemetry": float(telemetry_truth_module_present.mean()),
            "event": float(event_truth_module_present.mean()),
        },
        "module_localization_cases": cases,
    }


def _build_channel_localization_validation(per_truth_df: pd.DataFrame) -> dict[str, Any]:
    if per_truth_df.empty:
        return _empty_channel_localization_validation()

    case_columns = [
        "tail_id",
        "flight_id",
        "fault_window_id",
        "misbehavior_window_id",
        "subsystem_id",
        "module_id",
        "parameter_name",
        "dominant_score_component",
        "dominant_subsystem_match",
        "dominant_subsystem_mappable",
        "dominant_module_match",
        "dominant_module_mappable",
        "top_subsystem_candidate_present",
        "top_module_candidate_present",
        "top_subsystem_candidate_ids_detected",
        "top_module_candidate_ids_detected",
        "telemetry_parameter_match",
        "telemetry_selected_parameter_match",
        "event_parameter_match",
        "telemetry_selected_attributed_parameter_names",
    ]
    available_case_columns = [column for column in case_columns if column in per_truth_df.columns]
    cases = _records_with_none_for_missing(
        per_truth_df[available_case_columns].sort_values(
            [column for column in ("dominant_score_component", "fault_window_id", "misbehavior_window_id", "parameter_name") if column in available_case_columns],
            kind="mergesort",
        )
    )
    component_counts = (
        per_truth_df["dominant_score_component"].fillna("").astype(str).value_counts().sort_index().to_dict()
        if "dominant_score_component" in per_truth_df.columns
        else {}
    )
    component_counts = {str(component): int(count) for component, count in component_counts.items() if str(component)}
    return {
        "status": "ok",
        "truth_window_count": int(len(per_truth_df)),
        "truth_window_count_by_score_component": component_counts,
        "dominant_subsystem_match_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="dominant_subsystem_match",
            require_mappable_column="dominant_subsystem_mappable",
        ),
        "dominant_module_match_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="dominant_module_match",
            require_mappable_column="dominant_module_mappable",
        ),
        "top_subsystem_candidate_present_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="top_subsystem_candidate_present",
        ),
        "top_module_candidate_present_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="top_module_candidate_present",
        ),
        "telemetry_parameter_match_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="telemetry_parameter_match",
        ),
        "telemetry_selected_parameter_match_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="telemetry_selected_parameter_match",
        ),
        "event_parameter_match_rate_by_score_component": _rate_by_score_component(
            per_truth_df,
            value_column="event_parameter_match",
        ),
        "channel_localization_cases": cases,
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
        "top_subsystem_candidate_present_count": int(summary.get("top_subsystem_candidate_present_count", 0)),
        "top_subsystem_candidate_present_rate": summary.get("top_subsystem_candidate_present_rate"),
        "dominant_module_match_count": int(summary.get("dominant_module_match_count", 0)),
        "dominant_module_mappable_count": int(summary.get("dominant_module_mappable_count", 0)),
        "dominant_module_match_rate": summary.get("dominant_module_match_rate"),
        "dominant_module_mappable_rate": summary.get("dominant_module_mappable_rate"),
        "top_module_candidate_present_count": int(summary.get("top_module_candidate_present_count", 0)),
        "top_module_candidate_present_rate": summary.get("top_module_candidate_present_rate"),
        "telemetry_parameter_match_count": int(summary.get("telemetry_parameter_match_count", 0)),
        "event_parameter_match_count": int(summary.get("event_parameter_match_count", 0)),
        "telemetry_parameter_match_rate": summary.get("telemetry_parameter_match_rate"),
        "event_parameter_match_rate": summary.get("event_parameter_match_rate"),
        "telemetry_truth_subsystem_present_rate": summary.get("telemetry_truth_subsystem_present_rate"),
        "event_truth_subsystem_present_rate": summary.get("event_truth_subsystem_present_rate"),
        "module_localization_validation": summary.get(
            "module_localization_validation",
            _empty_module_localization_validation(),
        ),
        "channel_localization_validation": summary.get(
            "channel_localization_validation",
            _empty_channel_localization_validation(),
        ),
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
            "top_subsystem_candidate_present_count": 0,
            "top_subsystem_candidate_present_rate": None,
            "dominant_module_match_count": 0,
            "dominant_module_mappable_count": 0,
            "dominant_module_match_rate": None,
            "dominant_module_mappable_rate": None,
            "top_module_candidate_present_count": 0,
            "top_module_candidate_present_rate": None,
            "telemetry_parameter_match_count": 0,
            "event_parameter_match_count": 0,
            "telemetry_parameter_match_rate": None,
            "event_parameter_match_rate": None,
            "telemetry_truth_subsystem_present_rate": None,
            "event_truth_subsystem_present_rate": None,
            "module_localization_validation": _empty_module_localization_validation(),
            "channel_localization_validation": _empty_channel_localization_validation(),
            "parameter_localization_validation": _empty_parameter_localization_validation(),
            "misbehavior_windows": [],
        }

    windows = windows_df.copy()
    windows["t_start"] = pd.to_datetime(windows["t_start"], utc=True, errors="coerce")
    windows["t_end"] = pd.to_datetime(windows["t_end"], utc=True, errors="coerce")
    window_attr = anomaly_window_attribution_df.copy() if anomaly_window_attribution_df is not None else pd.DataFrame()
    telemetry_attr = anomaly_telemetry_attribution_df.copy() if anomaly_telemetry_attribution_df is not None else pd.DataFrame()
    event_attr = anomaly_event_attribution_df.copy() if anomaly_event_attribution_df is not None else pd.DataFrame()
    subsystem_truth_map = DetectedLocalizationTruthMap.from_hierarchy_frames(
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
        detected_id_field="subsystem_id",
        truth_id_field="subsystem_id",
    )
    module_truth_map = DetectedLocalizationTruthMap.from_hierarchy_frames(
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
        detected_id_field="module_id",
        truth_id_field="module_id",
    )
    truth_parameter_to_subsystem = _truth_parameter_to_subsystem_map(hierarchy_label_df)
    truth_parameter_to_module = _truth_parameter_to_module_map(hierarchy_label_df)

    matches = [
        _TruthWindowAttributionMatch.from_truth_record(
            truth=truth,
            windows_df=windows,
            anomaly_window_attribution_df=window_attr,
            anomaly_telemetry_attribution_df=telemetry_attr,
            anomaly_event_attribution_df=event_attr,
            subsystem_truth_map=subsystem_truth_map,
            module_truth_map=module_truth_map,
            truth_parameter_to_subsystem=truth_parameter_to_subsystem,
            truth_parameter_to_module=truth_parameter_to_module,
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
    top_subsystem_candidate_present_count = int(per_truth_df["top_subsystem_candidate_present"].sum()) if not per_truth_df.empty else 0
    dominant_module_match_count = int(per_truth_df["dominant_module_match"].sum()) if not per_truth_df.empty else 0
    dominant_module_mappable_count = int(per_truth_df["dominant_module_mappable"].sum()) if not per_truth_df.empty else 0
    top_module_candidate_present_count = int(per_truth_df["top_module_candidate_present"].sum()) if not per_truth_df.empty else 0
    module_localization_validation = _build_module_localization_validation(per_truth_df)
    channel_localization_validation = _build_channel_localization_validation(per_truth_df)
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
        "top_subsystem_candidate_present_count": top_subsystem_candidate_present_count,
        "top_subsystem_candidate_present_rate": float(per_truth_df["top_subsystem_candidate_present"].mean()) if not per_truth_df.empty else None,
        "dominant_module_match_count": dominant_module_match_count,
        "dominant_module_mappable_count": dominant_module_mappable_count,
        "dominant_module_match_rate": module_localization_validation.get("dominant_module_match_rate"),
        "dominant_module_mappable_rate": module_localization_validation.get("dominant_module_mappable_rate"),
        "top_module_candidate_present_count": top_module_candidate_present_count,
        "top_module_candidate_present_rate": module_localization_validation.get("top_module_candidate_present_rate"),
        "telemetry_parameter_match_count": int(exact_parameter_match_count_by_source.get("telemetry", 0)),
        "event_parameter_match_count": int(exact_parameter_match_count_by_source.get("event", 0)),
        "telemetry_parameter_match_rate": exact_parameter_match_rate_by_source.get("telemetry"),
        "event_parameter_match_rate": exact_parameter_match_rate_by_source.get("event"),
        "telemetry_truth_subsystem_present_rate": truth_subsystem_present_rate_by_source.get("telemetry"),
        "event_truth_subsystem_present_rate": truth_subsystem_present_rate_by_source.get("event"),
        "module_localization_validation": module_localization_validation,
        "channel_localization_validation": channel_localization_validation,
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
