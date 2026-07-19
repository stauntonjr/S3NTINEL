"""Anomaly attribution validation against simulator misbehavior truth with fault wrappers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.anomaly.frames import ANOMALY_LOCALIZATION_PARAMETER_TOP_K
from libs.scoring.validator import (
    STRICT_MAX_EARLY_LEAD_SECONDS,
    STRICT_WINDOW_COVERAGE_MIN_RATIO,
    build_truth_window_overlap_table,
    extract_misbehavior_truth_windows,
    strict_overlap_mask,
)

RECONSTRUCTION_ERROR_CHANNEL = "reconstruction_error"


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


def _empty_reconstruction_localization_validation() -> dict[str, Any]:
    return {
        "status": "ok",
        "truth_window_count": 0,
        "reconstruction_truth_window_count": 0,
        "reconstruction_failure_count": 0,
        "failure_count_by_bucket": {},
        "failure_rate_by_bucket": {},
        "truth_subsystem_present_in_selected_telemetry_count": 0,
        "truth_subsystem_present_in_selected_telemetry_rate": None,
        "truth_module_present_in_selected_telemetry_count": 0,
        "truth_module_present_in_selected_telemetry_rate": None,
        "truth_subsystem_present_in_top_subsystem_candidates_count": 0,
        "truth_subsystem_present_in_top_subsystem_candidates_rate": None,
        "truth_module_present_in_top_module_candidates_count": 0,
        "truth_module_present_in_top_module_candidates_rate": None,
        "top_ranked_selected_parameter_exact_match_count": 0,
        "top_ranked_selected_parameter_exact_match_rate": None,
        "top_ranked_selected_parameter_in_truth_subsystem_count": 0,
        "top_ranked_selected_parameter_in_truth_subsystem_rate": None,
        "top_ranked_selected_parameter_in_truth_module_count": 0,
        "top_ranked_selected_parameter_in_truth_module_rate": None,
        "reconstruction_localization_cases": [],
    }


def _empty_candidate_cut_validation() -> dict[str, Any]:
    return {
        "status": "ok",
        "structural_candidate_cut_width": ANOMALY_LOCALIZATION_PARAMETER_TOP_K,
        "truth_window_count": 0,
        "diagnostic_status_count": {},
        "ranked_truth_parameter_count": 0,
        "truth_parameter_selected_for_telemetry_count": 0,
        "truth_parameter_selected_for_telemetry_rate": None,
        "truth_parameter_within_structural_candidate_cut_count": 0,
        "truth_parameter_within_structural_candidate_cut_rate": None,
        "truth_parameter_below_structural_candidate_cut_count": 0,
        "truth_parameter_below_structural_candidate_cut_rate": None,
        "candidate_cut_cases": [],
    }


def _empty_hierarchy_cluster_alignment_validation() -> dict[str, Any]:
    return {
        "status": "ok",
        "methodology": {
            "interpretation": (
                "compares inferred dynamic hierarchy clusters to simulator labels by parameter membership; "
                "cluster identifiers are not expected to match literal truth identifiers"
            ),
            "mapping_rule": "a detected cluster maps only when one truth label has a strict plurality",
        },
        "by_level": {
            "subsystem": {
                "detected_cluster_count": 0,
                "mappable_detected_cluster_count": 0,
                "ambiguous_detected_cluster_count": 0,
                "pure_detected_cluster_count": 0,
                "mixed_detected_cluster_count": 0,
                "parameter_count": 0,
                "weighted_plurality_purity": None,
                "mean_cluster_plurality_purity": None,
                "clusters": [],
            },
            "module": {
                "detected_cluster_count": 0,
                "mappable_detected_cluster_count": 0,
                "ambiguous_detected_cluster_count": 0,
                "pure_detected_cluster_count": 0,
                "mixed_detected_cluster_count": 0,
                "parameter_count": 0,
                "weighted_plurality_purity": None,
                "mean_cluster_plurality_purity": None,
                "clusters": [],
            },
        },
    }


def _build_detected_cluster_alignment(
    *,
    hierarchy_sensor_map_df: pd.DataFrame | None,
    hierarchy_label_df: pd.DataFrame | None,
    detected_id_field: str,
    truth_id_field: str,
) -> dict[str, Any]:
    if (
        hierarchy_sensor_map_df is None
        or hierarchy_label_df is None
        or hierarchy_sensor_map_df.empty
        or hierarchy_label_df.empty
        or detected_id_field not in hierarchy_sensor_map_df.columns
        or truth_id_field not in hierarchy_label_df.columns
    ):
        return _empty_hierarchy_cluster_alignment_validation()["by_level"][
            "subsystem" if detected_id_field == "subsystem_id" else "module"
        ]

    joined = hierarchy_sensor_map_df[["parameter_name", detected_id_field]].merge(
        hierarchy_label_df[["parameter_name", truth_id_field]].rename(columns={truth_id_field: "_truth_id"}),
        on="parameter_name",
        how="inner",
    ).drop_duplicates()
    if joined.empty:
        return _empty_hierarchy_cluster_alignment_validation()["by_level"][
            "subsystem" if detected_id_field == "subsystem_id" else "module"
        ]

    clusters: list[dict[str, Any]] = []
    for detected_id, group in joined.groupby(detected_id_field, dropna=False):
        truth_counts = (
            group["_truth_id"]
            .fillna("")
            .astype(str)
            .value_counts()
            .sort_index()
            .sort_values(ascending=False, kind="mergesort")
        )
        if truth_counts.empty:
            continue
        dominant_truth_id = str(truth_counts.index[0])
        dominant_count = int(truth_counts.iloc[0])
        second_count = int(truth_counts.iloc[1]) if len(truth_counts) > 1 else -1
        parameter_count = int(len(group))
        strict_plurality = bool(dominant_truth_id and dominant_count > second_count)
        detected_id_text = "" if pd.isna(detected_id) else str(detected_id)
        clusters.append(
            {
                "detected_id": detected_id_text,
                "dominant_truth_id": dominant_truth_id or None,
                "parameter_count": parameter_count,
                "distinct_truth_id_count": int(len(truth_counts)),
                "dominant_truth_parameter_count": dominant_count,
                "plurality_purity": float(dominant_count / parameter_count),
                "mappable_by_strict_plurality": strict_plurality,
            }
        )

    clusters.sort(key=lambda row: row["detected_id"])
    if not clusters:
        return _empty_hierarchy_cluster_alignment_validation()["by_level"][
            "subsystem" if detected_id_field == "subsystem_id" else "module"
        ]
    parameter_count = sum(int(cluster["parameter_count"]) for cluster in clusters)
    dominant_parameter_count = sum(int(cluster["dominant_truth_parameter_count"]) for cluster in clusters)
    pure_count = sum(float(cluster["plurality_purity"]) == 1.0 for cluster in clusters)
    return {
        "detected_cluster_count": int(len(clusters)),
        "mappable_detected_cluster_count": int(sum(cluster["mappable_by_strict_plurality"] for cluster in clusters)),
        "ambiguous_detected_cluster_count": int(sum(not cluster["mappable_by_strict_plurality"] for cluster in clusters)),
        "pure_detected_cluster_count": int(pure_count),
        "mixed_detected_cluster_count": int(len(clusters) - pure_count),
        "parameter_count": int(parameter_count),
        "weighted_plurality_purity": float(dominant_parameter_count / parameter_count),
        "mean_cluster_plurality_purity": float(
            sum(float(cluster["plurality_purity"]) for cluster in clusters) / len(clusters)
        ),
        "clusters": clusters,
    }


def _build_hierarchy_cluster_alignment_validation(
    *,
    hierarchy_sensor_map_df: pd.DataFrame | None,
    hierarchy_label_df: pd.DataFrame | None,
) -> dict[str, Any]:
    summary = _empty_hierarchy_cluster_alignment_validation()
    summary["by_level"] = {
        "subsystem": _build_detected_cluster_alignment(
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            hierarchy_label_df=hierarchy_label_df,
            detected_id_field="subsystem_id",
            truth_id_field="subsystem_id",
        ),
        "module": _build_detected_cluster_alignment(
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            hierarchy_label_df=hierarchy_label_df,
            detected_id_field="module_id",
            truth_id_field="module_id",
        ),
    }
    return summary


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


def _optional_non_empty_string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    value_text = str(value)
    return value_text or None


def _optional_bool(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    return bool(value)


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


def _top_ranked_selected_parameter_details(
    telemetry_selected_hits: pd.DataFrame,
    *,
    truth_parameter_to_subsystem: dict[str, str],
    truth_parameter_to_module: dict[str, str],
) -> dict[str, Any]:
    if telemetry_selected_hits.empty or "parameter_name" not in telemetry_selected_hits.columns:
        return {}

    ranked = telemetry_selected_hits.copy()
    rank_series = (
        pd.to_numeric(ranked["parameter_support_rank_in_window"], errors="coerce")
        if "parameter_support_rank_in_window" in ranked.columns
        else pd.Series(pd.NA, index=ranked.index, dtype="float")
    )
    support_series = (
        pd.to_numeric(ranked["parameter_localization_support"], errors="coerce")
        if "parameter_localization_support" in ranked.columns
        else pd.Series(pd.NA, index=ranked.index, dtype="float")
    )
    ranked["_sort_rank"] = rank_series.fillna(float("inf"))
    ranked["_sort_support"] = support_series.fillna(float("-inf"))
    ranked = ranked.sort_values(
        ["_sort_rank", "_sort_support", "parameter_name"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    top = ranked.iloc[0]
    parameter_name = str(top.get("parameter_name") or "")
    support_value = top.get("parameter_localization_support")
    rank_value = top.get("parameter_support_rank_in_window")
    return {
        "top_ranked_selected_parameter_name": parameter_name or None,
        "top_ranked_selected_parameter_support": (
            None if pd.isna(support_value) else float(support_value)
        ),
        "top_ranked_selected_parameter_rank": (
            None if pd.isna(rank_value) else int(rank_value)
        ),
        "top_ranked_selected_parameter_truth_subsystem": (
            truth_parameter_to_subsystem.get(parameter_name) if parameter_name else None
        ),
        "top_ranked_selected_parameter_truth_module": (
            truth_parameter_to_module.get(parameter_name) if parameter_name else None
        ),
    }


def _classify_reconstruction_localization_failure(
    *,
    dominant_score_component: str,
    truth_parameter: str,
    truth_subsystem: str,
    truth_module: str,
    dominant_subsystem_match: bool,
    dominant_module_match: bool,
    telemetry_selected_truth_subsystem_present: bool,
    telemetry_selected_truth_module_present: bool,
    top_subsystem_candidate_present: bool,
    top_module_candidate_present: bool,
    top_ranked_selected_parameter_name: str | None,
    top_ranked_selected_parameter_truth_subsystem: str | None,
    top_ranked_selected_parameter_truth_module: str | None,
) -> str | None:
    if dominant_score_component != RECONSTRUCTION_ERROR_CHANNEL:
        return None
    if dominant_subsystem_match and (not truth_module or dominant_module_match):
        return None
    if not telemetry_selected_truth_subsystem_present:
        return "missing_truth_local_candidate"

    top_parameter_name = str(top_ranked_selected_parameter_name or "")
    top_truth_subsystem = str(top_ranked_selected_parameter_truth_subsystem or "")
    top_truth_module = str(top_ranked_selected_parameter_truth_module or "")

    if top_parameter_name == truth_parameter:
        if truth_module and not dominant_module_match:
            return "truth_module_present_but_lost"
        return "truth_subsystem_present_but_lost"
    if top_truth_subsystem and top_truth_subsystem != truth_subsystem:
        if top_subsystem_candidate_present:
            return "sibling_consequence_won"
        return "shared_source_won"
    if truth_module and telemetry_selected_truth_module_present:
        if top_truth_module and top_truth_module != truth_module:
            return "truth_module_present_but_lost"
        if not dominant_module_match or top_module_candidate_present:
            return "truth_module_present_but_lost"
    return "truth_subsystem_present_but_lost"


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
class CandidateCutDiagnostic:
    """Bounded evidence describing whether truth was lost at the structural candidate cut."""

    structural_candidate_cut_width: int
    diagnostic_status: str
    truth_parameter_support_rank: int | None
    truth_parameter_support: float | None
    support_margin_at_structural_cut: float | None
    minimum_candidate_breadth: int | None
    truth_parameter_selected_for_telemetry: bool | None
    truth_parameter_within_structural_candidate_cut: bool
    truth_parameter_detected_subsystem_id: str | None
    truth_parameter_detected_module_id: str | None
    truth_parameter_subsystem_cluster_mappable: bool | None
    truth_parameter_module_cluster_mappable: bool | None

    @classmethod
    def from_telemetry_hits(
        cls,
        *,
        telemetry_hits: pd.DataFrame,
        truth_parameter: str,
        subsystem_truth_map: DetectedLocalizationTruthMap,
        module_truth_map: DetectedLocalizationTruthMap,
    ) -> "CandidateCutDiagnostic":
        cut_width = ANOMALY_LOCALIZATION_PARAMETER_TOP_K
        if telemetry_hits.empty:
            return cls._unavailable(
                structural_candidate_cut_width=cut_width,
                diagnostic_status="no_qualifying_telemetry_attribution",
            )

        required_columns = {"win_id", "parameter_name", "parameter_support_rank_in_window"}
        if not required_columns.issubset(telemetry_hits.columns):
            return cls._unavailable(
                structural_candidate_cut_width=cut_width,
                diagnostic_status="candidate_rank_unavailable",
            )

        candidates = telemetry_hits.copy()
        candidates["_rank"] = pd.to_numeric(
            candidates["parameter_support_rank_in_window"], errors="coerce"
        )
        candidates["_support"] = (
            pd.to_numeric(candidates["parameter_localization_support"], errors="coerce")
            if "parameter_localization_support" in candidates.columns
            else pd.Series(pd.NA, index=candidates.index, dtype="Float64")
        )
        candidates["_win_id_sort"] = candidates["win_id"].astype(str)
        candidates = candidates.sort_values(
            ["_rank", "_support", "parameter_name"],
            ascending=[True, False, True],
            kind="mergesort",
        ).drop_duplicates(["win_id", "parameter_name"])
        truth_candidates = candidates[
            candidates["parameter_name"].fillna("").astype(str) == truth_parameter
        ]
        if truth_candidates.empty:
            return cls._unavailable(
                structural_candidate_cut_width=cut_width,
                diagnostic_status="truth_parameter_not_in_bounded_parameter_candidates",
            )

        truth_candidates = truth_candidates.sort_values(
            ["_rank", "_support", "_win_id_sort"],
            ascending=[True, False, True],
            kind="mergesort",
        )
        truth_candidate = truth_candidates.iloc[0]
        rank_value = truth_candidate["_rank"]
        support_value = truth_candidate["_support"]
        truth_parameter_selected_for_telemetry = _optional_bool(
            truth_candidate.get("parameter_localization_selected")
        )
        if pd.isna(rank_value):
            return cls(
                structural_candidate_cut_width=cut_width,
                diagnostic_status=(
                    "truth_parameter_not_ranked_in_bounded_candidates"
                    if truth_parameter_selected_for_telemetry is False
                    else "truth_parameter_rank_unavailable"
                ),
                truth_parameter_support_rank=None,
                truth_parameter_support=None if pd.isna(support_value) else float(support_value),
                support_margin_at_structural_cut=None,
                minimum_candidate_breadth=None,
                truth_parameter_selected_for_telemetry=truth_parameter_selected_for_telemetry,
                truth_parameter_within_structural_candidate_cut=False,
                truth_parameter_detected_subsystem_id=None,
                truth_parameter_detected_module_id=None,
                truth_parameter_subsystem_cluster_mappable=None,
                truth_parameter_module_cluster_mappable=None,
            )

        rank = int(rank_value)
        detected_subsystem_id = _optional_non_empty_string(
            truth_candidate.get("subsystem_id")
        )
        detected_module_id = _optional_non_empty_string(
            truth_candidate.get("module_id")
        )
        _subsystem_truth, subsystem_mappable = subsystem_truth_map.resolve(detected_subsystem_id or "")
        _module_truth, module_mappable = module_truth_map.resolve(detected_module_id or "")
        same_window = candidates[candidates["win_id"] == truth_candidate["win_id"]]
        cut_candidates = same_window[same_window["_rank"] == cut_width]
        cut_support = None if cut_candidates.empty else cut_candidates.iloc[0]["_support"]
        support_margin = (
            None
            if pd.isna(support_value) or pd.isna(cut_support)
            else float(float(support_value) - float(cut_support))
        )
        within_cut = rank <= cut_width
        return cls(
            structural_candidate_cut_width=cut_width,
            diagnostic_status=(
                "within_structural_candidate_cut"
                if within_cut
                else "below_structural_candidate_cut"
            ),
            truth_parameter_support_rank=rank,
            truth_parameter_support=None if pd.isna(support_value) else float(support_value),
            support_margin_at_structural_cut=support_margin,
            minimum_candidate_breadth=rank,
            truth_parameter_selected_for_telemetry=truth_parameter_selected_for_telemetry,
            truth_parameter_within_structural_candidate_cut=within_cut,
            truth_parameter_detected_subsystem_id=detected_subsystem_id,
            truth_parameter_detected_module_id=detected_module_id,
            truth_parameter_subsystem_cluster_mappable=(bool(subsystem_mappable) if detected_subsystem_id else None),
            truth_parameter_module_cluster_mappable=(bool(module_mappable) if detected_module_id else None),
        )

    @classmethod
    def _unavailable(
        cls,
        *,
        structural_candidate_cut_width: int,
        diagnostic_status: str,
    ) -> "CandidateCutDiagnostic":
        return cls(
            structural_candidate_cut_width=structural_candidate_cut_width,
            diagnostic_status=diagnostic_status,
            truth_parameter_support_rank=None,
            truth_parameter_support=None,
            support_margin_at_structural_cut=None,
            minimum_candidate_breadth=None,
            truth_parameter_selected_for_telemetry=None,
            truth_parameter_within_structural_candidate_cut=False,
            truth_parameter_detected_subsystem_id=None,
            truth_parameter_detected_module_id=None,
            truth_parameter_subsystem_cluster_mappable=None,
            truth_parameter_module_cluster_mappable=None,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "structural_candidate_cut_width": self.structural_candidate_cut_width,
            "diagnostic_status": self.diagnostic_status,
            "truth_parameter_support_rank": self.truth_parameter_support_rank,
            "truth_parameter_support": self.truth_parameter_support,
            "support_margin_at_structural_cut": self.support_margin_at_structural_cut,
            "minimum_candidate_breadth": self.minimum_candidate_breadth,
            "truth_parameter_selected_for_telemetry": self.truth_parameter_selected_for_telemetry,
            "truth_parameter_within_structural_candidate_cut": self.truth_parameter_within_structural_candidate_cut,
            "truth_parameter_detected_subsystem_id": self.truth_parameter_detected_subsystem_id,
            "truth_parameter_detected_module_id": self.truth_parameter_detected_module_id,
            "truth_parameter_subsystem_cluster_mappable": self.truth_parameter_subsystem_cluster_mappable,
            "truth_parameter_module_cluster_mappable": self.truth_parameter_module_cluster_mappable,
        }


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
    candidate_cut_diagnostic: CandidateCutDiagnostic
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
        top_ranked_selected_parameter = _top_ranked_selected_parameter_details(
            telemetry_selected_hits,
            truth_parameter_to_subsystem=truth_parameter_to_subsystem,
            truth_parameter_to_module=truth_parameter_to_module,
        )
        top_ranked_selected_parameter_name = str(
            top_ranked_selected_parameter.get("top_ranked_selected_parameter_name") or ""
        )
        top_ranked_selected_parameter_truth_subsystem = str(
            top_ranked_selected_parameter.get("top_ranked_selected_parameter_truth_subsystem") or ""
        )
        top_ranked_selected_parameter_truth_module = str(
            top_ranked_selected_parameter.get("top_ranked_selected_parameter_truth_module") or ""
        )
        top_ranked_selected_parameter_exact_match = bool(top_ranked_selected_parameter_name == truth_parameter)
        top_ranked_selected_parameter_in_truth_subsystem = bool(
            top_ranked_selected_parameter_truth_subsystem
            and top_ranked_selected_parameter_truth_subsystem == truth_subsystem
        )
        top_ranked_selected_parameter_in_truth_module = bool(
            truth_module
            and top_ranked_selected_parameter_truth_module
            and top_ranked_selected_parameter_truth_module == truth_module
        )
        candidate_cut_diagnostic = CandidateCutDiagnostic.from_telemetry_hits(
            telemetry_hits=telemetry_hits,
            truth_parameter=truth_parameter,
            subsystem_truth_map=subsystem_truth_map,
            module_truth_map=module_truth_map,
        )
        reconstruction_failure_bucket = _classify_reconstruction_localization_failure(
            dominant_score_component=dominant_score_component,
            truth_parameter=truth_parameter,
            truth_subsystem=truth_subsystem,
            truth_module=truth_module,
            dominant_subsystem_match=dominant_subsystem_match,
            dominant_module_match=dominant_module_match,
            telemetry_selected_truth_subsystem_present=bool(truth_subsystem in telemetry_selected_truth_subsystems),
            telemetry_selected_truth_module_present=bool(truth_module and truth_module in telemetry_selected_truth_modules),
            top_subsystem_candidate_present=top_subsystem_candidate_present,
            top_module_candidate_present=top_module_candidate_present,
            top_ranked_selected_parameter_name=top_ranked_selected_parameter_name,
            top_ranked_selected_parameter_truth_subsystem=top_ranked_selected_parameter_truth_subsystem,
            top_ranked_selected_parameter_truth_module=top_ranked_selected_parameter_truth_module,
        )

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
            "top_ranked_selected_parameter_name": (
                top_ranked_selected_parameter.get("top_ranked_selected_parameter_name")
            ),
            "top_ranked_selected_parameter_support": (
                top_ranked_selected_parameter.get("top_ranked_selected_parameter_support")
            ),
            "top_ranked_selected_parameter_rank": (
                top_ranked_selected_parameter.get("top_ranked_selected_parameter_rank")
            ),
            "top_ranked_selected_parameter_truth_subsystem": (
                top_ranked_selected_parameter.get("top_ranked_selected_parameter_truth_subsystem")
            ),
            "top_ranked_selected_parameter_truth_module": (
                top_ranked_selected_parameter.get("top_ranked_selected_parameter_truth_module")
            ),
            "top_ranked_selected_parameter_exact_match": top_ranked_selected_parameter_exact_match,
            "top_ranked_selected_parameter_in_truth_subsystem": (
                top_ranked_selected_parameter_in_truth_subsystem
            ),
            "top_ranked_selected_parameter_in_truth_module": (
                top_ranked_selected_parameter_in_truth_module
            ),
            "reconstruction_failure_bucket": reconstruction_failure_bucket,
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
            candidate_cut_diagnostic=candidate_cut_diagnostic,
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
        "top_ranked_selected_parameter_name",
        "top_ranked_selected_parameter_rank",
        "top_ranked_selected_parameter_support",
        "top_ranked_selected_parameter_truth_subsystem",
        "top_ranked_selected_parameter_truth_module",
        "top_ranked_selected_parameter_exact_match",
        "top_ranked_selected_parameter_in_truth_subsystem",
        "top_ranked_selected_parameter_in_truth_module",
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
        "reconstruction_failure_bucket",
        "top_ranked_selected_parameter_name",
        "top_ranked_selected_parameter_rank",
        "top_ranked_selected_parameter_truth_subsystem",
        "top_ranked_selected_parameter_truth_module",
        "top_ranked_selected_parameter_in_truth_subsystem",
        "top_ranked_selected_parameter_in_truth_module",
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


def _build_reconstruction_localization_validation(per_truth_df: pd.DataFrame) -> dict[str, Any]:
    if per_truth_df.empty or "dominant_score_component" not in per_truth_df.columns:
        return _empty_reconstruction_localization_validation()

    reconstruction_df = per_truth_df[
        per_truth_df["dominant_score_component"].fillna("").astype(str) == RECONSTRUCTION_ERROR_CHANNEL
    ].copy()
    if reconstruction_df.empty:
        return _empty_reconstruction_localization_validation()

    truth_subsystem_present_selected = reconstruction_df["telemetry_selected_truth_subsystem_present"].fillna(False).astype(bool)
    truth_module_present_selected = reconstruction_df["telemetry_selected_truth_module_present"].fillna(False).astype(bool)
    top_subsystem_candidate_present = reconstruction_df["top_subsystem_candidate_present"].fillna(False).astype(bool)
    top_module_candidate_present = reconstruction_df["top_module_candidate_present"].fillna(False).astype(bool)
    top_ranked_parameter_match = reconstruction_df["top_ranked_selected_parameter_exact_match"].fillna(False).astype(bool)
    top_ranked_in_truth_subsystem = reconstruction_df["top_ranked_selected_parameter_in_truth_subsystem"].fillna(False).astype(bool)
    top_ranked_in_truth_module = reconstruction_df["top_ranked_selected_parameter_in_truth_module"].fillna(False).astype(bool)
    failure_mask = reconstruction_df["reconstruction_failure_bucket"].fillna("").astype(str) != ""
    failure_counts = (
        reconstruction_df.loc[failure_mask, "reconstruction_failure_bucket"]
        .fillna("")
        .astype(str)
        .value_counts()
        .sort_index()
        .to_dict()
    )
    failure_counts = {str(bucket): int(count) for bucket, count in failure_counts.items() if str(bucket)}
    denominator = int(len(reconstruction_df))
    failure_rates = {
        bucket: float(count / denominator)
        for bucket, count in failure_counts.items()
    }
    case_columns = [
        "tail_id",
        "flight_id",
        "fault_window_id",
        "misbehavior_window_id",
        "subsystem_id",
        "module_id",
        "parameter_name",
        "dominant_score_component",
        "reconstruction_failure_bucket",
        "dominant_subsystem_match",
        "dominant_module_match",
        "telemetry_selected_truth_subsystem_present",
        "telemetry_selected_truth_module_present",
        "top_subsystem_candidate_present",
        "top_module_candidate_present",
        "top_ranked_selected_parameter_name",
        "top_ranked_selected_parameter_rank",
        "top_ranked_selected_parameter_support",
        "top_ranked_selected_parameter_truth_subsystem",
        "top_ranked_selected_parameter_truth_module",
        "top_ranked_selected_parameter_exact_match",
        "top_ranked_selected_parameter_in_truth_subsystem",
        "top_ranked_selected_parameter_in_truth_module",
        "telemetry_selected_attributed_parameter_names",
        "top_subsystem_candidate_truth_ids",
        "top_module_candidate_truth_ids",
    ]
    available_case_columns = [column for column in case_columns if column in reconstruction_df.columns]
    cases = _records_with_none_for_missing(
        reconstruction_df[available_case_columns].sort_values(
            [column for column in ("reconstruction_failure_bucket", "fault_window_id", "misbehavior_window_id", "parameter_name") if column in available_case_columns],
            kind="mergesort",
        )
    )
    return {
        "status": "ok",
        "truth_window_count": int(len(per_truth_df)),
        "reconstruction_truth_window_count": denominator,
        "reconstruction_failure_count": int(failure_mask.sum()),
        "failure_count_by_bucket": failure_counts,
        "failure_rate_by_bucket": failure_rates,
        "truth_subsystem_present_in_selected_telemetry_count": int(truth_subsystem_present_selected.sum()),
        "truth_subsystem_present_in_selected_telemetry_rate": float(truth_subsystem_present_selected.mean()),
        "truth_module_present_in_selected_telemetry_count": int(truth_module_present_selected.sum()),
        "truth_module_present_in_selected_telemetry_rate": float(truth_module_present_selected.mean()),
        "truth_subsystem_present_in_top_subsystem_candidates_count": int(top_subsystem_candidate_present.sum()),
        "truth_subsystem_present_in_top_subsystem_candidates_rate": float(top_subsystem_candidate_present.mean()),
        "truth_module_present_in_top_module_candidates_count": int(top_module_candidate_present.sum()),
        "truth_module_present_in_top_module_candidates_rate": float(top_module_candidate_present.mean()),
        "top_ranked_selected_parameter_exact_match_count": int(top_ranked_parameter_match.sum()),
        "top_ranked_selected_parameter_exact_match_rate": float(top_ranked_parameter_match.mean()),
        "top_ranked_selected_parameter_in_truth_subsystem_count": int(top_ranked_in_truth_subsystem.sum()),
        "top_ranked_selected_parameter_in_truth_subsystem_rate": float(top_ranked_in_truth_subsystem.mean()),
        "top_ranked_selected_parameter_in_truth_module_count": int(top_ranked_in_truth_module.sum()),
        "top_ranked_selected_parameter_in_truth_module_rate": float(top_ranked_in_truth_module.mean()),
        "reconstruction_localization_cases": cases,
    }


def _build_candidate_cut_validation(
    matches: list[_TruthWindowAttributionMatch],
) -> dict[str, Any]:
    if not matches:
        return _empty_candidate_cut_validation()

    diagnostics = [match.candidate_cut_diagnostic for match in matches]
    status_count: dict[str, int] = {}
    for diagnostic in diagnostics:
        status_count[diagnostic.diagnostic_status] = (
            status_count.get(diagnostic.diagnostic_status, 0) + 1
        )

    truth_window_count = len(diagnostics)
    ranked_truth_parameter_count = sum(
        diagnostic.truth_parameter_support_rank is not None
        for diagnostic in diagnostics
    )
    truth_parameter_selected_count = sum(
        diagnostic.truth_parameter_selected_for_telemetry is True
        for diagnostic in diagnostics
    )
    within_cut_count = sum(
        diagnostic.truth_parameter_within_structural_candidate_cut
        for diagnostic in diagnostics
    )
    below_cut_count = sum(
        diagnostic.diagnostic_status == "below_structural_candidate_cut"
        for diagnostic in diagnostics
    )
    case_identity_fields = (
        "tail_id",
        "flight_id",
        "fault_window_id",
        "misbehavior_window_id",
        "subsystem_id",
        "module_id",
        "parameter_name",
        "dominant_score_component",
        "primary_win_id",
        "matched_attribution_window_count",
    )
    cases = [
        {
            **{
                field: match.payload.get(field)
                for field in case_identity_fields
            },
            **match.candidate_cut_diagnostic.to_payload(),
        }
        for match in matches
    ]
    cases.sort(
        key=lambda case: tuple(
            str(case.get(field) or "")
            for field in ("fault_window_id", "misbehavior_window_id", "parameter_name")
        )
    )
    return {
        "status": "ok",
        "structural_candidate_cut_width": ANOMALY_LOCALIZATION_PARAMETER_TOP_K,
        "truth_window_count": truth_window_count,
        "diagnostic_status_count": dict(sorted(status_count.items())),
        "ranked_truth_parameter_count": ranked_truth_parameter_count,
        "truth_parameter_selected_for_telemetry_count": truth_parameter_selected_count,
        "truth_parameter_selected_for_telemetry_rate": float(
            truth_parameter_selected_count / truth_window_count
        ),
        "truth_parameter_within_structural_candidate_cut_count": within_cut_count,
        "truth_parameter_within_structural_candidate_cut_rate": float(
            within_cut_count / truth_window_count
        ),
        "truth_parameter_below_structural_candidate_cut_count": below_cut_count,
        "truth_parameter_below_structural_candidate_cut_rate": float(
            below_cut_count / truth_window_count
        ),
        "candidate_cut_cases": cases,
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
        "reconstruction_localization_validation": summary.get(
            "reconstruction_localization_validation",
            _empty_reconstruction_localization_validation(),
        ),
        "parameter_localization_validation": summary.get(
            "parameter_localization_validation",
            _empty_parameter_localization_validation(),
        ),
        "candidate_cut_validation": summary.get(
            "candidate_cut_validation",
            _empty_candidate_cut_validation(),
        ),
        "hierarchy_cluster_alignment_validation": summary.get(
            "hierarchy_cluster_alignment_validation",
            _empty_hierarchy_cluster_alignment_validation(),
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
            "reconstruction_localization_validation": _empty_reconstruction_localization_validation(),
            "parameter_localization_validation": _empty_parameter_localization_validation(),
            "candidate_cut_validation": _empty_candidate_cut_validation(),
            "hierarchy_cluster_alignment_validation": _empty_hierarchy_cluster_alignment_validation(),
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
    reconstruction_localization_validation = _build_reconstruction_localization_validation(per_truth_df)
    parameter_localization_validation = _build_parameter_localization_validation(per_truth_df)
    candidate_cut_validation = _build_candidate_cut_validation(matches)
    hierarchy_cluster_alignment_validation = _build_hierarchy_cluster_alignment_validation(
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
    )
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
        "reconstruction_localization_validation": reconstruction_localization_validation,
        "parameter_localization_validation": parameter_localization_validation,
        "candidate_cut_validation": candidate_cut_validation,
        "hierarchy_cluster_alignment_validation": hierarchy_cluster_alignment_validation,
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
