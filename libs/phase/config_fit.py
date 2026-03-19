from __future__ import annotations

import logging
from typing import Any

from libs.perf.logger import get_logger
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.selectors import (
    select_categorical_state_pairs_with_diagnostics_from_window_features_spark,
    select_event_types_with_diagnostics_from_window_features_spark,
)
from libs.phase.types import (
    PhaseFeatureSelectionDiagnostics,
    PhaseFeatureSelectionPolicy,
    PhaseSelectorDiagnostics,
)

LOGGER = get_logger("libs.phase.config_fit")


def fit_phase_feature_config_from_window_features_spark(
    window_features_df: "DataFrame",
    *,
    backbone_row: dict[str, Any],
    selection_policy: PhaseFeatureSelectionPolicy,
) -> PhaseFeatureConfig:
    return fit_phase_feature_config_with_diagnostics_from_window_features_spark(
        window_features_df,
        backbone_row=backbone_row,
        selection_policy=selection_policy,
    )[0]


def fit_phase_feature_config_with_diagnostics_from_window_features_spark(
    window_features_df: "DataFrame",
    *,
    backbone_row: dict[str, Any],
    selection_policy: PhaseFeatureSelectionPolicy,
    logger: logging.Logger | None = None,
) -> tuple[PhaseFeatureConfig, PhaseFeatureSelectionDiagnostics]:
    active_logger = logger or LOGGER
    selected_sensors_c = [str(item) for item in backbone_row.get("selected_sensors_c", []) if str(item)]
    selected_sensors = selected_sensors_c[: max(int(selection_policy.sensor_count), 1)]
    sensors_diagnostics = PhaseSelectorDiagnostics(
        selector_name="backbone_sensors",
        selected_count=len(selected_sensors),
        timing_ms=0.0,
        candidate_count=len(selected_sensors_c),
        fallback_used=False,
    )
    selected_event_types, event_type_diagnostics = select_event_types_with_diagnostics_from_window_features_spark(
        window_features_df,
        k=max(int(selection_policy.event_type_count), 0),
    )
    selected_categorical_state_pairs, categorical_state_pair_diagnostics = (
        select_categorical_state_pairs_with_diagnostics_from_window_features_spark(
            window_features_df,
            k=max(int(selection_policy.categorical_state_count), 0),
        )
    )
    config = PhaseFeatureConfig.from_backbone_row(
        backbone_row,
        phase_selected_sensors=selected_sensors,
        phase_selected_event_types=selected_event_types,
        phase_selected_categorical_state_pairs=selected_categorical_state_pairs,
    )
    diagnostics = PhaseFeatureSelectionDiagnostics(
        sensors=sensors_diagnostics,
        event_types=event_type_diagnostics,
        categorical_state_pairs=categorical_state_pair_diagnostics,
        selected_event_types=selected_event_types,
        selected_categorical_state_pairs=selected_categorical_state_pairs,
    )
    active_logger.info("phase_feature_selection diagnostics=%s", diagnostics.to_dict())
    return config, diagnostics


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
