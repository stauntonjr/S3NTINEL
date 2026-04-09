"""Phase feature and observation frame builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.common import empty_array, empty_map
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.utils import double_matrix_literal, string_array_literal
from libs.pyspark import Frame


@dataclass(frozen=True)
class PhaseFeatureFrame(Frame):
    _SIGN_EPSILON = 1e-6

    feature_names: list[str]

    @staticmethod
    def _continuous_count_columns() -> list["Column"]:
        from pyspark.sql import functions as F

        return [
            F.coalesce(F.element_at("event_type_counts", F.lit(event_type)).cast("double"), F.lit(0.0))
            for event_type in sorted(CONTINUOUS_EVENT_TYPES)
        ]

    @staticmethod
    def _categorical_count_columns() -> list["Column"]:
        from pyspark.sql import functions as F

        return [
            F.coalesce(F.element_at("event_type_counts", F.lit(event_type)).cast("double"), F.lit(0.0))
            for event_type in sorted(CATEGORICAL_EVENT_TYPES)
        ]

    @staticmethod
    def _map_value_sum(column_name: str, *, value_type: str = "double") -> "Column":
        from pyspark.sql import functions as F

        return F.aggregate(
            F.map_values(F.coalesce(F.col(column_name), empty_map("string", value_type))),
            F.lit(0.0),
            lambda acc, value: acc + value.cast("double"),
        )

    @classmethod
    def _map_value_max(cls, column_name: str, *, value_type: str = "double") -> "Column":
        from pyspark.sql import functions as F

        return F.aggregate(
            F.map_values(F.coalesce(F.col(column_name), empty_map("string", value_type))),
            F.lit(0.0),
            lambda acc, value: F.greatest(acc, value.cast("double")),
        )

    @staticmethod
    def _selected_sensor_value_column(column_name: str, parameter_name: str) -> "Column":
        from pyspark.sql import functions as F

        return F.coalesce(F.element_at(F.col(column_name), F.lit(parameter_name)), F.lit(0.0)).cast("double")

    @staticmethod
    def _duration_seconds_column() -> "Column":
        from pyspark.sql import functions as F

        return F.greatest(F.col("duration_ms").cast("double") / F.lit(1000.0), F.lit(1e-6))

    @staticmethod
    def _event_count_column() -> "Column":
        from pyspark.sql import functions as F

        return F.greatest(F.col("event_count").cast("double"), F.lit(1.0))

    @classmethod
    def _delta_abs_sum_column(cls, delta_array: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.aggregate(
            F.transform(delta_array, lambda value: F.abs(value)),
            F.lit(0.0),
            lambda acc, value: acc + value,
        )

    @classmethod
    def _delta_signed_sum_column(cls, delta_array: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.aggregate(delta_array, F.lit(0.0), lambda acc, value: acc + value)

    @classmethod
    def _delta_energy_column(cls, delta_array: "Column", *, delta_count: int) -> "Column":
        from pyspark.sql import functions as F

        return F.sqrt(
            F.aggregate(
                F.transform(delta_array, lambda value: value * value),
                F.lit(0.0),
                lambda acc, value: acc + value,
            )
        ) / F.lit(float(max(delta_count, 1)))

    @classmethod
    def _delta_directionality_column(cls, delta_array: "Column") -> "Column":
        from pyspark.sql import functions as F

        return (
            F.abs(cls._delta_signed_sum_column(delta_array))
            / F.greatest(cls._delta_abs_sum_column(delta_array), F.lit(cls._SIGN_EPSILON))
        ).cast("double")

    @classmethod
    def _changed_sensor_fraction_column(cls, delta_array: "Column", *, delta_count: int) -> "Column":
        from pyspark.sql import functions as F

        changed_sensor_count = F.aggregate(
            F.transform(
                delta_array,
                lambda value: F.when(F.abs(value) > F.lit(cls._SIGN_EPSILON), F.lit(1.0)).otherwise(F.lit(0.0)),
            ),
            F.lit(0.0),
            lambda acc, value: acc + value,
        )
        return (changed_sensor_count / F.lit(float(max(delta_count, 1)))).cast("double")

    @staticmethod
    def _average_columns(columns: list["Column"]) -> "Column":
        from pyspark.sql import functions as F

        if not columns:
            return F.lit(0.0)
        return (
            F.aggregate(
                F.array(*[column.cast("double") for column in columns]),
                F.lit(0.0),
                lambda acc, value: acc + value,
            )
            / F.lit(float(len(columns)))
        ).cast("double")

    @staticmethod
    def _event_rate_column(event_type: str) -> "Column":
        from pyspark.sql import functions as F

        return (
            F.coalesce(F.element_at("event_type_counts", F.lit(event_type)).cast("double"), F.lit(0.0))
            / PhaseFeatureFrame._event_count_column()
        ).cast("double")

    @staticmethod
    def _categorical_state_indicator_column(column_name: str, parameter_name: str, state: str) -> "Column":
        from pyspark.sql import functions as F

        return F.when(
            F.coalesce(F.element_at(column_name, F.lit(parameter_name)), F.lit("")) == F.lit(state),
            F.lit(1.0),
        ).otherwise(F.lit(0.0))

    @classmethod
    def _categorical_changed_indicator_column(cls, parameter_name: str, state: str) -> "Column":
        from pyspark.sql import functions as F

        state_start = F.coalesce(F.element_at("categorical_state_t_start", F.lit(parameter_name)), F.lit(""))
        state_end = F.coalesce(F.element_at("categorical_state_t_end", F.lit(parameter_name)), F.lit(""))
        return F.when(
            (state_start != state_end) & ((state_start == F.lit(state)) | (state_end == F.lit(state))),
            F.lit(1.0),
        ).otherwise(F.lit(0.0))

    @staticmethod
    def _active_sensor_fraction_column(*, active_sensor_denominator: int) -> "Column":
        from pyspark.sql import functions as F

        return (
            F.size(F.map_keys("continuous_vector_t_end_scaled")).cast("double")
            / F.lit(float(max(active_sensor_denominator, 1)))
        ).cast("double")

    @classmethod
    def _drift_rate_column(cls) -> "Column":
        from pyspark.sql import functions as F

        return (
            F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0)).cast("double")
            / cls._duration_seconds_column()
        ).cast("double")

    @classmethod
    def _reconstruction_error_per_sensor_column(cls) -> "Column":
        from pyspark.sql import functions as F

        return (
            F.coalesce(F.col("backbone_reconstruction_error"), F.lit(0.0)).cast("double")
            / F.greatest(F.size(F.map_keys("continuous_vector_t_end_scaled")).cast("double"), F.lit(1.0))
        ).cast("double")

    @classmethod
    def _slope_directionality_column(cls) -> "Column":
        from pyspark.sql import functions as F

        total_signed_impulse = cls._map_value_sum(
            "continuous_event_summary.slope_signed_impulse_by_parameter",
            value_type="double",
        )
        total_abs_impulse = cls._map_value_sum(
            "continuous_event_summary.slope_abs_impulse_by_parameter",
            value_type="double",
        )
        return (F.abs(total_signed_impulse) / F.greatest(total_abs_impulse, F.lit(cls._SIGN_EPSILON))).cast("double")

    @classmethod
    def _selected_sensor_delta_columns(cls, selected_sensors: list[str]) -> list["Column"]:
        return [
            (
                cls._selected_sensor_value_column("continuous_vector_t_end_scaled", parameter_name)
                - cls._selected_sensor_value_column("continuous_vector_t_start_scaled", parameter_name)
            ).cast("double")
            for parameter_name in selected_sensors
        ]

    @classmethod
    def _selected_sensor_delta_array(cls, selected_sensors: list[str]) -> "Column":
        from pyspark.sql import functions as F

        delta_columns = cls._selected_sensor_delta_columns(selected_sensors)
        return F.array(*delta_columns) if delta_columns else empty_array("double")

    @classmethod
    def _level_feature_array(cls, selected_sensors: list[str]) -> "Column":
        from pyspark.sql import functions as F

        level_columns = [
            cls._selected_sensor_value_column("continuous_vector_t_end_scaled", parameter_name)
            for parameter_name in selected_sensors
        ]
        return F.array(*level_columns) if level_columns else empty_array("double")

    @classmethod
    def _delta_feature_array(cls, selected_sensors: list[str]) -> "Column":
        from pyspark.sql import functions as F

        delta_array = cls._selected_sensor_delta_array(selected_sensors)
        delta_count = len(selected_sensors)
        return F.concat(
            delta_array,
            F.array(
                cls._delta_energy_column(delta_array, delta_count=delta_count).cast("double"),
                cls._delta_directionality_column(delta_array).cast("double"),
                cls._changed_sensor_fraction_column(delta_array, delta_count=delta_count).cast("double"),
            ),
        )

    @classmethod
    def _event_feature_array(cls, selected_event_types: list[str]) -> "Column":
        from pyspark.sql import functions as F

        event_columns = [cls._event_rate_column(event_type) for event_type in selected_event_types]
        return F.array(*event_columns) if event_columns else empty_array("double")

    @classmethod
    def _categorical_feature_array(cls, selected_pairs: list[tuple[str, str]]) -> "Column":
        from pyspark.sql import functions as F

        start_columns = [
            cls._categorical_state_indicator_column("categorical_state_t_start", parameter_name, state)
            for parameter_name, state in selected_pairs
        ]
        end_columns = [
            cls._categorical_state_indicator_column("categorical_state_t_end", parameter_name, state)
            for parameter_name, state in selected_pairs
        ]
        changed_columns = [
            cls._categorical_changed_indicator_column(parameter_name, state)
            for parameter_name, state in selected_pairs
        ]
        values = start_columns + end_columns + changed_columns
        return F.array(*values) if values else empty_array("double")

    @classmethod
    def _summary_feature_array(
        cls,
        *,
        active_sensor_denominator: int,
    ) -> "Column":
        from pyspark.sql import functions as F

        event_count = cls._event_count_column()
        total_peak_impulse = cls._map_value_sum(
            "continuous_event_summary.slope_peak_abs_delta_by_parameter",
            value_type="double",
        )
        max_peak_impulse = cls._map_value_max(
            "continuous_event_summary.slope_peak_abs_delta_by_parameter",
            value_type="double",
        )
        total_slope_runs = cls._map_value_sum(
            "continuous_event_summary.slope_run_count_by_parameter",
            value_type="int",
        )
        total_slope_reinforcements = cls._map_value_sum(
            "continuous_event_summary.slope_reinforcement_count_by_parameter",
            value_type="int",
        )
        total_signed_impulse = cls._map_value_sum(
            "continuous_event_summary.slope_signed_impulse_by_parameter",
            value_type="double",
        )
        total_abs_impulse = cls._map_value_sum(
            "continuous_event_summary.slope_abs_impulse_by_parameter",
            value_type="double",
        )
        switch_count = cls._map_value_sum(
            "continuous_event_summary.switch_count_by_parameter",
            value_type="int",
        )
        threshold_count = cls._map_value_sum(
            "continuous_event_summary.threshold_count_by_parameter",
            value_type="int",
        )
        oscillation_count = cls._map_value_sum(
            "continuous_event_summary.oscillation_count_by_parameter",
            value_type="int",
        )
        drift_guard_count = cls._map_value_sum(
            "continuous_event_summary.drift_guard_count_by_parameter",
            value_type="int",
        )
        continuous_event_count_col = sum(cls._continuous_count_columns(), F.lit(0.0))
        categorical_event_count_col = sum(cls._categorical_count_columns(), F.lit(0.0))
        active_sensor_fraction = cls._active_sensor_fraction_column(active_sensor_denominator=active_sensor_denominator)
        categorical_keys = F.array_union(
            F.map_keys(F.coalesce(F.col("categorical_state_t_start"), empty_map())),
            F.map_keys(F.coalesce(F.col("categorical_state_t_end"), empty_map())),
        )
        categorical_change_count = F.aggregate(
            categorical_keys,
            F.lit(0.0),
            lambda acc, key: acc
            + F.when(
                F.coalesce(F.element_at("categorical_state_t_start", key), F.lit(""))
                != F.coalesce(F.element_at("categorical_state_t_end", key), F.lit("")),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        categorical_key_count = F.greatest(F.size(categorical_keys).cast("double"), F.lit(1.0))
        return F.array(
            (F.col("event_count").cast("double") / cls._duration_seconds_column()).cast("double"),
            (continuous_event_count_col / event_count).cast("double"),
            (categorical_event_count_col / event_count).cast("double"),
            active_sensor_fraction.cast("double"),
            cls._drift_rate_column().cast("double"),
            cls._reconstruction_error_per_sensor_column().cast("double"),
            (total_slope_reinforcements / F.greatest(total_slope_runs, F.lit(1.0))).cast("double"),
            cls._slope_directionality_column().cast("double"),
            (max_peak_impulse / F.greatest(total_peak_impulse, F.lit(1e-6))).cast("double"),
            (switch_count / event_count).cast("double"),
            (threshold_count / event_count).cast("double"),
            (oscillation_count / event_count).cast("double"),
            (drift_guard_count / event_count).cast("double"),
            categorical_change_count.cast("double"),
            (categorical_change_count / categorical_key_count).cast("double"),
        )

    @classmethod
    def _temporal_feature_array(
        cls,
        *,
        phase_selected_sensors: list[str],
        phase_selected_event_types: list[str],
        phase_selected_categorical_state_pairs: list[tuple[str, str]],
        active_sensor_denominator: int,
        temporal_history_scales: tuple[int, ...],
    ) -> "Column":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        order_window = Window.partitionBy("tail_id", "flight_id").orderBy(
            F.col("t_end").asc_nulls_last(),
            F.col("win_id").asc(),
        )
        sensor_delta_columns = {
            parameter_name: (
                cls._selected_sensor_value_column("continuous_vector_t_end_scaled", parameter_name)
                - cls._selected_sensor_value_column("continuous_vector_t_start_scaled", parameter_name)
            ).cast("double")
            for parameter_name in phase_selected_sensors
        }
        delta_array = F.array(*sensor_delta_columns.values()) if sensor_delta_columns else empty_array("double")
        delta_abs_mean = cls._average_columns([F.abs(column) for column in sensor_delta_columns.values()])
        delta_energy = cls._delta_energy_column(delta_array, delta_count=len(phase_selected_sensors))
        delta_directionality = cls._delta_directionality_column(delta_array)
        event_rate_columns = {
            event_type: cls._event_rate_column(event_type)
            for event_type in phase_selected_event_types
        }
        categorical_end_columns = {
            (parameter_name, state): cls._categorical_state_indicator_column(
                "categorical_state_t_end",
                parameter_name,
                state,
            )
            for parameter_name, state in phase_selected_categorical_state_pairs
        }
        categorical_changed_columns = {
            (parameter_name, state): cls._categorical_changed_indicator_column(parameter_name, state)
            for parameter_name, state in phase_selected_categorical_state_pairs
        }
        categorical_change_fraction = cls._average_columns(list(categorical_changed_columns.values()))
        drift_rate = cls._drift_rate_column()
        reconstruction_error_per_sensor = cls._reconstruction_error_per_sensor_column()
        active_sensor_fraction = cls._active_sensor_fraction_column(active_sensor_denominator=active_sensor_denominator)
        slope_directionality = cls._slope_directionality_column()

        sensor_features: list["Column"] = []
        event_features: list["Column"] = []
        categorical_features: list["Column"] = []
        summary_features: list["Column"] = []
        summary_features_by_scale: dict[int, dict[str, "Column"]] = {}

        for scale in temporal_history_scales:
            history_window = order_window.rowsBetween(-int(scale), -1)
            history_count = F.count(F.lit(1)).over(history_window).cast("double")
            coverage = (history_count / F.lit(float(scale))).cast("double")
            trailing_sensor_means = {
                parameter_name: F.coalesce(F.avg(column).over(history_window), F.lit(0.0)).cast("double")
                for parameter_name, column in sensor_delta_columns.items()
            }
            trailing_event_rates = {
                event_type: F.coalesce(F.avg(column).over(history_window), F.lit(0.0)).cast("double")
                for event_type, column in event_rate_columns.items()
            }
            trailing_categorical_dwell = {
                pair: F.coalesce(F.avg(column).over(history_window), F.lit(0.0)).cast("double")
                for pair, column in categorical_end_columns.items()
            }

            sensor_features.extend(trailing_sensor_means.values())
            event_features.extend(trailing_event_rates.values())
            categorical_features.extend(trailing_categorical_dwell.values())

            continuation_columns = [
                F.when(
                    (F.abs(current_column) > F.lit(cls._SIGN_EPSILON))
                    & (F.abs(trailing_sensor_means[parameter_name]) > F.lit(cls._SIGN_EPSILON))
                    & ((current_column * trailing_sensor_means[parameter_name]) > F.lit(0.0)),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
                for parameter_name, current_column in sensor_delta_columns.items()
            ]
            reversal_columns = [
                F.when(
                    (F.abs(current_column) > F.lit(cls._SIGN_EPSILON))
                    & (F.abs(trailing_sensor_means[parameter_name]) > F.lit(cls._SIGN_EPSILON))
                    & ((current_column * trailing_sensor_means[parameter_name]) < F.lit(0.0)),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
                for parameter_name, current_column in sensor_delta_columns.items()
            ]
            event_shift = cls._average_columns(
                [
                    F.abs(current_column - trailing_event_rates[event_type]).cast("double")
                    for event_type, current_column in event_rate_columns.items()
                ]
            )
            summary_columns = {
                "history_coverage": coverage,
                "delta_abs_mean": F.coalesce(F.avg(delta_abs_mean).over(history_window), F.lit(0.0)).cast("double"),
                "delta_energy_mean": F.coalesce(F.avg(delta_energy).over(history_window), F.lit(0.0)).cast("double"),
                "delta_directionality_mean": F.coalesce(F.avg(delta_directionality).over(history_window), F.lit(0.0)).cast("double"),
                "delta_continuation_fraction": cls._average_columns(continuation_columns),
                "delta_reversal_fraction": cls._average_columns(reversal_columns),
                "event_shift": event_shift,
                "categorical_transition_rate": F.coalesce(F.avg(categorical_change_fraction).over(history_window), F.lit(0.0)).cast("double"),
                "drift_rate_mean": F.coalesce(F.avg(drift_rate).over(history_window), F.lit(0.0)).cast("double"),
                "reconstruction_error_mean": F.coalesce(F.avg(reconstruction_error_per_sensor).over(history_window), F.lit(0.0)).cast("double"),
                "active_sensor_fraction_mean": F.coalesce(F.avg(active_sensor_fraction).over(history_window), F.lit(0.0)).cast("double"),
                "slope_directionality_mean": F.coalesce(F.avg(slope_directionality).over(history_window), F.lit(0.0)).cast("double"),
            }
            summary_features_by_scale[int(scale)] = summary_columns
            summary_features.extend(summary_columns.values())

        if len(temporal_history_scales) >= 2:
            short_scale = int(temporal_history_scales[0])
            long_scale = int(temporal_history_scales[-1])
            short_summary = summary_features_by_scale[short_scale]
            long_summary = summary_features_by_scale[long_scale]
            summary_features.extend(
                [
                    (short_summary["delta_energy_mean"] - long_summary["delta_energy_mean"]).cast("double"),
                    (short_summary["drift_rate_mean"] - long_summary["drift_rate_mean"]).cast("double"),
                    (short_summary["event_shift"] - long_summary["event_shift"]).cast("double"),
                ]
            )
        else:
            summary_features.extend([F.lit(0.0), F.lit(0.0), F.lit(0.0)])

        temporal_arrays = [
            F.array(*[column.cast("double") for column in sensor_features]) if sensor_features else empty_array("double"),
            F.array(*[column.cast("double") for column in event_features]) if event_features else empty_array("double"),
            (
                F.array(*[column.cast("double") for column in categorical_features])
                if categorical_features
                else empty_array("double")
            ),
            F.array(*[column.cast("double") for column in summary_features]) if summary_features else empty_array("double"),
        ]
        return F.concat(*temporal_arrays)

    @staticmethod
    def _reconstruction_columns(*, all_sensors: list[str], weights_b: list[list[float]]) -> tuple["Column", "Column"]:
        from pyspark.sql import functions as F

        selected_sensor_count = len(weights_b)
        weights_cols = [
            F.array(*[F.lit(float(weights_b[i][j])) for i in range(selected_sensor_count)])
            if selected_sensor_count
            else empty_array("double")
            for j in range(len(all_sensors))
        ]
        x_hat_all = (
            F.array(
                *[
                    F.aggregate(
                        F.zip_with(F.col("x_c"), weights_col, lambda x, w: x * w),
                        F.lit(0.0),
                        lambda acc, value: acc + value,
                    )
                    for weights_col in weights_cols
                ]
            )
            if all_sensors
            else empty_array("double")
        )
        backbone_residual_array = F.zip_with("x_true_all", "x_hat_all", lambda truth, pred: truth - pred)
        return x_hat_all, backbone_residual_array

    @classmethod
    def _with_backbone_vectors(
        cls,
        window_features_df: "DataFrame",
        *,
        selected_sensors_c_lit: "Column",
        all_sensors_lit: "Column",
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        return (
            window_features_df.withColumn(
                "x_true_all",
                F.transform(
                    all_sensors_lit,
                    lambda sensor_name: F.coalesce(
                        F.element_at("continuous_vector_t_end_scaled", sensor_name),
                        F.lit(0.0),
                    ),
                ),
            )
            .withColumn(
                "x_c",
                F.transform(
                    selected_sensors_c_lit,
                    lambda sensor_name: F.coalesce(
                        F.element_at("continuous_vector_t_end_scaled", sensor_name),
                        F.lit(0.0),
                    ),
                ),
            )
        )

    @classmethod
    def _with_backbone_reconstruction(
        cls,
        dataframe: "DataFrame",
        *,
        all_sensors: list[str],
        all_sensors_lit: "Column",
        weights_b: list[list[float]],
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        x_hat_all, backbone_residual_array = cls._reconstruction_columns(
            all_sensors=all_sensors,
            weights_b=weights_b,
        )
        return (
            dataframe.withColumn("x_hat_all", x_hat_all)
            .withColumn("backbone_residual_array", backbone_residual_array)
            .withColumn(
                "backbone_reconstruction_error",
                F.sqrt(
                    F.aggregate(
                        F.transform("backbone_residual_array", lambda value: value * value),
                        F.lit(0.0),
                        lambda acc, value: acc + value,
                    )
                ),
            )
            .withColumn(
                "backbone_residual_by_parameter",
                F.map_from_arrays(all_sensors_lit, F.col("backbone_residual_array")),
            )
        )

    @classmethod
    def _phase_feature_vector_column(
        cls,
        *,
        phase_selected_sensors: list[str],
        phase_selected_event_types: list[str],
        phase_selected_categorical_state_pairs: list[tuple[str, str]],
        active_sensor_denominator: int,
        temporal_history_scales: tuple[int, ...],
    ) -> "Column":
        from pyspark.sql import functions as F

        return F.concat(
            cls._level_feature_array(phase_selected_sensors),
            cls._delta_feature_array(phase_selected_sensors),
            cls._event_feature_array(phase_selected_event_types),
            cls._categorical_feature_array(phase_selected_categorical_state_pairs),
            cls._summary_feature_array(
                active_sensor_denominator=active_sensor_denominator,
            ),
            cls._temporal_feature_array(
                phase_selected_sensors=phase_selected_sensors,
                phase_selected_event_types=phase_selected_event_types,
                phase_selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
                active_sensor_denominator=active_sensor_denominator,
                temporal_history_scales=temporal_history_scales,
            ),
        )

    @classmethod
    def _finalize_feature_frame(
        cls,
        dataframe: "DataFrame",
        *,
        feature_names_lit: "Column",
        selected_sensors_c_lit: "Column",
        phase_selected_event_types_lit: "Column",
        categorical_pairs_lit: "Column",
        all_sensors_lit: "Column",
        weights_b: list[list[float]],
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        dataframe = (
            dataframe.withColumn("feature_names", feature_names_lit)
            .withColumn("selected_sensors_c", selected_sensors_c_lit)
            .withColumn("selected_event_types", phase_selected_event_types_lit)
            .withColumn("selected_categorical_state_pairs", categorical_pairs_lit)
            .withColumn("backbone_all_sensors", all_sensors_lit)
            .withColumn("backbone_weights_b", double_matrix_literal(weights_b))
            .withColumn(
                "breadth",
                (
                    F.size(F.map_keys("continuous_vector_t_end_scaled")).cast("double")
                    / F.greatest(F.size(F.col("selected_sensors_c")).cast("double"), F.lit(1.0))
                ).cast("double"),
            )
            .withColumn("drift_magnitude", F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0)).cast("double"))
        )
        if "phase_label" not in dataframe.columns:
            dataframe = dataframe.withColumn("phase_label", F.lit(None).cast("string"))
        return dataframe

    @classmethod
    def from_window_features_df(
        cls,
        window_features_df: "DataFrame",
        *,
        phase_config: PhaseFeatureConfig,
    ) -> "PhaseFeatureFrame":
        from pyspark.sql import functions as F

        backbone_model = phase_config.backbone_model
        selected_sensors_c = list(backbone_model.selected_sensors_c)
        all_sensors = list(backbone_model.all_sensors)
        phase_selected_sensors = list(phase_config.phase_selected_sensors)
        phase_selected_event_types = list(phase_config.phase_selected_event_types)
        phase_selected_categorical_state_pairs = list(phase_config.phase_selected_categorical_state_pairs)
        selected_sensors_lit = string_array_literal(selected_sensors_c)
        all_sensors_lit = string_array_literal(all_sensors)
        phase_selected_event_types_lit = string_array_literal(phase_selected_event_types)
        categorical_pairs_lit = string_array_literal(phase_config.categorical_state_labels)
        weights_b = phase_config.backbone_weights_rows
        feature_names = phase_config.feature_names
        feature_names_lit = string_array_literal(feature_names)

        active_sensor_denominator = max(len(phase_selected_sensors), 1)
        dataframe = cls._with_backbone_vectors(
            window_features_df,
            selected_sensors_c_lit=selected_sensors_lit,
            all_sensors_lit=all_sensors_lit,
        )
        dataframe = cls._with_backbone_reconstruction(
            dataframe,
            all_sensors=all_sensors,
            all_sensors_lit=all_sensors_lit,
            weights_b=weights_b,
        ).withColumn(
            "s_w",
            cls._phase_feature_vector_column(
                phase_selected_sensors=phase_selected_sensors,
                phase_selected_event_types=phase_selected_event_types,
                phase_selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
                active_sensor_denominator=active_sensor_denominator,
                temporal_history_scales=phase_config.temporal_history_scales,
            ),
        )
        dataframe = cls._finalize_feature_frame(
            dataframe,
            feature_names_lit=feature_names_lit,
            selected_sensors_c_lit=selected_sensors_lit,
            phase_selected_event_types_lit=phase_selected_event_types_lit,
            categorical_pairs_lit=categorical_pairs_lit,
            all_sensors_lit=all_sensors_lit,
            weights_b=weights_b,
        )
        return cls(dataframe=dataframe, feature_names=feature_names)

    def payload_dataframe(self) -> "DataFrame":
        return self.dataframe.select(
            "tail_id",
            "flight_id",
            "win_id",
            "t_start",
            "t_end",
            "duration_ms",
            "event_count",
            "date_utc",
            "drift_magnitude_profiled",
            "breadth",
            "backbone_reconstruction_error",
            "backbone_residual_by_parameter",
            "x_c",
            "s_w",
        )


@dataclass(frozen=True)
class PhaseObservationFrame(Frame):

    @classmethod
    def from_feature_frame(cls, feature_frame: PhaseFeatureFrame) -> "PhaseObservationFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        order_window = Window.partitionBy("tail_id", "flight_id").orderBy(
            F.col("t_end").asc_nulls_last(),
            F.col("win_id").asc(),
        )
        return cls(
            dataframe=feature_frame.dataframe.select(
                "tail_id",
                "flight_id",
                "win_id",
                "t_start",
                "t_end",
                "duration_ms",
                "event_count",
                "s_w",
                "drift_magnitude_profiled",
            ).withColumn("phase_row_number", F.row_number().over(order_window).cast("int"))
        )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
