"""Phase feature and observation frame builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.common import empty_array
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.utils import double_matrix_literal, string_array_literal


@dataclass(frozen=True)
class PhaseFeatureFrame:
    dataframe: "DataFrame"
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
        phase_selected_sensors_lit: "Column",
        phase_selected_event_types_lit: "Column",
        phase_selected_categorical_state_pairs: list[tuple[str, str]],
        active_sensor_denominator: int,
    ) -> "Column":
        from pyspark.sql import functions as F

        continuous_event_count_col = sum(cls._continuous_count_columns(), F.lit(0.0))
        categorical_event_count_col = sum(cls._categorical_count_columns(), F.lit(0.0))
        return F.concat(
            F.transform(
                phase_selected_sensors_lit,
                lambda parameter_name: F.coalesce(
                    F.element_at("continuous_vector_t_end_scaled", parameter_name),
                    F.lit(0.0),
                ),
            ),
            F.transform(
                phase_selected_event_types_lit,
                lambda event_type: (
                    F.coalesce(F.element_at("event_type_counts", event_type).cast("double"), F.lit(0.0))
                    / F.greatest(F.col("event_count").cast("double"), F.lit(1.0))
                ),
            ),
            F.array(
                *[
                    F.when(
                        F.coalesce(F.element_at("categorical_state_t_end", F.lit(parameter_name)), F.lit(""))
                        == F.lit(state),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                    for parameter_name, state in phase_selected_categorical_state_pairs
                ]
            )
            if phase_selected_categorical_state_pairs
            else empty_array("double"),
            F.array(
                F.col("event_count").cast("double")
                / F.greatest(F.col("duration_ms").cast("double") / F.lit(1000.0), F.lit(1e-6)),
                (continuous_event_count_col / F.greatest(F.col("event_count").cast("double"), F.lit(1.0))).cast("double"),
                (categorical_event_count_col / F.greatest(F.col("event_count").cast("double"), F.lit(1.0))).cast("double"),
                F.size(F.map_keys("continuous_vector_t_end_scaled")).cast("double")
                / F.lit(float(active_sensor_denominator)),
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
            .withColumn("breadth", F.element_at("s_w", -1).cast("double"))
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
        phase_selected_sensors_lit = string_array_literal(phase_selected_sensors)
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
                phase_selected_sensors_lit=phase_selected_sensors_lit,
                phase_selected_event_types_lit=phase_selected_event_types_lit,
                phase_selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
                active_sensor_denominator=active_sensor_denominator,
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
class PhaseObservationFrame:
    dataframe: "DataFrame"

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
