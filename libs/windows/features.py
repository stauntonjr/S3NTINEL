"""Class-oriented builders for the canonical ``window_features`` artifact."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from libs.common import empty_map
from libs.perf.annotations import hot_path
from libs.perf.logger import get_logger

LOGGER = get_logger("libs.windows.features")


@dataclass(frozen=True)
class WindowFeatureVectorSpec:
    timestamp_column: str = "timestamp_utc"
    parameter_name_column: str = "parameter_name"
    numeric_value_column: str = "value_num"
    text_value_column: str = "parameter_value"


@dataclass(frozen=True)
class WindowFeatureStepDiagnostics:
    step_name: str
    row_count: int
    timing_ms: float


@dataclass(frozen=True)
class WindowFeaturesDiagnostics:
    steps: list[WindowFeatureStepDiagnostics]
    output_row_count: int
    total_timing_ms: float

    def to_dict(self) -> dict[str, object]:
        return {
            "steps": [
                {
                    "step_name": step.step_name,
                    "row_count": int(step.row_count),
                    "timing_ms": float(step.timing_ms),
                }
                for step in self.steps
            ],
            "output_row_count": int(self.output_row_count),
            "total_timing_ms": float(self.total_timing_ms),
        }


@dataclass(frozen=True)
class WindowFeaturesPlan:
    vector_spec: WindowFeatureVectorSpec = field(default_factory=WindowFeatureVectorSpec)

    @staticmethod
    def _checkpoint(df: "DataFrame") -> "DataFrame":
        return df.localCheckpoint(eager=True)

    def _materialize_step(
        self,
        name: str,
        build_frame: "Callable[[], DataFrame]",
        diagnostics: list[WindowFeatureStepDiagnostics],
    ) -> "DataFrame":
        start = time.perf_counter()
        dataframe = self._checkpoint(build_frame())
        row_count = int(dataframe.count())
        diagnostics.append(
            WindowFeatureStepDiagnostics(
                step_name=name,
                row_count=row_count,
                timing_ms=(time.perf_counter() - start) * 1000.0,
            )
        )
        return dataframe

    def _build_step(
        self,
        name: str,
        build_frame: "Callable[[], DataFrame]",
        *,
        materialize: "Callable[[str, Callable[[], DataFrame]], DataFrame] | None" = None,
    ) -> "DataFrame":
        if materialize is None:
            return build_frame()
        return materialize(name, build_frame)

    def _prepare_raw(self, raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        raw_columns = set(raw_df.columns)
        prepared_raw_df = raw_df
        if "timestamp_utc" not in raw_columns and "timestamp" in raw_columns:
            prepared_raw_df = prepared_raw_df.withColumn("timestamp_utc", F.col("timestamp").cast("timestamp"))
            raw_columns = set(prepared_raw_df.columns)

        parameter_value_col = (
            F.col("parameter_value").cast("string")
            if "parameter_value" in raw_columns
            else F.col("parameter_value_clean").cast("string")
            if "parameter_value_clean" in raw_columns
            else F.lit(None).cast("string")
        )
        value_num_col = (
            F.col("val").cast("double")
            if "val" in raw_columns
            else F.expr("try_cast(parameter_value as double)")
            if "parameter_value" in raw_columns
            else F.expr("try_cast(parameter_value_clean as double)")
            if "parameter_value_clean" in raw_columns
            else F.lit(None).cast("double")
        )
        return (
            prepared_raw_df.select(
                F.col("tail_id").cast("string").alias("tail_id"),
                F.col("flight_id").cast("string").alias("flight_id"),
                F.col("timestamp_utc").cast("timestamp").alias(self.vector_spec.timestamp_column),
                F.col("parameter_name").cast("string").alias(self.vector_spec.parameter_name_column),
                parameter_value_col.alias(self.vector_spec.text_value_column),
                value_num_col.alias(self.vector_spec.numeric_value_column),
            )
            .where(
                F.col("tail_id").isNotNull()
                & F.col("flight_id").isNotNull()
                & F.col(self.vector_spec.parameter_name_column).isNotNull()
                & F.col(self.vector_spec.timestamp_column).isNotNull()
            )
        )

    def _build_scaler_frame(
        self,
        prepared_raw_df: "DataFrame",
        *,
        scaling_profile_df: "DataFrame | None" = None,
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        if scaling_profile_df is not None:
            return scaling_profile_df.select(
                F.col("parameter_name").cast("string").alias("parameter_name"),
                F.col("scaling_center_median").cast("double").alias("median"),
                F.greatest(F.coalesce(F.col("scaling_iqr").cast("double"), F.lit(0.0)), F.lit(1e-6)).alias("iqr"),
            )
        return (
            prepared_raw_df.where(F.col(self.vector_spec.numeric_value_column).isNotNull())
            .groupBy(self.vector_spec.parameter_name_column)
            .agg(
                F.expr(
                    f"percentile({self.vector_spec.numeric_value_column}, array(0.25D, 0.5D, 0.75D))"
                ).alias("scaling_quantiles"),
            )
            .withColumn("q25", F.col("scaling_quantiles").getItem(0).cast("double"))
            .withColumn("median", F.col("scaling_quantiles").getItem(1).cast("double"))
            .withColumn("q75", F.col("scaling_quantiles").getItem(2).cast("double"))
            .withColumn("iqr", F.greatest(F.col("q75") - F.col("q25"), F.lit(1e-6)))
            .select(
                F.col(self.vector_spec.parameter_name_column).alias("parameter_name"),
                "median",
                "iqr",
            )
        )

    def _base_windows(self, windows_df: "DataFrame") -> "DataFrame":
        return windows_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            "t_start",
            "t_end",
            "duration_ms",
            "event_count",
            "date_utc",
        )

    def _build_raw_intervals(self, prepared_raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        parameter_window = Window.partitionBy(
            "tail_id",
            "flight_id",
            self.vector_spec.parameter_name_column,
        ).orderBy(self.vector_spec.timestamp_column)
        far_future_ts = F.lit("9999-12-31 23:59:59").cast("timestamp")
        return prepared_raw_df.withColumn(
            "next_timestamp_utc",
            F.lead(self.vector_spec.timestamp_column).over(parameter_window),
        ).withColumn("next_timestamp_utc", F.coalesce(F.col("next_timestamp_utc"), far_future_ts))

    def _build_snapshot_rows(
        self,
        *,
        base_windows_df: "DataFrame",
        raw_intervals_df: "DataFrame",
        anchor_column: str,
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        return (
            base_windows_df.alias("w")
            .join(
                raw_intervals_df.alias("r"),
                on=(
                    (F.col("w.tail_id") == F.col("r.tail_id"))
                    & (F.col("w.flight_id") == F.col("r.flight_id"))
                    & (F.col(f"w.{anchor_column}") >= F.col(f"r.{self.vector_spec.timestamp_column}"))
                    & (F.col(f"w.{anchor_column}") < F.col("r.next_timestamp_utc"))
                ),
                how="left",
            )
            .select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.flight_id").alias("flight_id"),
                F.col("w.win_id").alias("win_id"),
                F.col(f"r.{self.vector_spec.parameter_name_column}").alias("parameter_name"),
                F.col(f"r.{self.vector_spec.text_value_column}").alias("parameter_value"),
                F.col(f"r.{self.vector_spec.numeric_value_column}").alias("value_num"),
            )
            .where(F.col("parameter_name").isNotNull())
        )

    def _build_snapshot_feature_frames(
        self,
        *,
        snapshot_rows_df: "DataFrame",
        scaler_df: "DataFrame",
        suffix: str,
    ) -> tuple["DataFrame", "DataFrame", "DataFrame"]:
        from pyspark.sql import functions as F

        continuous_snapshot_df = (
            snapshot_rows_df.where(F.col("value_num").isNotNull())
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("value_num").cast("double")))
                ).alias(f"snapshot_continuous_vector_{suffix}")
            )
        )
        continuous_snapshot_scaled_df = (
            snapshot_rows_df.where(F.col("value_num").isNotNull())
            .join(scaler_df, on="parameter_name", how="left")
            .withColumn("scaled_value", (F.col("value_num") - F.col("median")) / F.col("iqr"))
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("scaled_value").cast("double")))
                ).alias(f"snapshot_continuous_vector_{suffix}_scaled")
            )
        )
        categorical_snapshot_df = (
            snapshot_rows_df.where(
                F.col("value_num").isNull()
                & F.col("parameter_value").isNotNull()
                & (F.length(F.trim(F.col("parameter_value"))) > 0)
            )
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("parameter_value").cast("string")))
                ).alias(f"snapshot_categorical_state_{suffix}")
            )
        )
        return continuous_snapshot_df, continuous_snapshot_scaled_df, categorical_snapshot_df

    def _build_events_in_windows(self, *, base_windows_df: "DataFrame", events_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        run_peak_delta_expr = (
            F.expr("try_cast(element_at(payload, 'run_peak_delta') as double)")
            if "payload" in events_df.columns
            else F.lit(None).cast("double")
        )
        emission_reason_col = (
            F.element_at("payload", F.lit("emission_reason")).cast("string")
            if "payload" in events_df.columns
            else F.lit(None).cast("string")
        )
        event_type_col = (
            F.col("event_type_detected").cast("string")
            if "event_type_detected" in events_df.columns
            else F.lit("").cast("string")
        )
        selected_columns = [
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("e.parameter_name").cast("string").alias("parameter_name"),
            event_type_col.alias("event_type_detected"),
            run_peak_delta_expr.alias("run_peak_delta"),
            emission_reason_col.alias("emission_reason"),
        ]
        return (
            base_windows_df.alias("w")
            .join(
                events_df.alias("e"),
                on=(
                    (F.col("w.tail_id") == F.col("e.tail_id"))
                    & (F.col("w.flight_id") == F.col("e.flight_id"))
                    & (F.col("e.timestamp_utc") >= F.col("w.t_start"))
                    & (F.col("e.timestamp_utc") <= F.col("w.t_end"))
                ),
                how="left",
            )
            .select(*selected_columns)
            .where(F.col("parameter_name").isNotNull())
        )

    @staticmethod
    def _empty_continuous_event_summary():
        from pyspark.sql import functions as F

        return F.struct(
            empty_map("string", "int").alias("slope_run_count_by_parameter"),
            empty_map("string", "int").alias("slope_reinforcement_count_by_parameter"),
            empty_map("string", "double").alias("slope_signed_impulse_by_parameter"),
            empty_map("string", "double").alias("slope_abs_impulse_by_parameter"),
            empty_map("string", "double").alias("slope_peak_abs_delta_by_parameter"),
            empty_map("string", "int").alias("switch_count_by_parameter"),
            empty_map("string", "int").alias("threshold_count_by_parameter"),
            empty_map("string", "int").alias("oscillation_count_by_parameter"),
            empty_map("string", "int").alias("drift_guard_count_by_parameter"),
        )

    def _build_event_feature_frames(
        self,
        *,
        events_in_windows_df: "DataFrame",
    ) -> tuple["DataFrame", "DataFrame"]:
        from pyspark.sql import functions as F

        event_type_counts_df = (
            events_in_windows_df.where(F.col("event_type_detected").isNotNull())
            .groupBy("tail_id", "flight_id", "win_id", "event_type_detected")
            .agg(F.count(F.lit(1)).cast("int").alias("event_type_count"))
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("event_type_detected"), F.col("event_type_count")))
                ).alias("window_event_type_counts")
            )
        )
        is_slope = F.col("event_type_detected").isin("slope_pos", "slope_neg")
        signed_peak_delta = (
            F.when(F.col("event_type_detected") == "slope_pos", F.coalesce(F.col("run_peak_delta"), F.lit(0.0)))
            .when(F.col("event_type_detected") == "slope_neg", -F.coalesce(F.col("run_peak_delta"), F.lit(0.0)))
            .otherwise(F.lit(0.0))
        )
        abs_peak_delta = F.when(
            is_slope,
            F.abs(F.coalesce(F.col("run_peak_delta"), F.lit(0.0))),
        ).otherwise(F.lit(0.0))
        per_parameter_summary_df = (
            events_in_windows_df.groupBy("tail_id", "flight_id", "win_id", "parameter_name")
            .agg(
                F.sum(F.when(is_slope, F.lit(1)).otherwise(F.lit(0))).cast("int").alias("slope_run_count"),
                F.sum(
                    F.when(
                        is_slope & (F.col("emission_reason") == F.lit("run_strengthen")),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                )
                .cast("int")
                .alias("slope_reinforcement_count"),
                F.sum(signed_peak_delta).cast("double").alias("slope_signed_impulse"),
                F.sum(abs_peak_delta).cast("double").alias("slope_abs_impulse"),
                F.max(abs_peak_delta).cast("double").alias("slope_peak_abs_delta"),
                F.sum(F.when(F.col("event_type_detected") == "switch", F.lit(1)).otherwise(F.lit(0)))
                .cast("int")
                .alias("switch_count"),
                F.sum(F.when(F.col("event_type_detected") == "threshold", F.lit(1)).otherwise(F.lit(0)))
                .cast("int")
                .alias("threshold_count"),
                F.sum(F.when(F.col("event_type_detected") == "oscillation", F.lit(1)).otherwise(F.lit(0)))
                .cast("int")
                .alias("oscillation_count"),
                F.sum(F.when(F.col("event_type_detected") == "drift_guard", F.lit(1)).otherwise(F.lit(0)))
                .cast("int")
                .alias("drift_guard_count"),
            )
        )
        continuous_event_summary_df = (
            per_parameter_summary_df.groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("slope_run_count").cast("int")))
                ).alias("slope_run_count_by_parameter"),
                F.map_from_entries(
                    F.collect_list(
                        F.struct(F.col("parameter_name"), F.col("slope_reinforcement_count").cast("int"))
                    )
                ).alias("slope_reinforcement_count_by_parameter"),
                F.map_from_entries(
                    F.collect_list(
                        F.struct(F.col("parameter_name"), F.col("slope_signed_impulse").cast("double"))
                    )
                ).alias("slope_signed_impulse_by_parameter"),
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("slope_abs_impulse").cast("double")))
                ).alias("slope_abs_impulse_by_parameter"),
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("slope_peak_abs_delta").cast("double")))
                ).alias("slope_peak_abs_delta_by_parameter"),
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("switch_count").cast("int")))
                ).alias("switch_count_by_parameter"),
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("threshold_count").cast("int")))
                ).alias("threshold_count_by_parameter"),
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("oscillation_count").cast("int")))
                ).alias("oscillation_count_by_parameter"),
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("drift_guard_count").cast("int")))
                ).alias("drift_guard_count_by_parameter"),
            )
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                F.struct(
                    F.coalesce(F.col("slope_run_count_by_parameter"), empty_map("string", "int")).alias(
                        "slope_run_count_by_parameter"
                    ),
                    F.coalesce(
                        F.col("slope_reinforcement_count_by_parameter"),
                        empty_map("string", "int"),
                    ).alias("slope_reinforcement_count_by_parameter"),
                    F.coalesce(F.col("slope_signed_impulse_by_parameter"), empty_map("string", "double")).alias(
                        "slope_signed_impulse_by_parameter"
                    ),
                    F.coalesce(F.col("slope_abs_impulse_by_parameter"), empty_map("string", "double")).alias(
                        "slope_abs_impulse_by_parameter"
                    ),
                    F.coalesce(F.col("slope_peak_abs_delta_by_parameter"), empty_map("string", "double")).alias(
                        "slope_peak_abs_delta_by_parameter"
                    ),
                    F.coalesce(F.col("switch_count_by_parameter"), empty_map("string", "int")).alias(
                        "switch_count_by_parameter"
                    ),
                    F.coalesce(F.col("threshold_count_by_parameter"), empty_map("string", "int")).alias(
                        "threshold_count_by_parameter"
                    ),
                    F.coalesce(F.col("oscillation_count_by_parameter"), empty_map("string", "int")).alias(
                        "oscillation_count_by_parameter"
                    ),
                    F.coalesce(F.col("drift_guard_count_by_parameter"), empty_map("string", "int")).alias(
                        "drift_guard_count_by_parameter"
                    ),
                ).alias("continuous_event_summary"),
            )
        )
        return event_type_counts_df, continuous_event_summary_df

    def _merge_feature_sources(
        self,
        *,
        base_windows_df: "DataFrame",
        continuous_snapshot_start_df: "DataFrame",
        continuous_snapshot_start_scaled_df: "DataFrame",
        continuous_snapshot_df: "DataFrame",
        continuous_snapshot_scaled_df: "DataFrame",
        categorical_snapshot_start_df: "DataFrame",
        categorical_snapshot_df: "DataFrame",
        event_type_counts_df: "DataFrame",
        continuous_event_summary_df: "DataFrame",
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        return (
            base_windows_df.join(continuous_snapshot_start_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_snapshot_start_scaled_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_snapshot_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_snapshot_scaled_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(categorical_snapshot_start_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(categorical_snapshot_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(event_type_counts_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_event_summary_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .withColumn(
                "continuous_vector_t_start",
                F.coalesce(
                    F.col("snapshot_continuous_vector_t_start"),
                    empty_map("string", "double"),
                ),
            )
            .withColumn(
                "continuous_vector_t_start_scaled",
                F.coalesce(
                    F.col("snapshot_continuous_vector_t_start_scaled"),
                    empty_map("string", "double"),
                ),
            )
            .withColumn(
                "continuous_vector_t_end",
                F.coalesce(
                    F.col("snapshot_continuous_vector_t_end"),
                    empty_map("string", "double"),
                ),
            )
            .withColumn(
                "continuous_vector_t_end_scaled",
                F.coalesce(
                    F.col("snapshot_continuous_vector_t_end_scaled"),
                    empty_map("string", "double"),
                ),
            )
            .withColumn(
                "categorical_state_t_start",
                F.when(
                    F.col("snapshot_categorical_state_t_start").isNotNull(),
                    F.col("snapshot_categorical_state_t_start"),
                ).otherwise(empty_map()),
            )
            .withColumn(
                "categorical_state_t_end",
                F.when(
                    F.col("snapshot_categorical_state_t_end").isNotNull(),
                    F.col("snapshot_categorical_state_t_end"),
                ).otherwise(empty_map()),
            )
            .withColumn(
                "event_type_counts",
                F.coalesce(F.col("window_event_type_counts"), empty_map("string", "int")),
            )
            .withColumn(
                "continuous_event_summary",
                F.coalesce(
                    F.col("continuous_event_summary"),
                    self._empty_continuous_event_summary(),
                ),
            )
        )

    def _with_drift_profile(self, combined_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        flight_window = Window.partitionBy("tail_id", "flight_id").orderBy("t_end", "win_id")
        return (
            combined_df.withColumn(
                "prev_continuous_vector_t_end_scaled",
                F.lag("continuous_vector_t_end_scaled").over(flight_window),
            )
            .withColumn(
                "drift_magnitude_profiled",
                F.expr(
                    """
                    sqrt(
                      aggregate(
                        map_values(
                          map_zip_with(
                            coalesce(continuous_vector_t_end_scaled, cast(map() as map<string,double>)),
                            coalesce(prev_continuous_vector_t_end_scaled, cast(map() as map<string,double>)),
                            (k, current_value, prev_value) -> coalesce(current_value, 0D) - coalesce(prev_value, 0D)
                          )
                        ),
                        cast(0.0 as double),
                        (acc, delta_value) -> acc + (delta_value * delta_value)
                      )
                    )
                    """
                ),
            )
        )

    def _finalize_feature_frame(self, enriched_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        return enriched_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            "t_start",
            "t_end",
            "duration_ms",
            "event_count",
            "date_utc",
            "event_type_counts",
            "continuous_event_summary",
            "continuous_vector_t_start",
            "continuous_vector_t_start_scaled",
            "continuous_vector_t_end",
            "continuous_vector_t_end_scaled",
            F.coalesce(
                F.col("categorical_state_t_start"),
                empty_map(),
            ).alias("categorical_state_t_start"),
            F.coalesce(
                F.col("categorical_state_t_end"),
                empty_map(),
            ).alias("categorical_state_t_end"),
            F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0)).cast("double").alias(
                "drift_magnitude_profiled"
            ),
            F.lit(None).cast("string").alias("phase_label"),
        )

    def _assemble_feature_frame(
        self,
        *,
        base_windows_df: "DataFrame",
        continuous_snapshot_start_df: "DataFrame",
        continuous_snapshot_start_scaled_df: "DataFrame",
        continuous_snapshot_df: "DataFrame",
        continuous_snapshot_scaled_df: "DataFrame",
        categorical_snapshot_start_df: "DataFrame",
        categorical_snapshot_df: "DataFrame",
        event_type_counts_df: "DataFrame",
        continuous_event_summary_df: "DataFrame",
    ) -> "DataFrame":
        combined_df = self._merge_feature_sources(
            base_windows_df=base_windows_df,
            continuous_snapshot_start_df=continuous_snapshot_start_df,
            continuous_snapshot_start_scaled_df=continuous_snapshot_start_scaled_df,
            continuous_snapshot_df=continuous_snapshot_df,
            continuous_snapshot_scaled_df=continuous_snapshot_scaled_df,
            categorical_snapshot_start_df=categorical_snapshot_start_df,
            categorical_snapshot_df=categorical_snapshot_df,
            event_type_counts_df=event_type_counts_df,
            continuous_event_summary_df=continuous_event_summary_df,
        )
        return self._finalize_feature_frame(self._with_drift_profile(combined_df))

    def _build_feature_frame(
        self,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
        scaling_profile_df: "DataFrame | None" = None,
        *,
        materialize: "Callable[[str, Callable[[], DataFrame]], DataFrame] | None" = None,
    ) -> "DataFrame":
        base_windows_df = self._build_step(
            "base_windows",
            lambda: self._base_windows(windows_df),
            materialize=materialize,
        )
        prepared_raw_df = self._build_step(
            "prepare_raw",
            lambda: self._prepare_raw(raw_df),
            materialize=materialize,
        )
        raw_intervals_df = self._build_step(
            "raw_intervals",
            lambda: self._build_raw_intervals(prepared_raw_df),
            materialize=materialize,
        )
        scaler_df = self._build_step(
            "scaler_frame",
            lambda: self._build_scaler_frame(prepared_raw_df, scaling_profile_df=scaling_profile_df),
            materialize=materialize,
        )
        snapshot_rows_start_df = self._build_step(
            "snapshot_rows_start",
            lambda: self._build_snapshot_rows(
                base_windows_df=base_windows_df,
                raw_intervals_df=raw_intervals_df,
                anchor_column="t_start",
            ),
            materialize=materialize,
        )
        snapshot_rows_end_df = self._build_step(
            "snapshot_rows_end",
            lambda: self._build_snapshot_rows(
                base_windows_df=base_windows_df,
                raw_intervals_df=raw_intervals_df,
                anchor_column="t_end",
            ),
            materialize=materialize,
        )
        snapshot_feature_frames_start = self._build_snapshot_feature_frames(
            snapshot_rows_df=snapshot_rows_start_df,
            scaler_df=scaler_df,
            suffix="t_start",
        )
        snapshot_feature_frames_end = self._build_snapshot_feature_frames(
            snapshot_rows_df=snapshot_rows_end_df,
            scaler_df=scaler_df,
            suffix="t_end",
        )
        continuous_snapshot_start_df = self._build_step(
            "continuous_snapshot_start",
            lambda: snapshot_feature_frames_start[0],
            materialize=materialize,
        )
        continuous_snapshot_start_scaled_df = self._build_step(
            "continuous_snapshot_start_scaled",
            lambda: snapshot_feature_frames_start[1],
            materialize=materialize,
        )
        categorical_snapshot_start_df = self._build_step(
            "categorical_snapshot_start",
            lambda: snapshot_feature_frames_start[2],
            materialize=materialize,
        )
        continuous_snapshot_df = self._build_step(
            "continuous_snapshot",
            lambda: snapshot_feature_frames_end[0],
            materialize=materialize,
        )
        continuous_snapshot_scaled_df = self._build_step(
            "continuous_snapshot_scaled",
            lambda: snapshot_feature_frames_end[1],
            materialize=materialize,
        )
        categorical_snapshot_df = self._build_step(
            "categorical_snapshot",
            lambda: snapshot_feature_frames_end[2],
            materialize=materialize,
        )
        events_in_windows_df = self._build_step(
            "events_in_windows",
            lambda: self._build_events_in_windows(base_windows_df=base_windows_df, events_df=events_df),
            materialize=materialize,
        )
        event_feature_frames = self._build_event_feature_frames(
            events_in_windows_df=events_in_windows_df,
        )
        event_type_counts_df = self._build_step(
            "event_type_counts",
            lambda: event_feature_frames[0],
            materialize=materialize,
        )
        continuous_event_summary_df = self._build_step(
            "continuous_event_summary",
            lambda: event_feature_frames[1],
            materialize=materialize,
        )
        return self._build_step(
            "assemble_feature_frame",
            lambda: self._assemble_feature_frame(
                base_windows_df=base_windows_df,
                continuous_snapshot_start_df=continuous_snapshot_start_df,
                continuous_snapshot_start_scaled_df=continuous_snapshot_start_scaled_df,
                continuous_snapshot_df=continuous_snapshot_df,
                continuous_snapshot_scaled_df=continuous_snapshot_scaled_df,
                categorical_snapshot_start_df=categorical_snapshot_start_df,
                categorical_snapshot_df=categorical_snapshot_df,
                event_type_counts_df=event_type_counts_df,
                continuous_event_summary_df=continuous_event_summary_df,
            ),
            materialize=materialize,
        )

    @hot_path
    def build(
        self,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
        scaling_profile_df: "DataFrame | None" = None,
    ) -> "DataFrame":
        return self._build_feature_frame(
            raw_df,
            events_df,
            windows_df,
            scaling_profile_df=scaling_profile_df,
        )

    @hot_path
    def build_with_diagnostics(
        self,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
        scaling_profile_df: "DataFrame | None" = None,
    ) -> tuple["DataFrame", WindowFeaturesDiagnostics]:
        start = time.perf_counter()
        step_diagnostics: list[WindowFeatureStepDiagnostics] = []
        output_df = self._build_feature_frame(
            raw_df,
            events_df,
            windows_df,
            scaling_profile_df=scaling_profile_df,
            materialize=lambda name, build_frame: self._materialize_step(
                name,
                build_frame,
                step_diagnostics,
            ),
        )
        diagnostics = WindowFeaturesDiagnostics(
            steps=step_diagnostics,
            output_row_count=step_diagnostics[-1].row_count if step_diagnostics else 0,
            total_timing_ms=(time.perf_counter() - start) * 1000.0,
        )
        LOGGER.info("window_features_build diagnostics=%s", diagnostics.to_dict())
        return output_df, diagnostics

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
