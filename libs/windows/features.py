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

    def _build_scaler_frame(self, prepared_raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

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

    def _build_snapshot_rows(self, *, base_windows_df: "DataFrame", raw_intervals_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        return (
            base_windows_df.alias("w")
            .join(
                raw_intervals_df.alias("r"),
                on=(
                    (F.col("w.tail_id") == F.col("r.tail_id"))
                    & (F.col("w.flight_id") == F.col("r.flight_id"))
                    & (F.col("w.t_end") >= F.col(f"r.{self.vector_spec.timestamp_column}"))
                    & (F.col("w.t_end") < F.col("r.next_timestamp_utc"))
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
    ) -> tuple["DataFrame", "DataFrame", "DataFrame"]:
        from pyspark.sql import functions as F

        continuous_snapshot_df = (
            snapshot_rows_df.where(F.col("value_num").isNotNull())
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("value_num").cast("double")))
                ).alias("snapshot_continuous_vector_t_end")
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
                ).alias("snapshot_continuous_vector_t_end_scaled")
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
                ).alias("snapshot_categorical_state_t_end")
            )
        )
        return continuous_snapshot_df, continuous_snapshot_scaled_df, categorical_snapshot_df

    def _build_events_in_windows(self, *, base_windows_df: "DataFrame", events_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        event_value_expr = (
            F.expr("try_cast(element_at(payload, 'value') as double)")
            if "payload" in events_df.columns
            else F.lit(None).cast("double")
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
            F.col("e.timestamp_utc").cast("timestamp").alias("timestamp_utc"),
            event_type_col.alias("event_type_detected"),
            event_value_expr.alias("value_num"),
        ]
        if "event_seq_id" in events_df.columns:
            selected_columns.append(F.col("e.event_seq_id").cast("long").alias("event_seq_id"))
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
            .where(F.col("parameter_name").isNotNull() & F.col("timestamp_utc").isNotNull())
        )

    def _build_event_feature_frames(
        self,
        *,
        events_in_windows_df: "DataFrame",
        scaler_df: "DataFrame",
    ) -> tuple["DataFrame", "DataFrame", "DataFrame"]:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        latest_event_order_columns = [F.col("timestamp_utc").desc()]
        if "event_seq_id" in events_in_windows_df.columns:
            latest_event_order_columns.append(F.col("event_seq_id").desc())
        latest_event_order_columns.append(F.col("event_type_detected").desc())
        latest_event_window = Window.partitionBy("tail_id", "flight_id", "win_id", "parameter_name").orderBy(
            *latest_event_order_columns,
        )
        event_numeric_latest = (
            events_in_windows_df.where(F.col("value_num").isNotNull())
            .withColumn("rn", F.row_number().over(latest_event_window))
            .where(F.col("rn") == 1)
            .drop("rn")
        )
        continuous_event_df = (
            event_numeric_latest.groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("value_num").cast("double")))
                ).alias("event_continuous_vector_t_end")
            )
        )
        continuous_event_scaled_df = (
            event_numeric_latest.join(scaler_df, on="parameter_name", how="left")
            .withColumn("scaled_value", (F.col("value_num") - F.col("median")) / F.col("iqr"))
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("parameter_name"), F.col("scaled_value").cast("double")))
                ).alias("event_continuous_vector_t_end_scaled")
            )
        )
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
        return continuous_event_df, continuous_event_scaled_df, event_type_counts_df

    def _merge_feature_sources(
        self,
        *,
        base_windows_df: "DataFrame",
        continuous_snapshot_df: "DataFrame",
        continuous_snapshot_scaled_df: "DataFrame",
        continuous_event_df: "DataFrame",
        continuous_event_scaled_df: "DataFrame",
        categorical_snapshot_df: "DataFrame",
        event_type_counts_df: "DataFrame",
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        return (
            base_windows_df.join(continuous_snapshot_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_snapshot_scaled_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_event_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(continuous_event_scaled_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(categorical_snapshot_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(event_type_counts_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .withColumn(
                "continuous_vector_t_end",
                F.when(
                    F.size(F.coalesce(F.col("event_continuous_vector_t_end"), empty_map("string", "double")))
                    > 0,
                    F.col("event_continuous_vector_t_end"),
                ).otherwise(
                    F.coalesce(
                        F.col("snapshot_continuous_vector_t_end"),
                        empty_map("string", "double"),
                    )
                ),
            )
            .withColumn(
                "continuous_vector_t_end_scaled",
                F.when(
                    F.size(
                        F.coalesce(
                            F.col("event_continuous_vector_t_end_scaled"),
                            empty_map("string", "double"),
                        )
                    )
                    > 0,
                    F.col("event_continuous_vector_t_end_scaled"),
                ).otherwise(
                    F.coalesce(
                        F.col("snapshot_continuous_vector_t_end_scaled"),
                        empty_map("string", "double"),
                    )
                ),
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
            "continuous_vector_t_end",
            "continuous_vector_t_end_scaled",
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
        continuous_snapshot_df: "DataFrame",
        continuous_snapshot_scaled_df: "DataFrame",
        continuous_event_df: "DataFrame",
        continuous_event_scaled_df: "DataFrame",
        categorical_snapshot_df: "DataFrame",
        event_type_counts_df: "DataFrame",
    ) -> "DataFrame":
        combined_df = self._merge_feature_sources(
            base_windows_df=base_windows_df,
            continuous_snapshot_df=continuous_snapshot_df,
            continuous_snapshot_scaled_df=continuous_snapshot_scaled_df,
            continuous_event_df=continuous_event_df,
            continuous_event_scaled_df=continuous_event_scaled_df,
            categorical_snapshot_df=categorical_snapshot_df,
            event_type_counts_df=event_type_counts_df,
        )
        return self._finalize_feature_frame(self._with_drift_profile(combined_df))

    def _build_feature_frame(
        self,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
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
            lambda: self._build_scaler_frame(prepared_raw_df),
            materialize=materialize,
        )
        snapshot_rows_df = self._build_step(
            "snapshot_rows",
            lambda: self._build_snapshot_rows(base_windows_df=base_windows_df, raw_intervals_df=raw_intervals_df),
            materialize=materialize,
        )
        snapshot_feature_frames = self._build_snapshot_feature_frames(
            snapshot_rows_df=snapshot_rows_df,
            scaler_df=scaler_df,
        )
        continuous_snapshot_df = self._build_step(
            "continuous_snapshot",
            lambda: snapshot_feature_frames[0],
            materialize=materialize,
        )
        continuous_snapshot_scaled_df = self._build_step(
            "continuous_snapshot_scaled",
            lambda: snapshot_feature_frames[1],
            materialize=materialize,
        )
        categorical_snapshot_df = self._build_step(
            "categorical_snapshot",
            lambda: snapshot_feature_frames[2],
            materialize=materialize,
        )
        events_in_windows_df = self._build_step(
            "events_in_windows",
            lambda: self._build_events_in_windows(base_windows_df=base_windows_df, events_df=events_df),
            materialize=materialize,
        )
        event_feature_frames = self._build_event_feature_frames(
            events_in_windows_df=events_in_windows_df,
            scaler_df=scaler_df,
        )
        continuous_event_df = self._build_step(
            "continuous_event",
            lambda: event_feature_frames[0],
            materialize=materialize,
        )
        continuous_event_scaled_df = self._build_step(
            "continuous_event_scaled",
            lambda: event_feature_frames[1],
            materialize=materialize,
        )
        event_type_counts_df = self._build_step(
            "event_type_counts",
            lambda: event_feature_frames[2],
            materialize=materialize,
        )
        return self._build_step(
            "assemble_feature_frame",
            lambda: self._assemble_feature_frame(
                base_windows_df=base_windows_df,
                continuous_snapshot_df=continuous_snapshot_df,
                continuous_snapshot_scaled_df=continuous_snapshot_scaled_df,
                continuous_event_df=continuous_event_df,
                continuous_event_scaled_df=continuous_event_scaled_df,
                categorical_snapshot_df=categorical_snapshot_df,
                event_type_counts_df=event_type_counts_df,
            ),
            materialize=materialize,
        )

    @hot_path
    def build(self, raw_df: "DataFrame", events_df: "DataFrame", windows_df: "DataFrame") -> "DataFrame":
        return self._build_feature_frame(raw_df, events_df, windows_df)

    @hot_path
    def build_with_diagnostics(
        self,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
    ) -> tuple["DataFrame", WindowFeaturesDiagnostics]:
        start = time.perf_counter()
        step_diagnostics: list[WindowFeatureStepDiagnostics] = []
        output_df = self._build_feature_frame(
            raw_df,
            events_df,
            windows_df,
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

@hot_path
def build_window_features_spark_table(raw_df: "DataFrame", events_df: "DataFrame", windows_df: "DataFrame") -> "DataFrame":
    """Build the canonical ``window_features`` artifact in Spark."""
    return WindowFeaturesPlan().build(raw_df, events_df, windows_df)


@hot_path
def build_window_features_with_diagnostics_spark_table(
    raw_df: "DataFrame",
    events_df: "DataFrame",
    windows_df: "DataFrame",
) -> tuple["DataFrame", WindowFeaturesDiagnostics]:
    """Build ``window_features`` and emit explicit per-step diagnostics for development tuning."""
    return WindowFeaturesPlan().build_with_diagnostics(raw_df, events_df, windows_df)


from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
