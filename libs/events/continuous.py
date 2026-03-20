"""Continuous-channel event detection over Spark DataFrames."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from libs.common import ParameterDataType, spark_normalized_parameter_datatype_expr
from libs.events.types import (
    DriftGuardEvent,
    ExtremaEvent,
    OscillationEvent,
    SlopeNegativeEvent,
    SlopePositiveEvent,
    SwitchEvent,
    ThresholdEvent,
    append_detected_events,
    empty_detected_event_array,
    null_detected_event,
)
from libs.perf.annotations import hot_path
from libs.spark_sequence import SegmentedSequencePlan, SequenceOrderingPolicy, segment_policy_from_env


def _default_event_segment_policy():
    return segment_policy_from_env(
        "EVENT",
        default_max_rows_per_segment=50_000,
        default_max_span_ms=900_000,
    )


@dataclass(frozen=True)
class ContinuousDetectorConfig:
    delta_threshold: float = 0.0
    ema_alpha: float = 0.2
    slope_source: str = "ema"
    residual_z_threshold: float = 3.0
    slope_abs_threshold: float = 1.0
    slope_min_persistence_samples: int = 2
    slope_reemit_ratio: float = 1.5
    switch_z_threshold: float = 4.0
    switch_delta_z_threshold: float = 3.0
    switch_min_abs_delta: float = 15.0
    switch_delta_scale: float = 6.0
    switch_residual_z_min: float = 0.75
    switch_refractory_samples: int = 20
    min_sigma: float = 1e-3
    oscillation_window: int = 8
    oscillation_amplitude_window: int = 200
    oscillation_ema_alpha: float = 0.12
    oscillation_sign_changes: int = 4
    oscillation_min_amplitude: float = 10.0
    oscillation_min_extrema: int = 4
    oscillation_period_cv_max: float = 0.9
    oscillation_min_period_samples: int = 2
    oscillation_min_alternation_ratio: float = 0.6
    oscillation_period_ema_alpha: float = 0.2
    oscillation_period_band_ratio: float = 0.8
    oscillation_refractory_samples: int = 80
    drift_guard_abs_change: float = 0.0
    drift_guard_max_gap_samples: int = 0
    emit_extrema_events: bool = False
    warmup_points: int = 5


@dataclass(frozen=True)
class ContinuousSequenceStateLayout:
    last_switch_index: str = "last_switch_index"
    last_oscillation_index: str = "last_oscillation_index"
    last_drift_guard_index: str = "last_drift_guard_index"
    drift_guard_cum_abs: str = "drift_guard_cum_abs"
    slope_run_sign: str = "slope_run_sign"
    slope_run_length: str = "slope_run_length"
    slope_run_peak_abs_delta: str = "slope_run_peak_abs_delta"
    slope_run_emitted: str = "slope_run_emitted"
    slope_run_last_emitted_peak_abs_delta: str = "slope_run_last_emitted_peak_abs_delta"
    emitted_events: str = "emitted_events"

    def initial_state_column(self) -> "Column":
        from pyspark.sql import functions as F

        return F.struct(
            F.lit(-1_000_000_000).cast("long").alias(self.last_switch_index),
            F.lit(-1_000_000_000).cast("long").alias(self.last_oscillation_index),
            F.lit(0).cast("long").alias(self.last_drift_guard_index),
            F.lit(0.0).cast("double").alias(self.drift_guard_cum_abs),
            F.lit(0).cast("int").alias(self.slope_run_sign),
            F.lit(0).cast("long").alias(self.slope_run_length),
            F.lit(0.0).cast("double").alias(self.slope_run_peak_abs_delta),
            F.lit(False).cast("boolean").alias(self.slope_run_emitted),
            F.lit(0.0).cast("double").alias(self.slope_run_last_emitted_peak_abs_delta),
            empty_detected_event_array().alias(self.emitted_events),
        )

    def state_after_step_column(
        self,
        *,
        acc: "Column",
        step: "Column",
        switch_refractory_samples: "Column",
        oscillation_refractory_samples: "Column",
        oscillation_window: "Column",
        drift_guard_abs_change: "Column",
        emit_extrema_events: "Column",
        drift_guard_max_gap_samples: "Column",
        slope_min_persistence_samples: "Column",
        slope_reemit_ratio: "Column",
        slope_source: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        cum_abs = acc[self.drift_guard_cum_abs] + F.abs(F.coalesce(step["delta_raw"], F.lit(0.0)))
        switch_emit = step["switch_candidate"] & (
            (step["sample_index"] - acc[self.last_switch_index]) >= switch_refractory_samples
        )
        drift_change_emit = (drift_guard_abs_change > F.lit(0.0)) & (cum_abs >= drift_guard_abs_change)
        drift_gap_emit = (drift_guard_max_gap_samples > F.lit(0)) & (
            (step["sample_index"] - acc[self.last_drift_guard_index]) >= drift_guard_max_gap_samples
        )
        drift_emit = drift_change_emit | drift_gap_emit
        extrema_emit = emit_extrema_events & step["extrema_kind"].isNotNull()
        oscillation_emit = step["oscillation_candidate"] & (
            (step["sample_index"] - acc[self.last_oscillation_index]) >= oscillation_refractory_samples
        )
        slope_candidate_sign = step["slope_candidate_sign"]
        slope_candidate_abs_delta = F.abs(F.coalesce(step["delta"], F.lit(0.0)))
        slope_run_reset = slope_candidate_sign == F.lit(0)
        slope_run_started = (slope_candidate_sign != F.lit(0)) & (slope_candidate_sign != acc[self.slope_run_sign])
        slope_run_length = (
            F.when(slope_run_reset, F.lit(0).cast("long"))
            .when(slope_run_started, F.lit(1).cast("long"))
            .otherwise(acc[self.slope_run_length] + F.lit(1).cast("long"))
        )
        slope_run_peak_abs_delta = (
            F.when(slope_run_reset, F.lit(0.0))
            .when(slope_run_started, slope_candidate_abs_delta)
            .otherwise(F.greatest(acc[self.slope_run_peak_abs_delta], slope_candidate_abs_delta))
        )
        slope_run_emitted_before = (
            F.when(slope_run_reset | slope_run_started, F.lit(False))
            .otherwise(acc[self.slope_run_emitted])
        )
        slope_last_emitted_peak_abs_delta = (
            F.when(slope_run_reset | slope_run_started, F.lit(0.0))
            .otherwise(acc[self.slope_run_last_emitted_peak_abs_delta])
        )
        effective_reemit_ratio = F.greatest(slope_reemit_ratio, F.lit(1.0))
        slope_run_ready = (slope_candidate_sign != F.lit(0)) & (slope_run_length >= slope_min_persistence_samples)
        slope_strengthened = slope_run_emitted_before & (
            slope_run_peak_abs_delta >= slope_last_emitted_peak_abs_delta * effective_reemit_ratio
        )
        slope_emit = slope_run_ready & (~slope_run_emitted_before | slope_strengthened)
        slope_emission_reason = (
            F.when(slope_run_emitted_before, F.lit("run_strengthen")).otherwise(F.lit("run_start"))
        )

        switch_event = SwitchEvent().optional_from_step(condition=switch_emit, step=step)
        slope_positive_event = SlopePositiveEvent().struct_from_observation(
            tail_id=step["tail_id"],
            flight_id=step["flight_id"],
            timestamp_utc=step["timestamp_utc"],
            parameter_name=step["parameter_name"],
            delta=step["delta"],
            delta_raw=step["delta_raw"],
            value=step["val"],
            slope_source=slope_source,
            effective_threshold=step["effective_slope_threshold"],
            run_length=slope_run_length,
            run_peak_delta=slope_run_peak_abs_delta,
            emission_reason=slope_emission_reason,
            date_utc=step["date_utc"],
            win_id=F.lit(None).cast("long"),
        )
        slope_negative_event = SlopeNegativeEvent().struct_from_observation(
            tail_id=step["tail_id"],
            flight_id=step["flight_id"],
            timestamp_utc=step["timestamp_utc"],
            parameter_name=step["parameter_name"],
            delta=step["delta"],
            delta_raw=step["delta_raw"],
            value=step["val"],
            slope_source=slope_source,
            effective_threshold=step["effective_slope_threshold"],
            run_length=slope_run_length,
            run_peak_delta=slope_run_peak_abs_delta,
            emission_reason=slope_emission_reason,
            date_utc=step["date_utc"],
            win_id=F.lit(None).cast("long"),
        )
        slope_event = (
            F.when(
                slope_emit,
                F.when(slope_candidate_sign > F.lit(0), slope_positive_event).otherwise(slope_negative_event),
            )
            .otherwise(null_detected_event())
        )
        drift_event = DriftGuardEvent().optional_from_step(
            condition=drift_emit,
            step=step,
            reason=F.when(drift_change_emit, F.lit("abs_change"))
            .when(drift_gap_emit, F.lit("max_gap"))
            .otherwise(F.lit(None).cast("string")),
            cum_abs_change=cum_abs,
            samples_since_guard=step["sample_index"] - acc[self.last_drift_guard_index],
        )
        extrema_event = ExtremaEvent().optional_from_step(condition=extrema_emit, step=step)
        oscillation_event = OscillationEvent().optional_from_step(
            condition=oscillation_emit,
            step=step,
            oscillation_window=oscillation_window,
        )
        emitted_events = append_detected_events(
            acc[self.emitted_events],
            slope_event,
            switch_event,
            drift_event,
            extrema_event,
            oscillation_event,
        )
        return F.struct(
            F.when(switch_emit, step["sample_index"]).otherwise(acc[self.last_switch_index]).alias(self.last_switch_index),
            F.when(oscillation_emit, step["sample_index"])
            .otherwise(acc[self.last_oscillation_index])
            .alias(self.last_oscillation_index),
            F.when(drift_emit, step["sample_index"])
            .otherwise(acc[self.last_drift_guard_index])
            .alias(self.last_drift_guard_index),
            F.when(drift_emit, F.lit(0.0)).otherwise(cum_abs).alias(self.drift_guard_cum_abs),
            F.when(slope_run_reset, F.lit(0)).otherwise(slope_candidate_sign).cast("int").alias(self.slope_run_sign),
            slope_run_length.alias(self.slope_run_length),
            slope_run_peak_abs_delta.alias(self.slope_run_peak_abs_delta),
            F.when(slope_run_reset, F.lit(False))
            .when(slope_emit, F.lit(True))
            .otherwise(slope_run_emitted_before)
            .alias(self.slope_run_emitted),
            F.when(slope_run_reset, F.lit(0.0))
            .when(slope_emit, slope_run_peak_abs_delta)
            .otherwise(slope_last_emitted_peak_abs_delta)
            .alias(self.slope_run_last_emitted_peak_abs_delta),
            emitted_events.alias(self.emitted_events),
        )

    def carry_state_column(self, *, state: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.struct(
            state[self.last_switch_index].alias(self.last_switch_index),
            state[self.last_oscillation_index].alias(self.last_oscillation_index),
            state[self.last_drift_guard_index].alias(self.last_drift_guard_index),
            state[self.drift_guard_cum_abs].alias(self.drift_guard_cum_abs),
            state[self.slope_run_sign].alias(self.slope_run_sign),
            state[self.slope_run_length].alias(self.slope_run_length),
            state[self.slope_run_peak_abs_delta].alias(self.slope_run_peak_abs_delta),
            state[self.slope_run_emitted].alias(self.slope_run_emitted),
            state[self.slope_run_last_emitted_peak_abs_delta].alias(self.slope_run_last_emitted_peak_abs_delta),
            empty_detected_event_array().alias(self.emitted_events),
        )


@dataclass(frozen=True)
class ContinuousEventDetector:
    config: ContinuousDetectorConfig = field(default_factory=ContinuousDetectorConfig)
    state_layout: ContinuousSequenceStateLayout = field(default_factory=ContinuousSequenceStateLayout)
    sequence_plan: SegmentedSequencePlan = field(
        default_factory=lambda: SegmentedSequencePlan(
            ordering=SequenceOrderingPolicy(
                key_columns=("tail_id", "flight_id", "parameter_name"),
                order_columns=("sample_seq_id",),
                timestamp_column="timestamp_utc",
                row_number_column="sample_seq_id",
            ),
            policy=_default_event_segment_policy(),
        )
    )

    def _ensure_segmented_source(self, raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        ordering = self.sequence_plan.ordering
        required = {
            *ordering.key_columns,
            *ordering.order_columns,
            ordering.segment_id_column,
            ordering.row_in_segment_column,
        }
        if ordering.row_number_column not in raw_df.columns:
            seed_order_columns = ordering.order_columns if all(column in raw_df.columns for column in ordering.order_columns) else (
                (ordering.timestamp_column,) if ordering.timestamp_column else ("timestamp_utc",)
            )
            order_window = Window.partitionBy(*ordering.key_columns).orderBy(*seed_order_columns)
            raw_df = raw_df.withColumn(ordering.row_number_column, F.row_number().over(order_window).cast("long"))
        if required.issubset(set(raw_df.columns)):
            return raw_df
        return self.sequence_plan.assign_segments(raw_df).rows_df

    def _feature_frame(self, raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        active = self.config
        slope_mode = _normalize_slope_source(active.slope_source)
        alpha = float(active.ema_alpha)
        if not (0.0 < alpha <= 1.0):
            raise ValueError(f"ema_alpha must be in (0, 1], got {active.ema_alpha}")
        if int(active.slope_min_persistence_samples) < 1:
            raise ValueError(
                "slope_min_persistence_samples must be >= 1, "
                f"got {active.slope_min_persistence_samples}"
            )
        if float(active.slope_reemit_ratio) < 1.0:
            raise ValueError(f"slope_reemit_ratio must be >= 1.0, got {active.slope_reemit_ratio}")

        parameter_col = "parameter_name" if "parameter_name" in raw_df.columns else "sensor"
        order_columns = ["sample_seq_id"] if "sample_seq_id" in raw_df.columns else ["timestamp_utc"]
        order_window = Window.partitionBy("tail_id", "flight_id", parameter_col).orderBy(*order_columns)
        prior_window = order_window.rowsBetween(Window.unboundedPreceding, -1)

        source_df = self._ensure_segmented_source(raw_df)
        if "parameter_datatype_normalized" in source_df.columns:
            source_df = source_df.where(F.col("parameter_datatype_normalized") == F.lit(ParameterDataType.NUMERIC.value))
        elif "parameter_datatype" in source_df.columns:
            source_df = source_df.where(
                spark_normalized_parameter_datatype_expr(F.col("parameter_datatype")) == F.lit(ParameterDataType.NUMERIC.value)
            )
        source_df = source_df.where(F.col("val").isNotNull())

        smoothing_span = max(int(round((2.0 / alpha) - 1.0)), 1)
        smoothing_window = order_window.rowsBetween(-(smoothing_span - 1), 0)
        residual_window_span = max(smoothing_span, int(active.warmup_points), 2)
        residual_window = order_window.rowsBetween(-(residual_window_span - 1), 0)
        oscillation_window_span = max(int(active.oscillation_window), 2)
        oscillation_sign_window = order_window.rowsBetween(-(oscillation_window_span - 1), 0)
        oscillation_amplitude_span = max(int(active.oscillation_amplitude_window), 2)
        oscillation_amplitude_window = order_window.rowsBetween(-(oscillation_amplitude_span - 1), 0)
        oscillation_history_span = max(int(active.oscillation_window) * 3, 6)
        oscillation_history_window = order_window.rowsBetween(-(oscillation_history_span - 1), 0)
        oscillation_alpha = float(active.oscillation_ema_alpha)
        if not (0.0 < oscillation_alpha <= 1.0):
            raise ValueError(f"oscillation_ema_alpha must be in (0, 1], got {active.oscillation_ema_alpha}")
        oscillation_smoothing_span = max(int(round((2.0 / oscillation_alpha) - 1.0)), 1)
        oscillation_smoothing_window = order_window.rowsBetween(-(oscillation_smoothing_span - 1), 0)
        period_band_ratio = abs(float(active.oscillation_period_band_ratio))

        return (
            source_df.withColumn(
                "sample_index",
                (
                    F.col("sample_seq_id").cast("long")
                    if "sample_seq_id" in source_df.columns
                    else F.row_number().over(order_window).cast("long")
                ),
            )
            .withColumn("prev_val", F.lag("val").over(order_window))
            .withColumn("smoothed_val", F.avg("val").over(smoothing_window))
            .withColumn("ema_prev", F.lag("smoothed_val").over(order_window))
            .withColumn("delta_raw", F.col("val") - F.col("prev_val"))
            .withColumn("delta", (F.col("smoothed_val") - F.col("ema_prev")) if slope_mode == "ema" else F.col("delta_raw"))
            .withColumn("effective_slope_threshold", F.lit(float(active.slope_abs_threshold)))
            .withColumn(
                "slope_candidate_sign",
                F.when(F.col("delta") > F.col("effective_slope_threshold"), F.lit(1))
                .when(F.col("delta") < -F.col("effective_slope_threshold"), F.lit(-1))
                .otherwise(F.lit(0)),
            )
            .withColumn("residual", F.col("val") - F.col("ema_prev"))
            .withColumn(
                "variance_estimate",
                F.avg(F.pow(F.coalesce(F.col("residual"), F.lit(0.0)), F.lit(2.0))).over(residual_window),
            )
            .withColumn(
                "sigma",
                F.greatest(
                    F.sqrt(F.coalesce(F.col("variance_estimate"), F.lit(0.0))),
                    F.lit(float(active.min_sigma)),
                ),
            )
            .withColumn("warmup_ready", F.col("sample_index") >= F.lit(int(active.warmup_points)))
            .withColumn("delta_abs_avg", F.avg(F.abs(F.coalesce(F.col("delta_raw"), F.lit(0.0)))).over(smoothing_window))
            .withColumn("osc_value", F.avg("val").over(oscillation_smoothing_window))
            .withColumn("osc_prev", F.lag("osc_value").over(order_window))
            .withColumn("osc_next", F.lead("osc_value").over(order_window))
            .withColumn("osc_delta", F.col("osc_value") - F.col("osc_prev"))
            .withColumn(
                "osc_sign",
                F.when(F.col("osc_delta") > 0, F.lit(1))
                .when(F.col("osc_delta") < 0, F.lit(-1))
                .otherwise(F.lit(0)),
            )
            .withColumn("prev_osc_sign", F.lag("osc_sign").over(order_window))
            .withColumn(
                "sign_change_flag",
                F.when(
                    (F.col("osc_sign") != 0)
                    & (F.col("prev_osc_sign") != 0)
                    & (F.col("osc_sign") != F.col("prev_osc_sign")),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .withColumn("sign_changes", F.sum("sign_change_flag").over(oscillation_sign_window))
            .withColumn(
                "local_amplitude",
                F.max("osc_value").over(oscillation_amplitude_window) - F.min("osc_value").over(oscillation_amplitude_window),
            )
            .withColumn(
                "extrema_kind",
                F.when(
                    F.col("osc_prev").isNotNull()
                    & F.col("osc_next").isNotNull()
                    & (F.col("osc_value") > F.col("osc_prev"))
                    & (F.col("osc_value") >= F.col("osc_next")),
                    F.lit("peak"),
                ).when(
                    F.col("osc_prev").isNotNull()
                    & F.col("osc_next").isNotNull()
                    & (F.col("osc_value") < F.col("osc_prev"))
                    & (F.col("osc_value") <= F.col("osc_next")),
                    F.lit("trough"),
                ),
            )
            .withColumn("extrema_flag", F.when(F.col("extrema_kind").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
            .withColumn("extrema_count", F.sum("extrema_flag").over(oscillation_history_window))
            .withColumn(
                "prev_extrema_index",
                F.last(F.when(F.col("extrema_kind").isNotNull(), F.col("sample_index")), ignorenulls=True).over(prior_window),
            )
            .withColumn(
                "prev_extrema_kind",
                F.last(F.when(F.col("extrema_kind").isNotNull(), F.col("extrema_kind")), ignorenulls=True).over(prior_window),
            )
            .withColumn(
                "extrema_interval",
                F.when(F.col("extrema_kind").isNotNull() & F.col("prev_extrema_index").isNotNull(), F.col("sample_index") - F.col("prev_extrema_index")),
            )
            .withColumn("period_mean_samples", F.avg("extrema_interval").over(oscillation_history_window))
            .withColumn("period_std_samples", F.stddev_pop("extrema_interval").over(oscillation_history_window))
            .withColumn(
                "period_cv",
                F.when(
                    F.col("period_mean_samples").isNotNull() & (F.col("period_mean_samples") > 0),
                    F.coalesce(F.col("period_std_samples"), F.lit(0.0)) / F.col("period_mean_samples"),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn("period_ema", F.avg("extrema_interval").over(oscillation_history_window))
            .withColumn(
                "alternation_flag",
                F.when(
                    F.col("extrema_kind").isNotNull()
                    & F.col("prev_extrema_kind").isNotNull()
                    & (F.col("extrema_kind") != F.col("prev_extrema_kind")),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .withColumn("alternation_matches", F.sum("alternation_flag").over(oscillation_history_window))
            .withColumn(
                "alternation_ratio",
                F.when(F.col("extrema_count") > 1, F.col("alternation_matches") / (F.col("extrema_count") - F.lit(1))).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "period_band_ok",
                F.when(
                    F.col("period_ema").isNotNull() & F.col("extrema_interval").isNotNull() & (F.col("period_ema") > 0),
                    (F.col("extrema_interval") >= F.col("period_ema") * F.lit(max(0.0, 1.0 - period_band_ratio)))
                    & (F.col("extrema_interval") <= F.col("period_ema") * F.lit(1.0 + period_band_ratio)),
                ).otherwise(F.lit(True)),
            )
            .withColumn(
                "oscillation_candidate",
                F.col("warmup_ready")
                & F.col("extrema_kind").isNotNull()
                & (F.col("sign_changes") >= F.lit(int(active.oscillation_sign_changes)))
                & (F.col("local_amplitude") >= F.lit(float(active.oscillation_min_amplitude)))
                & (F.col("extrema_count") >= F.lit(int(active.oscillation_min_extrema)))
                & (F.coalesce(F.col("period_mean_samples"), F.lit(0.0)) >= F.lit(float(active.oscillation_min_period_samples)))
                & (F.col("alternation_ratio") >= F.lit(float(active.oscillation_min_alternation_ratio)))
                & (F.col("period_cv").isNull() | (F.col("period_cv") <= F.lit(float(active.oscillation_period_cv_max))) | F.col("period_band_ok"))
            )
        )

    def _build_stateful_events(self, feature_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        active = self.config
        slope_mode = _normalize_slope_source(active.slope_source)
        segment_id_column = self.sequence_plan.ordering.segment_id_column
        key_columns = self.sequence_plan.ordering.key_columns
        switch_scale_candidate = (
            (
                F.abs(F.col("delta_raw"))
                >= F.greatest(
                    F.lit(float(active.switch_min_abs_delta)),
                    F.lit(float(active.switch_delta_scale)) * F.greatest(F.col("delta_abs_avg"), F.lit(float(active.min_sigma))),
                )
            )
            & (F.abs(F.col("residual")) >= F.lit(float(active.switch_residual_z_min)) * F.col("sigma"))
        )
        switch_sigma_candidate = (
            F.abs(F.col("delta_raw"))
            >= F.greatest(
                F.lit(float(active.switch_min_abs_delta)),
                F.lit(float(active.switch_delta_z_threshold)) * F.col("sigma"),
            )
        )
        switch_residual_candidate = F.abs(F.col("residual")) >= F.lit(float(active.switch_z_threshold)) * F.col("sigma")
        feature_df = feature_df.withColumn(
            "switch_candidate",
            (
                F.col("warmup_ready")
                & F.col("ema_prev").isNotNull()
                & (switch_scale_candidate | switch_sigma_candidate | switch_residual_candidate)
            ),
        )
        segment_steps_df = self.sequence_plan.build_segment_steps(
            feature_df,
            step_columns=(
                "tail_id",
                "flight_id",
                "parameter_name",
                "timestamp_utc",
                "date_utc",
                "sample_index",
                "switch_candidate",
                "val",
                "ema_prev",
                "residual",
                "delta",
                "delta_raw",
                "sigma",
                "effective_slope_threshold",
                "slope_candidate_sign",
                "extrema_kind",
                "osc_value",
                "sign_changes",
                "local_amplitude",
                "extrema_count",
                "period_mean_samples",
                "period_cv",
                "period_ema",
                "period_band_ok",
                "alternation_ratio",
                "oscillation_candidate",
            ),
        )
        carry_df: "DataFrame | None" = None
        event_frames: list["DataFrame"] = []
        segment_ids = self.sequence_plan.collect_segment_ids(feature_df.select(*key_columns, segment_id_column).distinct())
        initial_state = self.state_layout.initial_state_column()
        for segment_id in segment_ids:
            current_segments_df = segment_steps_df.where(F.col(segment_id_column) == F.lit(int(segment_id)))
            if carry_df is not None:
                current_segments_df = current_segments_df.join(carry_df, on=list(key_columns), how="left")
            else:
                current_segments_df = current_segments_df.withColumn("carry_state", initial_state)
            aggregated_df = current_segments_df.select(
                *list(key_columns),
                F.col(segment_id_column),
                F.aggregate(
                    F.col("steps"),
                    F.coalesce(F.col("carry_state"), initial_state),
                    lambda acc, step: self.state_layout.state_after_step_column(
                        acc=acc,
                        step=step,
                        switch_refractory_samples=F.lit(int(active.switch_refractory_samples)),
                        oscillation_refractory_samples=F.lit(int(active.oscillation_refractory_samples)),
                        oscillation_window=F.lit(int(active.oscillation_window)),
                        drift_guard_abs_change=F.lit(float(active.drift_guard_abs_change)),
                        emit_extrema_events=F.lit(bool(active.emit_extrema_events)),
                        drift_guard_max_gap_samples=F.lit(int(active.drift_guard_max_gap_samples)),
                        slope_min_persistence_samples=F.lit(int(max(active.slope_min_persistence_samples, 1))),
                        slope_reemit_ratio=F.lit(float(active.slope_reemit_ratio)),
                        slope_source=F.lit(slope_mode),
                    ),
                ).alias("state_after"),
            )
            segment_events_df = (
                aggregated_df.select(F.explode_outer(F.col("state_after.emitted_events")).alias("event"))
                .where(F.col("event").isNotNull())
                .select("event.*")
            )
            event_frames.append(segment_events_df)
            carry_df = aggregated_df.select(
                *list(key_columns),
                self.state_layout.carry_state_column(state=F.col("state_after")).alias("carry_state"),
            )
        if not event_frames:
            spark = feature_df.sparkSession
            return spark.createDataFrame(
                [],
                schema="tail_id string, flight_id string, win_id long, timestamp_utc timestamp, parameter_name string, event_type_detected string, payload map<string,string>, date_utc date",
            )
        events_df = event_frames[0]
        for frame in event_frames[1:]:
            events_df = events_df.unionByName(frame, allowMissingColumns=False)
        return events_df

    def build(self, raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        active = self.config
        effective_threshold = float(active.delta_threshold)
        feature_df = self._feature_frame(raw_df)

        threshold_by_delta = F.lit(False)
        if effective_threshold > 0.0:
            threshold_by_delta = F.col("delta").isNotNull() & (F.abs(F.col("delta")) >= F.lit(effective_threshold))
        threshold_rows = (
            feature_df.where(
                F.col("warmup_ready")
                & F.col("ema_prev").isNotNull()
                & (
                    (F.abs(F.col("residual")) >= F.lit(float(active.residual_z_threshold)) * F.col("sigma"))
                    | threshold_by_delta
                )
            )
            .select(
                ThresholdEvent().struct_from_observation(
                    tail_id=F.col("tail_id"),
                    flight_id=F.col("flight_id"),
                    timestamp_utc=F.col("timestamp_utc"),
                    parameter_name=F.col("parameter_name"),
                    value=F.col("val"),
                    ema_prev=F.col("ema_prev"),
                    residual=F.col("residual"),
                    delta=F.col("delta"),
                    delta_raw=F.col("delta_raw"),
                    sigma=F.col("sigma"),
                    date_utc=F.col("date_utc"),
                    win_id=F.lit(None).cast("long"),
                ).alias("event")
            )
            .select("event.*")
        )
        stateful_rows = self._build_stateful_events(feature_df)
        event_frames: list["DataFrame"] = [threshold_rows, stateful_rows]
        events_df = event_frames[0]
        for frame in event_frames[1:]:
            events_df = events_df.unionByName(frame, allowMissingColumns=False)
        return events_df


def _normalize_slope_source(slope_source: str) -> str:
    source = str(slope_source).strip().lower()
    if source not in {"raw", "ema"}:
        raise ValueError(f"Unsupported slope_source '{slope_source}'. Expected one of: raw, ema")
    return source


@hot_path
def build_continuous_events(
    raw_df: "DataFrame",
    config: ContinuousDetectorConfig | None = None,
) -> "DataFrame":
    return ContinuousEventDetector(config=config if config else ContinuousDetectorConfig()).build(raw_df)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
