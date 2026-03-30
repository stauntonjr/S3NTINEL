"""Parameter-level event profiling for detector policy inference."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from libs.io.schemas import PARAMETER_EVENT_PROFILE_SCHEMA
from libs.profiling.profiles import TelemetryProfileSource
from libs.pyspark import Table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


_PROFILE_GAIN_BOUNDS = {
    "low_scale_responsiveness": (0.75, 1.25),
    "repeatability_aggressiveness": (0.75, 1.25),
    "drift_conservatism": (0.75, 1.25),
    "chatter_suppression": (0.75, 1.35),
}


def _bounded_float(value: float, *, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _step_delta(value: float, *, aggressive_threshold: float = 1.1, conservative_threshold: float = 0.9) -> int:
    if float(value) >= aggressive_threshold:
        return -1
    if float(value) <= conservative_threshold:
        return 1
    return 0


@dataclass(frozen=True)
class EventProfileConfig:
    """Base detector settings and generic morphology-policy gains."""

    slope_source: str = "ema"
    slope_threshold_mode: str = "fixed"
    slope_threshold_quantile: float = 0.75
    slope_threshold_scale: float = 0.35
    slope_threshold_min: float = 1e-6
    slope_abs_threshold: float = 2.0
    slope_min_persistence_samples: int = 2
    slope_reemit_ratio: float = 1.5
    warmup_points: int = 4
    low_scale_responsiveness: float = 1.0
    repeatability_aggressiveness: float = 1.0
    drift_conservatism: float = 1.0
    chatter_suppression: float = 1.0

    def resolved(self) -> "EventProfileConfig":
        return EventProfileConfig(
            slope_source=str(self.slope_source),
            slope_threshold_mode=str(self.slope_threshold_mode),
            slope_threshold_quantile=_bounded_float(self.slope_threshold_quantile, lower=0.5, upper=0.95),
            slope_threshold_scale=max(float(self.slope_threshold_min), float(self.slope_threshold_scale)),
            slope_threshold_min=float(self.slope_threshold_min),
            slope_abs_threshold=max(float(self.slope_threshold_min), float(self.slope_abs_threshold)),
            slope_min_persistence_samples=max(1, int(self.slope_min_persistence_samples)),
            slope_reemit_ratio=max(1.0, float(self.slope_reemit_ratio)),
            warmup_points=max(1, int(self.warmup_points)),
            low_scale_responsiveness=_bounded_float(
                self.low_scale_responsiveness,
                lower=_PROFILE_GAIN_BOUNDS["low_scale_responsiveness"][0],
                upper=_PROFILE_GAIN_BOUNDS["low_scale_responsiveness"][1],
            ),
            repeatability_aggressiveness=_bounded_float(
                self.repeatability_aggressiveness,
                lower=_PROFILE_GAIN_BOUNDS["repeatability_aggressiveness"][0],
                upper=_PROFILE_GAIN_BOUNDS["repeatability_aggressiveness"][1],
            ),
            drift_conservatism=_bounded_float(
                self.drift_conservatism,
                lower=_PROFILE_GAIN_BOUNDS["drift_conservatism"][0],
                upper=_PROFILE_GAIN_BOUNDS["drift_conservatism"][1],
            ),
            chatter_suppression=_bounded_float(
                self.chatter_suppression,
                lower=_PROFILE_GAIN_BOUNDS["chatter_suppression"][0],
                upper=_PROFILE_GAIN_BOUNDS["chatter_suppression"][1],
            ),
        )

    def to_payload(self) -> dict[str, float | int | str]:
        return dict(asdict(self.resolved()))


@dataclass(frozen=True)
class ParameterEventProfile(Table):
    """Detector-policy recommendations inferred from raw parameter morphology."""

    @classmethod
    def spark_schema(cls):
        return PARAMETER_EVENT_PROFILE_SCHEMA()

    @classmethod
    def from_raw_input(
        cls,
        raw_input_df: "DataFrame",
        *,
        datatype_profile_df: "DataFrame",
        config: EventProfileConfig | None = None,
    ) -> "ParameterEventProfile":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        active = (config if config is not None else EventProfileConfig()).resolved()
        source = TelemetryProfileSource(raw_input_df)
        timestamp_column = source.resolved_timestamp_column()
        numeric_parameters_df = datatype_profile_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col("parameter_datatype_profiled").cast("string").alias("parameter_datatype_profiled"),
            F.col("sampling_rate_profiled_hz").cast("double").alias("sampling_rate_profiled_hz"),
        ).where(F.col("parameter_datatype_profiled").isin("numeric", "constant"))

        numeric_df = (
            raw_input_df.select(
                F.col("parameter_name").cast("string").alias("parameter_name"),
                F.col(timestamp_column).cast("timestamp").alias("timestamp_utc"),
                source.numeric_value_column("parameter_value").alias("value_num"),
            )
            .join(F.broadcast(numeric_parameters_df), on="parameter_name", how="inner")
            .where(F.col("timestamp_utc").isNotNull() & F.col("value_num").isNotNull())
        )

        order_window = Window.partitionBy("parameter_name").orderBy("timestamp_utc")
        low_scale_responsiveness = float(active.low_scale_responsiveness)
        repeatability_aggressiveness = float(active.repeatability_aggressiveness)
        drift_conservatism = float(active.drift_conservatism)
        chatter_suppression = float(active.chatter_suppression)

        def _archetype_literal(mapping: dict[str, float | int | str], *, default: float | int | str):
            entries = []
            for archetype_name, literal_value in mapping.items():
                entries.extend((F.lit(str(archetype_name)), F.lit(literal_value)))
            return F.coalesce(F.create_map(*entries)[F.col("recommended_slope_archetype")], F.lit(default))

        numeric_features_df = (
            numeric_df.withColumn("prev_value", F.lag("value_num").over(order_window))
            .withColumn("next_value", F.lead("value_num").over(order_window))
            .withColumn("delta_raw", F.col("value_num") - F.col("prev_value"))
            .withColumn("abs_delta_raw", F.abs(F.col("delta_raw")))
            .withColumn(
                "delta_sign",
                F.when(F.col("delta_raw") > 0, F.lit(1))
                .when(F.col("delta_raw") < 0, F.lit(-1))
                .otherwise(F.lit(0)),
            )
            .withColumn("prev_delta_sign", F.lag("delta_sign").over(order_window))
            .withColumn(
                "sign_flip_flag",
                F.when(
                    (F.col("delta_sign") != 0)
                    & (F.col("prev_delta_sign") != 0)
                    & (F.col("delta_sign") != F.col("prev_delta_sign")),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "extrema_flag",
                F.when(
                    F.col("prev_value").isNotNull()
                    & F.col("next_value").isNotNull()
                    & (
                        ((F.col("value_num") > F.col("prev_value")) & (F.col("value_num") >= F.col("next_value")))
                        | ((F.col("value_num") < F.col("prev_value")) & (F.col("value_num") <= F.col("next_value")))
                    ),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "sign_run_start",
                F.when(
                    (F.col("delta_sign") != 0)
                    & (
                        F.col("prev_delta_sign").isNull()
                        | (F.col("prev_delta_sign") == 0)
                        | (F.col("delta_sign") != F.col("prev_delta_sign"))
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .withColumn(
                "sign_run_id",
                F.when(
                    F.col("delta_sign") != 0,
                    F.sum("sign_run_start").over(order_window),
                ),
            )
        )
        sign_run_window = Window.partitionBy("parameter_name", "sign_run_id")
        numeric_features_df = numeric_features_df.withColumn(
            "run_length_samples",
            F.when(F.col("sign_run_id").isNotNull(), F.count(F.lit(1)).over(sign_run_window)).cast("double"),
        )

        numeric_profile_df = numeric_features_df.groupBy("parameter_name").agg(
            F.first("parameter_datatype_profiled", ignorenulls=True).alias("parameter_datatype_profiled"),
            F.first("sampling_rate_profiled_hz", ignorenulls=True).alias("sampling_rate_profiled_hz"),
            F.count(F.lit(1)).cast("long").alias("sample_count"),
            F.min("timestamp_utc").alias("profile_window_start_utc"),
            F.max("timestamp_utc").alias("profile_window_end_utc"),
            F.sum(F.coalesce(F.col("abs_delta_raw"), F.lit(0.0))).cast("double").alias("total_abs_change_profiled"),
            F.abs(F.sum(F.coalesce(F.col("delta_raw"), F.lit(0.0)))).cast("double").alias("net_change_abs_profiled"),
            F.percentile_approx(
                "abs_delta_raw",
                F.array(F.lit(0.5), F.lit(0.75), F.lit(0.9)),
                1000,
            ).alias("delta_abs_quantiles"),
            F.percentile_approx("run_length_samples", F.lit(0.9), 1000).cast("double").alias("run_length_p90_profiled"),
            F.avg("sign_flip_flag").cast("double").alias("sign_flip_rate_profiled"),
            F.avg("extrema_flag").cast("double").alias("local_extrema_rate_profiled"),
        ).select(
            "parameter_name",
            "parameter_datatype_profiled",
            "sample_count",
            "profile_window_start_utc",
            "profile_window_end_utc",
            "sampling_rate_profiled_hz",
            F.col("delta_abs_quantiles").getItem(0).cast("double").alias("delta_abs_q50"),
            F.col("delta_abs_quantiles").getItem(1).cast("double").alias("delta_abs_q75"),
            F.col("delta_abs_quantiles").getItem(2).cast("double").alias("delta_abs_q90"),
            "total_abs_change_profiled",
            "net_change_abs_profiled",
            F.when(
                F.col("total_abs_change_profiled") > F.lit(0.0),
                F.least(
                    F.lit(1.0),
                    F.col("net_change_abs_profiled") / F.col("total_abs_change_profiled"),
                ),
            )
            .otherwise(F.lit(0.0))
            .cast("double")
            .alias("directionality_ratio_profiled"),
            F.coalesce(F.col("run_length_p90_profiled"), F.lit(0.0)).cast("double").alias("run_length_p90_profiled"),
            "sign_flip_rate_profiled",
            "local_extrema_rate_profiled",
        )

        scale_rank_window = Window.orderBy(F.col("delta_abs_q90").asc_nulls_last())
        enriched_numeric_df = (
            numeric_profile_df.withColumn(
                "delta_scale_rank_profiled",
                F.coalesce(F.percent_rank().over(scale_rank_window), F.lit(0.0)),
            )
            .withColumn(
                "motion_scale_ratio_profiled",
                F.when(
                    F.coalesce(F.col("delta_abs_q50"), F.lit(0.0)) > F.lit(float(active.slope_threshold_min)),
                    F.col("delta_abs_q90") / F.greatest(F.col("delta_abs_q50"), F.lit(float(active.slope_threshold_min))),
                )
                .otherwise(F.lit(1.0))
                .cast("double"),
            )
            .withColumn(
                "run_length_score_profiled",
                F.least(F.lit(1.0), F.coalesce(F.col("run_length_p90_profiled"), F.lit(0.0)) / F.lit(8.0)),
            )
            .withColumn(
                "reversal_density_profiled",
                F.greatest(
                    F.coalesce(F.col("sign_flip_rate_profiled"), F.lit(0.0)),
                    F.coalesce(F.col("local_extrema_rate_profiled"), F.lit(0.0)),
                ),
            )
            .withColumn(
                "repeatability_score_profiled",
                F.least(
                    F.lit(1.0),
                    (F.lit(1.0) - F.col("delta_scale_rank_profiled"))
                    * F.col("reversal_density_profiled")
                    * (F.lit(0.35) + F.lit(0.65) * F.col("run_length_score_profiled")),
                ),
            )
            .withColumn(
                "drift_score_profiled",
                F.least(
                    F.lit(1.0),
                    (F.lit(0.4) + F.lit(0.6) * F.col("delta_scale_rank_profiled"))
                    * F.coalesce(F.col("directionality_ratio_profiled"), F.lit(0.0))
                    * (F.lit(1.0) - F.lit(0.7) * F.col("reversal_density_profiled")),
                ),
            )
            .withColumn(
                "chatter_score_profiled",
                F.least(
                    F.lit(1.0),
                    F.col("reversal_density_profiled")
                    * (F.lit(1.0) - F.lit(0.7) * F.col("run_length_score_profiled"))
                    * (F.lit(1.0) - F.lit(0.5) * F.coalesce(F.col("directionality_ratio_profiled"), F.lit(0.0))),
                ),
            )
            .withColumn(
                "smoothness_score_profiled",
                F.greatest(
                    F.lit(0.0),
                    F.lit(1.0)
                    - F.greatest(
                        F.col("repeatability_score_profiled"),
                        F.col("drift_score_profiled"),
                        F.col("chatter_score_profiled"),
                    ),
                ),
            )
        )
        responsive_threshold_gain = 1.0 / low_scale_responsiveness
        repeatable_threshold_gain = 1.0 / repeatability_aggressiveness
        drift_threshold_gain = drift_conservatism
        chatter_threshold_gain = chatter_suppression
        responsive_step_delta = _step_delta(low_scale_responsiveness)
        repeatable_step_delta = _step_delta(repeatability_aggressiveness)
        drift_step_delta = _step_delta(1.0 / drift_conservatism)
        chatter_step_delta = _step_delta(1.0 / chatter_suppression, aggressive_threshold=1.0 / 0.95)
        source_by_archetype = {
            "responsive_low_scale": "raw",
            "repeatable_low_scale": "raw",
            "meso_drift": "ema",
            "strong_drift": "ema",
            "chattery": "ema",
        }
        threshold_quantile_by_archetype = {
            "meso_drift": 0.75,
            "strong_drift": 0.9,
            "chattery": 0.9,
        }
        threshold_scale_factor_by_archetype = {
            "responsive_low_scale": 0.65 * responsive_threshold_gain,
            "repeatable_low_scale": 0.7 * repeatable_threshold_gain,
            "meso_drift": 0.8 * drift_threshold_gain,
            "strong_drift": 1.5 * drift_threshold_gain,
            "chattery": 1.75 * chatter_threshold_gain,
        }
        threshold_factor_by_archetype = {
            "responsive_low_scale": 0.65 * responsive_threshold_gain,
            "repeatable_low_scale": 0.75 * repeatable_threshold_gain,
            "meso_drift": 0.75 * drift_threshold_gain,
            "strong_drift": 1.15 * drift_threshold_gain,
            "chattery": 1.25 * chatter_threshold_gain,
        }
        persistence_offset_by_archetype = {
            "responsive_low_scale": responsive_step_delta,
            "repeatable_low_scale": repeatable_step_delta,
            "meso_drift": drift_step_delta,
            "strong_drift": 1 + drift_step_delta,
            "chattery": 2 + chatter_step_delta,
        }
        reemit_ratio_by_archetype = {
            "responsive_low_scale": float(active.slope_reemit_ratio) / low_scale_responsiveness,
            "repeatable_low_scale": float(active.slope_reemit_ratio) / repeatability_aggressiveness,
            "meso_drift": float(active.slope_reemit_ratio) * drift_conservatism,
            "strong_drift": (float(active.slope_reemit_ratio) + 0.25) * drift_conservatism,
            "chattery": (float(active.slope_reemit_ratio) + 0.5) * chatter_suppression,
        }
        warmup_offset_by_archetype = {
            "responsive_low_scale": -2 + responsive_step_delta,
            "repeatable_low_scale": repeatable_step_delta,
            "meso_drift": -1 + drift_step_delta,
            "strong_drift": 1 + drift_step_delta,
        }
        ema_default_warmup_points = max(int(active.warmup_points) + chatter_step_delta, 2)

        numeric_recommendations_df = (
            enriched_numeric_df.withColumn(
                "recommended_slope_archetype",
                F.when(
                    (F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) < F.lit(1.0))
                    & (F.col("reversal_density_profiled") < F.lit(0.55))
                    & (F.coalesce(F.col("repeatability_score_profiled"), F.lit(0.0)) < F.lit(0.25))
                    & (F.coalesce(F.col("run_length_p90_profiled"), F.lit(0.0)) <= F.lit(120.0)),
                    F.lit("responsive_low_scale"),
                )
                .when(F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) < F.lit(1.0), F.lit("repeatable_low_scale"))
                .when(
                    (F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) >= F.lit(5.0))
                    & (F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) < F.lit(25.0))
                    & (F.coalesce(F.col("delta_scale_rank_profiled"), F.lit(0.0)) >= F.lit(0.85)),
                    F.lit("meso_drift"),
                )
                .when(
                    (F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) >= F.lit(25.0))
                    | (
                        (F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) >= F.lit(5.0))
                        & (F.coalesce(F.col("sign_flip_rate_profiled"), F.lit(0.0)) <= F.lit(0.15))
                        & (F.coalesce(F.col("local_extrema_rate_profiled"), F.lit(0.0)) <= F.lit(0.15))
                    ),
                    F.lit("strong_drift"),
                )
                .when(
                    (F.coalesce(F.col("delta_abs_q90"), F.lit(0.0)) >= F.lit(1.0))
                    & (
                        (F.coalesce(F.col("sign_flip_rate_profiled"), F.lit(0.0)) >= F.lit(0.4))
                        | (F.coalesce(F.col("local_extrema_rate_profiled"), F.lit(0.0)) >= F.lit(0.4))
                    ),
                    F.lit("chattery"),
                )
                .otherwise(F.lit("smooth_midscale")),
            )
            .withColumn(
                "recommended_slope_source",
                _archetype_literal(source_by_archetype, default=str(active.slope_source)),
            )
            .withColumn("recommended_slope_threshold_mode", F.lit(active.slope_threshold_mode))
            .withColumn(
                "recommended_slope_threshold_quantile",
                _archetype_literal(threshold_quantile_by_archetype, default=float(active.slope_threshold_quantile)),
            )
            .withColumn(
                "recommended_slope_threshold_scale",
                F.greatest(
                    F.lit(float(active.slope_threshold_min)),
                    F.lit(float(active.slope_threshold_scale))
                    * _archetype_literal(threshold_scale_factor_by_archetype, default=1.0),
                ),
            )
            .withColumn("recommended_slope_threshold_min", F.lit(float(active.slope_threshold_min)))
            .withColumn(
                "recommended_slope_threshold",
                F.greatest(
                    F.lit(float(active.slope_threshold_min)),
                    F.coalesce(F.col("delta_abs_q90"), F.col("delta_abs_q75"), F.col("delta_abs_q50"), F.lit(0.0))
                    * F.lit(float(active.slope_abs_threshold))
                    * _archetype_literal(threshold_factor_by_archetype, default=1.0),
                ),
            )
            .withColumn(
                "recommended_slope_min_persistence_samples",
                F.greatest(
                    F.lit(1),
                    F.lit(int(active.slope_min_persistence_samples))
                    + _archetype_literal(persistence_offset_by_archetype, default=1),
                ),
            )
            .withColumn(
                "recommended_slope_reemit_ratio",
                F.greatest(
                    F.lit(1.0),
                    _archetype_literal(reemit_ratio_by_archetype, default=float(active.slope_reemit_ratio) + 0.25),
                ),
            )
            .withColumn(
                "recommended_warmup_points",
                F.when(
                    F.col("recommended_slope_archetype").isin(*tuple(warmup_offset_by_archetype)),
                    F.greatest(
                        F.lit(1),
                        F.lit(int(active.warmup_points))
                        + _archetype_literal(warmup_offset_by_archetype, default=0),
                    ),
                ).otherwise(
                    F.greatest(
                        F.lit(1),
                        F.when(F.col("recommended_slope_source") == F.lit("ema"), F.lit(ema_default_warmup_points)).otherwise(
                            F.lit(int(active.warmup_points))
                        ),
                    )
                ),
            )
            .withColumn("recommended_emit_switch", F.lit(False))
            .withColumn(
                "recommended_emit_oscillation",
                F.lit(False),
            )
            .withColumn("recommended_emit_threshold", F.lit(False))
        )

        all_parameters_df = datatype_profile_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col("parameter_datatype_profiled").cast("string").alias("parameter_datatype_profiled"),
            F.col("sampling_rate_profiled_hz").cast("double").alias("sampling_rate_profiled_hz"),
        )

        final_df = (
            all_parameters_df.join(numeric_recommendations_df, on=["parameter_name", "parameter_datatype_profiled", "sampling_rate_profiled_hz"], how="left")
            .withColumn("sample_count", F.coalesce(F.col("sample_count"), F.lit(0).cast("long")))
            .withColumn("recommended_emit_switch", F.coalesce(F.col("recommended_emit_switch"), F.lit(False)))
            .withColumn("recommended_emit_oscillation", F.coalesce(F.col("recommended_emit_oscillation"), F.lit(False)))
            .withColumn("recommended_emit_threshold", F.coalesce(F.col("recommended_emit_threshold"), F.lit(False)))
            .select(
                "parameter_name",
                "parameter_datatype_profiled",
                "sample_count",
                "profile_window_start_utc",
                "profile_window_end_utc",
                "sampling_rate_profiled_hz",
                "delta_abs_q50",
                "delta_abs_q75",
                "delta_abs_q90",
                "total_abs_change_profiled",
                "net_change_abs_profiled",
                "directionality_ratio_profiled",
                "run_length_p90_profiled",
                "delta_scale_rank_profiled",
                "motion_scale_ratio_profiled",
                "sign_flip_rate_profiled",
                "local_extrema_rate_profiled",
                "repeatability_score_profiled",
                "drift_score_profiled",
                "chatter_score_profiled",
                "smoothness_score_profiled",
                "recommended_slope_archetype",
                "recommended_slope_source",
                "recommended_slope_threshold_mode",
                "recommended_slope_threshold",
                "recommended_slope_threshold_quantile",
                "recommended_slope_threshold_scale",
                "recommended_slope_threshold_min",
                "recommended_slope_min_persistence_samples",
                "recommended_slope_reemit_ratio",
                "recommended_warmup_points",
                "recommended_emit_switch",
                "recommended_emit_oscillation",
                "recommended_emit_threshold",
            )
        )
        return cls(dataframe=final_df)
