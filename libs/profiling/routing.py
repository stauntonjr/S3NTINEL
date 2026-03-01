# File: libs/profiling/routing.py
"""Route parameters into processing channels from profile characteristics."""

from __future__ import annotations


def build_channel_routing(profile_df: "DataFrame", high_refresh_hz: float = 50.0) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        profile_df.withColumn(
            "channel",
            F.when(
                (F.col("detected_type") == F.lit("numeric")) & (F.col("sampling_rate_hz") >= F.lit(float(high_refresh_hz))),
                F.lit("continuous_high_refresh"),
            )
            .when(F.col("detected_type") == F.lit("numeric"), F.lit("continuous"))
            .when(F.col("detected_type").isin("binary", "categorical"), F.lit("categorical"))
            .otherwise(F.lit("meta")),
        )
        .withColumn("continuous_enabled", F.col("channel").isin("continuous", "continuous_high_refresh"))
        .withColumn("categorical_enabled", F.col("channel") == F.lit("categorical"))
        .withColumn(
            "event_policy",
            F.when(F.col("channel").isin("continuous", "continuous_high_refresh"), F.lit("extrema,threshold,slope"))
            .when(F.col("channel") == F.lit("categorical"), F.lit("transition,dwell,illegal"))
            .otherwise(F.lit("none")),
        )
        .select(
            "parameter_name",
            "detected_type",
            "sampling_rate_hz",
            "missing_rate",
            "channel",
            "continuous_enabled",
            "categorical_enabled",
            "event_policy",
        )
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
