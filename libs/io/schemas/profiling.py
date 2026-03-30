PARAMETER_DATATYPE_PROFILE_COLUMNS = [
    "parameter_name",
    "parameter_datatype_profiled",
    "total_count",
    "missing_count",
    "missing_rate",
    "numeric_rate",
    "distinct_value_count",
    "num_mean",
    "num_std",
    "num_min",
    "num_max",
    "num_q01",
    "num_q50",
    "num_q99",
    "median_interval_ms",
    "sampling_rate_profiled_hz",
]

CONTINUOUS_SCALING_PROFILE_COLUMNS = [
    "parameter_name",
    "support_count",
    "scaling_q25",
    "scaling_center_median",
    "scaling_q75",
    "scaling_iqr",
]


def PARAMETER_DATATYPE_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("parameter_datatype_profiled", T.StringType(), False),
            T.StructField("total_count", T.LongType(), False),
            T.StructField("missing_count", T.LongType(), False),
            T.StructField("missing_rate", T.DoubleType(), False),
            T.StructField("numeric_rate", T.DoubleType(), False),
            T.StructField("distinct_value_count", T.LongType(), False),
            T.StructField("num_mean", T.DoubleType(), True),
            T.StructField("num_std", T.DoubleType(), True),
            T.StructField("num_min", T.DoubleType(), True),
            T.StructField("num_max", T.DoubleType(), True),
            T.StructField("num_q01", T.DoubleType(), True),
            T.StructField("num_q50", T.DoubleType(), True),
            T.StructField("num_q99", T.DoubleType(), True),
            T.StructField("median_interval_ms", T.DoubleType(), True),
            T.StructField("sampling_rate_profiled_hz", T.DoubleType(), True),
        ]
    )


def CONTINUOUS_SCALING_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("support_count", T.LongType(), False),
            T.StructField("scaling_q25", T.DoubleType(), True),
            T.StructField("scaling_center_median", T.DoubleType(), True),
            T.StructField("scaling_q75", T.DoubleType(), True),
            T.StructField("scaling_iqr", T.DoubleType(), True),
        ]
    )

PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_COLUMNS = [
    "parameter_name",
    "parameter_datatype_profiled",
    "sample_count",
    "profile_window_start_utc",
    "profile_window_end_utc",
    "persistent_run_strength_profiled",
    "run_reinforcement_score_profiled",
    "reversal_rate_profiled",
    "sign_flip_rate_profiled",
    "center_occupancy_profiled",
    "excursion_rate_profiled",
    "excursion_return_ratio_profiled",
    "bound_occupancy_profiled",
    "saturation_rate_profiled",
    "monotone_accumulation_score_profiled",
    "reset_drop_rate_profiled",
    "oscillation_score_profiled",
    "tracking_error_score_profiled",
    "tracking_recovery_score_profiled",
    "lagged_response_score_profiled",
    "transition_rate_profiled",
    "mean_dwell_profiled",
    "dominant_state_ratio_profiled",
    "state_chatter_rate_profiled",
    "discrete_low_cardinality_score_profiled",
    "discrete_low_transition_score_profiled",
    "discrete_dwell_score_profiled",
    "transition_balance_score_profiled",
]

PARAMETER_BEHAVIOR_PROFILE_COLUMNS = [
    "parameter_name",
    "parameter_datatype_profiled",
    "behavior_family_profiled",
    "behavior_profile_confidence",
    "regulated_score_profiled",
    "tracking_score_profiled",
    "inertial_score_profiled",
    "accumulative_score_profiled",
    "discrete_state_score_profiled",
    "mixed_unknown_score_profiled",
    "sample_count",
    "profile_window_start_utc",
    "profile_window_end_utc",
    "persistent_run_strength_profiled",
    "run_reinforcement_score_profiled",
    "reversal_rate_profiled",
    "sign_flip_rate_profiled",
    "center_occupancy_profiled",
    "excursion_rate_profiled",
    "excursion_return_ratio_profiled",
    "bound_occupancy_profiled",
    "saturation_rate_profiled",
    "monotone_accumulation_score_profiled",
    "reset_drop_rate_profiled",
    "oscillation_score_profiled",
    "tracking_error_score_profiled",
    "tracking_recovery_score_profiled",
    "lagged_response_score_profiled",
    "transition_rate_profiled",
    "mean_dwell_profiled",
    "dominant_state_ratio_profiled",
    "state_chatter_rate_profiled",
]

PARAMETER_EVENT_PROFILE_COLUMNS = [
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
]


def PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("parameter_datatype_profiled", T.StringType(), True),
            T.StructField("sample_count", T.LongType(), False),
            T.StructField("profile_window_start_utc", T.TimestampType(), True),
            T.StructField("profile_window_end_utc", T.TimestampType(), True),
            *[T.StructField(name, T.DoubleType(), True) for name in PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_COLUMNS[5:]],
        ]
    )


def PARAMETER_BEHAVIOR_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("parameter_datatype_profiled", T.StringType(), True),
            T.StructField("behavior_family_profiled", T.StringType(), False),
            T.StructField("behavior_profile_confidence", T.DoubleType(), False),
            T.StructField("regulated_score_profiled", T.DoubleType(), False),
            T.StructField("tracking_score_profiled", T.DoubleType(), False),
            T.StructField("inertial_score_profiled", T.DoubleType(), False),
            T.StructField("accumulative_score_profiled", T.DoubleType(), False),
            T.StructField("discrete_state_score_profiled", T.DoubleType(), False),
            T.StructField("mixed_unknown_score_profiled", T.DoubleType(), False),
            T.StructField("sample_count", T.LongType(), False),
            T.StructField("profile_window_start_utc", T.TimestampType(), True),
            T.StructField("profile_window_end_utc", T.TimestampType(), True),
            *[T.StructField(name, T.DoubleType(), True) for name in PARAMETER_BEHAVIOR_PROFILE_COLUMNS[13:]],
        ]
    )


def PARAMETER_EVENT_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("parameter_datatype_profiled", T.StringType(), True),
            T.StructField("sample_count", T.LongType(), False),
            T.StructField("profile_window_start_utc", T.TimestampType(), True),
            T.StructField("profile_window_end_utc", T.TimestampType(), True),
            T.StructField("sampling_rate_profiled_hz", T.DoubleType(), True),
            T.StructField("delta_abs_q50", T.DoubleType(), True),
            T.StructField("delta_abs_q75", T.DoubleType(), True),
            T.StructField("delta_abs_q90", T.DoubleType(), True),
            T.StructField("total_abs_change_profiled", T.DoubleType(), True),
            T.StructField("net_change_abs_profiled", T.DoubleType(), True),
            T.StructField("directionality_ratio_profiled", T.DoubleType(), True),
            T.StructField("run_length_p90_profiled", T.DoubleType(), True),
            T.StructField("delta_scale_rank_profiled", T.DoubleType(), True),
            T.StructField("motion_scale_ratio_profiled", T.DoubleType(), True),
            T.StructField("sign_flip_rate_profiled", T.DoubleType(), True),
            T.StructField("local_extrema_rate_profiled", T.DoubleType(), True),
            T.StructField("repeatability_score_profiled", T.DoubleType(), True),
            T.StructField("drift_score_profiled", T.DoubleType(), True),
            T.StructField("chatter_score_profiled", T.DoubleType(), True),
            T.StructField("smoothness_score_profiled", T.DoubleType(), True),
            T.StructField("recommended_slope_archetype", T.StringType(), True),
            T.StructField("recommended_slope_source", T.StringType(), True),
            T.StructField("recommended_slope_threshold_mode", T.StringType(), True),
            T.StructField("recommended_slope_threshold", T.DoubleType(), True),
            T.StructField("recommended_slope_threshold_quantile", T.DoubleType(), True),
            T.StructField("recommended_slope_threshold_scale", T.DoubleType(), True),
            T.StructField("recommended_slope_threshold_min", T.DoubleType(), True),
            T.StructField("recommended_slope_min_persistence_samples", T.IntegerType(), True),
            T.StructField("recommended_slope_reemit_ratio", T.DoubleType(), True),
            T.StructField("recommended_warmup_points", T.IntegerType(), True),
            T.StructField("recommended_emit_switch", T.BooleanType(), True),
            T.StructField("recommended_emit_oscillation", T.BooleanType(), True),
            T.StructField("recommended_emit_threshold", T.BooleanType(), True),
        ]
    )
