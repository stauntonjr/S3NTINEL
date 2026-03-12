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

PARAMETER_BEHAVIOR_PROFILE_COLUMNS = [
    "parameter_name",
    "parameter_datatype_profiled",
    "behavior_family_profiled",
    "behavior_profile_confidence",
    "regulated_score_profiled",
    "inertial_score_profiled",
    "accumulative_score_profiled",
    "discrete_state_score_profiled",
    "mixed_unknown_score_profiled",
    "sample_count",
    "profile_window_start_utc",
    "profile_window_end_utc",
]


def PARAMETER_BEHAVIOR_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("parameter_datatype_profiled", T.StringType(), True),
            T.StructField("behavior_family_profiled", T.StringType(), False),
            T.StructField("behavior_profile_confidence", T.DoubleType(), False),
            T.StructField("regulated_score_profiled", T.DoubleType(), False),
            T.StructField("inertial_score_profiled", T.DoubleType(), False),
            T.StructField("accumulative_score_profiled", T.DoubleType(), False),
            T.StructField("discrete_state_score_profiled", T.DoubleType(), False),
            T.StructField("mixed_unknown_score_profiled", T.DoubleType(), False),
            T.StructField("sample_count", T.LongType(), False),
            T.StructField("profile_window_start_utc", T.TimestampType(), True),
            T.StructField("profile_window_end_utc", T.TimestampType(), True),
        ]
    )
