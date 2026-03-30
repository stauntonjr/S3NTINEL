WINDOWS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "t_start",
    "t_end",
    "duration_ms",
    "event_count",
    "sensor_count",
    "event_type_counts",
    "zoh_snapshot",
    "close_reason",
    "zoh_version",
    "date_utc",
]

WINDOW_POLICY_PROFILE_COLUMNS = [
    "profile_id",
    "profile_scope",
    "candidate_rank",
    "is_selected",
    "max_ms",
    "event_threshold",
    "min_ms",
    "inactivity_timeout_ms",
    "objective_score",
    "balance_penalty",
    "predicted_window_count",
    "mean_duration_ms",
    "p95_duration_ms",
    "mean_event_count",
    "p95_event_count",
    "mean_sensor_count",
    "p95_sensor_count",
    "mean_event_type_count",
    "p95_event_type_count",
    "event_threshold_close_rate",
    "max_ms_close_rate",
    "event_threshold_plus_max_ms_close_rate",
    "end_of_stream_close_rate",
    "pair_cost_proxy",
    "same_window_pair_expansion_proxy",
    "sampled_event_count",
    "sampled_flight_count",
]

def WINDOWS_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("win_id", T.LongType(), True),
            T.StructField("t_start", T.TimestampType(), True),
            T.StructField("t_end", T.TimestampType(), True),
            T.StructField("duration_ms", T.IntegerType(), False),
            T.StructField("event_count", T.IntegerType(), True),
            T.StructField("sensor_count", T.IntegerType(), False),
            T.StructField("event_type_counts", T.MapType(T.StringType(), T.IntegerType(), True), False),
            T.StructField("zoh_snapshot", T.MapType(T.StringType(), T.StringType(), True), False),
            T.StructField("close_reason", T.StringType(), True),
            T.StructField("zoh_version", T.IntegerType(), False),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )


def WINDOW_X_SCHEMA():
    from pyspark.sql import types as T

    int_map = T.MapType(T.StringType(), T.IntegerType(), True)
    double_map = T.MapType(T.StringType(), T.DoubleType(), True)
    string_map = T.MapType(T.StringType(), T.StringType(), True)
    continuous_event_summary = T.StructType(
        [
            T.StructField("slope_run_count_by_parameter", int_map, False),
            T.StructField("slope_reinforcement_count_by_parameter", int_map, False),
            T.StructField("slope_signed_impulse_by_parameter", double_map, False),
            T.StructField("slope_abs_impulse_by_parameter", double_map, False),
            T.StructField("slope_peak_abs_delta_by_parameter", double_map, False),
            T.StructField("switch_count_by_parameter", int_map, False),
            T.StructField("threshold_count_by_parameter", int_map, False),
            T.StructField("oscillation_count_by_parameter", int_map, False),
            T.StructField("drift_guard_count_by_parameter", int_map, False),
        ]
    )
    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("win_id", T.LongType(), True),
            T.StructField("t_start", T.TimestampType(), True),
            T.StructField("t_end", T.TimestampType(), True),
            T.StructField("duration_ms", T.IntegerType(), False),
            T.StructField("event_count", T.IntegerType(), True),
            T.StructField("date_utc", T.DateType(), True),
            T.StructField("event_type_counts", int_map, False),
            T.StructField("continuous_event_summary", continuous_event_summary, False),
            T.StructField("continuous_vector_t_end", double_map, False),
            T.StructField("continuous_vector_t_end_scaled", double_map, False),
            T.StructField("categorical_state_t_end", string_map, False),
            T.StructField("drift_magnitude_profiled", T.DoubleType(), False),
            T.StructField("phase_label", T.StringType(), True),
        ]
    )


def WINDOW_POLICY_PROFILE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("profile_id", T.StringType(), False),
            T.StructField("profile_scope", T.StringType(), False),
            T.StructField("candidate_rank", T.IntegerType(), False),
            T.StructField("is_selected", T.BooleanType(), False),
            T.StructField("max_ms", T.IntegerType(), False),
            T.StructField("event_threshold", T.IntegerType(), False),
            T.StructField("min_ms", T.IntegerType(), False),
            T.StructField("inactivity_timeout_ms", T.IntegerType(), False),
            T.StructField("objective_score", T.DoubleType(), False),
            T.StructField("balance_penalty", T.DoubleType(), False),
            T.StructField("predicted_window_count", T.LongType(), False),
            T.StructField("mean_duration_ms", T.DoubleType(), False),
            T.StructField("p95_duration_ms", T.DoubleType(), False),
            T.StructField("mean_event_count", T.DoubleType(), False),
            T.StructField("p95_event_count", T.DoubleType(), False),
            T.StructField("mean_sensor_count", T.DoubleType(), False),
            T.StructField("p95_sensor_count", T.DoubleType(), False),
            T.StructField("mean_event_type_count", T.DoubleType(), False),
            T.StructField("p95_event_type_count", T.DoubleType(), False),
            T.StructField("event_threshold_close_rate", T.DoubleType(), False),
            T.StructField("max_ms_close_rate", T.DoubleType(), False),
            T.StructField("event_threshold_plus_max_ms_close_rate", T.DoubleType(), False),
            T.StructField("end_of_stream_close_rate", T.DoubleType(), False),
            T.StructField("pair_cost_proxy", T.DoubleType(), False),
            T.StructField("same_window_pair_expansion_proxy", T.DoubleType(), False),
            T.StructField("sampled_event_count", T.LongType(), False),
            T.StructField("sampled_flight_count", T.IntegerType(), False),
        ]
    )
