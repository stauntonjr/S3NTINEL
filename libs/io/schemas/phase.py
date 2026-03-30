PHASE_WINDOWS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "t_start",
    "t_end",
    "duration_ms",
    "event_count",
    "phase_id_detected",
    "phase_state_detected",
    "phase_confidence_detected",
    "distance_to_centroid_detected",
    "drift_magnitude",
    "breadth",
    "backbone_reconstruction_error",
    "backbone_residual_by_parameter",
    "x_c",
    "s_w",
    "date_utc",
]

PHASE_BASELINES_COLUMNS = [
    "tail_id",
    "phase_id_detected",
    "phase_name_detected",
    "s_w_centroid",
    "reconstruction_median",
    "reconstruction_mad",
    "distance_median",
    "distance_mad",
    "stable_window_count",
    "version",
]

def PHASE_WINDOWS_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("win_id", T.IntegerType(), True),
            T.StructField("t_start", T.TimestampType(), True),
            T.StructField("t_end", T.TimestampType(), True),
            T.StructField("duration_ms", T.IntegerType(), True),
            T.StructField("event_count", T.IntegerType(), True),
            T.StructField("phase_id_detected", T.IntegerType(), False),
            T.StructField("phase_state_detected", T.StringType(), False),
            T.StructField("phase_confidence_detected", T.DoubleType(), False),
            T.StructField("distance_to_centroid_detected", T.DoubleType(), True),
            T.StructField("drift_magnitude", T.DoubleType(), False),
            T.StructField("breadth", T.DoubleType(), False),
            T.StructField("backbone_reconstruction_error", T.DoubleType(), True),
            T.StructField("backbone_residual_by_parameter", T.MapType(T.StringType(), T.DoubleType(), True), False),
            T.StructField("x_c", T.ArrayType(T.DoubleType(), False), False),
            T.StructField("s_w", T.ArrayType(T.DoubleType(), True), False),
            T.StructField("date_utc", T.DateType(), True),
            T.StructField("feature_names", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_sensors_c", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_event_types", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_categorical_state_pairs", T.ArrayType(T.StringType(), True), False),
            T.StructField("selected_window_cooccurrence_pairs", T.ArrayType(T.StringType(), True), False),
            T.StructField("backbone_all_sensors", T.ArrayType(T.StringType(), False), False),
        ]
    )


def PHASE_BASELINES_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("phase_id_detected", T.IntegerType(), False),
            T.StructField("phase_name_detected", T.StringType(), False),
            T.StructField("s_w_centroid", T.ArrayType(T.DoubleType(), True), False),
            T.StructField("reconstruction_median", T.DoubleType(), True),
            T.StructField("reconstruction_mad", T.DoubleType(), False),
            T.StructField("distance_median", T.DoubleType(), True),
            T.StructField("distance_mad", T.DoubleType(), False),
            T.StructField("stable_window_count", T.IntegerType(), False),
            T.StructField("feature_names", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_sensors_c", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_event_types", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_categorical_state_pairs", T.ArrayType(T.StringType(), True), False),
            T.StructField("selected_window_cooccurrence_pairs", T.ArrayType(T.StringType(), True), False),
            T.StructField("backbone_all_sensors", T.ArrayType(T.StringType(), False), False),
            T.StructField("backbone_weights_b", T.ArrayType(T.ArrayType(T.DoubleType(), False), False), False),
            T.StructField("version", T.IntegerType(), False),
        ]
    )
