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
    "transition_from_phase_id_detected",
    "transition_to_phase_id_detected",
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
    "baseline_source_mode",
    "baseline_window_count",
    "stable_window_count",
    "version",
]

PHASE_LABEL_CENTROIDS_COLUMNS = [
    "tail_id",
    "phase_label",
    "s_w_centroid",
    "labeled_window_count",
    "flight_count",
    "feature_names",
    "selected_sensors_c",
    "selected_event_types",
    "selected_categorical_state_pairs",
    "selected_window_cooccurrence_pairs",
    "backbone_all_sensors",
    "version",
]

PHASE_REFERENCE_MODEL_COLUMNS = [
    "tail_id",
    "flight_id",
    "phase_id_detected",
    "phase_feature_medians",
    "phase_feature_scales",
    "drift_threshold",
    "flight_window_count",
    "stable_window_count_raw",
    "stable_window_count_effective",
    "effective_phase_count",
    "dwell_limit",
    "can_refine_centroids",
    "s_w_centroid",
    "distance_scale",
    "phase_progress_start",
    "phase_progress_end",
    "phase_progress_center",
    "phase_progress_half_width",
    "phase_selected_sensors",
    "phase_selected_event_types",
    "phase_selected_categorical_state_pairs",
    "phase_selected_window_cooccurrence_pairs",
    "selected_sensors_c",
    "backbone_all_sensors",
    "backbone_weights_b",
    "backbone_lambda_ridge",
    "backbone_training_window_count",
    "backbone_version",
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
            T.StructField("transition_from_phase_id_detected", T.IntegerType(), True),
            T.StructField("transition_to_phase_id_detected", T.IntegerType(), True),
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
            T.StructField("baseline_source_mode", T.StringType(), False),
            T.StructField("baseline_window_count", T.IntegerType(), False),
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


def PHASE_LABEL_CENTROIDS_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("phase_label", T.StringType(), False),
            T.StructField("s_w_centroid", T.ArrayType(T.DoubleType(), True), False),
            T.StructField("labeled_window_count", T.IntegerType(), False),
            T.StructField("flight_count", T.IntegerType(), False),
            T.StructField("feature_names", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_sensors_c", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_event_types", T.ArrayType(T.StringType(), False), False),
            T.StructField("selected_categorical_state_pairs", T.ArrayType(T.StringType(), True), False),
            T.StructField("selected_window_cooccurrence_pairs", T.ArrayType(T.StringType(), True), False),
            T.StructField("backbone_all_sensors", T.ArrayType(T.StringType(), False), False),
            T.StructField("version", T.IntegerType(), False),
        ]
    )


def PHASE_REFERENCE_MODEL_SCHEMA():
    from pyspark.sql import types as T

    string_pairs = T.ArrayType(T.ArrayType(T.StringType(), False), False)
    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), False),
            T.StructField("flight_id", T.StringType(), False),
            T.StructField("phase_id_detected", T.IntegerType(), False),
            T.StructField("phase_feature_medians", T.ArrayType(T.DoubleType(), False), False),
            T.StructField("phase_feature_scales", T.ArrayType(T.DoubleType(), False), False),
            T.StructField("drift_threshold", T.DoubleType(), False),
            T.StructField("flight_window_count", T.IntegerType(), False),
            T.StructField("stable_window_count_raw", T.IntegerType(), False),
            T.StructField("stable_window_count_effective", T.IntegerType(), False),
            T.StructField("effective_phase_count", T.IntegerType(), False),
            T.StructField("dwell_limit", T.IntegerType(), False),
            T.StructField("can_refine_centroids", T.BooleanType(), False),
            T.StructField("s_w_centroid", T.ArrayType(T.DoubleType(), False), False),
            T.StructField("distance_scale", T.DoubleType(), False),
            T.StructField("phase_progress_start", T.DoubleType(), False),
            T.StructField("phase_progress_end", T.DoubleType(), False),
            T.StructField("phase_progress_center", T.DoubleType(), False),
            T.StructField("phase_progress_half_width", T.DoubleType(), False),
            T.StructField("phase_selected_sensors", T.ArrayType(T.StringType(), False), False),
            T.StructField("phase_selected_event_types", T.ArrayType(T.StringType(), False), False),
            T.StructField("phase_selected_categorical_state_pairs", string_pairs, False),
            T.StructField("phase_selected_window_cooccurrence_pairs", string_pairs, False),
            T.StructField("selected_sensors_c", T.ArrayType(T.StringType(), False), False),
            T.StructField("backbone_all_sensors", T.ArrayType(T.StringType(), False), False),
            T.StructField("backbone_weights_b", T.ArrayType(T.ArrayType(T.DoubleType(), False), False), False),
            T.StructField("backbone_lambda_ridge", T.DoubleType(), False),
            T.StructField("backbone_training_window_count", T.IntegerType(), False),
            T.StructField("backbone_version", T.IntegerType(), False),
            T.StructField("version", T.IntegerType(), False),
        ]
    )
