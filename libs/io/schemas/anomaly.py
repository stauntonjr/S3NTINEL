ANOMALY_WINDOW_ATTRIBUTION_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "phase_state_detected",
    "phase_id_detected",
    "phase_confidence_detected",
    "distance_to_centroid_detected",
    "drift_magnitude",
    "breadth",
    "global_score",
    "p_value",
    "severity",
    "dominant_subsystem_id",
    "dominant_module_id",
    "top_subsystem_candidates",
    "top_module_candidates",
    "dominant_score_component",
    "panel_context",
    "subsystems",
    "attribution_context",
    "artifact_versions",
    "date_utc",
]

ANOMALY_TELEMETRY_ATTRIBUTION_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "parameter_name",
    "parameter_value",
    "parameter_datatype_label",
    "system_id",
    "subsystem_id",
    "module_id",
    "window_global_score",
    "severity",
    "parameter_localization_support",
    "parameter_support_rank_in_window",
    "parameter_localization_selected",
    "date_utc",
]

ANOMALY_EVENT_ATTRIBUTION_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "parameter_name",
    "event_type_detected",
    "anomaly_type_detected",
    "anomaly_score_detected",
    "system_id",
    "subsystem_id",
    "module_id",
    "window_global_score",
    "severity",
    "date_utc",
]

def ANOMALY_WINDOW_ATTRIBUTION_SCHEMA():
    from pyspark.sql import types as T

    subsystem_score_component = T.MapType(T.StringType(), T.DoubleType(), True)
    top_sensor = T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), True),
            T.StructField("sensor_score", T.DoubleType(), True),
            T.StructField("event_score", T.DoubleType(), True),
            T.StructField("categorical_event_score", T.DoubleType(), True),
        ]
    )
    subsystem = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("score", T.DoubleType(), True),
            T.StructField("score_component_contrib", subsystem_score_component, True),
            T.StructField("top_sensors", T.ArrayType(top_sensor, True), True),
        ]
    )
    subsystem_candidate = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("support", T.DoubleType(), True),
            T.StructField("best_rank", T.IntegerType(), True),
        ]
    )
    module_candidate = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("subsystem_id", T.StringType(), True),
            T.StructField("support", T.DoubleType(), True),
            T.StructField("best_rank", T.IntegerType(), True),
        ]
    )
    panel_context = T.StructType(
        [
            T.StructField("text", T.ArrayType(T.StringType(), True), True),
            T.StructField("message_codes", T.ArrayType(T.StringType(), True), True),
            T.StructField("source", T.ArrayType(T.StringType(), True), True),
        ]
    )
    attribution_context = T.StructType(
        [
            T.StructField("score_component_scores", T.MapType(T.StringType(), T.DoubleType(), False), True),
            T.StructField("sensor_scores", T.MapType(T.StringType(), T.DoubleType(), True), False),
        ]
    )
    artifact_versions = T.StructType(
        [
            T.StructField("backbone", T.IntegerType(), False),
            T.StructField("graph", T.IntegerType(), False),
            T.StructField("phase", T.IntegerType(), False),
            T.StructField("scoring", T.IntegerType(), False),
            T.StructField("calibration", T.IntegerType(), False),
        ]
    )
    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("win_id", T.IntegerType(), True),
            T.StructField("timestamp_utc", T.TimestampType(), False),
            T.StructField("phase_state_detected", T.StringType(), True),
            T.StructField("phase_id_detected", T.IntegerType(), True),
            T.StructField("phase_confidence_detected", T.DoubleType(), True),
            T.StructField("distance_to_centroid_detected", T.DoubleType(), True),
            T.StructField("drift_magnitude", T.DoubleType(), True),
            T.StructField("breadth", T.DoubleType(), True),
            T.StructField("global_score", T.DoubleType(), True),
            T.StructField("p_value", T.DoubleType(), True),
            T.StructField("severity", T.StringType(), True),
            T.StructField("dominant_subsystem_id", T.StringType(), True),
            T.StructField("dominant_module_id", T.StringType(), True),
            T.StructField("top_subsystem_candidates", T.ArrayType(subsystem_candidate, True), False),
            T.StructField("top_module_candidates", T.ArrayType(module_candidate, True), False),
            T.StructField("dominant_score_component", T.StringType(), True),
            T.StructField("panel_context", panel_context, False),
            T.StructField("subsystems", T.ArrayType(subsystem, True), False),
            T.StructField("attribution_context", attribution_context, False),
            T.StructField("artifact_versions", artifact_versions, False),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )


def ANOMALY_TELEMETRY_ATTRIBUTION_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("win_id", T.IntegerType(), True),
            T.StructField("timestamp_utc", T.TimestampType(), True),
            T.StructField("parameter_name", T.StringType(), True),
            T.StructField("parameter_value", T.StringType(), True),
            T.StructField("parameter_datatype_label", T.StringType(), True),
            T.StructField("system_id", T.StringType(), True),
            T.StructField("subsystem_id", T.StringType(), True),
            T.StructField("module_id", T.StringType(), True),
            T.StructField("window_global_score", T.DoubleType(), True),
            T.StructField("severity", T.StringType(), True),
            T.StructField("parameter_localization_support", T.DoubleType(), True),
            T.StructField("parameter_support_rank_in_window", T.IntegerType(), True),
            T.StructField("parameter_localization_selected", T.BooleanType(), False),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )


def ANOMALY_EVENT_ATTRIBUTION_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("win_id", T.IntegerType(), True),
            T.StructField("timestamp_utc", T.TimestampType(), True),
            T.StructField("parameter_name", T.StringType(), True),
            T.StructField("event_type_detected", T.StringType(), True),
            T.StructField("anomaly_type_detected", T.StringType(), True),
            T.StructField("anomaly_score_detected", T.DoubleType(), True),
            T.StructField("system_id", T.StringType(), True),
            T.StructField("subsystem_id", T.StringType(), True),
            T.StructField("module_id", T.StringType(), True),
            T.StructField("window_global_score", T.DoubleType(), True),
            T.StructField("severity", T.StringType(), True),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )
