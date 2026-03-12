WINDOW_SCORES_RAW_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
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
    "dominant_score_component",
    "subsystem_scores",
    "score_component_scores",
    "date_utc",
]

WINDOW_SCORES_CALIBRATED_COLUMNS = WINDOW_SCORES_RAW_COLUMNS + [
    "warm",
    "emit_ready",
    "min_warm",
]


def WINDOW_SCORES_RAW_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), False),
            T.StructField("flight_id", T.StringType(), False),
            T.StructField("win_id", T.IntegerType(), False),
            T.StructField("phase_state_detected", T.StringType(), False),
            T.StructField("phase_id_detected", T.IntegerType(), False),
            T.StructField("phase_confidence_detected", T.DoubleType(), False),
            T.StructField("distance_to_centroid_detected", T.DoubleType(), True),
            T.StructField("drift_magnitude", T.DoubleType(), False),
            T.StructField("breadth", T.DoubleType(), False),
            T.StructField("global_score", T.DoubleType(), False),
            T.StructField("p_value", T.DoubleType(), False),
            T.StructField("severity", T.StringType(), False),
            T.StructField("dominant_subsystem_id", T.StringType(), True),
            T.StructField("dominant_score_component", T.StringType(), False),
            T.StructField("subsystem_scores", T.MapType(T.StringType(), T.DoubleType(), False), False),
            T.StructField("score_component_scores", T.MapType(T.StringType(), T.DoubleType(), False), False),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )


def WINDOW_SCORES_CALIBRATED_SCHEMA():
    from pyspark.sql import types as T

    fields = list(WINDOW_SCORES_RAW_SCHEMA().fields)
    fields.extend(
        [
            T.StructField("warm", T.BooleanType(), False),
            T.StructField("emit_ready", T.BooleanType(), False),
            T.StructField("min_warm", T.IntegerType(), False),
        ]
    )
    return T.StructType(fields)
