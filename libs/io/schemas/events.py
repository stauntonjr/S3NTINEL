EVENTS_COLUMNS = [
    "tail_id",
    "flight_id",
    "event_seq_id",
    "win_id",
    "timestamp_utc",
    "parameter_name",
    "event_type_detected",
    "anomaly_type_detected",
    "anomaly_score_detected",
    "payload",
    "date_utc",
]

def EVENTS_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("flight_id", T.StringType(), True),
            T.StructField("event_seq_id", T.LongType(), False),
            T.StructField("win_id", T.IntegerType(), True),
            T.StructField("timestamp_utc", T.TimestampType(), True),
            T.StructField("parameter_name", T.StringType(), True),
            T.StructField("event_type_detected", T.StringType(), True),
            T.StructField("anomaly_type_detected", T.StringType(), True),
            T.StructField("anomaly_score_detected", T.DoubleType(), True),
            T.StructField("payload", T.MapType(T.StringType(), T.StringType(), True), False),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )
