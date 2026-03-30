BACKBONE_COLUMNS = [
    "backbone_version",
    "selected_sensors_c",
    "all_sensors",
    "weights_b",
    "lambda_ridge",
    "training_window_count",
]

BACKBONE_SENSOR_ENERGY_COLUMNS = [
    "parameter_name",
    "energy",
    "support_count",
    "event_prior",
    "selection_score",
    "selected_backbone",
    "backbone_version",
]

def BACKBONE_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("backbone_version", T.IntegerType(), True),
            T.StructField("selected_sensors_c", T.ArrayType(T.StringType(), True), True),
            T.StructField("all_sensors", T.ArrayType(T.StringType(), True), True),
            T.StructField("weights_b", T.ArrayType(T.ArrayType(T.DoubleType(), True), True), True),
            T.StructField("lambda_ridge", T.DoubleType(), True),
            T.StructField("training_window_count", T.IntegerType(), True),
        ]
    )


def BACKBONE_SENSOR_ENERGY_SCHEMA():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), True),
            T.StructField("energy", T.DoubleType(), True),
            T.StructField("support_count", T.IntegerType(), False),
            T.StructField("event_prior", T.DoubleType(), True),
            T.StructField("selection_score", T.DoubleType(), True),
            T.StructField("selected_backbone", T.BooleanType(), True),
            T.StructField("backbone_version", T.IntegerType(), True),
        ]
    )
