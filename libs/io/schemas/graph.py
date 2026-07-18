def _types():
    from pyspark.sql import types as T
    return T


PRECISION_GRAPH_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "partial_corr",
    "precision_weight",
    "edge_family",
]

EVENT_GRAPH_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "cooccur_count",
    "event_weight",
    "edge_family",
]

LAG_GRAPH_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "lag_count",
    "lag_weight",
    "mean_lag_seconds",
    "edge_family",
]

LAG_PROFILE_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "lag_band",
    "lag_count",
    "lag_weight",
    "mean_lag_seconds",
    "support_flight_count",
    "edge_family",
]

TRANSITION_GRAPH_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "precedence_count",
    "precedence_weight",
    "edge_family",
]

FUSED_GRAPH_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "precision_weight",
    "event_weight",
    "lag_weight",
    "fused_weight",
    "edge_family",
]

GRAPH_PARAMETER_UNIVERSE_COLUMNS = [
    "parameter_name",
]

HIERARCHY_SENSOR_MAP_COLUMNS = [
    "parameter_name",
    "system_id",
    "subsystem_id",
    "module_id",
    "hierarchy_source",
    "hierarchy_profile_id",
]

HIERARCHY_EDGE_EVIDENCE_COLUMNS = [
    "parameter_name_u",
    "parameter_name_v",
    "rank_parameter_name_u",
    "rank_parameter_name_v",
    "precision_weight",
    "event_weight",
    "lag_weight",
    "fused_weight",
    "module_affinity_weight",
    "lag_count_u_to_v",
    "lag_weight_u_to_v",
    "mean_lag_seconds_u_to_v",
    "lag_count_v_to_u",
    "lag_weight_v_to_u",
    "mean_lag_seconds_v_to_u",
    "system_id",
    "subsystem_id",
    "module_id",
    "hierarchy_edge_role",
]


def PRECISION_GRAPH_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("partial_corr", T.DoubleType(), False),
            T.StructField("precision_weight", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def EVENT_GRAPH_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("cooccur_count", T.IntegerType(), False),
            T.StructField("event_weight", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def LAG_GRAPH_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("lag_count", T.IntegerType(), False),
            T.StructField("lag_weight", T.DoubleType(), False),
            T.StructField("mean_lag_seconds", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def LAG_PROFILE_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("lag_band", T.StringType(), False),
            T.StructField("lag_count", T.IntegerType(), False),
            T.StructField("lag_weight", T.DoubleType(), False),
            T.StructField("mean_lag_seconds", T.DoubleType(), False),
            T.StructField("support_flight_count", T.IntegerType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def TRANSITION_GRAPH_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("precedence_count", T.IntegerType(), False),
            T.StructField("precedence_weight", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def FUSED_GRAPH_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("precision_weight", T.DoubleType(), False),
            T.StructField("event_weight", T.DoubleType(), False),
            T.StructField("lag_weight", T.DoubleType(), False),
            T.StructField("fused_weight", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def GRAPH_PARAMETER_UNIVERSE_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
        ]
    )


def HIERARCHY_SENSOR_MAP_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), True),
            T.StructField("system_id", T.StringType(), True),
            T.StructField("subsystem_id", T.StringType(), True),
            T.StructField("module_id", T.StringType(), True),
            T.StructField("hierarchy_source", T.StringType(), True),
            T.StructField("hierarchy_profile_id", T.StringType(), True),
        ]
    )


def HIERARCHY_EDGE_EVIDENCE_SCHEMA():
    T = _types()
    return T.StructType(
        [
            T.StructField("parameter_name_u", T.StringType(), False),
            T.StructField("parameter_name_v", T.StringType(), False),
            T.StructField("rank_parameter_name_u", T.IntegerType(), False),
            T.StructField("rank_parameter_name_v", T.IntegerType(), False),
            T.StructField("precision_weight", T.DoubleType(), False),
            T.StructField("event_weight", T.DoubleType(), False),
            T.StructField("lag_weight", T.DoubleType(), False),
            T.StructField("fused_weight", T.DoubleType(), False),
            T.StructField("module_affinity_weight", T.DoubleType(), False),
            T.StructField("lag_count_u_to_v", T.IntegerType(), True),
            T.StructField("lag_weight_u_to_v", T.DoubleType(), True),
            T.StructField("mean_lag_seconds_u_to_v", T.DoubleType(), True),
            T.StructField("lag_count_v_to_u", T.IntegerType(), True),
            T.StructField("lag_weight_v_to_u", T.DoubleType(), True),
            T.StructField("mean_lag_seconds_v_to_u", T.DoubleType(), True),
            T.StructField("system_id", T.StringType(), False),
            T.StructField("subsystem_id", T.StringType(), False),
            T.StructField("module_id", T.StringType(), False),
            T.StructField("hierarchy_edge_role", T.StringType(), False),
        ]
    )
