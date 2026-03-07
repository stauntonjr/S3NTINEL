from libs.common import SensorDataType, normalize_sensor_datatype
from libs.simulation import build_default_sensor_behavior, flatten_hierarchy_spec


def test_normalize_sensor_datatype_aliases():
    assert normalize_sensor_datatype("  BOOLEAN ") == SensorDataType.BINARY.value
    assert normalize_sensor_datatype("integer") == SensorDataType.NUMERIC.value
    assert normalize_sensor_datatype("high-cardinality") == SensorDataType.HIGH_CARDINALITY.value
    assert normalize_sensor_datatype("category") == SensorDataType.CATEGORICAL.value
    assert normalize_sensor_datatype(None) == SensorDataType.UNKNOWN.value


def test_simulation_hierarchy_and_behavior_use_canonical_datatypes():
    hierarchy_spec = {
        "systems": {
            "SYS_X": {
                "subsystems": {
                    "SUB_X": {
                        "modules": {
                            "MOD_X": [
                                {"sensor": "s_bool", "datatype": "Boolean", "unit": "flag"},
                                {"sensor": "s_int", "datatype": " integer ", "unit": "u"},
                                {"sensor": "s_hc", "datatype": "high-cardinality", "unit": "code"},
                            ]
                        }
                    }
                }
            }
        }
    }

    hierarchy_df = flatten_hierarchy_spec(hierarchy_spec)
    by_sensor = {row.sensor: row.parameter_datatype for row in hierarchy_df.itertuples(index=False)}
    assert by_sensor["s_bool"] == SensorDataType.BINARY.value
    assert by_sensor["s_int"] == SensorDataType.NUMERIC.value
    assert by_sensor["s_hc"] == SensorDataType.HIGH_CARDINALITY.value

    behavior = build_default_sensor_behavior(hierarchy_df)
    assert behavior["s_bool"]["datatype"] == SensorDataType.BINARY.value
    assert behavior["s_int"]["datatype"] == SensorDataType.NUMERIC.value
    assert behavior["s_hc"]["datatype"] == SensorDataType.HIGH_CARDINALITY.value
