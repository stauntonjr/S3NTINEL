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

BACKBONE_SCHEMA = """
backbone_version int,
selected_sensors_c array<string>,
all_sensors array<string>,
weights_b array<array<double>>,
lambda_ridge double,
training_window_count int
"""

BACKBONE_SENSOR_ENERGY_SCHEMA = """
parameter_name string,
energy double,
support_count int,
event_prior double,
selection_score double,
selected_backbone boolean,
backbone_version int
"""
