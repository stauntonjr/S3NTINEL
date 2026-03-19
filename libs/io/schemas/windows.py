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

WINDOWS_SCHEMA = """
tail_id string,
flight_id string,
win_id int,
t_start timestamp,
t_end timestamp,
duration_ms int,
event_count int,
sensor_count int,
event_type_counts map<string,int>,
zoh_snapshot map<string,string>,
close_reason string,
zoh_version int,
date_utc date
"""

WINDOW_X_SCHEMA = """
tail_id string,
flight_id string,
win_id int,
t_start timestamp,
t_end timestamp,
duration_ms int,
event_count int,
date_utc date,
event_type_counts map<string,int>,
continuous_vector_t_end map<string,double>,
continuous_vector_t_end_scaled map<string,double>,
categorical_state_t_end map<string,string>,
drift_magnitude_profiled double,
phase_label string
"""
