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

EVENTS_SCHEMA = """
tail_id string,
flight_id string,
event_seq_id long,
win_id int,
timestamp_utc timestamp,
parameter_name string,
event_type_detected string,
anomaly_type_detected string,
anomaly_score_detected double,
payload map<string,string>,
date_utc date
"""
