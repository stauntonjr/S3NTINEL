from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from libs.io.transforms import normalize_raw_telemetry


def test_normalize_raw_telemetry_preserves_unit_and_rate_hz(spark):
    raw_pdf = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "parameter_name": "aircraft_altitude_ft",
                "parameter_value": "100.0",
                "unit": "ft",
                "rate_hz": 2.0,
            }
        ]
    )

    normalized = normalize_raw_telemetry(spark.createDataFrame(raw_pdf))
    rows = normalized.select("unit", "rate_hz").collect()

    assert len(rows) == 1
    assert rows[0]["unit"] == "ft"
    assert rows[0]["rate_hz"] == 2.0
