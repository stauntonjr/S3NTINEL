from datetime import datetime

import importlib
import pandas as pd

from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.delta import _jar_list_from_env

ingest_raw = importlib.import_module("pipelines.00_ingest_raw")


def test_pandas_records_for_spark_normalizes_timestamps_and_nested_values():
    frame = pd.DataFrame(
        [
            {
                "tail_id": "T001",
                "t_start": pd.Timestamp("2026-03-01T00:00:00Z"),
                "date_utc": pd.Timestamp("2026-03-01T00:00:00Z").date(),
                "values": [1.0, pd.Timestamp("2026-03-01T00:00:01Z")],
                "meta": {"ts": pd.Timestamp("2026-03-01T00:00:02Z"), "score": 3.5},
            }
        ]
    )

    records = pandas_records_for_spark(frame)

    assert len(records) == 1
    row = records[0]
    assert row["tail_id"] == "T001"
    assert row["t_start"] == datetime(2026, 3, 1, 0, 0, 0)
    assert row["date_utc"].isoformat() == "2026-03-01"
    assert row["values"] == [1.0, datetime(2026, 3, 1, 0, 0, 1)]
    assert row["meta"] == {"ts": datetime(2026, 3, 1, 0, 0, 2), "score": 3.5}
    assert row["t_start"].tzinfo is None


def test_resolve_output_format_falls_back_to_pipeline_table_format(monkeypatch):
    monkeypatch.delenv("S3NTINEL_RAW_OUTPUT_FORMAT", raising=False)
    monkeypatch.setenv("S3NTINEL_TABLE_FORMAT", "parquet")

    assert ingest_raw.resolve_output_format() == "parquet"


def test_resolve_output_format_prefers_stage_specific_override(monkeypatch):
    monkeypatch.setenv("S3NTINEL_TABLE_FORMAT", "parquet")
    monkeypatch.setenv("S3NTINEL_RAW_OUTPUT_FORMAT", "delta")

    assert ingest_raw.resolve_output_format() == "delta"


def test_jar_list_from_env_parses_csv_and_ignores_empty_segments():
    assert _jar_list_from_env(None) == []
    assert _jar_list_from_env("") == []
    assert _jar_list_from_env(" /a.jar , ,/b.jar ") == ["/a.jar", "/b.jar"]
