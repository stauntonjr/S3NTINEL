from pathlib import Path

import pytest

from libs.io.delta import read_table, upsert_table


def test_upsert_table_overwrites_existing_merge_key_row(spark_delta, tmp_path: Path):
    table_path = str(tmp_path / "anomalies_delta")

    first_df = spark_delta.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 7,
                "severity": "low",
                "global_score": 2.0,
                "date_utc": "2026-02-28",
            }
        ]
    )

    second_df = spark_delta.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 7,
                "severity": "high",
                "global_score": 9.5,
                "date_utc": "2026-02-28",
            }
        ]
    )

    upsert_table(
        first_df,
        path=table_path,
        merge_keys=["tail_id", "flight_id", "win_id"],
        fmt="delta",
        partition_by=["tail_id", "flight_id", "date_utc"],
    )
    upsert_table(
        second_df,
        path=table_path,
        merge_keys=["tail_id", "flight_id", "win_id"],
        fmt="delta",
        partition_by=["tail_id", "flight_id", "date_utc"],
    )

    rows = read_table(spark_delta, table_path, fmt="delta").collect()
    assert len(rows) == 1
    assert rows[0]["severity"] == "high"
    assert float(rows[0]["global_score"]) == pytest.approx(9.5, rel=1e-9)
