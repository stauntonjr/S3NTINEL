import argparse
from pathlib import Path

from scripts import run_partition_manifest_jobs as jobs


def test_grouped_pipeline_mode_runs_both_grouped_scripts(monkeypatch, tmp_path):
    row = {
        "tail_id": "T001",
        "flight_id": "F001",
        "output_path": "data/synthetic/fleet_partitioned/tail_id=T001/flight_id=F001",
    }
    args = argparse.Namespace(
        jobs_base_dir=str(tmp_path / "jobs"),
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        dry_run=False,
    )

    called = []

    def fake_run_path(path, run_name):
        called.append((Path(path).name, run_name))

    monkeypatch.setattr(jobs.runpy, "run_path", fake_run_path)

    jobs._run_grouped_pipeline_for_row(row, args)

    assert called == [
        ("97_run_fitting_pipeline.py", "__main__"),
        ("98_run_inference_pipeline.py", "__main__"),
    ]
