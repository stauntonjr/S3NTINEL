from __future__ import annotations

import pandas as pd

import libs.simulation.reporting as reporting


def test_build_misbehavior_attribution_summary_uses_full_hierarchy_views(monkeypatch):
    captured: dict[str, pd.DataFrame] = {}

    def fake_validate_attribution_against_misbehavior_truth(**kwargs):
        captured["hierarchy_sensor_map_df"] = kwargs["hierarchy_sensor_map_df"]
        captured["hierarchy_label_df"] = kwargs["hierarchy_label_df"]
        return {"status": "ok"}

    monkeypatch.setattr(reporting, "validate_attribution_against_misbehavior_truth", fake_validate_attribution_against_misbehavior_truth)

    hierarchy_rows = pd.DataFrame(
        [
            {
                "parameter_name": "p1",
                "system_id": "SYS_1",
                "subsystem_id": "SUB_1",
                "module_id": "MOD_1",
            }
        ]
    )

    class FakeTables:
        def pandas(self, view):
            if view is reporting.HIERARCHY_SENSOR_MAP_VIEW:
                return hierarchy_rows.copy()
            if view is reporting.HIERARCHY_LABEL_VIEW:
                return hierarchy_rows.copy()
            return pd.DataFrame()

    summary = reporting._build_misbehavior_attribution_summary(FakeTables())

    assert summary == {"status": "ok"}
    assert list(captured["hierarchy_sensor_map_df"].columns) == ["parameter_name", "system_id", "subsystem_id", "module_id"]
    assert list(captured["hierarchy_label_df"].columns) == ["parameter_name", "system_id", "subsystem_id", "module_id"]
