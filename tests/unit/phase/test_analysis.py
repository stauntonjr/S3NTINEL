from __future__ import annotations

import pandas as pd

from libs.phase import analyze_phase_behavior


def test_phase_behavior_diagnostics_reports_continuous_and_categorical_separation():
    telemetry_df = pd.DataFrame(
        [
            {
                "sensor": "s_num",
                "phase_name": "a",
                "parameter_datatype_label": "numeric",
                "parameter_value": "0.0",
                "parameter_value_clean": "0.0",
            },
            {
                "sensor": "s_num",
                "phase_name": "a",
                "parameter_datatype_label": "numeric",
                "parameter_value": "0.1",
                "parameter_value_clean": "0.1",
            },
            {
                "sensor": "s_num",
                "phase_name": "b",
                "parameter_datatype_label": "numeric",
                "parameter_value": "10.0",
                "parameter_value_clean": "10.0",
            },
            {
                "sensor": "s_num",
                "phase_name": "b",
                "parameter_datatype_label": "numeric",
                "parameter_value": "10.1",
                "parameter_value_clean": "10.1",
            },
            {
                "sensor": "s_cat",
                "phase_name": "a",
                "parameter_datatype_label": "categorical",
                "parameter_value": "OFF",
                "parameter_value_clean": "OFF",
            },
            {
                "sensor": "s_cat",
                "phase_name": "a",
                "parameter_datatype_label": "categorical",
                "parameter_value": "OFF",
                "parameter_value_clean": "OFF",
            },
            {
                "sensor": "s_cat",
                "phase_name": "b",
                "parameter_datatype_label": "categorical",
                "parameter_value": "ON",
                "parameter_value_clean": "ON",
            },
            {
                "sensor": "s_cat",
                "phase_name": "b",
                "parameter_datatype_label": "categorical",
                "parameter_value": "ON",
                "parameter_value_clean": "ON",
            },
        ]
    )

    out = analyze_phase_behavior(telemetry_df, top_k=5)

    assert out["continuous_top"][0]["sensor"] == "s_num"
    assert out["continuous_top"][0]["phase_separation_score"] > 0.9
    assert out["categorical_top"][0]["sensor"] == "s_cat"
    assert out["categorical_top"][0]["phase_separation_score"] > 0.4
