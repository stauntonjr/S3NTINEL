"""Phase-behavior diagnostics over simulator telemetry."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

import pandas as pd

from libs.common import SensorDataType, normalize_sensor_datatype


def compute_phase_behavior_diagnostics(
    telemetry_df: pd.DataFrame,
    *,
    top_k: int = 20,
) -> dict[str, Any]:
    frame = telemetry_df.copy()
    frame["sensor"] = frame["sensor"].astype(str)
    frame["phase_name"] = frame["phase_name"].astype(str)
    if "parameter_datatype_label" in frame.columns:
        frame["parameter_datatype_norm"] = frame["parameter_datatype_label"].map(normalize_sensor_datatype)
    else:
        frame["parameter_datatype_norm"] = frame["parameter_datatype"].map(normalize_sensor_datatype)

    numeric = frame[frame["parameter_datatype_norm"] == SensorDataType.NUMERIC.value].copy()
    numeric["value_num"] = pd.to_numeric(numeric.get("parameter_value_clean"), errors="coerce")
    numeric = numeric.dropna(subset=["value_num"])

    continuous_rows: list[dict[str, Any]] = []
    if not numeric.empty:
        global_means = numeric.groupby("sensor")["value_num"].mean()
        global_vars = numeric.groupby("sensor")["value_num"].var(ddof=0).fillna(0.0)
        grouped = numeric.groupby(["sensor", "phase_name"])["value_num"].agg(["count", "mean"]).reset_index()
        for sensor, sensor_rows in grouped.groupby("sensor"):
            total_var = float(global_vars.get(sensor, 0.0))
            if total_var <= 1e-12:
                continue
            global_mean = float(global_means.get(sensor, 0.0))
            between = 0.0
            phase_stats: list[dict[str, Any]] = []
            for row in sensor_rows.itertuples(index=False):
                count = int(row.count)
                mean = float(row.mean)
                between += float(count) * ((mean - global_mean) ** 2)
                phase_stats.append(
                    {
                        "phase_name": str(row.phase_name),
                        "count": count,
                        "mean": mean,
                    }
                )
            eta_sq = between / float(len(numeric[numeric["sensor"] == sensor]) * total_var)
            continuous_rows.append(
                {
                    "sensor": str(sensor),
                    "phase_separation_score": float(eta_sq),
                    "global_mean": global_mean,
                    "phase_means": sorted(phase_stats, key=lambda item: item["phase_name"]),
                }
            )
    continuous_rows.sort(key=lambda item: (-float(item["phase_separation_score"]), item["sensor"]))

    categorical = frame[
        frame["parameter_datatype_norm"].isin(
            [SensorDataType.BINARY.value, SensorDataType.CATEGORICAL.value]
        )
    ].copy()
    categorical["value_text"] = categorical.get("parameter_value").astype(str)
    categorical_rows: list[dict[str, Any]] = []
    if not categorical.empty:
        overall_counts_by_sensor: dict[str, Counter[str]] = defaultdict(Counter)
        phase_counts_by_sensor: dict[str, dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))
        for row in categorical.itertuples(index=False):
            sensor = str(row.sensor)
            phase_name = str(row.phase_name)
            value_text = str(row.value_text)
            overall_counts_by_sensor[sensor][value_text] += 1
            phase_counts_by_sensor[sensor][phase_name][value_text] += 1

        for sensor in sorted(phase_counts_by_sensor.keys()):
            overall = overall_counts_by_sensor[sensor]
            total_overall = float(sum(overall.values()))
            if total_overall <= 0.0:
                continue
            overall_probs = {state: count / total_overall for state, count in overall.items()}
            phase_stats: list[dict[str, Any]] = []
            tv_sum = 0.0
            for phase_name in sorted(phase_counts_by_sensor[sensor].keys()):
                counts = phase_counts_by_sensor[sensor][phase_name]
                total_phase = float(sum(counts.values()))
                if total_phase <= 0.0:
                    continue
                phase_probs = {state: count / total_phase for state, count in counts.items()}
                states = set(overall_probs.keys()) | set(phase_probs.keys())
                tv = 0.5 * sum(abs(float(phase_probs.get(state, 0.0)) - float(overall_probs.get(state, 0.0))) for state in states)
                tv_sum += tv
                dominant_state = counts.most_common(1)[0][0]
                phase_stats.append(
                    {
                        "phase_name": phase_name,
                        "dominant_state": str(dominant_state),
                        "distribution_shift_tv": float(tv),
                    }
                )
            categorical_rows.append(
                {
                    "sensor": sensor,
                    "phase_separation_score": float(tv_sum / float(max(len(phase_stats), 1))),
                    "phase_states": phase_stats,
                }
            )
    categorical_rows.sort(key=lambda item: (-float(item["phase_separation_score"]), item["sensor"]))

    return {
        "continuous_top": continuous_rows[: max(int(top_k), 0)],
        "categorical_top": categorical_rows[: max(int(top_k), 0)],
    }
