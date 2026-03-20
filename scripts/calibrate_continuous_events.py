"""Bounded pre-harness calibration pass for continuous event controls."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from libs.events import ContinuousEventCalibrationSpec, build_continuous_event_calibration_report_spark
from libs.io.delta import get_spark, read_table
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


def _parse_csv_floats(raw: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in str(raw).split(",") if item.strip())


def _parse_csv_strings(raw: str) -> tuple[str, ...]:
    return tuple(str(item).strip() for item in str(raw).split(",") if str(item).strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a bounded continuous-event calibration sweep")
    parser.add_argument("--slope-sources", default="ema,raw")
    parser.add_argument("--ema-alphas", default="0.2,0.35,0.5")
    parser.add_argument("--slope-abs-thresholds", default="0.0,0.5,1.0")
    parser.add_argument("--report-path", default="", help="Optional explicit output path for the calibration report JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)

    spark = get_spark("s3ntinel.calibrate_continuous_events")
    raw_df = read_table(spark, artifacts.raw_table, fmt=execution.table_format)
    datatype_profile_df = read_table(spark, artifacts.parameter_datatype_profile, fmt=execution.table_format)
    spec = ContinuousEventCalibrationSpec(
        slope_sources=_parse_csv_strings(args.slope_sources),
        ema_alphas=_parse_csv_floats(args.ema_alphas),
        slope_abs_thresholds=_parse_csv_floats(args.slope_abs_thresholds),
        delta_threshold=float(settings.events.delta_threshold),
        window_max_ms=int(settings.windowing.max_ms),
        window_event_threshold=int(settings.windowing.event_threshold),
        window_min_ms=int(settings.windowing.min_ms),
        window_inactivity_timeout_ms=int(settings.windowing.inactivity_timeout_ms),
        window_strategy=str(settings.windowing.strategy),
    )
    report = build_continuous_event_calibration_report_spark(
        raw_df,
        datatype_profile_df=datatype_profile_df,
        spec=spec,
    )
    default_path = Path("reports") / "continuous_event_calibration.json"
    report_path = Path(str(args.report_path).strip()) if str(args.report_path).strip() else default_path
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"report_path": str(report_path), "recommended_candidate": report.get("recommended_candidate")}, indent=2))


if __name__ == "__main__":
    main()
