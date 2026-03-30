from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from libs.io.delta import write_table
from libs.reporting import ReportFrame
from libs.io.schemas import (
    EXPLORER_ANOMALY_MARKERS_COLUMNS,
    EXPLORER_ANOMALY_WINDOWS_COLUMNS,
    EXPLORER_EVENT_MARKERS_COLUMNS,
    EXPLORER_PARAMETER_CATALOG_COLUMNS,
    EXPLORER_PHASE_INTERVALS_COLUMNS,
    EXPLORER_TELEMETRY_COLUMNS,
)


EXPLORER_BUNDLE_VERSION = "v1"


@dataclass(frozen=True)
class ExplorerBundle:
    root_dir: Path
    manifest: dict[str, Any]


def explorer_bundle_table_paths(root_dir: str | Path) -> dict[str, Path]:
    root = Path(root_dir)
    return {
        "telemetry": root / "telemetry",
        "parameter_catalog": root / "parameter_catalog",
        "event_markers": root / "event_markers",
        "anomaly_markers": root / "anomaly_markers",
        "anomaly_windows": root / "anomaly_windows",
        "phase_intervals": root / "phase_intervals",
    }


def explorer_bundle_manifest_path(root_dir: str | Path) -> Path:
    return Path(root_dir) / "bundle_manifest.json"


def _prepare_timestamp_df(df: pd.DataFrame) -> pd.DataFrame:
    return ReportFrame(dataframe=df).normalize_timestamps().to_pandas()


def build_explorer_telemetry_spark_table(raw_df: DataFrame, hierarchy_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    from pyspark.sql import functions as F

    hierarchy_cols = ["parameter_name", "system_id", "subsystem_id", "module_id"]
    hierarchy_lookup = hierarchy_df.select(*[c for c in hierarchy_cols if c in hierarchy_df.columns]).dropDuplicates(["parameter_name"])
    value_numeric = F.coalesce(
        F.col("parameter_value_clean").cast("double"),
        F.col("parameter_value").cast("double"),
    )
    base_df = (
        raw_df.select(
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "parameter_value",
            "parameter_value_clean",
            "unit",
            "rate_hz",
            "date_utc",
        )
        .join(hierarchy_lookup, on="parameter_name", how="left")
        .withColumn("plot_value_raw", value_numeric)
    )
    medians = (
        base_df.where(F.col("plot_value_raw").isNotNull())
        .groupBy("parameter_name")
        .agg(F.percentile_approx("plot_value_raw", 0.5, 10000).cast("double").alias("plot_median"))
    )
    with_medians = base_df.join(medians, on="parameter_name", how="left")
    abs_dev = with_medians.withColumn("abs_dev", F.abs(F.col("plot_value_raw") - F.col("plot_median")))
    stats = (
        abs_dev.groupBy("parameter_name")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.min("timestamp_utc").alias("timestamp_min"),
            F.max("timestamp_utc").alias("timestamp_max"),
            F.avg("plot_value_raw").cast("double").alias("plot_mean"),
            F.stddev_samp("plot_value_raw").cast("double").alias("plot_std"),
            F.first("plot_median", ignorenulls=True).cast("double").alias("plot_median"),
            F.percentile_approx("abs_dev", 0.5, 10000).cast("double").alias("plot_mad"),
            F.min("plot_value_raw").cast("double").alias("plot_min"),
            F.max("plot_value_raw").cast("double").alias("plot_max"),
            F.first("system_id", ignorenulls=True).alias("system_id"),
            F.first("subsystem_id", ignorenulls=True).alias("subsystem_id"),
            F.first("module_id", ignorenulls=True).alias("module_id"),
            F.first("unit", ignorenulls=True).alias("unit"),
            F.first("rate_hz", ignorenulls=True).cast("double").alias("rate_hz"),
        )
    )
    telemetry_df = (
        with_medians.join(
            stats.select("parameter_name", "plot_mean", "plot_std", "plot_mad"),
            on="parameter_name",
            how="left",
        )
        .withColumn(
            "plot_value_zscore",
            F.when(
                F.col("plot_value_raw").isNull(),
                F.lit(0.0),
            ).otherwise(
                ((F.col("plot_value_raw") - F.col("plot_mean")) / F.greatest(F.coalesce(F.col("plot_std"), F.lit(0.0)), F.lit(1e-6)))
            ),
        )
        .withColumn(
            "plot_value_robust",
            F.when(
                F.col("plot_value_raw").isNull(),
                F.lit(0.0),
            ).otherwise(
                ((F.col("plot_value_raw") - F.col("plot_median")) / F.greatest(F.coalesce(F.col("plot_mad"), F.lit(0.0)) * F.lit(1.4826), F.lit(1e-6)))
            ),
        )
        .withColumn("plot_value_default", F.col("plot_value_robust"))
        .select(*EXPLORER_TELEMETRY_COLUMNS)
    )
    catalog_df = stats.select(*EXPLORER_PARAMETER_CATALOG_COLUMNS)
    return telemetry_df, catalog_df


def build_explorer_event_markers_spark_table(events_df: DataFrame, anomaly_event_df: DataFrame | None) -> DataFrame:
    from pyspark.sql import functions as F

    base_cols = [
        "tail_id",
        "flight_id",
        "timestamp_utc",
        "parameter_name",
        "event_type_detected",
        "date_utc",
    ]
    event_markers_df = (
        events_df.select(*[c for c in base_cols if c in events_df.columns])
        .withColumn("anomaly_type_detected", F.lit(None).cast("string"))
        .withColumn("anomaly_score_detected", F.lit(None).cast("double"))
        .withColumn("marker_source", F.lit("event"))
        .withColumn("severity", F.lit(None).cast("string"))
        .withColumn("window_global_score", F.lit(None).cast("double"))
        .withColumn("system_id", F.lit(None).cast("string"))
        .withColumn("subsystem_id", F.lit(None).cast("string"))
        .withColumn("module_id", F.lit(None).cast("string"))
    )
    if anomaly_event_df is not None:
        anomaly_markers_df = anomaly_event_df.select(
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "event_type_detected",
            "anomaly_type_detected",
            "anomaly_score_detected",
            "severity",
            "window_global_score",
            "system_id",
            "subsystem_id",
            "module_id",
            "date_utc",
        ).withColumn("marker_source", F.lit("anomaly_event"))
        event_markers_df = event_markers_df.select(*EXPLORER_EVENT_MARKERS_COLUMNS).unionByName(
            anomaly_markers_df.select(*EXPLORER_EVENT_MARKERS_COLUMNS),
            allowMissingColumns=True,
        )
    return event_markers_df.select(*EXPLORER_EVENT_MARKERS_COLUMNS)


def build_explorer_anomaly_markers_spark_table(anomaly_telemetry_df: DataFrame) -> DataFrame:
    return anomaly_telemetry_df.select(*EXPLORER_ANOMALY_MARKERS_COLUMNS)


def build_explorer_anomaly_windows_spark_table(anomaly_window_df: DataFrame) -> DataFrame:
    return anomaly_window_df.select(*EXPLORER_ANOMALY_WINDOWS_COLUMNS)


def build_explorer_phase_intervals_spark_table(phase_windows_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        phase_windows_df.select(
            "tail_id",
            "flight_id",
            F.col("t_start").alias("timestamp_start"),
            F.col("t_end").alias("timestamp_end"),
            F.coalesce(F.col("phase_state_detected"), F.col("phase_id_detected").cast("string")).alias("phase_label"),
            "phase_id_detected",
            "phase_state_detected",
            "date_utc",
        )
        .withColumn("source", F.lit("phase_windows"))
        .select(*EXPLORER_PHASE_INTERVALS_COLUMNS)
    )


def write_explorer_bundle(
    *,
    root_dir: str | Path,
    telemetry_df: DataFrame,
    parameter_catalog_df: DataFrame,
    event_markers_df: DataFrame,
    anomaly_markers_df: DataFrame,
    anomaly_windows_df: DataFrame,
    phase_intervals_df: DataFrame,
    fmt: str,
    mode: str,
    partition_by: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    root = Path(root_dir)
    root.mkdir(parents=True, exist_ok=True)
    table_paths = explorer_bundle_table_paths(root)
    write_table(telemetry_df, path=str(table_paths["telemetry"]), fmt=fmt, mode=mode, partition_by=list(partition_by))
    write_table(parameter_catalog_df, path=str(table_paths["parameter_catalog"]), fmt=fmt, mode=mode)
    write_table(event_markers_df, path=str(table_paths["event_markers"]), fmt=fmt, mode=mode, partition_by=list(partition_by))
    write_table(anomaly_markers_df, path=str(table_paths["anomaly_markers"]), fmt=fmt, mode=mode, partition_by=list(partition_by))
    write_table(anomaly_windows_df, path=str(table_paths["anomaly_windows"]), fmt=fmt, mode=mode, partition_by=list(partition_by))
    write_table(phase_intervals_df, path=str(table_paths["phase_intervals"]), fmt=fmt, mode=mode, partition_by=list(partition_by))
    manifest = {
        "bundle_version": EXPLORER_BUNDLE_VERSION,
        "root_dir": str(root),
        "tables": {name: str(path) for name, path in table_paths.items()},
        "table_format": fmt,
        "counts": {
            "telemetry": int(telemetry_df.count()),
            "parameter_catalog": int(parameter_catalog_df.count()),
            "event_markers": int(event_markers_df.count()),
            "anomaly_markers": int(anomaly_markers_df.count()),
            "anomaly_windows": int(anomaly_windows_df.count()),
            "phase_intervals": int(phase_intervals_df.count()),
        },
    }
    explorer_bundle_manifest_path(root).write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return manifest


def load_explorer_bundle(run_dir_or_bundle: str | Path | Any) -> ExplorerBundle:
    if hasattr(run_dir_or_bundle, "artifact_inventory"):
        artifact = getattr(run_dir_or_bundle, "artifact_inventory", {}).get("explorer_bundle")
        if artifact is None or not artifact.get("exists", False):
            raise FileNotFoundError("explorer_bundle artifact is not available in this run bundle")
        root = Path(str(artifact["path"]))
    else:
        root = Path(run_dir_or_bundle)
        if root.name != "explorer_bundle":
            root = root / "delta" / "explorer_bundle" if (root / "delta").exists() else root
    manifest_path = explorer_bundle_manifest_path(root)
    if not manifest_path.exists():
        raise FileNotFoundError(f"explorer bundle manifest does not exist: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return ExplorerBundle(root_dir=root, manifest=manifest)


def load_explorer_table(bundle: ExplorerBundle, table_name: str, *, columns: list[str] | None = None) -> pd.DataFrame:
    table_path = Path(bundle.manifest["tables"][str(table_name)])
    frame = ReportFrame(dataframe=pd.read_parquet(table_path))
    if columns:
        frame = frame.select_available(columns)
    return frame.normalize_timestamps().to_pandas()


def load_explorer_filter_options(bundle: ExplorerBundle) -> dict[str, list[str]]:
    catalog_df = load_explorer_table(bundle, "parameter_catalog")
    return {
        "system_ids": sorted(str(v) for v in catalog_df.get("system_id", pd.Series(dtype="object")).dropna().unique()),
        "subsystem_ids": sorted(str(v) for v in catalog_df.get("subsystem_id", pd.Series(dtype="object")).dropna().unique()),
        "module_ids": sorted(str(v) for v in catalog_df.get("module_id", pd.Series(dtype="object")).dropna().unique()),
        "parameter_names": sorted(str(v) for v in catalog_df.get("parameter_name", pd.Series(dtype="object")).dropna().unique()),
    }


def load_explorer_slice(bundle: ExplorerBundle, state: Any) -> dict[str, pd.DataFrame]:
    catalog_df = load_explorer_table(bundle, "parameter_catalog")
    filtered_catalog = catalog_df.copy()
    if getattr(state, "system_id", None) and "system_id" in filtered_catalog.columns:
        filtered_catalog = filtered_catalog[filtered_catalog["system_id"] == state.system_id]
    if getattr(state, "subsystem_id", None) and "subsystem_id" in filtered_catalog.columns:
        filtered_catalog = filtered_catalog[filtered_catalog["subsystem_id"] == state.subsystem_id]
    if getattr(state, "module_id", None) and "module_id" in filtered_catalog.columns:
        filtered_catalog = filtered_catalog[filtered_catalog["module_id"] == state.module_id]
    if getattr(state, "parameter_names", ()):
        filtered_catalog = filtered_catalog[filtered_catalog["parameter_name"].isin(list(state.parameter_names))]
    parameter_names = sorted(filtered_catalog.get("parameter_name", pd.Series(dtype="object")).dropna().astype(str).unique().tolist())
    telemetry_df = load_explorer_table(bundle, "telemetry")
    if "plot_value" not in telemetry_df.columns and "plot_value_default" in telemetry_df.columns:
        telemetry_df = telemetry_df.assign(plot_value=telemetry_df["plot_value_default"])
    if parameter_names:
        telemetry_df = telemetry_df[telemetry_df["parameter_name"].isin(parameter_names)]
    if getattr(state, "time_start", None):
        telemetry_df = telemetry_df[telemetry_df["timestamp_utc"] >= pd.Timestamp(state.time_start, tz="UTC")]
    if getattr(state, "time_end", None):
        telemetry_df = telemetry_df[telemetry_df["timestamp_utc"] <= pd.Timestamp(state.time_end, tz="UTC")]
    if telemetry_df.empty:
        return {
            "telemetry": telemetry_df,
            "hierarchy": filtered_catalog,
            "events": pd.DataFrame(),
            "anomaly_event": pd.DataFrame(),
            "anomaly_telemetry": pd.DataFrame(),
            "anomaly_window": pd.DataFrame(),
            "phase_intervals": pd.DataFrame(),
        }
    min_time = telemetry_df["timestamp_utc"].min()
    max_time = telemetry_df["timestamp_utc"].max()

    def _load_and_filter(table_name: str, *, parameter_col: str | None = "parameter_name") -> pd.DataFrame:
        df = load_explorer_table(bundle, table_name)
        if parameter_col and parameter_col in df.columns and parameter_names:
            df = df[df[parameter_col].isin(parameter_names)]
        if "timestamp_utc" in df.columns:
            df = df[(df["timestamp_utc"] >= min_time) & (df["timestamp_utc"] <= max_time)]
        if "timestamp_start" in df.columns and "timestamp_end" in df.columns:
            df = df[(df["timestamp_end"] >= min_time) & (df["timestamp_start"] <= max_time)]
        return df

    return {
        "telemetry": telemetry_df,
        "hierarchy": filtered_catalog,
        "events": _load_and_filter("event_markers"),
        "anomaly_event": _load_and_filter("event_markers"),
        "anomaly_telemetry": _load_and_filter("anomaly_markers"),
        "anomaly_window": _load_and_filter("anomaly_windows", parameter_col=None),
        "phase_intervals": _load_and_filter("phase_intervals", parameter_col=None),
    }


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
