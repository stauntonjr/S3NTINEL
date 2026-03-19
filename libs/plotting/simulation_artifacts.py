from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
from plotly.subplots import make_subplots

from libs.plotting.explorer_bundle import load_explorer_bundle, load_explorer_filter_options, load_explorer_slice


DEFAULT_VALIDATION_REPORTS = (
    "profile_validation_summary.json",
    "event_validation_summary.json",
    "label_contract_summary.json",
    "phase_validation_summary.json",
    "hierarchy_validation_summary.json",
    "coupling_validation_summary.json",
    "score_validation_summary.json",
    "misbehavior_score_validation_summary.json",
    "misbehavior_window_validation_summary.json",
    "misbehavior_attribution_validation_summary.json",
    "fault_window_validation_summary.json",
    "attribution_validation_summary.json",
)

DEFAULT_OPTIONAL_SUMMARIES = (
    "profile_pipeline_run_summary.json",
    "structural_pipeline_run_summary.json",
    "pipeline_run_summary.json",
)

LOG_LINE_PATTERN = re.compile(
    r"^(?P<timestamp>\S+)\s+\|\s+(?P<level>\S+)\s+\|\s+(?P<logger>[^|]+)\|\s+(?P<message>.*)$"
)


@dataclass(frozen=True)
class SimulationRunBundlePaths:
    run_dir: Path
    reports_dir: Path
    logs_dir: Path
    delta_dir: Path
    manifest_path: Path
    log_path: Path


@dataclass(frozen=True)
class SimulationRunBundle:
    paths: SimulationRunBundlePaths
    manifest: dict[str, Any] | None
    pipeline_summary: dict[str, Any] | None
    optional_summaries: dict[str, dict[str, Any]]
    validation_reports: dict[str, dict[str, Any] | None]
    artifact_inventory: dict[str, dict[str, Any]]
    log_text: str | None
    missing_files: tuple[str, ...]


@dataclass(frozen=True)
class SensorExplorerState:
    system_id: str | None = None
    subsystem_id: str | None = None
    module_id: str | None = None
    parameter_names: tuple[str, ...] = ()
    scale_mode: str = "normalized"
    show_events: bool = True
    show_anomaly_markers: bool = True
    show_anomaly_windows: bool = True
    show_phase_shading: bool = True
    time_start: str | None = None
    time_end: str | None = None


def discover_latest_run_dir(base_dir: str | Path = "data/simulation_runs") -> Path:
    base_path = Path(base_dir)
    if not base_path.exists():
        raise FileNotFoundError(f"run base directory does not exist: {base_path}")
    candidates = [
        path.parent.parent
        for path in base_path.rglob("reports/run_manifest.json")
        if path.is_file()
    ]
    if not candidates:
        raise FileNotFoundError(f"no run bundles with reports/run_manifest.json found under {base_path}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def build_run_bundle_paths(run_dir: str | Path) -> SimulationRunBundlePaths:
    run_path = Path(run_dir).resolve()
    return SimulationRunBundlePaths(
        run_dir=run_path,
        reports_dir=run_path / "reports",
        logs_dir=run_path / "logs",
        delta_dir=run_path / "delta",
        manifest_path=run_path / "reports" / "run_manifest.json",
        log_path=run_path / "logs" / "run.log",
    )


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _load_text_if_exists(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def _resolve_artifact_path(run_dir: Path, path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    repo_root = run_dir.parents[2]
    repo_candidate = (repo_root / path).resolve()
    if repo_candidate.exists():
        return repo_candidate
    run_candidate = (run_dir / path).resolve()
    if run_candidate.exists():
        return run_candidate
    return repo_candidate


def _artifact_inventory_from_manifest(paths: SimulationRunBundlePaths, manifest: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if isinstance(manifest, dict) and isinstance(manifest.get("artifacts"), dict):
        inventory = {}
        for name, payload in manifest["artifacts"].items():
            if isinstance(payload, dict):
                resolved_path = _resolve_artifact_path(paths.run_dir, str(payload.get("path", "")))
                inventory[str(name)] = {
                    "path": str(resolved_path),
                    "exists": bool(payload.get("exists", False)) and resolved_path.exists(),
                }
        return inventory

    inventory: dict[str, dict[str, Any]] = {}
    if not paths.delta_dir.exists():
        return inventory
    for artifact_dir in sorted(p for p in paths.delta_dir.iterdir() if p.is_dir()):
        inventory[artifact_dir.name] = {"path": str(artifact_dir), "exists": True}
    return inventory


def load_simulation_run_bundle(run_dir: str | Path) -> SimulationRunBundle:
    paths = build_run_bundle_paths(run_dir)
    manifest = _load_json_if_exists(paths.manifest_path)
    pipeline_summary = _load_json_if_exists(paths.reports_dir / "pipeline_run_summary.json")
    optional_summaries = {
        filename: payload
        for filename in DEFAULT_OPTIONAL_SUMMARIES
        if (payload := _load_json_if_exists(paths.reports_dir / filename)) is not None
    }
    validation_reports = {
        filename: _load_json_if_exists(paths.reports_dir / filename)
        for filename in DEFAULT_VALIDATION_REPORTS
    }
    artifact_inventory = _artifact_inventory_from_manifest(paths, manifest)
    log_text = _load_text_if_exists(paths.log_path)

    missing_files = []
    if manifest is None:
        missing_files.append(str(paths.manifest_path))
    if pipeline_summary is None:
        missing_files.append(str(paths.reports_dir / "pipeline_run_summary.json"))
    if log_text is None:
        missing_files.append(str(paths.log_path))
    for filename, payload in validation_reports.items():
        if payload is None:
            missing_files.append(str(paths.reports_dir / filename))

    return SimulationRunBundle(
        paths=paths,
        manifest=manifest,
        pipeline_summary=pipeline_summary,
        optional_summaries=optional_summaries,
        validation_reports=validation_reports,
        artifact_inventory=artifact_inventory,
        log_text=log_text,
        missing_files=tuple(missing_files),
    )


def artifact_availability_table(bundle: SimulationRunBundle) -> pd.DataFrame:
    rows = []
    for artifact_name, payload in sorted(bundle.artifact_inventory.items()):
        rows.append(
            {
                "artifact_name": artifact_name,
                "exists": bool(payload.get("exists", False)),
                "path": str(payload.get("path", "")),
            }
        )
    return pd.DataFrame(rows)


def validation_summary_table(bundle: SimulationRunBundle) -> pd.DataFrame:
    rows = []
    for report_name, payload in bundle.validation_reports.items():
        report_type = report_name.removesuffix(".json")
        if payload is None:
            rows.append({"report_name": report_type, "status": "missing", "summary": "not produced"})
            continue
        status = str(payload.get("status", payload.get("result", "ok")))
        summary = ""
        for key in ("reason", "message", "summary"):
            if key in payload:
                summary = str(payload[key])
                break
        rows.append({"report_name": report_type, "status": status, "summary": summary})
    return pd.DataFrame(rows)


def extract_log_records(log_text: str | None) -> pd.DataFrame:
    if not log_text:
        return pd.DataFrame(columns=["timestamp", "level", "logger", "message"])
    rows = []
    for line in log_text.splitlines():
        match = LOG_LINE_PATTERN.match(line.strip())
        if match is None:
            continue
        rows.append(match.groupdict())
    df = pd.DataFrame(rows)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def load_artifact_table(
    bundle: SimulationRunBundle,
    artifact_name: str,
    *,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    artifact = bundle.artifact_inventory.get(str(artifact_name))
    if artifact is None:
        raise KeyError(f"unknown artifact name: {artifact_name}")
    if not artifact.get("exists", False):
        return pd.DataFrame()
    path = Path(str(artifact["path"]))
    df = pd.read_parquet(path)
    if columns:
        selected = [column for column in columns if column in df.columns]
        df = df[selected]
    if limit is not None:
        df = df.head(int(limit))
    return df


def plot_stage_timings(bundle: SimulationRunBundle):
    summary = bundle.pipeline_summary
    if not isinstance(summary, dict) or not isinstance(summary.get("stages"), list):
        return None
    stage_df = pd.DataFrame(summary["stages"])
    if stage_df.empty or "elapsed_ms" not in stage_df.columns:
        return None
    stage_df = stage_df.copy()
    stage_df["elapsed_sec"] = stage_df["elapsed_ms"].astype(float) / 1000.0
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.barplot(data=stage_df, x="elapsed_sec", y="stage_script", hue="status", dodge=False, ax=ax, palette="crest")
    ax.set_title("Stage timings")
    ax.set_xlabel("elapsed_sec")
    ax.set_ylabel("stage_script")
    return fig


def plot_validation_status(bundle: SimulationRunBundle):
    df = validation_summary_table(bundle)
    if df.empty:
        return None
    order = {"ok": 0, "success": 0, "missing": 1, "failed": 2, "error": 2}
    df = df.copy()
    df["status_rank"] = df["status"].map(lambda value: order.get(str(value), 1))
    df = df.sort_values(["status_rank", "report_name"])
    fig, ax = plt.subplots(figsize=(8, 3))
    sns.countplot(data=df, y="report_name", hue="status", ax=ax, palette="Set2")
    ax.set_title("Validation report status")
    ax.set_xlabel("count")
    ax.set_ylabel("report_name")
    return fig


def plot_fleet_structure(telemetry_df: pd.DataFrame) -> go.Figure | None:
    if telemetry_df.empty:
        return None
    required = {"tail_id", "flight_id", "timestamp_utc"}
    if not required.issubset(telemetry_df.columns):
        return None
    df = _prepare_timestamp_df(telemetry_df)
    flight_counts = (
        df.groupby(["tail_id", "flight_id"], dropna=False)
        .size()
        .rename("row_count")
        .reset_index()
    )
    tail_counts = (
        flight_counts.groupby("tail_id", dropna=False)["row_count"]
        .sum()
        .rename("row_count")
        .reset_index()
    )
    ids = ["fleet"]
    labels = ["fleet"]
    parents = [""]
    values = [int(flight_counts["row_count"].sum())]
    for row in tail_counts.itertuples(index=False):
        tail_id = str(row.tail_id)
        ids.append(f"tail::{tail_id}")
        labels.append(tail_id)
        parents.append("fleet")
        values.append(int(row.row_count))
    for row in flight_counts.itertuples(index=False):
        tail_id = str(row.tail_id)
        flight_id = str(row.flight_id)
        ids.append(f"flight::{tail_id}/{flight_id}")
        labels.append(flight_id)
        parents.append(f"tail::{tail_id}")
        values.append(int(row.row_count))
    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            maxdepth=2,
            hovertemplate="%{label}<br>row_count=%{value}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Fleet / tail / flight row counts",
        template="plotly_white",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_flight_timelines(telemetry_df: pd.DataFrame) -> go.Figure | None:
    if telemetry_df.empty:
        return None
    required = {"tail_id", "flight_id", "timestamp_utc"}
    if not required.issubset(telemetry_df.columns):
        return None
    df = _prepare_timestamp_df(telemetry_df)
    summary_df = (
        df.groupby(["tail_id", "flight_id"], dropna=False)
        .agg(
            timestamp_start=("timestamp_utc", "min"),
            timestamp_end=("timestamp_utc", "max"),
            row_count=("timestamp_utc", "size"),
        )
        .reset_index()
    )
    if summary_df.empty:
        return None
    summary_df["flight_label"] = summary_df["tail_id"].astype(str) + " / " + summary_df["flight_id"].astype(str)
    summary_df["duration_seconds"] = (
        (summary_df["timestamp_end"] - summary_df["timestamp_start"]).dt.total_seconds().fillna(0.0)
    )
    fig = px.timeline(
        summary_df.sort_values(["tail_id", "timestamp_start", "flight_id"]),
        x_start="timestamp_start",
        x_end="timestamp_end",
        y="flight_label",
        color="tail_id",
        hover_data={
            "tail_id": True,
            "flight_id": True,
            "row_count": True,
            "duration_seconds": ":.1f",
            "timestamp_start": True,
            "timestamp_end": True,
            "flight_label": False,
        },
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        title="Flight timelines and row counts",
        template="plotly_white",
        xaxis_title="timestamp_utc",
        yaxis_title="tail / flight",
        legend_title_text="tail_id",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_hierarchy_overview(hierarchy_df: pd.DataFrame):
    if hierarchy_df.empty:
        return None
    fig, axes = plt.subplots(1, 3, figsize=(18, 4))
    sns.countplot(
        data=hierarchy_df,
        x="system_id",
        hue="system_id",
        ax=axes[0],
        palette="Blues",
        legend=False,
    )
    axes[0].tick_params(axis="x", rotation=30)
    axes[0].set_title("Parameters by system")
    sns.countplot(
        data=hierarchy_df,
        x="subsystem_id",
        hue="subsystem_id",
        ax=axes[1],
        palette="Greens",
        legend=False,
    )
    axes[1].tick_params(axis="x", rotation=45)
    axes[1].set_title("Parameters by subsystem")
    module_counts = hierarchy_df.groupby("module_id", as_index=False).size().sort_values("size", ascending=False).head(12)
    sns.barplot(
        data=module_counts,
        x="size",
        y="module_id",
        hue="module_id",
        ax=axes[2],
        palette="mako",
        legend=False,
    )
    axes[2].set_title("Top modules by parameter count")
    axes[2].set_xlabel("parameter_count")
    return fig


def plot_hierarchy_structure(hierarchy_df: pd.DataFrame) -> go.Figure | None:
    if hierarchy_df.empty:
        return None
    required = {"system_id", "subsystem_id", "module_id", "parameter_name"}
    if not required.issubset(hierarchy_df.columns):
        return None
    plot_df = hierarchy_df[list(required)].copy()
    for col in ("system_id", "subsystem_id", "module_id", "parameter_name"):
        plot_df[col] = plot_df[col].fillna("missing").astype(str)
    module_sizes = (
        plot_df.groupby(["system_id", "subsystem_id", "module_id"], dropna=False)
        .size()
        .rename("parameter_count")
        .reset_index()
    )
    system_sizes = (
        module_sizes.groupby("system_id", dropna=False)["parameter_count"].sum().rename("parameter_count").reset_index()
    )
    subsystem_sizes = (
        module_sizes.groupby(["system_id", "subsystem_id"], dropna=False)["parameter_count"]
        .sum()
        .rename("parameter_count")
        .reset_index()
    )
    ids = ["fleet"]
    labels = ["fleet"]
    parents = [""]
    values = [int(module_sizes["parameter_count"].sum())]
    for row in system_sizes.itertuples(index=False):
        ids.append(f"system::{row.system_id}")
        labels.append(row.system_id)
        parents.append("fleet")
        values.append(int(row.parameter_count))
    for row in subsystem_sizes.itertuples(index=False):
        ids.append(f"subsystem::{row.system_id}/{row.subsystem_id}")
        labels.append(row.subsystem_id)
        parents.append(f"system::{row.system_id}")
        values.append(int(row.parameter_count))
    for row in module_sizes.itertuples(index=False):
        ids.append(f"module::{row.system_id}/{row.subsystem_id}/{row.module_id}")
        labels.append(row.module_id)
        parents.append(f"subsystem::{row.system_id}/{row.subsystem_id}")
        values.append(int(row.parameter_count))
    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            maxdepth=3,
            hovertemplate="%{label}<br>parameter_count=%{value}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Hierarchy structure",
        template="plotly_white",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_hierarchy_behavior_map(
    hierarchy_df: pd.DataFrame,
    behavior_profile_df: pd.DataFrame,
) -> go.Figure | None:
    if hierarchy_df.empty or behavior_profile_df.empty:
        return None
    required_hierarchy = {"system_id", "subsystem_id", "module_id", "parameter_name"}
    if not required_hierarchy.issubset(hierarchy_df.columns):
        return None
    if "parameter_name" not in behavior_profile_df.columns or "behavior_family_profiled" not in behavior_profile_df.columns:
        return None
    plot_df = hierarchy_df[
        ["system_id", "subsystem_id", "module_id", "parameter_name"]
    ].drop_duplicates(subset=["parameter_name"]).merge(
        behavior_profile_df[
            [
                c
                for c in [
                    "parameter_name",
                    "behavior_family_profiled",
                    "behavior_profile_confidence",
                    "parameter_datatype_profiled",
                ]
                if c in behavior_profile_df.columns
            ]
        ].drop_duplicates(subset=["parameter_name"]),
        on="parameter_name",
        how="left",
    )
    for col in ("system_id", "subsystem_id", "module_id", "parameter_name", "behavior_family_profiled"):
        plot_df[col] = plot_df[col].fillna("unknown").astype(str)
    if "behavior_profile_confidence" in plot_df.columns:
        plot_df["behavior_profile_confidence"] = pd.to_numeric(plot_df["behavior_profile_confidence"], errors="coerce")
    behavior_palette = {
        "regulated": "#1f77b4",
        "inertial": "#ff7f0e",
        "accumulative": "#2ca02c",
        "discrete_state": "#d62728",
        "mixed_unknown": "#9467bd",
        "unknown": "#9aa1a9",
    }
    fig = px.treemap(
        plot_df,
        path=["system_id", "subsystem_id", "module_id", "parameter_name"],
        values=[1] * len(plot_df),
        color="behavior_family_profiled",
        color_discrete_map=behavior_palette,
        hover_data={
            "behavior_profile_confidence": True,
            "parameter_datatype_profiled": True,
            "behavior_family_profiled": True,
        },
    )
    fig.update_traces(
        root_color="#f3f4f6",
        hovertemplate=(
            "system=%{currentPath}<br>"
            "node=%{label}<br>"
            "behavior=%{color}<br>"
            "count=%{value}<extra></extra>"
        ),
    )
    fig.update_layout(
        title="Hierarchy by parameter behavior",
        template="plotly_white",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_hierarchy_datatype_map(
    hierarchy_df: pd.DataFrame,
    behavior_profile_df: pd.DataFrame,
) -> go.Figure | None:
    if hierarchy_df.empty or behavior_profile_df.empty:
        return None
    required_hierarchy = {"system_id", "subsystem_id", "module_id", "parameter_name"}
    if not required_hierarchy.issubset(hierarchy_df.columns):
        return None
    if "parameter_name" not in behavior_profile_df.columns or "parameter_datatype_profiled" not in behavior_profile_df.columns:
        return None
    plot_df = hierarchy_df[
        ["system_id", "subsystem_id", "module_id", "parameter_name"]
    ].drop_duplicates(subset=["parameter_name"]).merge(
        behavior_profile_df[
            [
                c
                for c in [
                    "parameter_name",
                    "parameter_datatype_profiled",
                    "behavior_family_profiled",
                    "behavior_profile_confidence",
                ]
                if c in behavior_profile_df.columns
            ]
        ].drop_duplicates(subset=["parameter_name"]),
        on="parameter_name",
        how="left",
    )
    for col in ("system_id", "subsystem_id", "module_id", "parameter_name", "parameter_datatype_profiled"):
        plot_df[col] = plot_df[col].fillna("unknown").astype(str)
    datatype_palette = {
        "continuous_numeric": "#1f77b4",
        "categorical_state": "#2ca02c",
        "binary_state": "#d62728",
        "discrete_numeric": "#ff7f0e",
        "text": "#8c564b",
        "unknown": "#9aa1a9",
    }
    fig = px.treemap(
        plot_df,
        path=["system_id", "subsystem_id", "module_id", "parameter_name"],
        values=[1] * len(plot_df),
        color="parameter_datatype_profiled",
        color_discrete_map=datatype_palette,
        hover_data={
            "parameter_datatype_profiled": True,
            "behavior_family_profiled": True,
            "behavior_profile_confidence": True,
        },
    )
    fig.update_traces(
        root_color="#f3f4f6",
        hovertemplate=(
            "system=%{currentPath}<br>"
            "node=%{label}<br>"
            "datatype=%{color}<br>"
            "count=%{value}<extra></extra>"
        ),
    )
    fig.update_layout(
        title="Hierarchy by parameter datatype",
        template="plotly_white",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_graph_edge_weights(graph_df: pd.DataFrame, *, top_n: int = 20):
    if graph_df.empty:
        return None
    weight_candidates = [name for name in ("weight", "edge_weight", "abs_weight", "corr", "partial_corr") if name in graph_df.columns]
    if not weight_candidates:
        return None
    weight_col = weight_candidates[0]
    label_col = None
    if {"parameter_name_u", "parameter_name_v"}.issubset(graph_df.columns):
        plot_df = graph_df.copy()
        plot_df["edge_label"] = plot_df["parameter_name_u"].astype(str) + " -> " + plot_df["parameter_name_v"].astype(str)
        label_col = "edge_label"
    else:
        label_candidates = [name for name in graph_df.columns if name != weight_col]
        if not label_candidates:
            return None
        label_col = label_candidates[0]
        plot_df = graph_df.copy()
    plot_df = plot_df.sort_values(weight_col, ascending=False).head(top_n)
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(data=plot_df, x=weight_col, y=label_col, ax=ax, palette="viridis")
    ax.set_title(f"Top {top_n} graph edges by {weight_col}")
    ax.set_xlabel(weight_col)
    ax.set_ylabel("edge")
    return fig


def plot_phase_overview(phase_df: pd.DataFrame):
    if phase_df.empty:
        return None
    label_col = "phase_label" if "phase_label" in phase_df.columns else None
    if label_col is None:
        return None
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.countplot(data=phase_df, y=label_col, order=phase_df[label_col].value_counts().index, ax=ax, palette="crest")
    ax.set_title("Phase label frequency")
    ax.set_xlabel("count")
    ax.set_ylabel("phase_label")
    return fig


def plot_phase_timelines(phase_df: pd.DataFrame) -> go.Figure | None:
    if phase_df.empty:
        return None
    required = {"tail_id", "flight_id", "t_start", "t_end"}
    if not required.issubset(phase_df.columns):
        return None
    df = _prepare_timestamp_df(phase_df).copy()
    label_col = "phase_state_detected" if "phase_state_detected" in df.columns else (
        "phase_id_detected" if "phase_id_detected" in df.columns else None
    )
    if label_col is None:
        return None
    df["phase_label"] = df[label_col].astype(str)
    df["flight_label"] = df["tail_id"].astype(str) + " / " + df["flight_id"].astype(str)
    fig = px.timeline(
        df.sort_values(["tail_id", "flight_id", "t_start"]),
        x_start="t_start",
        x_end="t_end",
        y="flight_label",
        color="phase_label",
        hover_data={
            "tail_id": True,
            "flight_id": True,
            "phase_label": True,
            "phase_id_detected": True if "phase_id_detected" in df.columns else False,
            "phase_confidence_detected": True if "phase_confidence_detected" in df.columns else False,
            "flight_label": False,
        },
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        title="Detected phase timelines",
        template="plotly_white",
        xaxis_title="timestamp_utc",
        yaxis_title="tail / flight",
        legend_title_text="phase",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_phase_confidence(phase_df: pd.DataFrame) -> go.Figure | None:
    if phase_df.empty or "phase_confidence_detected" not in phase_df.columns:
        return None
    df = _prepare_timestamp_df(phase_df).copy()
    label_col = "phase_state_detected" if "phase_state_detected" in df.columns else (
        "phase_id_detected" if "phase_id_detected" in df.columns else None
    )
    if label_col is None:
        return None
    df["phase_label"] = df[label_col].astype(str)
    if "t_start" in df.columns:
        df["phase_midpoint"] = df["t_start"] + ((df["t_end"] - df["t_start"]) / 2)
    elif "timestamp_utc" in df.columns:
        df["phase_midpoint"] = df["timestamp_utc"]
    else:
        return None
    fig = px.scatter(
        df.sort_values("phase_midpoint"),
        x="phase_midpoint",
        y="phase_confidence_detected",
        color="phase_label",
        hover_data={
            "tail_id": True if "tail_id" in df.columns else False,
            "flight_id": True if "flight_id" in df.columns else False,
            "phase_label": True,
            "phase_id_detected": True if "phase_id_detected" in df.columns else False,
        },
    )
    fig.update_layout(
        title="Phase confidence over time",
        template="plotly_white",
        xaxis_title="timestamp_utc",
        yaxis_title="phase_confidence_detected",
        legend_title_text="phase",
        margin={"t": 60, "l": 10, "r": 10, "b": 10},
    )
    return fig


def plot_score_distribution(scores_df: pd.DataFrame):
    if scores_df.empty:
        return None
    score_col = next((name for name in ("global_score", "score", "p_value") if name in scores_df.columns), None)
    if score_col is None:
        return None
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.histplot(data=scores_df, x=score_col, bins=30, kde=True, ax=ax, color="#4c78a8")
    ax.set_title(f"Distribution of {score_col}")
    ax.set_xlabel(score_col)
    return fig


def plot_anomaly_summary(anomaly_df: pd.DataFrame):
    if anomaly_df.empty:
        return None
    category_col = next((name for name in ("severity", "anomaly_type_label", "anomaly_type_detected") if name in anomaly_df.columns), None)
    if category_col is None:
        return None
    plot_df = anomaly_df.copy()
    plot_df[category_col] = plot_df[category_col].astype(str).fillna("missing")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.countplot(data=plot_df, y=category_col, order=plot_df[category_col].value_counts().index, ax=ax, palette="rocket")
    ax.set_title(f"Anomaly counts by {category_col}")
    ax.set_xlabel("count")
    ax.set_ylabel(category_col)
    return fig


def build_parameter_explorer_dataset(bundle: SimulationRunBundle) -> dict[str, pd.DataFrame]:
    explorer_bundle = load_explorer_bundle(bundle)
    options = load_explorer_filter_options(explorer_bundle)
    default_parameters = tuple(options["parameter_names"][: min(3, len(options["parameter_names"]))])
    return load_explorer_slice(explorer_bundle, SensorExplorerState(parameter_names=default_parameters))


def prepare_parameter_telemetry_dataframe(
    telemetry_df: pd.DataFrame,
    hierarchy_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if telemetry_df.empty:
        return telemetry_df.copy()
    df = telemetry_df.copy()
    if hierarchy_df is not None and not hierarchy_df.empty:
        hierarchy_cols = ["parameter_name", "system_id", "subsystem_id", "module_id"]
        missing_cols = [col for col in hierarchy_cols[1:] if col not in df.columns]
        if missing_cols:
            df = df.merge(
                hierarchy_df[hierarchy_cols].drop_duplicates(subset=["parameter_name"]),
                on="parameter_name",
                how="left",
            )
    df = _prepare_timestamp_df(df)
    df["parameter_value_numeric"] = pd.to_numeric(df.get("parameter_value"), errors="coerce")
    df["parameter_value_clean_numeric"] = pd.to_numeric(df.get("parameter_value_clean"), errors="coerce")
    value_source = df["parameter_value_clean_numeric"].where(df["parameter_value_clean_numeric"].notna(), df["parameter_value_numeric"])
    df["plot_value_raw"] = value_source
    grouped = df.groupby("parameter_name", dropna=False)["plot_value_raw"]
    means = grouped.transform("mean")
    stds = grouped.transform("std").replace(0, pd.NA)
    medians = grouped.transform("median")
    abs_dev = (df["plot_value_raw"] - medians).abs()
    mad = abs_dev.groupby(df["parameter_name"], dropna=False).transform("median").replace(0, pd.NA)
    df["plot_value_zscore"] = ((df["plot_value_raw"] - means) / stds).fillna(0.0)
    df["plot_value_robust"] = ((df["plot_value_raw"] - medians) / (1.4826 * mad)).fillna(0.0)
    df["plot_value"] = df["plot_value_robust"]
    return df.sort_values(["timestamp_utc", "parameter_name"]).reset_index(drop=True)


def _prepare_timestamp_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "timestamp_utc" in out.columns:
        out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True, errors="coerce")
    if "t_start" in out.columns:
        out["t_start"] = pd.to_datetime(out["t_start"], utc=True, errors="coerce")
    if "t_end" in out.columns:
        out["t_end"] = pd.to_datetime(out["t_end"], utc=True, errors="coerce")
    return out


def build_phase_interval_table(phase_windows_df: pd.DataFrame, phase_labels_df: pd.DataFrame) -> pd.DataFrame:
    if not phase_windows_df.empty and {"t_start", "t_end"}.issubset(phase_windows_df.columns):
        df = _prepare_timestamp_df(phase_windows_df)
        label_col = "phase_state_detected" if "phase_state_detected" in df.columns else "phase_id_detected"
        return pd.DataFrame(
            {
                "timestamp_start": df["t_start"],
                "timestamp_end": df["t_end"],
                "phase_label": df[label_col].astype(str),
                "source": "phase_windows",
            }
        )
    if phase_labels_df.empty or "timestamp_utc" not in phase_labels_df.columns:
        return pd.DataFrame(columns=["timestamp_start", "timestamp_end", "phase_label", "source"])
    df = _prepare_timestamp_df(phase_labels_df).sort_values(["step_index", "timestamp_utc"]).reset_index(drop=True)
    if "phase_label" not in df.columns:
        return pd.DataFrame(columns=["timestamp_start", "timestamp_end", "phase_label", "source"])
    df["phase_group"] = (df["phase_label"] != df["phase_label"].shift()).cumsum()
    intervals = (
        df.groupby(["phase_group", "phase_label"], dropna=False)
        .agg(timestamp_start=("timestamp_utc", "min"), timestamp_end=("timestamp_utc", "max"))
        .reset_index()
    )
    intervals["source"] = "phase_labels"
    return intervals[["timestamp_start", "timestamp_end", "phase_label", "source"]]


def build_parameter_explorer_filter_options(telemetry_df: pd.DataFrame) -> dict[str, list[str]]:
    if telemetry_df.empty:
        return {"system_ids": [], "subsystem_ids": [], "module_ids": [], "parameter_names": []}
    return {
        "system_ids": sorted(str(v) for v in telemetry_df["system_id"].dropna().unique()) if "system_id" in telemetry_df.columns else [],
        "subsystem_ids": sorted(str(v) for v in telemetry_df["subsystem_id"].dropna().unique()) if "subsystem_id" in telemetry_df.columns else [],
        "module_ids": sorted(str(v) for v in telemetry_df["module_id"].dropna().unique()) if "module_id" in telemetry_df.columns else [],
        "parameter_names": sorted(str(v) for v in telemetry_df["parameter_name"].dropna().unique()),
    }


def filter_parameter_explorer_data(dataset: dict[str, pd.DataFrame], state: SensorExplorerState) -> dict[str, pd.DataFrame]:
    telemetry_df = dataset["telemetry"].copy()
    masks = pd.Series(True, index=telemetry_df.index)
    if state.system_id and "system_id" in telemetry_df.columns:
        masks &= telemetry_df["system_id"] == state.system_id
    if state.subsystem_id and "subsystem_id" in telemetry_df.columns:
        masks &= telemetry_df["subsystem_id"] == state.subsystem_id
    if state.module_id and "module_id" in telemetry_df.columns:
        masks &= telemetry_df["module_id"] == state.module_id
    if state.parameter_names:
        masks &= telemetry_df["parameter_name"].isin(list(state.parameter_names))
    if state.time_start:
        masks &= telemetry_df["timestamp_utc"] >= pd.Timestamp(state.time_start, tz="UTC")
    if state.time_end:
        masks &= telemetry_df["timestamp_utc"] <= pd.Timestamp(state.time_end, tz="UTC")
    telemetry_df = telemetry_df.loc[masks].copy()

    if telemetry_df.empty:
        return {name: frame.iloc[0:0].copy() for name, frame in dataset.items()}

    visible_parameters = set(telemetry_df["parameter_name"].dropna())
    min_time = telemetry_df["timestamp_utc"].min()
    max_time = telemetry_df["timestamp_utc"].max()

    def _filter_overlay(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()
        out = frame.copy()
        if "parameter_name" in out.columns:
            out = out[out["parameter_name"].isin(visible_parameters)]
        if "timestamp_utc" in out.columns:
            out = out[(out["timestamp_utc"] >= min_time) & (out["timestamp_utc"] <= max_time)]
        if "timestamp_start" in out.columns and "timestamp_end" in out.columns:
            out = out[(out["timestamp_end"] >= min_time) & (out["timestamp_start"] <= max_time)]
        for scope_col, scope_value in (
            ("system_id", state.system_id),
            ("subsystem_id", state.subsystem_id),
            ("module_id", state.module_id),
        ):
            if scope_value and scope_col in out.columns:
                out = out[out[scope_col] == scope_value]
        return out

    return {
        "telemetry": telemetry_df,
        "hierarchy": _filter_overlay(dataset["hierarchy"]),
        "events": _filter_overlay(dataset["events"]),
        "anomaly_event": _filter_overlay(dataset["anomaly_event"]),
        "anomaly_telemetry": _filter_overlay(dataset["anomaly_telemetry"]),
        "anomaly_window": _filter_overlay(dataset["anomaly_window"]),
        "phase_intervals": _filter_overlay(dataset["phase_intervals"]),
    }


def build_parameter_explorer_figure(dataset: dict[str, pd.DataFrame], state: SensorExplorerState) -> go.Figure:
    filtered = filter_parameter_explorer_data(dataset, state)
    telemetry_df = filtered["telemetry"]
    if telemetry_df.empty:
        fig = go.Figure()
        fig.update_layout(title="No telemetry rows matched the current explorer state.")
        return fig

    scale_mode = state.scale_mode
    value_col = "plot_value_raw" if scale_mode in {"raw", "dual_axis"} else "plot_value"
    title_suffix = "raw units" if scale_mode in {"raw", "dual_axis"} else "normalized"
    parameter_names = list(telemetry_df["parameter_name"].dropna().unique())
    use_subplots = scale_mode == "subplots"
    use_dual_axis = scale_mode == "dual_axis" and len(parameter_names) >= 2

    if use_subplots:
        fig = make_subplots(rows=len(parameter_names), cols=1, shared_xaxes=True, vertical_spacing=0.03, subplot_titles=parameter_names)
    elif use_dual_axis:
        fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])
    else:
        fig = go.Figure()

    palette = [
        "#4c78a8",
        "#f58518",
        "#54a24b",
        "#e45756",
        "#72b7b2",
        "#b279a2",
        "#ff9da6",
        "#9d755d",
        "#bab0ab",
    ]

    param_to_axis: dict[str, int] = {}
    for index, parameter_name in enumerate(parameter_names):
        series_df = telemetry_df[telemetry_df["parameter_name"] == parameter_name].sort_values("timestamp_utc")
        color = palette[index % len(palette)]
        customdata = series_df[
            [
                col
                for col in [
                    "system_id",
                    "subsystem_id",
                    "module_id",
                    "parameter_value",
                    "parameter_value_clean",
                    "phase_label",
                    "event_type_detected",
                    "fault_active",
                    "severity",
                ]
                if col in series_df.columns
            ]
        ]
        hovertemplate = (
            "parameter=%{fullData.name}<br>"
            "timestamp=%{x}<br>"
            f"value=%{{y}}<br>"
            + ("system=%{customdata[0]}<br>" if "system_id" in series_df.columns else "")
            + ("subsystem=%{customdata[1]}<br>" if "subsystem_id" in series_df.columns else "")
            + ("module=%{customdata[2]}<br>" if "module_id" in series_df.columns else "")
            + "<extra></extra>"
        )
        trace = go.Scatter(
            x=series_df["timestamp_utc"],
            y=series_df[value_col],
            mode="lines",
            name=str(parameter_name),
            line={"color": color, "width": 2},
            customdata=customdata.to_numpy() if not customdata.empty else None,
            hovertemplate=hovertemplate,
        )
        param_to_axis[parameter_name] = index
        if use_subplots:
            fig.add_trace(trace, row=index + 1, col=1)
        elif use_dual_axis:
            fig.add_trace(trace, row=1, col=1, secondary_y=bool(index % 2))
        else:
            fig.add_trace(trace)

    if state.show_phase_shading:
        for row in filtered["phase_intervals"].itertuples(index=False):
            fig.add_vrect(
                x0=row.timestamp_start,
                x1=row.timestamp_end,
                fillcolor="LightSkyBlue",
                opacity=0.09,
                line_width=0,
                annotation_text=str(row.phase_label),
                annotation_position="top left",
            )

    if state.show_events and not filtered["events"].empty:
        grouped = filtered["events"].groupby("parameter_name", dropna=False)
        for parameter_name, event_df in grouped:
            if parameter_name not in parameter_names:
                continue
            series_df = telemetry_df[telemetry_df["parameter_name"] == parameter_name][["timestamp_utc", value_col]]
            event_points = event_df.merge(series_df, on="timestamp_utc", how="left")
            marker = go.Scatter(
                x=event_points["timestamp_utc"],
                y=event_points[value_col].fillna(0.0),
                mode="markers",
                name=f"{parameter_name} events",
                marker={"symbol": "diamond", "size": 8, "color": "#222222"},
                hovertemplate="event=%{text}<br>timestamp=%{x}<extra></extra>",
                text=event_points["event_type_detected"].astype(str),
                showlegend=False,
            )
            if use_subplots:
                fig.add_trace(marker, row=param_to_axis[parameter_name] + 1, col=1)
            elif use_dual_axis:
                fig.add_trace(marker, row=1, col=1, secondary_y=bool(param_to_axis[parameter_name] % 2))
            else:
                fig.add_trace(marker)

    if state.show_anomaly_markers and not filtered["anomaly_telemetry"].empty:
        grouped = filtered["anomaly_telemetry"].groupby("parameter_name", dropna=False)
        for parameter_name, anomaly_df in grouped:
            if parameter_name not in parameter_names:
                continue
            series_df = telemetry_df[telemetry_df["parameter_name"] == parameter_name][["timestamp_utc", value_col]]
            anomaly_points = anomaly_df.merge(series_df, on="timestamp_utc", how="left")
            marker = go.Scatter(
                x=anomaly_points["timestamp_utc"],
                y=anomaly_points[value_col].fillna(0.0),
                mode="markers",
                name=f"{parameter_name} anomalies",
                marker={"symbol": "x", "size": 10, "color": "#d62728"},
                hovertemplate="severity=%{text}<br>timestamp=%{x}<extra></extra>",
                text=anomaly_points["severity"].astype(str),
                showlegend=False,
            )
            if use_subplots:
                fig.add_trace(marker, row=param_to_axis[parameter_name] + 1, col=1)
            elif use_dual_axis:
                fig.add_trace(marker, row=1, col=1, secondary_y=bool(param_to_axis[parameter_name] % 2))
            else:
                fig.add_trace(marker)

    if state.show_anomaly_windows and not filtered["anomaly_window"].empty:
        for row in filtered["anomaly_window"].itertuples(index=False):
            timestamp = getattr(row, "timestamp_utc", None)
            if pd.isna(timestamp):
                continue
            fig.add_shape(
                type="line",
                x0=timestamp,
                x1=timestamp,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                line={"dash": "dot", "color": "#d62728", "width": 1},
                opacity=0.5,
            )
            fig.add_annotation(
                x=timestamp,
                y=1,
                xref="x",
                yref="paper",
                text=str(getattr(row, "severity", "anomaly")),
                showarrow=False,
                yanchor="bottom",
                xanchor="left",
                font={"color": "#d62728", "size": 10},
            )

    fig.update_layout(
        title=f"Parameter explorer ({title_suffix})",
        hovermode="x unified",
        template="plotly_white",
        height=max(500, 250 * len(parameter_names)) if use_subplots else 650,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
    )
    fig.update_xaxes(title_text="timestamp_utc")
    if not use_subplots:
        fig.update_yaxes(title_text="normalized value" if scale_mode not in {"raw", "dual_axis"} else "raw value")

    scale_buttons = [
        {
            "label": "Normalized",
            "method": "relayout",
            "args": [{"title": "Parameter explorer (normalized)"}],
        },
        {
            "label": "Current Mode",
            "method": "relayout",
            "args": [{"title": f"Parameter explorer ({title_suffix})"}],
        },
    ]
    fig.update_layout(
        updatemenus=[
            {
                "type": "dropdown",
                "direction": "down",
                "x": 1.02,
                "y": 1.0,
                "showactive": True,
                "buttons": scale_buttons,
            }
        ]
    )
    return fig


def build_sensor_explorer_dataset(bundle: SimulationRunBundle) -> dict[str, pd.DataFrame]:
    return build_parameter_explorer_dataset(bundle)


def prepare_sensor_telemetry_dataframe(
    telemetry_df: pd.DataFrame,
    hierarchy_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    return prepare_parameter_telemetry_dataframe(telemetry_df, hierarchy_df)


def build_sensor_explorer_filter_options(telemetry_df: pd.DataFrame) -> dict[str, list[str]]:
    return build_parameter_explorer_filter_options(telemetry_df)


def filter_sensor_explorer_data(dataset: dict[str, pd.DataFrame], state: SensorExplorerState) -> dict[str, pd.DataFrame]:
    return filter_parameter_explorer_data(dataset, state)


def build_sensor_explorer_figure(dataset: dict[str, pd.DataFrame], state: SensorExplorerState) -> go.Figure:
    return build_parameter_explorer_figure(dataset, state)
