"""Unified validation harness report for iterative simulation tuning runs."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from libs.simulation.phase.runtime import PhaseProgram
from libs.simulation.run_bundle import load_json_if_exists
from libs.simulation.run_context import RunPaths, resolve_flight_stochasticity, write_manifest

if TYPE_CHECKING:
    from libs.simulation import FlightSpec


@dataclass(frozen=True)
class HarnessParameterRecord:
    scope_name: str
    parameter_path: str
    value: Any
    source_path: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "scope_name": self.scope_name,
            "parameter_path": self.parameter_path,
            "value": self.value,
        }
        if self.source_path is not None:
            payload["source_path"] = self.source_path
        return payload


@dataclass(frozen=True)
class HarnessMetricRecord:
    category: str
    scope_name: str
    subscope_name: str
    metric_path: str
    value: float | int

    def to_payload(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "scope_name": self.scope_name,
            "subscope_name": self.subscope_name,
            "metric_path": self.metric_path,
            "value": self.value,
        }


@dataclass(frozen=True)
class StageValidationHarness:
    stage_script: str
    stage_manifest_path: str | None
    fit_parameters: dict[str, Any]
    validation_metrics: dict[str, Any]
    compute_performance: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "stage_script": self.stage_script,
            "fit_parameters": self.fit_parameters,
            "validation_metrics": self.validation_metrics,
            "compute_performance": self.compute_performance,
        }
        if self.stage_manifest_path is not None:
            payload["stage_manifest_path"] = self.stage_manifest_path
        return payload


@dataclass(frozen=True)
class ValidationHarnessReport:
    report_version: str
    status: str | None
    run_dir: str
    source_artifacts: dict[str, str]
    workload_signature: dict[str, Any]
    simulation_context: dict[str, Any]
    fit_parameters: dict[str, Any]
    validation_metrics: dict[str, Any]
    compute_performance: dict[str, Any]
    stage_harness: tuple[StageValidationHarness, ...]
    methodology: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "status": self.status,
            "run_dir": self.run_dir,
            "source_artifacts": self.source_artifacts,
            "workload_signature": self.workload_signature,
            "simulation_context": self.simulation_context,
            "fit_parameters": self.fit_parameters,
            "validation_metrics": self.validation_metrics,
            "compute_performance": self.compute_performance,
            "stage_harness": [stage.to_payload() for stage in self.stage_harness],
            "methodology": self.methodology,
        }


def _append_path(prefix: str, part: str) -> str:
    return f"{prefix}.{part}" if prefix else part


def _flatten_parameter_records(
    payload: Any,
    *,
    scope_name: str,
    source_path: str | None,
    prefix: str = "",
) -> list[HarnessParameterRecord]:
    if isinstance(payload, dict):
        records: list[HarnessParameterRecord] = []
        for key in sorted(payload):
            records.extend(
                _flatten_parameter_records(
                    payload[key],
                    scope_name=scope_name,
                    source_path=source_path,
                    prefix=_append_path(prefix, str(key)),
                )
            )
        return records
    if isinstance(payload, (list, tuple)):
        records: list[HarnessParameterRecord] = []
        for idx, item in enumerate(payload):
            records.extend(
                _flatten_parameter_records(
                    item,
                    scope_name=scope_name,
                    source_path=source_path,
                    prefix=f"{prefix}[{idx}]",
                )
            )
        return records
    if not prefix:
        return []
    return [
        HarnessParameterRecord(
            scope_name=scope_name,
            parameter_path=prefix,
            value=payload,
            source_path=source_path,
        )
    ]


def _is_finite_numeric(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if isinstance(value, float):
        return math.isfinite(value)
    return True


def _flatten_numeric_metric_records(
    payload: Any,
    *,
    category: str,
    scope_name: str,
    subscope_name: str,
    prefix: str = "",
) -> list[HarnessMetricRecord]:
    if isinstance(payload, dict):
        records: list[HarnessMetricRecord] = []
        for key in sorted(payload):
            records.extend(
                _flatten_numeric_metric_records(
                    payload[key],
                    category=category,
                    scope_name=scope_name,
                    subscope_name=subscope_name,
                    prefix=_append_path(prefix, str(key)),
                )
            )
        return records
    if isinstance(payload, (list, tuple)):
        if not payload or len(payload) > 16:
            return []
        if not all(_is_finite_numeric(item) for item in payload):
            return []
        records: list[HarnessMetricRecord] = []
        for idx, item in enumerate(payload):
            records.extend(
                _flatten_numeric_metric_records(
                    item,
                    category=category,
                    scope_name=scope_name,
                    subscope_name=subscope_name,
                    prefix=f"{prefix}[{idx}]",
                )
            )
        return records
    if not prefix or not _is_finite_numeric(payload):
        return []
    return [
        HarnessMetricRecord(
            category=category,
            scope_name=scope_name,
            subscope_name=subscope_name,
            metric_path=prefix,
            value=payload,
        )
    ]


def _count_values(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _window_detail_label(window: Any) -> str:
    context = dict(getattr(window, "context", {}) or {})
    metadata = dict(getattr(window, "metadata", {}) or {})
    return str(
        metadata.get("misbehavior_detail_label")
        or context.get("misbehavior_detail_label")
        or context.get("violation_type")
        or metadata.get("fault_type")
        or metadata.get("misbehavior_family_label")
        or context.get("misbehavior_family_label")
        or ""
    )


def _window_family_label(window: Any) -> str:
    context = dict(getattr(window, "context", {}) or {})
    metadata = dict(getattr(window, "metadata", {}) or {})
    return str(
        metadata.get("misbehavior_family_label")
        or context.get("misbehavior_family_label")
        or _window_detail_label(window)
        or "unspecified"
    )


def _summarize_parameter_catalog(flight: "FlightSpec") -> dict[str, Any]:
    module_specs = tuple(flight.aircraft_spec.iter_module_specs())
    parameter_specs = [parameter for module in module_specs for parameter in module.parameters]
    sampling_rates = sorted(
        {
            float(parameter.sampling_rate_hz)
            for parameter in parameter_specs
            if parameter.sampling_rate_hz is not None
        }
    )
    return {
        "parameter_count": len(parameter_specs),
        "parameter_names": sorted(str(parameter.parameter_name) for parameter in parameter_specs),
        "datatype_counts": _count_values(
            [str(parameter.parameter_datatype_label or "unspecified") for parameter in parameter_specs]
        ),
        "behavior_family_counts": _count_values(
            [str(parameter.behavior_family_label or "unspecified") for parameter in parameter_specs]
        ),
        "sampling_rate_hz": {
            "parameter_count_with_sampling_rate": len(
                [parameter for parameter in parameter_specs if parameter.sampling_rate_hz is not None]
            ),
            "min": (min(sampling_rates) if sampling_rates else None),
            "max": (max(sampling_rates) if sampling_rates else None),
            "unique_values": sampling_rates,
        },
        "module_family_counts": _count_values([str(module.module_family or "unspecified") for module in module_specs]),
    }


def _summarize_hierarchy(flight: "FlightSpec") -> dict[str, Any]:
    systems = tuple(flight.aircraft_spec.systems)
    subsystems = [subsystem for system in systems for subsystem in system.subsystems]
    modules = [module for subsystem in subsystems for module in subsystem.modules]
    parameter_specs = [parameter for module in modules for parameter in module.parameters]
    couplings = tuple(flight.aircraft_spec.couplings)
    return {
        "aircraft_id": str(flight.aircraft_spec.aircraft_id),
        "system_count": len(systems),
        "subsystem_count": len(subsystems),
        "module_count": len(modules),
        "coupling_count": len(couplings),
        "system_ids": sorted(str(system.system_id) for system in systems),
        "subsystem_ids": sorted(str(subsystem.subsystem_id) for subsystem in subsystems),
        "module_ids": sorted(str(module.module_id) for module in modules),
        "coupling_ids": sorted(str(coupling.coupling_id) for coupling in couplings),
        "coupling_relation_type_counts": _count_values([str(coupling.relation_type) for coupling in couplings]),
        "parameter_count_by_system": _count_values([str(parameter.system_id) for parameter in parameter_specs]),
        "parameter_count_by_subsystem": _count_values([str(parameter.subsystem_id) for parameter in parameter_specs]),
        "parameter_count_by_module": _count_values([str(parameter.module_id) for parameter in parameter_specs]),
    }


def _summarize_input_program(
    *,
    flight: "FlightSpec",
    n_steps: int,
    dt_seconds: float,
) -> dict[str, Any]:
    steps = tuple(flight.input_program_spec.steps)
    module_counts = [len(step) for step in steps]
    parameter_counts = [sum(len(parameters) for parameters in step.values()) for step in steps]
    targeted_parameter_names = sorted(
        {
            str(parameter_name)
            for step in steps
            for parameter_inputs in step.values()
            for parameter_name in parameter_inputs
        }
    )
    return {
        "authored_step_count": len(steps),
        "hold_last_step": bool(flight.input_program_spec.hold_last_step),
        "targeted_parameter_count": len(targeted_parameter_names),
        "targeted_parameter_names": targeted_parameter_names,
        "module_inputs_per_step": {
            "min": (min(module_counts) if module_counts else 0),
            "max": (max(module_counts) if module_counts else 0),
            "mean": ((sum(module_counts) / len(module_counts)) if module_counts else 0.0),
        },
        "parameter_inputs_per_step": {
            "min": (min(parameter_counts) if parameter_counts else 0),
            "max": (max(parameter_counts) if parameter_counts else 0),
            "mean": ((sum(parameter_counts) / len(parameter_counts)) if parameter_counts else 0.0),
        },
        "configured_duration_seconds": float(len(steps) * dt_seconds),
        "run_step_count": int(n_steps),
        "nominal_duration_seconds": float(n_steps * dt_seconds),
        "metadata": dict(flight.input_program_spec.metadata or {}),
    }


def _summarize_phases(
    *,
    flight: "FlightSpec",
    n_steps: int,
    dt_seconds: float,
) -> dict[str, Any]:
    phase_spec = flight.phase_program_spec
    phase_program = PhaseProgram.from_spec(phase_spec)
    schedule = phase_program.schedule
    schedule_segments = (
        [
            {
                "phase_label": str(segment.phase_label),
                "duration_steps": int(segment.duration_steps),
            }
            for segment in schedule.segments
        ]
        if schedule is not None
        else []
    )
    configured_phase_labels = sorted(
        {
            *(str(label) for label in phase_program.explicit_labels_by_step if label),
            *(str(segment.phase_label) for segment in (schedule.segments if schedule is not None else ())),
            *(str(label) for label in phase_program.envelopes_by_label),
        }
    )
    run_phase_labels = [phase_program.label_for_step(step_index) for step_index in range(max(int(n_steps), 0))]
    non_null_run_phase_labels = [str(label) for label in run_phase_labels if label]
    return {
        "configured_phase_count": len(configured_phase_labels),
        "configured_phase_labels": configured_phase_labels,
        "schedule_repeat": (bool(schedule.repeat) if schedule is not None else False),
        "schedule_segment_count": len(schedule_segments),
        "schedule_segments": schedule_segments,
        "explicit_label_count": len([label for label in phase_program.explicit_labels_by_step if label]),
        "envelope_count": len(phase_program.envelopes_by_label),
        "envelope_phase_labels": sorted(str(label) for label in phase_program.envelopes_by_label),
        "run_step_count": int(n_steps),
        "run_duration_seconds": float(n_steps * dt_seconds),
        "run_phase_counts_by_label": _count_values(non_null_run_phase_labels),
        "run_phase_sequence_preview": [str(label) for label in run_phase_labels[:32] if label],
    }


def _summarize_misbehavior_program(
    *,
    flight: "FlightSpec",
    dt_seconds: float,
) -> dict[str, Any]:
    windows = tuple(getattr(flight.misbehavior_program_spec, "windows", ()) or ())
    durations_steps = [max(int(window.end_step_exclusive) - int(window.start_step), 0) for window in windows]
    targeted_parameters = sorted(
        {
            str(window.parameter_name)
            for window in windows
            if str(getattr(window, "subject_kind", "parameter")) == "parameter" and getattr(window, "parameter_name", None)
        }
    )
    targeted_couplings = sorted(
        {
            str(window.coupling_id)
            for window in windows
            if str(getattr(window, "subject_kind", "parameter")) == "coupling" and getattr(window, "coupling_id", None)
        }
    )
    return {
        "window_count": len(windows),
        "subject_kind_counts": _count_values([str(getattr(window, "subject_kind", "parameter")) for window in windows]),
        "family_counts": _count_values([_window_family_label(window) for window in windows]),
        "detail_counts": _count_values([_window_detail_label(window) or "unspecified" for window in windows]),
        "targeted_parameter_names": targeted_parameters,
        "targeted_coupling_ids": targeted_couplings,
        "duration_steps": {
            "min": (min(durations_steps) if durations_steps else 0),
            "max": (max(durations_steps) if durations_steps else 0),
            "mean": ((sum(durations_steps) / len(durations_steps)) if durations_steps else 0.0),
        },
        "duration_seconds": {
            "min": (min(durations_steps) * float(dt_seconds) if durations_steps else 0.0),
            "max": (max(durations_steps) * float(dt_seconds) if durations_steps else 0.0),
            "mean": (((sum(durations_steps) / len(durations_steps)) * float(dt_seconds)) if durations_steps else 0.0),
        },
        "metadata": dict(getattr(flight.misbehavior_program_spec, "metadata", {}) or {}),
    }


def _build_simulation_context(
    *,
    flight: "FlightSpec",
    manifest: dict[str, Any],
) -> dict[str, Any]:
    simulation = dict(manifest.get("simulation") or {})
    n_steps = int(simulation.get("n_steps") or 0)
    dt_seconds = float(simulation.get("dt_seconds") or 0.0)
    stochasticity = resolve_flight_stochasticity(
        flight=flight,
        sim_seed=((manifest.get("pipeline") or {}).get("sim_seed")),
    )
    return {
        "flight": {
            "flight_name": str((manifest.get("source") or {}).get("flight_name", "") or ""),
            "aircraft_id": str(flight.aircraft_spec.aircraft_id),
            "nominal_step_count": n_steps,
            "nominal_duration_seconds": float(n_steps * dt_seconds),
            "dt_seconds": dt_seconds,
            "metadata": dict(flight.metadata or {}),
        },
        "stochasticity": stochasticity,
        "input_program": _summarize_input_program(flight=flight, n_steps=n_steps, dt_seconds=dt_seconds),
        "parameter_catalog": _summarize_parameter_catalog(flight),
        "hierarchy": _summarize_hierarchy(flight),
        "phases": _summarize_phases(flight=flight, n_steps=n_steps, dt_seconds=dt_seconds),
        "misbehavior_program": _summarize_misbehavior_program(flight=flight, dt_seconds=dt_seconds),
    }


def _build_stage_harness(
    *,
    engineering_stages: list[dict[str, Any]],
) -> tuple[tuple[StageValidationHarness, ...], dict[str, dict[str, Any]], list[HarnessParameterRecord]]:
    stage_harness: list[StageValidationHarness] = []
    stage_configs: dict[str, dict[str, Any]] = {}
    parameter_records: list[HarnessParameterRecord] = []
    for stage in engineering_stages:
        stage_script = str(stage.get("stage_script", ""))
        engineering = dict(stage.get("engineering_performance") or {})
        manifest_path = engineering.get("manifest_path")
        manifest_payload = load_json_if_exists(Path(manifest_path)) if isinstance(manifest_path, str) and manifest_path else None
        fit_parameters = dict((manifest_payload or {}).get("config") or {})
        stage_configs[stage_script] = fit_parameters
        parameter_records.extend(
            _flatten_parameter_records(
                fit_parameters,
                scope_name=stage_script,
                source_path=(str(manifest_path) if isinstance(manifest_path, str) and manifest_path else None),
            )
        )
        stage_harness.append(
            StageValidationHarness(
                stage_script=stage_script,
                stage_manifest_path=(str(manifest_path) if isinstance(manifest_path, str) and manifest_path else None),
                fit_parameters=fit_parameters,
                validation_metrics=dict(stage.get("modeling_performance") or {}),
                compute_performance=engineering,
            )
        )
    return tuple(stage_harness), stage_configs, parameter_records


def _build_validation_metric_records(
    *,
    overall_validation: dict[str, Any],
    stage_harness: tuple[StageValidationHarness, ...],
) -> list[HarnessMetricRecord]:
    records: list[HarnessMetricRecord] = []
    for report_key, payload in sorted(overall_validation.items()):
        records.extend(
            _flatten_numeric_metric_records(
                payload,
                category="validation",
                scope_name="overall",
                subscope_name=report_key,
            )
        )
    for stage in stage_harness:
        for report_key, payload in sorted(stage.validation_metrics.items()):
            records.extend(
                _flatten_numeric_metric_records(
                    payload,
                    category="validation",
                    scope_name=stage.stage_script,
                    subscope_name=report_key,
                )
            )
    return records


def _build_compute_metric_records(
    *,
    engineering_performance: dict[str, Any],
    stage_harness: tuple[StageValidationHarness, ...],
) -> list[HarnessMetricRecord]:
    records: list[HarnessMetricRecord] = []
    overall = dict(engineering_performance.get("overall") or {})
    scale_signature = dict(engineering_performance.get("scale_signature") or {})
    records.extend(
        _flatten_numeric_metric_records(
            overall,
            category="compute",
            scope_name="overall",
            subscope_name="overall",
        )
    )
    records.extend(
        _flatten_numeric_metric_records(
            scale_signature,
            category="compute",
            scope_name="overall",
            subscope_name="scale_signature",
        )
    )
    for stage in stage_harness:
        records.extend(
            _flatten_numeric_metric_records(
                stage.compute_performance,
                category="compute",
                scope_name=stage.stage_script,
                subscope_name="engineering_performance",
            )
        )
    return records


def _build_bottleneck_stages(stage_harness: tuple[StageValidationHarness, ...]) -> list[dict[str, Any]]:
    bottlenecks: list[dict[str, Any]] = []
    for stage in stage_harness:
        compute = stage.compute_performance
        bottlenecks.append(
            {
                "stage_script": stage.stage_script,
                "elapsed_ms": compute.get("elapsed_ms"),
                "share_of_total_elapsed": compute.get("share_of_total_elapsed"),
                "fit_parameter_count": len(stage.fit_parameters),
                "validation_report_keys": sorted(stage.validation_metrics.keys()),
            }
        )
    return sorted(
        bottlenecks,
        key=lambda item: float(item.get("elapsed_ms") or 0.0),
        reverse=True,
    )[:5]


def _build_methodology(
    *,
    workload_signature: dict[str, Any],
    fit_parameter_count: int,
    validation_metric_count: int,
    compute_metric_count: int,
    bottleneck_stages: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "ready",
        "comparison_unit": "single simulation run on a matched workload signature",
        "workload_guardrails": {
            "flight_name": ((workload_signature.get("source") or {}).get("flight_name")),
            "tail_id": ((workload_signature.get("source") or {}).get("tail_id")),
            "flight_id": ((workload_signature.get("source") or {}).get("flight_id")),
            "n_steps": ((workload_signature.get("simulation") or {}).get("n_steps")),
            "dt_seconds": ((workload_signature.get("simulation") or {}).get("dt_seconds")),
            "pipeline_mode": ((workload_signature.get("pipeline") or {}).get("mode")),
            "stochastic_profile_name": ((workload_signature.get("stochasticity") or {}).get("profile_name")),
            "stochastic_profile_version": ((workload_signature.get("stochasticity") or {}).get("profile_version")),
            "sim_seed": ((workload_signature.get("stochasticity") or {}).get("seed")),
        },
        "coverage": {
            "fit_parameter_record_count": fit_parameter_count,
            "validation_metric_record_count": validation_metric_count,
            "compute_metric_record_count": compute_metric_count,
            "bottleneck_stage_count": len(bottleneck_stages),
        },
        "recommended_loop": [
            "Hold the workload signature constant before comparing runs.",
            "For stochastic presets, treat the stochastic profile and resolved sim seed as part of the workload signature.",
            "Change a small, stage-local set of fit parameters at a time.",
            "Compare validation and compute records jointly instead of optimizing either in isolation.",
            "Promote a tuning change only when the validation gain is worth the compute regression, or the compute win preserves validation quality.",
            "Use scripts/profile_pipeline_performance.py to benchmark promising semantics-preserving variants after the single-run harness identifies likely bottlenecks.",
        ],
        "guardrails": [
            "Do not compare runs with different flight_name, n_steps, dt_seconds, pipeline mode, stochastic profile, or sim seed as if they were like-for-like tuning trials.",
            "Prefer stage-local tuning based on the bottleneck stages and attached validation reports.",
            "Treat missing validation sections as coverage gaps and close them before making broad claims about model quality.",
        ],
        "priority_stages": bottleneck_stages,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    fit_parameters = report.get("fit_parameters", {})
    simulation_context = report.get("simulation_context", {})
    validation_metrics = report.get("validation_metrics", {})
    compute_performance = report.get("compute_performance", {})
    methodology = report.get("methodology", {})
    lines = [
        "# Validation Harness Report",
        "",
        "## Workload Signature",
        "```json",
        json.dumps(report.get("workload_signature", {}), indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Simulation Context",
        "```json",
        json.dumps(simulation_context, indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Measurement Framework",
        "```json",
        json.dumps(methodology, indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Fit Parameter Coverage",
        "```json",
        json.dumps(
            {
                "pipeline_keys": sorted((fit_parameters.get("pipeline") or {}).keys()),
                "stage_count": len(fit_parameters.get("by_stage") or {}),
                "parameter_record_count": len(fit_parameters.get("parameter_records") or []),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        "```",
        "",
        "## Validation Metrics",
        "```json",
        json.dumps(
            {
                "report_keys": sorted((validation_metrics.get("overall") or {}).keys()),
                "metric_record_count": len(validation_metrics.get("metric_records") or []),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        "```",
        "",
        "## Compute Performance",
        "```json",
        json.dumps(
            {
                "overall": compute_performance.get("overall"),
                "scale_signature": compute_performance.get("scale_signature"),
                "bottleneck_stages": compute_performance.get("bottleneck_stages"),
                "metric_record_count": len(compute_performance.get("metric_records") or []),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        "```",
        "",
        "## Stage Harness",
    ]
    for stage in report.get("stage_harness", []) or []:
        lines.extend(
            [
                "",
                f"### {stage.get('stage_script')}",
                "```json",
                json.dumps(
                    {
                        "fit_parameters": stage.get("fit_parameters"),
                        "validation_report_keys": sorted((stage.get("validation_metrics") or {}).keys()),
                        "compute_performance": {
                            "elapsed_ms": ((stage.get("compute_performance") or {}).get("elapsed_ms")),
                            "share_of_total_elapsed": ((stage.get("compute_performance") or {}).get("share_of_total_elapsed")),
                            "summary_path": ((stage.get("compute_performance") or {}).get("summary_path")),
                            "manifest_path": ((stage.get("compute_performance") or {}).get("manifest_path")),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                ),
                "```",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def write_validation_harness_report(
    *,
    paths: RunPaths,
    manifest: dict[str, Any],
    full_run_report: dict[str, Any],
    flight: "FlightSpec",
) -> dict[str, Any]:
    engineering_performance = dict(full_run_report.get("engineering_performance") or {})
    engineering_stages = list(engineering_performance.get("stages") or [])
    stage_harness, stage_configs, stage_parameter_records = _build_stage_harness(engineering_stages=engineering_stages)

    pipeline_parameters = dict(manifest.get("pipeline") or {})
    pipeline_parameter_records = _flatten_parameter_records(
        pipeline_parameters,
        scope_name="pipeline",
        source_path=str(paths.manifest_path),
    )
    parameter_records = pipeline_parameter_records + stage_parameter_records

    overall_validation = dict(full_run_report.get("modeling_performance") or {})
    window_policy_profile = dict(full_run_report.get("window_policy_profile") or {})
    if window_policy_profile:
        overall_validation["window_policy_profile"] = window_policy_profile
    validation_metric_records = _build_validation_metric_records(
        overall_validation=overall_validation,
        stage_harness=stage_harness,
    )
    compute_metric_records = _build_compute_metric_records(
        engineering_performance=engineering_performance,
        stage_harness=stage_harness,
    )
    bottleneck_stages = _build_bottleneck_stages(stage_harness)

    workload_signature = {
        "source": dict(manifest.get("source") or {}),
        "simulation": dict(manifest.get("simulation") or {}),
        "pipeline": dict(manifest.get("pipeline") or {}),
        "stochasticity": resolve_flight_stochasticity(
            flight=flight,
            sim_seed=((manifest.get("pipeline") or {}).get("sim_seed")),
        ),
        "seed_counts": dict(manifest.get("seed_counts") or {}),
    }
    report = ValidationHarnessReport(
        report_version="v1",
        status=full_run_report.get("status"),
        run_dir=str(paths.run_dir),
        source_artifacts={
            "run_manifest_path": str(paths.manifest_path),
            "full_run_report_path": str(paths.run_dir / "reports" / "full_run_report.json"),
        },
        workload_signature=workload_signature,
        simulation_context=_build_simulation_context(flight=flight, manifest=manifest),
        fit_parameters={
            "pipeline": pipeline_parameters,
            "by_stage": stage_configs,
            "parameter_records": [record.to_payload() for record in parameter_records],
        },
        validation_metrics={
            "overall": overall_validation,
            "by_stage": {stage.stage_script: stage.validation_metrics for stage in stage_harness},
            "metric_records": [record.to_payload() for record in validation_metric_records],
        },
        compute_performance={
            "overall": dict(engineering_performance.get("overall") or {}),
            "scale_signature": dict(engineering_performance.get("scale_signature") or {}),
            "by_stage": {stage.stage_script: stage.compute_performance for stage in stage_harness},
            "metric_records": [record.to_payload() for record in compute_metric_records],
            "bottleneck_stages": bottleneck_stages,
        },
        stage_harness=stage_harness,
        methodology=_build_methodology(
            workload_signature=workload_signature,
            fit_parameter_count=len(parameter_records),
            validation_metric_count=len(validation_metric_records),
            compute_metric_count=len(compute_metric_records),
            bottleneck_stages=bottleneck_stages,
        ),
    )
    report_payload = report.to_payload()
    write_manifest(paths.run_dir / "reports" / "validation_harness_report.json", report_payload)
    (paths.run_dir / "reports" / "validation_harness_report.md").write_text(
        _render_markdown(report_payload),
        encoding="utf-8",
    )
    return report_payload
