"""Compatibility wrappers around the generic native dataset runtime."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import pandas as pd

from libs.behavior import BehaviorStepInput
from libs.simulation.assembly_runtime import AssemblyRuntime
from libs.simulation.native_dataset import (
    build_native_dataset_context,
    native_samples_to_rows,
    simulate_native_dataset,
)
from libs.simulation.specs import HierarchyAssemblySpec


NativeSimulationExampleContext = AssemblyRuntime


def build_example_context(assembly_spec: HierarchyAssemblySpec) -> NativeSimulationExampleContext:
    return build_native_dataset_context(assembly_spec)


samples_to_rows = native_samples_to_rows


def simulate_example(
    *,
    context: NativeSimulationExampleContext,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]],
    phase_label_for_step: Callable[[int], str | None],
) -> pd.DataFrame:
    telemetry_df, _ = simulate_native_dataset(
        context=context,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
    )
    return telemetry_df
