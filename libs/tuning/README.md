# Tuning

`libs/tuning` owns run-level objective policy over measurement bundles.

It does not own:
- simulation-specific context extraction
- validator implementations
- pipeline stage orchestration

Those belong to `libs/simulation`, validator-owning packages, and `pipelines/`.

## Purpose

This package answers:
- which metrics should go up
- which metrics should go down
- which metrics are hard constraints
- whether a run is comparable enough to rank against other runs

The canonical input is the validation harness report produced from simulation runs.

## Contents

- `benchmark_variants.py`
  - reusable benchmark variant dataclass plus named variant-family policy for pipeline performance profiling, including the broad one-at-a-time `full_parameter_sweep` over the benchmark CLI tuning knobs
- `benchmark_search.py`
  - stage-local combinatorial search spaces and variant generation for bounded tuning loops, currently covering `profile`, `event`, `windowing`, `structure`, `phase`, and `anomaly`, including mixed arg/env search dimensions where needed
- `benchmark_reporting.py`
  - reusable benchmark result dataclass plus experiment-plan and summary payload/markdown builders
- `benchmark_planning.py`
  - reusable benchmark objective-resolution, replay-target inference, closure, and variant-plan policy
- `benchmark_runtime.py`
  - reusable child-run artifact loading, validation-metric extraction, replay drift classification, and result assembly
- `benchmark_execution.py`
  - reusable child run command construction and replay cloning/resume command helpers
- `benchmark_runner.py`
  - shared per-variant benchmark execution over the planning, execution, runtime, and reporting helpers
- `benchmark_invocation.py`
  - benchmark-dir naming and top-level benchmark CLI validation helpers
- `objectives.py`
  - objective dataclasses, default mode-aware objective specs, payload loading, and harness evaluation
- `presets.py`
  - named objective presets for reusable tuning-policy families
- `validation_panels.py`
  - reusable validation panel modes, defaults, shortlist policy, and panel-construction heuristics for summary/report surfaces
- `reporting.py`
  - persisted objective-evaluation report writing
