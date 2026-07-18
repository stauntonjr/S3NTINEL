# Smoke and Simulation Consistency

The local smoke pipeline is a structural check, not a substitute for a simulation
benchmark. It must execute the same persisted stage modules and artifact contracts
as the simulation runner, while simulation replays remain the authority for
positive anomaly detection and attribution behavior.

## Requirements

- Keep `scripts/smoke_test_pipeline.py::SMOKE_STAGE_SCRIPTS` synchronized with the
  canonical fitting and inference stage lists in `pipelines/plans.py`.
- Every smoke input consumed by a stage must be written to the same canonical
  artifact path that the stage runtime resolves. Do not maintain a separate
  input-only fixture that stages do not read.
- New persisted artifacts must be added together to the runtime path resolver,
  smoke path setup, row-count inventory, and active contract checks.
- Smoke seed profiles must be computed from clean telemetry; injected smoke
  excursions must be explicit and must not silently alter baseline profiles.
- Smoke fixtures must declare whether they are structural-only or anomaly-positive.
  A structural smoke may legitimately emit zero `emit_ready` rows.
- Positive attribution assertions must run against a simulation replay or a
  fixture with enough warm windows for the configured conformal calibration.
  A one-window fixture must not be used to claim detector recall.
- Simulation runs must feed canonical telemetry and simulation truth through the
  same stage entrypoints used by smoke. Do not add a simulation-only modeling path
  to make smoke assertions pass.
- When a stage, artifact, or schema changes, run both the local smoke and the
  narrowest affected simulation replay before updating the contract inventory.

## Verification

Structural smoke:

```bash
S3NTINEL_SPARK_PROFILE=laptop_large_sim \
  python -m scripts.smoke_test_pipeline \
  --base-dir data/smoke \
  --format parquet \
  --min-warm 1
```

Simulation-backed positive validation:

```bash
  python -m scripts.run_sim_pipeline \
  --flight-name power_pressurization_hierarchy_composite \
  --base-dir data/simulation_runs \
  --mode full \
  --format parquet
```

Then inspect the persisted replay with `scripts/report_sim_replay.py` and run the
simulation validation or benchmark gate appropriate to the changed stage.
