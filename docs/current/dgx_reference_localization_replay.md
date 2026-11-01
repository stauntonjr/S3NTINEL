# DGX Spark — paired aviation localization replay

This is a **DGX-hosted scientific validation**, not a GitHub Actions test.
See [ADR-0003](../decisions/0003-reference-fit-target-inference.md) for the fit/apply contract.

## Execution preflight

Run from a checkout of the desired commit on the **actual DGX host**. The
`dgx_spark` profile configures local CPU Spark on DGX; it does not enable GPU
acceleration.

```bash
export S3NTINEL_SPARK_PROFILE=dgx_spark
python - <<'PY'
import os
from libs.io.delta import describe_spark_runtime_config
cfg = describe_spark_runtime_config()
assert os.environ["S3NTINEL_SPARK_PROFILE"] == "dgx_spark"
print("Spark profile:", os.environ["S3NTINEL_SPARK_PROFILE"])
for key in ("spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions"):
    print(f"{key}: {cfg[key]}")
PY
```

Verify available RAM, free disk space, Java 17, and Spark/Python dependencies
before launching. Avoid running the full suite on a GitHub-hosted runner.

## Paired nominal reference and faulted target

Start with a bounded, seeded diagnostic, then scale only after a successful
artifact-lineage and report check:

```bash
python scripts/run_sim_reference_inference.py \
  --flight-name power_pressurization_hierarchy_composite \
  --sim-seed 17 \
  --n-steps 6 \
  --dt-seconds 1.0 \
  --base-dir data/dgx_reference_inference \
  --format parquet \
  --write-mode overwrite
```

This command performs **three pipeline executions**: nominal reference fit,
faulted self-fit, and faulted inference using copied reference artifacts.
It resets Spark between runs and writes
`reports/reference_inference_comparison_report.json` under the timestamped
pair directory. Six steps are a smoke diagnostic, **not** a representative
aviation benchmark.

Inspect the report's `artifact_lineage`, `phase_comparison`,
`score_comparison`, and `candidate_evidence_comparison`. Preserve the
run manifest and log files for all three runs. Do not accept localization
metrics unless the observed-value contract remains `parameter_value_only`
and the reference inference does not refit target observations.

## Decision gate

1. Confirm all three runs succeed and report lineage is complete.
2. Compare self-fit and reference-inference phase assignments, emitted scores,
   candidate support/rank changes, and structural/telemetry cuts.
3. Repeat at the intended benchmark scale on DGX, record actual CPU, RAM,
   disk, and Spark runtime settings, and run the existing benchmark-tier gates.
4. Classify first failures as detection, parameter, subsystem, or module;
   prioritize subsystem truth-mapping gaps before a model change.
5. Keep simulator truth and clean parameter values validation-only.

Do not infer performance from GitHub's `ci_small` profile. No GPU execution
is implied by the `dgx_spark` name.
