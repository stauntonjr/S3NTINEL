# DGX Spark — paired aviation localization replay

This is a **DGX-hosted scientific validation**, not a GitHub Actions test.
See [ADR-0003](../decisions/0003-reference-fit-target-inference.md) for the fit/apply contract.

## Execution preflight

Run from a checkout of the desired commit on the **actual DGX host**, in the
`sentinel-spark35` conda environment. Inspect and record capacity **before**
selecting a Spark profile. Check both the intended spill filesystem (`/tmp`
unless overridden) and the output filesystem:

```bash
conda activate sentinel-spark35
mkdir -p data/dgx_reference_inference
preflight_dir=$(mktemp -d /tmp/s3ntinel-dgx-preflight.XXXXXX)
{
  date -u
  hostname
  git rev-parse HEAD
  nproc
  free -h
  df -h /tmp data/dgx_reference_inference
  java -version
  python -c 'import sys, pyspark; print(sys.version); print("Spark", pyspark.__version__)'
} 2>&1 | tee "$preflight_dir/hardware.txt"
```

Select `dgx_spark` only when the observed CPU, available RAM, swap use, and
free space support eight local workers and a 16 GiB driver heap alongside
other host workloads. Otherwise select a smaller profile or explicit
`S3NTINEL_SPARK_*` overrides and record those settings. The profile configures
CPU Spark; it does not enable GPU acceleration.

```bash
export S3NTINEL_SPARK_PROFILE=dgx_spark
export S3NTINEL_TABLE_FORMAT=parquet
python - <<'PY' | tee "$preflight_dir/spark-runtime.txt"
import os
from libs.io.delta import describe_spark_runtime_config
cfg = describe_spark_runtime_config()
assert os.environ["S3NTINEL_SPARK_PROFILE"] == "dgx_spark"
print("Spark profile:", os.environ["S3NTINEL_SPARK_PROFILE"])
for key, value in sorted(cfg.items()):
    print(f"{key}: {value}")
PY
```

Keep these preflight records with the initial diagnostic's logs and manifests.
Java 17 and Spark 3.5 are required. Avoid running the full suite on a
GitHub-hosted runner.

## Paired nominal reference and faulted target

Start with a bounded, seeded diagnostic, then scale only after a successful
artifact-lineage and report check:

```bash
python -m scripts.run_sim_reference_inference \
  --flight-name power_pressurization_hierarchy_composite \
  --sim-seed 17 \
  --n-steps 600 \
  --dt-seconds 0.5 \
  --base-dir data/dgx_reference_inference \
  --format parquet \
  --write-mode overwrite
```

This command performs **three pipeline executions**: nominal reference fit,
faulted self-fit, and faulted inference using copied reference artifacts.
It resets Spark between runs and writes
`reports/reference_inference_comparison_report.json` under the timestamped
pair directory. The 600-step prefix preserves the preset's native 0.5-second
cadence and includes early bias and coupling fault windows. It is a bounded
diagnostic, **not** a representative aviation benchmark.

Do not truncate this preset to six steps: those samples are all in
`gate_turnaround`, the first authored fault starts at step 180, and this
prefix produces no events or adaptive windows. The paired runner rejects
horizons that end before the first authored fault. Backbone fitting also rejects
empty training windows instead of creating an invalid model.

After the bounded diagnostic passes its lineage and report checks, repeat
without `--n-steps` or `--dt-seconds` to use the complete authored mission
(3,360 steps at 0.5 seconds):

```bash
python -m scripts.run_sim_reference_inference \
  --flight-name power_pressurization_hierarchy_composite \
  --sim-seed 17 \
  --base-dir data/dgx_reference_inference_full \
  --format parquet \
  --write-mode overwrite
```

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

For the recorded baseline failure, verified paired runs, and the existing
generic fault-flag metadata limitation, see the
[DGX validation record](dgx_reference_localization_validation.md).
