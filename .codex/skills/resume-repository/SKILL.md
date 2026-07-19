---
name: resume-repository
description: Use when returning to this repository after a long pause, changing IDE or agent tooling, rebuilding the environment, switching branches, or needing a trustworthy development baseline before implementation.
---

# Resume Repository

Re-establish a reliable local baseline before proposing or implementing model
changes. This skill is diagnostic by default and does not modify production
code, generated reports, or Git history.

## Workflow

1. Inspect `git status`, current branch, recent commits, and changed files.
   Preserve unrelated work and call out staged versus unstaged changes.
2. Read `README.md`, the relevant package README, `docs/current/`, and any
   active plan covering the likely workstream. Prefer current code and package
   READMEs over generated architecture snapshots.
3. Verify the runtime in `sentinel-spark35`:

   ```bash
   conda run -n sentinel-spark35 python --version
   conda run -n sentinel-spark35 python -c "import pyspark, yaml; print(pyspark.__version__)"
   java -version
   ```

4. Run cheap checks before Spark workloads:

   ```bash
   conda run -n sentinel-spark35 pytest tests/unit tests/contracts
   ```

5. Before selecting a Spark profile or explicit Spark resource overrides for a
   smoke, simulation, replay, or benchmark run, inspect the local hardware:

   ```bash
   nproc
   free -h
   df -h /tmp data
   ```

   Record the CPU count, total/available memory, swap, and free space on the
   intended spill/output filesystems. Choose the profile and any memory or
   parallelism overrides from those observations. Do not assume that a profile
   is appropriate from its name alone.
6. Run the smallest representative parquet smoke path with the justified Spark
   profile or overrides:

   ```bash
   conda run -n sentinel-spark35 python -m scripts.smoke_test_pipeline \
     --base-dir data/resume_smoke --format parquet
   ```

7. For modeling work, run the narrowest relevant benchmark gate and preserve
   its report paths. Do not start with a full composite replay unless the
   smoke and targeted gates are healthy.
8. Summarize the baseline with:
   - environment status
   - observed local hardware and selected Spark profile/overrides
   - Git/worktree state
   - tests and smoke results
   - current reports and metrics
   - blockers or stale documentation
   - the smallest justified next workstream

## Boundaries

- Do not recreate environments or install dependencies without explicit user
  authorization for the external write or network operation.
- Do not overwrite existing `data/`, `reports/`, or replay bundles; use a new
  clearly named baseline directory.
- Do not infer that a passing unit suite proves Spark or end-to-end health.
- If the environment is unavailable, report the exact failing command and
  continue with read-only repository inspection.
