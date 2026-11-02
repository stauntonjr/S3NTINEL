# Isolated simulation smoke validation

## Scope and failed baseline

Follow-up to the post-merge check of PR #65 at main commit `86cf747`, on
November 2, 2026. This change repairs the smoke-test runtime boundary and
adds hosted CI coverage. It does not change production pipeline stages,
model fitting, simulator observations, truth labels, or scoring thresholds.
The earlier [truth and interval-alignment validation](dgx_truth_alignment_validation.md)
remains a historical record of those separately completed checks.

The additional local post-merge suite reported 499 tests: 497 passed, one
skipped, one failed, with no errors, in 10,451.052 seconds. The failure was
`test_sim_runner_full_smoke_emits_bundle`, after 9,035.900 seconds. Stage 80
failed while writing scores with `java.lang.StackOverflowError`, following
repeated failed allocations of 16,777,200-byte Spark memory pages. The
recursive frames were in `TaskMemoryManager.allocatePage`, not application
recursion. The failed bundle is preserved locally under
`data/diagnostics/pr65_stackoverflow_20261102`.

The suite launched its JVM with a 2 GiB heap, but the failed run manifest
declared the runner's default `laptop_large_sim` profile: 8 GiB driver and
`local[4]`. Stopping a Spark context does not terminate PySpark's reusable
gateway JVM, so the old preflight `get_spark` / `stop` did not provide a
fresh runtime or apply a different launch-time heap. The old test also
converted Spark startup errors into skips. GitHub CI explicitly deselected
this smoke, so its four green post-merge checks did not exercise this path.

These are demonstrated sizing, isolation, and coverage defects. The
controlled success below supports fixing the harness, but does not prove
the complete retained-object history of the failed JVM or repair an
upstream Spark allocator.

## Runtime contract

The unchanged 12-step `power_chain` full pipeline now runs in a child Python
process with a fresh gateway JVM, `ci_small`, `local[2]`, and an actual heap
cap of 2 GiB. The test strips inherited Spark profile overrides and gateway
credentials, uses private temporary spill/warehouse directories, and sets a
360-second child deadline. It deliberately supplies conflicting parent
profile, heap, and gateway settings to test the isolation boundary.

The child checks the JVM's actual maximum heap and Spark master before
running the canonical pipeline. The parent verifies a different process
ID, the runtime receipt, declared manifest configuration, successful final
manifest/report status, and all existing stage/artifact/data assertions.
Missing Spark or startup failure now fails the test rather than skipping.
Normal cleanup stops the Spark context; on Linux, gateway stdin closes on
child exit, including timeout termination.

A dedicated GitHub Actions job runs this smoke once per PR/main push,
with Java 17, Python 3.11, the Spark dependencies, and a ten-minute job
deadline. Matrix jobs continue to deselect this case to avoid duplicate
execution. The larger public-API end-to-end simulation remains a separate
local validation gate.

## Host and local evidence

Hardware inspected before selecting the test budget: 20 CPUs, 121 GiB RAM
with approximately 83 GiB available, 15 GiB swap with 460 KiB used, and
approximately 2.0 TiB free on the shared spill/output filesystem.
Environment: `sentinel-spark35`, Parquet, CPU only. The test budget remains
2 GiB; no GPU service was interrupted and no production profile was changed.

The pre-change controlled probe ran the original smoke in a fresh process
with a matching `ci_small` profile and 2 GiB heap. It exited zero: one test,
147.402 seconds overall (147.12-second call). Its final manifest reports
success, all 16 stages, `ci_small`, 2 GiB, and `local[2]`.

The repaired runner integration module then exited zero: 11 passed, no
skips/failures/errors, in 143.929 seconds; the smoke call took 143.01
seconds. That run preceded the final extra conflicting-parent-heap input
and removal of the skip guard.

The complete final-code suite subsequently exited zero: **498 passed, one
skipped, zero failures or errors**, 499 tests in 1,579.849 seconds. The
smoke passed in 147.742 seconds, including the conflicting parent settings.
Its final run manifest reports success, `ci_small`, 2 GiB, and `local[2]`;
the pipeline summary records all 16 stages successful in 138.868 seconds.
The public-API end-to-end simulation also passed. The sole skip was
`test_upsert_table_overwrites_existing_merge_key_row`, because Delta JVM
classes are unavailable in this Parquet runtime.

Retained workspace evidence includes `dgx-main-pr65-pytest.xml` / `.log`,
`dgx-stage80-ci-profile-probe.xml` / `.log`,
`dgx-stage80-isolated-runner.xml` / `.log`, and the separately named
`dgx-stage80-fixed-pytest.xml` / `.log` final-suite attempt. Data bundles and
logs are ignored local artifacts, not checked-in datasets.

Read-only review found the skip guard and it was removed. No production
model quality, benchmark accuracy, or GPU execution is claimed by this
test/runtime repair.
