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
process with a fresh gateway JVM, the `ci_small` two-worker topology, and
an explicit 8 GiB driver override. Both the JVM launch arguments and
effective Spark configuration declare 8 GiB; the actual maximum heap is
checked independently. The test strips inherited Spark profile overrides and gateway
credentials, uses private temporary spill/warehouse directories, and sets a
900-second child deadline. It deliberately supplies conflicting parent
profile, heap, and gateway settings to test the isolation boundary.

The child checks the JVM's actual maximum heap and Spark master before
running the canonical pipeline. The parent verifies a different process
ID, the runtime receipt, declared manifest configuration, successful final
manifest/report status, and all existing stage/artifact/data assertions.
Missing Spark or startup failure now fails the test rather than skipping.
Normal cleanup stops the Spark context; on Linux, gateway stdin closes on
child exit, including timeout termination.

A dedicated GitHub Actions job runs this smoke once per PR/main push,
with Java 17, Python 3.11, the Spark dependencies, and a twenty-minute job
deadline. Matrix jobs continue to deselect this case to avoid duplicate
execution. The larger public-API end-to-end simulation remains a separate
local validation gate.

## Host and local evidence

Hardware inspected before selecting the test budget: 20 CPUs, 121 GiB RAM
with approximately 83 GiB available, 15 GiB swap with 460 KiB used, and
approximately 2.0 TiB free on the shared spill/output filesystem.
Environment: `sentinel-spark35`, Parquet, CPU only. The historical local
checks below used the old 2 GiB budget; no GPU service was interrupted and
no production profile was changed.

The pre-change controlled probe ran the original smoke in a fresh process
with a matching `ci_small` profile and 2 GiB heap. It exited zero: one test,
147.402 seconds overall (147.12-second call). Its final manifest reports
success, all 16 stages, `ci_small`, 2 GiB, and `local[2]`.

The repaired runner integration module then exited zero: 11 passed, no
skips/failures/errors, in 143.929 seconds; the smoke call took 143.01
seconds. That run preceded the final extra conflicting-parent-heap input
and removal of the skip guard.

The complete suite at `6b18671` subsequently exited zero: **498 passed, one
skipped, zero failures or errors**, 499 tests in 1,579.849 seconds. The
smoke passed in 147.742 seconds, including the conflicting parent settings.
Its final run manifest reports success, `ci_small`, 2 GiB, and `local[2]`;
the pipeline summary records all 16 stages successful in 138.868 seconds.
The public-API end-to-end simulation also passed. The sole skip was
`test_upsert_table_overwrites_existing_merge_key_row`, because Delta JVM
classes are unavailable in this Parquet runtime.

## Hosted timing qualification

The first dedicated hosted smoke, job `125324233693` in CI run
`41459387812`, failed with `subprocess.TimeoutExpired` at the initial
360-second deadline. The traceback did not expose enough captured child
output to establish progress or a stall; this is not evidence of another
allocator failure or a successful pipeline. The local success above does
not qualify hosted execution.

The follow-up allows a bounded 900 seconds, without changing the 2 GiB
heap, two-worker profile, pipeline input, or output assertions. Timeout
failures expose a bounded child-stderr tail. CI records its own host
resources and always prints final or partial manifests, stage summaries,
and run-log tails before cleanup. The twenty-minute job deadline leaves
room for dependency setup and diagnostics. This is a hosted timing probe,
not a performance or completion claim. The hosted rerun, rather than local
timing, determines whether this budget suffices; see
[PR #66](https://github.com/stauntonjr/S3NTINEL/pull/66) for the live CI receipt.

After this deadline/diagnostic-only adjustment, the complete runner module
again passed locally: 11 tests, no skips/failures/errors; smoke call 149.58
seconds. Injected timeout checks verified both bytes and text stderr
produce explicit failures with bounded diagnostic tails. The full-suite
receipt above is from before this adjustment; no production code or normal
pipeline assertions changed afterward.

## Hardware-sized smoke correction

The hosted retry at `435c7b0`, CI run `41462849453`, failed with an actual
stage-80 `SparkOutOfMemoryError`: an aggregation map could not acquire
262,144 bytes during score writing. It exited before the 900-second child
deadline, with a failed final manifest and pipeline summary. The four
other checks passed. This establishes failure under the imposed 2 GiB
budget, not a production scoring defect under adequate resources.

The hosted resource receipt recorded four CPUs, 15 GiB RAM with about
14 GiB available, 3 GiB unused swap, and 86 GiB free spill/output space.
The correction selects an 8 GiB driver heap, leaving host capacity for
the OS, Python workers, and JVM native memory. The two-worker topology,
shuffle/default parallelism, pipeline input, deadlines, and all output
assertions are unchanged, isolating the heap-sizing change. The existing
matrix fixtures retain their separate 2 GiB budget. No production scoring
code or DGX profile is changed; the full DGX mission profile remains
16 GiB with eight workers.

Before local qualification of the correction, the host was rechecked:
20 CPUs, 121 GiB RAM with about 82 GiB available, 15 GiB swap with 460 KiB
used, and about 2.0 TiB free spill/output space. The new smoke explicitly
checks an actual heap above 7 GiB and no greater than 8 GiB, rejecting an
accidentally reused smaller gateway. Its final manifest must independently
report the effective 8 GiB override and `local[2]`.

An isolated local PySpark 3.5.9 comparison had already passed the old
2 GiB smoke in 125.81 seconds (126.065 seconds overall), with a successful
final manifest and all 16 stages. Therefore the local/hosted Spark version
difference alone did not reproduce the hosted failure. The isolated
installation leaves the existing conda environment unchanged.

The corrected 8 GiB runner module passed all 11 tests, with zero skips,
failures, or errors, in 144.042 seconds. The smoke call took 143.10 seconds
and verified the actual heap bounds above. Independent inspection confirmed
the final manifest's success status and effective 8 GiB / `local[2]`
configuration; the pipeline summary recorded all 16 stages successful in
133.959 seconds. The workspace retains `dgx-stage80-sized8g-runner.xml`
and `.log`. Final full-suite and hosted receipts are recorded on
[PR #66](https://github.com/stauntonjr/S3NTINEL/pull/66); earlier 2 GiB passes
and failures above remain historical evidence, not substitutes for those
gates.

Retained workspace evidence includes `dgx-main-pr65-pytest.xml` / `.log`,
`dgx-stage80-ci-profile-probe.xml` / `.log`,
`dgx-stage80-isolated-runner.xml` / `.log`, and the separately named
`dgx-stage80-fixed-pytest.xml` / `.log` final-suite attempt. Data bundles and
logs are ignored local artifacts, not checked-in datasets.

Read-only review found the skip guard and it was removed. No production
model quality, benchmark accuracy, or GPU execution is claimed by this
test/runtime repair.
