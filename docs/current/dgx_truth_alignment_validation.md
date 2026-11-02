# DGX truth and interval-alignment follow-up

## Scope and baseline

Follow-up to PR #64, starting from merged commit `21f3e3a` on November 1,
2026 (local time). Historical bundles and measurements in the
[original validation record](dgx_reference_localization_validation.md) are
preserved. This repair changes simulator truth metadata and comparison
alignment, not generated observations, model fitting, or scoring thresholds.

Two defects were reproduced before implementation:

- `Module.iter_samples` treated missing coupling activity flags as true.
  Ordinary wiring identity therefore labeled nominal observations faulty,
  and could overwrite active-but-unapplied parameter fault metadata.
- Cross-run joins treated adaptive `win_id` ordinals as shared identities,
  even when the branches used different time intervals.

Review additionally identified legitimate repeated intervals: event-budget
closure can produce several windows at one timestamp. The regression
reproduced overcounting from one-sided and two-sided duplicate intervals
before the cardinality-safe implementation.

## Corrected contracts

Nominal transfers keep coupling identity without creating activity, applied,
window, family, or anomaly labels. Explicit active coupling metadata takes
priority over nominal ports. Applied parameter faults retain their existing
precedence, and nominal transfers do not overwrite unapplied parameter
fault context. Current and deprecated fault aliases remain consistent.

Report version 2 declares
`window_alignment: unique_exact_interval_tail_flight_date`. Phase comparison
requires complete exact interval keys unique within each branch. Candidate
comparison resolves each branch's own window IDs, rejects ambiguous window
identities/intervals and duplicate interval/parameter identities, and compares
only unique complete matches. Raw branch totals are retained: missing,
ambiguous, and unpaired rows count as unmatched rather than disappearing or
forming Cartesian products. Rank/cut changes use null-safe comparisons.

Score/emission counts are marginal branch totals. Phase IDs originate in
independently fitted seed buckets; ID disagreement is diagnostic, not proof
of semantic phase equivalence, detection accuracy, or localization quality.
No cluster remapping, benchmark-tier accuracy qualification, or multi-label
representation of simultaneous parameter/coupling faults is included.

## Host and verification setup

Measured before selecting profiles: 20 CPUs, 121 GiB RAM with approximately
82 GiB available, 15 GiB swap with 460 KiB used, and approximately 2.0 TiB
free on the shared spill/output filesystem. Environment `sentinel-spark35`,
Parquet, CPU profile `dgx_spark` (`local[8]`, 16 GiB driver, 4 GiB result
limit, 32 shuffle partitions, default parallelism 16, AQE/coalescing and
Kryo). No GPU execution or explicit Spark overrides. The project suite uses
its existing local single-worker fixture and a 2 GiB driver heap.

## Verification status

All 33 focused regression/unit tests pass, including nominal companion
simulation, real activity/alias propagation, active-but-unapplied parameter
faults, active input precedence, shifted boundaries, changed IDs, null
boundaries/evidence, orphan candidates, and one-/two-sided duplicate
intervals. Lint and changed-file formatting pass.

The initial complete-suite attempt was affected by the agent's interrupt
request: PySpark's signal handler called `cancelAllJobs` on a stopped
context during
`test_smoke_localization_focus_family_packs_filter_fault_types`. This is
not accepted as a clean suite result; its log and JUnit file are retained
separately (493 passed, one skipped, one interrupt-induced failure;
495 tests in 1,559.781 seconds).

The fresh complete project suite exited zero on the final code: **498
passed, one skipped, zero failures or errors**, 499 tests in 1,577.627
seconds with the same 2 GiB driver heap. The sole skip is
`tests/regression/delta/test_delta_upsert.py::test_upsert_table_overwrites_existing_merge_key_row`:
Delta JVM classes are unavailable in this Parquet runtime. The previously
interrupted example-builder test passes in this clean run.

## Bounded 600-step replay

Seed 17, 600 steps at 0.5 seconds, using the runbook command with base
`data/dgx_truth_alignment_20261101_600`. Bundle:
`20261102T022838Z_power_pressurization_hierarchy_composite_reference_comparison`.

| Execution | Status | Stages | Seconds | Raw rows | Events | Windows | Candidate rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Nominal reference fit | success | 16 | 165.584 | 21,000 | 30 | 75 | 321 |
| Faulted self-fit | success | 16 | 155.927 | 21,000 | 30 | 75 | 354 |
| Reference inference | success | 9 | 48.856 | 21,000 | 30 | 75 | 1,022 |

The process exited zero. Independent manifest/stage/Parquet checks pass;
all label-contract reports are `ok`, all 19 reference artifact trees are
byte-identical after inference, and phase execution is `apply_reference`
with no target structural-fit stages.

Generic applied flags fell from 11,901 to **zero** in the nominal reference.
Target executions each contain 645 active rows and 460 applied rows, all
applied rows carrying authored window IDs. The distinction between active
and applied is preserved; deprecated fault aliases agree with current flags.
Every observed and clean value in all three executions (63,000 rows keyed
by tail, flight, timestamp, and parameter) exactly equals the retained
pre-fix baseline.

The bounded replay started before the duplicate-interval review repair.
Its original report is preserved. The final comparison builder was then
run against the same immutable tables and lineage, writing the separate
`reports/reference_inference_comparison_report_unique_intervals.json`.
Independent PyArrow inspection confirmed complete unique intervals and
reproduced all final phase/candidate comparison counts:

- 75 matched windows, zero unmatched per branch, zero phase-ID disagreements.
- Emit-ready marginal totals: self-fit 20, inference 58.
- 341 matched candidates, unmatched self-fit 13 / inference 681.
- Rank changes 50, telemetry-cut changes 13, structural-cut changes 13.

No observation/model replay was necessary for this reporting-only
cardinality repair; full-mission validation uses the final code throughout.

## Full 3,360-step paired mission

Seed 17, preset defaults (3,360 steps at 0.5 seconds), base
`data/dgx_truth_alignment_20261101_full`. Bundle:
`20261102T032115Z_power_pressurization_hierarchy_composite_reference_comparison`.

| Execution | Status | Stages | Seconds | Raw rows | Events | Windows | Candidate rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Nominal reference fit | success | 16 | 198.256 | 117,600 | 68 | 308 | 745 |
| Faulted self-fit | success | 16 | 190.852 | 117,600 | 135 | 314 | 1,471 |
| Reference inference | success | 9 | 66.483 | 117,600 | 136 | 318 | 2,161 |

The final-code process exited zero. All three manifests/stage summaries and
label-contract reports pass. All 19 reference artifact trees remain
byte-identical after inference, and phase execution is `apply_reference`
with no target structural-fit stages.

Independent PyArrow checks confirmed every observed and clean parameter
value exactly matches the retained full-mission baseline (352,800 rows
across the three executions). Nominal generic applied flags fell from
93,321 to **zero**. Each target has 3,849 active rows and 3,409 applied rows;
all applied rows have authored window IDs, and current/deprecated flags
agree. The correction did not change model outputs or emission counts.

The version-2 report was independently reproduced from persisted tables.
Recomputing exact-interval alignment on the historical baseline gives the
same corrected counts, isolating this change to comparison semantics:

| Comparison | Old ID join | Correct unique interval join |
| --- | ---: | ---: |
| Matched windows | 314 | 79 |
| Phase-ID disagreements | 120 | 54 |
| Matched parameter candidates | 604 | 182 |
| Candidate support-rank changes | 165 | 27 |
| Telemetry-cut changes | 117 | 14 |
| Structural-cut changes | 71 | 13 |

Unmatched windows: self-fit 235, reference inference 239. Unmatched candidate
rows: self-fit 1,289, reference inference 1,979. Emit-ready marginal totals
remain 78 versus 121. These uncovered intervals/candidates and independent
phase-ID semantics prevent interpreting the report as a localization
accuracy comparison; benchmark-tier accuracy gates remain outside this fix.

## Retained evidence

All data bundles are ignored local artifacts, not checked-in datasets. The
workspace retains the old and new bundles, pipeline logs
`dgx-truth-alignment-600.log` / `dgx-truth-alignment-full.log`, initial and
final suite logs/JUnit files, the separate rebuilt bounded report, and
read-only verification scripts for lineage, numeric-row equality, truth
flags, and independently reconstructed interval comparisons.

Read-only review found and resolved the interval-cardinality issue, then
accepted the corrected implementation/contracts with no further findings,
conditional on completing final verification. Semantic cluster remapping,
simultaneous-fault multi-label representation, and benchmark accuracy are
explicitly excluded, not assumed proven.
