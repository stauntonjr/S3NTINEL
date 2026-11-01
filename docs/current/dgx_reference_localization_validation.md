# DGX reference-inference validation, November 1, 2026

## Source and failure history

The baseline is PR #64 at `e5125f05cda3d36aefa64f9c43e7c7167d21d43b`,
plus the working changes on `fix/dgx-reference-replay`.

The six-step command was introduced by `e5125f0` on October 31, 2026
(November 1 UTC). It does not establish a regression from a previously
working six-step composite run. The underlying unguarded empty backbone
aggregation originated in `644c90b` on March 30, 2026. The paired runner
was introduced in `ec4b66e` on July 19, 2026.

The unchanged six-step example failed after 37.276 seconds in nominal
reference stage `40_backbone_fit.py`. It persisted 294 raw rows, zero events,
and zero windows, then called Spark aggregation with no selected backbone
sensors (`AssertionError: exprs should not be empty`). All six phase labels
were `gate_turnaround`; the first authored fault starts at step 180.
Neither target execution started and no comparison report was produced.

## Host and selected runtime

Measured before selecting the execution profile:

- Host `spark-3a8f`, ARM64, 20 available CPUs.
- RAM 121 GiB total, approximately 82 GiB available at fix preflight;
  15 GiB swap with no swap in use at that point.
- Spill `/tmp` and output `/home/jrs` share a 3.7 TiB filesystem with
  approximately 2.0 TiB free.
- `sentinel-spark35`: Python 3.11.15, Spark 3.5.1, OpenJDK 17.0.18,
  PyArrow 25.0.0, pandas 2.3.3.
- CPU Spark profile `dgx_spark`: `local[8]`, 16 GiB driver heap,
  4 GiB result limit, 32 shuffle partitions, default parallelism 16,
  adaptive execution and coalescing enabled, Kryo serialization.
- Parquet; no explicit Spark setting overrides.

## Bounded paired diagnostic

Command: the [runbook's bounded diagnostic](dgx_reference_localization_replay.md),
with seed 17, 600 steps, and the preset's native 0.5-second cadence.

Bundle: `data/dgx_reference_inference_20261101_fix600/20261101T122837Z_power_pressurization_hierarchy_composite_reference_comparison`.
This is a local validation artifact, not a checked-in dataset.

| Execution | Result | Stages | Seconds | Raw rows | Events | Windows | Candidate rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Nominal reference fit | success | 16 | 161.928 | 21,000 | 30 | 75 | 321 |
| Faulted self-fit | success | 16 | 154.557 | 21,000 | 30 | 75 | 354 |
| Reference inference | success | 9 | 46.755 | 21,000 | 30 | 75 | 1,022 |

The process exited zero. All child manifests and stage summaries record
success. Independent checks of the persisted report, stage summaries,
Parquet row counts, and file SHA-256 hashes confirmed:

- All 19 declared reference artifacts remain byte-identical to their nominal
  sources after inference, including scaling, structural, and phase models.
- Target phase execution is `apply_reference`; no profiling, backbone,
  graph, or hierarchy fitting stages run in reference inference.
- The report declares `observed_value_contract: parameter_value_only`;
  the label-contract report has status `ok` and no failures.
- All 75 windows match between self-fit and reference inference, with no
  phase assignment changes in this bounded prefix.
- Emit-ready windows: self-fit 20, reference inference 58.
- Matched candidate rows: 341; support ranks changed on 50 rows,
  telemetry cuts on 13, and structural cuts on 13.
- Applied authored fault-window rows: nominal reference 0, self-fit 460,
  reference inference 460. Seeded nominal observation noise is retained.

These are execution and comparison results, not proof of localization
accuracy. The bounded prefix does not cover the full mission or every
fault family.

### Existing metadata limitation

The generic `misbehavior_applied` / `fault_applied` flags also mark ordinary
coupling transfers because `Module.iter_samples` defaults missing coupling
activity flags to true. Those lines date to `d453031` on March 19, 2026.
In the nominal bounded reference, all 11,901 flagged rows have coupling
identity but no authored window ID, family/detail label, fault type, or
nonzero anomaly score. Nominal observed-versus-clean differences also include
the intended seeded observation noise. Generic activity flags alone therefore
do not establish authored fault prevalence. This execution fix does not
change simulator truth metadata semantics; fault coverage checks use authored
window IDs and labels.

## Full paired mission

Command: the runbook's full-mission command, seed 17 with no horizon or
cadence override. All three manifests confirm 3,360 steps at 0.5 seconds.

Bundle: `data/dgx_reference_inference_20261101_full/20261101T123507Z_power_pressurization_hierarchy_composite_reference_comparison`.

| Execution | Result | Stages | Seconds | Raw rows | Events | Windows | Candidate rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Nominal reference fit | success | 16 | 206.326 | 117,600 | 68 | 308 | 745 |
| Faulted self-fit | success | 16 | 182.726 | 117,600 | 135 | 314 | 1,471 |
| Reference inference | success | 9 | 67.175 | 117,600 | 136 | 318 | 2,161 |

The process exited zero. Independent manifest, stage, Parquet, label-contract,
and SHA-256 checks pass. All 19 reference artifacts remain byte-identical
after inference, phase execution is `apply_reference`, and no structural fit
stages ran on the target. Authored applied-fault rows: nominal 0, both target
executions 3,409. All three label-contract reports have status `ok`.

The comparison report records 314 matched window IDs, 120 phase-assignment
changes, and emit-ready counts of 78 for self-fit versus 121 for reference
inference. Candidate rows matched on ID/parameter: 604; support ranks changed
on 165, telemetry cuts on 117, and structural cuts on 71.

The two target executions emit different window counts because reference
inference applies the nominal window policy. The existing comparison joins
by window ID rather than verifying identical time intervals, so these counts
must not be presented as a like-for-like localization-accuracy improvement.
Benchmark-tier gates and a separate temporal-alignment/accuracy analysis are
not included in this execution repair.

## Regression checks

The 11 reference-inference unit tests and complete 14-test backbone integration
file pass. New regressions were observed failing before their corresponding
fixes. Lint, changed-file formatting, and Markdown validation pass.

The first complete project suite ran 483 tests in 1,789.863 seconds:
481 passed, one skipped, and one failed. The failure was
`tests/integration/simulation/test_dataset.py::test_canonical_sim_rows_flow_into_events_windows_and_window_features`,
with `java.lang.OutOfMemoryError: Java heap space` under a 2 GiB test driver.
That integration fixture inlined detector/window query lineage into feature
joins rather than using the canonical runner's persisted stage boundaries.

The fixture now writes and reloads raw, event, and window Parquet artifacts,
retaining every existing assertion. All four tests in that module then
passed under the same 2 GiB heap; the repaired case took 17.768 seconds in
that module run. Its existing CI deselection was removed so this path is
exercised on future PRs.

The fresh complete-suite run, including the four additional unit cases added
during the first run, exited zero: **486 passed, one skipped, zero failures
or errors**, 487 total, in 1,520.904 seconds under the same 2 GiB test heap.
The skip is
`tests/regression/delta/test_delta_upsert.py::test_upsert_table_overwrites_existing_merge_key_row`:
Delta JVM classes are unavailable in this Parquet-configured runtime.

Independent read-only code review found no introduced correctness issues in
the execution guards, horizon checks, regression tests, or CI fixture repair.
The final changed-file lint/format checks, Markdown validation, and
`git diff --check` pass. No production scoring thresholds or simulator truth
semantics were changed.
