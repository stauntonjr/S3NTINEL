# Simulator conditional-information pilot

Status: approved by the user on November 2, 2026. No evaluation has run.

## Intent and scope

Evaluate draft PR #57's discrete conditional-information model on bounded,
simulator-generated aviation telemetry. Determine whether cross-sensor
conditioning improves held-out nominal predictive efficiency and whether its
window surprise detects authored faults at a separately calibrated false-alarm
budget. Negative or inconclusive results are valid outcomes.

The user selected the simulator approach over a synthetic discrete-data test.
The concrete choices below originated as engineering defaults and were approved
with this protocol. This is a small research harness, but a full design
is warranted because encoding and leakage can invalidate the experiment.

## Approaches

1. **Selected: bounded simulator evaluation.** Reuse the existing simulator and
   its observed-value contract, with nominal-only encoding and disjoint seed
   sets. This exercises realistic mixed rates, noise, and authored faults; it
   needs explicit synchronization, finite alphabets, and truth separation.
2. Synthetic discrete sequences. Cheap and analytically interpretable, but
   would not establish behavior on the project's simulator. Existing unit tests
   already cover a simple coherence break.

Production integration is not an alternative in this pilot: no evidence yet
supports changing stage-80 scoring or stage-90 attribution.

## Existing boundaries

- Baseline research revision: `05fd5b4638fda1c6c5bb02a1c48169e741559c20`.
- `CategoricalPredictor` consumes complete synchronized states, finite integer
  alphabets, and a fixed within-state decoding order. It does not consume raw
  floating telemetry, missing values, or arbitrary parent cycles.
- `Flight.simulate_rows` emits canonical observed `parameter_value` rows at
  parameter-specific rates; values are strings and include categorical states.
- Use `resolve_flight` and `nominal_companion_flight`; create a fresh runtime
  for each mission. Do not reuse mutable flight state between roles.
- Simulator clean values, fault flags, fault names, hierarchy truth, and phase
  labels must never enter the symbol encoder, fitted model, parent selection,
  calibration thresholds, or model selection. Authored fault metadata belongs
  only in the post-scoring evaluator. Static parameter datatype/rate declarations
  are input-schema information, not fault labels.

## Frozen protocol

### Missions and splits

Use `power_pressurization_hierarchy_composite`, its full 3,360 steps at 0.5
seconds, not the earlier 600-step diagnostic prefix. Generate 16 missions:

| Role | Seeds | Fault program | Permitted use |
| --- | --- | --- | --- |
| Fit | 101, 102, 103, 104 | Nominal companion | Encoding, parent selection, count fitting |
| Calibration | 201, 202, 203, 204 | Nominal companion | Thresholds only |
| Nominal test | 301, 302, 303, 304 | Nominal companion | Held-out cross-entropy and false alarms |
| Faulted test | 301, 302, 303, 304 | Authored composite | Detection and paired descriptive contrasts |

The two test roles intentionally share each seed for nominal/faulted pairing;
no seed crosses fit, calibration, and test partitions. Keep the preset mission
template and start time fixed, with distinct role/seed flight identifiers.
This tests seed variation within one authored mission template, not unseen
flight schedules, aircraft, or real aviation data. The scenario seed also
controls role profiles and fault magnitudes, not just observation noise;
preserve those seeded variations and report the realized fault parameters.
Verify seed changes actually change observed values and same-seed reruns
reproduce them; stop if not.

Freeze and hash the protocol, sensor list, encoder, alphabets, parent maps,
fitted counts, and thresholds before accessing either test role's scores.
Do not select better seeds, encodings, orders, thresholds, or faults after
seeing test results. A revised protocol is a new experiment, not a replacement
for an unfavorable run.

### Synchronization and bounds

Use all uniquely named parameters in the preset, capped at 64. Use observed
values only at common native sample steps: the least common multiple of each
parameter's declared emission stride under `FlightTick._should_emit_sample`.
For the current 0.5/1/2 Hz channels this is every four simulator steps (2 s).
Validate rates and integer-compatible strides explicitly. Do not interpolate,
forward-fill, use clean values, or choose timestamps from fault outcomes.

Every selected grid step must have exactly one finite/valid observed value
for every declared sensor. Reject duplicate identities, missing grid rows,
unexpected sensors, non-finite numeric values, or incompatible sample rates;
report the exact reason rather than dropping data. Count original rows,
retained rows, off-grid rows, synchronized states, and rate-specific retention.
Retain original timestamps and step indices. No context crosses missions.

Process one bounded mission at a time; retain synchronized symbols and score
tables on disk rather than accumulating all raw missions. Cap raw rows at
250,000 per mission. These are reference-harness bounds, not production scale
or a 2 GiB Spark memory requirement.

### Nominal-only finite encoding

Fit one encoder per sensor using fit-role observed values only.

- Numeric: eight nominal interior bins using empirical fit octiles (linear
  quantiles), plus explicit underflow and overflow symbols. Save fit min/max
  and seven interior cuts. Values below min map to 0; above max to 9; otherwise
  map to `1 + bisect_right(cuts, value)` (1 through 8). Duplicate cuts are kept,
  including constant channels. The declared alphabet is always 0 through 9.
- Categorical: sorted fit-role string vocabulary, at most 16 distinct values;
  assign symbols 1 through N and reserve 0 for unseen categories. Preserve
  exact observed strings; do not silently numerically reinterpret categorical
  declarations. Save the mapping and report unknown rates on every role.
- Use the preset's declared datatype to choose numeric versus categorical.
  Unknown datatypes or bound violations stop the run; no target-based fallback.

Report bin occupancy, constant channels, overflow/underflow/unknown rates, and
the losses inherent in common-rate subsampling. These are representation
diagnostics, not evidence that a detected surprise is a fault.

### Models and parent-order sensitivity

Compare the existing marginal, first-order per-sensor temporal, and conditional
outputs with additive smoothing fixed at alpha=1. No tuning or new density
estimator. Preserve the existing unseen-context marginal fallback.

Primary order: lexical parameter-name order. Each sensor has at most one
earlier sensor parent, chosen by highest empirical same-step mutual information
on fit-role symbols only; lexical tie-break; choose no parent when all values
are zero. Compute mutual information from pooled fit states, not labels.
This is a predictive association, not a claim of physical causality.

Run a predeclared reverse-lexical order sensitivity arm with the same one-parent
rule and the same fit/calibration/test data. Fit and calibrate it independently
before test scoring. Report both arms, never pick the favorable test arm.
Marginal/temporal outputs should be order-invariant, which is also a testable
implementation contract. Record parent support and unseen-context fallback
rates to expose sparse-count effects.

### Scores, thresholds, and truth

Use fixed nonoverlapping 20-second windows, anchored at mission start, with
half-open time intervals. Require complete windows; report and exclude any
final partial window explicitly. For each model, window score is mean ideal
bits per sensor observation. Calibration/test windows use identical boundaries.

For each arm/model, sort its N nominal calibration-window scores. At a nominal
1% false-alarm budget, threshold at the 1-indexed order statistic
`ceil((N+1)*0.99)`, capped at N; alert only when score is strictly greater.
Record N, ties, threshold, and empirical calibration exceedances. If the index
exceeds N, report inadequate calibration resolution and do not claim a 1%
guarantee. Correlated windows invalidate an independent-sample guarantee in any
case; 1% is a calibration target, not a guaranteed held-out false-alarm rate.

Truth is loaded separately after scores are frozen. Use authored nonempty
window identities and half-open step/time intervals from the fault program,
not generic coupling/activity flags. Report declared intervals separately from
observed applied-fault support. A fault interval is detected when any alert
window overlaps it; report counts, rates, overlap durations, and overlapping
fault identities so one alert is not mistaken for independent successes.
Include all declared intervals in denominators, with explicit zero-support
and off-grid-only cases; do not silently drop undetected or unobserved faults.

Report results by authored fault detail/family and mission seed. Inventory
which of cross-sensor, temporal, stuck-at, and drift challenges are genuinely
present; mark absent families as not covered rather than injecting new faults
or claiming that the full research gate passed. No label-guided sensor subset.

## Evidence and interpretation

Emit a versioned JSON report, readable Markdown summary, protocol and input
hashes, frozen encoder/model/threshold artifacts, per-window scores, and
per-authored-fault detection rows. Keep role-tagged raw/synchronized artifacts
and a completion manifest in a unique local output directory; never overwrite
earlier attempts. The report must distinguish failed, incomplete, and complete
experiments and retain partial evidence on failure.

Required comparisons:

1. Held-out nominal bits/observation for each model, pooled and by seed/sensor;
   conditional-minus-temporal and conditional-minus-marginal differences.
2. Calibration exceedances and independent nominal-test false-alarm counts,
   denominators, rates, and per-seed variation, without an independence claim.
3. Fault interval detection and per-family coverage for all three models at
   their own frozen thresholds; paired nominal/faulted score differences.
4. Both parent orders, fallback rates, unknown/overflow rates, wall time,
   peak process RSS, and output size. Four test seeds are descriptive evidence,
   not sufficient for a broad scientific or statistical superiority claim.
5. A few timestamped observed-value/symbol/score examples, including an alert,
   a missed authored interval, and a nominal false alert when each exists.
   State explicitly when an example category does not exist.

Technical completion means reproducible generation, no leakage, immutable
fit/calibration artifacts during test scoring, all required denominators and
coverage, and valid reports. Scientific benefit is a separate outcome: report
whether conditional likelihood improves held-out cross-entropy and the observed
false-alarm/detection tradeoff; do not turn a failed comparison into a pass.

## Implementation boundaries and acceptance

Put research-only encoding/evaluation helpers under `libs/research` and a thin
entrypoint under `scripts`; keep bounded local evaluation out of production
Spark Table/Frame stages. Use `sentinel-spark35` for execution. This pilot calls
the native simulator directly and needs no Spark JVM or GPU, does not rerun
the paired production pipeline, and must not interrupt shared serving.
Record actual CPU/RAM/swap/disk preflight and chosen bounds before execution.

Tests must cover deterministic splits and seed behavior, count/encoding
immutability, fit-only quantiles/vocabulary/parent selection, constant and
out-of-range inputs, categorical unknowns, synchronization rejection, sequence
boundaries, exact window alignment and threshold ties/resolution, authored
truth overlap, uncovered families, and incomplete-run reporting. Add a tiny
simulator-backed test; keep the full 16-mission pilot an explicit DGX-local
qualification, not an expensive PR CI test. Run changed-file lint/format,
Markdown checks, targeted tests, proportionate broader tests, and read-only
review before publication.

Builder may choose helper names, serialization layout, test fixture shapes,
and efficient bounded implementation details without changing this protocol.
Data-contract mismatch, resource-bound overflow, or a needed scientific-method
change stops qualification and is reported, not silently repaired or tuned.

## Explicit remaining gates

This pilot partially addresses research evaluation gates 1 and 2. Recoverability-
tier parameter/module/subsystem localization and meaningful parent-order
attribution validation (gate 3) remain separate. Ranking surprise contributions
does not prove root cause. Absent fault families remain open coverage gaps.

No production thresholds, stage-80 scores, stage-90 attribution, simulator
semantics, Spark integration, real entropy coding, scaling claim, merge,
deployment, or draft promotion is authorized by this design. PR #57 stays draft.
Protocol approval permits implementation and local qualification, not promotion.
