# Simulator conditional-information pilot: November 2, 2026

## Decision

The approved pilot completed technically, but **does not qualify the draft for
promotion**. Conditional models reduced held-out nominal code length in both
predeclared parent orders. Their held-out false-alert rates were **3.27% and
5.36%**, exceeding the 1% calibration target. Detection counts are authored
interval overlaps, not root-cause accuracy or proof of an applied fault effect.
No thresholds, encoding, seeds, sensor subsets, or parent orders were retuned
after test results. Production scoring and simulator semantics are unchanged.

## Protocol and retained evidence

- [Approved design](../superpowers/specs/2026-11-02-simulator-conditional-information-design.md)
  and [implementation plan](../superpowers/plans/2026-11-02-simulator-conditional-information.md).
- Executed code: `d71477b15328bf96db7e8d5caf42e844e9a70859`, clean worktree.
- Local bundle: `data/conditional_information_20261102_approved_a1` (not a
  checked-in dataset). Inspect `manifest.json`, `freeze.json`, `hardware.json`,
  `report.json`, `summary.md`, per-seed truth, and role/seed mission artifacts.
- Task-workspace evidence: `pr57-pilot-approved-a1.log`,
  `pr57-pilot-approved-a1-audit.json`, `pr57-pilot-reviewed-research.xml`, and
  `pr57-pilot-simulation.xml`. Full-suite verification is recorded separately
  after its completion, not inferred from the pilot result.

All 16 declared missions completed: fit seeds 101–104, calibration 201–204,
and paired nominal/faulted test seeds 301–304. Each used the full 3,360-step
mission at 0.5 seconds. A separate same-seed fit-generation probe confirmed
reproducibility without expanding the scoring cohort; different fit seeds
produced different observed values.

There were 56 sensors, 117,600 raw observations per mission, and 840 complete
synchronized states per mission at a 2-second common native cadence. This
retained 47,040 sensor observations per mission; high-rate off-grid observations
were not interpolated or silently treated as model inputs. The 16-mission cohort
contains 1,881,600 raw rows. Each evaluated mission produced 84 fixed 20-second
windows per model/order. Four calibration missions supplied 336 windows to
each of the six independent arm/model thresholds.

An independent read-only audit verified all 152 artifact SHA-256 hashes,
all five frozen core-file hashes, exact role/seed membership, native raw-to-grid
observed-value projections, complete windows, all six nominal false-alert
denominators, encoding rates, family denominators, and all 2,016 paired
window contrasts. Runtime freeze guards additionally checked in-memory models,
encoding, protocol and thresholds before/after test scoring.

## Descriptive results

Lower nominal bits/observation means better predictive efficiency on these
held-out seeds. Both parent orders were predeclared; neither was selected using
test results. Marginal/temporal results are order-invariant within floating-point
rounding.

| Order | Model | Nominal bits/observation | Nominal false alerts | Authored intervals overlapped by alerts |
| --- | --- | ---: | ---: | ---: |
| Lexical | Marginal | 2.703968 | 10/336 (2.98%) | 12/92 |
| Lexical | Temporal | 1.386389 | 19/336 (5.65%) | 41/92 |
| Lexical | Conditional | 1.372276 | 11/336 (3.27%) | 42/92 |
| Reverse lexical | Marginal | 2.703968 | 10/336 (2.98%) | 12/92 |
| Reverse lexical | Temporal | 1.386389 | 19/336 (5.65%) | 41/92 |
| Reverse lexical | Conditional | 1.334044 | 18/336 (5.36%) | 42/92 |

Conditional-minus-temporal differences were -0.014113 bits/observation for
lexical order and -0.052345 for reverse lexical. The nominal predictive
improvement does not establish improved scientific detection quality.

Every threshold had calibration rank 334 of 336, one threshold tie, and two
strict calibration exceedances (0.60%). Lexical conditional threshold:
1.6643202795072476; reverse conditional: 1.608771664253457. Actual held-out
rates were higher, with substantial seed variation:

| Test seed | Lexical conditional false alerts | Reverse conditional false alerts | Conditional authored-interval detections (either order) |
| --- | ---: | ---: | ---: |
| 301 | 8/84 | 11/84 | 15/23 |
| 302 | 0/84 | 0/84 | 12/23 |
| 303 | 0/84 | 0/84 | 3/23 |
| 304 | 3/84 | 7/84 | 12/23 |

These correlated windows and four test seeds do not support an independent-
sample false-alarm guarantee, superiority test, or real-flight generalization.

## Observed coverage and representation limits

Across the four faulted missions, 65 of 92 authored intervals had emitted
applied-flag support, and 27 had none recorded. All 92 remained in every
detector's denominator. Common-grid support also existed for those 65, but
support is not proof of an observable fault effect. Specifically:

- Cross-sensor challenges: 12 declared intervals, 8 with emitted/common-grid
  support, all for `coupling_inversion`. Four `coupling_break` intervals were
  declared but had zero recorded applied support. Their overlap detections
  must not be claimed as successful detection of observed coupling breaks.
- Temporal challenges: 20 declared intervals, 17 supported on the common grid,
  covering `state_chatter`, `illegal_transition`, and `timing_jitter`.
- Drift: 12 declared intervals, 9 supported on the common grid.
- Stuck-at: not represented. Localization/recoverability tiers were not run.

For either conditional order, family-level interval detections were regulated
15/24, accumulative 9/12, coupling 18/36, discrete-state 0/8, and inertial 0/12.
These preserve unsupported declarations; they are not support-filtered recall.

Each role has 188,160 synchronized sensor observations across its four missions:

| Role | Underflow symbols | Overflow symbols | Unknown categories |
| --- | ---: | ---: | ---: |
| Fit | 0 | 0 | 0 |
| Calibration | 51 | 37 | 0 |
| Nominal test | 67 | 366 | 0 |
| Faulted test | 623 | 829 | 0 |

The report retains per-role/seed/sensor numerators, denominators, occupancies,
and explicit rates. These observations do not identify why every alert fired.

### Actual source examples

The following are primary lexical-conditional examples, not cherry-picked
model selection. The full report retains three observed rows per example.

- Faulted alert, seed 301, steps `[320,360)` (160–180 seconds): window score
  1.700000 bits/observation exceeded threshold 1.664320. At step 320,
  `bus_voltage_v` observed `29.21507525662854`, encoded overflow symbol 9,
  with conditional contribution 11.718533 bits.
- Missed authored `MBW_STATE_CHATTER_CONTROL_SHARED`, seed 301, steps
  `[2880,3060)`: 45 emitted applied rows, all on the grid, but no overlapping
  conditional alert. The first overlapping window scored 1.483521, below
  threshold. At step 2884, `pack_mode_state` observed `OFF`, symbol 4,
  contribution 8.233620 bits. A large individual contribution need not make
  the aggregate over 56 sensors cross threshold.
- Nominal false alert, seed 301, steps `[1320,1360)` (660–680 seconds): score
  2.417689 exceeded threshold. At step 1320, `aircraft_altitude_ft` observed
  `35733.961198949764`, encoded overflow symbol 9, contribution 11.718533 bits.
  Nominal extreme values therefore appear among the actual high-surprise rows;
  this example alone does not establish the cause of all false alerts.

## Runtime and engineering checks

Preflight recorded 20 CPUs, 121 GiB RAM, approximately 82 GiB available,
15 GiB swap with 460 KiB used, and about 2.0 TiB free spill/output storage.
The native CPU pilot used no Spark JVM or GPU and interrupted no shared service.
It completed in 142.740 seconds with peak process RSS 370,794,496 bytes
(approximately 354 MiB), retaining 2,288,784,810 bytes of output (approximately
2.29 GB). Output size is not a memory cap or a production scale measurement.

All 38 research tests and 149 simulation unit tests passed. Read-only code
review identified and regression-verified corrections to declaration-only
coverage, explicit encoding rates and authored-family summaries. Lint,
formatting, Markdown and whitespace checks passed before qualification.

The held-out false-alarm target remains unmet; stuck-at and observed coupling-
break coverage, recoverability-tier localization, broader generalization,
Spark integration, entropy coding and scale remain open. Any follow-up protocol
requires a new frozen experiment, not tuning against these test outcomes.
PR #57 remains draft; this record does not authorize merge or production use.
