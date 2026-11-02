# Conditional information baselines (research-only)

This experiment tests whether sparse cross-sensor conditioning improves the
nominal predictive code length and detects coherence breaks beyond independent
sensor models. It does **not** implement arithmetic coding, lossless storage,
continuous-valued density estimation, phase conditioning, or a production score.

- Input: synchronized discrete sensor symbols with declared finite alphabets.
- Fit: separate nominal reference sequences; sequence boundaries reset temporal context.
- Evaluation: held-out nominal and faulted sequences, with fixed model.
- Models: smoothed marginal, first-order per-sensor temporal, and causal
  within-state parent-conditioned temporal probability tables.
- Outputs: ideal negative-log2 likelihood per observation and aggregate window
  totals/means. Actual encoded bytes and model costs are not measured.
- An unseen context backs off to the sensor marginal distribution.
- Parent sensors must precede their children in a fixed decoding order.
- Joint likelihood contributions depend on that ordering; they do not prove
  root cause or causal relationships.
- Dense/high-cardinality telemetry needs a separate bounded representation
  design before scaling this reference implementation.

## Evaluation gates

1. Compare held-out nominal cross-entropy (bits/observation) for each model.
2. Evaluate injected cross-sensor, temporal, stuck-at, and drift faults at a
   fixed false-alarm budget using separate nominal calibration sequences.
3. Evaluate parameter/module/subsystem attribution against existing benchmark
   recoverability tiers; report ranking and parent-order sensitivity.
4. Keep existing stage-80 scores and stage-90 attribution unchanged.
5. Only consider Spark integration and real entropy coding after evidence
   shows improved localization or predictive efficiency.

Run: `pytest tests/unit/research/test_conditional_information.py`.

## Input-validation and retry contract

Smoothing must be finite and positive. NaN and either infinity are rejected
at construction, before a fitted predictor can emit non-finite likelihoods.
Training validates every reference sequence before changing any counts. A
validation error in a later sequence leaves the predictor unfitted and
does not contaminate a subsequent valid fit. The one-successful-fit guard
and sequence-boundary reset semantics remain unchanged.

The November 2, 2026 qualification first reproduced both defects against
the original draft. With additive smoothing of one, a failed fit followed
by reference `[1, 1]` incorrectly gave marginal surprise 0.5849625 bits
instead of the hand-derived 0.4150375 bits (`P(1)=3/4`). The retry tests
also check the temporal and conditional transition value of 0.5849625 bits
(`P(1|1)=2/3`), for both invalid-symbol and invalid-sensor-set failures.
Separate rejection tests reproduced accepted NaN and positive-infinite
smoothing; negative infinity remains rejected.

These checks qualify implementation behavior only. No held-out detection,
false-alarm calibration, localization improvement, Spark integration,
entropy-bitstream efficiency, or scale claim follows from passing them.
The evaluation gates above remain prerequisites for advancing this draft.

## Bounded simulator pilot

The approved [simulator design](../superpowers/specs/2026-11-02-simulator-conditional-information-design.md)
and [implementation plan](../superpowers/plans/2026-11-02-simulator-conditional-information.md)
define a fixed 16-mission native-simulator experiment. It uses separate fit,
calibration, and test seeds, nominal-only finite encoding, a common native
sampling grid, two fixed parent-order arms, and independently calibrated
marginal/temporal/conditional thresholds. The 1% false-alarm budget is a
calibration target, not a held-out guarantee with correlated windows.

From the research branch in `sentinel-spark35`, run with a new local directory:

```bash
python -u -m scripts.evaluate_sim_conditional_information \
  --output-dir data/conditional_information_unique_attempt
```

No Spark JVM or GPU is used by this bounded evaluation. Production Table/Frame
stages, simulator semantics, scoring, and attribution are unchanged. Tiny
test protocols are explicitly non-qualification runs; the CLI does not expose
tuning options. Existing output directories are rejected, including incomplete
attempts. A `running` or `failed` manifest is not a completed qualification.

Inspect `manifest.json`, `freeze.json`, `report.json`, and `summary.md`, plus
role/seed raw, synchronized-symbol, encoding-diagnostic, contribution, and
window-score artifacts. Input and frozen-artifact hashes, thresholds, per-seed
and per-sensor metrics, denominators, absent fault families, parent-order
sensitivity, and actual source examples must accompany any result. Surprise
is not root-cause evidence. Recoverability-tier localization, uncovered fault
families, Spark integration, entropy coding, and scale remain separate gates;
PR #57 remains draft even after a technically complete pilot.
