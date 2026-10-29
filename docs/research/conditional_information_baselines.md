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
