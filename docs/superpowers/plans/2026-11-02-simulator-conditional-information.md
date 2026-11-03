# Simulator Conditional-Information Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run the approved bounded simulator evaluation without changing production behavior or promoting draft PR #57.

**Architecture:** A research-only adapter generates isolated simulator missions, projects observed values onto a declared common grid, and fits finite encoders and one-parent models using only fit-role data. A separate calibration/scoring layer freezes thresholds before test evaluation; post-scoring truth evaluation produces versioned evidence and explicit coverage limits. The script is a thin wrapper over research libraries.

**Tech Stack:** Python 3.10+ in `sentinel-spark35`, existing native simulator and `CategoricalPredictor`, NumPy already used by the simulator, pytest, Ruff, JSON/JSONL and Markdown. No new dependency, Spark JVM, or GPU.

**Spec:** `docs/superpowers/specs/2026-11-02-simulator-conditional-information-design.md`

## Global Constraints

- Baseline research revision: `05fd5b4638fda1c6c5bb02a1c48169e741559c20`.
- Use `power_pressurization_hierarchy_composite`, its full 3,360 steps at 0.5 seconds.
- Fit seeds: 101, 102, 103, 104; calibration: 201, 202, 203, 204; nominal and faulted test: 301, 302, 303, 304.
- Use all uniquely named parameters in the preset, capped at 64. Cap raw rows at 250,000 per mission.
- Numeric: eight nominal interior bins using empirical fit octiles (linear quantiles), plus explicit underflow and overflow symbols.
- Categorical: sorted fit-role string vocabulary, at most 16 distinct values; assign symbols 1 through N and reserve 0 for unseen categories.
- Compare the existing marginal, first-order per-sensor temporal, and conditional outputs with additive smoothing fixed at alpha=1.
- Primary order: lexical parameter-name order. Each sensor has at most one earlier sensor parent.
- Run a predeclared reverse-lexical order sensitivity arm with the same one-parent rule and the same fit/calibration/test data.
- Use fixed nonoverlapping 20-second windows, anchored at mission start, with half-open time intervals.
- At a nominal 1% false-alarm budget, threshold at the 1-indexed order statistic `ceil((N+1)*0.99)`, capped at N; alert only when score is strictly greater.
- No clean values, labels, phase truth, fault truth, or test-role values may enter fitting, parent selection, calibration, or model selection.
- No production scoring, simulator semantics, Spark integration, entropy coding, merge, deployment, or draft promotion changes.
- Retain unfavorable outcomes and incomplete artifacts. Changed methodology needs a new protocol rather than silent tuning.

## Review Focus

- Same-seed fingerprints must exclude role-specific identifiers while preserving observed-value differences; Task 1 tests reproducibility and real seed variation.
- A missing common-grid value or duplicate sensor must fail rather than silently changing the denominator; Task 1 tests both and off-grid counts.
- Constant numeric channels and novel categorical strings must retain declared finite support without target refitting; Task 2 tests exact boundary symbols.
- Touching but nonoverlapping intervals, shared alerts, and zero-support authored faults must remain honest denominators; Task 4 tests these cases.
- A failure after scores exist must not leave a success manifest or allow overwrite/reuse of a partial directory; Task 5 injects failure and verifies evidence retention.

## File structure

- `libs/research/simulator_protocol.py`: frozen protocol and shared typed records.
- `libs/research/simulator_data.py`: schema extraction, native generation, observed-only synchronization and fingerprints.
- `libs/research/symbol_encoding.py`: fit-only finite encoders and diagnostics.
- `libs/research/conditional_evaluation.py`: parent selection, arm fitting, window scores, calibration, immutable model snapshots.
- `libs/research/simulator_truth.py`: authored interval inventory and post-scoring detection/coverage evaluation.
- `libs/research/simulator_pilot.py`: ordered lifecycle, frozen artifacts, reproducibility checks and completion manifest.
- `libs/research/simulator_reporting.py`: aggregate JSON/Markdown, comparisons and bounded source examples.
- `scripts/evaluate_sim_conditional_information.py`: argument parsing and delegation only.
- `tests/unit/research/test_simulator_{data,encoding,evaluation,truth,pilot}.py`: focused contracts, with a tiny simulator-backed integration fixture in the pilot file.
- `docs/research/conditional_information_baselines.md`: invocation, evidence links, and explicit nonclaims.

## Shared records

Define frozen dataclasses in `simulator_protocol.py`; collection fields use tuples, and serialization explicitly preserves stable ordering.

- `PilotProtocol`: exact approved preset/splits/horizon/cadence, alpha, limits, window duration and false-alarm target; constructor validates split disjointness except paired test seeds.
- `SensorSpec(name: str, kind: str, rate_hz: float, stride: int)`: kinds only `numeric` or `categorical`, from static declarations.
- `Mission(role: str, seed: int, n_steps: int, dt_seconds: float, sensors: tuple[SensorSpec, ...], steps: tuple[int, ...], values: tuple[dict[str, str], ...], diagnostics: dict)`: observed-only common-grid values, timestamps recoverable from the fixed mission start and steps; role/seed metadata is never a model feature.
- `EncodedMission(role: str, seed: int, steps: tuple[int, ...], states: tuple[dict[str, int], ...], diagnostics: dict)`.
- `WindowScore(role: str, seed: int, arm: str, model: str, start_step: int, end_step_exclusive: int, observations: int, mean_bits: float)`.
- `Threshold(value: float, count: int, requested_rank: int, ties: int, exceedances: int, resolution_sufficient: bool)`.
- `AuthoredInterval(seed: int, window_id: str, start_step: int, end_step_exclusive: int, family: str, detail: str, subject: dict, realized_parameters: dict)`.

Models remain existing `CategoricalPredictor` instances; snapshot helpers must serialize alphabets, ordered parent maps, alpha, fitted flag, and all three count families without lossy tuple-key conversion.

### Task 1: Protocol, native mission generation and synchronization

**Files:** Create `simulator_protocol.py`, `simulator_data.py`; test `test_simulator_data.py`.

**Interfaces:** `sensor_specs(flight: FlightSpec, dt_seconds: float) -> tuple[SensorSpec, ...]`; `generate_mission(protocol: PilotProtocol, role: str, seed: int, output_dir: Path) -> Mission`; `synchronize_rows(rows: Sequence[Mapping], sensors: tuple[SensorSpec, ...], *, role: str, seed: int, n_steps: int, dt_seconds: float) -> Mission`; `observed_fingerprint(mission: Mission) -> str`.

- [ ] Write failing tests for exact protocol roles/seeds, rejected split leakage, fixed row/sensor limits, unique sensor identities, declared numeric/categorical types, valid integer strides, and fresh runtime per role.
- [ ] Add synchronization fixtures with 2/1/0.5 Hz sensors at dt=0.5: `mission.steps == (0, 4, 8)` for 12 steps; `retained_rows == 9`; missing step-4 sensor, duplicates, unexpected sensors, NaN/inf, and incompatible rates raise descriptive `ValueError`.
- [ ] Add a small real simulator test: same seed produces equal observed fingerprints despite different role IDs; a different seed changes the fingerprint. Assert clean values and truth-only columns do not appear in `Mission.values`.
- [ ] Run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/research/test_simulator_data.py`; verify failure is the missing new contract.
- [ ] Implement the records and functions using `resolve_flight`, `nominal_companion_flight`, `Flight.from_spec`, native `simulate_rows`, declared strides and LCM. Write role-tagged raw JSONL once, with observed input and validation-only truth separately accessible. Reject existing destination files; record original/retained/off-grid/rate counts.
- [ ] Rerun the file; require all tests pass. Inspect a tiny artifact for observed-only projection and complete counts, then commit only Task 1 files plus approved design/plan if publication is authorized.

### Task 2: Frozen nominal-only finite encoder

**Files:** Create `symbol_encoding.py`; test `test_simulator_encoding.py`.

**Interfaces:** `fit_encoder(fit_missions: Sequence[Mission]) -> SymbolEncoder`; `SymbolEncoder.alphabets: dict[str, tuple[int, ...]]`; `SymbolEncoder.transform(mission: Mission) -> EncodedMission`; `SymbolEncoder.to_payload() -> dict`. Import `Mission`/`EncodedMission` from Task 1.

- [ ] Write failing numeric tests with fit values 0 through 8: `encode(-1)==0`, `encode(9)==9`, `encode(0)==1`, `encode(8)==8`, and numeric alphabet equals `tuple(range(10))`; assert interior-cut ties use `bisect_right`.
- [ ] Test a constant fit channel at 5: symbols for 4/5/6 are 0/8/9. Test sorted categorical vocabulary `['b','a']`: a/b/novel map to 1/2/0; preserve the literal strings `1` and `1.0`; reject more than 16 fit categories and non-fit roles.
- [ ] Test encoder payload hashes are identical before and after transforming extreme calibration/test observations; check per-role bin occupancy, under/overflow/unknown counts and constant-channel diagnostics.
- [ ] Run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/research/test_simulator_encoding.py`; confirm missing encoder contract causes failure.
- [ ] Implement fit-only linear octiles, explicit boundary bins, categorical vocabulary and diagnostics with no mutable transform-time fitting.
- [ ] Rerun Task 1 and Task 2 tests; require pass and commit the isolated encoder deliverable.

### Task 3: Parent maps, immutable model arms and calibrated scores

**Files:** Create `conditional_evaluation.py`; test `test_simulator_evaluation.py`. Do not change the existing predictor mathematics.

**Interfaces:** `select_parents(fit: Sequence[EncodedMission], order: tuple[str, ...]) -> dict[str, tuple[str, ...]]`; `fit_arms(fit: Sequence[EncodedMission], alphabets: Mapping[str, Sequence[int]]) -> dict[str, CategoricalPredictor]`; `score_mission(model: CategoricalPredictor, mission: EncodedMission, *, arm: str, protocol: PilotProtocol) -> tuple[list[WindowScore], dict]`; `calibrate(scores: Sequence[float], false_alarm_target: float = 0.01) -> Threshold`; `model_payload(model: CategoricalPredictor) -> dict`.

- [ ] Write failing tests for same-step empirical MI parent selection, lexical ties, no parent for zero MI, reverse-order arm, fit-only role guard and rejection of schema mismatch.
- [ ] Test hand-derived likelihoods, no transitions across mission boundaries, unchanged model hashes after scoring, unseen-context counts, and marginal/temporal order invariance.
- [ ] Test dt=0.5/stride=4 produces ten synchronized states per 20-second window and step intervals `[0,40)`, `[40,80)`; final partial windows are counted and excluded. Require model means use all sensor observations, not sensor totals averaged unequally.
- [ ] Test scores 0 through 335 give `calibrate(...).requested_rank == 334`, threshold 333 and exactly two strict exceedances; tied thresholds do not alert, empty/nonfinite calibration fails, and fewer-than-required windows mark insufficient resolution rather than a guaranteed 1% rate.
- [ ] Run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/research/test_simulator_evaluation.py`; confirm missing evaluation contract causes failure.
- [ ] Implement parent selection from pooled fit symbols, alpha=1 arms and immutable serialization. Compute windows from original steps, not run-local IDs; report per-sensor sums/counts, fallback diagnostics and excluded partial windows alongside the window rows.
- [ ] Rerun all research tests, require pass, then commit the calibrated-model deliverable.

### Task 4: Separate authored-truth evaluation

**Files:** Create `simulator_truth.py`; test `test_simulator_truth.py`.

**Interfaces:** `authored_intervals(flight: FlightSpec, seed: int) -> tuple[AuthoredInterval, ...]`; `evaluate_truth(intervals: Sequence[AuthoredInterval], scores: Sequence[WindowScore], thresholds: Mapping, raw_truth_path: Path, retained_steps: Sequence[int]) -> dict`; `coverage_inventory(intervals: Sequence[AuthoredInterval]) -> dict`.

- [ ] Write failing tests: an alert `[0,40)` does not overlap a fault `[40,80)`; an alert overlapping two authored intervals lists both IDs and their overlap durations; one unsupported interval stays in the detection denominator.
- [ ] Test nominal coupling/activity flags without authored identity never create faults, declared intervals remain distinct from applied support, and off-grid-only support is reported explicitly. Read full raw truth only here, after frozen scoring artifacts exist.
- [ ] Test per-family/per-seed counts, realized authored parameters, empty/duplicate window-ID rejection, and absent stuck-at coverage reports `not_covered` rather than success. Preserve detailed source labels; challenge-group mappings are explicit and included in the report.
- [ ] Run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/research/test_simulator_truth.py`; confirm missing evaluator causes failure.
- [ ] Implement half-open intersections and support accounting using authored specifications plus nonempty matching raw identities, without exposing truth to earlier-task APIs.
- [ ] Rerun research tests, require pass, then commit the separate truth-evaluation deliverable.

### Task 5: Evidence lifecycle, report and thin entrypoint

**Files:** Create `simulator_pilot.py`, `simulator_reporting.py`, `scripts/evaluate_sim_conditional_information.py`; test `test_simulator_pilot.py`; update research README document.

**Interfaces:** `run_pilot(output_dir: Path, protocol: PilotProtocol = PilotProtocol()) -> Path` returns final JSON report path; `build_report(protocol: PilotProtocol, evidence: Mapping) -> dict`; `render_markdown(report: Mapping) -> str`. Entrypoint accepts only `--output-dir`; full protocol is fixed, tiny protocols are internal test fixtures explicitly labeled non-qualification.

- [ ] Write failing lifecycle tests: generate fit then calibration, freeze protocol/encoder/counts/parents/threshold hashes, then generate and score both test roles, then load truth. Assert artifact hashes unchanged throughout testing and mutation is a hard failure.
- [ ] Test a tiny real-simulator run writes versioned report, readable summary, raw/synchronized/score tables, per-authored-fault rows, exact input hashes, unique output paths and manifest `complete`; test schema-match and seed-reproducibility receipts.
- [ ] Inject a scoring failure and assert manifest `failed`, exact failing role/seed/stage, preserved partial evidence, no success report, and refusal to reuse a nonempty directory. An interrupted `running` manifest is not complete and no automatic resume is claimed.
- [ ] Test pooled and per-seed/sensor bits, both conditional differences, held-out nominal FPR denominators, paired same-boundary contrasts, all arms/models and coverage; examples must come from real observed rows and explicitly mark absent alert/miss/false-alert categories.
- [ ] Run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/research/test_simulator_pilot.py`; confirm missing lifecycle/report contract causes failure.
- [ ] Implement ordered orchestration, atomic lifecycle JSON writes, stable JSON hashes, bounded JSONL artifacts, descriptive summaries, wall time, peak RSS and output bytes. Emit raw role evidence before transformations; never keep every raw mission in memory. Preserve actual hardware preflight and software/revision metadata.
- [ ] Implement the thin script; document the fixed invocation and no-scientific-superiority/non-promotion boundary. Require tiny smoke, all research tests, changed-file Ruff lint/format, Markdown checks and `git diff --check` to pass; commit the harness deliverable.

### Task 6: Review, full local pilot and publication

**Files:** Record results under a fresh task-specific `data/` directory; add concise evidence to `docs/research/conditional_information_baselines.md` and PR #57. Raw datasets remain local ignored artifacts, not Git additions.

- [ ] Request a read-only whole-branch review against `05fd5b4`, including approved protocol alignment and leakage/failure cases. Resolve blocking findings with regressions before qualification.
- [ ] Run all research and simulation tests in `sentinel-spark35`, followed by the full suite in one persistent elevated session with a hardware-justified Spark heap if broader verification is required. Preserve exact results and the unavailable-Delta skip boundary; do not infer results from prior revision.
- [ ] Inspect and record `nproc`, `free -h`, and `df -h /tmp data` before selecting resource bounds. No Spark JVM/GPU is launched by the pilot, and no shared service is interrupted.
- [ ] Run `conda run --no-capture-output -n sentinel-spark35 python -u -m scripts.evaluate_sim_conditional_information --output-dir data/conditional_information_20261102_<unique_attempt>` in one persistent terminal, polling until exit. Never overwrite an earlier attempt; do not launch a second pilot concurrently.
- [ ] Independently inspect the final manifest, all 16 mission-role receipts, synchronized counts, freeze hashes, all six arm/model thresholds, report denominators, actual source examples, and output hashes. The reproducibility probe may add separately labeled source-generation receipts; it does not expand the scoring cohort.
- [ ] Record conditional-versus-baseline predictive bits and the observed held-out false-alarm/detection tradeoff, including failures, absent families, temporal dependence and order sensitivity. A technically complete run can show no scientific benefit.
- [ ] Rerun cheap checks and inspect status/stale references. Publish code/docs and truthful qualification evidence to existing draft PR #57 only when authorized; no force push, merge, ready-for-review promotion or production change. Preserve the primary checkout's clean main and all local artifacts.

## Self-review and handoff

Spec coverage: Tasks 1–2 own input/encoding bounds and seed contracts; Task 3 owns frozen predictive/calibration comparisons; Task 4 owns truth/coverage; Task 5 owns evidence/failure lifecycle; Task 6 owns real qualification and explicit research boundaries. All five Review Focus classes have an owning regression task. Public interfaces use the shared records above; no production module or simulator changes are planned.

Execution: the user approved native implementation in the existing separate execution clone. Tasks 1–5 were implemented with red/green tests and committed separately. Independent whole-branch review and its regression-backed fix pass are complete. The full pilot completed technically; its held-out false-alarm target was not met. Task 6 broader verification and publication receipts are recorded in the execution ledger and validation record as they finish. No implementation agents were used, and no production or simulator code changed.
