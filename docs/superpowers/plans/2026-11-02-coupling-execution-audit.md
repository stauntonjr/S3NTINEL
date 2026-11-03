# Coupling Execution Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional, truthful coupling execution receipts without changing simulation results or telemetry provenance.

**Architecture:** Immutable receipts describe the current coupling attempt separately from any effective delayed delivery. An optional caller-owned list passes through the existing runtime hierarchy; Flight exposes only tick-local tuples and rows. Existing simulation and pipeline APIs remain audit-disabled by default.

**Tech Stack:** Python dataclasses and typing; pytest and Ruff in the existing `sentinel-spark35` conda environment. No dependency additions.

**Spec:** `docs/superpowers/specs/2026-11-02-coupling-execution-audit-design.md`

## Global Constraints

- Telemetry, transfer values, gates, scoring, and the frozen pilot stay unchanged.
- No new pipeline table, pipeline stage, CLI switch, scoring input, or default artifact is introduced.
- `Flight.simulate_rows` keeps its existing signature and two-element return, and does not enable auditing.
- There is no cumulative history on `Flight` or `Aircraft`.
- No GPU workload or shared-service interruption is needed.
- Use `sentinel-spark35` for execution. Inspect CPU, RAM, swap, and output/spill disk before selecting run profiles or overrides; Spark-backed tests require an elevated persistent session.
- Native execution is the preserved user-selected method. Use a separate local branch from main; preserve ignored data and the research branch. Do not commit, push, create a PR, or merge without publication authorization.

## Review Focus

- A prior queued delivery and a blocked/broken current attempt can coexist; delivery identity must not be overwritten by current context (Task 1).
- Several due transfers or changed effective keys must preserve existing last-due selection and queue ownership (Task 1).
- Missing timestamps and inactive enable/inhibit signals must retain existing write/no-write behavior (Task 1).
- Generic mode gates, source/target gate precedence, and phase-only bypass must report the actual first blocker (Task 1).
- Audit collection must not retain mutable contexts, accumulate across ticks, change RNG consumption, or alter fault-disabled execution (Tasks 1 and 2).

---

### Task 1: Coupling-level receipts

**Files:**

- Create: `libs/simulation/coupling/audit.py`
- Modify: `libs/simulation/coupling/runtime.py`, `libs/simulation/coupling/__init__.py`
- Test: `tests/unit/simulation/test_coupling_audit.py`

**Interfaces:**

- Produces frozen/slotted `CouplingDelivery` with `effective_timestamp_utc: datetime` and string fields `coupling_id`, `misbehavior_window_id`, `fault_window_id`, `misbehavior_family_label`, `misbehavior_detail_label`, `fault_family_label`, and `fault_type` (missing identities become empty strings). Review clarification: `coupling_id` is copied from the queued transfer, because a different coupling can drain a shared key.
- Produces frozen/slotted `CouplingExecution` with string fields `coupling_id`, `source_module_id`, `source_port_name`, `target_module_id`, `target_port_name`, `relation_type`; nullable datetime fields `timestamp_utc`, `source_timestamp_utc`; `phase_label: str | None`; `gain: float`, `sign: int`, `lag_seconds: float`, `ignore_phase_gate: bool`, `context_present: bool`; the same six string context fields as delivery; `action: Literal["blocked", "suppressed_break", "enqueued", "written", "no_write"]`, `gate_block_reason: Literal["phase", "source_mode", "target_mode", "mode"] | None`, `scheduled_for_timestamp_utc: datetime | None`, and `delivered_transfer: CouplingDelivery | None`.
- Adds `Coupling.apply(..., audit_sink: list[CouplingExecution] | None = None) -> None`.
- Changes private `_drain_due_transfers(...)` to return the actual `DelayedTransfer | None` it wrote; preserves `DelayedTransferQueue.drain_due` semantics and public return.
- Adds private `_blocking_gate(source_module: Module, target_module: Module, *, current_phase_label: str | None = None, ignore_phase_gate: bool = False) -> Literal["phase", "source_mode", "target_mode", "mode"] | None`; `is_active` delegates to the same decision.

- [x] Establish baseline: inspect Git status/base, use the existing execution clone on `feat/coupling-execution-audit`, and run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/simulation/test_transfer.py tests/unit/simulation/test_module_step.py tests/unit/simulation/test_orchestrator.py`. Stop/report baseline failures before implementation.
- [x] Write nominal and break tests first using real modules: source `28.0` writes target `28.0`; changed source `30.0` with admitted break leaves target `28.0`, timestamp and metadata unchanged, reports `suppressed_break`, and retains current window ID `break-1`. Both calls still return `None`.
- [x] Run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/simulation/test_coupling_audit.py` and observe the missing audit argument/receipt behavior before implementing it.
- [x] Implement immutable records and optional receipt emission. Use copied scalar context fields, not whole dictionaries or references. Do not allocate receipts on the disabled path or mutate port metadata for suppressed transfers.
- [x] Add a gate table with literal expected reasons in existing precedence order; include phase bypass with source mode `OFF` still blocked, generic mode fallback, absent mode state, and simultaneous phase/source blockers. Observe failing assertions before centralizing gate evaluation.
- [x] Implement `_blocking_gate` preserving existing order and fallback; make `is_active` and `apply` consume that decision.
- [x] Add delayed tests: at `t0` enqueue value `28.0` for `t0+1s` with ID `prior-1`; at `t0+1s` a break delivers `28.0` with `prior-1` while reporting current `break-1` as suppressed. Repeat with a gate-blocked attempt. Assert port timestamp/metadata and pending queues, not merely receipt text.
- [x] Add multiple-due and transfer-key tests. Multiple due entries retain last-in-queue selection; a changed gain/sign/lag key does not drain another key. Verify the log describes only the effective last write and no fabricated delivery. Observe red before changing the private drain return and recording the delivery.
- [x] Add inactive enable (`written` with target `None`), inactive inhibit (`no_write` with held target), and positive lag with absent timestamps (`written`, no enqueue) tests. Include break-plus-inhibit precedence, copied context after caller mutation, and exception propagation without a fabricated successful receipt. Implement only missing receipt branches after each red test.
- [x] Run the new file plus `test_transfer.py` and `test_module_step.py`; require zero failures, and check the original literal transfer assertions still pass.

### Task 2: Tick-local collection and parity

**Files:**

- Modify: `libs/simulation/module/runtime.py`, `libs/simulation/subsystem/runtime.py`, `libs/simulation/system/runtime.py`, `libs/simulation/aircraft/runtime.py`, `libs/simulation/flight/runtime.py`, `libs/simulation/__init__.py`
- Test: `tests/unit/simulation/test_coupling_audit.py`, `tests/unit/simulation/test_orchestrator.py`

**Interfaces:**

- Consumes Task 1 records and `Coupling.apply(..., audit_sink=...)`.
- Adds optional `audit_sink: list[CouplingExecution] | None = None` to hierarchy step methods, forwarding the same list without changing their returns.
- Adds `collect_coupling_audit: bool = False` to `Flight.step` and `Flight.iter_ticks`.
- Adds `FlightTick.coupling_executions: tuple[CouplingExecution, ...] = ()` and `FlightTick.coupling_audit_rows() -> list[dict[str, object]]`.
- Row fields flatten execution/delivery identities with unambiguous `delivered_` prefixes, and add `tail_id`, `flight_id`, `step_index`; timestamps remain datetime objects as in existing row APIs. Missing delivery fields remain null, not current-attempt identities.

- [x] Write a failing consumer test using `build_power_chain_flight_spec`: an enabled tick contains one receipt per outgoing coupling invocation in execution order; disabled ticks have `()`, and later ticks cannot alter prior tuples. Row tail/flight/step values match that tick; delivery fields cannot inherit current identities.
- [x] Run the targeted file and observe missing collection support before forwarding the sink and implementing the tick API. Keep the newly added dataclass field last with its empty default for existing constructors.
- [x] Write parity tests using independently initialized identical `build_named_flight_spec("power_pressurization_hierarchy_smoke", seed=17)` flights over `600` steps at `0.5` seconds, parameterized with `apply_faults=True` and `False`: audit-on/off telemetry and phase rows, step contexts, final port values/timestamps/metadata, and pending delayed queues are equal. Assert the audited run has nonempty receipts so an ignored flag cannot pass; the faulted run must include a nonempty window identity. With `apply_faults=False`, verify no requested fault identities appear.
- [x] Run the tests, then implement any missing propagation/default behavior. Do not change behavior/RNG, telemetry construction, `simulate_rows`, seed bundle writing, or fault labeling to obtain parity.
- [x] Verify `simulate_rows` still unpacks to two outputs and matches equivalent unaudited ticks after existing event-label annotation; run `conda run --no-capture-output -n sentinel-spark35 pytest -q tests/unit/simulation`.

### Task 3: Qualification and handoff

**Files:**

- Create: `docs/current/coupling_execution_audit.md` for usage, contracts, and limitations.
- Retain qualification scripts and run-specific artifacts outside tracked source in a fresh workspace/output directory; no new production runner is needed.

**Interfaces:**

- Consumes `Flight.iter_ticks(..., collect_coupling_audit=True)` and `tick.coupling_audit_rows()`.
- Produces a reproducible validation receipt with baseline revision, final diff identity, environment, resource preflight, commands, test results, row hashes, and per-window execution counts. This is not a detector-quality report.

- [x] Record `nproc`, `free -h`, and `df -h` for `/tmp` and the intended output filesystem. Choose a CPU-only full-length composite parity run (`3360` steps, `0.5` seconds, simulator seed `17`) in a unique directory, without overriding or touching the frozen pilot. Build independent audit-on/off flights using `build_named_flight_spec("power_pressurization_hierarchy_composite", seed=17)` with explicit identical tail/flight IDs, once with faults enabled and once disabled.
- [x] Stream per-tick canonical telemetry/phase digests and audit rows; compare hashes and final state/queue contents between each audit-on/off pair. Summarize authored break and phase-context intervals separately: admitted breaks must show suppression, while the turnaround OFF-mode phase bypass remains blocked. Keep declared, attempted, delivered, and observed-effect claims distinct.
- [x] Run Ruff check and formatting checks for changed Python files. Run the full project `pytest -q --durations=30 --junitxml=<unique-result-path>` in `sentinel-spark35` in one persistent elevated session with hardware-justified driver memory and parquet. Poll until exit and report every failure or skip by name; do not misclassify loopback permission failures as pipeline failures.
- [x] Write usage documentation showing opt-in iteration, bounded tick-local retention, gate/break versus delayed-delivery interpretation, unchanged telemetry/scoring, and lack of downstream-effect proof. Attach the actual verification receipt, not predicted counts.
- [x] Obtain one fresh read-only whole-branch review under the requesting-code-review skill, fix supported findings with regression tests, and rerun affected checks. No implementation delegation is planned.
- [x] Finish with `git diff --check`, an obsolete-reference search for superseded audit API names, `git status`, and a check that the retained pilot manifest/hashes and research branch were not modified. Report exact residual limitations and request publication direction; leave commits/push/merge to separate authorization.

## Plan self-review

The three tasks cover all approved receipt outcomes, concurrent delayed delivery, identity provenance, opt-in lifecycle, API compatibility, parity, and qualification. Review-focus cases have explicit tests in their owning tasks. No task changes gates, schedules, telemetry labels, detector calibration, or frozen evaluation artifacts. Native execution completed; the verified, uncommitted candidate is preserved for the user's publication decision.
