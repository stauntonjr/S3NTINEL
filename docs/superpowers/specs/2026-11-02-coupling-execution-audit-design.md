# Coupling execution audit

Approved in conversation on 2026-11-02. The API details below are implementation choices, not additional user requirements.

## Intent and boundary

Explain what a coupling actually did without mislabeling a held input as a newly transferred faulty value. The user approved a separate, opt-in execution log for gate-blocked, break-suppressed, queued, and written transfers, with delayed deliveries recorded separately and coupling/fault-window identities retained.

Telemetry, transfer values, gates, scoring, and the frozen pilot stay unchanged.

This is instrumentation, not a simulator behavior repair or evidence of downstream detectability. It does not make the OFF-mode phase-context declarations apply, change fault schedules, promote declaration coverage to applied coverage, or retune the research pilot. The retained pilot bundle and PR #57 remain untouched. No new pipeline table, pipeline stage, CLI switch, scoring input, or default artifact is introduced.

## Approaches

The selected approach is a separate execution receipt. Writing fault metadata onto a held port would misrepresent the provenance of its value; reconstructing execution from declarations would not establish whether a gate or break suppressed the transfer. Neither meets the approved boundary.

## Receipt contract

Create immutable, scalar-only `CouplingExecution` and `CouplingDelivery` records in `libs/simulation/coupling/audit.py`.

Each successful `Coupling.apply` invocation emits exactly one execution receipt when an optional caller-owned audit list is supplied. An invocation that raises retains its existing exception behavior and does not claim successful completion. Existing return values remain unchanged. With auditing disabled, no receipt or audit list is allocated.

The execution receipt records:

- Coupling ID, source/target module and port IDs, and relation type.
- Invocation timestamp, resolved source timestamp, and current phase.
- Effective gain, sign, lag, and whether the phase gate was bypassed.
- Context presence and the supplied misbehavior/fault window IDs, family, and detail/type labels, copied into scalar fields rather than retaining mutable context dictionaries.
- Current attempt action: `blocked`, `suppressed_break`, `enqueued`, `written`, or `no_write`.
- First blocking gate in the existing evaluation order: `phase`, `source_mode`, `target_mode`, or `mode`; otherwise no blocking reason.
- Scheduled delivery timestamp for an enqueue, otherwise none.
- A separate optional delivery record for the previously queued transfer actually selected and written during this invocation, including its effective timestamp, originating coupling ID, and window identities. Review clarification: different coupling IDs can share an existing transfer key, so the delivery's originating ID must not be inferred from the current/draining invocation.

A break is recorded as `suppressed_break` only after the gates admit the current attempt. An inactive inhibit is `no_write`, while inactive enable still writes `None`, as today. Positive lag without a usable timestamp still follows the existing immediate-write fallback.

Delayed queues currently drain all due entries and write only the last due entry in queue order. The delivery record describes that effective write, not every drained entry. It must not imply chronological reordering or a complete queue history. A previous delivery may coexist with a blocked or break-suppressed current attempt. Queues with other effective transfer keys are not drained merely to improve audit completeness.

## Integration and lifecycle

Add `audit_sink: list[CouplingExecution] | None = None` to `Coupling.apply` and forward the same optional sink through module, subsystem, system, and aircraft stepping. Centralize the existing gate decision so `is_active` and the receipt reason cannot disagree; retain gate precedence and generic-mode fallback semantics.

Add `collect_coupling_audit: bool = False` to `Flight.step` and `Flight.iter_ticks`. Enabled calls collect a fresh list for that tick and store an immutable tuple on a new default-empty `FlightTick.coupling_executions` field. Add `FlightTick.coupling_audit_rows()` to flatten receipts with tail ID, flight ID, step index, and timestamp context, using datetime values consistently with the existing row APIs. Export the record types from the coupling package and top-level simulation package.

There is no cumulative history on `Flight` or `Aircraft`. Callers stream or explicitly retain tick rows. `Flight.simulate_rows` keeps its existing signature and two-element return, and does not enable auditing. Canonical telemetry/phase schemas, sample metadata, port metadata, delayed transfer metadata, and RNG consumption remain unchanged.

## Verification

Use real source/target modules for literal transfer tests: nominal write, each gate, admitted break with held value, enqueue, delayed delivery concurrent with break/block, inactive enable/inhibit, missing timestamp fallback, and changed transfer keys. Verify window identity comes from the correct current attempt or delivered transfer and is not an alias to a mutable context.

At the flight boundary, compare independently initialized identical seeded flights with auditing off/on, both nominal and faulted. Their telemetry rows, phase rows, port values/timestamps/metadata, and pending queue contents must match; receipts must be tick-local and absent by default. Preserve the two-element `simulate_rows` API.

After targeted tests, run the project suite and a separate CPU-only, full-length composite audit/parity qualification in a unique output directory. Record resources, revision, configuration, output hashes, receipt summaries, and honest limitations. Demonstrate break suppression and OFF-mode blocking in the authored intervals without rescoring or modifying the frozen pilot. No GPU workload or shared-service interruption is needed.

## Publication

Implement as a separate local branch based on clean main `20e2858df20f54b696e97eac29320bde4b5336e4`, not on the research branch. Use the existing execution clone and preserve personal branches and ignored artifacts. Commit, push, PR creation, and merge are not part of this approval; request publication direction after verified implementation.
