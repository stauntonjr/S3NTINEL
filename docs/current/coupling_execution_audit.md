# Coupling execution audit

The simulator can optionally explain each coupling invocation without changing sensor telemetry or assigning a new fault label to a held input value. This log is execution evidence, not detector input or proof that a fault caused an observable downstream effect.

## Usage

```python
from libs.simulation import Flight
from libs.simulation.flight.examples import build_named_flight_spec

flight = Flight.from_spec(
    build_named_flight_spec("power_pressurization_hierarchy_composite", seed=17),
    tail_id="AUDIT-T17",
    flight_id="AUDIT-F17",
)
for tick in flight.iter_ticks(
    n_steps=3360,
    dt_seconds=0.5,
    collect_coupling_audit=True,
):
    for row in tick.coupling_audit_rows():
        # Stream to a caller-owned writer; the simulator does not persist rows.
        print(row["step_index"], row["coupling_id"], row["action"])
```

`Flight.step` accepts the same optional collection flag. Each returned tick owns an immutable `coupling_executions` tuple; each receipt is a frozen `CouplingExecution`, with an optional frozen `CouplingDelivery`. Row dictionaries are fresh copies, with datetime values matching the other tick row APIs. There is no cumulative audit buffer on a flight or aircraft. Retaining every tick or row is a caller-controlled memory cost.

Collection defaults to off: tuples are empty, no audit list/receipts are created, and the pipeline emits no new artifact. `simulate_rows` retains its two-output telemetry/phase API and does not collect audit records. Lower-level callers may pass their own `audit_sink` list to aircraft, system, subsystem, module stepping, or `Coupling.apply`; those APIs retain their existing return values. Clear or consume that caller-owned list as needed.

## Interpretation

One receipt describes a successfully completed invocation, not necessarily a transferred value. Calls that raise keep their normal exception and do not fabricate a completed receipt.

| Current attempt `action` | Meaning |
| --- | --- |
| `blocked` | The existing gates rejected the current attempt. `gate_block_reason` reports the first blocker: phase, source mode, target mode, or generic mode. |
| `suppressed_break` | Gates admitted the attempt, but a coupling break suppressed the new write/enqueue. |
| `enqueued` | A new delayed transfer was queued for `scheduled_for_timestamp_utc`, not necessarily delivered yet. |
| `written` | The current attempt wrote the target input immediately, including a legitimate write of `None`. |
| `no_write` | The relation produced no current write, such as an inactive inhibit signal. |

The record includes endpoint and coupling identity, invocation and resolved source timestamps, phase, effective gain/sign/lag, phase-gate override, context presence, and supplied misbehavior/fault identities. Context identity is copied as scalar strings; it does not imply that the context was applied to an emitted sensor sample. `ignore_phase_gate` reports the effective override flag, not a bypass of mode gates. A phase-context override can still be blocked by an OFF source mode.

Delayed deliveries are independent: `delivered_transfer` records the previously queued transfer actually written during this invocation. Flattened rows use `delivered_` fields for its effective timestamp, originating coupling ID, and window labels. With no previous delivery these fields are null, not copies of the current context. Nominal transfers can be delivered with empty window identities. Distinct couplings can share an existing delayed-transfer key: the outer `coupling_id` identifies the current/draining invocation, while `delivered_coupling_id` identifies who originally queued the delivered transfer. They need not match.

A receipt may therefore show `suppressed_break` or `blocked` and still have a delayed delivery. It is incorrect to infer “the target did not change” solely from the current attempt action. Queues drain before the gate decision, as before. Of multiple due entries, the existing runtime writes only the last due entry in queue order; the log records that effective delivery, not a complete history of every drained entry. Other effective transfer keys are not drained for audit completeness. Positive lag without a usable timestamp preserves the existing immediate-write fallback.

## Boundaries

- No transfer values, fault schedules, gates, sampling rates, telemetry/phase schemas, port provenance, or detector scoring are changed.
- Coupling breaks remain held-input behavior; their identities belong in the execution log, not fabricated raw-sample fault labels.
- Declared windows, executed attempts, delayed writes, emitted sample labels, and detectable effects are different evidence levels.
- The frozen conditional-information pilot is not relabeled, rescored, or overwritten. Instrumentation does not improve its held-out false-alert results.
- No new Spark table, pipeline stage, CLI flag, default output artifact, dependency, or GPU workload is introduced.

## Qualification

Validation uses real module fixtures for gates, breaks, queues, missing timestamps, and enable/inhibit behavior; seeded flight tests compare audit-on/off telemetry, phases, contexts, ports, and queue state. Full-length nominal and faulted composite runs additionally compare streamed row/state hashes against pristine main, not just against another execution of the modified code. See the [qualification receipt](coupling_execution_audit_validation.md) for results and limitations.
