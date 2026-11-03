# Coupling execution audit qualification — 2026-11-02

## Candidate and boundary

Local branch `feat/coupling-execution-audit`, based on main `20e2858df20f54b696e97eac29320bde4b5336e4`. Qualification ran on an uncommitted candidate; the validation workspace seals its Python source file hashes rather than claiming a then-published candidate revision. The separate primary checkout at that same revision supplied the pristine baseline. The retained local PR #57 research branch remains at `13352426d149bbc345a2490c8b80193fac927b50`.

This qualifies opt-in instrumentation, not scientific detector performance or a fault-behavior repair. No frozen-pilot rescoring, model tuning, new coverage promotion, pipeline deployment, or publication occurred.

## Environment and commands

Preflight: 20 logical CPUs, 121 GiB RAM with 82 GiB available, 15 GiB swap virtually unused, and 2.0 TiB free on the shared output and `/tmp` filesystem. All project execution used `sentinel-spark35`. The CPU-only qualification runs used no Spark session, GPU, or memory override. The final full project suite uses an elevated loopback-capable session with `PYSPARK_SUBMIT_ARGS='--driver-memory 8g pyspark-shell'`, parquet, fixture `local[1]`, and a fresh `/tmp/s3ntinel-coupling-final-fQC6cm/pytest` directory.

The retained local workspace is `.superpowers/sdd/2026-11-02-coupling-execution-audit/`. Its `qualify.py` was run in separate output directories against:

1. `PYTHONPATH=<primary-checkout>` with auditing unavailable/off: `qualification-baseline-01`.
2. `PYTHONPATH=<execution-checkout>` with auditing off: `qualification-disabled-02`.
3. `PYTHONPATH=<execution-checkout>` with `--audit`: `qualification-enabled-02`.

Each invocation ran independent nominal and faulted `power_pressurization_hierarchy_composite` flights, seed `17`, `3360` steps at `0.5` seconds, identical explicit tail/flight IDs. `compare.py --candidate-attempt 02` checks completed reports, compares the hashes below across all three implementations/modes, and writes `qualification-comparison-02.json`. Audit rows are streamed to compressed JSONL; no mission-wide audit buffer is retained by the simulator. Earlier pre-review-fix attempts remain separately retained, not overwritten.

## Full-length parity

Each of the six runs emitted `117600` telemetry rows and `3360` phase rows. All three variants matched for each role, including per-tick port, parameter, latent/controller/mode, and pending-queue state, not only final telemetry.

| Digest | Nominal | Faulted |
| --- | --- | --- |
| Telemetry SHA256 | `fc5fa8b4413a479fabf6c37a353ad6dc71f157d3dfcedbcb9935154a0365cd73` | `44c3dc246b1e5a79013926cdca4799d99b77e6fca6c131dc8b04fcd4e857e1b5` |
| Per-tick runtime state SHA256 | `7a5a65e554bc15614fc1a015e3c19e7bc6090a5198a82faea583dbc7c82b1f55` | `f2bb209ce54de590c734bb56dd2aa29e5190c6b7863bf36fa9d477fb079e696b` |
| Final runtime state SHA256 | `76540e9fae8e2a78ebfec65344ec53e8e7531e711c6409cd262805609a5686f9` | `16fa645952f5a380d583d50f91c5bea8f1ddb635e55c4690c4396beee9af7c68` |

Phase SHA256 for all runs: `74bb4fba9c587bd328d01a3346a612ecd03a838252b2079953e71b6750963b98`. These are canonical JSON row/state stream digests, not hashes of Parquet files or the old research bundle.

Audited runs emitted exactly `127680` receipts each (`38` couplings × `3360` steps). The nominal run recorded `67200` immediate writes, `53280` enqueues, and `7200` phase-gate blocks. The faulted run recorded `67200` immediate writes, `53000` enqueues, `280` break-suppressed attempts, and `7200` gate blocks (`6720` phase, `480` source-mode). Neither run encountered an inactive-inhibit `no_write`; that outcome is covered by unit fixtures.

For `MBW_COUPLING_STRUCTURE_AFT`, all `280` attempts in steps `[1760,2040)` were break-suppressed. Two previously queued nominal values were still delivered at steps `1760` and `1761`; their delivery window IDs remain empty rather than being relabeled as coupling breaks. Each of the three phase-context windows in steps `[280,440)` had `160` source-mode blocks despite the phase override. These are execution facts, not claims of raw-sample labels, causal downstream effect, or detector recall. The comparison report distinguishes deliveries originating in a window from previous deliveries concurrent with that window's current attempt.

Final audited runs took `17.63` seconds nominal and `19.77` seconds faulted, including full per-tick state hashing and compressed audit output. Peak process RSS was `209793024` bytes (about `200` MiB), versus `209911808` bytes for the pristine baseline process. These are bounded qualification measurements, not a controlled overhead benchmark.

## Tests and integrity

- Baseline transfer/module/orchestrator checks: `30` passed before implementation.
- Coupling-level red/green checks covered missing API, blocked receipts, enqueue and previous-delivery evidence before their implementation.
- Initial simulation unit suite: `180` passed. After the review fix and new shared-key regression, the final simulation unit suite passed `181` tests, including seeded fault-on/off parity and the unchanged two-output `simulate_rows` contract.
- Ruff check and format check passed for all `11` changed/new Python files.
- Fresh read-only review found one Important provenance gap and no Critical/Minor issues: distinct coupling IDs can share a delayed key, so delivery records must preserve the original queuing coupling ID. A real shared-key regression failed before adding `CouplingDelivery.coupling_id`; all `62` focused tests passed after the fix. Full-length qualification was refreshed for the fixed source. No minor findings were deferred.
- Final full project suite: `531` collected, `530` passed, `1` skipped, `0` failures/errors; exit `0`, elapsed `1507.615` seconds. The skipped test is `tests/regression/delta/test_delta_upsert.py::test_upsert_table_overwrites_existing_merge_key_row`, reason `delta JVM classes unavailable for this Spark runtime`. Delta merge behavior is not qualified by this environment; the tested local pipeline format is parquet. Existing PySpark/pandas deprecation warnings remain.
- Frozen pilot: all `152` artifact hashes and all `5` freeze hashes verified unchanged. Manifest SHA256: `5ceca9f469bb469422ad647550d26fd6a7cd753a59d18f0b3908a602460ca7a1`; freeze SHA256: `a22b98fdd6cc4e6256f366eda3b30d07dc9ad80ec06ab1d8a88bcc299e7161c0`.

Run-specific scripts, audit logs, full test logs/XML, source-hash maps, and comparisons are retained locally in the validation workspace. They are not default production artifacts or committed mission datasets.

`final-check.py` verifies final source hashes against the refreshed qualification, the full/unit/focused XML results, Ruff, whitespace, frozen-pilot integrity, and the preserved primary checkout/research branch. Its final receipt is `validation-final.json`. The qualified simulator source-map SHA256 is `3c0472d3003490f830cd89d0bf9bfb6186b6ed9453b86f772b82e266e2d5fa2a`. After qualification, the user selected publication option 2: commit the verified candidate, push the feature branch, and open a PR against main. That authorization does not include merging; local run-specific artifacts stay out of the commit.
