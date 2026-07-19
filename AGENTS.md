# S3NTINEL Agent Guidance

## Repository Defaults

- Treat `README.md`, package READMEs, `docs/current/`, code, schemas, contracts, and validation outputs as authoritative. Treat `docs/plans/` as non-authoritative roadmap material and `docs/architecture/` as generated.
- Use the `sentinel-spark35` conda environment for project execution, Spark tests, and pipeline validation.
- Prefer `S3NTINEL_TABLE_FORMAT=parquet` for local runs unless Delta JVM jars are known to be available.
- Before selecting a Spark profile or explicit memory/parallelism overrides for a simulation, smoke, replay, or benchmark run, inspect the local CPU count, total/available memory, swap, and free space on the intended Spark spill/output filesystems. Choose the profile from that evidence; do not infer hardware capacity from a profile name such as `laptop_large_sim`. Record the observed hardware and chosen profile/overrides with the validation result.
- Spark starts a local Py4J gateway that binds a loopback socket. In agent sandboxes, run Spark-backed tests and pipeline runners with elevated execution; a normal sandbox failure such as `Py4JNetworkException` or `JAVA_GATEWAY_EXITED` with `Operation not permitted` is an execution-permission failure, not a pipeline result.
- For a full replay or gate suite, use one persistent elevated terminal session and poll it until exit. Use a new `/tmp` or `data/` base directory per attempt, inspect the final run manifest and reports, and do not treat an incomplete child bundle as validation evidence.
- Keep production modeling semantics on the canonical Spark `Table` / `Frame` / stage-library path. Local pandas is limited to bounded reporting, validation, evaluation, plotting, and final assertions.
- Keep pipeline entrypoints thin; put domain logic in the owning `libs/*` package.

## Verification

- Inspect `git status` before editing and preserve unrelated user changes.
- Run cheap checks first, then targeted tests, then integration or replay validation.
- Modeling or validation changes require a recorded baseline and inspection of the relevant reports, not just a passing test command.
- Finish with `git diff --check`, a stale-reference search, and `git status`.

## Git And Safety

- Do not use destructive Git commands unless explicitly requested.
- Do not commit or push unless the user asks for it.
- Do not edit generated architecture artifacts manually; regenerate them through the documented workflow when needed.
- Report incomplete verification and unresolved risks explicitly.
