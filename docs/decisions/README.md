# Architecture Decision Records

This directory records durable architectural decisions that shape the active S3NTINEL implementation.

ADRs complement, rather than replace, the active contracts in `docs/current/` and the deeper rationale in `docs/design/`. Use an ADR when a decision constrains future implementation choices, has meaningful alternatives, or would be costly to rediscover.

## Status

- **Accepted**: current architectural constraint.
- **Superseded**: retained for history; a newer ADR owns the decision.
- **Proposed**: under active review and not yet authoritative.

## Index

- [ADR-0001: Keep growing fact-table computation in Spark](0001-spark-fact-table-boundary.md)
- [ADR-0002: Persist artifacts between major pipeline stages](0002-persisted-stage-artifacts.md)
- [ADR-0003: Separate reference fitting from target inference](0003-reference-fit-target-inference.md)
- [ADR-0004: Keep simulation truth outside production inference](0004-simulation-truth-boundary.md)

## Conventions

Each ADR should state context, decision, consequences, alternatives considered, and conditions that would justify revisiting the decision. New ADRs use the next four-digit sequence number and should be linked here.
