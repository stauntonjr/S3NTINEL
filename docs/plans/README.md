# Plans

This directory contains non-authoritative roadmap and proposal artifacts.

Use plans for:
- next-step engineering proposals
- medium-term sequencing
- deferred design notes that are not part of the active contract yet

Do not use plans as the source of truth for current behavior. For that, prefer:
- [README.md](../../README.md)
- [docs/current/](../current)
- package READMEs near the code
- current code, schemas, contracts, and validation outputs

## Structure

- `libs/`
  - library-owned plan docs mirroring the `libs/` repo structure
  - use [libs/README.md](libs/README.md) as the main index
- `docs/`
  - documentation-owned plans for navigation, terminology, and documentation tooling
  - use [docs/README.md](docs/README.md) as the main index
- avoid flat topic files at this root level
- add future repo-mirrored subtrees only when there is a real ownership boundary, for example `pipelines/` or `scripts/`

## Current Plan Entry Points

The current resume order is:

1. [anomaly.md](libs/anomaly.md): use the authored-fault composite replay to
   improve subsystem/module localization without changing the canonical Spark
   modeling path.
2. [simulation.md](libs/simulation.md): keep smoke structural, use the
   authored-fault composite for positive validation, and preserve replay and
   benchmark consistency.
3. [phase.md](libs/phase.md): defer additional phase-simulation expansion until
   anomaly and simulation gates are stable.
4. [windows.md](libs/windows.md): defer rate-aware window-feature work until
   phase, hierarchy, and anomaly bottlenecks justify changing the feature
   contract.

- [libs/README.md](libs/README.md)
  - library-by-library plan index that mirrors `libs/`
- [libs/anomaly.md](libs/anomaly.md)
  - current anomaly, scoring, and hierarchy-decision next steps
- [libs/simulation.md](libs/simulation.md)
  - current simulation, realism, and performance next steps
- [libs/phase.md](libs/phase.md)
  - current phase-simulation next step
- [libs/windows.md](libs/windows.md)
  - deferred windows representation note

## Completed Plans

- [docs/documentation_quality_plan.md](docs/documentation_quality_plan.md)
  - documentation quality, navigation, terminology, and design-rationale work

## Maintenance Rules

- every plan doc should include:
  - `Status: Plan`
  - `Authority: Non-authoritative roadmap. Use package READMEs and docs/current/ for current behavior.`
- keep plan artifacts organized by repo ownership instead of flat topic piles
- update an existing plan before creating a second one on the same topic
- when a planned change is implemented, move the authoritative semantics into code, contracts, package READMEs, or `docs/current/`
- remove or clearly retire plans that no longer describe real next steps
