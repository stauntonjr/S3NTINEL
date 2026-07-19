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

The current development-pass sequence is:

1. [simulation.md](libs/simulation.md): run the benchmark-first structural
   localization pass. Establish whether each declared localization target is
   observable in the canonical scenario, then compare reference-fit and
   faulted-inference replays before changing detector logic.
2. [anomaly.md](libs/anomaly.md): preserve bounded per-window candidate
   evidence from the canonical scoring path so a persisted top-k cut is not
   mistaken for candidate-generation absence. Only then make at most one
   bounded, generic localization change when the evidence identifies a
   specific downstream loss.
3. [phase.md](libs/phase.md): defer additional phase-simulation expansion until
   the localization benchmark and anomaly gates are stable.
4. [windows.md](libs/windows.md): defer rate-aware window-feature work until
   the validated bottleneck is feature representation rather than simulation
   observability or attribution mapping.

### Current Pass Scope

In scope:

- simulation observability, benchmark-target, and truth-scope review
- scenario and validation changes that make a declared localization target
  defensible without encoding simulator truth into downstream logic
- one replay-gated anomaly change when the evidence justifies it

Explicitly deferred until source data and an ICD are available:

- A-MATS or AFDX ingestion
- source adapters, payload decoding, and telemetry-provenance contracts
- AFDX-specific performance claims or transport-derived model features

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
