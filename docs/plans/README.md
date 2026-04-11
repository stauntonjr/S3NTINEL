# Plans

This directory contains non-authoritative roadmap and proposal artifacts.

Use plans for:
- next-step engineering proposals
- medium-term sequencing
- deferred design notes that are not part of the active contract yet

Do not use plans as the source of truth for current behavior. For that, prefer:
- [README.md](/home/jrs/code/S3NTINEL/sentinel/README.md)
- [docs/current/](/home/jrs/code/S3NTINEL/sentinel/docs/current)
- package READMEs near the code
- current code, schemas, contracts, and validation outputs

## Structure

- `libs/`
  - library-owned plan docs mirroring the `libs/` repo structure
  - use [libs/README.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/README.md) as the main index
- avoid flat topic files at this root level
- add future repo-mirrored subtrees only when there is a real ownership boundary, for example `pipelines/` or `scripts/`

## Current Plan Entry Points

- [libs/README.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/README.md)
  - library-by-library plan index that mirrors `libs/`
- [libs/anomaly.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/anomaly.md)
  - current anomaly, scoring, and hierarchy-decision next steps
- [libs/simulation.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/simulation.md)
  - current simulation, realism, and performance next steps
- [libs/phase.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/phase.md)
  - current phase-simulation next step
- [libs/windows.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/windows.md)
  - deferred windows representation note

## Maintenance Rules

- every plan doc should include:
  - `Status: Plan`
  - `Authority: Non-authoritative roadmap. Use package READMEs and docs/current/ for current behavior.`
- keep plan artifacts organized by repo ownership instead of flat topic piles
- update an existing plan before creating a second one on the same topic
- when a planned change is implemented, move the authoritative semantics into code, contracts, package READMEs, or `docs/current/`
- remove or clearly retire plans that no longer describe real next steps
