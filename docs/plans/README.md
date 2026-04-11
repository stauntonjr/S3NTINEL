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

## Active Plans

- [anomaly_modeling_next_steps.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/anomaly_modeling_next_steps.md)
  - current next-step plan for anomaly scoring, localization, and hierarchy decision gates
- [simulation_medium_term_plan.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/simulation_medium_term_plan.md)
  - medium-term simulation, realism, and performance sequencing
- [phaseplan_2.1.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/phaseplan_2.1.md)
  - proposed next phase-simulation model for schedule and envelope semantics

## Targeted Proposal Docs

- [behavior_simulation_improvements.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/behavior_simulation_improvements.md)
  - focused simulator changes to make behavior families more observable

## Deferred Notes

- [v2_1_notes.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/v2_1_notes.md)
  - deferred continuous representation ideas for a later modeling pass

## Maintenance Rules

- every plan doc should include:
  - `Status: Plan`
  - `Authority: Non-authoritative roadmap. Use package READMEs and docs/current/ for current behavior.`
- update an existing plan before creating a second one on the same topic
- when a planned change is implemented, move the authoritative semantics into code, contracts, package READMEs, or `docs/current/`
- remove or clearly retire plans that no longer describe real next steps
