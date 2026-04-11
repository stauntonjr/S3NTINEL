# Documentation

This directory contains the longer-form conceptual and architectural notes for the repo.

Use these docs for:
- theory and math
- architectural rationale
- planning and taxonomy
- subject-matter guidance

Use the newer area READMEs near the code for:
- current package ownership
- current module names
- current entrypoints
- current artifact/schema locations

Recommended orientation order:

1. [README.md](/home/jrs/code/S3NTINEL/sentinel/README.md)
2. [libs/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/README.md)
3. [pipelines/README.md](/home/jrs/code/S3NTINEL/sentinel/pipelines/README.md)
4. the package-level README for the area you are editing
5. the deeper conceptual notes in this directory

Directory layout:
- `current/`
  - active implementation-facing docs
  - start here for architecture, workflow, validation, and complexity
- `reference/`
  - stable taxonomy, schema, and theory reference docs
- `design/`
  - design notes and detailed architecture/spec material
- `simulation/`
  - simulator-specific guidance and diagrams
- `plans/`
  - roadmap and proposal docs
  - non-authoritative for current behavior
- `research/`
  - exploratory or research-note material
- `architecture/`
  - generated architecture snapshot subtree
  - non-authoritative until regenerated

Useful starting points:
- [current/v2_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/current/v2_architecture.md)
- [current/fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/current/fitting_workflow.md)
- [current/phase_validation_semantics.md](/home/jrs/code/S3NTINEL/sentinel/docs/current/phase_validation_semantics.md)
- [current/computational_complexity_report.md](/home/jrs/code/S3NTINEL/sentinel/docs/current/computational_complexity_report.md)
- [reference/theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/reference/theory_foundations.md)
- [design/artifact_replay_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/design/artifact_replay_design.md)
- [simulation/simulation_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation/simulation_architecture.md)
- [plans/README.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/README.md)
- [plans/anomaly_modeling_next_steps.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/anomaly_modeling_next_steps.md)
- [plans/simulation_medium_term_plan.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/simulation_medium_term_plan.md)
- [architecture/README.md](/home/jrs/code/S3NTINEL/sentinel/docs/architecture/README.md)

Notes:
- These docs preserve conceptual material even where the code-level READMEs are now the primary implementation guide.
- If a conceptual doc and a package README disagree on current implementation ownership, prefer the package README.
- Production modeling semantics live in the canonical Spark `Table` / `Frame` owners; local pandas code is limited to bounded validation/reporting/evaluation and final test assertions.
- `docs/architecture/` is a generated snapshot subtree. If it lags behind current code ownership, treat it as stale until regenerated and prefer the package READMEs plus current code.
