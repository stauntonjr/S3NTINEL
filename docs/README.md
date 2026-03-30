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

Useful starting points:
- [simulation_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_architecture.md)
- [behavior_simulation_improvements.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_simulation_improvements.md)
- [fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/fitting_workflow.md)
- [computational_complexity_report.md](/home/jrs/code/S3NTINEL/sentinel/docs/computational_complexity_report.md)
- [theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/theory_foundations.md)
- [v2_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/v2_architecture.md)
- [artifact_replay_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/artifact_replay_design.md)
- [simulation_medium_term_plan.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_medium_term_plan.md)

Notes:
- These docs preserve conceptual material even where the code-level READMEs are now the primary implementation guide.
- If a conceptual doc and a package README disagree on current implementation ownership, prefer the package README.
