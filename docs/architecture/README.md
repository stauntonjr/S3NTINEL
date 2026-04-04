# Architecture Artifacts

This directory contains the checked-in architecture-generation inputs and outputs for the repo.

Manual source of truth:

- `annotations.yaml`

Generated artifacts:

- `architecture_facts.json`
- `workspace.dsl`
- `architecture_metrics.json`
- `architecture_metrics.md`
- `pipeline_data_flow.md`
- `pipeline_layered_architecture.md`
- `taxonomy_diagram.md`
- `view_index.md`
- `raw/`
- `ai_review/` when explicitly requested

Canonical commands:

- `python tools/architecture_workflow.py render`
- `python tools/architecture_workflow.py check`
- `python tools/architecture_workflow.py ai-draft`
- `bash tools/generate_architecture_artifacts.sh`

Notes:

- LOC is AST span LOC derived from syntax node line ranges.
- Generated files should be regenerated, not edited manually.
- The C4 DSL is intended for local preview with the installed Systemticks extension.
