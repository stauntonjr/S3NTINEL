Status: Completed
Authority: Non-authoritative roadmap. Use package READMEs and `docs/current/` for current behavior.

# Documentation Quality Plan

## Objective

Make the repository documentation easy to orient in as a human and safe to
navigate, index, and validate as a machine. The target is an active-V2-only
documentation surface with clear authority, valid links, concise entry points,
and design rationale for the structural modeling path.

## Completed Work

The 2026-07-18 implementation completed all six workstreams:

- added document-class, authority, linking, terminology, and Notes rules in
  `docs/reference/documentation_conventions.md`;
- rebuilt the root README as a concise orientation page and added the pipeline
  stage-to-artifact index;
- added an offline Markdown checker, unit coverage, and a CI gate;
- removed the obsolete project-pitch note, repaired local links, and normalized
  active terminology;
- reorganized the glossary around the active telemetry-to-attribution data flow;
- added focused graph/hierarchy and anomaly-attribution design rationale.

## Scope And Non-Goals

In scope:

- human orientation, navigation, and concise task entry points
- local-link and local-anchor validation
- active terminology, glossary, and authority boundaries
- retiring obsolete material and isolating proposal-facing discussion
- missing design rationale for active architecture

Out of scope:

- changing production model behavior or simulator semantics
- rewriting generated `docs/architecture/` by hand
- treating a runtime compatibility identifier as obsolete solely because its
  spelling is historical
- external URL uptime monitoring in the first pass

## Workstreams

### 1. Establish Documentation Contracts - Completed

Create `docs/reference/documentation_conventions.md` with:

- document classes: current contract, reference, design rationale, simulation
  guidance, research note, plan, and generated output
- the expected authority statement and owner link for each class
- heading, link, terminology, and `Notes` conventions
- a rule that active behavior belongs in code/package READMEs/`docs/current`,
  while proposals belong in `docs/plans`

Update `docs/README.md` to route readers by task instead of by directory alone.

Acceptance:

- each top-level documentation area states its authority and intended reader
- a contributor can identify the authoritative source for an artifact or stage
  without reading a roadmap

### 2. Rebuild The Root README As A Human Entry Point - Completed

Replace the current opening with a brief summary covering:

- **What:** a Spark-oriented telemetry anomaly-detection and attribution
  pipeline with a simulation validation harness
- **Why:** recover meaningful system, subsystem, module, and parameter context
  from mixed-rate aircraft-style telemetry
- **How:** profile telemetry, extract events, build structural graphs and a
  hierarchy, detect phases, score anomalies, and emit attribution artifacts
- **Who it is for:** developers resuming model, simulation, or pipeline work
- **Start here:** environment setup, smoke command, active architecture, and
  package/stage ownership links

Keep detailed environment switches, exhaustive artifact lists, and stage I/O
tables in their owning documents. Retain one canonical full-pipeline sequence
and link to `pipelines/README.md` rather than repeating it.

Add a compact stage-to-artifact index to `pipelines/README.md`. It should map
each stage to its primary persisted outputs and link readers back to the
concept-oriented glossary and active architecture contract. Do not restructure
the glossary by stage.

Acceptance:

- the first screen gives a new reader purpose, workflow, and first command
- the root README has one quick-start path and no obsolete conventions section
- all root README relative links resolve from the repository root
- stage composition has one concise stage-to-artifact index outside the glossary

### 3. Make Markdown Validation A Repository Check - Completed

Add a repository-owned checker, for example `tools/check_markdown_docs.py`,
with tests covering:

- inline Markdown links to local files
- local anchors derived from headings
- root-relative versus document-relative paths
- exclusion of generated architecture content and intentionally external URLs
- duplicate top-level headings and missing required plan status/authority lines

Wire it into the existing local verification guidance and CI after it has a
stable, low-noise baseline. External links should be reported separately from
local correctness; they require a network-capable scheduled check rather than
blocking ordinary offline development.

Initial known local-link repairs:

- `README.md`: convert `../docs/...` and `../notebooks/...` links to
  repository-root-relative paths
- `docs/current/computational_complexity_report.md`: repair references to the
  actual backbone module
- `libs/phase/README.md` and `libs/testing/README.md`: replace stale absolute
  or malformed relative paths

Acceptance:

- zero unresolved local file links and local anchors in checked Markdown
- a documented command verifies this without network access
- CI prevents regressions once the baseline is clean

### 4. Remove Obsolete Context And Normalize Active Terminology - Completed

Delete the obsolete project-pitch note and remove any index or link that would
retain it.

Audit historical terminology in non-generated Markdown:

- remove historical implementation narrative from current/reference/design
  documents
- replace phrases such as an outdated `lag_graph` description with the active artifact name
  `lag_graph` when no compatibility distinction is needed
- keep a narrow `Compatibility Notes` explanation only where a current
  persisted field, command, or runtime fallback genuinely requires it
- do not rename active identifiers such as objective or stochastic-profile
  names merely for documentation cleanup

Move proposal-looking prose out of authoritative docs. When short context is
useful, place it in a final `## Notes` section that links to the relevant plan
instead of presenting it as current behavior. Keep actual sequencing and
acceptance criteria in `docs/plans/`.

Acceptance:

- no obsolete organization-specific project references remain in tracked Markdown
- current docs describe active behavior without historical implementation narrative
- proposal language is either in a plan artifact or a final Notes section

### 5. Refresh The Glossary And Artifact Vocabulary - Completed

Rewrite `docs/reference/glossary.md` around the active data flow:

1. telemetry and parameter semantics
2. profiles, events, windows, and phase outputs
3. graph artifacts and directed-lag terminology
4. hierarchy artifacts, including `hierarchy_edge_evidence`
5. scoring, attribution, simulation truth, and validation reports

For every term, distinguish canonical artifact/field names from descriptive
aliases. Add entries for `coupling_id_label`, `lag_profile`, `lag_graph`,
`fused_graph`, `graph_parameter_universe`, `hierarchy_edge_evidence`,
`hierarchy_sensor_map`, and the hierarchy-edge evidence report.

Acceptance:

- glossary terms match `libs/io/schemas`, `libs/config/pipeline.py`, and
  `docs/current/v2_architecture.md`
- no glossary statement claims support for obsolete code paths

### 6. Fill Design-Rationale Gaps - Completed

Add focused, current design documents rather than one oversized architecture
essay:

- `docs/design/graph_hierarchy_design.md`
  - precision/event/directed-lag inputs, fusion, bounded mutual-top-k hierarchy
    construction, compatibility constraints, and retained-edge evidence
  - explain the directed-lag versus undirected hierarchy distinction and replay
    invariants
- `docs/design/anomaly_attribution_design.md`
  - score channels, parameter candidate generation, subsystem/module rollup,
    attribution artifacts, validation semantics, and known limits

Before adding any further design documents, perform a gap review against the
package READMEs. Event/windowing and simulator material already have substantial
owning documentation and should receive a new design document only when their
rationale cannot be maintained in those owners.

Acceptance:

- each new document identifies its code owners, inputs/outputs, invariants, and
  links to current contracts
- `docs/README.md` and the root README link to the new design entry points

## Execution Order

1. Establish document conventions and add the offline local-link checker.
2. Repair existing links and enforce the clean baseline in tests.
3. Rewrite the root README and docs index around the agreed navigation model.
4. Remove the obsolete project-pitch note and clean historical terminology.
5. Refresh the glossary and add graph/hierarchy plus attribution design notes.
6. Re-run documentation checks, architecture generation where source docs affect
   it, and the full Markdown link audit.

## Completion Criteria

- root README provides an accurate one-screen human overview
- local Markdown links and anchors pass the new offline check
- no tracked Markdown references obsolete organization-specific project language
  or historical implementation behavior
- plan material is confined to `docs/plans/` or final Notes sections that link
  there
- glossary and new design notes cover the active graph/hierarchy and attribution
  paths
- documentation navigation identifies current contracts, design rationale, and
  plans without ambiguity
