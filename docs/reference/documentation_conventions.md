# Documentation Conventions

This document defines the documentation contract for S3NTINEL. It keeps current
implementation behavior, stable vocabulary, design rationale, research context,
and proposals distinguishable to both readers and tooling.

## Document Classes

| Location | Purpose | Authority |
| --- | --- | --- |
| Root `README.md` | human orientation and first commands | entry point only; link onward for detail |
| Package README | code ownership, entrypoints, and current local behavior | authoritative with code and tests |
| `docs/current/` | cross-package active contracts and operational semantics | authoritative for the documented contract |
| `docs/reference/` | stable terminology, schemas, and theory | authoritative vocabulary/reference |
| `docs/design/` | rationale, boundaries, invariants, and tradeoffs | explains active design; code remains authoritative |
| `docs/simulation/` | simulation architecture and subject-matter guidance | authoritative simulation guidance where linked by current contracts |
| `docs/research/` | exploratory context and evaluation methods | non-authoritative unless adopted by a current contract |
| `docs/plans/` | roadmap and implementation sequencing | non-authoritative |
| `docs/archive/` | historical source material and captured artifacts | non-authoritative; excluded from active link validation because historical links may be stale |
| `docs/architecture/` | generated C4 and repository maps | generated snapshot; stale until regenerated |

## Authority And Ownership

Current behavior belongs in code, tests, schemas, package READMEs, or
`docs/current/`. A cross-package document should link to the owning package
README or implementation surface near its opening. When sources disagree,
prefer current code and its tests, then the package README.

Every active plan, except an index `README.md`, starts with:

```text
Status: Plan
Authority: Non-authoritative roadmap. Use package READMEs and docs/current/ for current behavior.
```

Use `Status: Completed` with the same authority line when a plan is retained as
an implementation record rather than an active proposal.

## Links And Headings

- Use repository-relative links from the root README, such as `docs/README.md`.
- Use document-relative links elsewhere; `/path` means repository-root-relative.
- Link local Markdown headings with their GitHub-style anchor.
- Do not use absolute filesystem links in tracked documentation.
- Give each document one top-level `#` heading.
- Run `python tools/check_markdown_docs.py` after changing tracked Markdown.

The checker validates local files, local anchors, duplicate top-level headings,
and plan headers without using the network. External links are intentionally not
part of this offline gate.

## Terminology And Planning

- Use names from the [glossary](glossary.md) for persisted artifacts and fields.
- Prefer active names over historical aliases. Describe a compatibility field
  only when it is part of a current runtime contract.
- Keep implementation sequencing in `docs/plans/`.
- If an authoritative or research document needs a brief boundary statement,
  put it in a final `## Notes` section and link to the relevant plan.

## Maintenance

When adding an artifact, stage, or durable term:

1. update its owning schema, package README, and tests as appropriate;
2. update the glossary when the name crosses package boundaries;
3. update a current contract or design rationale when it changes system behavior;
4. update generated architecture only through its generation workflow.
