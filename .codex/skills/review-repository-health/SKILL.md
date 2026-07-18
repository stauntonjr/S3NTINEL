---
name: review-repository-health
description: Use when auditing repository setup, documentation, IDE or Codex configuration, test workflow, generated artifacts, or implementation drift before resuming development or accepting a large change.
---

# Review Repository Health

Audit the repository as a working system. This skill is review-only by default:
identify risks and propose the smallest fixes, but do not edit unless the user
also asks for remediation.

## Review Surface

Inspect:

- Git status, branch, recent history, and staged/unstaged boundaries
- root and nested `AGENTS.md` guidance, `.codex/`, and `.vscode/`
- environment specs, package metadata, test configuration, and documented commands
- current package READMEs versus `docs/current/` and generated architecture docs
- pipeline stage catalogs versus actual grouped runner definitions
- tracked, ignored, generated, and untracked artifacts
- tests covering setup, public contracts, pipeline orchestration, and replay

## Method

1. Build an inventory before reading deeply.
2. Establish current source-of-truth precedence.
3. Search for stale paths, removed names, duplicate configuration, and commands
   that do not match the actual files or runners.
4. Distinguish defects from intentional generated or historical artifacts.
5. Run only cheap read-only checks unless the user authorizes execution of
   tests or workloads.

## Findings Format

Report findings first, ordered by severity. Each finding should include:

- severity and concise title
- file and line reference when available
- observed evidence
- practical impact
- smallest recommended fix

Then report:

- confirmed healthy areas
- open questions or assumptions
- verification not performed
- a short prioritized remediation sequence

Do not treat stale plans as current behavior, generated architecture snapshots
as manual source, or a passing unit suite as proof of Spark runtime health.
