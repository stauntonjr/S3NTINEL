---
name: implement-python-pyspark
description: Use for most implementation, edit, review, and refactor work in this repository. The repository has strong intentional patterns around domain dataclasses, stable payload modeling, thin scripts and top-level workflow composition, reusable domain-stage modules, and explicit Spark hot-path logic. New code should align to those patterns immediately, and structural refactoring should be treated as part of task completion rather than optional cleanup.
---

# Implement Python PySpark

## Overview

Implement, review, and refactor Python and PySpark code so it matches this repository's established design language.

Read these references before making substantial structural changes:

- [references/repo-design-principles.md](references/repo-design-principles.md)
- [references/naming-principles.md](references/naming-principles.md)
- [references/layering-rules.md](references/layering-rules.md)
- [references/pyspark-primitives.md](references/pyspark-primitives.md)
- [references/stable-payload-modeling.md](references/stable-payload-modeling.md)

Use them as the source of truth for:
- local design precedent
- naming discipline
- layering
- dataclass and inheritance decisions
- `Frame` / `Table` usage where appropriate
- stable payload modeling

## Refactoring Is Part Of Correctness In This Repo

This repository's current structure reflects repeated correction and consolidation, not accidental style.

Assume that many first-pass implementations will need structural refactoring to match the repo's design language. In this codebase, stopping to refactor is normal and often required to complete the task correctly.

Do not treat refactoring as optional polish when you notice:
- repeated local helper patterns
- dict payload assembly for stable concepts
- duplicated implementations across production and test paths
- behavior placed on the wrong module or object
- script or top-level workflow files growing domain logic
- names that drift from surrounding domain language
- stable Spark outputs represented only as bare `DataFrame`s when a typed object would be clearer

When these appear, fix them directly rather than preserving the initial shape.

## Repo-Local Structural Preferences

Default preferences for this repository:

- use concrete dataclasses for stable concepts with named fields and durable meaning
- use a narrow shared base class when several concrete payloads share real mechanics
- prefer classmethods and direct construction over builder-heavy or registry-heavy assembly
- keep scripts and top-level workflow modules thin
- keep domain-stage Spark logic in reusable library modules
- keep Spark hot-path logic explicit and inspectable
- reuse local naming patterns when they fit, but do not force new concepts into an old naming bucket if a better noun exists

Do not replace a coherent object model with loose dict assembly, `build_*` helpers, or registries unless the payloads are genuinely transient or heterogeneous.

## User Preference Override

When the user asks for a dataclass payload, a shared base class, a `Frame` / `Table` hierarchy, or another repo-aligned object model, treat that preference as binding unless it would clearly break semantics or materially worsen Spark hot-path behavior.

In those cases:
- refine the requested design
- minimize it
- make it coherent
- proceed

Do not push back with generic anti-abstraction arguments if the requested structure matches existing repo patterns.

## Single Modeling Path Rule

Production modeling concepts in this repository must have exactly one implementation path.

That means:

- canonical model semantics live in the Spark `Table` / `Frame` / stage-library owner
- local pandas or in-memory code may consume model outputs for bounded reporting, validation, evaluation, plotting, or final test assertions
- local pandas or in-memory code must not re-implement production model semantics

When you find parallel Spark and local/test implementations of the same model concept:

- consolidate on the canonical Spark owner
- delete the duplicate path
- rewrite tests to hit the canonical owner directly
- update contract tests and docs in the same pass

Do not leave duplicate local/test/production modeling implementations as a follow-up cleanup item.

## Workflow

1. Read the existing code path before proposing structural changes.
2. Identify the stable domain concepts already present in names, schemas, transforms, and persisted artifacts.
3. Check whether the code belongs in:
   - top-level workflow composition modules
   - domain-stage implementation modules
   - domain model classes
   - stable payload or persisted output objects
4. Replace primitive bags of state with focused dataclasses when the state has real domain meaning.
5. Replace bare persisted `DataFrame` outputs with typed objects when the output is a stable artifact and that modeling clarifies ownership.
6. Attach behavior to the natural owner.
7. Remove duplicate implementations instead of preserving local, test, and production variants.
8. Keep Spark work in Spark on the hot path.
9. Update tests so they use the same taxonomy, object model, and boundaries as the code.
10. Verify behavior.
11. Update documentation and naming after verification.

## Response Format

When proposing or applying a change:

1. Summarize the current structural and naming problems.
2. State any semantic or Spark-performance risks.
3. Propose the minimal coherent repo-aligned design.
4. Show the updated code.
5. Show the test updates or new tests.
6. Note any documentation or naming changes that should also be made.

## Self-Check Before Finalizing

Before finalizing, ask:

- Did I model stable concepts as dataclasses or classes where appropriate?
- Did I leave behind dict payloads that should be typed objects?
- Did I leave a stable persisted artifact as a bare `DataFrame` when a typed object would be clearer?
- Did I introduce helper functions where a natural owner exists?
- Did I duplicate behavior instead of consolidating it?
- Did I keep scripts and top-level workflow modules thin?
- Did I keep domain-stage Spark logic in the right library module?
- Did I preserve explicit Spark hot-path logic?
- Did I choose names that fit the surrounding domain language rather than forcing a template?

If not, refactor before finalizing.
