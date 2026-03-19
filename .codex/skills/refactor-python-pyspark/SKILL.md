---
name: refactor-python-pyspark
description: Refactor, review, and diagnose Python and PySpark code toward concise, domain-driven designs with dataclasses, coherent object boundaries, and production-scale Spark hot paths. Use when Codex needs to check existing Python or PySpark code for refactor opportunities, restructure implementations, collapse duplicate paths, move behavior onto the right domain objects, replace primitive state with dataclasses, keep tests aligned with the production taxonomy, or improve Spark pipeline structure and performance without introducing wrapper-heavy abstractions.
---

# Refactor Python PySpark

## Overview

Refactor existing Python and PySpark code into concise, domain-driven designs that are easy to reason about and performant on Spark hot paths.

Read [references/refactoring-principles.md](references/refactoring-principles.md) before making substantial structural changes. Use it as the source of truth for object design, state modeling, and Spark hot-path decisions.

## Workflow

1. Read the existing code path before proposing new structures.
2. Identify the domain objects already present in names, schemas, and transformations.
3. Remove duplicate implementations instead of preserving local, test, and production variants.
4. Replace primitive bags of state with focused dataclasses where the state has real domain meaning.
5. Attach construction and transformation logic to the most natural domain class.
6. Keep Spark work in Spark on the hot path; avoid Python-side row loops, per-record UDFs, or local fallback logic unless the code path is explicitly cold.
7. Update tests so they use the same taxonomy, object model, and boundaries as the code; do not preserve stale test abstractions that force the implementation into the wrong shape.
8. Run the relevant tests or other behavioral checks and confirm the refactor preserved the intended contract.
9. After tests pass, update any impacted documentation so it uses the same taxonomy, object boundaries, and behavior described by the code.
10. Re-check whether the refactor reduced conceptual overhead, line count, and duplicated logic.

## Refactoring Rules

- Prefer one implementation shared across call sites.
- Prefer dataclasses over tuples, dicts, and parallel primitive arguments when the values represent one domain object.
- Prefer dataclass properties for derived metadata and `pyspark.sql.Column` expressions that logically belong to that object.
- Prefer direct instantiation and classmethods over external builders unless the codebase already has a strong competing pattern.
- Prefer object methods for transformations that depend on object state.
- Place methods on the class with the clearest domain ownership.
- Respect existing code before adding new classes or files; first ask whether the behavior belongs on an existing type.
- Keep tests aligned with the code's domain model and naming; testing should serve the code, not the other way around.
- Do not let test-only taxonomy, fixtures, or helper layers preserve obsolete production structure.
- After behavior is verified, update affected documentation to match the current code and domain language.
- Avoid thin wrappers, naming drift, and repeated glue code.

## Spark Hot Path

- Assume the hot path must scale in production.
- Optimize algorithm and data-structure choices before polishing interfaces.
- Express hot-path Spark behavior coherently through domain dataclasses when possible.
- Model Spark metadata as object state, `Column` expressions as properties, and `DataFrame` transforms as methods when that improves clarity without hiding the plan.
- Keep transformations composable and inspectable.

## Output Checklist

- Produce one clear implementation.
- Keep tests and production code in the same conceptual model.
- Keep documentation synchronized after tests confirm the refactor.
- Preserve or improve Spark execution characteristics on the hot path.
- Leave the codebase easier to read than before.
- Use names that match the business/domain concepts rather than temporary implementation details.
- Minimize added code and prefer integrating with existing modules over creating new ones.
