# Repo Design Principles

Use these principles as the default design policy for this repository.

Examples in this document are illustrative, not exhaustive. Do not treat them as a closed list of approved type names, suffixes, or object categories.

## Core Shape

1. Prefer real domain objects.
   If a concept has stable identity, named fields, or coherent behavior, model it as a dataclass or class rather than a loose dict or helper-function cluster.

2. Prefer concrete dataclasses for stable payloads.
   Reports, manifests, summaries, specs, plans, bundles, artifact sets, frames, tables, and other typed state objects often deserve concrete dataclasses with explicit fields. Use that as a preference, not a forced template.

3. Prefer narrow shared base classes for repeated mechanics.
   When multiple concrete payloads share persistence, serialization, rendering, or DataFrame-backed behavior, introduce a minimal shared base class instead of duplicating methods or replacing the model with helper functions.

4. Prefer direct construction and classmethods over builder sprawl.
   Use direct constructors and classmethods such as `from_raw`, `from_frame`, `from_spec`, `from_dict`, or similar repo-local shapes before introducing builder registries or assembly frameworks.

5. Let local naming patterns inform new names without closing the vocabulary.
   Reuse established distinctions when they fit. Introduce a new precise noun when that better represents the concept.

6. Put behavior on the natural owner.
   If a method reads like behavior of a specific object, place it there instead of scattering it across helpers.

7. Keep top-level workflow and script files thin.
   Top-level workflow modules should compose major stages. Scripts should parse args, call library code, and exit.

8. Keep hot-path Spark logic explicit.
   Spark DataFrame and Column work should stay readable and inspectable. Do not hide joins, windows, or aggregations behind unnecessary object ceremony.

9. Use free functions for small mechanics, not as a substitute for modeling.
   Tiny stateless transforms, Spark expression helpers, file helpers, and one-off adapters are good free functions. Stable domain payloads are not.

10. Reduce code size and duplication directly.
    Prefer one coherent implementation over layered wrappers, duplicate local/test variants, or registry-plus-helper indirection when a simple object model is clearer.

## Refactor Decision Policy

11. In this repo, refactoring is often part of correctness.
    If generated code drifts from existing repository patterns, stop and refactor it into the established shape rather than preserving the first draft.

12. When the user states a structural preference that matches repo precedent, treat it as binding unless it clearly harms semantics or Spark hot-path behavior.

13. Prefer alignment with local precedent over generic framework instincts.
