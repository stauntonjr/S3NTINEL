# PySpark Primitives

Use thin typed dataclasses to push Spark boilerplate down and keep domain logic prominent.

Examples in this document are illustrative, not exhaustive. Do not treat them as a closed framework.

## Core Direction

A useful primitive set in this repository often includes concepts like:
- a small column or field descriptor
- a simple schema object
- a typed DataFrame-backed object
- a persisted or materializable DataFrame-backed object

These are patterns, not a forced ontology.

## Frame and Table

When the distinction is useful:

- `Frame` can represent a typed DataFrame-backed domain object
- `Table` can represent a persisted or materializable DataFrame-backed object

In many cases, `Table` may be a subtype of `Frame` when that reflects a real domain relationship.

Use that hierarchy when it clarifies ownership and artifact identity. Do not force it where a more precise domain noun is better.

## Design Rules

1. Prefer wrapping DataFrames in typed domain objects when they have stable meaning.

2. Prefer a typed persisted object over a bare `DataFrame` when the output is a stable artifact and object modeling clarifies ownership.

3. Use `*_df` only for local temporary variables, not for stable object fields.

4. Keep primitive classes thin and explicit; do not build a framework for its own sake.

5. Put business/domain transforms on concrete typed objects when the behavior naturally belongs there.

6. Put persistence/materialization behavior on the persisted object when that is the clearest owner.

7. Keep Spark plans inspectable; do not hide execution logic behind excessive wrappers.

8. Add optimization or observability helpers sparingly and only after repeated need.

## Inheritance Guidance

Use inheritance when it expresses a real subtype relation already present in the repo's domain model.

Examples of good inheritance:
- a persisted DataFrame-backed type as a subtype of a more general DataFrame-backed type
- a narrow shared base class for concrete payloads that genuinely share real mechanics

Avoid inheritance that exists only for surface similarity.
