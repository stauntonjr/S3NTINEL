---
name: review-spark-hot-path
description: Use when reviewing or refining PySpark code for distributed execution, semantic correctness, and performance on the hot path.
---

# Review Spark Hot Path

## Overview

Review Spark code for:
- distributed execution safety
- semantic correctness
- DataFrame plan clarity
- performance risks
- accidental Python-side fallback

Read:
- [references/spark-hot-path-guardrails.md](references/spark-hot-path-guardrails.md)
- [references/layering-rules.md](references/layering-rules.md)
- [references/pyspark-primitives.md](references/pyspark-primitives.md)

## Focus

- keep hot-path logic in Spark
- avoid `collect()`, `toPandas()`, and local row loops
- prefer built-in Spark functions over Python UDFs
- treat joins, aggregations, windows, cardinality changes, and null semantics as semantic risks
- preserve inspectability of the Spark plan
- do not hide execution semantics behind wrappers

## Review Output

Report findings first, ordered by severity, with file and line references when
available. For each finding, state the semantic or runtime risk and the
smallest corrective direction.

Check explicitly for:

- unbounded `collect()` or `toPandas()` on fact tables
- Python UDFs or row loops where built-in Spark expressions would work
- join fanout, accidental repartitioning, skew, and unexpected cardinality
- null, duplicate-key, ordering, and time-window semantics
- driver-side work hidden behind helpers or typed wrappers
- missing targeted tests for the changed plan or contract

Treat bounded driver-side reference artifacts as acceptable only when their
bound is explicit, validated, and documented. Separate confirmed findings from
residual risks and untested assumptions.
