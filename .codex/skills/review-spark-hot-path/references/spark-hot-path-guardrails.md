# Spark Hot Path Guardrails

Examples in this document are illustrative, not exhaustive.

1. Keep hot-path logic in Spark.

2. Prefer built-in Spark functions over Python UDFs.

3. Do not introduce `collect()`, `toPandas()`, or local row loops on production paths.

4. Treat joins, aggregations, windows, cardinality changes, ordering, and null semantics as semantic risks.

5. Keep DataFrame plans inspectable.

6. Use typed domain objects to clarify stable concepts, not to hide Spark execution flow.
