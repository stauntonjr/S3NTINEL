# Reporting Distinctions

Use these distinctions for reporting-related code when they clarify the design.

Examples in this document are illustrative, not exhaustive.

## Report

A `Report` is often:
- a structured persisted payload with semantic interpretation
- multi-section or multi-part output
- content intended for JSON, markdown, or other human/API consumption

## Summary

A `Summary` is often:
- a concise reduced view of a larger result
- compact top-level facts or metrics

Do not use `Summary` for a full report.

## Manifest

A `Manifest` is often:
- inventory
- paths
- artifact metadata
- emitted-output listings
- lineage or file-level metadata

Do not use `Manifest` for richly interpreted analysis if a more precise noun would be clearer.

## Rules

1. Keep `Report`, `Summary`, and `Manifest` distinct when those distinctions matter.
2. Prefer concrete dataclasses for each stable payload type.
3. Keep run-level report assembly thin.
4. Preserve current filenames and file contracts unless explicitly changing them.
5. If a better domain noun exists, use it rather than forcing one of these labels.
