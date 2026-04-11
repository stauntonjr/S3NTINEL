# Naming Principles

Use precise domain nouns and preserve semantic distinctions already present in the repository.

Examples in this document are illustrative, not exhaustive. Do not treat them as a closed list of approved type names, suffixes, or object categories.

## General Rules

1. Prefer concrete domain nouns over generic technical nouns.

2. Reuse existing naming distinctions when they genuinely fit the concept.

3. Do not force a new concept into an existing noun family if that weakens semantic clarity.

4. When introducing a new noun, prefer one that:
   - names the domain concept directly
   - matches surrounding naming grain
   - distinguishes the object from nearby concepts
   - will still make sense after refactors

5. Prefer nouns over role labels.
   Avoid names like `Manager`, `Builder`, `Processor`, `Registry`, `Helper`, `Handler`, or `Utils` unless the role is truly generic and no clearer domain noun fits.

6. Use suffixes consistently within a local concept family once chosen.

## Distinctions Worth Preserving

This repository often distinguishes concepts such as:
- declarative definitions
- derived or operational plans
- fitted or semantic models
- grouped artifacts
- DataFrame-backed computational objects
- persisted or materialized outputs
- grouped persisted packages
- runtime contexts
- filesystem paths
- structured reports
- concise summaries
- inventory manifests

Reuse distinctions like these when they fit. Introduce a new noun when it is more precise.

## DataFrame-Backed Naming

- Use `*_df` for local temporary variables.
- Prefer a typed object name for stable public, persisted, or reusable concepts.
- Use names like `Frame` or `Table` only when they fit the concept; do not rename a better domain noun just to force that pattern.
