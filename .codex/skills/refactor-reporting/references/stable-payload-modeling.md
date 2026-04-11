# Stable Payload Modeling

Use concrete dataclasses for stable persisted or passable payloads across the repository.

Examples in this document are illustrative, not exhaustive.

## Apply This Broadly

This guidance applies to many kinds of stable concepts, including:
- persisted outputs
- typed results
- manifests
- reports
- summaries
- grouped artifacts
- DataFrame-backed domain objects
- contract-bearing payloads

The exact nouns should follow the local domain, not a fixed suffix list.

## Rules

1. If a payload has stable named fields and durable meaning, prefer a concrete dataclass.

2. If several concrete payloads share real persistence, serialization, rendering, or DataFrame-backed mechanics, use a narrow shared base class.

3. Do not replace a coherent object model with dict payload builders or helper-only glue.

4. Use free functions for small mechanics, not as a substitute for stable payload types.

5. Use raw `DataFrame` values for local temporary variables and transient internal transforms, not as the long-term API for stable artifacts.

6. Prefer direct construction and classmethods over builder-heavy assembly.

7. When a persisted artifact is DataFrame-backed, prefer a typed object over a bare `DataFrame` when that improves ownership, naming, and cohesion.
