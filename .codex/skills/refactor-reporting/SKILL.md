---
name: refactor-reporting
description: Use when refining report, summary, manifest, or run-output code. This skill adds reporting-specific naming and modeling distinctions on top of the repository's general design policy.
---

# Refactor Reporting

## Overview

Use this skill for reporting-specific refinements after applying the repository's broad design rules.

Read:
- [references/reporting-distinctions.md](references/reporting-distinctions.md)
- [references/naming-principles.md](references/naming-principles.md)
- [references/stable-payload-modeling.md](references/stable-payload-modeling.md)
- [references/layering-rules.md](references/layering-rules.md)

## Focus

This skill is specifically for:
- distinguishing report-like payloads from other persisted outputs
- clarifying `Report` vs `Summary` vs `Manifest` when those distinctions matter
- keeping run-level report orchestration thin
- preserving file and payload contracts while improving structure

Do not use this skill to override the repo's general design rules. It only adds reporting-specific distinctions.
