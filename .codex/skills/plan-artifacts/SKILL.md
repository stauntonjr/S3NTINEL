---
name: plan-artifacts
description: Use when creating, revising, consolidating, or retiring roadmap and proposal documents. This skill treats docs/plans as the canonical location for non-authoritative plan artifacts and keeps planning material separate from current implementation docs.
---

# Plan Artifacts

## Use This Skill When

Use this skill for work such as:

- creating a new technical plan
- updating an existing roadmap or proposal
- consolidating overlapping plan docs
- retiring a stale plan after implementation
- moving planning notes out of authoritative current docs

## Core Rule

`docs/plans/` is the canonical home for plan artifacts in this repository.

Plans are not the source of truth for current behavior. Current behavior belongs in:

- `README.md`
- package READMEs
- `docs/current/`
- code, schemas, contracts, and validation outputs

## Required Workflow

### 1. Inspect Before Writing

Before creating or updating a plan:

- read the relevant package README files
- read the current implementation docs in `docs/current/`
- inspect `docs/plans/` for an existing plan on the same topic
- avoid creating a second plan when one should be updated or replaced

If a current doc and a plan disagree, treat the current doc and code as authoritative.

### 2. Decide The Correct Action

Use exactly one of these actions:

- create a new plan artifact
- update an existing plan artifact
- consolidate multiple plan artifacts into one
- remove or retire a stale plan artifact

Do not create a new plan file just to avoid editing an older plan.

### 3. Keep Plans In `docs/plans/`

All roadmap and proposal documents belong under `docs/plans/`.

Use descriptive snake_case filenames such as:

- `phase_boundary_plan.md`
- `anomaly_channel_expansion_plan.md`
- `simulation_medium_term_plan.md`

Do not place planning material in:

- `docs/current/`
- package READMEs
- generated `docs/architecture/`

unless the user explicitly wants a short pointer or summary there.

### 4. Use A Visible Plan Status Header

Every plan artifact should start with:

- `Status: Plan`
- `Authority: Non-authoritative roadmap. Use package READMEs and docs/current/ for current behavior.`

If the file is being retired instead of deleted, update the header to reflect that clearly.

### 5. Write Plans Against The Real Repo

Ground the plan in the current codebase, not memory or wishful structure.

When a plan discusses implementation:

- name the current canonical owner modules
- name the current artifact and report surfaces
- distinguish current behavior from proposed changes
- avoid stale paths, deleted modules, and superseded seams

### 6. Prefer Updating Over Duplicating

If an existing plan covers the same work:

- revise it in place
- add a focused section for the new proposal
- or replace it and remove the obsolete file

Do not leave multiple overlapping plan docs that disagree about next steps.

### 7. Clean Up After Implementation

When a planned change has been implemented:

- move authoritative semantics into code, contracts, READMEs, or `docs/current/`
- shrink, mark obsolete, or delete the plan artifact
- remove stale links that still present the plan as future work

Plans should not remain active-looking after the code has already moved on.

### 8. Update Navigation When Needed

If you add, remove, or materially rename a plan artifact:

- update `docs/README.md` if the plan is part of the recommended docs surface
- update any package README or current doc that links to it
- keep `docs/plans/` discoverable but clearly non-authoritative

## What Good Looks Like

- one plan per topic, not several overlapping drafts
- plan files live under `docs/plans/`
- each plan has a visible non-authoritative status header
- plans reference current code paths and current docs correctly
- implemented work is migrated out of plans and into authoritative docs
- stale plans are removed or clearly marked

## Red Flags

Stop and clean up if you see:

- plan material in `docs/current/`
- roadmap text in package READMEs pretending to describe current behavior
- multiple plan docs for the same topic with different next steps
- plan docs referencing deleted files or old module paths
- implemented behavior still documented only in `docs/plans/`
