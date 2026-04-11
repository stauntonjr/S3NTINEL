---
name: repo-agent-loop
description: Use when a repository task should be handled end-to-end: inspect the codebase and relevant skills, capture baseline metrics when modeling is involved, edit the canonical code path, update tests/contracts/comments/docs, run staged verification, refresh validation outputs or replays when needed, back out dead ends, and commit/push when the task explicitly calls for delivery.
---

# Repo Agent Loop

## Use This Skill When

Use this skill for end-to-end repository work where the agent is expected to drive the task through implementation, verification, documentation, and delivery rather than stopping after a partial code change.

This is especially important for:

- modeling changes
- validation or reporting changes
- architecture refactors
- tasks that should end in a verified commit

## Core Rule

Do not treat coding, testing, validation, docs, and delivery as separate follow-up tasks.

The loop is:

1. inspect
2. define success
3. change the canonical owner
4. verify
5. compare against baseline
6. update docs/contracts
7. deliver cleanly

## Required Loop

### 1. Inspect First

Before editing:

- read the relevant package README files
- load any repo-local skills that apply
- inspect `git status`
- identify the canonical owner for the behavior you are changing
- search for duplicate semantics in `libs/`, `pipelines/`, and `tests/`

If there is already a duplicate local/test path, consolidate before adding more logic.

### 2. Capture Baseline And Success Criteria

If the task changes modeling, validation, or reporting semantics, record:

- current baseline metrics
- current relevant report or replay artifact paths
- current failing tests or known gaps
- explicit success criteria

Success criteria must be concrete, for example:

- behavior-preserving cleanup
- specific metric improvement
- no regression on protected metrics
- new validation surface emitted
- stale contract removed

Do not edit first and decide later whether the change helped.

### 3. Protect User Work

Assume the worktree may be dirty.

- do not overwrite unrelated local changes
- do not revert user work
- stop if the task conflicts with existing edits you cannot safely integrate

### 4. Edit The Canonical Path

Make the change in the canonical production owner only.

- for production modeling, this means the canonical Spark `Table` / `Frame` / stage-library path
- local pandas code may consume outputs for bounded validation/reporting/evaluation, but must not become a second model path

Keep the change minimal and coherent.

### 5. Update The Full Surface

When the code changes, update all affected surfaces in the same pass:

- unit tests
- integration tests where appropriate
- contract tests
- inline comments when they clarify non-obvious logic
- package README files
- repo docs if public semantics changed
- relevant Codex skills if the repo workflow changed

Do not leave stale documentation or stale guardrails behind.

### 6. Verify In Stages

Run verification in this order:

1. cheap checks
   - `py_compile`
   - static/contract checks
2. targeted unit tests
3. targeted integration tests
4. broader replay or end-to-end run only when needed

Do not jump straight to the broadest test band if a smaller band can localize failures faster.

### 7. For Modeling Work, Inspect Validation Outputs

If the task affects modeling performance or validation semantics:

- refresh the relevant reports or replay outputs
- compare results directly against the recorded baseline
- inspect the changed validation sections, not just pass/fail

Look at the actual artifact outputs that matter:

- validation summaries
- full-run reports
- stage reports
- replay artifacts

### 8. Back Out Dead Ends

For modeling or heuristic changes:

- keep improvements
- keep behavior-preserving refactors
- revert dead ends that do not improve the agreed target

Do not accumulate speculative heuristics just because they compile.

### 9. Final Hygiene

Before finalizing:

- run `git diff --check`
- run `git status`
- search for stale references to removed APIs, configs, or semantics
- make sure docs and tests reflect the kept code, not abandoned experiments

Also avoid process sprawl while iterating:

- reuse existing long-running sessions when possible
- do not leave many redundant test or replay processes open

### 10. Commit And Push Deliberately

Create a commit only when the change is coherent and verified enough to stand on its own.

Push only when:

- the user explicitly asked for push
- or the task is explicitly a finish-and-deliver workflow

If verification is incomplete, say so clearly before committing or pushing.

## Modeling-Specific Checks

When the task touches modeling performance, always ask:

- did I record the starting metrics?
- did I compare the end metrics against the start?
- did I inspect the validation artifact that actually measures the target behavior?
- did I revert dead ends instead of stacking them?

If the answer to any of these is no, the loop is not complete.

## Good Outcomes

The skill is being followed correctly when:

- the canonical code path changed
- tests target that same path
- reports or validation outputs were checked when relevant
- docs and skills were updated in the same pass
- the branch is cleaner at the end, not more confused
