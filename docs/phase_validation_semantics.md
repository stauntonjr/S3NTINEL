# Phase Validation Semantics

This note documents the current meaning of steady-phase validation versus
transition-region validation in the active V2 phase stack.

For current implementation ownership, see:
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)
- [docs/v2_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/v2_architecture.md)

## Primary phase taxonomy

The primary detected phase taxonomy remains the steady operating regimes:
- `gate_turnaround`
- `takeoff_climb`
- `cruise`
- `descent_approach`

These remain the headline labels for:
- phase mapping
- phase confusion matrices
- macro F1
- centroid comparison

The current headline metric intentionally does **not** add a fifth transition
class. This keeps steady-phase results comparable across runs.

## Auxiliary transition state

`phase_windows` also carries an auxiliary detected state:
- `phase_state_detected = stable`
- `phase_state_detected = transition_region`

This auxiliary state is meant to mark ambiguous boundary intervals between
neighboring steady regimes. It is not part of the primary phase taxonomy.

Detected transition windows may also carry:
- `transition_from_phase_id_detected`
- `transition_to_phase_id_detected`

These fields are metadata only. They do not expand the label space into
pairwise transition classes.

## Truth transition derivation

Simulation truth does not author a separate transition taxonomy.

Instead, transition truth is derived from existing `phase_label` overlap:
- `truth_phase_label_primary`
  - majority overlapping steady label for the window
- `truth_phase_state`
  - `transition_region` when the window overlaps 2 or more distinct truth labels
  - `stable` otherwise
- `truth_transition_from_label`
- `truth_transition_to_label`
  - first and last distinct overlapping truth labels when the truth state is
    `transition_region`

This keeps the transition truth generic and derivable from any labeled phase
timeline, not only from the current simulator.

## Two complementary transition metrics

Current phase validation now reports transition quality in two different ways.

### 1. Exact transition-window overlap

`transition_state_validation` includes:
- `truth_phase_state_counts`
- `detected_phase_state_counts`
- `phase_state_confusion`
- `transition_region_precision`
- `transition_region_recall`
- `transition_region_f1`

These metrics answer:
- did the detector mark the same windows as transition regions?

This is a strict metric and can understate useful behavior when the detector
finds the correct boundary but places it a few windows early or late.

### 2. Transition-event alignment

`transition_state_validation.transition_event_alignment` collapses contiguous
transition windows into transition events and compares them by:
- `tail_id`
- `flight_id`
- `transition_from_label`
- `transition_to_label`

It reports:
- truth and detected transition-event counts
- counts by transition label pair
- nearest detected event per truth transition event
- `mean_abs_win_id_delta`
- `mean_abs_progress_delta`

These metrics answer:
- did the detector find the correct transition pair?
- how far away was the detected boundary from the truth boundary?

`mean_abs_progress_delta` is normalized by flight window count, so it is easier
to compare across flights with different segmentation densities.

## Steady-phase centroid semantics

Centroid comparison now excludes truth windows whose derived truth state is
`transition_region` when building steady-phase truth centroids.

The comparison summary therefore includes:
- `truth_label_window_counts`
- `stable_window_label_counts`
- `excluded_transition_window_counts_by_phase_label`

This keeps steady-regime centroids from being polluted by boundary windows.

## Practical reading guidance

When reviewing a run:

1. Use primary phase macro F1 to judge steady-regime detection quality.
2. Use transition-window precision/recall to judge exact overlap quality.
3. Use transition-event alignment to judge whether the detector found the right
   boundary pair even when exact timing differs.

If steady-phase metrics are strong but transition-window F1 is weak:
- check transition-event alignment before changing the detector
- a large alignment error suggests boundary timing drift
- a small alignment error suggests truth-overlap labeling is stricter than the
  detector semantics

## Scope

This document describes the current validation/report semantics only.

It does not imply:
- a fifth learned transition phase
- a pairwise transition taxonomy in production inference
- a fleet-learned revisitable transition model

Those remain possible future extensions, but they are not part of the current
phase contract.
