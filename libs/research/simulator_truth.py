"""Validation-only authored fault evaluation; never imported by model fitting."""

import json
from collections import Counter
from dataclasses import asdict

from libs.research.simulator_protocol import AuthoredInterval
from libs.simulation.fault.spec import resolve_window_fault_type

CHALLENGES = {
    "cross_sensor": ("coupling_break", "coupling_inversion"),
    "temporal": ("timing_jitter", "state_chatter", "illegal_transition"),
    "stuck_at": ("stuck_at",),
    "drift": ("drift",),
}


def authored_intervals(flight, seed):
    intervals = []
    program = flight.misbehavior_program_spec
    for window in program.windows if program else ():
        meta = window.metadata
        identity = meta.get("misbehavior_window_id") or meta.get("fault_window_id")
        if not identity or window.end_step_exclusive <= window.start_step:
            raise ValueError("missing authored identity or invalid interval")
        family = str(meta.get("fault_family_label") or meta.get("misbehavior_family_label") or "unknown")
        intervals.append(
            AuthoredInterval(
                seed,
                str(identity),
                window.start_step,
                window.end_step_exclusive,
                family,
                str(resolve_window_fault_type(window) or "unknown"),
                {
                    "kind": window.subject_kind,
                    "module_id": window.module_id,
                    "parameter_name": window.parameter_name,
                    "coupling_id": window.coupling_id,
                },
                dict(window.context),
            )
        )
    _validate(intervals)
    return tuple(intervals)


def _validate(intervals):
    identities = [(i.seed, i.window_id) for i in intervals]
    if len(identities) != len(set(identities)) or any(not i.window_id for i in intervals):
        raise ValueError("duplicate/empty authored interval identity")


def coverage_inventory(intervals):
    details = {i.detail for i in intervals}
    return {
        challenge: {
            "status": "covered" if details.intersection(names) else "not_covered",
            "accepted_detail_labels": list(names),
            "present_details": sorted(details.intersection(names)),
        }
        for challenge, names in CHALLENGES.items()
    }


def evaluate_truth(intervals, scores, thresholds, raw_truth_path, retained_steps):
    _validate(intervals)
    applied, grid_applied = Counter(), Counter()
    retained = set(retained_steps)
    known = {i.window_id: i for i in intervals}
    with raw_truth_path.open() as stream:
        for line in stream:
            row = json.loads(line)
            identity = row.get("misbehavior_window_id") or row.get("fault_window_id")
            active = row.get("misbehavior_applied") is True or row.get("fault_applied") is True
            if identity in known and active:
                interval = known[identity]
                step = row["step_index"]
                if interval.start_step <= step < interval.end_step_exclusive:
                    applied[identity] += 1
                    grid_applied[identity] += step in retained
    faults, alerts = [], []
    for key, threshold in thresholds.items():
        arm, model = key.split("/")
        matching = [s for s in scores if s.arm == arm and s.model == model and s.role == "faulted_test"]
        alerting = [s for s in matching if s.mean_bits > threshold.value]
        for score in alerting:
            overlapping = [
                i.window_id
                for i in intervals
                if i.seed == score.seed
                and min(i.end_step_exclusive, score.end_step_exclusive) > max(i.start_step, score.start_step)
            ]
            alerts.append({**asdict(score), "overlapping_fault_ids": overlapping})
        for interval in intervals:
            overlaps = [
                {
                    "start_step": s.start_step,
                    "end_step_exclusive": s.end_step_exclusive,
                    "overlap_steps": min(interval.end_step_exclusive, s.end_step_exclusive)
                    - max(interval.start_step, s.start_step),
                }
                for s in alerting
                if s.seed == interval.seed
                and min(interval.end_step_exclusive, s.end_step_exclusive) > max(interval.start_step, s.start_step)
            ]
            faults.append(
                {
                    **asdict(interval),
                    "arm": arm,
                    "model": model,
                    "detected": bool(overlaps),
                    "overlaps": overlaps,
                    "overlap_steps": sum(o["overlap_steps"] for o in overlaps),
                    "applied_rows": applied[interval.window_id],
                    "grid_applied_rows": grid_applied[interval.window_id],
                    "off_grid_only": applied[interval.window_id] > 0 and grid_applied[interval.window_id] == 0,
                }
            )
    return {"faults": faults, "alerts": alerts, "coverage": coverage_inventory(intervals)}
