"""Descriptive pilot reports: no statistical superiority or localization claims."""

from collections import defaultdict
from dataclasses import asdict

from libs.research.simulator_protocol import PilotProtocol
from libs.research.simulator_truth import CHALLENGES


def _detection(rows):
    return {
        "declared_intervals": len(rows),
        "detected_intervals": sum(r["detected"] for r in rows),
        "detected_rate": sum(r["detected"] for r in rows) / len(rows) if rows else None,
        "applied_support_intervals": sum(r["applied_rows"] > 0 for r in rows),
        "zero_applied_support_intervals": sum(r["applied_rows"] == 0 for r in rows),
        "off_grid_only_intervals": sum(r["off_grid_only"] for r in rows),
    }


def build_report(protocol, evidence):
    scores, thresholds = evidence["scores"], evidence["thresholds"]
    faults = [r for truth in evidence.get("truths", []) for r in truth["faults"]]
    performance, comparisons, per_seed = [], [], []
    means = {}
    for key, threshold in sorted(thresholds.items()):
        arm, model = key.split("/")
        nominal = [s for s in scores if s.role == "nominal_test" and s.arm == arm and s.model == model]
        n = len(nominal)
        if not n:
            raise ValueError("missing held-out nominal denominator")
        observations = sum(s.observations for s in nominal)
        means[key] = sum(s.mean_bits * s.observations for s in nominal) / observations
        false_alerts = sum(s.mean_bits > threshold.value for s in nominal)
        relevant = [r for r in faults if r["arm"] == arm and r["model"] == model]
        by_family = {
            detail: _detection([r for r in relevant if r["detail"] == detail])
            for detail in sorted({r["detail"] for r in relevant})
        }
        performance.append(
            {
                "arm": arm,
                "model": model,
                "nominal_windows": n,
                "false_alerts": false_alerts,
                "false_alert_rate": false_alerts / n,
                "nominal_bits_per_observation": means[key],
                "nominal_observations": observations,
                "calibration": asdict(threshold),
                **_detection(relevant),
                "by_fault_detail": by_family,
            }
        )
        for seed in sorted({s.seed for s in nominal}):
            subset = [s for s in nominal if s.seed == seed]
            count = sum(s.observations for s in subset)
            per_seed.append(
                {
                    "arm": arm,
                    "model": model,
                    "seed": seed,
                    "windows": len(subset),
                    "false_alerts": sum(s.mean_bits > threshold.value for s in subset),
                    "false_alert_rate": sum(s.mean_bits > threshold.value for s in subset) / len(subset),
                    "bits_per_observation": sum(s.mean_bits * s.observations for s in subset) / count,
                    **_detection([r for r in relevant if r["seed"] == seed]),
                }
            )
    for arm in sorted({key.split("/")[0] for key in thresholds}):
        comparisons.append(
            {
                "arm": arm,
                "conditional_minus_temporal": means[arm + "/conditional"] - means[arm + "/temporal"],
                "conditional_minus_marginal": means[arm + "/conditional"] - means[arm + "/marginal"],
            }
        )
    pairs = defaultdict(dict)
    for s in scores:
        if s.role in {"nominal_test", "faulted_test"}:
            key = (s.seed, s.arm, s.model, s.start_step, s.end_step_exclusive)
            if s.role in pairs[key]:
                raise ValueError("duplicate paired-window identity")
            pairs[key][s.role] = s.mean_bits
    contrasts = []
    for key, pair in sorted(pairs.items()):
        if set(pair) != {"nominal_test", "faulted_test"}:
            raise ValueError("unmatched paired-window boundary")
        seed, arm, model, start, end = key
        contrasts.append(
            {
                "seed": seed,
                "arm": arm,
                "model": model,
                "start_step": start,
                "end_step_exclusive": end,
                "faulted_minus_nominal": pair["faulted_test"] - pair["nominal_test"],
            }
        )
    coverage = {}
    for name, accepted in CHALLENGES.items():
        present = sorted({r["detail"] for r in faults}.intersection(accepted))
        coverage[name] = {
            "status": "covered" if present else "not_covered",
            "accepted_detail_labels": list(accepted),
            "present_details": present,
        }
    return {
        "version": 1,
        "qualification": protocol == PilotProtocol(),
        "scientific_claim": "descriptive_only",
        "protocol": asdict(protocol),
        "thresholds": {k: asdict(v) for k, v in thresholds.items()},
        "performance": performance,
        "comparisons": comparisons,
        "per_seed": per_seed,
        "paired_window_contrasts": contrasts,
        "coverage": coverage,
        "faults": faults,
        "sensor_metrics": evidence.get("details", []),
        "missions": evidence.get("missions", []),
        "examples": evidence.get("examples", {}),
        "reproducibility": evidence.get("reproducibility", {}),
        "freeze_verified": evidence.get("freeze_verified", False),
        "limits": [
            "Same mission template; seed variation is not unseen-flight generalization.",
            "Four test seeds and correlated windows: descriptive evidence, no independent-sample guarantee.",
            "Common-rate subsampling loses high-rate observations; frozen bins discard numeric detail.",
            "Authored interval overlap is detection evidence, not applied-effect or root-cause proof.",
            "Uncovered faults, localization recoverability, Spark integration and scale gates remain open.",
        ],
    }


def render_markdown(report):
    lines = [
        "# Simulator conditional-information pilot",
        "",
        "Descriptive research only. Production unchanged; PR #57 remains draft.",
        "",
        f"Approved full protocol: {report['qualification']}. Frozen artifacts verified: {report['freeze_verified']}.",
        "",
        "| Arm | Model | Nominal bits/observation | False alerts/windows | Detected/declared intervals |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for r in report["performance"]:
        lines.append(
            f"| {r['arm']} | {r['model']} | {r['nominal_bits_per_observation']:.6f} | {r['false_alerts']}/{r['nominal_windows']} | {r['detected_intervals']}/{r['declared_intervals']} |"
        )
    lines.extend(["", "## Conditional comparisons", ""])
    for r in report["comparisons"]:
        lines.append(
            f"- {r['arm']}: conditional minus temporal {r['conditional_minus_temporal']:.6f}; conditional minus marginal {r['conditional_minus_marginal']:.6f} bits/observation. Negative is better predictive efficiency."
        )
    lines.extend(["", "## Coverage and limits", ""])
    lines.extend(
        f"- {name}: {value['status']} ({', '.join(value['present_details']) or 'no represented details'})."
        for name, value in report["coverage"].items()
    )
    lines.extend("- " + value for value in report["limits"])
    lines.extend(
        [
            "",
            "Full per-seed/sensor metrics, actual source examples, thresholds and denominators are in report.json.",
            "",
        ]
    )
    return "\n".join(lines)
