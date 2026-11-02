"""Ordered, bounded native-simulator research qualification and evidence lifecycle."""

import json
import os
import resource
import shutil
import subprocess
import sys
from dataclasses import asdict
from datetime import timedelta
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from time import perf_counter

from libs.research.conditional_evaluation import calibrate, fit_arms, model_payload, score_mission
from libs.research.simulator_data import generate_mission, observed_fingerprint
from libs.research.simulator_protocol import PilotProtocol
from libs.research.simulator_reporting import build_report, render_markdown
from libs.research.simulator_truth import authored_intervals, evaluate_truth
from libs.research.symbol_encoding import fit_encoder
from libs.simulation.cli import DEFAULT_START_TIMESTAMP_UTC, resolve_flight


def _json(value):
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))


def _write(path, value):
    with path.open("x") as stream:
        stream.write(_json(value) + "\n")


def _jsonl(path, rows):
    with path.open("x") as stream:
        for row in rows:
            stream.write(_json(row) + "\n")


def _hash(path):
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hardware(output):
    mem = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}:
            mem[key + "_bytes"] = int(value.split()[0]) * 1024

    def git(*args):
        return subprocess.check_output(["git", *args], text=True).strip()

    return {
        "cpus_available": len(os.sched_getaffinity(0)),
        "memory": mem,
        "disk": {str(p): shutil.disk_usage(p)._asdict() for p in (Path("/tmp"), output)},
        "python": sys.version,
        "numpy": version("numpy"),
        "spark_package": version("pyspark"),
        "revision": git("rev-parse", "HEAD"),
        "worktree_status": git("status", "--porcelain"),
        "runtime": "native CPU simulator and bounded Python evaluation; no Spark JVM or GPU",
    }


def _examples(output, scores, thresholds, truths, protocol):
    key = "lexical/conditional"
    threshold = thresholds[key].value
    alerts = [s for s in scores if s.arm == "lexical" and s.model == "conditional" and s.mean_bits > threshold]
    fault_alert = next(
        (
            s
            for s in alerts
            if s.role == "faulted_test"
            and any(
                r["seed"] == s.seed
                and min(r["end_step_exclusive"], s.end_step_exclusive) > max(r["start_step"], s.start_step)
                for truth in truths
                for r in truth["faults"]
            )
        ),
        None,
    )
    false_alert = next((s for s in alerts if s.role == "nominal_test"), None)
    miss = next(
        (
            r
            for truth in truths
            for r in truth["faults"]
            if r["arm"] == "lexical" and r["model"] == "conditional" and not r["detected"]
        ),
        None,
    )
    missed_score = next(
        (
            s
            for s in scores
            if miss
            and s.arm == "lexical"
            and s.model == "conditional"
            and s.role == "faulted_test"
            and s.seed == miss["seed"]
            and min(miss["end_step_exclusive"], s.end_step_exclusive) > max(miss["start_step"], s.start_step)
        ),
        None,
    )
    result = {}
    for name, score in (
        ("fault_alert", fault_alert),
        ("missed_interval", missed_score),
        ("nominal_false_alert", false_alert),
    ):
        if score is None:
            result[name] = {"status": "no_observed_window" if name == "missed_interval" and miss else "not_present"}
            if name == "missed_interval" and miss:
                result[name]["interval"] = miss
            continue
        directory = output / "missions" / f"{score.role}_{score.seed}"
        mission = json.loads((directory / "mission.json").read_text())
        values = dict(zip(mission["steps"], mission["values"]))
        candidates = []
        with (directory / "lexical.contributions.jsonl").open() as stream:
            for line in stream:
                row = json.loads(line)
                if score.start_step <= row["step"] < score.end_step_exclusive:
                    candidates.append(row)
        examples = []
        for row in sorted(candidates, key=lambda r: (-r["conditional"], r["step"], r["sensor"]))[:3]:
            examples.append(
                {
                    **row,
                    "observed_value": values[row["step"]][row["sensor"]],
                    "timestamp_utc": (
                        DEFAULT_START_TIMESTAMP_UTC + timedelta(seconds=row["step"] * protocol.dt_seconds)
                    ).isoformat(),
                }
            )
        result[name] = {"status": "present", "window": asdict(score), "threshold": threshold, "observations": examples}
        if name == "missed_interval":
            result[name]["interval"] = miss
    return result


def run_pilot(output_dir: Path, protocol: PilotProtocol = PilotProtocol()) -> Path:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    started = perf_counter()
    manifest = {"version": 1, "status": "running", "cursor": {}, "missions": []}

    def persist():
        temp = output / "manifest.tmp"
        temp.write_text(_json(manifest) + "\n")
        temp.replace(output / "manifest.json")

    def stage(name, role=None, seed=None):
        manifest["cursor"] = {"stage": name, "role": role, "seed": seed}
        persist()
        with (output / "events.jsonl").open("a") as stream:
            stream.write(_json({**manifest["cursor"], "elapsed_seconds": perf_counter() - started}) + "\n")
        print(f"pilot stage={name} role={role} seed={seed}", flush=True)

    try:
        stage("preflight")
        _write(output / "protocol.json", asdict(protocol))
        _write(output / "hardware.json", _hardware(output))
        fit, missions = [], manifest["missions"]
        first_fingerprint = None

        def generate(role, seed, directory=None):
            stage("generate", role, seed)
            mission = generate_mission(protocol, role, seed, directory or output / "missions" / f"{role}_{seed}")
            if missions and mission.sensors != schema:
                raise ValueError("mission schema differs from frozen fit schema")
            return mission

        for seed in protocol.fit_seeds:
            mission = generate("fit", seed)
            if not missions:
                schema = mission.sensors
                first_fingerprint = observed_fingerprint(mission)
            fit.append(mission)
            missions.append(
                {
                    "role": "fit",
                    "seed": seed,
                    "fingerprint": observed_fingerprint(mission),
                    "diagnostics": mission.diagnostics,
                }
            )
        stage("reproducibility")
        probe = generate_mission(protocol, "fit", protocol.fit_seeds[0], output / "reproducibility_probe")
        same = observed_fingerprint(probe) == first_fingerprint
        different = len({observed_fingerprint(m) for m in fit}) > 1
        if not same or not different:
            raise ValueError("seed reproducibility/variation contract failed")
        reproducibility = {
            "same_seed_equal": same,
            "different_seed_differs": different,
            "probe_fingerprint": observed_fingerprint(probe),
        }
        del probe
        stage("fit")
        encoder = fit_encoder(fit)
        encoded_fit = [encoder.transform(m) for m in fit]
        for m, encoded in zip(fit, encoded_fit):
            directory = output / "missions" / f"fit_{m.seed}"
            _jsonl(
                directory / "symbols.jsonl",
                [{"step": t, "symbols": row} for t, row in zip(encoded.steps, encoded.states)],
            )
            _write(directory / "encoding.json", encoded.diagnostics)
        del fit
        arms = fit_arms(encoded_fit, encoder.alphabets)
        del encoded_fit
        models = {k: model_payload(v) for k, v in arms.items()}
        _write(output / "sensors.json", [asdict(s) for s in schema])
        _write(output / "encoder.json", encoder.to_payload())
        _write(output / "models.json", models)
        scores, details = [], []

        def evaluate(role, seed):
            mission = generate(role, seed)
            directory = output / "missions" / f"{role}_{seed}"
            stage("encode", role, seed)
            encoded = encoder.transform(mission)
            _jsonl(
                directory / "symbols.jsonl",
                [{"step": t, "symbols": row} for t, row in zip(encoded.steps, encoded.states)],
            )
            _write(directory / "encoding.json", encoded.diagnostics)
            stage("score", role, seed)
            for arm, model in arms.items():
                rows, diagnostics = score_mission(model, encoded, arm=arm, protocol=protocol)
                _jsonl(directory / f"{arm}.scores.jsonl", [asdict(s) for s in rows])
                _jsonl(directory / f"{arm}.contributions.jsonl", diagnostics.pop("contributions"))
                _write(directory / f"{arm}.diagnostics.json", diagnostics)
                scores.extend(rows)
                details.append({"role": role, "seed": seed, "arm": arm, **diagnostics})
            missions.append(
                {
                    "role": role,
                    "seed": seed,
                    "fingerprint": observed_fingerprint(mission),
                    "diagnostics": mission.diagnostics,
                }
            )
            persist()
            return mission.steps

        for seed in protocol.calibration_seeds:
            evaluate("calibration", seed)
        stage("calibrate")
        thresholds = {
            f"{arm}/{name}": calibrate(
                [s.mean_bits for s in scores if s.role == "calibration" and s.arm == arm and s.model == name]
            )
            for arm in arms
            for name in ("marginal", "temporal", "conditional")
        }
        _write(output / "thresholds.json", {k: asdict(v) for k, v in thresholds.items()})
        core_files = ("protocol.json", "sensors.json", "encoder.json", "models.json", "thresholds.json")
        frozen = {name: _hash(output / name) for name in core_files}
        _write(output / "freeze.json", frozen)
        frozen_memory = _json(
            {
                "protocol": asdict(protocol),
                "encoder": encoder.to_payload(),
                "models": models,
                "thresholds": {k: asdict(v) for k, v in thresholds.items()},
            }
        )

        def verify_frozen():
            now = _json(
                {
                    "protocol": asdict(protocol),
                    "encoder": encoder.to_payload(),
                    "models": {k: model_payload(v) for k, v in arms.items()},
                    "thresholds": {k: asdict(v) for k, v in thresholds.items()},
                }
            )
            if now != frozen_memory or any(_hash(output / name) != value for name, value in frozen.items()):
                raise ValueError("frozen fit/calibration artifacts changed")

        stage("frozen")
        retained = {}
        for role in ("nominal_test", "faulted_test"):
            for seed in protocol.test_seeds:
                verify_frozen()
                retained[role, seed] = evaluate(role, seed)
                verify_frozen()
        truths = []
        for seed in protocol.test_seeds:
            stage("truth", "faulted_test", seed)
            intervals = authored_intervals(resolve_flight(protocol.flight_name, seed), seed)
            truth = evaluate_truth(
                intervals,
                [s for s in scores if s.role == "faulted_test" and s.seed == seed],
                thresholds,
                output / "missions" / f"faulted_test_{seed}" / "raw.jsonl",
                retained["faulted_test", seed],
            )
            for row in truth["faults"]:
                row["overlap_seconds"] = row["overlap_steps"] * protocol.dt_seconds
            _write(output / f"truth_{seed}.json", truth)
            truths.append(truth)
        stage("report")
        verify_frozen()
        examples = _examples(output, scores, thresholds, truths, protocol)
        report = build_report(
            protocol,
            {
                "scores": scores,
                "thresholds": thresholds,
                "truths": truths,
                "details": details,
                "missions": missions,
                "reproducibility": reproducibility,
                "examples": examples,
                "freeze_verified": True,
            },
        )
        _write(output / "report.json", report)
        (output / "summary.md").write_text(render_markdown(report))
        manifest.update(
            status="complete",
            elapsed_seconds=perf_counter() - started,
            peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            output_bytes_excluding_manifest=sum(
                p.stat().st_size for p in output.rglob("*") if p.is_file() and p.name != "manifest.json"
            ),
            artifact_hashes={
                str(p.relative_to(output)): _hash(p)
                for p in sorted(output.rglob("*"))
                if p.is_file() and p.name != "manifest.json"
            },
        )
        persist()
        print(f"pilot complete report={output / 'report.json'}", flush=True)
        return output / "report.json"
    except BaseException as error:
        manifest.update(
            status="failed", error=f"{type(error).__name__}: {error}", elapsed_seconds=perf_counter() - started
        )
        persist()
        raise
