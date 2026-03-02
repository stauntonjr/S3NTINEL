"""Render CUR contraction profile JSON into a markdown report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render CUR contraction profile markdown report")
    parser.add_argument(
        "--input-json",
        default="reports/cur_contraction_profile_stable.json",
        help="Path to profile JSON produced by scripts.profile_cur_contraction_modes",
    )
    parser.add_argument(
        "--output-md",
        default="reports/cur_contraction_profile_stable.md",
        help="Path to output markdown report",
    )
    return parser.parse_args()


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def _build_summary_table(summary: dict[str, Any], modes: list[str]) -> str:
    lines = [
        "| mode | runs | elapsed mean (s) | elapsed std (s) | u_nnz mean | effective modes |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for mode in modes:
        row = summary.get(mode, {}) if isinstance(summary.get(mode), dict) else {}
        elapsed = row.get("elapsed_seconds", {}) if isinstance(row.get("elapsed_seconds"), dict) else {}
        u_nnz = row.get("u_nnz", {}) if isinstance(row.get("u_nnz"), dict) else {}
        effective = row.get("effective_modes", []) if isinstance(row.get("effective_modes"), list) else []
        lines.append(
            "| "
            + f"{mode} | {int(row.get('run_count', 0) or 0)} | {_fmt(elapsed.get('mean'))} | {_fmt(elapsed.get('std'))} | {_fmt(u_nnz.get('mean'))} | {', '.join(str(x) for x in effective) or 'n/a'} |"
        )
    return "\n".join(lines)


def _build_complexity_table(complexity: dict[str, Any]) -> str:
    keys = ["core_w", "pivot_restricted_a", "full_a", "svd"]
    lines = [
        "| component | complexity note |",
        "|---|---|",
    ]
    for key in keys:
        value = complexity.get(key)
        if value is None:
            continue
        lines.append(f"| {key} | {str(value).replace('|', '\\|')} |")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    modes = payload.get("modes", []) if isinstance(payload.get("modes"), list) else []
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    complexity = payload.get("complexity", {}) if isinstance(payload.get("complexity"), dict) else {}

    lines: list[str] = []
    lines.append("# CUR Contraction Profile Report")
    lines.append("")
    lines.append(f"- Input: `{payload.get('raw_table_path', 'n/a')}`")
    lines.append(f"- Format: `{payload.get('table_format', 'n/a')}`")
    lines.append(f"- Modes: `{', '.join(modes) if modes else 'n/a'}`")
    lines.append(f"- Repeats: `{payload.get('repeats', 'n/a')}`")
    lines.append("")
    lines.append("## Runtime Summary")
    lines.append("")
    lines.append(_build_summary_table(summary=summary, modes=modes))
    lines.append("")
    lines.append("## Complexity Notes")
    lines.append("")
    lines.append(_build_complexity_table(complexity=complexity))
    lines.append("")

    output_path = Path(args.output_md)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"markdown_report: {output_path}")


if __name__ == "__main__":
    main()
