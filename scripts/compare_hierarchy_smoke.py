"""Compare two hierarchy smoke summary JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two hierarchy smoke summary JSON outputs")
    parser.add_argument(
        "--left-json",
        default="reports/hierarchy_smoke_summary.json",
        help="Left summary JSON (typically stage-10 run)",
    )
    parser.add_argument(
        "--right-json",
        default="reports/hierarchy_smoke_summary_fixture.json",
        help="Right summary JSON (typically fixture/baseline)",
    )
    parser.add_argument(
        "--left-label",
        default="stage10",
        help="Label for left summary",
    )
    parser.add_argument(
        "--right-label",
        default="fixture",
        help="Label for right summary",
    )
    parser.add_argument(
        "--show-paths",
        type=int,
        default=3,
        help="How many sample paths to print from each summary",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Optional path to write machine-readable comparison JSON",
    )
    return parser.parse_args()


def _load_json(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object JSON at {path}")
    return payload


def _value(dct: dict, key: str) -> int:
    value = dct.get(key, 0)
    try:
        return int(value)
    except Exception:
        return 0


def _build_count_delta(left: dict, right: dict, keys: list[str]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for key in keys:
        left_value = _value(left, key)
        right_value = _value(right, key)
        out[key] = {
            "left": left_value,
            "right": right_value,
            "delta": right_value - left_value,
        }
    return out


def main() -> None:
    args = parse_args()

    left = _load_json(args.left_json)
    right = _load_json(args.right_json)

    node_keys = ["global", "system", "subsystem", "module", "sensor"]
    map_keys = ["sensor_rows", "systems", "subsystems", "modules"]

    left_nodes = left.get("node_counts", {}) if isinstance(left.get("node_counts"), dict) else {}
    right_nodes = right.get("node_counts", {}) if isinstance(right.get("node_counts"), dict) else {}
    left_map = left.get("map_counts", {}) if isinstance(left.get("map_counts"), dict) else {}
    right_map = right.get("map_counts", {}) if isinstance(right.get("map_counts"), dict) else {}

    node_delta = _build_count_delta(left_nodes, right_nodes, node_keys)
    map_delta = _build_count_delta(left_map, right_map, map_keys)

    left_null = _value({"x": left.get("sensor_map_null_rows", 0)}, "x")
    right_null = _value({"x": right.get("sensor_map_null_rows", 0)}, "x")

    print(f"Node counts ({args.left_label} vs {args.right_label}):")
    for key in node_keys:
        row = node_delta[key]
        print(f"  {key}: {row['left']} vs {row['right']} (delta={row['delta']})")

    print(f"Map counts ({args.left_label} vs {args.right_label}):")
    for key in map_keys:
        row = map_delta[key]
        print(f"  {key}: {row['left']} vs {row['right']} (delta={row['delta']})")

    print(f"Null rows: {left_null} vs {right_null} (delta={right_null - left_null})")

    show_paths = max(int(args.show_paths), 0)
    if show_paths > 0:
        left_paths = left.get("sample_paths", [])
        right_paths = right.get("sample_paths", [])
        left_paths = left_paths if isinstance(left_paths, list) else []
        right_paths = right_paths if isinstance(right_paths, list) else []

        print(f"{args.left_label} sample paths:")
        if left_paths[:show_paths]:
            for path in left_paths[:show_paths]:
                print(f"  - {path}")
        else:
            print("  - none")

        print(f"{args.right_label} sample paths:")
        if right_paths[:show_paths]:
            for path in right_paths[:show_paths]:
                print(f"  - {path}")
        else:
            print("  - none")

    if args.output_json:
        payload = {
            "left_label": args.left_label,
            "right_label": args.right_label,
            "left_json": args.left_json,
            "right_json": args.right_json,
            "node_counts": node_delta,
            "map_counts": map_delta,
            "null_rows": {
                "left": left_null,
                "right": right_null,
                "delta": right_null - left_null,
            },
            "left_sample_paths": (left.get("sample_paths", []) if isinstance(left.get("sample_paths", []), list) else []),
            "right_sample_paths": (right.get("sample_paths", []) if isinstance(right.get("sample_paths", []), list) else []),
        }
        output = Path(args.output_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"comparison_json: {output}")


if __name__ == "__main__":
    main()
