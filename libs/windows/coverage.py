"""Window coverage-sampling object."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd


def _norm(value: float, min_value: float, max_value: float) -> float:
    if max_value <= min_value:
        return 0.5
    out = (float(value) - float(min_value)) / (float(max_value) - float(min_value))
    return min(max(out, 0.0), 1.0)


def _bin_index(value_01: float, bins_per_axis: int) -> int:
    bins = max(int(bins_per_axis), 1)
    return min(int(math.floor(value_01 * bins)), bins - 1)


@dataclass(frozen=True)
class WindowCoverageSampler:
    sample_size_per_flight: int = 32
    bins_per_axis: int = 4

    def sample(self, windows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        sample_target = max(int(self.sample_size_per_flight), 0)
        if sample_target <= 0:
            return []

        by_flight: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for item in windows:
            tail_id = str(item.get("tail_id", ""))
            flight_id = str(item.get("flight_id", ""))
            if not tail_id or not flight_id:
                continue
            by_flight[(tail_id, flight_id)].append(dict(item))

        selected: list[dict[str, Any]] = []
        for flight_key in sorted(by_flight.keys(), key=lambda item: (item[0], item[1])):
            items = by_flight[flight_key]
            if not items:
                continue

            enriched: list[dict[str, Any]] = []
            for item in items:
                drift = float(item.get("drift_magnitude_profiled", 0.0) or 0.0)
                duration_ms = float(item.get("duration_ms", 0.0) or 0.0)
                t_end = pd.to_datetime(item.get("t_end"), utc=True)
                if pd.isna(t_end):
                    continue
                enriched.append(
                    {
                        "row": item,
                        "drift": drift,
                        "duration_ms": duration_ms,
                        "t_end_s": float(t_end.timestamp()),
                        "win_id": int(item.get("win_id", 0)),
                    }
                )

            if not enriched:
                continue

            drift_min = min(item["drift"] for item in enriched)
            drift_max = max(item["drift"] for item in enriched)
            dur_min = min(item["duration_ms"] for item in enriched)
            dur_max = max(item["duration_ms"] for item in enriched)
            t_min = min(item["t_end_s"] for item in enriched)
            t_max = max(item["t_end_s"] for item in enriched)

            strata: dict[tuple[int, int, int], list[dict[str, Any]]] = defaultdict(list)
            for item in enriched:
                x = _norm(item["drift"], drift_min, drift_max)
                y = _norm(item["duration_ms"], dur_min, dur_max)
                z = _norm(item["t_end_s"], t_min, t_max)
                item["coords"] = (x, y, z)
                bucket = (_bin_index(x, self.bins_per_axis), _bin_index(y, self.bins_per_axis), _bin_index(z, self.bins_per_axis))
                strata[bucket].append(item)

            for bucket in strata:
                strata[bucket] = sorted(strata[bucket], key=lambda row: (row["win_id"], row["t_end_s"]))

            target = min(sample_target, len(enriched))
            local_selected: list[dict[str, Any]] = []
            selected_keys: set[tuple[int, float]] = set()
            buckets = sorted(strata.keys(), key=lambda item: (item[0], item[1], item[2]))

            exhausted = False
            while len(local_selected) < target and not exhausted:
                exhausted = True
                for bucket in buckets:
                    rows = strata[bucket]
                    if not rows:
                        continue
                    exhausted = False
                    pick = rows.pop(0)
                    key = (pick["win_id"], pick["t_end_s"])
                    if key in selected_keys:
                        continue
                    selected_keys.add(key)
                    local_selected.append(pick)
                    if len(local_selected) >= target:
                        break

            if len(local_selected) < target:
                remaining = [item for item in enriched if (item["win_id"], item["t_end_s"]) not in selected_keys]
                while remaining and len(local_selected) < target:
                    if not local_selected:
                        remaining = sorted(remaining, key=lambda row: (row["win_id"], row["t_end_s"]))
                        pick = remaining.pop(0)
                        local_selected.append(pick)
                        selected_keys.add((pick["win_id"], pick["t_end_s"]))
                        continue

                    chosen_coords = [item["coords"] for item in local_selected]
                    best_index = -1
                    best_score = -1.0
                    best_tie = None
                    for idx, item in enumerate(remaining):
                        cx, cy, cz = item["coords"]
                        min_dist_sq = min(
                            ((cx - sx) ** 2 + (cy - sy) ** 2 + (cz - sz) ** 2)
                            for sx, sy, sz in chosen_coords
                        )
                        tie = (item["win_id"], item["t_end_s"])
                        if min_dist_sq > best_score or (
                            abs(min_dist_sq - best_score) <= 1e-12 and (best_tie is None or tie < best_tie)
                        ):
                            best_score = min_dist_sq
                            best_index = idx
                            best_tie = tie

                    pick = remaining.pop(best_index)
                    local_selected.append(pick)
                    selected_keys.add((pick["win_id"], pick["t_end_s"]))

            selected.extend(item["row"] for item in local_selected[:target])

        return sorted(
            selected,
            key=lambda item: (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0))),
        )
