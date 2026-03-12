"""Window context resolution for feature extraction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.io.contracts import AdaptiveWindowRow, DetectedEventRow


@dataclass(frozen=True)
class WindowContext:
    row: AdaptiveWindowRow


@dataclass(frozen=True)
class FlightWindowContextResolver:
    raw_rows: pd.DataFrame
    event_rows: pd.DataFrame

    @staticmethod
    def prepare_raw_telemetry(raw_df: pd.DataFrame) -> pd.DataFrame:
        rows = raw_df.copy()
        if "timestamp_utc" not in rows.columns and "timestamp" in rows.columns:
            rows["timestamp_utc"] = rows["timestamp"]
        if "parameter_name" not in rows.columns and "sensor" in rows.columns:
            rows["parameter_name"] = rows["sensor"]
        if "parameter_value" not in rows.columns and "parameter_value_clean" in rows.columns:
            rows["parameter_value"] = rows["parameter_value_clean"]
        rows["tail_id"] = rows.get("tail_id", "").astype(str)
        rows["flight_id"] = rows.get("flight_id", "").astype(str)
        rows["parameter_name"] = rows.get("parameter_name", "").astype(str)
        rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
        rows = rows.dropna(subset=["tail_id", "flight_id", "parameter_name", "timestamp_utc"])
        return rows.sort_values(["tail_id", "flight_id", "timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)

    @staticmethod
    def prepare_events(events_df: pd.DataFrame) -> pd.DataFrame:
        rows = events_df.copy()
        rows["tail_id"] = rows.get("tail_id", "").astype(str)
        rows["flight_id"] = rows.get("flight_id", "").astype(str)
        if "parameter_name" not in rows.columns and "sensor" in rows.columns:
            rows["parameter_name"] = rows["sensor"]
        if "timestamp_utc" not in rows.columns and "ts" in rows.columns:
            rows["timestamp_utc"] = rows["ts"]
        rows["parameter_name"] = rows.get("parameter_name", "").astype(str)
        rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
        if "event_type_detected" not in rows.columns:
            rows["event_type_detected"] = ""
        rows["event_type_detected"] = rows["event_type_detected"].astype(str)
        rows = rows.dropna(subset=["tail_id", "flight_id", "parameter_name", "timestamp_utc"])
        return rows.sort_values(
            ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "event_type_detected"],
            kind="mergesort",
        ).reset_index(drop=True)

    @staticmethod
    def prepare_windows(windows_df: pd.DataFrame) -> pd.DataFrame:
        rows = windows_df.copy()
        rows["tail_id"] = rows.get("tail_id", "").astype(str)
        rows["flight_id"] = rows.get("flight_id", "").astype(str)
        rows["win_id"] = pd.to_numeric(rows.get("win_id"), errors="coerce").fillna(0).astype(int)
        rows["t_start"] = pd.to_datetime(rows.get("t_start"), utc=True, errors="coerce")
        rows["t_end"] = pd.to_datetime(rows.get("t_end"), utc=True, errors="coerce")
        rows["duration_ms"] = pd.to_numeric(rows.get("duration_ms"), errors="coerce").fillna(0).astype(int)
        rows["event_count"] = pd.to_numeric(rows.get("event_count"), errors="coerce").fillna(0).astype(int)
        rows = rows.dropna(subset=["tail_id", "flight_id", "t_start", "t_end"])
        if "date_utc" not in rows.columns:
            rows["date_utc"] = rows["t_start"].dt.date
        return rows.sort_values(["tail_id", "flight_id", "t_start", "win_id"], kind="mergesort").reset_index(drop=True)

    def resolve(self, windows_df: pd.DataFrame) -> list[WindowContext]:
        raw_idx = 0
        event_idx = 0
        zoh_snapshot: dict[str, Any] = {}
        raw_len = len(self.raw_rows)
        event_len = len(self.event_rows)
        out: list[WindowContext] = []

        for window in windows_df.sort_values(["t_end", "win_id"], kind="mergesort").to_dict(orient="records"):
            t_end = pd.to_datetime(window["t_end"], utc=True)
            t_start = pd.to_datetime(window["t_start"], utc=True)

            while raw_idx < raw_len:
                row = self.raw_rows.iloc[raw_idx]
                timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                if timestamp_utc > t_end:
                    break
                zoh_snapshot[str(row["parameter_name"])] = row.get("parameter_value")
                raw_idx += 1

            window_events: list[DetectedEventRow] = []
            event_type_counts: dict[str, int] = {}
            while event_idx < event_len:
                row = self.event_rows.iloc[event_idx]
                timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                if timestamp_utc > t_end:
                    break
                if timestamp_utc >= t_start:
                    payload = row.get("payload")
                    event = {
                        "tail_id": str(window.get("tail_id", "")),
                        "flight_id": str(window.get("flight_id", "")),
                        "parameter_name": str(row.get("parameter_name", "")),
                        "timestamp_utc": timestamp_utc.to_pydatetime(),
                        "event_type_detected": str(row.get("event_type_detected", "")),
                        "payload": payload if isinstance(payload, dict) else {},
                    }
                    window_events.append(event)
                    event_type = event["event_type_detected"]
                    event_type_counts[event_type] = int(event_type_counts.get(event_type, 0)) + 1
                event_idx += 1

            enriched = dict(window)
            enriched["zoh_snapshot"] = dict(zoh_snapshot)
            enriched["event_type_counts"] = dict(event_type_counts)
            enriched["window_events"] = window_events
            out.append(WindowContext(row=enriched))
        return out


@dataclass(frozen=True)
class WindowContextResolver:
    raw_by_flight: dict[tuple[str, str], pd.DataFrame]
    events_by_flight: dict[tuple[str, str], pd.DataFrame]

    prepare_raw_telemetry = staticmethod(FlightWindowContextResolver.prepare_raw_telemetry)
    prepare_events = staticmethod(FlightWindowContextResolver.prepare_events)
    prepare_windows = staticmethod(FlightWindowContextResolver.prepare_windows)

    @classmethod
    def from_frames(
        cls,
        *,
        raw_df: pd.DataFrame,
        events_df: pd.DataFrame,
    ) -> "WindowContextResolver":
        raw_rows = FlightWindowContextResolver.prepare_raw_telemetry(raw_df)
        event_rows = FlightWindowContextResolver.prepare_events(events_df)
        return cls(
            raw_by_flight={
                key: group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)
                for key, group in raw_rows.groupby(["tail_id", "flight_id"], sort=True)
            },
            events_by_flight={
                key: group.sort_values(
                    ["timestamp_utc", "parameter_name", "event_type_detected"],
                    kind="mergesort",
                ).reset_index(drop=True)
                for key, group in event_rows.groupby(["tail_id", "flight_id"], sort=True)
            },
        )

    def resolve(self, windows_df: pd.DataFrame) -> list[WindowContext]:
        window_rows = FlightWindowContextResolver.prepare_windows(windows_df)
        out: list[WindowContext] = []
        for (tail_id, flight_id), group in window_rows.groupby(["tail_id", "flight_id"], sort=True):
            resolver = FlightWindowContextResolver(
                raw_rows=self.raw_by_flight.get((tail_id, flight_id), pd.DataFrame()),
                event_rows=self.events_by_flight.get((tail_id, flight_id), pd.DataFrame()),
            )
            out.extend(resolver.resolve(group))
        return out
