"""Shared canonical windows-table builder."""

from __future__ import annotations

from libs.windows.adaptive import build_adaptive_windows, build_adaptive_windows_stream_parity


def build_windows_table(
    events_df: "DataFrame",
    *,
    max_ms: int,
    event_threshold: int,
    min_ms: int,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> "DataFrame":
    resolved_strategy = str(strategy).strip().lower()
    if resolved_strategy == "stream_parity":
        return build_adaptive_windows_stream_parity(
            events_df,
            max_ms=max_ms,
            event_threshold=event_threshold,
            min_ms=min_ms,
            inactivity_timeout_ms=inactivity_timeout_ms,
        )
    return build_adaptive_windows(
        events_df,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
