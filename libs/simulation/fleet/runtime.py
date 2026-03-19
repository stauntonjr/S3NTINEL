"""Live fleet runtime objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from libs.simulation.tail.runtime import Tail

if TYPE_CHECKING:
    from libs.simulation.flight.runtime import Flight
    from libs.simulation.flight.spec import FlightSpec


@dataclass(slots=True)
class Fleet:
    id: str
    tails: tuple[Tail, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    _tails_by_id: dict[str, Tail] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        tails_by_id = {tail.id: tail for tail in self.tails}
        if len(tails_by_id) != len(self.tails):
            raise ValueError("Fleet tail ids must be unique.")
        self._tails_by_id = tails_by_id

    @classmethod
    def from_tails(
        cls,
        tails: tuple[Tail, ...] | list[Tail],
        *,
        fleet_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> "Fleet":
        return cls(
            id=str(fleet_id),
            tails=tuple(tails),
            metadata=dict(metadata or {}),
        )

    @property
    def tail_ids(self) -> tuple[str, ...]:
        return tuple(self._tails_by_id)

    def tail(self, tail_id: str) -> Tail:
        return self._tails_by_id[str(tail_id)]

    def start_flight(
        self,
        *,
        tail_id: str,
        spec: "FlightSpec",
        flight_id: str = "",
        start_timestamp_utc: datetime | None = None,
    ) -> "Flight":
        return self.tail(tail_id).start_flight(
            spec,
            flight_id=flight_id,
            start_timestamp_utc=start_timestamp_utc,
        )
