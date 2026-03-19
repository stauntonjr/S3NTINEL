"""Live tail runtime objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from libs.behavior import BehaviorRegistry
from libs.simulation.aircraft.runtime import Aircraft
from libs.simulation.aircraft.spec import AircraftSpec

if TYPE_CHECKING:
    from libs.simulation.flight.runtime import Flight
    from libs.simulation.flight.spec import FlightSpec


@dataclass(slots=True)
class Tail:
    id: str
    aircraft: Aircraft
    metadata: dict[str, Any] = field(default_factory=dict)
    _flight_history: list["Flight"] = field(default_factory=list, repr=False)
    _active_flight: "Flight | None" = field(default=None, repr=False)

    @classmethod
    def from_spec(
        cls,
        aircraft_spec: AircraftSpec,
        *,
        tail_id: str,
        behavior_registry: BehaviorRegistry | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "Tail":
        return cls(
            id=str(tail_id),
            aircraft=Aircraft.from_spec(aircraft_spec, behavior_registry=behavior_registry),
            metadata=dict(metadata or {}),
        )

    @property
    def active_flight(self) -> "Flight | None":
        return self._active_flight

    @property
    def flight_history(self) -> tuple["Flight", ...]:
        return tuple(self._flight_history)

    def start_flight(
        self,
        spec: "FlightSpec",
        *,
        flight_id: str = "",
        start_timestamp_utc: datetime | None = None,
    ) -> "Flight":
        from libs.simulation.flight.runtime import Flight

        flight = Flight.from_tail(
            self,
            spec,
            flight_id=flight_id,
            start_timestamp_utc=start_timestamp_utc,
        )
        self._active_flight = flight
        return flight

    def record_flight(self, flight: "Flight") -> None:
        if flight.tail is not self:
            raise ValueError("Cannot record a flight that belongs to a different tail.")
        self._flight_history.append(flight)
        if self._active_flight is flight:
            self._active_flight = None
