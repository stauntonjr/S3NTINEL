"""Shared CLI helpers for simulation scripts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from libs.simulation.flight.examples import build_named_flight_spec


DEFAULT_START_TIMESTAMP_UTC = datetime(2025, 1, 1, tzinfo=timezone.utc)


def add_source_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--flight-name", default="power_chain", help="Named flight")
    parser.add_argument("--tail-id", default="T_SIM", help="Synthetic tail identifier")
    parser.add_argument("--flight-id", default="F_SIM", help="Synthetic flight identifier")
    parser.add_argument("--n-steps", type=int, default=None, help="Number of simulation ticks; defaults to the flight preset mission length")
    parser.add_argument("--dt-seconds", type=float, default=None, help="Tick duration in seconds; defaults to the flight preset mission cadence")
    return parser


def add_event_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--delta-threshold", type=float, default=0.0, help="Continuous event delta threshold")
    parser.add_argument("--slope-source", default="ema", choices=("ema", "raw"), help="Continuous slope source")
    parser.add_argument("--ema-alpha", type=float, default=0.2, help="EMA alpha when slope-source=ema")
    parser.add_argument("--slope-abs-threshold", type=float, default=1.0, help="Minimum signed delta magnitude for slope runs")
    parser.add_argument(
        "--slope-min-persistence-samples",
        type=int,
        default=2,
        help="Minimum consecutive above-threshold samples before emitting a slope run",
    )
    parser.add_argument(
        "--slope-reemit-ratio",
        type=float,
        default=1.5,
        help="Minimum peak-delta growth ratio required to re-emit within a continuing slope run",
    )
    return parser


def add_window_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--window-max-ms", type=int, default=10000, help="Maximum window duration in ms")
    parser.add_argument("--window-event-threshold", type=int, default=20, help="Window event threshold")
    parser.add_argument("--window-min-ms", type=int, default=50, help="Minimum window duration in ms")
    parser.add_argument("--window-inactivity-timeout-ms", type=int, default=0, help="Window inactivity timeout in ms")
    parser.add_argument("--window-strategy", default="segmented", choices=("segmented",))
    return parser


def add_backbone_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--backbone-parameter-count", type=int, default=8, help="Backbone parameter count")
    parser.add_argument("--backbone-ridge-lambda", type=float, default=1.0, help="Backbone ridge lambda")
    parser.add_argument(
        "--backbone-event-prior-alpha",
        type=float,
        default=0.35,
        help="Weight applied to the event-derived prior when ranking backbone sensors",
    )
    return parser


def resolve_flight(flight_name: str):
    return build_named_flight_spec(str(flight_name))
