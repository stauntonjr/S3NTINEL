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
    parser.add_argument("--sim-seed", type=int, default=None, help="Optional simulation seed for stochastic named presets")
    parser.add_argument("--n-steps", type=int, default=None, help="Number of simulation ticks; defaults to the flight preset mission length")
    parser.add_argument("--dt-seconds", type=float, default=None, help="Tick duration in seconds; defaults to the flight preset mission cadence")
    return parser


def add_profile_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--profile-numeric-ratio-threshold",
        type=float,
        default=0.8,
        help="Minimum numeric-observation ratio required to classify a parameter as numeric-like during profiling",
    )
    parser.add_argument(
        "--profile-categorical-cardinality-max",
        type=int,
        default=200,
        help="Maximum distinct-value count still treated as categorical during profiling",
    )
    parser.add_argument(
        "--profile-behavior-significant-diff-threshold",
        type=float,
        default=0.05,
        help="Minimum scaled first-difference magnitude treated as behaviorally significant during profiling",
    )
    parser.add_argument(
        "--profile-behavior-center-band-width",
        type=float,
        default=1.0,
        help="Scaled center-band width used to separate in-band occupancy from excursions during behavior profiling",
    )
    parser.add_argument(
        "--profile-behavior-soft-bound-width",
        type=float,
        default=2.5,
        help="Scaled soft-bound width used for bounded-occupancy scoring during behavior profiling",
    )
    parser.add_argument(
        "--profile-behavior-hard-bound-width",
        type=float,
        default=2.0,
        help="Scaled hard-bound width used for saturation scoring during behavior profiling",
    )
    parser.add_argument(
        "--profile-behavior-mixed-unknown-low-score-threshold",
        type=float,
        default=0.38,
        help="Top-family score threshold below which behavior profiling falls back to mixed_unknown",
    )
    parser.add_argument(
        "--profile-behavior-mixed-unknown-ambiguous-score-threshold",
        type=float,
        default=0.55,
        help="Top-family score threshold used for ambiguous mixed_unknown gating during behavior profiling",
    )
    parser.add_argument(
        "--profile-behavior-mixed-unknown-ambiguous-margin-threshold",
        type=float,
        default=0.03,
        help="Top-two-family score margin threshold used for ambiguous mixed_unknown gating during behavior profiling",
    )
    return parser


def add_event_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--delta-threshold", type=float, default=0.0, help="Continuous event delta threshold")
    parser.add_argument("--slope-source", default="ema", choices=("ema", "raw"), help="Continuous slope source")
    parser.add_argument("--ema-alpha", type=float, default=0.35, help="EMA alpha when slope-source=ema")
    parser.add_argument(
        "--slope-threshold-mode",
        default="fixed",
        choices=("fixed", "adaptive_run"),
        help="Continuous slope threshold mode",
    )
    parser.add_argument(
        "--slope-threshold-quantile",
        type=float,
        default=0.75,
        help="Per-parameter absolute-delta quantile used when slope-threshold-mode=adaptive_run",
    )
    parser.add_argument(
        "--slope-threshold-scale",
        type=float,
        default=0.35,
        help="Scale applied to the adaptive per-parameter slope threshold",
    )
    parser.add_argument(
        "--slope-threshold-min",
        type=float,
        default=1e-6,
        help="Minimum adaptive slope threshold floor",
    )
    parser.add_argument("--slope-abs-threshold", type=float, default=2.0, help="Minimum signed delta magnitude for slope runs")
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
    parser.add_argument(
        "--event-warmup-points",
        type=int,
        default=4,
        help="Warmup samples required before continuous event detection begins",
    )
    parser.add_argument(
        "--event-low-scale-responsiveness",
        type=float,
        default=1.0,
        help="Generic gain for how eagerly the profiler tunes low-scale responsive channels",
    )
    parser.add_argument(
        "--event-repeatability-aggressiveness",
        type=float,
        default=1.0,
        help="Generic gain for how eagerly the profiler tunes repeatable low-scale channels",
    )
    parser.add_argument(
        "--event-drift-conservatism",
        type=float,
        default=1.0,
        help="Generic gain for how conservative the profiler is on drift-dominated channels",
    )
    parser.add_argument(
        "--event-chatter-suppression",
        type=float,
        default=1.0,
        help="Generic gain for how strongly the profiler suppresses chatter-prone channels",
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


def resolve_flight(flight_name: str, sim_seed: int | None = None):
    return build_named_flight_spec(str(flight_name), seed=sim_seed)
