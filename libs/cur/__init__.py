# File: libs/cur/__init__.py
"""CUR decomposition components."""

from libs.cur.window_energy import aggregate_sensor_energy_over_corpus, compute_window_sensor_energy

__all__ = [
    "compute_window_sensor_energy",
    "aggregate_sensor_energy_over_corpus",
]
