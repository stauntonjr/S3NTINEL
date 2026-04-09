from __future__ import annotations

import sys

from libs.simulation.run_cli import parse_args


def test_run_cli_defaults_phase_count_to_four(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_sim_pipeline",
            "--flight-name",
            "power_chain",
        ],
    )

    args = parse_args()

    assert args.phase_count == 4
