"""Run the fixed approved research pilot; no production pipeline changes."""

import argparse
from pathlib import Path

from libs.research.simulator_pilot import run_pilot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True, help="New unique local evidence directory")
    args = parser.parse_args()
    print(run_pilot(args.output_dir))


if __name__ == "__main__":
    main()
