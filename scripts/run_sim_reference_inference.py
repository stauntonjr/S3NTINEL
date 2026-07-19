"""Run a paired nominal-reference-fit and faulted-inference diagnostic."""

from libs.simulation.reference_inference import run_paired_reference_inference
from libs.simulation.run_cli import parse_args
from libs.simulation.run_context import PipelineRunConfig


def main() -> None:
    config = PipelineRunConfig.from_args(parse_args())
    result = run_paired_reference_inference(config)
    print(result.report_path)


if __name__ == "__main__":
    main()
