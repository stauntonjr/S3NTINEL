import logging

from libs.perf import mlflow as mlflow_helpers


class _FakeDataApi:
    def __init__(self) -> None:
        self.from_pandas_calls: list[dict] = []

    def from_pandas(self, df, **kwargs):
        self.from_pandas_calls.append({"df": df, "kwargs": kwargs})
        return {"dataset": "ok"}


class _FakeMlflow:
    def __init__(self) -> None:
        self.data = _FakeDataApi()
        self.logged_inputs: list[tuple[object, str]] = []

    def active_run(self):
        return object()

    def log_input(self, dataset, context: str):
        self.logged_inputs.append((dataset, context))


def test_log_dataset_records_uses_mlflow_compatible_from_pandas_signature() -> None:
    fake_mlflow = _FakeMlflow()

    mlflow_helpers._log_dataset_records_if_active(
        mlflow=fake_mlflow,
        logger=logging.getLogger("test"),
        name="stage_metadata",
        records=[{"run_id": "abc", "stage": "x"}],
        context="stage_run",
    )

    assert len(fake_mlflow.data.from_pandas_calls) == 1
    call = fake_mlflow.data.from_pandas_calls[0]
    assert call["kwargs"].get("name") == "stage_metadata"
    assert "source" not in call["kwargs"]
    assert len(fake_mlflow.logged_inputs) == 1
    dataset, context = fake_mlflow.logged_inputs[0]
    assert dataset == {"dataset": "ok"}
    assert context == "stage_run"