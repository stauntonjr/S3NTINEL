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


class _FakeSchema:
    def simpleString(self):
        return "struct<a:int,b:string>"


class _FakeRDD:
    def getNumPartitions(self):
        return 3


class _FakeDataFrame:
    columns = ["a", "b"]
    schema = _FakeSchema()
    rdd = _FakeRDD()

    def count(self):
        return 12


def test_log_dataframe_dataset_logs_safe_metadata_record() -> None:
    fake_mlflow = _FakeMlflow()

    original_get_mlflow = mlflow_helpers._get_mlflow
    try:
        mlflow_helpers._get_mlflow = lambda: fake_mlflow
        mlflow_helpers.log_dataframe_dataset_if_active(
            name="stage10_cur_graph",
            dataframe=_FakeDataFrame(),
            context="stage10_output",
            logger=logging.getLogger("test"),
        )
    finally:
        mlflow_helpers._get_mlflow = original_get_mlflow

    assert len(fake_mlflow.data.from_pandas_calls) == 1
    call = fake_mlflow.data.from_pandas_calls[0]
    assert call["kwargs"]["name"] == "stage10_cur_graph"
    assert "source" not in call["kwargs"]

    payload_df = call["df"]
    assert int(payload_df.iloc[0]["row_count"]) == 12
    assert int(payload_df.iloc[0]["column_count"]) == 2
    assert payload_df.iloc[0]["schema"] == "struct<a:int,b:string>"
    assert int(payload_df.iloc[0]["partition_count"]) == 3

    assert len(fake_mlflow.logged_inputs) == 1
    _, context = fake_mlflow.logged_inputs[0]
    assert context == "stage10_output"
