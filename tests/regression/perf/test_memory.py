import logging

import pytest

from libs.perf import memory


class _FakeMlflow:
    def __init__(self) -> None:
        self.metrics: list[tuple[str, float, int | None]] = []
        self.artifacts: list[tuple[dict, str]] = []


class _FakeTuple2:
    def __init__(self, left, right) -> None:
        self._left = left
        self._right = right

    def _1(self):
        return self._left

    def _2(self):
        return self._right


class _FakeIterator:
    def __init__(self, items) -> None:
        self._items = list(items)
        self._index = 0

    def hasNext(self):
        return self._index < len(self._items)

    def next(self):
        item = self._items[self._index]
        self._index += 1
        return item


class _FakeScalaMap:
    def __init__(self, items) -> None:
        self._items = items

    def iterator(self):
        return _FakeIterator(self._items)


class _FakeBlockManagerId:
    def __init__(self, executor_id: str, host: str, port: int) -> None:
        self._executor_id = executor_id
        self._host = host
        self._port = port

    def executorId(self):
        return self._executor_id

    def host(self):
        return self._host

    def port(self):
        return self._port


class _FakeConf:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _FakeSc:
    def __init__(self, items) -> None:
        self._items = items

    def getExecutorMemoryStatus(self):
        return _FakeScalaMap(self._items)


class _FakeJsc:
    def __init__(self, items) -> None:
        self._items = items

    def sc(self):
        return _FakeSc(self._items)


class _FakeSparkContext:
    def __init__(self, items) -> None:
        self._items = items
        self._jsc = _FakeJsc(items)

    def getConf(self):
        return _FakeConf(
            {
                "spark.master": "local[2]",
                "spark.executor.memory": "4g",
                "spark.driver.memory": "2g",
            }
        )


class _FakeSpark:
    def __init__(self, items) -> None:
        self.sparkContext = _FakeSparkContext(items)


def test_log_memory_usage_emits_metrics_and_artifacts(monkeypatch, caplog) -> None:
    fake_mlflow = _FakeMlflow()
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_ENABLED", "true")
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_MODE", "light")
    monkeypatch.setattr(memory, "_capture_process_memory", lambda: {"rss_bytes": 10, "vms_bytes": 20, "peak_rss_bytes": 30})
    monkeypatch.setattr(memory, "log_metric_if_active", lambda name, value, step=None: fake_mlflow.metrics.append((name, float(value), step)))
    monkeypatch.setattr(memory, "log_dict_artifact_if_active", lambda payload, artifact_file: fake_mlflow.artifacts.append((payload, artifact_file)))

    @memory.log_memory_usage(logger=logging.getLogger("test"), label="unit_memory")
    def sample():
        return "ok"

    with caplog.at_level(logging.INFO):
        assert sample() == "ok"

    assert "memory_snapshot label=unit_memory event=start" in caplog.text
    assert "memory_snapshot label=unit_memory event=success" in caplog.text
    assert "memory_snapshot label=unit_memory event=end" in caplog.text
    assert [artifact_file for _, artifact_file in fake_mlflow.artifacts] == [
        "reports/memory/unit_memory_start.json",
        "reports/memory/unit_memory_success.json",
        "reports/memory/unit_memory_end.json",
    ]
    metric_names = {name for name, _, _ in fake_mlflow.metrics}
    assert "unit_memory_start_rss_bytes" in metric_names
    assert "unit_memory_success_peak_rss_bytes" in metric_names
    assert "unit_memory_end_vms_bytes" in metric_names


def test_log_memory_usage_preserves_exceptions(monkeypatch) -> None:
    fake_mlflow = _FakeMlflow()
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_ENABLED", "true")
    monkeypatch.setattr(memory, "_capture_process_memory", lambda: {"rss_bytes": 1, "vms_bytes": 2, "peak_rss_bytes": 3})
    monkeypatch.setattr(memory, "log_metric_if_active", lambda name, value, step=None: fake_mlflow.metrics.append((name, float(value), step)))
    monkeypatch.setattr(memory, "log_dict_artifact_if_active", lambda payload, artifact_file: fake_mlflow.artifacts.append((payload, artifact_file)))

    @memory.log_memory_usage(logger=logging.getLogger("test"), label="failing_memory")
    def sample():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        sample()

    artifact_files = [artifact_file for _, artifact_file in fake_mlflow.artifacts]
    assert artifact_files == [
        "reports/memory/failing_memory_start.json",
        "reports/memory/failing_memory_failure.json",
        "reports/memory/failing_memory_end.json",
    ]


def test_capture_memory_snapshot_collects_detailed_spark_summary(monkeypatch) -> None:
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_MODE", "detailed")
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_SPARK_ENABLED", "true")
    monkeypatch.setattr(memory, "_capture_process_memory", lambda: {"rss_bytes": 100, "vms_bytes": 200, "peak_rss_bytes": 300})
    spark = _FakeSpark(
        [
            _FakeTuple2(_FakeBlockManagerId("driver", "driver-host", 7077), _FakeTuple2(1000, 600)),
            _FakeTuple2(_FakeBlockManagerId("1", "exec-host", 7078), _FakeTuple2(2000, 500)),
        ]
    )

    snapshot = memory.capture_memory_snapshot(
        label="spark_stage",
        event="end",
        started_at=0.0,
        status="success",
        spark=spark,
        include_spark=True,
    )

    assert snapshot["spark"]["executor_count"] == 2
    assert snapshot["spark"]["used_memory_bytes"] == 1900
    assert snapshot["spark"]["spark_conf"]["spark.executor.memory"] == "4g"
    assert snapshot["warnings"] == []


def test_capture_memory_snapshot_handles_missing_spark(monkeypatch) -> None:
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_MODE", "detailed")
    monkeypatch.setenv("S3NTINEL_OBSERVABILITY_MEMORY_SPARK_ENABLED", "true")
    monkeypatch.setattr(memory, "_capture_process_memory", lambda: {"rss_bytes": 100, "vms_bytes": 200, "peak_rss_bytes": 300})

    snapshot = memory.capture_memory_snapshot(
        label="spark_stage",
        event="end",
        status="success",
        spark=None,
        include_spark=True,
    )

    assert snapshot["spark"] is None
    assert snapshot["warnings"] == []
