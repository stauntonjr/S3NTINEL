from pathlib import Path
import os
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _set_pyspark_python_env() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    _set_pyspark_python_env()
    session = (
        SparkSession.builder.master("local[1]")
        .appName("s3ntinel-spark-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def spark_delta():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _set_pyspark_python_env()
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("s3ntinel-delta-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        session._jvm.java.lang.Class.forName("delta.DefaultSource")
    except Exception:
        session.stop()
        pytest.skip("delta JVM classes unavailable for this Spark runtime")
    yield session
    session.stop()
