import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def parquet_ok(spark, tmp_path):
    """Skip on environments where local Parquet writes are unavailable.

    On Windows without HADOOP_HOME/winutils, Spark cannot write Parquet. CI runs
    on Linux where this fixture is a no-op, so the Parquet-writing tests still
    run there.
    """
    try:
        spark.range(1).write.mode("overwrite").parquet(str(tmp_path / "_probe"))
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"local Parquet writes unavailable: {exc}")
