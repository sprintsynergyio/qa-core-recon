from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError

from qa_core_recon.spark import (
    ICEBERG_SPARK_PACKAGE,
    MAVEN_CENTRAL,
    IcebergRuntimeError,
    SessionStartError,
    catalog_name_for,
    ivy_cache_dir,
    local_session,
    missing_runtime_message,
    runtime_jar_cached,
    session_start_failure,
)


def test_session_is_local_and_fixed(spark):
    spark.range(1).count()
    assert spark.sparkContext.master == "local[1]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.ui.enabled") == "false"
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"
    assert spark.conf.get("spark.jars.packages") == ICEBERG_SPARK_PACKAGE
    assert spark.conf.get("spark.jars.ivy") == str(ivy_cache_dir())
    assert spark.conf.get("spark.jars.repositories") == MAVEN_CENTRAL
    pending = catalog_name_for("/not/a/registered/warehouse")
    with pytest.raises(Py4JJavaError):
        spark.conf.get(f"spark.sql.catalog.{pending}")
    assert spark.conf.get("spark.driver.host") == "127.0.0.1"
    assert spark.conf.get("spark.driver.bindAddress") == "127.0.0.1"
    assert spark.conf.get("spark.sql.caseSensitive") == "true"
    assert spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "false"
    assert spark.conf.get("spark.sql.csv.parser.columnPruning.enabled") == "false"
    assert local_session() is spark


def test_running_session_without_iceberg_cannot_gain_it(spark, monkeypatch):
    import qa_core_recon.spark as spark_mod

    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    with pytest.raises(RuntimeError) as caught:
        local_session(iceberg=True)
    assert str(caught.value) == (
        "the local Spark session is already running without the Iceberg runtime"
    )


def test_missing_iceberg_runtime_is_a_clear_read_error(tmp_path):
    cache = tmp_path / "empty-ivy"
    assert runtime_jar_cached(cache) is False
    other = tmp_path / "wrong-jar"
    other.mkdir()
    (other / "some-other-library.jar").write_bytes(b"jar")
    assert runtime_jar_cached(other) is False

    resolution = RuntimeError(
        "unresolved dependency: org.apache.iceberg#iceberg-spark-runtime-3.5_2.12;1.7.1: not found"
    )
    failure = session_start_failure(resolution, iceberg=True, ivy=cache)
    assert isinstance(failure, IcebergRuntimeError)
    assert str(failure) == missing_runtime_message(cache)
    assert ICEBERG_SPARK_PACKAGE in str(failure)
    assert "Maven Central" in str(failure)
    assert str(cache) in str(failure)
    assert "unresolved dependency" not in str(failure)

    ivy_failure = session_start_failure(
        RuntimeError("Ivy resolution failed for the package"),
        iceberg=True,
        ivy=other,
    )
    assert isinstance(ivy_failure, IcebergRuntimeError)
    assert str(ivy_failure) == missing_runtime_message(other)

    present = tmp_path / "warm-ivy"
    present.mkdir()
    (present / "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.7.1.jar").write_bytes(b"jar")
    assert runtime_jar_cached(present) is True
    cached = session_start_failure(resolution, iceberg=True, ivy=present)
    assert isinstance(cached, SessionStartError)
    assert not isinstance(cached, IcebergRuntimeError)
    assert str(cached).startswith("cannot start the local Spark session")
    assert session_start_failure(resolution, iceberg=False, ivy=cache) is resolution


def test_other_session_start_failures_stay_generic(tmp_path):
    cache = tmp_path / "empty-ivy"
    original = RuntimeError("JAVA_GATEWAY_EXITED")
    failure = session_start_failure(original, iceberg=True, ivy=cache)
    assert isinstance(failure, SessionStartError)
    assert not isinstance(failure, IcebergRuntimeError)
    assert str(failure) == "cannot start the local Spark session: JAVA_GATEWAY_EXITED"
    assert ICEBERG_SPARK_PACKAGE not in str(failure)


def _call_local_session_with_failed_start(monkeypatch, cache: Path, message: str):
    import qa_core_recon.spark as spark_mod

    monkeypatch.setattr(spark_mod, "ivy_cache_dir", lambda: cache)
    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)

    def fail(_builder):
        raise RuntimeError(message)

    monkeypatch.setattr(spark_mod, "_start_quietly", fail)
    return local_session(iceberg=True)


def test_local_session_maps_an_unresolved_runtime(spark, monkeypatch, tmp_path):
    cache = tmp_path / "empty-ivy"
    with pytest.raises(IcebergRuntimeError) as caught:
        _call_local_session_with_failed_start(
            monkeypatch,
            cache,
            "unresolved dependency: org.apache.iceberg#iceberg-spark-runtime: not found",
        )
    assert str(caught.value) == missing_runtime_message(cache)


def test_local_session_keeps_a_generic_start_error(spark, monkeypatch, tmp_path):
    cache = tmp_path / "empty-ivy"
    with pytest.raises(SessionStartError) as caught:
        _call_local_session_with_failed_start(monkeypatch, cache, "JAVA_GATEWAY_EXITED")
    assert not isinstance(caught.value, IcebergRuntimeError)
    assert str(caught.value) == "cannot start the local Spark session: JAVA_GATEWAY_EXITED"


def test_session_leaves_no_hive_files_in_the_repo(spark):
    spark.range(1).count()
    root = Path(__file__).resolve().parents[1]
    for name in ("metastore_db", "derby.log", "spark-warehouse"):
        assert not (root / name).exists()
        assert not (Path.cwd() / name).exists()
    warehouse = spark.conf.get("spark.sql.warehouse.dir")
    assert str(root) not in warehouse
    ivy = Path(spark.conf.get("spark.jars.ivy"))
    assert root not in ivy.parents
    assert Path.cwd() not in ivy.parents
