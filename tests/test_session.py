import hashlib
import os
import shutil
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError

from qa_core_recon.spark import (
    AWS_SDK_BUNDLE_JAR,
    AWS_SDK_BUNDLE_PACKAGE,
    AWS_SDK_BUNDLE_VERSION,
    HADOOP_AWS_JAR,
    HADOOP_AWS_PACKAGE,
    HADOOP_VERSION,
    ICEBERG_RUNTIME_JAR,
    ICEBERG_SPARK_PACKAGE,
    ICEBERG_VERSION,
    IcebergRuntimeError,
    S3RuntimeError,
    SessionStartError,
    catalog_name_for,
    ivy_cache_dir,
    jars_packages,
    local_session,
    missing_runtime_message,
    missing_s3_runtime_message,
    required_jars,
    resolve_verified_jars,
    runtime_jar_cached,
    s3_jars_cached,
    session_start_failure,
)


def test_session_is_local_and_fixed(spark):
    spark.range(1).count()
    assert spark.sparkContext.master == "local[1]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.ui.enabled") == "false"
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"
    jars = spark.conf.get("spark.jars")
    for pin in required_jars(iceberg=True, s3=True):
        assert pin.filename in jars
        assert pin.sha256
    assert spark.conf.get("spark.jars.packages", "") == ""
    assert spark.conf.get("spark.jars.ivy") == str(ivy_cache_dir())
    pending = catalog_name_for("/not/a/registered/warehouse")
    with pytest.raises(Py4JJavaError):
        spark.conf.get(f"spark.sql.catalog.{pending}")
    assert spark.conf.get("spark.driver.host") == "127.0.0.1"
    assert spark.conf.get("spark.driver.bindAddress") == "127.0.0.1"
    assert spark.conf.get("spark.sql.caseSensitive") == "true"
    assert spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "false"
    assert spark.conf.get("spark.sql.csv.parser.columnPruning.enabled") == "false"
    assert local_session() is spark


def test_running_session_without_s3_cannot_gain_it(spark, monkeypatch):
    import qa_core_recon.spark as spark_mod

    monkeypatch.setattr(spark_mod, "_s3_ready", False)
    with pytest.raises(RuntimeError) as caught:
        local_session(s3=True)
    assert str(caught.value) == (
        "the local Spark session is already running without the S3 runtime"
    )


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
    plain = session_start_failure(resolution, iceberg=False, ivy=cache)
    assert isinstance(plain, SessionStartError)
    assert not isinstance(plain, IcebergRuntimeError)
    assert str(plain).startswith("cannot start the local Spark session")


def test_other_session_start_failures_stay_generic(tmp_path):
    cache = tmp_path / "empty-ivy"
    original = RuntimeError("JAVA_GATEWAY_EXITED")
    failure = session_start_failure(original, iceberg=True, ivy=cache)
    assert isinstance(failure, SessionStartError)
    assert not isinstance(failure, IcebergRuntimeError)
    assert str(failure) == "cannot start the local Spark session: JAVA_GATEWAY_EXITED"
    assert ICEBERG_SPARK_PACKAGE not in str(failure)


def _skip_jar_resolution(monkeypatch) -> None:
    """Keep start-failure tests on the Spark error path.

    An empty cache would otherwise download the pinned jars before Spark
    starts. These tests are about how a failed start is classified.
    """
    import qa_core_recon.spark as spark_mod

    monkeypatch.setattr(spark_mod, "resolve_verified_jars", lambda *_args, **_kwargs: ())


def _call_local_session_with_failed_start(monkeypatch, cache: Path, message: str):
    import qa_core_recon.spark as spark_mod

    _skip_jar_resolution(monkeypatch)
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


def _failed_get_or_create(stderr: bytes):
    def fail(_self):
        os.write(2, stderr)
        raise RuntimeError("[JAVA_GATEWAY_EXITED] Java gateway process exited")

    return fail


def _local_session_with_fake_start(monkeypatch, cache: Path, stderr: bytes):
    from pyspark.sql import SparkSession

    import qa_core_recon.spark as spark_mod

    _skip_jar_resolution(monkeypatch)
    monkeypatch.setattr(spark_mod, "ivy_cache_dir", lambda: cache)
    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    monkeypatch.setattr(SparkSession.Builder, "getOrCreate", _failed_get_or_create(stderr))
    return local_session(iceberg=True)


def test_startup_log_marks_an_unresolved_runtime(spark, monkeypatch, tmp_path):
    cache = tmp_path / "empty-ivy"
    log = (
        b":: UNRESOLVED DEPENDENCIES ::\n"
        b"unresolved dependency: org.apache.iceberg#"
        b"iceberg-spark-runtime-3.5_2.12;1.7.1: not found\n"
    )
    with pytest.raises(IcebergRuntimeError) as caught:
        _local_session_with_fake_start(monkeypatch, cache, log)
    assert str(caught.value) == missing_runtime_message(cache)
    assert "UNRESOLVED" not in str(caught.value)
    assert "JAVA_GATEWAY" not in str(caught.value)


def test_startup_log_keeps_a_broken_java_home_generic(spark, monkeypatch, tmp_path):
    cache = tmp_path / "empty-ivy"
    log = b"Error: JAVA_HOME is set to an invalid directory\n"
    with pytest.raises(SessionStartError) as caught:
        _local_session_with_fake_start(monkeypatch, cache, log)
    assert not isinstance(caught.value, IcebergRuntimeError)
    assert str(caught.value) == (
        "cannot start the local Spark session: [JAVA_GATEWAY_EXITED] Java gateway process exited"
    )
    assert "JAVA_HOME" not in str(caught.value)


def test_session_start_failure_checks_s3_then_iceberg_then_stays_generic(tmp_path):
    cache = tmp_path / "empty"
    unresolved = RuntimeError("unresolved dependency: missing")
    s3_failure = session_start_failure(unresolved, iceberg=True, ivy=cache, s3=True)
    assert isinstance(s3_failure, S3RuntimeError)
    assert not isinstance(s3_failure, IcebergRuntimeError)
    assert "unexpected error" not in str(s3_failure)

    iceberg_failure = session_start_failure(unresolved, iceberg=True, ivy=cache, s3=False)
    assert isinstance(iceberg_failure, IcebergRuntimeError)
    assert str(iceberg_failure) == missing_runtime_message(cache)
    assert "unexpected error" not in str(iceberg_failure)

    warm = tmp_path / "warm-s3"
    warm.mkdir()
    (warm / "hadoop-aws-3.3.4.jar").write_bytes(b"jar")
    (warm / "aws-java-sdk-bundle-1.12.262.jar").write_bytes(b"jar")
    iceberg_after_s3 = session_start_failure(unresolved, iceberg=True, ivy=warm, s3=True)
    assert isinstance(iceberg_after_s3, IcebergRuntimeError)

    other = session_start_failure(
        RuntimeError("JAVA_GATEWAY_EXITED"),
        iceberg=False,
        ivy=cache,
        s3=False,
    )
    assert isinstance(other, SessionStartError)
    assert not isinstance(other, S3RuntimeError)
    assert not isinstance(other, IcebergRuntimeError)
    assert str(other) == "cannot start the local Spark session: JAVA_GATEWAY_EXITED"
    assert "unexpected error" not in str(other)


def test_missing_s3_runtime_is_a_clear_read_error(tmp_path):
    cache = tmp_path / "empty-ivy"
    assert s3_jars_cached(cache) is False
    one = tmp_path / "half"
    one.mkdir()
    (one / "org.apache.hadoop_hadoop-aws-3.3.4.jar").write_bytes(b"jar")
    assert s3_jars_cached(one) is False

    resolution = RuntimeError(
        "unresolved dependency: org.apache.hadoop#hadoop-aws;3.3.4: not found"
    )
    failure = session_start_failure(resolution, iceberg=False, ivy=cache, s3=True)
    assert isinstance(failure, S3RuntimeError)
    assert str(failure) == missing_s3_runtime_message(cache)
    assert HADOOP_AWS_PACKAGE in str(failure)
    assert AWS_SDK_BUNDLE_PACKAGE in str(failure)
    assert "Maven Central" in str(failure)
    assert str(cache) in str(failure)
    assert "unresolved dependency" not in str(failure)

    present = tmp_path / "warm-s3"
    present.mkdir()
    (present / "org.apache.hadoop_hadoop-aws-3.3.4.jar").write_bytes(b"jar")
    (present / "com.amazonaws_aws-java-sdk-bundle-1.12.262.jar").write_bytes(b"jar")
    assert s3_jars_cached(present) is True
    cached = session_start_failure(resolution, iceberg=False, ivy=present, s3=True)
    assert isinstance(cached, SessionStartError)
    assert not isinstance(cached, S3RuntimeError)
    assert str(cached).startswith("cannot start the local Spark session")


def test_local_only_session_does_not_request_s3_packages(spark, monkeypatch):
    import qa_core_recon.spark as spark_mod

    seen: dict[str, str | None] = {}

    def capture(builder):
        seen["packages"] = builder._options.get("spark.jars.packages")
        seen["jars"] = builder._options.get("spark.jars")
        seen["ivy"] = builder._options.get("spark.jars.ivy")
        raise RuntimeError("stopped before the jvm")

    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    monkeypatch.setattr(spark_mod, "_s3_ready", False)
    monkeypatch.setattr(spark_mod, "_start_quietly", capture)
    with pytest.raises(SessionStartError, match="stopped before the jvm"):
        local_session(iceberg=False, s3=False)
    assert seen["packages"] is None
    assert seen["jars"] is None
    assert seen["ivy"] is None

    def capture_s3(builder):
        seen["packages"] = builder._options.get("spark.jars.packages")
        seen["jars"] = builder._options.get("spark.jars")
        raise RuntimeError("stopped before the jvm")

    monkeypatch.setattr(spark_mod, "_start_quietly", capture_s3)
    with pytest.raises(SessionStartError, match="stopped before the jvm"):
        local_session(iceberg=False, s3=True)
    assert seen["packages"] is None
    assert seen["jars"] is not None
    assert HADOOP_AWS_JAR.filename in seen["jars"]
    assert AWS_SDK_BUNDLE_JAR.filename in seen["jars"]
    assert ICEBERG_RUNTIME_JAR.filename not in seen["jars"]
    assert jars_packages(s3=True) == f"{HADOOP_AWS_PACKAGE},{AWS_SDK_BUNDLE_PACKAGE}"


def test_s3_session_sets_s3_builder_settings(spark, monkeypatch):
    import qa_core_recon.spark as spark_mod

    seen: dict[str, str | None] = {}

    def capture(builder):
        for key, value in builder._options.items():
            seen[key] = value
        raise RuntimeError("stopped before the jvm")

    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    monkeypatch.setattr(spark_mod, "_s3_ready", False)
    monkeypatch.setattr(spark_mod, "_start_quietly", capture)
    with pytest.raises(SessionStartError, match="stopped before the jvm"):
        local_session(iceberg=False, s3=True)
    assert "spark.jars.packages" not in seen
    jars = seen["spark.jars"]
    assert jars is not None
    assert HADOOP_AWS_JAR.filename in jars
    assert AWS_SDK_BUNDLE_JAR.filename in jars
    assert ICEBERG_RUNTIME_JAR.filename not in jars
    assert seen["spark.hadoop.fs.s3.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert seen["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert seen["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert seen["spark.hadoop.fs.s3a.endpoint"] == os.environ["QA_CORE_RECON_S3_ENDPOINT"]
    assert seen["spark.hadoop.fs.s3a.retry.limit"] == "1"
    assert seen["spark.hadoop.fs.s3a.retry.interval"] == "200ms"
    provider = seen["spark.hadoop.fs.s3a.aws.credentials.provider"]
    assert provider is not None
    assert "DefaultAWSCredentialsProviderChain" in provider
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    assert all(secret not in (value or "") for value in seen.values())


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


def test_pinned_jars_match_version_constants():
    assert ICEBERG_RUNTIME_JAR.version == ICEBERG_VERSION
    assert ICEBERG_RUNTIME_JAR.coordinate == ICEBERG_SPARK_PACKAGE
    assert ICEBERG_RUNTIME_JAR.filename == f"iceberg-spark-runtime-3.5_2.12-{ICEBERG_VERSION}.jar"
    assert HADOOP_AWS_JAR.version == HADOOP_VERSION
    assert HADOOP_AWS_JAR.coordinate == HADOOP_AWS_PACKAGE
    assert HADOOP_AWS_JAR.filename == f"hadoop-aws-{HADOOP_VERSION}.jar"
    assert AWS_SDK_BUNDLE_JAR.version == AWS_SDK_BUNDLE_VERSION
    assert AWS_SDK_BUNDLE_JAR.coordinate == AWS_SDK_BUNDLE_PACKAGE
    assert AWS_SDK_BUNDLE_JAR.filename == f"aws-java-sdk-bundle-{AWS_SDK_BUNDLE_VERSION}.jar"
    pins = required_jars(iceberg=True, s3=True)
    assert pins == (ICEBERG_RUNTIME_JAR, HADOOP_AWS_JAR, AWS_SDK_BUNDLE_JAR)
    for pin in pins:
        assert pin.sha256 == pin.sha256.lower()
        assert len(pin.sha256) == 64
        int(pin.sha256, 16)


def _refuse_network(*_args, **_kwargs):
    raise AssertionError("this test must not download a jar")


def _copy_verified_jars(cache: Path) -> None:
    jar_dir = cache / "jars"
    jar_dir.mkdir(parents=True)
    source = ivy_cache_dir() / "jars"
    for pin in required_jars(iceberg=True, s3=True):
        shutil.copyfile(source / pin.filename, jar_dir / pin.filename)


def test_correct_cache_passes_offline(spark, tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    cache = tmp_path / "warm"
    _copy_verified_jars(cache)
    monkeypatch.setattr(spark_mod.urllib.request, "urlopen", _refuse_network)
    paths = resolve_verified_jars(cache, iceberg=True, s3=True)
    assert [path.name for path in paths] == [
        pin.filename for pin in required_jars(iceberg=True, s3=True)
    ]
    for path, pin in zip(paths, required_jars(iceberg=True, s3=True), strict=True):
        assert path.is_file()
        assert pin.filename in str(path)


def test_prefixed_ivy_cache_name_passes_offline(spark, tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    pin = ICEBERG_RUNTIME_JAR
    cache = tmp_path / "ivy-layout"
    cache.mkdir()
    source = ivy_cache_dir() / "jars" / pin.filename
    target = cache / f"org.apache.iceberg_{pin.filename}"
    shutil.copyfile(source, target)
    monkeypatch.setattr(spark_mod.urllib.request, "urlopen", _refuse_network)
    assert resolve_verified_jars(cache, iceberg=True, s3=False) == (target,)


def test_corrupted_cached_jar_is_rejected_before_spark_starts(spark, monkeypatch, tmp_path):
    import qa_core_recon.spark as spark_mod

    pin = ICEBERG_RUNTIME_JAR
    cache = tmp_path / "ivy"
    (cache / "jars").mkdir(parents=True)
    payload = b"corrupted jar"
    (cache / "jars" / pin.filename).write_bytes(payload)
    actual = hashlib.sha256(payload).hexdigest()

    def refuse_start(_builder):
        raise AssertionError("Spark started on an unverified jar")

    monkeypatch.setattr(spark_mod.urllib.request, "urlopen", _refuse_network)
    monkeypatch.setattr(spark_mod, "ivy_cache_dir", lambda: cache)
    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    monkeypatch.setattr(spark_mod, "_s3_ready", False)
    monkeypatch.setattr(spark_mod, "_start_quietly", refuse_start)
    with pytest.raises(SessionStartError) as caught:
        local_session(iceberg=True, s3=False)
    message = str(caught.value)
    assert isinstance(caught.value, SessionStartError)
    assert not isinstance(caught.value, IcebergRuntimeError)
    assert pin.filename in message
    assert pin.sha256 in message
    assert actual in message
    assert "failed verification" in message


def test_missing_jar_is_rejected_before_spark_starts(spark, monkeypatch, tmp_path):
    import qa_core_recon.spark as spark_mod

    pin = HADOOP_AWS_JAR
    cache = tmp_path / "empty-ivy"

    def refuse_download(*_args, **_kwargs):
        raise OSError("maven central is unreachable")

    def refuse_start(_builder):
        raise AssertionError("Spark started without a jar")

    monkeypatch.setattr(spark_mod.urllib.request, "urlopen", refuse_download)
    monkeypatch.setattr(spark_mod, "ivy_cache_dir", lambda: cache)
    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    monkeypatch.setattr(spark_mod, "_s3_ready", False)
    monkeypatch.setattr(spark_mod, "_start_quietly", refuse_start)
    with pytest.raises(SessionStartError) as caught:
        local_session(iceberg=False, s3=True)
    message = str(caught.value)
    assert "could not download" in message
    assert pin.filename in message
    assert pin.maven_url() in message
    assert str(cache) in message
    assert "maven central is unreachable" in message
    assert "actual sha256 missing" not in message
    assert "unexpected error" not in message
    assert "Check the network" in message
