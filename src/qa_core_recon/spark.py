"""Deterministic local Spark session.

The session runs in local mode on one thread (``local[1]``), so task order
does not depend on a thread pool. Shuffle partitions are fixed at 1. The
session time zone is UTC. The UI is off. The driver binds to 127.0.0.1, so it
does not advertise an external address. Arrow conversion is off, so decimal
values stay on the Python parser in :mod:`qa_core_recon.decimals`.

The session catalog is in memory. Hive is not started. Warehouse, derby, and
scratch directories are created under a temporary directory, so a run does not
leave ``metastore_db``, ``derby.log``, or ``spark-warehouse`` in the working
directory or the repo root.

The Iceberg Spark runtime is loaded only when the caller asks for it, because
Spark cannot add jars after the session exists. The jar is
``org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1``, the Iceberg
version AWS Glue 5.0 ships for Spark 3.5 and Scala 2.12. Spark resolves that
coordinate from Maven Central into an Ivy cache at
``~/.cache/qa-core-recon/ivy``, or ``QA_CORE_RECON_IVY`` when that variable
is set. The cache is not inside the session temp directory, so stopping the
session does not delete it. A Hadoop catalog for one warehouse is registered
later, and only when a spec reads Iceberg.

S3 reads use that same cache and Maven Central. The jars are
``org.apache.hadoop:hadoop-aws:3.3.4`` and
``com.amazonaws:aws-java-sdk-bundle:1.12.262``. PySpark 3.5 ships Hadoop
3.3.4, and hadoop-aws 3.3.4 is built against that SDK bundle. They are added
only when the caller asks for S3, so a local spec does not download them.
Credentials stay on the AWS default chain. They are not copied into the Spark
configuration. ``QA_CORE_RECON_S3_ENDPOINT``, when set, points s3a at that
endpoint and turns on path-style access.

Spark prints a startup banner on the process stderr before Python can change
its log level, and that banner contains a timestamp. Ivy prints a settings
line on stdout when the Iceberg runtime is resolved. Both are captured and
dropped so the CLI's output stays limited to refusals, errors, and the
result line.

The first call creates the session. Later calls in the same process return
that session. Spark allows one context per JVM, so a stopped session is not
replaced here. Stopping the session deletes its temporary directory.
"""

import hashlib
import os
import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Confirmed on the AWS Glue versions page: Glue 5.0 ships Iceberg 1.7.1
# with Spark 3.5.4 and Scala 2.12.
ICEBERG_VERSION = "1.7.1"
ICEBERG_SPARK_PACKAGE = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
# PySpark 3.5.0 through 3.5.9 set hadoop.version to 3.3.4. The hadoop-aws
# 3.3.4 POM depends on aws-java-sdk-bundle 1.12.262.
HADOOP_VERSION = "3.3.4"
AWS_SDK_BUNDLE_VERSION = "1.12.262"
HADOOP_AWS_PACKAGE = f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
AWS_SDK_BUNDLE_PACKAGE = f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_BUNDLE_VERSION}"
S3_ENDPOINT_ENV = "QA_CORE_RECON_S3_ENDPOINT"
_DEFAULT_S3_REGION = "us-east-1"
MAVEN_CENTRAL = "https://repo1.maven.org/maven2"

_session: SparkSession | None = None
_warehouse: Path | None = None
_iceberg_ready: bool = False
_s3_ready: bool = False
_RUNTIME_JAR_SUFFIX = f"iceberg-spark-runtime-3.5_2.12-{ICEBERG_VERSION}.jar"
_HADOOP_AWS_JAR_SUFFIX = f"hadoop-aws-{HADOOP_VERSION}.jar"
_AWS_SDK_JAR_SUFFIX = f"aws-java-sdk-bundle-{AWS_SDK_BUNDLE_VERSION}.jar"


class SessionStartError(Exception):
    """The local Spark session did not start. This is a read error."""


class IcebergRuntimeError(SessionStartError):
    """The Iceberg Spark runtime jar is not available. This is a read error."""


class S3RuntimeError(SessionStartError):
    """The Hadoop AWS jars are not available. This is a read error."""


def local_session_active() -> bool:
    """Return True when this process already has a session."""
    return _session is not None


def ivy_cache_dir() -> Path:
    """Return the Ivy cache that holds the Iceberg runtime jar."""
    override = os.environ.get("QA_CORE_RECON_IVY", "").strip()
    if override:
        return Path(override)
    return Path.home() / ".cache" / "qa-core-recon" / "ivy"


def catalog_name_for(warehouse: str) -> str:
    """Return the Spark catalog name for one warehouse path string.

    The name is derived from the path text. The same text always maps to the
    same catalog, and a different warehouse does not reuse it.
    """
    digest = hashlib.sha256(warehouse.encode("utf-8")).hexdigest()[:16]
    return f"iceberg_{digest}"


def hadoop_catalog(spark: SparkSession, warehouse: str) -> str:
    """Register a Hadoop catalog for ``warehouse`` and return its name.

    The session catalog stays in memory. This catalog is separate, local, and
    uncached, so a later read sees the current snapshot.
    """
    name = catalog_name_for(warehouse)
    prefix = f"spark.sql.catalog.{name}"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{prefix}.type", "hadoop")
    spark.conf.set(f"{prefix}.warehouse", warehouse)
    spark.conf.set(f"{prefix}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    spark.conf.set(f"{prefix}.cache-enabled", "false")
    return name


def missing_runtime_message(cache: Path) -> str:
    """Return the read error used when the Iceberg jar cannot be resolved."""
    return (
        f"the Iceberg runtime {ICEBERG_SPARK_PACKAGE} could not be fetched "
        f"from Maven Central or found in the Ivy cache at {cache}"
    )


def _jar_named(cache: Path, suffix: str) -> bool:
    if not cache.is_dir():
        return False
    return any(path.is_file() and path.name.endswith(suffix) for path in cache.rglob("*.jar"))


def runtime_jar_cached(cache: Path) -> bool:
    """Return True when the pinned Iceberg runtime jar is already in ``cache``."""
    return _jar_named(cache, _RUNTIME_JAR_SUFFIX)


def s3_jars_cached(cache: Path) -> bool:
    """Return True when both pinned S3 jars are already in ``cache``."""
    return _jar_named(cache, _HADOOP_AWS_JAR_SUFFIX) and _jar_named(cache, _AWS_SDK_JAR_SUFFIX)


def jars_packages(*, iceberg: bool = False, s3: bool = False) -> str:
    """Return the Maven coordinates Spark should resolve, or an empty string."""
    parts: list[str] = []
    if iceberg:
        parts.append(ICEBERG_SPARK_PACKAGE)
    if s3:
        parts.append(HADOOP_AWS_PACKAGE)
        parts.append(AWS_SDK_BUNDLE_PACKAGE)
    return ",".join(parts)


def resolve_s3_region() -> str | None:
    """Return ``AWS_REGION``, else ``AWS_DEFAULT_REGION``, else None."""
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def s3_hadoop_settings() -> dict[str, str]:
    """Return Hadoop s3a settings for this process.

    Credentials are not included. The default AWS chain reads them from the
    environment, a profile, or an instance role when a request is sent.
    ``QA_CORE_RECON_S3_ENDPOINT`` switches on path-style access. An ``http://``
    endpoint also turns TLS off, and retries are limited to one attempt after
    200ms so a dead endpoint fails quickly. With that override and no region
    variable, the client uses ``us-east-1``. With no override, ``AWS_REGION``
    or ``AWS_DEFAULT_REGION`` is copied to ``fs.s3a.endpoint.region`` when
    set. When neither is set the property is left empty. Hadoop 3.3.4's
    DefaultS3ClientFactory then uses ``us-east-1`` and global bucket access.
    It does not consult the AWS SDK region chain.
    """
    settings = {
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    endpoint = os.environ.get(S3_ENDPOINT_ENV, "").strip()
    if endpoint:
        settings["fs.s3a.endpoint"] = endpoint
        settings["fs.s3a.path.style.access"] = "true"
        settings["fs.s3a.connection.ssl.enabled"] = (
            "false" if endpoint.lower().startswith("http://") else "true"
        )
        # Moto and some compatible stores do not return stable object etags.
        settings["fs.s3a.change.detection.mode"] = "none"
        settings["fs.s3a.change.detection.version.required"] = "false"
        settings["fs.s3a.connection.establish.timeout"] = "5000"
        settings["fs.s3a.connection.timeout"] = "10000"
        settings["fs.s3a.attempts.maximum"] = "2"
        settings["fs.s3a.retry.limit"] = "1"
        settings["fs.s3a.retry.interval"] = "200ms"
        settings["fs.s3a.multiobjectdelete.enable"] = "false"
    region = resolve_s3_region()
    if region is None and endpoint:
        region = _DEFAULT_S3_REGION
    if region:
        settings["fs.s3a.endpoint.region"] = region
    return settings


def apply_s3_config(spark: SparkSession) -> None:
    """Apply :func:`s3_hadoop_settings` to the live Hadoop configuration."""
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in s3_hadoop_settings().items():
        conf.set(key, value)
    scrub_s3_credentials(spark)


_CREDENTIAL_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)
# Old s3n keys and the s3a keys. The provider class name is not in this list.
_CREDENTIAL_KEY_MARKERS = (
    "access.key",
    "secret.key",
    "session.token",
    "awsaccesskeyid",
    "awssecretaccesskey",
    "awssessiontoken",
)


def _credential_values() -> tuple[str, ...]:
    values = []
    for name in _CREDENTIAL_ENV:
        value = os.environ.get(name, "").strip()
        if len(value) >= 8:
            values.append(value)
    return tuple(values)


def _holds_credential(key: str, text: str, secrets: tuple[str, ...]) -> bool:
    lowered = key.lower()
    if any(marker in lowered for marker in _CREDENTIAL_KEY_MARKERS):
        return True
    return any(secret in text for secret in secrets)


def scrub_s3_credentials(spark: SparkSession) -> None:
    """Drop credentials Hadoop copied into its configuration.

    The AWS chain still supplies them on the next request. They are not left
    where a later log line or config dump could print them. This covers the
    s3a keys and the older s3n keys Hadoop fills from the environment.
    """
    secrets = _credential_values()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    names: list[str] = []
    iterator = conf.iterator()
    while iterator.hasNext():
        entry = iterator.next()
        key = str(entry.getKey())
        value = entry.getValue()
        text = "" if value is None else str(value)
        if _holds_credential(key, text, secrets):
            names.append(key)
    for name in names:
        conf.unset(name)
    for key, value in spark.sparkContext.getConf().getAll():
        if _holds_credential(key, value, secrets):
            try:
                spark.conf.unset(key)
            except Exception:
                continue


def missing_s3_runtime_message(cache: Path) -> str:
    """Return the read error used when the S3 jars cannot be resolved."""
    return (
        f"the S3 runtime {HADOOP_AWS_PACKAGE} and {AWS_SDK_BUNDLE_PACKAGE} "
        f"could not be fetched from Maven Central or found in the Ivy cache at {cache}"
    )


def _dependency_resolution_failure(exc: BaseException) -> bool:
    """True when Spark's output says Ivy could not resolve a package.

    Ivy writes ``:: UNRESOLVED DEPENDENCIES ::`` to the process stderr, not
    into the exception text. ``_start_quietly`` keeps that text on
    ``startup_log`` so it can be classified without being shown to the user.
    """
    parts = [str(exc)]
    log = getattr(exc, "startup_log", None)
    if isinstance(log, str) and log:
        parts.append(log)
    text = "\n".join(parts).lower()
    return "unresolved dependency" in text or "ivy" in text


def _generic_start_error(exc: BaseException) -> SessionStartError:
    detail = " ".join(str(exc).split()) or exc.__class__.__name__
    if detail.startswith("cannot start the local Spark session"):
        return SessionStartError(detail)
    return SessionStartError(f"cannot start the local Spark session: {detail}")


def session_start_failure(
    exc: BaseException, *, iceberg: bool, ivy: Path | None, s3: bool = False
) -> BaseException:
    """Map a failed session start to a read error.

    A runtime is named only when the output points at dependency resolution
    and the pinned jar is not in the Ivy cache. S3 is checked first, then
    Iceberg. Any other start failure keeps the generic session message. A
    session that was not asked to load either runtime is left unchanged.
    """
    if not iceberg and not s3:
        return exc
    if ivy is not None and _dependency_resolution_failure(exc):
        if s3 and not s3_jars_cached(ivy):
            return S3RuntimeError(missing_s3_runtime_message(ivy))
        if iceberg and not runtime_jar_cached(ivy):
            return IcebergRuntimeError(missing_runtime_message(ivy))
    return _generic_start_error(exc)


def local_session(
    warehouse: Path | None = None, *, iceberg: bool = False, s3: bool = False
) -> SparkSession:
    """Return the process-wide local session, creating it on the first call.

    ``iceberg`` loads the Iceberg runtime. ``s3`` loads the Hadoop AWS client
    and the AWS SDK bundle. Spark accepts jars only while the session is
    created, so a session that was started without one of them cannot gain
    it later.
    """
    global _session, _warehouse, _iceberg_ready, _s3_ready
    if _session is not None:
        if iceberg and not _iceberg_ready:
            raise RuntimeError(
                "the local Spark session is already running without the Iceberg runtime"
            )
        if s3 and not _s3_ready:
            raise RuntimeError("the local Spark session is already running without the S3 runtime")
        return _session

    root = warehouse if warehouse is not None else Path(tempfile.mkdtemp(prefix="qa-core-recon-"))
    root.mkdir(parents=True, exist_ok=True)
    derby = root / "derby"
    scratch = root / "local"
    tmp = root / "tmp"
    meta = root / "warehouse"
    for path in (derby, scratch, tmp, meta):
        path.mkdir(parents=True, exist_ok=True)

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    java_options = f"-Dderby.system.home={derby} -Djava.io.tmpdir={tmp}"
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("qa-core-recon")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(meta))
        .config("spark.local.dir", str(scratch))
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    ivy: Path | None = None
    if iceberg or s3:
        ivy = ivy_cache_dir()
        ivy.mkdir(parents=True, exist_ok=True)
        builder = (
            builder.config("spark.jars.packages", jars_packages(iceberg=iceberg, s3=s3))
            .config("spark.jars.ivy", str(ivy))
            .config("spark.jars.repositories", MAVEN_CENTRAL)
        )
    if s3:
        for key, value in s3_hadoop_settings().items():
            builder = builder.config(f"spark.hadoop.{key}", value)
    try:
        session = _start_quietly(builder)
        session.sparkContext.setLogLevel("ERROR")
        if s3:
            apply_s3_config(session)
            _quiet_s3_logs(session)
    except Exception as exc:
        shutil.rmtree(root, ignore_errors=True)
        failure = session_start_failure(exc, iceberg=iceberg, ivy=ivy, s3=s3)
        if isinstance(failure, SessionStartError):
            raise failure from None
        raise
    _session = session
    _warehouse = root
    _iceberg_ready = iceberg
    _s3_ready = s3
    return session


def _quiet_s3_logs(session: SparkSession) -> None:
    """Keep the S3 client from writing request lines to the process stderr."""
    try:
        configurator = session._jvm.org.apache.logging.log4j.core.config.Configurator
        level = session._jvm.org.apache.logging.log4j.Level.ERROR
        for name in ("org.apache.hadoop.fs.s3a", "com.amazonaws", "com.amazonaws.auth"):
            configurator.setLevel(name, level)
    except Exception:
        return


def _start_quietly(builder: SparkSession.Builder) -> SparkSession:
    """Create the session and drop Spark and Ivy startup lines.

    The JVM writes its banner to file descriptor 2, and Ivy writes a settings
    line to file descriptor 1. Redirecting Python's ``sys`` streams does not
    catch either, so both descriptors are pointed at a temporary file for the
    duration of startup and then restored. On failure the captured text is
    kept on the raised error for classification. It is not part of the message.
    """
    captured = tempfile.NamedTemporaryFile(prefix="qa-core-recon-spark-log-", delete=False)
    saved_out = os.dup(1)
    saved_err = os.dup(2)
    try:
        os.dup2(captured.fileno(), 1)
        os.dup2(captured.fileno(), 2)
        try:
            return builder.getOrCreate()
        except Exception as exc:
            os.dup2(saved_out, 1)
            os.dup2(saved_err, 2)
            captured.flush()
            try:
                startup_log = Path(captured.name).read_text(encoding="utf-8", errors="replace")
            except OSError:
                startup_log = ""
            detail = " ".join(str(exc).split()) or exc.__class__.__name__
            error = RuntimeError(f"cannot start the local Spark session: {detail}")
            error.startup_log = startup_log
            raise error from None
    finally:
        os.dup2(saved_out, 1)
        os.dup2(saved_err, 2)
        os.close(saved_out)
        os.close(saved_err)
        captured.close()
        Path(captured.name).unlink(missing_ok=True)


def stop_local_session() -> None:
    """Stop the session and delete its temporary directory."""
    global _session, _warehouse, _iceberg_ready, _s3_ready
    session = _session
    root = _warehouse
    _session = None
    _warehouse = None
    _iceberg_ready = False
    _s3_ready = False
    if session is not None:
        session.stop()
    if root is not None:
        shutil.rmtree(root, ignore_errors=True)
