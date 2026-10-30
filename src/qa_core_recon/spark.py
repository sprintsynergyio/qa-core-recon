"""Deterministic local Spark session.

The session runs in local mode on one thread (``local[1]``), so task order
does not depend on a thread pool. Shuffle partitions are fixed at 1. The
session time zone is UTC. The UI is off. The driver binds to 127.0.0.1, so it
does not advertise an external address. Arrow conversion is off, so decimal
values stay on the Python parser in :mod:`qa_core_recon.decimals`.

The catalog is in memory. Hive is not started. Warehouse, derby, and scratch
directories are created under a temporary directory, so a run does not leave
``metastore_db``, ``derby.log``, or ``spark-warehouse`` in the working
directory or the repo root.

Spark prints a startup banner on the process stderr before Python can change
its log level, and that banner contains a timestamp. The banner is captured
and dropped so the CLI's stderr stays limited to refusals and errors.

The first call creates the session. Later calls in the same process return
that session. Spark allows one context per JVM, so a stopped session is not
replaced here.
"""

import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

_session: SparkSession | None = None


def local_session_active() -> bool:
    """Return True when this process already has a session."""
    return _session is not None


def local_session(warehouse: Path | None = None) -> SparkSession:
    """Return the process-wide local session, creating it on the first call."""
    global _session
    if _session is not None:
        return _session

    root = warehouse if warehouse is not None else Path(tempfile.mkdtemp(prefix="qa-core-recon-"))
    root.mkdir(parents=True, exist_ok=True)
    derby = root / "derby"
    scratch = root / "local"
    tmp = root / "tmp"
    meta = root / "warehouse"
    ivy = root / "ivy"
    for path in (derby, scratch, tmp, meta, ivy):
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
        .config("spark.jars.ivy", str(ivy))
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    session = _start_quietly(builder)
    session.sparkContext.setLogLevel("ERROR")
    _session = session
    return session


def _start_quietly(builder: SparkSession.Builder) -> SparkSession:
    """Create the session and drop Spark's timestamped startup banner.

    The JVM writes that banner to file descriptor 2. Redirecting Python's
    ``sys.stderr`` does not catch it, so the descriptor itself is pointed at a
    temporary file for the duration of startup and then restored.
    """
    captured = tempfile.NamedTemporaryFile(prefix="qa-core-recon-spark-log-", delete=False)
    saved = os.dup(2)
    try:
        os.dup2(captured.fileno(), 2)
        try:
            return builder.getOrCreate()
        except Exception as exc:
            os.dup2(saved, 2)
            detail = " ".join(str(exc).split()) or exc.__class__.__name__
            raise RuntimeError(f"cannot start the local Spark session: {detail}") from None
    finally:
        os.dup2(saved, 2)
        os.close(saved)
        captured.close()
        Path(captured.name).unlink(missing_ok=True)


def stop_local_session() -> None:
    """Stop the session created by :func:`local_session`, if there is one."""
    global _session
    session = _session
    _session = None
    if session is not None:
        session.stop()
