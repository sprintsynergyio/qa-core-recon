"""One Spark session for the whole test run.

Spark allows a single context in a JVM. Starting it once keeps the suite
fast, and stopping it here is the only stop in this process. The Iceberg
runtime is loaded here because this process cannot add jars later, and the
Iceberg tests share the session. The CLI loads that runtime only when a spec
uses Iceberg.
"""

import logging
import tempfile
from pathlib import Path

import pytest

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)


@pytest.fixture(scope="session", autouse=True)
def spark():
    from qa_core_recon.spark import local_session, stop_local_session

    warehouse = Path(tempfile.mkdtemp(prefix="qa-core-recon-test-"))
    session = local_session(warehouse, iceberg=True)
    yield session
    stop_local_session()
    if warehouse.exists():
        raise AssertionError(f"spark temp dir was not removed: {warehouse}")
