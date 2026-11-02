"""One Spark session for the whole test run.

Spark allows a single context in a JVM. Starting it once keeps the suite
fast, and stopping it here is the only stop in this process. The Iceberg
runtime and the S3 jars are loaded here because this process cannot add jars
later, and the Iceberg and S3 tests share the session. The CLI loads each
runtime only when a spec needs it.

Moto listens on 127.0.0.1 at a random port. Fake credentials are set only in
this process, before the JVM starts, so the AWS chain can see them. They are
not written into a spec.
"""

import logging
import os
import tempfile
from pathlib import Path

import pytest

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

_S3_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "QA_CORE_RECON_S3_ENDPOINT",
)


def _push_s3_env(endpoint: str) -> dict[str, str | None]:
    saved = {key: os.environ.get(key) for key in _S3_ENV}
    os.environ["AWS_ACCESS_KEY_ID"] = "qa-core-recon-test-access-key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "qa-core-recon-test-secret-key"
    os.environ.pop("AWS_SESSION_TOKEN", None)
    os.environ.pop("AWS_SECURITY_TOKEN", None)
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ.pop("AWS_DEFAULT_REGION", None)
    os.environ["QA_CORE_RECON_S3_ENDPOINT"] = endpoint
    return saved


def _pop_s3_env(saved: dict[str, str | None]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture(scope="session", autouse=True)
def spark():
    from moto.server import ThreadedMotoServer

    from qa_core_recon.spark import local_session, stop_local_session

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    saved = _push_s3_env(f"http://{host}:{port}")
    warehouse = Path(tempfile.mkdtemp(prefix="qa-core-recon-test-"))
    try:
        session = local_session(warehouse, iceberg=True, s3=True)
    except Exception:
        server.stop()
        _pop_s3_env(saved)
        raise
    try:
        yield session
    finally:
        stop_local_session()
        server.stop()
        _pop_s3_env(saved)
        if warehouse.exists():
            raise AssertionError(f"spark temp dir was not removed: {warehouse}")
