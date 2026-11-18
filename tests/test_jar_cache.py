"""Cold-cache downloads stay safe when several runs share one cache."""

import hashlib
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pytest

from qa_core_recon.cli import main
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.spark import (
    ICEBERG_RUNTIME_JAR,
    JAR_DOWNLOAD_CONNECT_TIMEOUT_SECONDS,
    JAR_DOWNLOAD_TOTAL_TIMEOUT_SECONDS,
    PinnedJar,
    SessionStartError,
)
from tests.support import spec_data, write_csv, write_spec_file

_WORKER = """
import os
from pathlib import Path

import qa_core_recon.spark as spark

spark.MAVEN_CENTRAL = os.environ["MIRROR"]
cache = Path(os.environ["CACHE"])
for item in os.environ["PINS"].split(";"):
    group, artifact, version, sha256 = item.split(",")
    pin = spark.PinnedJar(group, artifact, version, sha256)
    path = spark._resolve_one(cache, pin)
    if spark._file_sha256(path) != sha256:
        raise SystemExit(f"bad hash for {path}")
"""


class _MirrorHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        delay = getattr(self.server, "delay", 0)
        if delay:
            time.sleep(delay)
        body = self.server.files.get(self.path)
        if body is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def _mirror():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MirrorHandler)
    server.files = {}
    server.delay = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}/maven2"
    return server, base


def _pin(group: str, artifact: str, body: bytes) -> tuple[PinnedJar, bytes]:
    pin = PinnedJar(group, artifact, "1", hashlib.sha256(body).hexdigest())
    return pin, body


def _publish(server, base: str, pins: list[tuple[PinnedJar, bytes]]) -> None:
    import qa_core_recon.spark as spark_mod

    saved = spark_mod.MAVEN_CENTRAL
    spark_mod.MAVEN_CENTRAL = base
    try:
        for pin, body in pins:
            server.files[urlparse(pin.maven_url()).path] = body
    finally:
        spark_mod.MAVEN_CENTRAL = saved


def _assert_one_jar_per_pin(cache: Path, pins: list[tuple[PinnedJar, bytes]]) -> None:
    files = sorted(path for path in cache.rglob("*") if path.is_file())
    expected = sorted(pin.filename for pin, _body in pins)
    assert [path.name for path in files] == expected
    by_name = {pin.filename: pin for pin, _body in pins}
    for path in files:
        pin = by_name[path.name]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == pin.sha256
        assert not path.name.endswith(".download")


def _fetch_all(cache: Path, pins: list[PinnedJar]) -> None:
    import qa_core_recon.spark as spark_mod

    for pin in pins:
        spark_mod._resolve_one(cache, pin)


def test_download_timeouts_fail_a_blackhole_in_under_a_minute():
    assert 0 < JAR_DOWNLOAD_CONNECT_TIMEOUT_SECONDS < JAR_DOWNLOAD_TOTAL_TIMEOUT_SECONDS < 60


def test_concurrent_processes_share_a_cold_cache(tmp_path):
    pins = [
        _pin("org.example", "alpha", b"alpha-jar-bytes"),
        _pin("org.example", "beta", b"beta-jar-bytes"),
    ]
    server, base = _mirror()
    server.delay = 0.05
    cache = tmp_path / "ivy"
    try:
        _publish(server, base, pins)
        env = os.environ.copy()
        env["MIRROR"] = base
        env["CACHE"] = str(cache)
        env["PINS"] = ";".join(
            f"{pin.group},{pin.artifact},{pin.version},{pin.sha256}" for pin, _body in pins
        )
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", _WORKER],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(3)
        ]
        for process in processes:
            _stdout, stderr = process.communicate(timeout=60)
            assert process.returncode == 0, stderr
        _assert_one_jar_per_pin(cache, pins)
    finally:
        server.shutdown()
        server.server_close()


def test_concurrent_threads_share_a_cold_cache(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    pins = [
        _pin("org.example", "alpha", b"alpha-jar-bytes"),
        _pin("org.example", "beta", b"beta-jar-bytes"),
    ]
    server, base = _mirror()
    server.delay = 0.05
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    try:
        _publish(server, base, pins)
        errors: list[BaseException] = []

        def run() -> None:
            try:
                _fetch_all(cache, [pin for pin, _body in pins])
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
            assert not thread.is_alive()
        assert errors == []
        _assert_one_jar_per_pin(cache, pins)
    finally:
        server.shutdown()
        server.server_close()


def _isolate_session(monkeypatch, cache: Path) -> None:
    import qa_core_recon.spark as spark_mod

    monkeypatch.setattr(spark_mod, "ivy_cache_dir", lambda: cache)
    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)
    monkeypatch.setattr(spark_mod, "_s3_ready", False)

    def refuse_start(_builder):
        raise AssertionError("Spark started on an unverified jar")

    monkeypatch.setattr(spark_mod, "_start_quietly", refuse_start)


def _iceberg_spec(tmp_path: Path) -> Path:
    source = tmp_path / "warehouse"
    target = tmp_path / "target.csv"
    write_csv(target, "1,10.50,2024-01-01,a\n")
    return write_spec_file(
        tmp_path,
        spec_data(
            tmp_path,
            source,
            target,
            source_format="iceberg",
            target_format="csv",
            source_table="payments.txn",
        ),
    )


def test_wrong_download_is_rejected_and_leaves_the_cache_empty(
    spark, monkeypatch, tmp_path, capsys
):
    import qa_core_recon.spark as spark_mod

    wrong = b"not-the-pinned-jar"
    server, base = _mirror()
    cache = tmp_path / "ivy"
    _isolate_session(monkeypatch, cache)
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    try:
        server.files[urlparse(ICEBERG_RUNTIME_JAR.maven_url()).path] = wrong
        spec = _iceberg_spec(tmp_path)
        code = main(["validate", "--spec", str(spec), "--check-data"])
    finally:
        server.shutdown()
        server.server_close()
    captured = capsys.readouterr()
    assert code == 2
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err.startswith("qa-core-recon: error:")
    assert "unexpected error" not in captured.err
    assert "failed verification" in captured.err
    assert ICEBERG_RUNTIME_JAR.sha256 in captured.err
    assert hashlib.sha256(wrong).hexdigest() in captured.err
    assert str(cache) in captured.err
    assert "not saved" in captured.err
    assert [path for path in cache.rglob("*") if path.is_file()] == []


def test_read_only_cold_cache_names_the_cache(spark, monkeypatch, tmp_path, capsys):
    parent = tmp_path / "ro"
    parent.mkdir()
    cache = parent / "ivy"
    _isolate_session(monkeypatch, cache)
    parent.chmod(0o555)
    try:
        spec = _iceberg_spec(tmp_path)
        code = main(["validate", "--spec", str(spec), "--check-data"])
    finally:
        parent.chmod(0o755)
    captured = capsys.readouterr()
    assert code == 2
    assert code is ExitCode.REFUSED
    assert "unexpected error" not in captured.err
    assert "is not writable" in captured.err
    assert str(cache) in captured.err
    assert "QA_CORE_RECON_IVY" in captured.err


def test_unreadable_cached_jar_is_a_read_error(spark, monkeypatch, tmp_path, capsys):
    cache = tmp_path / "ivy"
    jar = cache / "jars" / ICEBERG_RUNTIME_JAR.filename
    jar.parent.mkdir(parents=True)
    jar.write_bytes(b"cannot-read-me")
    jar.chmod(0o000)
    _isolate_session(monkeypatch, cache)
    try:
        spec = _iceberg_spec(tmp_path)
        code = main(["validate", "--spec", str(spec), "--check-data"])
    finally:
        jar.chmod(0o644)
    captured = capsys.readouterr()
    assert code == 2
    assert code is ExitCode.REFUSED
    assert "unexpected error" not in captured.err
    assert "cannot read jar" in captured.err
    assert str(jar) in captured.err
    assert str(cache) in captured.err
    assert "permissions" in captured.err


def test_jar_directory_is_a_read_error(spark, monkeypatch, tmp_path, capsys):
    cache = tmp_path / "ivy"
    folder = cache / "jars" / ICEBERG_RUNTIME_JAR.filename
    folder.mkdir(parents=True)
    _isolate_session(monkeypatch, cache)
    spec = _iceberg_spec(tmp_path)
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    assert code == 2
    assert code is ExitCode.REFUSED
    assert "unexpected error" not in captured.err
    assert "is a directory, not a file" in captured.err
    assert str(folder) in captured.err
    assert str(cache) in captured.err
    assert folder.is_dir()


def test_corrupt_cache_still_names_expected_and_actual(tmp_path):
    import qa_core_recon.spark as spark_mod

    pin, _body = _pin("org.example", "alpha", b"good-bytes")
    cache = tmp_path / "ivy"
    jar = cache / "jars" / pin.filename
    jar.parent.mkdir(parents=True)
    jar.write_bytes(b"bad-bytes")
    with pytest.raises(SessionStartError) as caught:
        spark_mod._resolve_one(cache, pin)
    message = str(caught.value)
    assert "failed verification" in message
    assert pin.sha256 in message
    assert hashlib.sha256(b"bad-bytes").hexdigest() in message
    assert str(cache) in message
    assert jar.read_bytes() == b"bad-bytes"
