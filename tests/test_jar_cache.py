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
    JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS,
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
        server = self.server
        hold = getattr(server, "hold", None)
        if hold is not None:
            hold.wait(timeout=10)
        delay = getattr(server, "delay", 0)
        if delay:
            time.sleep(delay)
        body = server.files.get(self.path)
        if body is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        mode = getattr(server, "mode", "normal")
        piece = getattr(server, "piece", 1)
        gap = getattr(server, "gap", 0)
        if mode == "stall":
            try:
                if body:
                    self.wfile.write(body[:1])
                    self.wfile.flush()
                time.sleep(getattr(server, "pause", 5))
                if len(body) > 1:
                    self.wfile.write(body[1:])
            except (BrokenPipeError, ConnectionResetError):
                return
            return
        if mode in {"drip", "throttle"}:
            for start in range(0, len(body), piece):
                self.wfile.write(body[start : start + piece])
                self.wfile.flush()
                time.sleep(gap)
            return
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def _mirror():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MirrorHandler)
    server.files = {}
    server.delay = 0
    server.hold = None
    server.mode = "normal"
    server.piece = 1
    server.gap = 0
    server.pause = 5
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}/maven2"
    return server, base


def _stop_mirror(server) -> None:
    hold = getattr(server, "hold", None)
    if hold is not None:
        hold.set()
    server.shutdown()
    server.server_close()


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


def test_download_timeouts_are_a_connect_limit_and_a_stall_limit():
    assert JAR_DOWNLOAD_CONNECT_TIMEOUT_SECONDS == 10
    assert JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS == 30
    assert JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS > JAR_DOWNLOAD_CONNECT_TIMEOUT_SECONDS


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
        _stop_mirror(server)


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
        _stop_mirror(server)


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
        _stop_mirror(server)
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
    import qa_core_recon.spark as spark_mod

    cache = tmp_path / "ivy"
    folder = cache / "jars" / ICEBERG_RUNTIME_JAR.filename
    folder.mkdir(parents=True)
    _isolate_session(monkeypatch, cache)

    def refuse_download(*_args, **_kwargs):
        raise AssertionError("downloaded a jar path that is a directory")

    monkeypatch.setattr(spark_mod.urllib.request, "urlopen", refuse_download)
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


def _download_temps(cache: Path, filename: str) -> list[Path]:
    jar_dir = cache / "jars"
    if not jar_dir.is_dir():
        return []
    return sorted(
        path
        for path in jar_dir.iterdir()
        if path.is_file()
        and path.name.startswith(f".{filename}.")
        and path.name.endswith(".download")
    )


def test_unique_temp_files_not_one_partial_name(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    pin, body = _pin("org.example", "alpha", b"unique-temp-body")
    server, base = _mirror()
    server.hold = threading.Event()
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    errors: list[BaseException] = []

    def run() -> None:
        try:
            spark_mod._resolve_one(cache, pin)
        except Exception as exc:
            errors.append(exc)

    try:
        _publish(server, base, [(pin, body)])
        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + 5
        temps: list[Path] = []
        while time.monotonic() < deadline:
            temps = _download_temps(cache, pin.filename)
            if len(temps) >= 2:
                break
            time.sleep(0.02)
        assert len(temps) >= 2
        assert len({path.name for path in temps}) >= 2
        assert not (cache / "jars" / f"{pin.filename}.partial").exists()
        server.hold.set()
        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()
        assert errors == []
        _assert_one_jar_per_pin(cache, [(pin, body)])
    finally:
        _stop_mirror(server)


def test_failed_download_does_not_delete_a_raced_jar(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    good = b"correct-jar-body"
    wrong = b"wrong-jar-body!!"
    pin = PinnedJar("org.example", "alpha", "1", hashlib.sha256(good).hexdigest())
    server, base = _mirror()
    server.hold = threading.Event()
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            outcome["path"] = spark_mod._resolve_one(cache, pin)
        except Exception as exc:
            outcome["error"] = exc

    try:
        _publish(server, base, [(pin, wrong)])
        thread = threading.Thread(target=run)
        thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not _download_temps(cache, pin.filename):
            time.sleep(0.02)
        assert _download_temps(cache, pin.filename)
        destination = cache / "jars" / pin.filename
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(good)
        server.hold.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert "error" not in outcome
        assert destination.read_bytes() == good
    finally:
        _stop_mirror(server)


def test_stall_is_enforced_between_small_reads(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    pin, body = _pin("org.example", "alpha", b"stall-after-one-byte-and-more")
    server, base = _mirror()
    server.mode = "stall"
    server.pause = 5
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    monkeypatch.setattr(spark_mod, "JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS", 0.4)
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            spark_mod._resolve_one(cache, pin)
        except Exception as exc:
            outcome["error"] = exc

    try:
        _publish(server, base, [(pin, body)])
        started = time.monotonic()
        thread = threading.Thread(target=run)
        thread.start()
        thread.join(timeout=2)
        elapsed = time.monotonic() - started
        assert not thread.is_alive()
        error = outcome.get("error")
        assert isinstance(error, SessionStartError)
        assert "no bytes received" in str(error)
        assert elapsed < 2
        assert not (cache / "jars" / pin.filename).exists()
    finally:
        _stop_mirror(server)


def test_slow_drip_succeeds_when_bytes_keep_arriving(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    body = b"drip-byte" * 8
    pin, _body = _pin("org.example", "alpha", body)
    server, base = _mirror()
    server.mode = "drip"
    server.piece = 1
    server.gap = 0.05
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    monkeypatch.setattr(spark_mod, "JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS", 0.5)
    try:
        _publish(server, base, [(pin, body)])
        started = time.monotonic()
        path = spark_mod._resolve_one(cache, pin)
        elapsed = time.monotonic() - started
        assert elapsed > 0.5
        assert path.read_bytes() == body
        assert not _download_temps(cache, pin.filename)
    finally:
        _stop_mirror(server)


def test_throttled_download_succeeds(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    body = b"throttle" * 512
    pin, _body = _pin("org.example", "alpha", body)
    server, base = _mirror()
    server.mode = "throttle"
    server.piece = 1024
    server.gap = 0.02
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    try:
        _publish(server, base, [(pin, body)])
        path = spark_mod._resolve_one(cache, pin)
        assert path.read_bytes() == body
    finally:
        _stop_mirror(server)


def test_stale_download_temp_is_swept_and_a_fresh_one_stays(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    pin, body = _pin("org.example", "alpha", b"fresh-jar-bytes")
    server, base = _mirror()
    cache = tmp_path / "ivy"
    jar_dir = cache / "jars"
    jar_dir.mkdir(parents=True)
    old = jar_dir / f".{pin.filename}.olddead.download"
    fresh = jar_dir / f".{pin.filename}.stilllive.download"
    old.write_bytes(b"stale")
    fresh.write_bytes(b"live")
    stale_at = time.time() - JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS - 5
    os.utime(old, (stale_at, stale_at))
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    try:
        _publish(server, base, [(pin, body)])
        spark_mod._resolve_one(cache, pin)
        assert not old.exists()
        assert fresh.is_file()
        assert fresh.read_bytes() == b"live"
        assert (jar_dir / pin.filename).read_bytes() == body
    finally:
        _stop_mirror(server)


def test_live_download_touches_its_temp_file(tmp_path, monkeypatch):
    import qa_core_recon.spark as spark_mod

    body = b"t" * 20
    pin, _body = _pin("org.example", "alpha", body)
    server, base = _mirror()
    server.mode = "drip"
    server.piece = 1
    server.gap = 0.1
    cache = tmp_path / "ivy"
    monkeypatch.setattr(spark_mod, "MAVEN_CENTRAL", base)
    monkeypatch.setattr(spark_mod, "JAR_DOWNLOAD_STALL_TIMEOUT_SECONDS", 0.5)
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            outcome["path"] = spark_mod._resolve_one(cache, pin)
        except Exception as exc:
            outcome["error"] = exc

    try:
        _publish(server, base, [(pin, body)])
        thread = threading.Thread(target=run)
        thread.start()
        deadline = time.monotonic() + 5
        temps: list[Path] = []
        while time.monotonic() < deadline:
            temps = _download_temps(cache, pin.filename)
            if temps:
                break
            time.sleep(0.02)
        assert temps
        time.sleep(0.6)
        spark_mod._sweep_stale_downloads(cache / "jars", pin.filename)
        assert temps[0].is_file()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert "error" not in outcome
        assert outcome["path"].read_bytes() == body
    finally:
        _stop_mirror(server)
