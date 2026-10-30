import subprocess
import sys
from importlib.metadata import version
from importlib.resources import files

import yaml

from qa_core_recon import __version__
from qa_core_recon.cli import main
from qa_core_recon.exit_codes import ExitCode

# Read independently of cli.load_template so the test does not check the code against itself.
TEMPLATE = (files("qa_core_recon") / "templates" / "recon.yaml").read_bytes()


def test_version(capsys):
    assert main(["--version"]) is ExitCode.MATCH
    assert capsys.readouterr().out == "qa-core-recon 0.1.0\n"


def test_version_via_python_m():
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == f"qa-core-recon {__version__}\n"


def test_installed_version_matches_package():
    assert version("qa-core-recon") == __version__


def test_init_writes_packaged_template(tmp_path, capsys):
    target = tmp_path / "recon.yaml"
    assert main(["init", "--path", str(target)]) is ExitCode.MATCH
    assert target.read_bytes() == TEMPLATE
    assert capsys.readouterr().out == f"{target}\n"


def test_init_defaults_to_recon_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) is ExitCode.MATCH
    assert (tmp_path / "recon.yaml").read_bytes() == TEMPLATE


def test_init_refuses_to_overwrite(tmp_path, capsys):
    target = tmp_path / "recon.yaml"
    target.write_bytes(b"existing: true\n")
    assert main(["init", "--path", str(target)]) is ExitCode.REFUSED
    assert target.read_bytes() == b"existing: true\n"
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        captured.err == f"qa-core-recon: error: {target} already exists; use --force to overwrite\n"
    )
    assert "Traceback" not in captured.err


def _assert_stderr_line(code, captured, expected: str):
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err == expected
    assert "Traceback" not in captured.err
    assert captured.err.count("\n") == 1


def test_init_directory_exits_refused(tmp_path, capsys):
    target = tmp_path / "recon.yaml"
    target.mkdir()
    code = main(["init", "--path", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, f"qa-core-recon: error: {target} is a directory\n")
    assert "--force" not in captured.err
    assert not any(target.iterdir())


def test_init_directory_with_force_still_exits_refused(tmp_path, capsys):
    target = tmp_path / "recon.yaml"
    target.mkdir()
    code = main(["init", "--path", str(target), "--force"])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, f"qa-core-recon: error: {target} is a directory\n")
    assert "--force" not in captured.err


def test_init_missing_parent_exits_refused(tmp_path, capsys):
    target = tmp_path / "missing" / "recon.yaml"
    code = main(["init", "--path", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        f"qa-core-recon: error: parent directory does not exist: {target.parent}\n",
    )
    assert "--force" not in captured.err
    assert not target.exists()


def test_init_parent_is_file_exits_refused(tmp_path, capsys):
    parent = tmp_path / "not-a-directory"
    parent.write_text("x", encoding="utf-8")
    target = parent / "recon.yaml"
    code = main(["init", "--path", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        f"qa-core-recon: error: {target} cannot be created because a parent is not a directory\n",
    )
    assert "--force" not in captured.err
    assert "Traceback" not in captured.err


def test_init_permission_denied_exits_refused(tmp_path, capsys):
    parent = tmp_path / "locked"
    parent.mkdir()
    target = parent / "recon.yaml"
    parent.chmod(0o500)
    try:
        code = main(["init", "--path", str(target)])
        captured = capsys.readouterr()
    finally:
        parent.chmod(0o700)
    _assert_stderr_line(code, captured, f"qa-core-recon: error: permission denied: {target}\n")
    assert "--force" not in captured.err
    assert not target.exists()


def test_unexpected_exception_exits_refused(monkeypatch, capsys):
    def explode(args):
        raise RuntimeError("boom")

    monkeypatch.setattr("qa_core_recon.cli._init", explode)
    code = main(["init", "--path", "recon.yaml"])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, "qa-core-recon: error: unexpected error: boom\n")


def test_init_force_overwrites(tmp_path):
    target = tmp_path / "recon.yaml"
    target.write_bytes(b"existing: true\n")
    assert main(["init", "--path", str(target), "--force"]) is ExitCode.MATCH
    assert target.read_bytes() == TEMPLATE


def test_template_keys_in_order_and_tolerance_is_a_string():
    spec = yaml.safe_load(TEMPLATE)
    assert list(spec) == [
        "version",
        "source",
        "target",
        "keys",
        "amounts",
        "group_by",
        "hash_columns",
        "tolerance",
        "allowlist",
    ]
    assert spec["tolerance"] == {"value": "0", "reason": ""}


def _write_spec(tmp_path, text: bytes):
    target = tmp_path / "recon.yaml"
    target.write_bytes(text)
    return target


def test_validate_default_template(tmp_path, capsys):
    target = _write_spec(tmp_path, TEMPLATE)
    assert main(["validate", "--spec", str(target)]) is ExitCode.MATCH
    captured = capsys.readouterr()
    assert captured.out == "spec ok\n"
    assert captured.err == ""


def test_init_then_validate_default_template(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) is ExitCode.MATCH
    assert capsys.readouterr().out == "recon.yaml\n"
    assert main(["validate", "--spec", "recon.yaml"]) is ExitCode.MATCH
    captured = capsys.readouterr()
    assert captured.out == "spec ok\n"
    assert captured.err == ""


def test_validate_missing_key_exits_refused(tmp_path, capsys):
    target = _write_spec(tmp_path, TEMPLATE.replace(b"keys: [txn_id]", b"keys: []", 1))
    code = main(["validate", "--spec", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, "refused: missing-key: the spec has no business key\n")


def test_validate_tolerance_without_reason_exits_refused(tmp_path, capsys):
    text = TEMPLATE.replace(b'value: "0"', b'value: "0.01"', 1)
    target = _write_spec(tmp_path, text)
    code = main(["validate", "--spec", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: tolerance-without-reason: tolerance is above zero and reason is empty\n",
    )


def test_validate_path_not_allowed_exits_refused(tmp_path, capsys):
    text = TEMPLATE.replace(b"path: ./data/raw/payments.csv", b"path: ./database/x.csv", 1)
    target = _write_spec(tmp_path, text)
    code = main(["validate", "--spec", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: path-not-allowed: source path './database/x.csv' is outside the allowlist\n",
    )


def test_validate_float_tolerance_exits_refused(tmp_path, capsys):
    text = TEMPLATE.replace(b'value: "0"', b"value: 0.01", 1)
    target = _write_spec(tmp_path, text)
    code = main(["validate", "--spec", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "qa-core-recon: error: tolerance.value must be a quoted decimal string; "
        "a YAML float is not accepted\n",
    )


def test_validate_output_is_deterministic(tmp_path, capsys):
    target = _write_spec(tmp_path, TEMPLATE.replace(b"keys: [txn_id]", b"keys: []", 1))
    first = main(["validate", "--spec", str(target)])
    first_captured = capsys.readouterr()
    second = main(["validate", "--spec", str(target)])
    second_captured = capsys.readouterr()
    assert first is second is ExitCode.REFUSED
    assert first_captured.err == second_captured.err
    assert first_captured.out == second_captured.out == ""


def test_init_directory_process_exits_refused(tmp_path):
    target = tmp_path / "recon.yaml"
    target.mkdir()
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "init", "--path", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == ExitCode.REFUSED
    assert result.stdout == ""
    assert result.stderr == f"qa-core-recon: error: {target} is a directory\n"
    assert "Traceback" not in result.stderr


def test_validate_missing_file_exits_refused(tmp_path, capsys):
    missing = tmp_path / "missing.yaml"
    code = main(["validate", "--spec", str(missing)])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, f"qa-core-recon: error: spec file not found: {missing}\n")
