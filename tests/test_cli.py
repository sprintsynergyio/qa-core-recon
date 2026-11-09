import os
import subprocess
import sys
from importlib.metadata import version
from importlib.resources import files

import pytest
import yaml

from qa_core_recon import __version__
from qa_core_recon.cli import main
from qa_core_recon.exit_codes import ExitCode

# Read independently of cli.load_template so the test does not check the code against itself.
TEMPLATE = (files("qa_core_recon") / "templates" / "recon.yaml").read_bytes()


def test_version(capsys):
    assert main(["--version"]) is ExitCode.MATCH
    assert capsys.readouterr().out == "qa-core-recon 0.1.0\n"


_HELP_EXIT_CODES = (
    "exit codes:\n"
    "  0  init wrote the file, validate passed, or run matched.\n"
    "  1  run found a difference. init and validate never use this code.\n"
    "  2  refused, or a dataset or the output directory could not be used.\n"
)


def test_help_states_exit_codes():
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stderr == ""
    assert _HELP_EXIT_CODES in result.stdout
    assert "A missing command prints help to stderr and exits 2." in result.stdout


@pytest.mark.parametrize("command", ["init", "validate", "run"])
def test_command_help_states_exit_codes(command):
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", command, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stderr == ""
    assert _HELP_EXIT_CODES in result.stdout
    assert "A missing command" not in result.stdout


def test_missing_command_prints_help_and_exits_refused(capsys):
    code = main([])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err.startswith("usage: qa-core-recon")
    assert _HELP_EXIT_CODES in captured.err
    assert "A missing command prints help to stderr and exits 2." in captured.err
    assert "Traceback" not in captured.err


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


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses directory mode bits")
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
        "sampled",
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


def test_validate_float_tolerance_is_malformed(tmp_path, capsys):
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


def test_init_overlong_path_is_a_clean_error(tmp_path, capsys):
    target = tmp_path / ("n" * 300)
    code = main(["init", "--path", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code, captured, f"qa-core-recon: error: cannot use {target}: File name too long\n"
    )


def test_validate_duplicate_allowlist_exits_refused(tmp_path, capsys):
    text = (
        TEMPLATE.replace(b"path: ./data/raw/payments.csv", b"path: /etc/passwd", 1)
        + b'allowlist: ["/", "s3://demo-bucket"]\n'
    )
    target = _write_spec(tmp_path, text)
    code = main(["validate", "--spec", str(target)])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err.startswith("qa-core-recon: error: duplicate key 'allowlist' at line ")
    assert "Traceback" not in captured.err
    assert "unexpected error" not in captured.err
    assert captured.err.count("\n") == 1


def test_validate_deeply_nested_yaml_is_malformed(tmp_path, capsys):
    lines = [f"{'  ' * level}a:" for level in range(1200)]
    lines.append(f"{'  ' * 1200}1")
    target = _write_spec(tmp_path, ("\n".join(lines) + "\n").encode())
    code = main(["validate", "--spec", str(target)])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, "qa-core-recon: error: YAML is nested too deeply\n")


def test_validate_missing_file_exits_refused(tmp_path, capsys):
    missing = tmp_path / "missing.yaml"
    code = main(["validate", "--spec", str(missing)])
    captured = capsys.readouterr()
    _assert_stderr_line(code, captured, f"qa-core-recon: error: spec file not found: {missing}\n")


def test_validate_without_check_data_does_not_open_datasets(monkeypatch, tmp_path, capsys):
    def fail(*_args, **_kwargs):
        raise AssertionError("datasets were opened")

    monkeypatch.setattr("qa_core_recon.readers.check_data", fail)
    target = _write_spec(tmp_path, TEMPLATE)
    assert main(["validate", "--spec", str(target)]) is ExitCode.MATCH
    assert capsys.readouterr().out == "spec ok\n"


def _local_spec(tmp_path, source_body: str, target_body: str | None = None, **changes):
    from tests.support import spec_data, write_csv, write_spec_file

    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, source_body)
    write_csv(target, source_body if target_body is None else target_body)
    data = spec_data(tmp_path, source, target, target_format="csv")
    data.update(changes)
    return write_spec_file(tmp_path, data)


def test_check_data_prints_data_ok(tmp_path, capsys):
    spec = _local_spec(tmp_path, "1,10.50,2024-01-01,a\n", "1,99.00,2024-01-01,b\n")
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    captured = capsys.readouterr()
    assert captured.out == "data ok\n"
    assert captured.err == ""


def test_check_data_refuses_inexact_amount(tmp_path, capsys):
    spec = _local_spec(tmp_path, "1,10.005,2024-01-01,a\n")
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: inexact-amount: source amount column 'amount' value '10.005' "
        "is not an exact decimal at scale 2\n",
    )


def test_check_data_refuses_sample_only_without_reading(tmp_path, capsys):
    from tests.support import spec_data, write_spec_file

    source = tmp_path / "absent-source.csv"
    target = tmp_path / "absent-target.csv"
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["sampled"] = True
    spec = write_spec_file(tmp_path, data)
    assert main(["validate", "--spec", str(spec)]) is ExitCode.MATCH
    assert capsys.readouterr().out == "spec ok\n"
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: sample-only: the spec declares sampling, so a full recon cannot be claimed\n",
    )


def test_check_data_refuses_unmapped_column(tmp_path, capsys):
    from tests.support import spec_data, write_csv, write_spec_file

    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,10.00,2024-01-01\n", header="txn_id,amount,day\n")
    write_csv(target, "1,10.00,2024-01-01,a\n")
    spec = write_spec_file(tmp_path, spec_data(tmp_path, source, target, target_format="csv"))
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: unmapped-column: source is missing column 'note'\n",
    )


def test_check_data_refuses_float_amount(spark, tmp_path, capsys):
    from pyspark.sql.types import DoubleType

    from tests.support import spec_data, write_csv, write_spec_file
    from tests.test_readers import _write_parquet

    source = tmp_path / "source.csv"
    target = tmp_path / "target"
    write_csv(source, "1,10.00,2024-01-01,a\n")
    _write_parquet(spark, target, [("1", 1.5, "2024-01-01", "a")], DoubleType())
    spec = write_spec_file(tmp_path, spec_data(tmp_path, source, target))
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: float-amount: target amount column 'amount' is double "
        "and the spec has no cast rule\n",
    )


def test_check_data_path_not_allowed_wins_over_sample_only(tmp_path, capsys):
    text = TEMPLATE.replace(b"path: ./data/raw/payments.csv", b"path: ./database/x.csv", 1)
    text = text.replace(b"sampled: false", b"sampled: true", 1)
    target = _write_spec(tmp_path, text)
    code = main(["validate", "--spec", str(target), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: path-not-allowed: source path './database/x.csv' is outside the allowlist\n",
    )


def test_check_data_s3_is_an_error_not_a_refusal(tmp_path, capsys):
    from tests.support import spec_data, write_csv, write_spec_file

    source = tmp_path / "source.csv"
    write_csv(source, "1,10.00,2024-01-01,a\n")
    data = spec_data(tmp_path, source, tmp_path / "unused.csv")
    data["target"] = {"path": "s3://demo-bucket/curated/payments", "format": "parquet"}
    data["allowlist"] = [str(tmp_path), "s3://demo-bucket"]
    spec = write_spec_file(tmp_path, data)
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "qa-core-recon: error: target bucket not found: s3://demo-bucket/curated/payments\n",
    )
    assert "qa-core-recon-test-secret-key" not in captured.err


def test_check_data_output_is_deterministic(tmp_path, capsys):
    spec = _local_spec(tmp_path, "1,10.005,2024-01-01,a\n2,9.001,2024-01-01,b\n")
    first = main(["validate", "--spec", str(spec), "--check-data"])
    first_captured = capsys.readouterr()
    second = main(["validate", "--spec", str(spec), "--check-data"])
    second_captured = capsys.readouterr()
    assert first is second is ExitCode.REFUSED
    assert first_captured.err == second_captured.err
    assert "10.005" in first_captured.err
    assert "9.001" not in first_captured.err


def test_sample_only_does_not_start_spark(monkeypatch, tmp_path, capsys):
    def fail(*_args, **_kwargs):
        raise AssertionError("spark started")

    monkeypatch.setattr("qa_core_recon.spark.local_session", fail)
    from tests.support import spec_data, write_spec_file

    source = tmp_path / "absent-source.csv"
    target = tmp_path / "absent-target.csv"
    sampled = spec_data(tmp_path, source, target, target_format="csv")
    sampled["sampled"] = True
    spec = write_spec_file(tmp_path, sampled)
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_stderr_line(
        code,
        captured,
        "refused: sample-only: the spec declares sampling, so a full recon cannot be claimed\n",
    )


def test_malformed_csv_exits_refused(tmp_path, capsys):
    from tests.support import spec_data, write_csv, write_spec_file

    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,10.00\n")
    write_csv(target, "1,10.00,2024-01-01,a\n")
    spec = write_spec_file(tmp_path, spec_data(tmp_path, source, target, target_format="csv"))
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert "qa-core-recon: error: source csv has a malformed row" in captured.err
    assert "Traceback" not in captured.err


def test_check_data_process_exits(tmp_path):
    spec = _local_spec(tmp_path, "1,10.00,2024-01-01,a\n")
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "validate", "--spec", str(spec), "--check-data"],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    assert result.returncode == 0
    assert result.stdout == "data ok\n"
    assert result.stderr == ""
    assert "Traceback" not in result.stderr
