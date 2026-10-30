import subprocess
import sys
from importlib.metadata import version
from importlib.resources import files

import yaml

from qa_core_recon import __version__
from qa_core_recon.cli import main

# Read independently of cli.load_template so the test does not check the code against itself.
TEMPLATE = (files("qa_core_recon") / "templates" / "recon.yaml").read_bytes()


def test_version(capsys):
    assert main(["--version"]) == 0
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
    assert main(["init", "--path", str(target)]) == 0
    assert target.read_bytes() == TEMPLATE
    assert capsys.readouterr().out == f"{target}\n"


def test_init_defaults_to_recon_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    assert (tmp_path / "recon.yaml").read_bytes() == TEMPLATE


def test_init_refuses_to_overwrite(tmp_path, capsys):
    target = tmp_path / "recon.yaml"
    target.write_bytes(b"existing: true\n")
    assert main(["init", "--path", str(target)]) == 2
    assert target.read_bytes() == b"existing: true\n"
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "already exists" in captured.err


def test_init_force_overwrites(tmp_path):
    target = tmp_path / "recon.yaml"
    target.write_bytes(b"existing: true\n")
    assert main(["init", "--path", str(target), "--force"]) == 0
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
