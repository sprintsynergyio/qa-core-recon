"""Check the built sdist and wheel.

The wheel must not ship data files or example files. The sdist may ship the
small CSV fixture under ``examples/demo/data``, and no other dataset files.
"""

import sys
import tarfile
import zipfile
from pathlib import Path

DIST = Path("dist")
FIXTURE = "examples/demo/data/"
MAX_FIXTURE_FILE = 100_000
MAX_FIXTURE_TOTAL = 1_000_000
DATASET_SUFFIXES = (".csv", ".parquet", ".avro", ".orc")
TEMPLATE = "qa_core_recon/templates/recon.yaml"


def main() -> int:
    wheels = sorted(DIST.glob("*.whl"))
    sdists = sorted(DIST.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        print(f"expected one wheel and one sdist in {DIST}", file=sys.stderr)
        return 1
    errors = _check_wheel(wheels[0]) + _check_sdist(sdists[0])
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(f"wheel {wheels[0].name} has no data or example files")
    print(f"sdist {sdists[0].name} dataset files are only the small fixture")
    return 0


def _check_wheel(path: Path) -> list[str]:
    errors = []
    names = []
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            name = info.filename
            if name.endswith("/"):
                continue
            names.append(name)
            if _wheel_forbidden(name):
                errors.append(f"wheel contains {name}")
    if TEMPLATE not in names:
        errors.append(f"wheel is missing {TEMPLATE}")
    return errors


def _wheel_forbidden(name: str) -> bool:
    parts = name.split("/")
    if "examples" in parts:
        return True
    if "dist-info" not in parts and "data" in parts:
        return True
    return name.lower().endswith(DATASET_SUFFIXES)


def _check_sdist(path: Path) -> list[str]:
    errors = []
    total = 0
    fixture_files = 0
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            name = member.name.split("/", 1)[1] if "/" in member.name else member.name
            lowered = name.lower()
            if lowered.startswith(FIXTURE):
                fixture_files += 1
                total += member.size
                if "/iceberg/" in lowered:
                    errors.append(f"sdist fixture includes an Iceberg file: {name}")
                if not lowered.endswith(".csv"):
                    errors.append(f"sdist fixture is not a csv file: {name}")
                if member.size > MAX_FIXTURE_FILE:
                    errors.append(f"sdist fixture file is too large: {name}")
                continue
            if lowered.endswith(DATASET_SUFFIXES):
                errors.append(f"sdist has a dataset outside the fixture: {name}")
    if fixture_files == 0:
        errors.append("sdist is missing the demo fixture")
    if total > MAX_FIXTURE_TOTAL:
        errors.append(f"sdist fixture is {total} bytes, above {MAX_FIXTURE_TOTAL}")
    return errors


if __name__ == "__main__":
    raise SystemExit(main())
