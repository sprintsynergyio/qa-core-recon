"""Command-line interface for qa-core-recon."""

import argparse
import sys
from importlib.resources import files
from pathlib import Path

from qa_core_recon import __version__
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.refusals import RefusalError
from qa_core_recon.spec import SpecError, load_spec

PROG = "qa-core-recon"
DEFAULT_SPEC_PATH = "recon.yaml"


def load_template() -> bytes:
    """Return the packaged recon.yaml template exactly as shipped."""
    return (files("qa_core_recon") / "templates" / "recon.yaml").read_bytes()


def _print_error(message: str) -> None:
    line = " ".join(message.split())
    print(f"{PROG}: error: {line}", file=sys.stderr)


def _reject_if_directory(path: Path) -> ExitCode | None:
    try:
        is_directory = path.is_dir()
    except OSError as exc:
        detail = exc.strerror or str(exc)
        _print_error(f"cannot use {path}: {detail}")
        return ExitCode.REFUSED
    if is_directory:
        _print_error(f"{path} is a directory")
        return ExitCode.REFUSED
    return None


def _init(args: argparse.Namespace) -> ExitCode:
    path = Path(args.path)
    # Exclusive create reports an existing directory as "already exists". --force
    # only replaces a file, so a directory has to be rejected before the open.
    rejected = _reject_if_directory(path)
    if rejected is not None:
        return rejected
    # "x" mode fails if the file exists, so the check and the write are one step.
    mode = "wb" if args.force else "xb"
    try:
        with path.open(mode) as fh:
            fh.write(load_template())
    except FileExistsError:
        rejected = _reject_if_directory(path)
        if rejected is not None:
            return rejected
        _print_error(f"{path} already exists; use --force to overwrite")
        return ExitCode.REFUSED
    except IsADirectoryError:
        _print_error(f"{path} is a directory")
        return ExitCode.REFUSED
    except NotADirectoryError:
        _print_error(f"{path} cannot be created because a parent is not a directory")
        return ExitCode.REFUSED
    except FileNotFoundError:
        _print_error(f"parent directory does not exist: {path.parent}")
        return ExitCode.REFUSED
    except PermissionError:
        _print_error(f"permission denied: {path}")
        return ExitCode.REFUSED
    except OSError as exc:
        detail = exc.strerror or str(exc)
        _print_error(f"cannot write {path}: {detail}")
        return ExitCode.REFUSED
    print(path)
    return ExitCode.MATCH


def _validate(args: argparse.Namespace) -> ExitCode:
    spec = load_spec(Path(args.spec))
    if not args.check_data:
        print("spec ok")
        return ExitCode.MATCH
    # Imported here so init and a spec-only validate do not load PySpark.
    # sample-only and s3 are decided before the session, so they do not start a JVM.
    # The Iceberg runtime is requested only when the spec uses Iceberg.
    from qa_core_recon.readers import ReadError, check_before_session, check_data

    try:
        check_before_session(spec)
    except ReadError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED

    from qa_core_recon.spark import (
        SessionStartError,
        local_session,
        local_session_active,
        stop_local_session,
    )

    needs_iceberg = spec.source.format == "iceberg" or spec.target.format == "iceberg"
    created = not local_session_active()
    try:
        spark = local_session(iceberg=needs_iceberg)
    except SessionStartError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED
    try:
        check_data(spark, spec)
    except ReadError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED
    else:
        print("data ok")
        return ExitCode.MATCH
    finally:
        if created:
            stop_local_session()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG, description="Prove a target dataset matches its source."
    )
    parser.add_argument("--version", action="store_true", help="print the version and exit")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND")

    init = commands.add_parser("init", help="write a recon.yaml spec template")
    init.add_argument(
        "--path", default=DEFAULT_SPEC_PATH, help=f"file to write (default: {DEFAULT_SPEC_PATH})"
    )
    init.add_argument("--force", action="store_true", help="overwrite the file if it exists")
    init.set_defaults(handler=_init)

    validate = commands.add_parser("validate", help="check that a recon.yaml spec is usable")
    validate.add_argument("--spec", required=True, help="path to the spec file")
    validate.add_argument(
        "--check-data",
        action="store_true",
        help="open both datasets and run the data-level refusals",
    )
    validate.set_defaults(handler=_validate)

    return parser


def _dispatch(argv: list[str] | None) -> ExitCode:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print(f"{PROG} {__version__}")
        return ExitCode.MATCH
    if args.command is None:
        parser.print_help(sys.stderr)
        return ExitCode.REFUSED
    return args.handler(args)


def main(argv: list[str] | None = None) -> ExitCode:
    """Run the CLI and return an exit code.

    Exit code 1 means a dataset mismatch. init and validate never report one.
    Any unexpected exception is printed as a single stderr line and returns 2,
    so a bug cannot surface as exit code 1 and cannot print a traceback.
    """
    try:
        return _dispatch(argv)
    except RefusalError as exc:
        refusal = exc.refusal
        message = " ".join(refusal.message.split())
        print(f"refused: {refusal.code}: {message}", file=sys.stderr)
        return ExitCode.REFUSED
    except SpecError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED
    except Exception as exc:
        detail = " ".join(str(exc).split()) or exc.__class__.__name__
        _print_error(f"unexpected error: {detail}")
        return ExitCode.REFUSED


if __name__ == "__main__":
    raise SystemExit(main())
