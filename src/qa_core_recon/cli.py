"""Command-line interface for qa-core-recon."""

import argparse
import sys
from importlib.resources import files
from pathlib import Path

from qa_core_recon import __version__
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.refusals import RefusalError
from qa_core_recon.spec import SpecError, load_spec, uses_s3

PROG = "qa-core-recon"
DEFAULT_SPEC_PATH = "recon.yaml"
# Shown by --help. The wording matches the Exit Codes section of the README.
_EXIT_CODES = """\
exit codes:
  0  init wrote the file, validate passed, or run matched.
  1  run found a difference. init and validate never use this code.
  2  refused, or a dataset or the output directory could not be used.
"""
_MISSING_COMMAND = "A missing command prints help to stderr and exits 2.\n"


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


def _run(args: argparse.Namespace) -> ExitCode:
    # Imported here so init and a spec-only validate do not load PySpark.
    from qa_core_recon.readers import ReadError
    from qa_core_recon.report import OutputError, ensure_output_available
    from qa_core_recon.spark import SessionStartError

    try:
        out = ensure_output_available(args.out, force=args.force)
    except OutputError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED
    try:
        return _execute_run(Path(args.spec), out)
    except OutputError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED
    except ReadError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED
    except SessionStartError as exc:
        _print_error(str(exc))
        return ExitCode.REFUSED


def _execute_run(spec_path: Path, out: Path) -> ExitCode:
    """Run the same refusals as validate --check-data, then the aggregate checks.

    A refusal still writes the report. A read error does not. The output
    directory is created only when a report is published.
    """
    from qa_core_recon.checks import execute_checks
    from qa_core_recon.compare import exit_code_for
    from qa_core_recon.readers import check_before_session, read_datasets
    from qa_core_recon.report import (
        build_result_report,
        publish,
        reject_output_overlap,
        render_markdown,
        spec_sha256,
    )
    from qa_core_recon.spark import local_session, local_session_active, stop_local_session
    from qa_core_recon.spec import enforce_spec_refusals, load_spec_shape

    # Shape only. A malformed spec raises SpecError and leaves --out untouched.
    spec = load_spec_shape(spec_path)
    reject_output_overlap(out, spec.source.path, spec.target.path)
    # Previous reports go away only after the path is accepted, so a data file
    # named report.json inside a refused output directory is left in place.
    _drop_previous_reports(out)
    try:
        enforce_spec_refusals(spec)
        # sample-only is decided here, before a JVM starts.
        check_before_session(spec)
    except RefusalError as exc:
        _publish_refused(spec_path, out, exc)
        raise

    created = not local_session_active()
    needs_iceberg = spec.source.format == "iceberg" or spec.target.format == "iceberg"
    spark = local_session(iceberg=needs_iceberg, s3=uses_s3(spec))
    try:
        try:
            source, target = read_datasets(spark, spec)
            checks, row_counts = execute_checks(source, target, spec)
        except RefusalError as exc:
            _publish_refused(spec_path, out, exc)
            raise
    finally:
        if created:
            stop_local_session()

    report = build_result_report(spec_sha256(spec_path), checks, row_counts)
    publish(out, report, render_markdown(report))
    print(report["status"])
    return exit_code_for(report["status"])


def _drop_previous_reports(out: Path) -> None:
    """Remove a previous report so a later failure cannot leave it behind."""
    from qa_core_recon.report import OutputError, _cannot_use

    if not out.is_dir():
        return
    try:
        (out / "report.json").unlink(missing_ok=True)
        (out / "report.md").unlink(missing_ok=True)
    except OSError as exc:
        raise OutputError(_cannot_use(out, exc)) from None


def _publish_refused(spec_path: Path, out: Path, exc: RefusalError) -> None:
    from qa_core_recon.report import build_refused_report, publish, render_markdown, spec_sha256

    report = build_refused_report(spec_sha256(spec_path), exc.refusal)
    publish(out, report, render_markdown(report))


def _validate(args: argparse.Namespace) -> ExitCode:
    spec = load_spec(Path(args.spec))
    if not args.check_data:
        print("spec ok")
        return ExitCode.MATCH
    # Imported here so init and a spec-only validate do not load PySpark.
    # sample-only is decided before the session, so it does not start a JVM.
    # Iceberg jars load only when the spec uses Iceberg. S3 jars load only
    # when a path uses s3://.
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
        spark = local_session(iceberg=needs_iceberg, s3=uses_s3(spec))
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
        prog=PROG,
        description="Prove a target dataset matches its source.",
        epilog=_EXIT_CODES + _MISSING_COMMAND,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="store_true", help="print the version and exit")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND")

    init = commands.add_parser(
        "init",
        help="write a recon.yaml spec template",
        epilog=_EXIT_CODES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    init.add_argument(
        "--path", default=DEFAULT_SPEC_PATH, help=f"file to write (default: {DEFAULT_SPEC_PATH})"
    )
    init.add_argument("--force", action="store_true", help="overwrite the file if it exists")
    init.set_defaults(handler=_init)

    validate = commands.add_parser(
        "validate",
        help="check that a recon.yaml spec is usable",
        epilog=_EXIT_CODES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    validate.add_argument("--spec", required=True, help="path to the spec file")
    validate.add_argument(
        "--check-data",
        action="store_true",
        help="open both datasets and run the data-level refusals",
    )
    validate.set_defaults(handler=_validate)

    run = commands.add_parser(
        "run",
        help="compare source and target and write a report",
        epilog=_EXIT_CODES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    run.add_argument("--spec", required=True, help="path to the spec file")
    run.add_argument("--out", required=True, help="directory for report.json and report.md")
    run.add_argument(
        "--force",
        action="store_true",
        help="write the report into a directory that is not empty",
    )
    run.set_defaults(handler=_run)

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
    run returns 1 when any check does not match.
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
