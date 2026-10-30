"""Command-line interface for qa-core-recon."""

import argparse
import sys
from importlib.resources import files
from pathlib import Path

from qa_core_recon import __version__
from qa_core_recon.exit_codes import ExitCode

PROG = "qa-core-recon"
DEFAULT_SPEC_PATH = "recon.yaml"


def load_template() -> bytes:
    """Return the packaged recon.yaml template exactly as shipped."""
    return (files("qa_core_recon") / "templates" / "recon.yaml").read_bytes()


def _init(args: argparse.Namespace) -> int:
    path = Path(args.path)
    # "x" mode fails if the file exists, so the check and the write are one step.
    mode = "wb" if args.force else "xb"
    try:
        with path.open(mode) as fh:
            fh.write(load_template())
    except FileExistsError:
        print(f"{PROG}: error: {path} already exists; use --force to overwrite", file=sys.stderr)
        return ExitCode.REFUSED
    print(path)
    return 0


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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print(f"{PROG} {__version__}")
        return 0
    if args.command is None:
        parser.print_help(sys.stderr)
        return ExitCode.REFUSED
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
