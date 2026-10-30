"""Load and validate a recon.yaml spec.

A malformed spec raises SpecError. A spec that is well formed but must not run
raises RefusalError. Callers map both to exit code 2. Nothing in this module
decides that two datasets differ, so it never uses exit code 1.

Monetary amounts are parsed with Decimal from quoted strings. A YAML float is
rejected and is never converted.

Amount ``scale`` is an integer from 0 through 38, Spark's maximum decimal
precision. An optional per-amount ``cast`` is ``half-even`` or ``half-up``.
``sampled`` is a required boolean. A true value is stored on the spec and is
refused when datasets are opened, not while parsing.

Parsing uses PyYAML's safe loader. A duplicate key at any level is a malformed
spec: the error names the key and its line. The last value is not kept.

Allowlist checks compare normalised local path components. They do not touch
the filesystem, so they do not resolve symlinks and they do not join paths to
the current directory. An allowlist entry that normalises to an empty path,
the filesystem root, or a path that still contains ".." is malformed. s3://
URIs keep key segments as written. A "." segment, a ".." segment, an empty
segment, a query, or a fragment is malformed rather than folded.

``format`` is ``csv``, ``parquet``, or ``iceberg``. An Iceberg dataset adds
``table`` and an optional ``snapshot_id``. Those keys are rejected on CSV and
Parquet. ``path`` is still the location the allowlist checks: a file or
directory for CSV and Parquet, and the Hadoop catalog warehouse for Iceberg.
``table`` is a ``namespace.table`` identifier, not a filesystem path.
"""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

from qa_core_recon.decimals import CAST_MODE_NAMES, DECIMAL_PRECISION
from qa_core_recon.refusals import Refusal, RefusalCode, RefusalError

SUPPORTED_VERSION = 1
SUPPORTED_FORMATS = frozenset({"csv", "parquet", "iceberg"})
_SNAPSHOT_ID_MIN = -1 << 63
_SNAPSHOT_ID_MAX = (1 << 63) - 1
# One directory name under the warehouse. A dot or a slash would add or climb
# a directory. A leading digit is rejected so Spark can use the name unquoted.
_IDENTIFIER_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")

_TOP_LEVEL = (
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
)
_REQUIRED = tuple(key for key in _TOP_LEVEL if key != "keys")
_S3_SCHEME = "s3://"


class SpecError(ValueError):
    """The spec is malformed. Refusals were not evaluated."""


@dataclass(frozen=True)
class Dataset:
    path: str
    format: str
    table: str | None = None
    snapshot_id: int | None = None


@dataclass(frozen=True)
class Amount:
    column: str
    scale: int
    cast: str | None = None


@dataclass(frozen=True)
class Tolerance:
    value: Decimal
    reason: str


@dataclass(frozen=True)
class Spec:
    version: int
    source: Dataset
    target: Dataset
    keys: tuple[str, ...]
    amounts: tuple[Amount, ...]
    group_by: tuple[str, ...]
    hash_columns: tuple[str, ...]
    tolerance: Tolerance
    sampled: bool
    allowlist: tuple[str, ...]


@dataclass(frozen=True)
class _LocalPath:
    absolute: bool
    parts: tuple[str, ...]


@dataclass(frozen=True)
class _S3Path:
    bucket: str
    parts: tuple[str, ...]


class _SafeUniqueKeyLoader(yaml.SafeLoader):
    """Safe loader that rejects a repeated key instead of keeping the last one."""


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.Node, deep: bool = False) -> dict:
    if not isinstance(node, yaml.MappingNode):
        raise yaml.constructor.ConstructorError(
            None,
            None,
            f"expected a mapping node, but found {node.id}",
            node.start_mark,
        )
    seen: set[object] = set()
    for key_node, _value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            repeated = key in seen
        except TypeError:
            continue
        if repeated:
            line = key_node.start_mark.line + 1
            raise SpecError(f"duplicate key {key!r} at line {line}")
        seen.add(key)
    return yaml.constructor.SafeConstructor.construct_mapping(loader, node, deep)


_SafeUniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_spec(path: Path | str) -> Spec:
    """Read a spec file and return it, or raise SpecError or RefusalError."""
    spec_path = Path(path)
    try:
        text = spec_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise SpecError(f"spec file not found: {spec_path}") from None
    except IsADirectoryError:
        raise SpecError(f"spec path is a directory: {spec_path}") from None
    except PermissionError:
        raise SpecError(f"permission denied reading spec file: {spec_path}") from None
    except UnicodeDecodeError:
        raise SpecError(f"spec file is not valid UTF-8: {spec_path}") from None
    except OSError as exc:
        detail = exc.strerror or str(exc)
        raise SpecError(f"cannot read spec file {spec_path}: {detail}") from None
    return parse_spec(text)


def parse_spec(text: str) -> Spec:
    """Parse spec text. Shape is checked before any refusal."""
    data = _load_mapping(text)
    _reject_unknown(data, _TOP_LEVEL, "spec")
    for key in _REQUIRED:
        if key not in data:
            raise SpecError(f"missing {key}")

    version = _require_int(data["version"], "version")
    if version != SUPPORTED_VERSION:
        raise SpecError(
            f"unsupported spec version {version}; only version {SUPPORTED_VERSION} is supported"
        )

    source = _dataset(data["source"], "source")
    target = _dataset(data["target"], "target")
    keys = _keys(data)
    amounts = _amounts(data["amounts"])
    group_by = _name_list(data["group_by"], "group_by")
    hash_columns = _name_list(data["hash_columns"], "hash_columns")
    tolerance = _tolerance(data["tolerance"])
    sampled = _sampled(data["sampled"])
    allowlist = _name_list(data["allowlist"], "allowlist")

    _require_location(source.path, "source.path")
    _require_location(target.path, "target.path")
    for index, entry in enumerate(allowlist):
        _require_allowlist_entry(entry, f"allowlist[{index}]")

    if keys is None:
        raise RefusalError(Refusal(RefusalCode.MISSING_KEY, "the spec has no business key"))
    if tolerance.value > 0 and tolerance.reason.strip() == "":
        raise RefusalError(
            Refusal(
                RefusalCode.TOLERANCE_WITHOUT_REASON,
                "tolerance is above zero and reason is empty",
            )
        )
    for label, dataset in (("source", source), ("target", target)):
        if not _path_allowed(dataset.path, allowlist):
            raise RefusalError(
                Refusal(
                    RefusalCode.PATH_NOT_ALLOWED,
                    f"{label} path {dataset.path!r} is outside the allowlist",
                )
            )

    return Spec(
        version=version,
        source=source,
        target=target,
        keys=keys,
        amounts=amounts,
        group_by=group_by,
        hash_columns=hash_columns,
        tolerance=tolerance,
        sampled=sampled,
        allowlist=allowlist,
    )


def _load_mapping(text: str) -> dict:
    try:
        # SafeLoader subclass: Python object tags stay rejected, duplicate keys do not.
        loaded = yaml.load(text, Loader=_SafeUniqueKeyLoader)
    except RecursionError:
        raise SpecError("YAML is nested too deeply") from None
    except yaml.YAMLError as exc:
        detail = " ".join(str(exc).split())
        raise SpecError(f"invalid YAML: {detail}") from None
    if not isinstance(loaded, dict):
        raise SpecError("spec must be a mapping")
    return loaded


def _dataset(value: object, label: str) -> Dataset:
    mapping = _require_mapping(value, label)
    # table and snapshot_id exist only on Iceberg. Any other format, including
    # a missing or invalid one, treats them as unknown keys.
    if mapping.get("format") == "iceberg":
        allowed = ("path", "format", "table", "snapshot_id")
    else:
        allowed = ("path", "format")
    _reject_unknown(mapping, allowed, label)
    if "path" not in mapping:
        raise SpecError(f"missing {label}.path")
    if "format" not in mapping:
        raise SpecError(f"missing {label}.format")
    path = _require_text(mapping["path"], f"{label}.path")
    fmt = _require_str(mapping["format"], f"{label}.format")
    if fmt not in SUPPORTED_FORMATS:
        raise SpecError(f"unsupported {label} format {fmt!r}; use csv, parquet, or iceberg")
    table = None
    snapshot_id = None
    if fmt == "iceberg":
        if "table" not in mapping:
            raise SpecError(f"missing {label}.table")
        table = _require_table_identifier(mapping["table"], f"{label}.table")
        snapshot_id = _optional_snapshot(mapping, label)
    return Dataset(path=path, format=fmt, table=table, snapshot_id=snapshot_id)


def _require_table_identifier(value: object, label: str) -> str:
    text = _require_text(value, label)
    parts = text.split(".")
    if len(parts) < 2 or any(part == "" for part in parts):
        raise SpecError(f"{label} must be a namespace.table identifier, such as 'payments.txn'")
    for part in parts:
        if _IDENTIFIER_PART.fullmatch(part) is None:
            raise SpecError(
                f"{label} part {part!r} must start with a letter or underscore "
                "and contain only letters, digits, and underscores"
            )
    return text


def _optional_snapshot(mapping: dict, label: str) -> int | None:
    if "snapshot_id" not in mapping:
        return None
    value = _require_int(mapping["snapshot_id"], f"{label}.snapshot_id")
    if value < _SNAPSHOT_ID_MIN or value > _SNAPSHOT_ID_MAX:
        raise SpecError(f"{label}.snapshot_id must fit in a 64-bit signed integer")
    return value


def _keys(data: dict) -> tuple[str, ...] | None:
    """Return the business keys, or None when they are absent, null, or empty."""
    if "keys" not in data or data["keys"] is None:
        return None
    value = data["keys"]
    if not isinstance(value, list):
        raise SpecError("keys must be a list")
    if len(value) == 0:
        return None
    return tuple(_require_text(item, f"keys[{index}]") for index, item in enumerate(value))


def _amounts(value: object) -> tuple[Amount, ...]:
    if not isinstance(value, list):
        raise SpecError("amounts must be a list")
    amounts: list[Amount] = []
    for index, item in enumerate(value):
        label = f"amounts[{index}]"
        mapping = _require_mapping(item, label)
        _reject_unknown(mapping, ("column", "scale", "cast"), label)
        if "column" not in mapping:
            raise SpecError(f"missing {label}.column")
        if "scale" not in mapping:
            raise SpecError(f"missing {label}.scale")
        column = _require_text(mapping["column"], f"{label}.column")
        scale = _require_int(mapping["scale"], f"{label}.scale")
        if scale < 0:
            raise SpecError(f"{label}.scale must be zero or greater")
        if scale > DECIMAL_PRECISION:
            raise SpecError(f"{label}.scale must be at most {DECIMAL_PRECISION}")
        cast = _optional_cast(mapping, label)
        amounts.append(Amount(column=column, scale=scale, cast=cast))
    return tuple(amounts)


def _optional_cast(mapping: dict, label: str) -> str | None:
    if "cast" not in mapping:
        return None
    cast = _require_str(mapping["cast"], f"{label}.cast")
    if cast not in CAST_MODE_NAMES:
        allowed = ", ".join(repr(mode) for mode in CAST_MODE_NAMES)
        raise SpecError(f"{label}.cast must be one of {allowed}")
    return cast


def _sampled(value: object) -> bool:
    # A YAML boolean only. A string such as "false" must not count as false,
    # or an omitted decision could be smuggled in as text.
    if not isinstance(value, bool):
        raise SpecError("sampled must be a boolean")
    return value


def _tolerance(value: object) -> Tolerance:
    mapping = _require_mapping(value, "tolerance")
    _reject_unknown(mapping, ("value", "reason"), "tolerance")
    if "value" not in mapping:
        raise SpecError("missing tolerance.value")
    if "reason" not in mapping:
        raise SpecError("missing tolerance.reason")
    raw = mapping["value"]
    if isinstance(raw, float):
        raise SpecError(
            "tolerance.value must be a quoted decimal string; a YAML float is not accepted"
        )
    if not isinstance(raw, str):
        raise SpecError("tolerance.value must be a quoted decimal string")
    try:
        number = Decimal(raw.strip())
    except InvalidOperation:
        raise SpecError("tolerance.value is not a decimal string") from None
    if not number.is_finite() or number < 0:
        raise SpecError("tolerance.value must be a finite decimal of zero or greater")
    reason = _require_str(mapping["reason"], "tolerance.reason")
    return Tolerance(value=number, reason=reason)


def _name_list(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise SpecError(f"{label} must be a list")
    return tuple(_require_text(item, f"{label}[{index}]") for index, item in enumerate(value))


def _require_mapping(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        raise SpecError(f"{label} must be a mapping")
    return value


def _require_str(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise SpecError(f"{label} must be a string")
    return value


def _require_text(value: object, label: str) -> str:
    text = _require_str(value, label)
    if text.strip() == "":
        raise SpecError(f"{label} must be a non-empty string")
    if "\n" in text or "\r" in text:
        raise SpecError(f"{label} must be a single line")
    return text


def _require_int(value: object, label: str) -> int:
    # bool is a subclass of int; a YAML boolean is not a scale or a version.
    if isinstance(value, bool) or not isinstance(value, int):
        raise SpecError(f"{label} must be an integer")
    return value


def _reject_unknown(mapping: dict, allowed: tuple[str, ...], label: str) -> None:
    unknown = sorted(str(key) for key in mapping if key not in allowed)
    if not unknown:
        return
    rendered = ", ".join(repr(key) for key in unknown)
    if label == "spec":
        noun = "key" if len(unknown) == 1 else "keys"
        raise SpecError(f"unknown {noun}: {rendered}")
    noun = "key" if len(unknown) == 1 else "keys"
    raise SpecError(f"{label} has unknown {noun}: {rendered}")


def _require_location(path: str, label: str) -> _LocalPath | _S3Path:
    if path.startswith(_S3_SCHEME):
        return _parse_s3(path, label)
    scheme_at = path.find("://")
    slash_at = path.find("/")
    if scheme_at != -1 and (slash_at == -1 or scheme_at < slash_at):
        scheme = path[:scheme_at]
        raise SpecError(f"{label} uses unsupported scheme {scheme!r}; use a local path or s3://")
    if path.startswith("s3:"):
        raise SpecError(f"{label} is not a valid s3:// URI")
    return _parse_local(path)


def _parse_local(path: str) -> _LocalPath:
    absolute = path.startswith("/")
    parts: list[str] = []
    for piece in path.split("/"):
        if piece in ("", "."):
            continue
        if piece == "..":
            if absolute:
                if parts:
                    parts.pop()
            elif parts and parts[-1] != "..":
                parts.pop()
            else:
                parts.append("..")
            continue
        parts.append(piece)
    return _LocalPath(absolute=absolute, parts=tuple(parts))


def _require_allowlist_entry(path: str, label: str) -> None:
    location = _require_location(path, label)
    if not isinstance(location, _LocalPath):
        return
    if location.absolute and not location.parts:
        raise SpecError(f"{label} must not be the filesystem root")
    if not location.parts:
        raise SpecError(f"{label} must not be an empty path")
    if ".." in location.parts:
        raise SpecError(f"{label} must not contain '..'")


def _parse_s3(path: str, label: str) -> _S3Path:
    # Key segments are literal. "." , ".." , and empty segments are not folded,
    # and a query or fragment is not part of the key.
    if "?" in path or "#" in path:
        raise SpecError(f"{label} must not include a query or fragment")
    rest = path[len(_S3_SCHEME) :]
    bucket, separator, key = rest.partition("/")
    if bucket == "" or bucket in {".", ".."}:
        raise SpecError(f"{label} is missing an s3 bucket")
    if not separator:
        return _S3Path(bucket=bucket, parts=())
    parts: list[str] = []
    for piece in key.split("/"):
        if piece == "":
            raise SpecError(f"{label} has an empty key segment")
        if piece in {".", ".."}:
            raise SpecError(f"{label} has a {piece!r} key segment")
        parts.append(piece)
    return _S3Path(bucket=bucket, parts=tuple(parts))


def _path_allowed(path: str, allowlist: tuple[str, ...]) -> bool:
    location = _require_location(path, "path")
    for entry in allowlist:
        if _covers(_require_location(entry, "allowlist"), location):
            return True
    return False


def _covers(prefix: _LocalPath | _S3Path, path: _LocalPath | _S3Path) -> bool:
    if isinstance(prefix, _S3Path) and isinstance(path, _S3Path):
        if prefix.bucket != path.bucket:
            return False
        return _parts_cover(prefix.parts, path.parts)
    if isinstance(prefix, _LocalPath) and isinstance(path, _LocalPath):
        if prefix.absolute != path.absolute:
            return False
        return _parts_cover(prefix.parts, path.parts)
    return False


def _parts_cover(prefix: tuple[str, ...], path: tuple[str, ...]) -> bool:
    """True when every prefix component equals the same whole path component.

    String prefixes are not used, so "data" does not cover "database". A path
    that still contains ".." after the prefix has climbed out of it: an empty
    prefix would otherwise accept "../secret", and a normalised escape would
    match a shorter entry.
    """
    if len(path) < len(prefix):
        return False
    if path[: len(prefix)] != prefix:
        return False
    return ".." not in path[len(prefix) :]
