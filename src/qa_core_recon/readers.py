"""Read source and target datasets for a recon spec.

CSV, Parquet, and Iceberg on local disk only. S3 is a later PR. Every path is
checked with the spec allowlist before it is opened. The check uses the path
text and does not resolve symlinks.

CSV is read with an explicit string schema and ``inferSchema`` off. Amount
columns are then parsed by :func:`qa_core_recon.decimals.to_decimal`. Parquet
and Iceberg keep the file schema. ``mergeSchema`` is off for Parquet. Iceberg
is read through a Hadoop catalog on the warehouse in ``path``. The current
snapshot is read, or ``snapshot_id`` when the spec sets one.

``check_data`` refuses, in this order:

1. ``sample-only`` when ``sampled`` is true, before either dataset is opened.
2. ``unmapped-column`` when a key, amount, ``group_by``, or hash column is
   missing. Source is checked before target. On one side the roles are keys,
   amounts, ``group_by``, then hash columns, in spec order. A repeated name
   is checked once, at its first role.
3. ``float-amount`` when a Parquet or Iceberg amount column is float or double
   and that amount has no cast rule. Source, then target, then spec order.
4. ``inexact-amount`` when a value is not an exact decimal at the spec scale,
   or the column type is not a number type. The reported cell is the
   lexicographically smallest offending text in that column. The scan runs
   inside Spark and only that one value is returned to the driver.

``path-not-allowed`` is re-checked before those reads. An allowlisted ``s3://``
path raises :class:`ReadError` instead, because the allowlist already passed.
"""

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import min as spark_min
from pyspark.sql.types import (
    ByteType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

from qa_core_recon.decimals import DECIMAL_PRECISION, InexactAmount, to_decimal
from qa_core_recon.refusals import Refusal, RefusalCode, RefusalError
from qa_core_recon.spark import hadoop_catalog
from qa_core_recon.spec import Amount, Dataset, Spec, _path_allowed

_MISSING_TABLE_MARKERS = (
    "NoSuchTableException",
    "Table does not exist",
    "TABLE_OR_VIEW_NOT_FOUND",
    "Table or view not found",
)

_INTEGER_TYPES = (ByteType, ShortType, IntegerType, LongType)


class ReadError(ValueError):
    """The dataset could not be opened. This is not a comparison result."""


def check_before_session(spec: Spec) -> None:
    """Raise sample-only or the S3 read error before a JVM is started."""
    if spec.sampled:
        raise RefusalError(
            Refusal(
                RefusalCode.SAMPLE_ONLY,
                "the spec declares sampling, so a full recon cannot be claimed",
            )
        )
    _reject_remote_or_unlisted(spec)


def check_data(spark: SparkSession, spec: Spec) -> None:
    """Open both datasets and raise the first data-level refusal."""
    _prepare(spark, spec)


def read_datasets(spark: SparkSession, spec: Spec) -> tuple[DataFrame, DataFrame]:
    """Return source and target with amount columns typed as decimals.

    Runs the same refusals as :func:`check_data`. The frames are not compared.
    """
    source, target = _prepare(spark, spec)
    return _apply_amounts(source, spec), _apply_amounts(target, spec)


def _prepare(spark: SparkSession, spec: Spec) -> tuple[DataFrame, DataFrame]:
    check_before_session(spec)
    source = _open(spark, spec.source, "source")
    target = _open(spark, spec.target, "target")
    _refuse_unmapped(source, "source", spec)
    _refuse_unmapped(target, "target", spec)
    _refuse_float(source, "source", spec)
    _refuse_float(target, "target", spec)
    _refuse_inexact(source, "source", spec)
    _refuse_inexact(target, "target", spec)
    return source, target


def _reject_remote_or_unlisted(spec: Spec) -> None:
    for side, dataset in (("source", spec.source), ("target", spec.target)):
        if not _path_allowed(dataset.path, spec.allowlist):
            raise RefusalError(
                Refusal(
                    RefusalCode.PATH_NOT_ALLOWED,
                    f"{side} path {dataset.path!r} is outside the allowlist",
                )
            )
    for side, dataset in (("source", spec.source), ("target", spec.target)):
        if dataset.path.startswith("s3://"):
            raise ReadError(f"s3 reads are not supported yet: {side} path {dataset.path!r}")


def _refuse_unmapped(df: DataFrame, side: str, spec: Spec) -> None:
    present = set(df.columns)
    for name in _named_columns(spec):
        if name not in present:
            raise RefusalError(
                Refusal(
                    RefusalCode.UNMAPPED_COLUMN,
                    f"{side} is missing column {name!r}",
                )
            )


def _refuse_float(df: DataFrame, side: str, spec: Spec) -> None:
    fields = _field_map(df)
    for amount in spec.amounts:
        dtype = fields.get(amount.column)
        if dtype is None or amount.cast is not None:
            continue
        if isinstance(dtype, (FloatType, DoubleType)):
            raise RefusalError(
                Refusal(
                    RefusalCode.FLOAT_AMOUNT,
                    (
                        f"{side} amount column {amount.column!r} is {dtype.simpleString()} "
                        "and the spec has no cast rule"
                    ),
                )
            )


def _refuse_inexact(df: DataFrame, side: str, spec: Spec) -> None:
    fields = _field_map(df)
    for amount in spec.amounts:
        dtype = fields.get(amount.column)
        if dtype is None:
            continue
        kind = _amount_kind(dtype)
        if kind == "other":
            raise RefusalError(
                Refusal(
                    RefusalCode.INEXACT_AMOUNT,
                    (
                        f"{side} amount column {amount.column!r} has type "
                        f"{dtype.simpleString()}, which is not a number type"
                    ),
                )
            )
        if kind == "null":
            continue
        display = _first_inexact(df, amount, kind, side, spec)
        if display is not None:
            raise RefusalError(
                Refusal(
                    RefusalCode.INEXACT_AMOUNT,
                    (
                        f"{side} amount column {amount.column!r} value {display!r} "
                        f"is not an exact decimal at scale {amount.scale}"
                    ),
                )
            )


def _bad_text(scale: int, cast: str | None):
    """Return the clipped text of a bad cell, or null when the cell is exact."""

    def parse(value: object) -> str | None:
        try:
            to_decimal(value, scale, cast)
        except InexactAmount as exc:
            return exc.display
        return None

    return udf(parse, StringType())


def _first_inexact(df: DataFrame, amount: Amount, kind: str, side: str, spec: Spec) -> str | None:
    use_cast = amount.cast if kind == "float" else None
    marker = _bad_text(amount.scale, use_cast)(_column(amount.column))
    dataset = spec.source if side == "source" else spec.target
    try:
        # min ignores nulls, so only the smallest bad text reaches the driver.
        return df.select(marker.alias("bad")).agg(spark_min("bad")).first()[0]
    except Exception:
        raise ReadError(f"cannot read {side} as {dataset.format}") from None


def _apply_amounts(df: DataFrame, spec: Spec) -> DataFrame:
    fields = _field_map(df)
    for amount in spec.amounts:
        dtype = fields[amount.column]
        kind = _amount_kind(dtype)
        if kind == "null":
            typed = col(_quote(amount.column)).cast(DecimalType(DECIMAL_PRECISION, amount.scale))
            df = df.withColumn(amount.column, typed)
            continue
        use_cast = amount.cast if kind == "float" else None
        parser = _parser(amount.scale, use_cast)
        df = df.withColumn(amount.column, parser(_column(amount.column)))
    return df


def _parser(scale: int, cast: str | None):
    def parse(value: object):
        return to_decimal(value, scale, cast)

    return udf(parse, DecimalType(DECIMAL_PRECISION, scale))


def _named_columns(spec: Spec) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    names = (
        *spec.keys,
        *(amount.column for amount in spec.amounts),
        *spec.group_by,
        *spec.hash_columns,
    )
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return tuple(ordered)


def _field_map(df: DataFrame) -> dict[str, DataType]:
    fields: dict[str, DataType] = {}
    for field in df.schema.fields:
        if field.name not in fields:
            fields[field.name] = field.dataType
    return fields


def _amount_kind(dtype: DataType) -> str:
    if isinstance(dtype, (FloatType, DoubleType)):
        return "float"
    if isinstance(dtype, StringType):
        return "string"
    if isinstance(dtype, (DecimalType, *_INTEGER_TYPES)):
        return "exact"
    if isinstance(dtype, NullType):
        return "null"
    return "other"


def _open(spark: SparkSession, dataset: Dataset, side: str) -> DataFrame:
    if dataset.format == "iceberg":
        return _read_iceberg(spark, dataset, side)
    location = _require_local(dataset.path, side)
    try:
        if dataset.format == "csv":
            frame = _read_csv(spark, location, side)
            _parse_all_csv_rows(frame, side)
        elif dataset.format == "parquet":
            frame = spark.read.option("mergeSchema", "false").parquet(dataset.path)
        else:
            raise ReadError(f"cannot read {side} as {dataset.format}")
        # Touch the schema so a corrupt file fails here, with a stable message.
        if not isinstance(frame.schema, StructType):
            raise ReadError(f"cannot read {side} as {dataset.format}")
    except ReadError:
        raise
    except Exception:
        raise ReadError(f"cannot read {side} as {dataset.format}") from None
    return frame


def _read_iceberg(spark: SparkSession, dataset: Dataset, side: str) -> DataFrame:
    _require_warehouse(dataset.path, side)
    if not dataset.table:
        raise ReadError(f"cannot read {side} as iceberg")
    try:
        catalog = hadoop_catalog(spark, dataset.path)
        reader = spark.read.format("iceberg")
        if dataset.snapshot_id is not None:
            reader = reader.option("snapshot-id", str(dataset.snapshot_id))
        frame = reader.load(f"{catalog}.{dataset.table}")
        # load() resolves snapshot-id on Iceberg 1.7.1, so a missing snapshot
        # fails here. Touching the schema still rejects a frame that is not a table.
        if not isinstance(frame.schema, StructType):
            raise ReadError(f"cannot read {side} as iceberg")
    except ReadError:
        raise
    except Exception as exc:
        raise _iceberg_read_error(exc, dataset, side) from None
    return frame


def _require_warehouse(path: str, side: str) -> Path:
    location = _require_local(path, side)
    if not location.is_dir():
        raise ReadError(f"{side} iceberg warehouse is not a directory: {path}")
    return location


def _iceberg_read_error(exc: BaseException, dataset: Dataset, side: str) -> ReadError:
    text = _exception_text(exc)
    # A negative id is rewritten as a metadata table name, snapshot_id_-1, and
    # Spark then reports that name as a missing table. It is still a snapshot miss.
    snapshot_marker = f"snapshot_id_{dataset.snapshot_id}"
    if dataset.snapshot_id is not None and (
        "Cannot find snapshot" in text or snapshot_marker in text
    ):
        return ReadError(
            f"{side} iceberg snapshot {dataset.snapshot_id} was not found "
            f"for table {dataset.table!r}"
        )
    if any(marker in text for marker in _MISSING_TABLE_MARKERS):
        return ReadError(f"{side} iceberg table {dataset.table!r} was not found in {dataset.path}")
    return ReadError(f"cannot read {side} as iceberg")


def _exception_text(exc: BaseException) -> str:
    parts = [exc.__class__.__name__, str(exc)]
    java = getattr(exc, "java_exception", None)
    if java is not None:
        try:
            parts.append(str(java.getClass().getName()))
        except Exception:
            pass
        parts.append(str(java))
    return "\n".join(parts)


def _require_local(path: str, side: str) -> Path:
    location = Path(path)
    try:
        exists = location.exists()
        kind = "file" if location.is_file() else "dir" if location.is_dir() else "other"
    except OSError as exc:
        detail = exc.strerror or exc.__class__.__name__
        raise ReadError(f"cannot read {side} path {path!r}: {detail}") from None
    if not exists:
        raise ReadError(f"{side} path not found: {path}")
    if kind == "other":
        raise ReadError(f"{side} path is not a file or directory: {path}")
    return location


def _read_csv(spark: SparkSession, location: Path, side: str) -> DataFrame:
    files = _csv_files(location, side)
    names = _header_names(_first_line(files[0], side), side)
    for extra in files[1:]:
        other = _header_names(_first_line(extra, side), side)
        if other != names:
            raise ReadError(f"{side} csv header in {extra.name!r} does not match {files[0].name!r}")
    schema = StructType([StructField(name, StringType(), True) for name in names])
    return (
        spark.read.schema(schema)
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .csv([str(item) for item in files])
    )


def _parse_all_csv_rows(df: DataFrame, side: str) -> None:
    """Parse every field so a short or broken row cannot be skipped."""
    columns = [_column(name) for name in df.columns]
    if not columns:
        return
    try:
        df.select(*columns).count()
    except Exception:
        raise ReadError(f"{side} csv has a malformed row") from None


def _csv_files(location: Path, side: str) -> list[Path]:
    if location.is_file():
        return [location]
    files = [
        child
        for child in location.iterdir()
        if child.is_file() and not child.name.startswith(".") and not child.name.startswith("_")
    ]
    files.sort(key=lambda child: child.name)
    if not files:
        raise ReadError(f"{side} csv has no data files")
    return files


def _first_line(path: Path, side: str) -> str:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            line = handle.readline()
    except UnicodeDecodeError:
        raise ReadError(f"{side} csv is not valid UTF-8") from None
    except OSError as exc:
        detail = exc.strerror or exc.__class__.__name__
        raise ReadError(f"cannot read {side} path {str(path)!r}: {detail}") from None
    if line == "":
        raise ReadError(f"{side} csv is empty")
    return line


def _header_names(line: str, side: str) -> list[str]:
    try:
        row = next(csv.reader([line]))
    except csv.Error:
        raise ReadError(f"{side} csv header is invalid") from None
    seen: set[str] = set()
    for name in row:
        if name == "":
            raise ReadError(f"{side} csv has an empty column name")
        if name in seen:
            raise ReadError(f"{side} csv has a duplicate column {name!r}")
        seen.add(name)
    if not row:
        raise ReadError(f"{side} csv is empty")
    return row


def _column(name: str):
    return col(_quote(name))


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"
