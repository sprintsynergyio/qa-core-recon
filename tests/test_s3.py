"""S3 reads through moto. No AWS account and no calls to AWS."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import DecimalType, DoubleType, StringType, StructField, StructType

from qa_core_recon.cli import main
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.readers import (
    ReadError,
    _classify_s3_error,
    _read_csv_s3,
    check_data,
    read_datasets,
)
from qa_core_recon.refusals import RefusalCode, RefusalError
from qa_core_recon.spark import (
    _holds_credential,
    apply_s3_config,
    s3_hadoop_settings,
    scrub_s3_credentials,
)
from qa_core_recon.spec import to_s3a
from tests.support import parse_data, spec_data, write_csv, write_spec_file

CSV = "txn_id,amount,day,note\n1,10.50,2024-01-01,a\n"
OTHER = "txn_id,amount,day,note\n1,99.00,2024-01-01,b\n"
TABLE = "payments.txn"


def _client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["QA_CORE_RECON_S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _bucket(label: str) -> str:
    name = f"recon-{label}-{uuid.uuid4().hex[:8]}"
    _client().create_bucket(Bucket=name)
    return name


def _put_csv(bucket: str, key: str, text: str) -> None:
    _put_bytes(bucket, key, text.encode("utf-8"))


def _put_bytes(bucket: str, key: str, body: bytes) -> None:
    _client().put_object(Bucket=bucket, Key=key, Body=body)


def _spec(tmp_path: Path, source: dict, target: dict, allow: list[str]) -> dict:
    data = spec_data(tmp_path, tmp_path / "unused-source", tmp_path / "unused-target")
    data["source"] = source
    data["target"] = target
    data["allowlist"] = allow
    return data


def _local_csv(tmp_path: Path, name: str, text: str = OTHER) -> Path:
    path = tmp_path / name
    write_csv(path, text.split("\n", 1)[1], header=text.split("\n", 1)[0] + "\n")
    return path


def _assert_data_ok(tmp_path: Path, data: dict, capsys) -> None:
    spec = write_spec_file(tmp_path, data)
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    captured = capsys.readouterr()
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    assert captured.out == "data ok\n"
    assert captured.err == ""
    assert secret not in captured.out
    assert secret not in captured.err


def _schema(amount_type):
    return StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", amount_type, True),
            StructField("day", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )


def _write_parquet(spark, path: Path, rows, amount_type) -> None:
    spark.createDataFrame(rows, _schema(amount_type)).write.mode("overwrite").parquet(str(path))


def _upload_parquet(spark, bucket: str, prefix: str, rows, amount_type) -> None:
    directory = Path(tempfile.mkdtemp(prefix="qa-core-recon-parquet-"))
    try:
        _write_parquet(spark, directory, rows, amount_type)
        client = _client()
        for path in directory.rglob("*"):
            if not path.is_file():
                continue
            if path.name.startswith(".") or path.name.startswith("_"):
                continue
            relative = path.relative_to(directory).as_posix()
            client.upload_file(str(path), bucket, f"{prefix}/{relative}")
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def _write_iceberg(spark, bucket: str, rows, schema) -> None:
    from qa_core_recon.spark import hadoop_catalog, scrub_s3_credentials

    warehouse = to_s3a(f"s3://{bucket}/warehouse")
    catalog = hadoop_catalog(spark, warehouse)
    try:
        spark.createDataFrame(rows, schema).writeTo(f"{catalog}.{TABLE}").createOrReplace()
    finally:
        scrub_s3_credentials(spark)


def _write_iceberg_with_s3_scheme(spark, bucket: str, rows, schema) -> None:
    """Write a table the way Glue does: metadata and data paths stay ``s3://``."""
    from qa_core_recon.spark import apply_s3_config, hadoop_catalog, scrub_s3_credentials

    apply_s3_config(spark)
    warehouse = f"s3://{bucket}/warehouse"
    catalog = hadoop_catalog(spark, warehouse)
    try:
        spark.createDataFrame(rows, schema).writeTo(f"{catalog}.{TABLE}").createOrReplace()
    finally:
        scrub_s3_credentials(spark)


def _iceberg_metadata_text(bucket: str) -> str:
    client = _client()
    names: list[str] = []
    token = None
    while True:
        kwargs: dict = {"Bucket": bucket}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        names.extend(item["Key"] for item in page.get("Contents", []))
        if not page.get("IsTruncated"):
            break
        token = page["NextContinuationToken"]
    metadata = [name for name in names if name.endswith(".metadata.json")]
    assert metadata, names
    body = client.get_object(Bucket=bucket, Key=metadata[0])["Body"].read()
    return body.decode("utf-8")


def test_check_data_csv_on_s3_prints_data_ok(spark, tmp_path, capsys):
    bucket = _bucket("csv")
    _put_csv(bucket, "raw/a.csv", CSV)
    _put_csv(bucket, "raw/b.csv", OTHER)
    _put_csv(bucket, "raw/_SUCCESS", "not,a,header\n")
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw", "format": "csv"},
        {"path": f"s3://{bucket}/raw", "format": "csv"},
        [f"s3://{bucket}"],
    )
    # Two part files share a header. The marker file must not be read as one.
    data["source"] = {"path": f"s3://{bucket}/raw/a.csv", "format": "csv"}
    data["target"] = {"path": f"s3://{bucket}/raw/b.csv", "format": "csv"}
    _assert_data_ok(tmp_path, data, capsys)

    directory = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw", "format": "csv"},
        {"path": f"s3://{bucket}/raw/b.csv", "format": "csv"},
        [f"s3://{bucket}/raw/"],
    )
    _assert_data_ok(tmp_path, directory, capsys)


def test_check_data_parquet_on_s3_prints_data_ok(spark, tmp_path, capsys):
    bucket = _bucket("parquet")
    _upload_parquet(
        spark,
        bucket,
        "payments",
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        DecimalType(10, 2),
    )
    source = _local_csv(tmp_path, "source.csv", OTHER)
    data = _spec(
        tmp_path,
        {"path": str(source), "format": "csv"},
        {"path": f"s3://{bucket}/payments", "format": "parquet"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    _assert_data_ok(tmp_path, data, capsys)


def test_check_data_iceberg_on_s3_prints_data_ok(spark, tmp_path, capsys):
    bucket = _bucket("iceberg")
    _write_iceberg(
        spark,
        bucket,
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        _schema(DecimalType(10, 2)),
    )
    source = _local_csv(tmp_path, "source.csv", OTHER)
    data = _spec(
        tmp_path,
        {"path": str(source), "format": "csv"},
        {"path": f"s3://{bucket}/warehouse", "format": "iceberg", "table": TABLE},
        [str(tmp_path), f"s3://{bucket}"],
    )
    _assert_data_ok(tmp_path, data, capsys)


def test_check_data_s3_csv_against_local_parquet_prints_data_ok(spark, tmp_path, capsys):
    bucket = _bucket("mixed")
    _put_csv(bucket, "raw/payments.csv", CSV)
    target = tmp_path / "target"
    _write_parquet(
        spark,
        target,
        [("1", Decimal("99.00"), "2024-01-01", "b")],
        DecimalType(10, 2),
    )
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": str(target), "format": "parquet"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    _assert_data_ok(tmp_path, data, capsys)


def test_trailing_slash_path_and_allowlist_read(spark, tmp_path, capsys):
    bucket = _bucket("slash")
    _put_csv(bucket, "curated/payments.csv", CSV)
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/curated/payments.csv/", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}/curated/"],
    )
    loaded = parse_data(data)
    assert loaded.source.path.endswith("/")
    assert to_s3a(loaded.source.path) == f"s3a://{bucket}/curated/payments.csv"
    _assert_data_ok(tmp_path, data, capsys)


def test_s3_path_outside_the_allowlist_is_refused_before_read(tmp_path, capsys):
    source = _local_csv(tmp_path, "source.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": str(source), "format": "csv"},
        {"path": "s3://other-bucket/file.parquet", "format": "parquet"},
        [str(tmp_path), "s3://demo-bucket"],
    )
    spec = write_spec_file(tmp_path, data)
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert "path-not-allowed" in captured.err
    assert "s3://other-bucket/file.parquet" in captured.err
    assert "not found" not in captured.err
    assert "Traceback" not in captured.err


def test_missing_s3_key_names_the_side_and_path(spark, tmp_path):
    bucket = _bucket("missing-key")
    target = _local_csv(tmp_path, "target.csv", CSV)
    path = f"s3://{bucket}/raw/missing.csv"
    data = _spec(
        tmp_path,
        {"path": path, "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == f"source path not found: {path}"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] not in str(caught.value)


def test_missing_s3_bucket_names_the_side_and_path(spark, tmp_path):
    target = _local_csv(tmp_path, "target.csv", CSV)
    path = "s3://recon-absent-bucket/raw/file.csv"
    data = _spec(
        tmp_path,
        {"path": path, "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), "s3://recon-absent-bucket"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == f"source bucket not found: {path}"


def test_s3_refusals_cover_unmapped_float_and_inexact(spark, tmp_path):
    bucket = _bucket("refusals")
    _put_csv(bucket, "raw/short.csv", "txn_id,amount,day\n1,10.50,2024-01-01\n")
    target = _local_csv(tmp_path, "target.csv", CSV)
    unmapped = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/short.csv", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(RefusalError) as caught:
        check_data(spark, parse_data(unmapped))
    assert caught.value.refusal.code is RefusalCode.UNMAPPED_COLUMN
    assert caught.value.refusal.message == "source is missing column 'note'"

    _upload_parquet(spark, bucket, "doubled", [("1", 2.5, "2024-01-01", "a")], DoubleType())
    floated = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/doubled", "format": "parquet"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(RefusalError) as caught:
        check_data(spark, parse_data(floated))
    assert caught.value.refusal.code is RefusalCode.FLOAT_AMOUNT
    assert caught.value.refusal.message.startswith("source amount column 'amount' is double")

    _put_csv(
        bucket,
        "raw/inexact.csv",
        "txn_id,amount,day,note\n1,10.005,2024-01-01,a\n9,9.001,2024-01-01,b\n",
    )
    inexact = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/inexact.csv", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(RefusalError) as caught:
        check_data(spark, parse_data(inexact))
    assert caught.value.refusal.code is RefusalCode.INEXACT_AMOUNT
    assert "value '10.005'" in caught.value.refusal.message
    assert "9.001" not in caught.value.refusal.message


def test_s3_error_lines_name_the_side_and_hide_secrets():
    secret = "qa-core-recon-test-secret-key"
    path = "s3://recon-bucket/raw/payments.csv"
    denied = RuntimeError(
        "AmazonS3Exception: Access Denied (Status Code: 403; Error Code: AccessDenied) " + secret
    )
    unreachable = RuntimeError(
        "SdkClientException: Unable to execute HTTP request: Connection refused " + secret
    )
    missing = RuntimeError("java.io.FileNotFoundException: No such file or directory")
    absent = RuntimeError("Error Code: NoSuchBucket; Status Code: 404; " + secret)
    creds = RuntimeError("Unable to load AWS credentials from any provider in the chain " + secret)
    wrapped = RuntimeError(
        "java.nio.file.AccessDeniedException: "
        "org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: No AWS Credentials provided"
    )
    lines = (
        _classify_s3_error(denied, "source", path, missing_key=True),
        _classify_s3_error(unreachable, "target", path, missing_key=True),
        _classify_s3_error(missing, "source", path, missing_key=True),
        _classify_s3_error(absent, "target", path, missing_key=True),
        _classify_s3_error(creds, "source", path, missing_key=True),
        _classify_s3_error(wrapped, "source", path, missing_key=True),
    )
    assert lines == (
        f"access denied reading source path {path}",
        f"cannot reach S3 for target path {path}",
        f"source path not found: {path}",
        f"target bucket not found: {path}",
        f"no AWS credentials found for source path {path}",
        f"no AWS credentials found for source path {path}",
    )
    assert all(secret not in line for line in lines)


def test_unreachable_endpoint_is_a_read_error(spark, tmp_path):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    bucket = "recon-closed-port"
    prefix = f"fs.s3a.bucket.{bucket}."
    conf.set(prefix + "endpoint", "http://127.0.0.1:9")
    conf.set(prefix + "path.style.access", "true")
    conf.set(prefix + "connection.ssl.enabled", "false")
    conf.set(prefix + "connection.establish.timeout", "1000")
    conf.set(prefix + "connection.timeout", "1000")
    conf.set(prefix + "attempts.maximum", "1")
    target = _local_csv(tmp_path, "target.csv", CSV)
    path = f"s3://{bucket}/raw/file.csv"
    data = _spec(
        tmp_path,
        {"path": path, "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == f"cannot reach S3 for source path {path}"


def test_s3_settings_follow_the_endpoint_and_region(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "qa-core-recon-test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "qa-core-recon-test-secret-key")
    monkeypatch.setenv("QA_CORE_RECON_S3_ENDPOINT", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    settings = s3_hadoop_settings()
    blob = " ".join(settings.values())
    assert "qa-core-recon-test-secret-key" not in blob
    assert "qa-core-recon-test-access-key" not in blob
    assert settings["fs.s3.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert settings["fs.s3a.endpoint"] == "http://127.0.0.1:9"
    assert settings["fs.s3a.path.style.access"] == "true"
    assert settings["fs.s3a.connection.ssl.enabled"] == "false"
    assert settings["fs.s3a.endpoint.region"] == "eu-west-1"
    assert settings["fs.s3a.retry.limit"] == "1"
    assert settings["fs.s3a.retry.interval"] == "200ms"
    assert "DefaultAWSCredentialsProviderChain" in settings["fs.s3a.aws.credentials.provider"]

    monkeypatch.setenv("QA_CORE_RECON_S3_ENDPOINT", "https://store.example.test")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    https = s3_hadoop_settings()
    assert https["fs.s3a.connection.ssl.enabled"] == "true"
    assert https["fs.s3a.path.style.access"] == "true"
    assert https["fs.s3a.endpoint.region"] == "us-east-1"

    monkeypatch.delenv("QA_CORE_RECON_S3_ENDPOINT", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    plain = s3_hadoop_settings()
    assert "fs.s3a.endpoint" not in plain
    assert "fs.s3a.path.style.access" not in plain
    assert "fs.s3a.retry.limit" not in plain
    assert "fs.s3a.retry.interval" not in plain
    assert plain["fs.s3a.endpoint.region"] == "eu-central-1"


def test_live_s3_config_has_the_endpoint_and_no_credentials(spark):
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    access = os.environ["AWS_ACCESS_KEY_ID"]
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    assert conf.get("fs.s3a.endpoint") == os.environ["QA_CORE_RECON_S3_ENDPOINT"]
    assert conf.get("fs.s3a.path.style.access") == "true"
    assert conf.get("fs.s3a.connection.ssl.enabled") == "false"
    assert conf.get("fs.s3a.endpoint.region") == "us-east-1"
    assert (
        conf.get("fs.s3a.aws.credentials.provider")
        == "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    for name in ("fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token"):
        assert conf.get(name) in (None, "")
    iterator = conf.iterator()
    while iterator.hasNext():
        entry = iterator.next()
        key = str(entry.getKey())
        value = entry.getValue()
        text = "" if value is None else str(value)
        if secret in text or access in text:
            raise AssertionError(f"credential stored in hadoop key {key}")
    for key, value in spark.sparkContext.getConf().getAll():
        if secret in value or access in value:
            raise AssertionError(f"credential stored in spark key {key}")


def test_cli_requests_the_s3_runtime_only_when_a_path_is_s3(monkeypatch, tmp_path, capsys):
    calls = []

    def fake_session(*_args, **kwargs):
        calls.append((kwargs.get("iceberg"), kwargs.get("s3")))
        return None

    monkeypatch.setattr("qa_core_recon.spark.local_session", fake_session)
    monkeypatch.setattr("qa_core_recon.spark.local_session_active", lambda: False)
    monkeypatch.setattr("qa_core_recon.spark.stop_local_session", lambda: None)
    monkeypatch.setattr("qa_core_recon.readers.check_data", lambda *_args, **_kwargs: None)

    local = spec_data(
        tmp_path, tmp_path / "source.csv", tmp_path / "target.csv", target_format="csv"
    )
    spec = write_spec_file(tmp_path, local)
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    assert calls == [(False, False)]

    remote = spec_data(tmp_path, tmp_path / "source.csv", tmp_path / "unused.csv")
    remote["target"] = {"path": "s3://demo-bucket/curated/payments", "format": "parquet"}
    remote["allowlist"] = [str(tmp_path), "s3://demo-bucket"]
    calls.clear()
    spec = write_spec_file(tmp_path, remote)
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    assert calls == [(False, True)]

    iceberg = spec_data(
        tmp_path,
        tmp_path / "source.csv",
        tmp_path / "warehouse",
        target_format="iceberg",
        target_table=TABLE,
    )
    iceberg["target"]["path"] = "s3://demo-bucket/warehouse"
    iceberg["allowlist"] = [str(tmp_path), "s3://demo-bucket"]
    calls.clear()
    spec = write_spec_file(tmp_path, iceberg)
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    assert calls == [(True, True)]
    assert capsys.readouterr().out == "data ok\ndata ok\ndata ok\n"


def test_s3_glob_path_does_not_read(spark, tmp_path, capsys):
    bucket = _bucket("glob")
    _upload_parquet(
        spark,
        bucket,
        "pq/east",
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        DecimalType(10, 2),
    )
    _upload_parquet(
        spark,
        bucket,
        "pq/west",
        [("1", Decimal("11.00"), "2024-01-01", "b")],
        DecimalType(10, 2),
    )
    source = _local_csv(tmp_path, "source.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": str(source), "format": "csv"},
        {"path": f"s3://{bucket}/pq/*", "format": "parquet"},
        [str(tmp_path), f"s3://{bucket}/pq"],
    )
    spec = write_spec_file(tmp_path, data)
    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert "contains glob character '*'" in captured.err
    assert "data ok" not in captured.out


def test_missing_parquet_prefix_names_the_target_path(spark, tmp_path):
    bucket = _bucket("missing-pq")
    source = _local_csv(tmp_path, "source.csv", CSV)
    path = f"s3://{bucket}/pq/absent"
    data = _spec(
        tmp_path,
        {"path": str(source), "format": "csv"},
        {"path": path, "format": "parquet"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == f"target path not found: {path}"


def test_s3_csv_malformed_row_is_a_read_error(spark, tmp_path):
    bucket = _bucket("bad-row")
    _put_csv(bucket, "raw/payments.csv", "txn_id,amount,day,note\n1,10.50\n")
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == "source csv has a malformed row"


def test_s3_csv_header_bom_is_stripped(spark, tmp_path):
    bucket = _bucket("bom")
    _put_bytes(bucket, "raw/payments.csv", b"\xef\xbb\xbf" + CSV.encode("utf-8"))
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    loaded, _other = read_datasets(spark, parse_data(data))
    assert "txn_id" in loaded.columns
    assert all("\ufeff" not in name for name in loaded.columns)


def test_s3_csv_header_that_is_not_utf8_is_a_read_error(spark, tmp_path):
    bucket = _bucket("utf8")
    _put_bytes(bucket, "raw/payments.csv", b"\xff\xfetxn_id,amount\n")
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == "source csv is not valid UTF-8"


def test_s3_csv_files_with_different_headers_are_refused(spark, tmp_path):
    bucket = _bucket("headers")
    _put_csv(bucket, "dir/a.csv", CSV)
    _put_csv(bucket, "dir/b.csv", "txn_id,amount,day,other\n1,10.50,2024-01-01,a\n")
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/dir", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == "source csv header in 'b.csv' does not match 'a.csv'"


def test_s3_csv_object_name_with_a_glob_character_is_a_read_error(spark, tmp_path):
    bucket = _bucket("brackets")
    _put_csv(bucket, "dir/a[1].csv", CSV)
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/dir", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == "source csv object 'a[1].csv' contains glob character '['"


def test_s3_csv_files_are_compared_in_sorted_name_order(spark, monkeypatch):
    class _Path:
        def __init__(self, name: str) -> None:
            self._name = name

        def getName(self) -> str:
            return self._name

        def toString(self) -> str:
            return f"s3a://bucket/dir/{self._name}"

    class _Status:
        def __init__(self, name: str, *, is_file: bool = True) -> None:
            self._path = _Path(name)
            self._file = is_file

        def isFile(self) -> bool:
            return self._file

        def getPath(self) -> _Path:
            return self._path

    class _FS:
        def getFileStatus(self, _path: object) -> _Status:
            return _Status("dir", is_file=False)

        def listStatus(self, _path: object) -> list[_Status]:
            # Deliberately not sorted. The reader must sort before it compares.
            return [_Status("z.csv"), _Status("m.csv"), _Status("a.csv")]

    monkeypatch.setattr(
        "qa_core_recon.readers._hadoop_file",
        lambda _spark, _uri: (_FS(), object()),
    )

    def line(_spark: object, uri: str, _side: str) -> str:
        if uri.endswith("/z.csv"):
            return "id,amount"
        return "txn_id,amount"

    monkeypatch.setattr("qa_core_recon.readers._first_s3_line", line)
    with pytest.raises(ReadError) as caught:
        _read_csv_s3(spark, "s3a://bucket/dir", "source")
    assert str(caught.value) == "source csv header in 'z.csv' does not match 'a.csv'"


def test_apply_s3_config_unsets_access_and_secret_keys(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.s3a.access.key", "qa-core-recon-test-access-key")
    conf.set("fs.s3a.secret.key", "qa-core-recon-test-secret-key")
    apply_s3_config(spark)
    assert conf.get("fs.s3a.access.key") in (None, "")
    assert conf.get("fs.s3a.secret.key") in (None, "")


def test_s3_read_unsets_access_and_secret_keys(spark, tmp_path):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.s3a.access.key", "qa-core-recon-test-access-key")
    conf.set("fs.s3a.secret.key", "qa-core-recon-test-secret-key")
    bucket = _bucket("scrub-key")
    target = _local_csv(tmp_path, "target.csv", CSV)
    path = f"s3://{bucket}/raw/missing.csv"
    data = _spec(
        tmp_path,
        {"path": path, "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    with pytest.raises(ReadError):
        check_data(spark, parse_data(data))
    assert conf.get("fs.s3a.access.key") in (None, "")
    assert conf.get("fs.s3a.secret.key") in (None, "")


def test_scrub_removes_fs_s3_credential_keys(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3.awsAccessKeyId", "qa-core-recon-test-access-key")
    conf.set("fs.s3.awsSecretAccessKey", "qa-core-recon-test-secret-key")
    scrub_s3_credentials(spark)
    assert conf.get("fs.s3.awsAccessKeyId") in (None, "")
    assert conf.get("fs.s3.awsSecretAccessKey") in (None, "")
    assert conf.get("fs.s3.impl") == "org.apache.hadoop.fs.s3a.S3AFileSystem"


def test_scrub_removes_a_secret_stored_under_an_unmarked_name(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    conf.set("com.example.note", secret)
    scrub_s3_credentials(spark)
    assert conf.get("com.example.note") in (None, "")


def test_status_403_is_access_denied():
    path = "s3://recon-bucket/raw/payments.csv"
    exc = RuntimeError("Service: Amazon S3; Status Code: 403; Error Code: Forbidden")
    assert _classify_s3_error(exc, "target", path, missing_key=True) == (
        f"access denied reading target path {path}"
    )


def test_missing_key_markers_apply_only_when_requested():
    path = "s3://recon-bucket/pq/absent"
    exc = RuntimeError("Path does not exist: s3a://recon-bucket/pq/absent PATH_NOT_FOUND")
    assert _classify_s3_error(exc, "target", path, missing_key=True) == (
        f"target path not found: {path}"
    )
    assert _classify_s3_error(exc, "target", path, missing_key=False) is None


def test_holds_credential_matches_a_key_name_only():
    assert _holds_credential("fs.s3n.awsSecretAccessKey", "plain", ()) is True


def test_holds_credential_matches_a_value_only():
    secret = "qa-core-recon-test-secret-key"
    assert _holds_credential("com.example.note", secret, (secret,)) is True
    assert _holds_credential("fs.s3a.endpoint", "http://127.0.0.1:9", (secret,)) is False


def test_run_reads_iceberg_metadata_stored_as_s3_urls(spark, tmp_path, capsys):
    bucket = _bucket("glue")
    _write_iceberg_with_s3_scheme(
        spark,
        bucket,
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        _schema(DecimalType(10, 2)),
    )
    metadata = _iceberg_metadata_text(bucket)
    assert "s3://" in metadata
    assert "s3a://" not in metadata
    source = _local_csv(tmp_path, "source.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": str(source), "format": "csv"},
        {"path": f"s3://{bucket}/warehouse", "format": "iceberg", "table": TABLE},
        [str(tmp_path), f"s3://{bucket}"],
    )
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = main(["run", "--spec", str(spec), "--out", str(out)])
    captured = capsys.readouterr()
    assert code is ExitCode.MATCH, captured.err
    assert captured.out == "match\n"
    assert captured.err == ""
    assert "No FileSystem for scheme" not in captured.err

    _put_csv(bucket, "raw/payments.csv", CSV)
    csv_data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": str(source), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    _assert_data_ok(tmp_path, csv_data, capsys)


def test_run_matches_and_mismatches_csv_on_s3(spark, tmp_path, capsys):
    bucket = _bucket("run")
    _put_csv(bucket, "raw/payments.csv", CSV)
    _put_csv(bucket, "curated/same.csv", CSV)
    matched = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": f"s3://{bucket}/curated/same.csv", "format": "csv"},
        [f"s3://{bucket}"],
    )
    spec = write_spec_file(tmp_path, matched)
    out = tmp_path / "match-out"
    code = main(["run", "--spec", str(spec), "--out", str(out)])
    captured = capsys.readouterr()
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    assert code is ExitCode.MATCH
    assert captured.out == "match\n"
    assert captured.err == ""
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    assert report["status"] == "match"
    published = (out / "report.json").read_text(encoding="utf-8") + (out / "report.md").read_text(
        encoding="utf-8"
    )
    assert secret not in published
    assert secret not in captured.out

    _put_csv(bucket, "curated/other.csv", OTHER)
    differed = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": f"s3://{bucket}/curated/other.csv", "format": "csv"},
        [f"s3://{bucket}"],
    )
    other_spec = write_spec_file(tmp_path, differed)
    other_out = tmp_path / "mismatch-out"
    code = main(["run", "--spec", str(other_spec), "--out", str(other_out)])
    captured = capsys.readouterr()
    assert code is ExitCode.MISMATCH
    assert captured.out == "mismatch\n"
    assert captured.err == ""
    report = json.loads((other_out / "report.json").read_text(encoding="utf-8"))
    assert report["status"] == "mismatch"
    published = (other_out / "report.json").read_text(encoding="utf-8")
    assert secret not in published


def test_run_s3_changed_row_is_key_diff(spark, tmp_path, capsys):
    bucket = _bucket("changed")
    _put_csv(bucket, "raw/payments.csv", CSV)
    _put_csv(bucket, "curated/other.csv", OTHER)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": f"s3://{bucket}/curated/other.csv", "format": "csv"},
        [f"s3://{bucket}"],
    )
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = main(["run", "--spec", str(spec), "--out", str(out)])
    captured = capsys.readouterr()
    assert code is ExitCode.MISMATCH
    assert captured.out == "mismatch\n"
    assert captured.err == ""
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    checks = {check["name"]: check for check in report["checks"]}
    assert checks["key_diff"]["status"] == "mismatch"
    assert checks["key_diff"]["details"]["changed"]["count"] == 1
    assert checks["key_diff"]["details"]["changed"]["examples"] == [["1"]]
    assert checks["key_diff"]["details"]["missing"]["count"] == 0
    assert checks["key_diff"]["details"]["extra"]["count"] == 0
    columns = [column["column"] for column in checks["changed_columns"]["details"]["columns"]]
    assert columns == ["amount", "note"]
    assert checks["changed_columns"]["status"] == "mismatch"


def test_local_only_process_does_not_request_s3_packages(tmp_path):
    source = _local_csv(tmp_path, "source.csv", CSV)
    target = _local_csv(tmp_path, "target.csv", CSV)
    spec = write_spec_file(
        tmp_path,
        spec_data(tmp_path, source, target, target_format="csv"),
    )
    ivy = tmp_path / "ivy"
    env = os.environ.copy()
    env["QA_CORE_RECON_IVY"] = str(ivy)
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "validate", "--spec", str(spec), "--check-data"],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
        env=env,
    )
    assert result.returncode == 0
    assert result.stdout == "data ok\n"
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    assert secret not in result.stdout
    assert secret not in result.stderr
    names = [path.name for path in ivy.rglob("*")] if ivy.exists() else []
    assert not any(
        "hadoop-aws" in name or "aws-java-sdk" in name or "iceberg" in name for name in names
    )


def test_check_data_s3_process_prints_only_data_ok(tmp_path):
    bucket = _bucket("proc")
    _put_csv(bucket, "raw/payments.csv", CSV)
    target = _local_csv(tmp_path, "target.csv", OTHER)
    data = _spec(
        tmp_path,
        {"path": f"s3://{bucket}/raw/payments.csv", "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), f"s3://{bucket}"],
    )
    spec = write_spec_file(tmp_path, data)
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "validate", "--spec", str(spec), "--check-data"],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "data ok\n"
    assert result.stderr == ""
    assert "Traceback" not in result.stderr
    assert os.environ["AWS_SECRET_ACCESS_KEY"] not in result.stdout
    assert os.environ["AWS_SECRET_ACCESS_KEY"] not in result.stderr


def test_no_aws_credentials_names_the_source_path(tmp_path):
    path = "s3://recon-absent-bucket/raw/payments.csv"
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": path, "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), "s3://recon-absent-bucket"],
    )
    spec = write_spec_file(tmp_path, data)
    env = os.environ.copy()
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    ):
        env.pop(name, None)
    home = tmp_path / "empty-home"
    home.mkdir()
    env["HOME"] = str(home)
    env["AWS_SHARED_CREDENTIALS_FILE"] = str(tmp_path / "missing-credentials")
    env["AWS_CONFIG_FILE"] = str(tmp_path / "missing-config")
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "validate", "--spec", str(spec), "--check-data"],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
        env=env,
    )
    assert result.returncode == 2
    assert result.stdout == ""
    assert f"no AWS credentials found for source path {path}" in result.stderr
    assert "access denied" not in result.stderr
    assert "Traceback" not in result.stderr


_AUTH_DENIED = """\
import os
import sys

os.environ["INITIAL_NO_AUTH_ACTION_COUNT"] = "0"
from moto.server import ThreadedMotoServer

server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
server.start()
host, port = server.get_host_and_port()
os.environ["QA_CORE_RECON_S3_ENDPOINT"] = f"http://{host}:{port}"
os.environ["AWS_ACCESS_KEY_ID"] = "qa-core-recon-test-access-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "qa-core-recon-test-secret-key"
os.environ["AWS_REGION"] = "us-east-1"
os.environ.pop("AWS_SESSION_TOKEN", None)
os.environ.pop("AWS_SECURITY_TOKEN", None)

from qa_core_recon.cli import main

raise SystemExit(int(main(["validate", "--spec", sys.argv[1], "--check-data"])))
"""


def test_moto_with_auth_required_is_access_denied(tmp_path):
    path = "s3://recon-auth-bucket/raw/payments.csv"
    target = _local_csv(tmp_path, "target.csv", CSV)
    data = _spec(
        tmp_path,
        {"path": path, "format": "csv"},
        {"path": str(target), "format": "csv"},
        [str(tmp_path), "s3://recon-auth-bucket"],
    )
    spec = write_spec_file(tmp_path, data)
    script = tmp_path / "auth_denied.py"
    script.write_text(_AUTH_DENIED, encoding="utf-8")
    env = os.environ.copy()
    env["INITIAL_NO_AUTH_ACTION_COUNT"] = "0"
    result = subprocess.run(
        [sys.executable, str(script), str(spec)],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
        env=env,
    )
    assert result.returncode == 2, result.stderr
    assert f"access denied reading source path {path}" in result.stderr
    assert "no AWS credentials" not in result.stderr
    assert "Traceback" not in result.stderr
