# Changelog

## 0.1.0

First release.

### Command Line

- `qa-core-recon --version` prints `qa-core-recon 0.1.0` and exits 0.
- `--help` on the program and on `init`, `validate`, and `run` prints the exit codes and exits 0. A missing command prints help to stderr and exits 2.
- `init` writes a commented spec template. It exits 2 when the file exists, the path is a directory, the parent is missing, or the write is denied. `--force` overwrites an existing file only.
- `validate` checks that a spec loads. `validate --check-data` opens CSV, Parquet, and Iceberg on local disk and on S3 and runs the data-level refusals. It does not compare the datasets.
- `run` compares the datasets and writes `report.json` and `report.md`. Exit 0 is a match. Exit 1 is a mismatch. Exit 2 is a refusal or a read error. A refusal still writes the report. A read error does not.

### Checks And Reports

- Checks, in order: row count, amount sum, null keys, duplicate keys, key diff, and changed columns. A mismatch does not skip later checks.
- Row counts and amount sums are reported for the whole side and for each `group_by` value.
- Amounts are exact decimals at the spec scale. A YAML float or integer tolerance is rejected. Null amounts are left out of a sum. A blank amount cell is empty or holds only spaces and tabs. A cell that is only a line break or a Unicode space is `inexact-amount`.
- An optional `cast` of `half-even` or `half-up` applies only to a Parquet or Iceberg float or double amount.
- Tolerance is a quoted decimal. It applies to each total and each group sum on its own. It does not apply to the row hash or the key diff.
- A sum that does not fit `decimal(38, scale)` is a read error. No report is written.
- Null keys are counted and are a mismatch. They are excluded from the key diff.
- Duplicate keys are a mismatch. The report gives the count and up to 20 example keys. The key diff still compares one row per distinct key: the row with the smallest row hash.
- The key diff lists missing, extra, and changed keys. Changed columns name which compared fields differ. Each example list is capped at 20. The count is the full number.
- `report.json` is canonical: sorted keys, indent 2, UTF-8, and one trailing newline.
- `report.md` uses Title Case headings. Dataset text in the markdown report is escaped. Neither report file has a timestamp.

### Readers

- CSV, Parquet, and Iceberg, on local disk and on `s3://` (read as `s3a://`).
- Iceberg uses a Hadoop catalog. `table` is `namespace.table`. Metadata table names and the changelog name are rejected. `snapshot_id` selects one snapshot.
- S3 credentials come from the AWS chain. The spec has no key or secret. The tool does not copy credentials into the Spark configuration.
- Paths are checked against the allowlist before a file is opened. The check uses the path text and does not resolve symlinks.

### Spec And Refusals

- Spec version 1. Formats are `csv`, `parquet`, and `iceberg`.
- Refusal codes are `missing-key`, `float-amount`, `inexact-amount`, `tolerance-without-reason`, `unmapped-column`, `path-not-allowed`, and `sample-only`.
- `sampled: true` is `sample-only` and does not start Spark.

### Demo

- `examples/demo` is a made-up payments dataset with a fixed seed. Specs cover match, missing rows, extra rows, changed rows, a sum mismatch, Iceberg, and S3 (moto). Expected reports are committed. Tests compare those bytes.
- The difference cases list `txn_id` values from the key diff. Lists longer than 20 show the first 20 and the full count.

### Release

- `docs/release-acceptance.md` records what 0.1.0 ships and what is deferred.
- `RELEASING.md` is the owner checklist for the PyPI Trusted Publisher and the `v0.1.0` tag. This repository does not publish from a pull request.
