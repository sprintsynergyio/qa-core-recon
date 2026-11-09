# Release acceptance for 0.1.0

This is the gap audit for `qa-core-recon` 0.1.0. Each row is either a behavior the README promises or a property this version needs in order to reconcile two datasets safely. **Shipped** means the code on this branch does that, and a test locks it. **Deferred** means it is not in 0.1.0. The reason is in the row. Nothing here is a measured runtime or a row count from a machine other than the committed demo fixture (`ROW_COUNT = 200` in `examples/demo/generate.py`).

The README Quick Start, the refusal summary, and `--help` were corrected so they match the rows below. The earlier Quick Start said every changed `txn_id` is listed. The tool lists at most 20 examples. The earlier refusal summary said a non-unique key is `missing-key`. A duplicate key is a mismatch on `run`.

## Commands

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| `--version` | Prints `qa-core-recon 0.1.0` and exits 0. | Shipped. |
| `--help` | The program and `init`, `validate`, and `run` print the three exit codes and exit 0. | Shipped. `tests/test_cli.py`. |
| No command | Help goes to stderr and the process exits 2. | Shipped. `tests/test_cli.py`. |
| `init` | Writes the packaged template. Exits 2 when the file exists, the path is a directory, the parent is missing, or the write is denied. `--force` overwrites a file only. Never exits 1. | Shipped. |
| `validate` | Prints `spec ok` and exits 0, or one `refused:` or `qa-core-recon: error:` line and exits 2. | Shipped. |
| `validate --check-data` | Opens both datasets and runs the data-level refusals. It does not compare them. Different amounts still print `data ok`. | Shipped. |
| `run` | Prints `match` or `mismatch`. Exit 0 or 1. A refusal still writes both report files. A read error, a malformed spec, a bad `--out`, or a sum that does not fit `decimal(38, scale)` writes no report and exits 2. | Shipped. |
| Unexpected errors | One `qa-core-recon: error:` line, no traceback, exit 2. Exit 1 is only a real mismatch. | Shipped. |

## Checks

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| Order | Row count, amount sum, null keys, duplicate keys, key diff, changed columns. A mismatch does not skip the later checks. | Shipped. |
| Row count | Totals, and one row per `group_by` value. A group missing on either side is a mismatch. | Shipped. |
| Amount sum | Exact `decimal(38, scale)` per total and per group. Null amounts are left out. Two null sums match. Tolerance applies to each sum on its own, not to the key diff. | Shipped. |
| Null keys | Any null in a key column is a mismatch. The report gives both counts. Those rows are excluded from the key diff. | Shipped. `tests/test_run.py`, `tests/test_key_diff.py`. The demo rows have no null keys. |
| Duplicate keys | Any duplicated key value is a mismatch. The count is complete. Examples are the first 20 in sorted order, nulls first. | Shipped. The demo rows have no duplicate keys. |
| Key diff | Full outer join on the aligned key. Counts of missing, extra, and changed keys. Any count above zero is a mismatch. | Shipped. |
| Changed columns | For changed keys, each compared column has a count and up to 20 examples with source and target tokens. | Shipped. |
| Row hash | SHA-256 of the amount, `group_by`, and `hash_columns` values, keys removed, names sorted. It is not its own check. An empty compared-column set cannot detect changes, and the report says so. | Shipped. |
| Example cap | `KEY_DIFF_EXAMPLE_LIMIT` and `DUPLICATE_EXAMPLE_LIMIT` are 20. The count is the full number. | Shipped. A spec flag for a different cap is deferred. The cap is what keeps a large mismatch from copying every key to the driver. |
| Demo key diff | Missing rows: count 40, first 20 ids `TXN000001` through `TXN000096` step 5. Extra rows: `TXN900001`, `TXN900002`, `TXN900003`. Changed rows: `TXN000001` through `TXN000005`. Sum mismatch: count 200, first 20 ids `TXN000001` through `TXN000020`. | Shipped. `tests/test_demo.py` compares `report.json` and `report.md` bytes and checks these ids. |

## Decimals

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| Scale | Each amount is `decimal(38, scale)` with `scale` from 0 through 38. A value that needs more digits is `inexact-amount`. It is not truncated. | Shipped. `tests/test_decimals.py`. |
| Text | ASCII only. Spaces and tabs around the number are allowed. A line break or a Unicode space is `inexact-amount`. `1_000` and non-ASCII digits are refused. | Shipped. |
| Equivalent spellings | `100` and `1e2` are the same value. Trailing zeros that do not change the value are exact. Extra non-zero fractional digits are refused. | Shipped. |
| Cast | `half-even` and `half-up` round a Parquet or Iceberg float or double only. CSV text, integers, and decimals are not rounded. `NaN` and infinities stay `inexact-amount`. | Shipped. |
| Sum overflow | The sum is an exact decimal fold, not Spark's decimal sum. A total or group sum that does not fit `decimal(38, scale)` is a read error, exit 2, and no report is written. | Shipped. `tests/test_run.py`. |
| Tolerance | A quoted string. A YAML float or integer is a malformed spec. Above zero requires a non-blank reason. | Shipped. |

## Readers

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| CSV, Parquet, Iceberg | Local files and `s3://` paths. `s3://` is opened as `s3a://`. The spec text stays `s3://`. | Shipped. `tests/test_readers.py`, `tests/test_iceberg.py`, `tests/test_s3.py`. |
| CSV details | Header schema, `inferSchema` off, `FAILFAST`, marker files skipped, part-file headers must match, a leading UTF-8 BOM is stripped. | Shipped. |
| Multi-line CSV | A field that spans lines is a read error. | Deferred. The README says multi-line CSV is not supported. |
| Iceberg catalog | Hadoop catalog only. `table` is `namespace.table`. Metadata names, `changes`, `branch_*`, and `tag_*` are rejected. Optional `snapshot_id`. | Shipped. |
| Glue Data Catalog | Not used. | Deferred. The README says this is not the Glue catalog. A Hadoop catalog covers a local warehouse and an `s3://` warehouse. |
| Full read | Readers do not sample or limit. `sampled: true` is `sample-only` before Spark starts. | Shipped. |
| Column names | Case sensitive. Source and target use the same names. | Shipped. A rename map is deferred. The README describes an identity mapping. |

## Reports

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| `report.json` | UTF-8, sorted keys, indent 2, one trailing newline. No timestamp. `spec_sha256` is the SHA-256 of the spec file bytes. | Shipped. |
| `report.md` | Title Case headings. Dataset text is a code span. Backticks, line breaks, and `|` cannot break the line. | Shipped. |
| Same inputs | Two runs write the same bytes. | Shipped. |
| Output directory | Local only. `s3://` is rejected. Overlap with a dataset path is rejected. A non-empty directory needs `--force`. Publish uses temp names and renames into place. | Shipped. |
| Refused report | Status `refused`, the refusal list, null row counts, and no checks. | Shipped. |

## Large data

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| Session | `local[1]`, one shuffle partition, UTC, UI off, driver bound to `127.0.0.1`. | Shipped. Cluster or Glue-job execution is deferred. One thread is what makes the report independent of partition order. |
| What returns to the driver | Amount sums are folded in Spark. One state comes back when there is no `group_by`. One row per group comes back when `group_by` is set. Key-diff and duplicate-key examples are limited to 20. Counts are complete. | Shipped. |
| Key-diff memory | The join is cached as one row per distinct key (the hash and the compared tokens), then unpersisted. A very large key set still needs memory for that cache on the single local executor. | Accepted for 0.1.0. The README already says the session is `local[1]`. Spilling that cache, or running on a cluster, is deferred with cluster mode. |
| High-cardinality `group_by` | Every group is listed, including groups that exist on only one side. Grouping by a unique column pulls one result row per data row. | Accepted for 0.1.0. Dropping groups would hide a missing group. The README says every group is listed. |

## Security

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| Allowlist | Whole path components, before any open. An `s3://` path matches only an `s3://` entry. `.` and `..` are folded. The filesystem root and an empty allowlist path are a malformed spec. | Shipped. |
| Globs and colon paths | `* ? [ ] { } \` in a path segment are rejected. A local segment that contains a colon is rejected, including after `..` is folded. | Shipped. |
| Credentials | The spec has no key or secret. Hadoop settings do not copy them in. If Hadoop copies one into its configuration, it is removed before the call returns. Error text does not include a stack trace. | Shipped. `tests/test_s3.py`. |
| Symlinks | The allowlist uses the path text and does not resolve symlinks. A symlink loop is a read error, not a missing path. | Accepted for 0.1.0. Following links would change the path-text contract the allowlist tests lock. A link placed inside an allowed directory is the operator's file. |
| Iceberg identifiers | Each part is a letter or underscore, then letters, digits, and underscores, so the catalog cannot turn the name into a directory outside the warehouse. | Shipped. |
| Maven jars | Iceberg `1.7.1`, `hadoop-aws` `3.3.4`, and `aws-java-sdk-bundle` `1.12.262`, downloaded only when the spec needs them. | Shipped as pinned coordinates. Checksum pinning is deferred. Spark's package resolver in this tool takes Maven coordinates, not a hash. |
| Publish action | `pypa/gh-action-pypi-publish@release/v1`, Trusted Publisher, `id-token: write`, no API token, `skip-existing: true`. | Shipped. Pinning that action to a commit SHA is deferred. PyPI's own setup uses the `release/v1` tag. The workflow does not run on a pull request. |
| Real AWS | Reads are implemented. Tests use moto on `127.0.0.1` and do not call AWS. | Deferred as a live check. There is no code path that is moto-only. The owner can point a spec at a bucket after setting `AWS_REGION` and the normal credential chain. |

## Release metadata

| Item | v0.1 behavior | Decision |
| ---- | ------------- | -------- |
| Version | `0.1.0` in `src/qa_core_recon/__init__.py`. Hatch reads it. `requires-python` is `>=3.11`. | Shipped. |
| License and owners | MIT, copyright SprintSynergy. Authors `SprintSynergy <usman@sprintsynergy.com>`. Project URLs use `https://github.com/sprintsynergyio/qa-core-recon`. | Shipped. |
| Package contents | The wheel has the template and no dataset files. The sdist may include the small demo CSV files and no Parquet or Iceberg files. | Shipped. `scripts/check_distributions.py`, run in CI. |
| CI | Python 3.11 and Java 17. Ruff, pytest, build, twine, and the distribution check. | Shipped. A matrix for 3.12 and later is deferred. The README names 3.11 because that is the Python line this project tests, and `requires-python` allows newer interpreters. |
| PyPI | `.github/workflows/publish.yml` publishes on a GitHub release or a `v*` tag. | The workflow is in the repo. Publishing is the owner's step in `RELEASING.md`. This pull request does not tag and does not upload. |

## What 0.1.0 does not add

These were considered and left out on purpose.

| Item | Why it waits |
| ---- | ------------ |
| Configurable duplicate policy | Any duplicate is a mismatch. The smallest row hash only chooses which copy is compared. A spec rule that accepts duplicates would let a run pass while two rows share a key. |
| Column rename map | Both sides use one name per column. |
| Glue catalog, multi-line CSV, cluster execution | Named above. Each one changes a contract the README already states. |
| A higher example cap | 20 is enough to identify the break. The count says how many keys remain. |
