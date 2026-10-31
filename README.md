# qa-core-recon

Prove that a target dataset matches its source after a Glue or PySpark job, using deterministic checks. No AI model decides pass or fail.

**Status: key diff.** `--version`, `init`, `validate`, and `run` work. `validate --check-data` opens local CSV, Parquet, and Iceberg and runs the data-level refusals. `run` compares row counts, amount sums, null keys, duplicate keys, and the key diff, and writes `report.json` and `report.md`. S3 reads and the demo are not in this release.

## Install From Source

Requires Python 3.11 and Java 17. Java 17 is the runtime AWS Glue 5.0 uses with Spark 3.5.

```bash
git clone https://github.com/sprintsynergyhq/qa-core-recon.git
cd qa-core-recon
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

PySpark is pinned to the 3.5 line (`pyspark>=3.5,<3.6`), the Spark line in AWS Glue 5.0. `init` and plain `validate` do not start a JVM. `validate --check-data` and `run` do. `run` skips the JVM when a spec-level refusal, `sample-only`, or an `s3://` input stops the command first.

A spec that uses Iceberg also needs the Iceberg Spark runtime `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1`. The first `validate --check-data` or `run` on that spec downloads it from Maven Central into `~/.cache/qa-core-recon/ivy`. Set `QA_CORE_RECON_IVY` to use another cache directory. A CSV or Parquet spec does not download it.

## Commands

Print the version:

```bash
qa-core-recon --version
```

Write a commented spec template to `recon.yaml`, or to another file with `--path`:

```bash
qa-core-recon init [--path recon.yaml] [--force]
```

If the file already exists, `init` leaves it untouched and exits with code 2. Pass `--force` to overwrite that file.

If the path is an existing directory, the parent directory is missing, or the write is denied, `init` prints one `qa-core-recon: error:` line to stderr and exits with code 2. `--force` only overwrites an existing file, so those messages leave it out. `init` never exits with code 1.

Check that a spec file loads, validates, and passes the spec-level refusals:

```bash
qa-core-recon validate --spec recon.yaml
```

A usable spec prints `spec ok` and exits with code 0.

Also open both datasets and run the data-level refusals:

```bash
qa-core-recon validate --spec recon.yaml --check-data
```

When those checks pass, the command prints `data ok` and exits with code 0. It does not compare the two datasets, so different amounts on the two sides still print `data ok`. `run` compares counts, sums, and keys and writes a report.

A refusal prints `refused: <code>: <message>` to stderr and exits with code 2. A malformed spec prints `qa-core-recon: error: <message>` to stderr and exits with code 2. A dataset that cannot be opened, including an allowlisted `s3://` path, prints `qa-core-recon: error: <message>` to stderr and exits with code 2.

## Run

Compare the source and the target and write a report:

```bash
qa-core-recon run --spec recon.yaml --out out
```

The command prints `match` or `mismatch` on stdout. A refusal prints the same `refused:` line as `validate` and still writes both report files, with status `refused`. A read error, a malformed spec, an unusable `--out`, and a sum that does not fit `decimal(38, scale)` print `qa-core-recon: error:` and write no report file.

`--out` is a local directory. It is created when it is missing. It is not checked against the allowlist. `s3://` is rejected. It must not equal a local source or target path, sit inside one, or contain one, or the next read can pick up `report.json` as a data file. If the directory already contains any file, the command exits 2 and writes nothing, unless you pass `--force`. A leftover `.report.json.<pid>.tmp` or `.report.md.<pid>.tmp` file from a crashed publish is removed and does not count as content. Other names, and symlinks, are left in place. After the spec loads and the output path is accepted, any previous `report.json` and `report.md` are removed, so a read error does not leave the old report in place. A malformed spec, or an output path that overlaps a dataset, leaves that directory as it was. `--force` still leaves every other file alone. Both report files are written to temporary names in that directory and then renamed into place. If either rename fails, both report files are removed.

Nothing in pass or fail calls a model. Two runs on the same inputs write byte-identical files. The report has no timestamp. It does not add a path of its own. `spec_sha256` is the SHA-256 of the spec file bytes.

| Code | Result |
| ---- | ------ |
| 0 | Every check matched. |
| 1 | At least one check did not match. |
| 2 | The run was refused, or a dataset or the output directory could not be used. |

### Example Spec

```yaml
version: 1
source:
  path: ./data/raw/payments.csv
  format: csv
target:
  path: ./data/curated/payments
  format: parquet
keys: [txn_id]
amounts:
  - column: amount
    scale: 2
group_by: [day]
hash_columns: [note]
tolerance:
  value: "0"
  reason: ""
sampled: false
allowlist: ["./data"]
```

`tolerance.value` stays a quoted string. A value above zero needs a reason. That rule is the same one `validate` already enforces. `run` then uses the value as an exact decimal limit on amount sums.

### Checks

All six checks run inside Spark. Only the aggregates come back to the driver: one total per side, one row per group, the null-key counts, the duplicate-key count, the key-diff counts, the changed-column counts, and at most 20 example keys for each list. A mismatch does not skip the later checks. The report lists the checks in this order: row count, amount sum, null keys, duplicate keys, key diff, and changed columns. The row hash below is the encoding the last two checks use. It is not its own check.

**Row Count.** Total rows on each side. When `group_by` is set, also the rows in each group. Groups are sorted. A group that is missing on either side is a mismatch and is listed. A count of 0 on one side means that group was not present. Group values compare by value across types, so an int 1 and a decimal 1.00 are the same group. Every float NaN is one shared group, shown as NaN, and it sorts after every other float. -0.0 is the same group as 0.0.

**Amount Sum.** For each amount column, the exact sum on each side, stored as `decimal(38, scale)`, for the whole side and for each group. Decimals are written as strings with the full scale, so scale 2 renders `10.50`. Null amounts are left out of the sum. They are not added as zero. A side or a group with no non-null amount has a null sum. Two null sums match. A null sum against a number, including zero, is a mismatch.

Sums are compared exactly. When the tolerance is above zero, the absolute difference must be less than or equal to that tolerance, and the report names the reason. Equality is inside the range. The same tolerance is applied to the total and to each group on its own. It is not a budget shared across groups. A missing group has no sum, so the tolerance does not cover it. Tolerance applies only to these sums. It does not apply to the row hash or to the key diff. A row whose amount differs is a changed row even when the sums are inside the tolerance, and the run does not match.

Spark's own decimal sum is not used. With precision loss allowed, that sum returns null on overflow, and null is also what an all-null column produces. This command folds the sum as an exact decimal and then requires it to fit `decimal(38, scale)`. A total or a group sum that does not fit is a read error, exit 2, and no report is written. The message says `sum` for a total and `group sum` for one group. Source is checked before target. On one side, groups are checked in sorted order and then the total. Amount columns follow the spec order.

**Null Keys.** The number of rows with a null in any key column, on each side. Any null key is a mismatch. The report gives both counts. An empty CSV field is an empty string, not a null. Parquet and Iceberg can store a real null.

**Duplicate Keys.** The number of key values that appear more than once, on each side. Any duplicate is a mismatch. The report gives the counts and up to 20 example keys. Those 20 are the first in sorted order, with nulls first. 20 is fixed (`DUPLICATE_EXAMPLE_LIMIT`). The count is the full number, so it can be higher than the number of examples. A key value is the whole key: every column in `keys`.

**Row Hash.** For every row, Spark computes a SHA-256 over the compared columns. The compared columns are the amount columns and the `hash_columns`, with key columns removed. `group_by` is not included unless that column is also an amount or a hash column. Source and target use the same column names, so the hash order is those names sorted ascending. That is the target order after the identity mapping. A column that is not named in the spec is not hashed.

The encoding is one token per value, concatenated in that order. Null is the single character `n`. Every other value is `tag:length:payload`. `length` is the UTF-8 byte length of `payload`, written in decimal with no leading zeros. This keeps `("a,b", "c")` distinct from `("a", "b,c")`, and it keeps null distinct from the empty string (`string:0:`).

| Value | Token |
| ----- | ----- |
| null | `n` |
| boolean | `bool:4:true` or `bool:5:false` |
| integer | `int:<length>:<digits>` |
| decimal | `decimal:<length>:<plain value>` |
| float | `float:<length>:<text>` |
| string | `string:<length>:<text>` |
| bytes | `bytes:<length>:<lowercase hex>` |
| date | `date:10:YYYY-MM-DD` |
| timestamp | `timestamp:27:YYYY-MM-DDTHH:MM:SS.ffffffZ` |

A decimal is the plain string of its exact numeric value. Trailing zeros that only record scale are removed, so `1.0` and `1.00` both encode as `decimal:1:1`. Every significant digit is kept. The string is not rounded and is not written in scientific notation. Negative zero is `0`. An integer and a decimal stay different tags, so integer `1` and decimal `1` do not match. Amount columns are already `decimal(38, scale)` before this encoding. The amount sum check still prints them at the spec scale. The row hash does not.

A float uses one payload for every NaN, including a negative NaN (`NaN`). `-0.0` and `0.0` both encode as `0.0`. Other finite floats use Python `repr`. `Infinity` and `-Infinity` are fixed words.

A date is `YYYY-MM-DD`. A timestamp is UTC with six fractional digits and a `Z` suffix. A value without a time zone is UTC, because the Spark session time zone is UTC. A value with a time zone is converted to UTC first.

The row hash is not a separate pass or fail check. `key_diff` and `changed_columns` use it.

**Key Diff.** A full outer join on the key columns. The join runs in Spark. Only the three counts, the per-column counts, and at most 20 example keys come back to the driver. `KEY_DIFF_EXAMPLE_LIMIT` is 20, the same cap as `DUPLICATE_EXAMPLE_LIMIT`. Examples are sorted with nulls first. The count is the full number, so it can be higher than 20.

Three counts are reported. Any count above zero is a mismatch.

| Count | Meaning |
| ----- | ------- |
| `missing` | The key is on the source and not on the target. |
| `extra` | The key is on the target and not on the source. |
| `changed` | The key is on both sides and the row hash differs. |

Rows with a null in any key column are excluded. They stay in the null-key check and are not listed as missing, extra, or changed. A target key that has no matching source key is still extra: the excluded source row is not a key, so it does not match that target key.

Counts are by distinct key value, not by physical row. When several rows share a key, the row with the smallest row hash is the one that is compared. Two identical copies of one row therefore do not make the key changed. A second copy whose hash is larger is not the row that is compared. The duplicate-key check still reports the duplicate. The report states this rule on every run, and it says when duplicate keys were found.

**Changed Columns.** For the changed keys, each compared column has a count of how many distinct keys differ in that column, and up to 20 examples. An example has the key, the source token, and the target token, using the row-hash encoding above. Columns are sorted by name. Only columns with a count above zero are listed. A count above zero is a mismatch. The example is the row with the smallest row hash, the same row the key diff compared.

### Report Files

`report.json` is UTF-8 JSON with sorted keys, indent 2, and one trailing newline. The fields are:

| Field | Meaning |
| ----- | ------- |
| `tool` | `qa-core-recon` |
| `version` | the package version |
| `spec_sha256` | SHA-256 of the spec file bytes |
| `status` | `match`, `mismatch`, or `refused` |
| `refusals` | a list of `{code, message}`. Empty when the datasets were compared. |
| `checks` | a list of `{name, status, details}`. Empty when the run was refused. Decimals in amount sums are strings. The names, in order, are `row_count`, `amount_sum`, `null_keys`, `duplicate_keys`, `key_diff`, and `changed_columns`. |
| `row_counts` | `{source, target}`. Both are null when the run was refused, because the rows were not counted. |

### Sample Report

This is `report.md` from a match of two identical two-row files. Amounts were `10.50` and `1.25`, grouped by day. The spec hash is the SHA-256 of that spec file.

```text
# Reconciliation Report

## Result

Status: Match

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: 35b87470e839cca880695b3166435a855a8f77f3ac338c0b5fd4a76f830af52f

## Row Counts

Source: 2
Target: 2

## Checks

### Row Count

Status: Match

Source rows: 2. Target rows: 2.

Grouped by day.

Groups, in sorted order:

- day=2024-01-01. Source rows: 1. Target rows: 1. Match.
- day=2024-01-02. Source rows: 1. Target rows: 1. Match.

### Amount Sum

Status: Match

Column amount, scale 2. Source sum: 11.75. Target sum: 11.75. Difference: 0.00. Tolerance: 0. Match.

Groups, in sorted order:

- day=2024-01-01. Source sum: 10.50. Target sum: 10.50. Difference: 0.00. Match.
- day=2024-01-02. Source sum: 1.25. Target sum: 1.25. Difference: 0.00. Match.

### Null Keys

Status: Match

Source rows with a null key: 0.
Target rows with a null key: 0.

### Duplicate Keys

Status: Match

Example limit: 20.

Source duplicate keys: 0.

Target duplicate keys: 0.

### Key Diff

Status: Match

Compared columns, in hash order: amount, note.

Rows with a null in any key column are excluded.
Counts are by distinct key value.
When a key is duplicated, the row with the smallest row hash is used.

Example limit: 20.

Missing keys: 0.

Extra keys: 0.

Changed keys: 0.

### Changed Columns

Status: Match

Example limit: 20.

Each count is the number of distinct keys whose value differs. The example is the row with the smallest row hash.

No compared column differs.
```

## Exit Codes

| Code | Name | Meaning |
| ---- | ---- | ------- |
| 0 | `MATCH` | Success. For `validate`, the spec is usable. For `validate --check-data`, both datasets passed the data-level refusals. For `run`, every check matched. |
| 1 | `MISMATCH` | `run` found a difference. At least one check did not match. |
| 2 | `REFUSED` | The tool declined to run, or the input or the output directory could not be used, so there is no comparison result. A refusal still writes a report. A read error does not. |

`init` and `validate` use codes 0 and 2 only. `--check-data` is part of `validate`, so it stays on those codes too. `run` uses 0, 1, and 2. An amount that cannot be represented at the spec scale is a refusal, exit 2, because the tool has no decimal to compare. A sum that does not fit `decimal(38, scale)` is a read error on `run`, exit 2, because a rounded total would be a wrong number. `init` and `validate` never return code 1.

A malformed spec, a refusal, an unreadable spec file, an unreadable dataset, and any unexpected error all exit with code 2. An unexpected error inside the CLI is caught, printed as one `qa-core-recon: error:` line on stderr, and exits with code 2. The CLI prints no traceback.

## Refusal Codes

| Code | Meaning |
| ---- | ------- |
| `missing-key` | the spec has no business key, or the key is not unique and no rule says how to handle that. |
| `float-amount` | an amount column is float or double and the spec has no cast rule to a decimal scale. |
| `inexact-amount` | an amount value is not an exact decimal at the spec scale, or the column type is not a number type. |
| `tolerance-without-reason` | a tolerance above zero is set with no written reason. |
| `unmapped-column` | a column named in the spec is missing from the source or the target. |
| `path-not-allowed` | a path is outside the allowlist (local disk and the moto S3 bucket in the MVP). |
| `sample-only` | the input was sampled, so a full reconciliation cannot be claimed. |

`validate` enforces the spec-level codes. `validate --check-data` also enforces the data-level codes. The same file always prints the same line.

### Spec-Level Refusals

| Code | When Validate Refuses |
| ---- | --------------------- |
| `missing-key` | `keys` is absent, null, or empty. |
| `tolerance-without-reason` | `tolerance.value` is above zero and `reason` is empty or only whitespace. |
| `path-not-allowed` | `source.path` or `target.path` is outside `allowlist`. Source is checked first. |

Shape is checked before these refusals. A malformed spec exits before any refusal. `sampled: true` is well formed, so plain `validate` prints `spec ok`.

### Data-Level Refusals

These run only with `--check-data`, after the spec-level checks have passed.

| Code | When Check Data Refuses |
| ---- | ----------------------- |
| `sample-only` | `sampled` is true. Decided before either dataset is opened. |
| `unmapped-column` | A key, amount, `group_by`, or hash column is missing. The message names the side and the column. |
| `float-amount` | A Parquet or Iceberg amount column is float or double and that amount has no `cast` rule. |
| `inexact-amount` | An amount value is not an exact decimal at the spec scale, or the column type is not a number type. |

First match wins, in this order:

1. Spec shape.
2. `missing-key`, then `tolerance-without-reason`, then `path-not-allowed`.
3. `sample-only`, then `unmapped-column`, then `float-amount`, then `inexact-amount`.

Within `unmapped-column`, `float-amount`, and `inexact-amount`, source is checked before target. On one side the roles are keys, then amounts, then `group_by`, then `hash_columns`, in the order written in the spec. A column named in more than one role is checked once, at its first role. For `inexact-amount`, the reported cell is the lexicographically smallest offending text in that column, so row order does not change the line. Text longer than 80 characters is cut there and marked with `...`.

## Readers

CSV, Parquet, and local Iceberg. S3 is a later PR. See Iceberg for the catalog, the table identifier, and the pinned runtime. An allowlisted `s3://` path remains a valid spec, and plain `validate` accepts the packaged template. `--check-data` on an `s3://` path prints `qa-core-recon: error: s3 reads are not supported yet: ...` and exits 2. That is a read error, not `path-not-allowed`, because the allowlist check already passed. Source is named before target.

Every path is checked against the allowlist again before it is opened. The check uses the path text, the same way `validate` does. It does not resolve symlinks, and it does not join a relative path to the spec file. A relative path is opened from the current directory.

The Spark session is local mode on one thread (`local[1]`), with `spark.sql.shuffle.partitions` set to 1, the session time zone set to UTC, and the UI turned off. The driver binds to `127.0.0.1`. Arrow conversion is off. The session catalog is in memory, so Hive is not started. Warehouse, derby, and scratch directories are created under a temporary directory. A run does not leave `metastore_db`, `derby.log`, or `spark-warehouse` in the working directory or the repo root. Spark's startup banner is dropped, because it carries a timestamp and is not part of the tool's result. When the spec uses Iceberg, that session loads the Iceberg Spark runtime and registers a Hadoop catalog for the warehouse. A CSV or Parquet spec does not.

Column names are case sensitive. `Amount` does not satisfy a spec column named `amount`.

CSV is read from a file, or from a directory of part files. Marker files whose names start with `.` or `_` are skipped, so a Spark `_SUCCESS` file is not treated as a header. Every part file must use the header of the first file in sorted name order. A leading UTF-8 BOM is stripped from that header, so it does not rename the first column. The header becomes an explicit string schema, and `inferSchema` is off. The read uses `FAILFAST`, and `spark.sql.csv.parser.columnPruning.enabled` is false, so every field of every row is parsed. A short row, or any field that spans lines (quoted or not, since multi-line CSV is not supported yet), is a read error: `source csv has a malformed row`. Amount columns are parsed from those strings. A CSV amount is never a float, so a CSV file does not produce `float-amount`.

Parquet is read from a file or a directory, with `mergeSchema` off. The file schema is kept until amount columns are retyped. Iceberg keeps its table schema the same way, then the amount columns are retyped.

Readers read every row. They do not sample, limit, or keep a head. `sampled: true` is refused even when the files contain a full extract, because the spec declared a sample.

Blank amount cells are null. They are not turned into zero. `run` leaves a null amount out of the sum, and it counts null keys separately.

A missing path, a CSV file that is not UTF-8, a malformed CSV row, or a file that Spark cannot read is a read error (`qa-core-recon: error:`), exit 2. It is not a refusal code. A symlink loop in a source or target path is also a read error. The message is `cannot read <side> path ...: Symlink loop from ...`. It is not reported as a missing path, and it is not an unexpected error.

## Iceberg

`format: iceberg` reads an Iceberg table through a Hadoop catalog. `path` is the warehouse directory. `table` is exactly `namespace.table`, two parts, the same shape as a Glue Data Catalog name (`database.table`). A third part is rejected, so a name cannot open a metadata table, the changelog, or time-travel (`payments.txn.snapshots`, `payments.txn.changes`, `payments.txn.snapshot_id_1`). Spark reads `<table>.changes` as the changelog, including deleted rows, so `changes` is reserved too. A part that is itself a metadata table name, such as `snapshots`, or a selector such as `branch_main`, `branch_`, or `tag_`, is rejected. A table or namespace named like an Iceberg metadata table or selector (for example snapshots, changes, branch_*, tag_*) is refused on purpose, even when a real table has that name. Use `snapshot_id` to choose a snapshot. `snapshot_id` is optional. Leave it out to read the current snapshot. Set it to read that snapshot only.

```yaml
target:
  path: ./data/warehouse
  format: iceberg
  table: payments.txn
```

`path` goes through the allowlist before any read. The allowlist matches path text and does not resolve symlinks, for Iceberg and for every other format. The identifier is not a path. Each part must start with a letter or underscore and then use only letters, digits, and underscores. That keeps the Hadoop catalog from turning the identifier into a directory outside the warehouse. `table` and `snapshot_id` on a CSV or Parquet dataset are unknown keys. A missing `table`, a bad identifier, a snapshot id that is not an integer, and a snapshot id outside a 64-bit signed integer are a malformed spec.

The catalog is `org.apache.iceberg.spark.SparkCatalog` with `type` set to `hadoop`. It is registered for that warehouse only, and only when the spec uses Iceberg. The Spark session catalog stays in memory, Hive is not started, and the Iceberg catalog cache is off so a read sees the current snapshot. This is not the Glue catalog.

The runtime jar is `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1` (Spark 3.5, Scala 2.12). AWS Glue 5.0 ships Iceberg 1.7.1 with Spark 3.5.4, as listed in the AWS Glue versions documentation. Spark downloads the jar from Maven Central the first time a session is started for an Iceberg spec, and stores it in the Ivy cache (`~/.cache/qa-core-recon/ivy`, or `QA_CORE_RECON_IVY`). Later runs reuse the cache. CSV and Parquet do not download it. When Spark reports an unresolved dependency or an Ivy failure, and the jar is not already in that cache, `--check-data` and `run` print a read error that names the runtime and the cache directory, and exit 2. Any other failure to start the session prints `cannot start the local Spark session` and exits 2.

Iceberg on a local warehouse is supported. An allowlisted `s3://` warehouse is still a valid spec. `--check-data` prints `s3 reads are not supported yet` and exits 2, before Spark starts. S3 and the Glue catalog are later PRs.

After the table is open, Iceberg uses the same checks as Parquet. Decimal columns are typed to `decimal(38, scale)` and must already fit that scale. A float or double amount is `float-amount` unless the spec sets `cast`. A missing column is `unmapped-column`. A missing table, a missing warehouse, or a snapshot id that is not in the table is a read error (`qa-core-recon: error:`), exit 2.

## Decimal Rules

Each amount column is read as Spark `decimal(38, scale)`. 38 is Spark's maximum decimal precision. `scale` comes from the spec and is an integer from 0 through 38. The integer part may use `38 - scale` digits. A value that needs more digits is `inexact-amount`. It is not truncated.

Parsing is exact. A value is accepted when it equals the same number quantized to the spec scale. At scale 2, `10`, `10.5`, `10.50`, and `10.500` are exact (`10.00` or `10.50`). `10.005` is refused. Trailing zeros add no new information. Extra non-zero fractional digits would have to be rounded, and this tool does not invent a rounded amount. Equivalent spellings of one number, such as `100` and `1e2`, are the same value.

A text amount is ASCII only: optional surrounding whitespace, an optional sign, digits, an optional fraction, and an optional exponent such as `1e2` or `+10.5`. `1_000`, Arabic-Indic digits, and fullwidth digits are `inexact-amount`.

That refusal is `inexact-amount`, exit 2. It is not a mismatch. A mismatch means the target differs from the source, and exit 1 is reserved for `run`. A value that does not fit the declared scale gives the tool no amount to compare.

`amounts[].cast` is optional. The only values are `half-even` and `half-up`. When it is present, a Parquet or Iceberg float or double amount column is rounded to the spec scale with that mode. The value that is rounded is the shortest decimal spelling of the float (`repr`), which is the spelling Spark's `cast` uses. `2.675` at scale 2 is `2.68` for both `half-up` and `half-even`. The rule does not authorize rounding a CSV string, an integer, or a Parquet or Iceberg decimal. `10.005` at scale 2 is still `inexact-amount` when `cast` is set. Without `cast`, a float or double amount column is `float-amount` and its values are not read.

Non-finite values (`NaN`, `Infinity`) are `inexact-amount` even when a cast rule is present. A rounding mode does not define a decimal for them.

Integer Parquet columns are whole amounts. `1050` at scale 2 becomes `1050.00`. The reader does not treat an integer as minor units.

A column type other than string, decimal, integer, float, or double (for example boolean) is `inexact-amount`. The message names the side, the column, and the type, and says that type is not a number type. Float and double without `cast` are `float-amount` instead, because that refusal is more specific.

## Spec File

`recon.yaml` is parsed with PyYAML's safe loader. A repeated key at any level is a malformed spec: the error names the key and the line, and the last value is not kept. The packaged template is a valid example. `init` writes it, and `validate --spec` on that file exits 0, because both example paths sit inside the example allowlist. The template target is `s3://`, so `validate --check-data` on the unmodified template reports that S3 reads are not supported yet.

| Field | Rule |
| ----- | ---- |
| `version` | Integer `1`. Any other version is rejected. |
| `source`, `target` | Each has `path` and `format`. Format is `csv`, `parquet`, or `iceberg`. Iceberg also has `table` and an optional `snapshot_id`. |
| `keys` | List of column names. Absent, null, or empty is a `missing-key` refusal. |
| `amounts` | List of `{column, scale}` with optional `cast`. `scale` is an integer from 0 through 38. `cast` is `half-even` or `half-up`. The list may be empty. |
| `group_by`, `hash_columns` | Lists of column names. Either list may be empty. |
| `tolerance` | `value` and `reason`. |
| `sampled` | Boolean. `true` means the spec describes a sample. |
| `allowlist` | List of local paths or `s3://` URIs. An empty list allows no path. |

Unknown keys are rejected. Column names are non-empty single-line strings. Formats and the version number are exact: `csv`, `parquet`, `iceberg`, and integer `1`.

`sampled` must be a real boolean. Write `true` or `false`. A string such as `"false"` is a malformed spec. A missing `sampled` is a malformed spec, so an omitted declaration cannot be treated as a full reconciliation. A spec written by an earlier `init`, before this field existed, must add `sampled: false` before it will validate. `sampled: true` loads successfully. `validate` prints `spec ok`. `validate --check-data` refuses with `sample-only` and does not start Spark. An allowlisted `s3://` path is also reported before Spark starts.

`amounts[].cast`, when present, must be the string `half-even` or `half-up`. Null, a number, or any other string is a malformed spec. Omitting the key means there is no cast rule.

`tolerance.value` is a monetary amount. It must be a quoted string, parsed with `decimal.Decimal`. A YAML float is an error and is never converted. A YAML integer is an error too, because only a quoted string keeps the exact digits. The value must be finite and zero or greater. Zero may have an empty reason. A value above zero needs a reason that contains something other than whitespace.

Paths are local paths or `s3://bucket/key` URIs. Any other scheme is a malformed spec, which exits 2 with `qa-core-recon: error:`, and is not a `path-not-allowed` refusal.

## Allowlist Matching

Matching compares normalised whole path components. It reads the path text in the spec. It does not look at the filesystem, so it does not resolve symlinks and it does not treat the path as relative to the spec file or the current directory. Readers run this same check again before opening a file.

| Path | Allowlist Entry | Result |
| ---- | --------------- | ------ |
| `./data/x.csv` | `./data` | Allowed. `data` is one whole component, and `x.csv` sits under it. |
| `./database/x.csv` | `./data` | Outside. `database` and `data` are different components. |
| `./data/../data/x.csv` | `./data` | Allowed. Normalisation folds this back to `data/x.csv`. |
| `./data/../../secret.csv` | `./data` | Outside. `..` climbs out of `data`. |
| `./data/x.csv` | `./data/` | Allowed. A trailing slash does not add a component. |
| `s3://demo-bucket/curated/payments` | `s3://demo-bucket` | Allowed. The bucket matches, then the key sits under it. |
| `s3://demo-bucket-other/file.csv` | `s3://demo-bucket` | Outside. The bucket name differs by a whole component. |
| `s3://demo-bucket/curated2/file.csv` | `s3://demo-bucket/curated` | Outside. `curated2` and `curated` are different key components. |
| `/data/x.csv` | `./data` | Outside. An absolute path matches only an absolute entry. |

An `s3://` path matches only an `s3://` entry, by bucket and then by key components. Key segments are literal. A segment that is `.` or `..`, an empty segment (including a trailing slash), a query (`?`), or a fragment (`#`) makes the spec malformed. Comparison is case sensitive.

An allowlist entry that normalises to an empty path (`.` or `./data/..`), the filesystem root (`/`), or a path that still contains `..` is a malformed spec. A source or target path may still use `..`; that path is normalised and then compared with the allowlist.
