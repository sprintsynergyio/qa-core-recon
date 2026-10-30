# qa-core-recon

Prove that a target dataset matches its source after a Glue or PySpark job, using deterministic checks. No AI model decides pass or fail.

**Status: readers.** `--version`, `init`, and `validate` work. `validate --check-data` opens local CSV and Parquet and runs the data-level refusals. `run` arrives in a later PR. Counts, sums, hashes, diffs, and reports are not in this release.

## Install From Source

Requires Python 3.11 and Java 17. Java 17 is the runtime AWS Glue 5.0 uses with Spark 3.5.

```bash
git clone https://github.com/sprintsynergyhq/qa-core-recon.git
cd qa-core-recon
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

PySpark is pinned to the 3.5 line (`pyspark>=3.5,<3.6`), the Spark line in AWS Glue 5.0. `init` and `validate` do not start a JVM. `validate --check-data` does.

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

When those checks pass, the command prints `data ok` and exits with code 0. It does not compare the two datasets, so different amounts on the two sides still print `data ok`. Counts, sums, hashes, and diffs arrive with `run`.

A refusal prints `refused: <code>: <message>` to stderr and exits with code 2. A malformed spec prints `qa-core-recon: error: <message>` to stderr and exits with code 2. A dataset that cannot be opened, including an allowlisted `s3://` path, prints `qa-core-recon: error: <message>` to stderr and exits with code 2.

`run` arrives in a later PR.

## Exit Codes

| Code | Name | Meaning |
| ---- | ---- | ------- |
| 0 | `MATCH` | Success. For `validate`, the spec is usable. For `validate --check-data`, both datasets passed the data-level refusals. For a later `run`, the target matches the source. |
| 1 | `MISMATCH` | The target differs from the source. Reserved for `run`. |
| 2 | `REFUSED` | The tool declined to run, or the input could not be opened, so there is no result. |

`init` and `validate` use codes 0 and 2 only. `--check-data` is part of `validate`, so it stays on those codes too. An amount that cannot be represented at the spec scale is a refusal, exit 2, because the tool has no decimal to compare. Code 1 stays reserved for a dataset mismatch, so these commands never return it.

A malformed spec, a refusal, an unreadable spec file, an unreadable dataset, and any unexpected error all exit with code 2. An unexpected error inside the CLI is caught, printed as one `qa-core-recon: error:` line on stderr, and exits with code 2. The CLI prints no traceback.

## Refusal Codes

| Code | Meaning |
| ---- | ------- |
| `missing-key` | the spec has no business key, or the key is not unique and no rule says how to handle that. |
| `float-amount` | an amount column is float or double and the spec has no cast rule to a decimal scale. |
| `inexact-amount` | an amount value is not an exact decimal at the spec scale, or the column type cannot be read as a decimal. |
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
| `float-amount` | A Parquet amount column is float or double and that amount has no `cast` rule. |
| `inexact-amount` | An amount value is not an exact decimal at the spec scale, or the column type cannot be read as a decimal. |

First match wins, in this order:

1. Spec shape.
2. `missing-key`, then `tolerance-without-reason`, then `path-not-allowed`.
3. `sample-only`, then `unmapped-column`, then `float-amount`, then `inexact-amount`.

Within `unmapped-column`, `float-amount`, and `inexact-amount`, source is checked before target. On one side the roles are keys, then amounts, then `group_by`, then `hash_columns`, in the order written in the spec. A column named in more than one role is checked once, at its first role. For `inexact-amount`, the reported cell is the lexicographically smallest offending text in that column, so row order does not change the line. Text longer than 80 characters is cut there and marked with `...`.

## Readers

CSV and Parquet on local disk. S3 and Iceberg are later PRs. An allowlisted `s3://` path remains a valid spec, and plain `validate` accepts the packaged template. `--check-data` on an `s3://` path prints `qa-core-recon: error: s3 reads are not supported yet: ...` and exits 2. That is a read error, not `path-not-allowed`, because the allowlist check already passed. Source is named before target.

Every path is checked against the allowlist again before it is opened. The check uses the path text, the same way `validate` does. It does not resolve symlinks, and it does not join a relative path to the spec file. A relative path is opened from the current directory.

The Spark session is local mode on one thread (`local[1]`), with `spark.sql.shuffle.partitions` set to 1, the session time zone set to UTC, and the UI turned off. The driver binds to `127.0.0.1`. Arrow conversion is off. The catalog is in memory, so Hive is not started. Warehouse, derby, and scratch directories are created under a temporary directory. A run does not leave `metastore_db`, `derby.log`, or `spark-warehouse` in the working directory or the repo root. Spark's startup banner is dropped, because it carries a timestamp and is not part of the tool's result.

Column names are case sensitive. `Amount` does not satisfy a spec column named `amount`.

CSV is read from a file, or from a directory of part files. Marker files whose names start with `.` or `_` are skipped, so a Spark `_SUCCESS` file is not treated as a header. Every part file must use the header of the first file in sorted name order. The header becomes an explicit string schema, and `inferSchema` is off. Amount columns are parsed from those strings. A CSV amount is never a float, so a CSV file does not produce `float-amount`.

Parquet is read from a file or a directory, with `mergeSchema` off. The file schema is kept until amount columns are retyped.

Readers read every row. They do not sample, limit, or keep a head. `sampled: true` is refused even when the files contain a full extract, because the spec declared a sample.

Blank amount cells are null. They are not turned into zero. Null counts arrive with `run`.

A missing path, a CSV file that is not UTF-8, or a file that Spark cannot read is a read error (`qa-core-recon: error:`), exit 2. It is not a refusal code.

## Decimal Rules

Each amount column is read as Spark `decimal(38, scale)`. 38 is Spark's maximum decimal precision. `scale` comes from the spec and is an integer from 0 through 38. The integer part may use `38 - scale` digits. A value that needs more digits is `inexact-amount`. It is not truncated.

Parsing is exact. A value is accepted when it equals the same number quantized to the spec scale. At scale 2, `10`, `10.5`, `10.50`, and `10.500` are exact (`10.00` or `10.50`). `10.005` is refused. Trailing zeros add no new information. Extra non-zero fractional digits would have to be rounded, and this tool does not invent a rounded amount. Equivalent spellings of one number, such as `100` and `1e2`, are the same value.

That refusal is `inexact-amount`, exit 2. It is not a mismatch. A mismatch means the target differs from the source, and exit 1 is reserved for `run`. A value that does not fit the declared scale gives the tool no amount to compare.

`amounts[].cast` is optional. The only values are `half-even` and `half-up`. When it is present, a Parquet float or double amount column is rounded to the spec scale with that mode. The rule does not authorize rounding a CSV string, an integer, or a Parquet decimal. `10.005` at scale 2 is still `inexact-amount` when `cast` is set. Without `cast`, a float or double amount column is `float-amount` and its values are not read.

Non-finite values (`NaN`, `Infinity`) are `inexact-amount` even when a cast rule is present. A rounding mode does not define a decimal for them.

Integer Parquet columns are whole amounts. `1050` at scale 2 becomes `1050.00`. The reader does not treat an integer as minor units.

A column type other than string, decimal, integer, float, or double (for example boolean) is `inexact-amount`. Float and double without `cast` are `float-amount` instead, because that refusal is more specific.

## Spec File

`recon.yaml` is parsed with PyYAML's safe loader. A repeated key at any level is a malformed spec: the error names the key and the line, and the last value is not kept. The packaged template is a valid example. `init` writes it, and `validate --spec` on that file exits 0, because both example paths sit inside the example allowlist. The template target is `s3://`, so `validate --check-data` on the unmodified template reports that S3 reads are not supported yet.

| Field | Rule |
| ----- | ---- |
| `version` | Integer `1`. Any other version is rejected. |
| `source`, `target` | Each has `path` and `format`. Format is `csv` or `parquet`. |
| `keys` | List of column names. Absent, null, or empty is a `missing-key` refusal. |
| `amounts` | List of `{column, scale}` with optional `cast`. `scale` is an integer from 0 through 38. `cast` is `half-even` or `half-up`. The list may be empty. |
| `group_by`, `hash_columns` | Lists of column names. Either list may be empty. |
| `tolerance` | `value` and `reason`. |
| `sampled` | Boolean. `true` means the spec describes a sample. |
| `allowlist` | List of local paths or `s3://` URIs. An empty list allows no path. |

Unknown keys are rejected. Column names are non-empty single-line strings. Formats and the version number are exact: `csv`, `parquet`, and integer `1`.

`sampled` must be a real boolean. Write `true` or `false`. A string such as `"false"` is a malformed spec. A missing `sampled` is a malformed spec, so an omitted declaration cannot be treated as a full reconciliation. `sampled: true` loads successfully. `validate` prints `spec ok`. `validate --check-data` refuses with `sample-only`.

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
