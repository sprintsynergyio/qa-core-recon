# qa-core-recon

Prove that a target dataset matches its source after a Glue or PySpark job, using deterministic checks. No AI model decides pass or fail.

**Status: spec loader.** `--version`, `init`, and `validate` work. `run` arrives in a later PR. Reconciliation itself is not in this release.

## Install From Source

Requires Python 3.11.

```bash
git clone https://github.com/sprintsynergyhq/qa-core-recon.git
cd qa-core-recon
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

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

A usable spec prints `spec ok` and exits with code 0. A refusal prints `refused: <code>: <message>` to stderr and exits with code 2. A malformed spec prints `qa-core-recon: error: <message>` to stderr and exits with code 2.

`run` arrives in a later PR.

## Exit Codes

| Code | Name | Meaning |
| ---- | ---- | ------- |
| 0 | `MATCH` | Success. For `validate`, the spec is usable. For a later `run`, the target matches the source. |
| 1 | `MISMATCH` | The target differs from the source. Reserved for `run`. |
| 2 | `REFUSED` | The tool declined to run, so there is no result. |

`init` and `validate` use codes 0 and 2 only. A malformed spec, a spec-level refusal, an unreadable spec file, and any unexpected error all exit with code 2. Code 1 stays reserved for a dataset mismatch, so these commands never return it.

An unexpected error inside the CLI is caught, printed as one `qa-core-recon: error:` line on stderr, and exits with code 2. The CLI prints no traceback.

## Refusal Codes

| Code | Meaning |
| ---- | ------- |
| `missing-key` | the spec has no business key, or the key is not unique and no rule says how to handle that. |
| `float-amount` | an amount column is float or double and the spec has no cast rule to a decimal scale. |
| `tolerance-without-reason` | a tolerance above zero is set with no written reason. |
| `unmapped-column` | a column named in the spec is missing from the source or the target. |
| `path-not-allowed` | a path is outside the allowlist (local disk and the moto S3 bucket in the MVP). |
| `sample-only` | the input was sampled, so a full reconciliation cannot be claimed. |

`validate` enforces the three spec-level codes below. `float-amount`, `unmapped-column`, and `sample-only` need a dataset, so they arrive with `run` in a later PR.

| Code | When Validate Refuses |
| ---- | --------------------- |
| `missing-key` | `keys` is absent, null, or empty. |
| `tolerance-without-reason` | `tolerance.value` is above zero and `reason` is empty or only whitespace. |
| `path-not-allowed` | `source.path` or `target.path` is outside `allowlist`. Source is checked first. |

Shape is checked before refusals. The first problem wins, in a fixed order, so the same file always prints the same line.

## Spec File

`recon.yaml` is parsed with PyYAML's safe loader. A repeated key at any level is a malformed spec: the error names the key and the line, and the last value is not kept. The packaged template is a valid example. `init` writes it, and `validate --spec` on that file exits 0, because both example paths sit inside the example allowlist.

| Field | Rule |
| ----- | ---- |
| `version` | Integer `1`. Any other version is rejected. |
| `source`, `target` | Each has `path` and `format`. Format is `csv` or `parquet`. |
| `keys` | List of column names. Absent, null, or empty is a `missing-key` refusal. |
| `amounts` | List of `{column, scale}`. `scale` is an integer of zero or greater. The list may be empty. |
| `group_by`, `hash_columns` | Lists of column names. Either list may be empty. |
| `tolerance` | `value` and `reason`. |
| `allowlist` | List of local paths or `s3://` URIs. An empty list allows no path. |

Unknown keys are rejected. Column names are non-empty single-line strings. Formats and the version number are exact: `csv`, `parquet`, and integer `1`.

`tolerance.value` is a monetary amount. It must be a quoted string, parsed with `decimal.Decimal`. A YAML float is an error and is never converted. A YAML integer is an error too, because only a quoted string keeps the exact digits. The value must be finite and zero or greater. Zero may have an empty reason. A value above zero needs a reason that contains something other than whitespace.

Paths are local paths or `s3://bucket/key` URIs. Any other scheme is a malformed spec, which exits 2 with `qa-core-recon: error:`, and is not a `path-not-allowed` refusal.

## Allowlist Matching

Matching compares normalised whole path components. It reads the path text in the spec. It does not look at the filesystem, so it does not resolve symlinks and it does not treat the path as relative to the spec file or the current directory.

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
