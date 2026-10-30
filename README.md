# qa-core-recon

Prove that a target dataset matches its source after a Glue or PySpark job, using deterministic checks. No AI model decides pass or fail.

**Status: early scaffold.** Only `--version` and `init` work so far. Reconciliation arrives in later releases.

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

If the file already exists, `init` leaves it untouched and exits with code 2. Pass `--force` to overwrite it.

## Exit Codes

| Code | Name | Meaning |
| ---- | ---- | ------- |
| 0 | `MATCH` | The target matches the source. |
| 1 | `MISMATCH` | The target differs from the source. |
| 2 | `REFUSED` | The tool declined to run, so there is no result. |

## Refusal Codes

| Code | Meaning |
| ---- | ------- |
| `missing-key` | the spec has no business key, or the key is not unique and no rule says how to handle that. |
| `float-amount` | an amount column is float or double and the spec has no cast rule to a decimal scale. |
| `tolerance-without-reason` | a tolerance above zero is set with no written reason. |
| `unmapped-column` | a column named in the spec is missing from the source or the target. |
| `path-not-allowed` | a path is outside the allowlist (local disk and the moto S3 bucket in the MVP). |
| `sample-only` | the input was sampled, so a full reconciliation cannot be claimed. |
