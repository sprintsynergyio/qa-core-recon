# Changelog

## 0.1.0

First release of the checks that are on main.

### Command Line

- `qa-core-recon --version` prints the version.
- `init` writes a commented spec template. It exits 2 when the file exists, the path is a directory, the parent is missing, or the write is denied.
- `validate` checks that a spec loads. `validate --check-data` opens local CSV, Parquet, and Iceberg on local disk and on S3 and runs the data-level refusals.
- `run` compares row counts, amount sums, null keys, duplicate keys, the key diff, and changed columns. It writes `report.json` and `report.md`.

### Checks And Reports

- Amounts are exact decimals at the spec scale. A YAML float is rejected. Null amounts are left out of a sum. A blank amount cell is empty or holds only spaces and tabs; other whitespace-only cells are `inexact-amount`.
- Tolerance is a quoted decimal. It applies to each total and each group sum on its own.
- The key diff lists missing, extra, and changed keys. Changed columns name which compared fields differ. Example keys are capped at 20.
- `report.json` is canonical: sorted keys, indent 2, UTF-8, and one trailing newline.
- `report.md` uses Title Case headings. Dataset text in the markdown report is escaped. Neither report file has a timestamp.
- Exit code 0 is a match. Exit code 1 is a mismatch. Exit code 2 is a refusal or a read error.

### Spec And Refusals

- Spec version 1. Formats are `csv`, `parquet`, and `iceberg`.
- Refusal codes are `missing-key`, `float-amount`, `inexact-amount`, `tolerance-without-reason`, `unmapped-column`, `path-not-allowed`, and `sample-only`.
- Paths are checked against the allowlist before a file is opened.

### Demo

- `examples/demo` is a made-up payments dataset with a fixed seed. Specs cover match, missing rows, extra rows, changed rows, a sum mismatch, Iceberg, and S3 (moto). Expected reports are committed.
