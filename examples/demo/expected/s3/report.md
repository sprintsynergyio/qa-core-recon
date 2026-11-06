# Reconciliation Report

## Result

Status: Match

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: 8d68ac8e5fc04de3b944c99a3b6578c23fcac9d6edcc80c99447471c6c3c9683

## Row Counts

Source: 200
Target: 200

## Checks

### Row Count

Status: Match

Source rows: 200. Target rows: 200.

Grouped by `txn_date`.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-02`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-03`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-04`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-05`. Source rows: 40. Target rows: 40. Match.

### Amount Sum

Status: Match

Column `amount`, scale 2. Source sum: 110517.18. Target sum: 110517.18. Difference: 0.00. Tolerance: 0. Match.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source sum: 24088.22. Target sum: 24088.22. Difference: 0.00. Match.
- `txn_date`=`2024-03-02`. Source sum: 27138.71. Target sum: 27138.71. Difference: 0.00. Match.
- `txn_date`=`2024-03-03`. Source sum: 19067.29. Target sum: 19067.29. Difference: 0.00. Match.
- `txn_date`=`2024-03-04`. Source sum: 18979.63. Target sum: 18979.63. Difference: 0.00. Match.
- `txn_date`=`2024-03-05`. Source sum: 21243.33. Target sum: 21243.33. Difference: 0.00. Match.

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

Compared columns, in hash order: `account_id`, `amount`, `currency`, `txn_date`, `txn_type`.

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

Each count is the number of distinct keys whose value differs. Examples are listed in sorted order. When a key is duplicated, the compared row is the one with the smallest row hash.

No compared column differs.
