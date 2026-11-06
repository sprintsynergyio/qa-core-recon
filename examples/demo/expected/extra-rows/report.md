# Reconciliation Report

## Result

Status: Mismatch

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: a9b3ae8ad899741f58d92fab9692db89d16261d5c71962bbdc63020a287ec50c

## Row Counts

Source: 200
Target: 203

## Checks

### Row Count

Status: Mismatch

Source rows: 200. Target rows: 203.

Grouped by `txn_date`.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-02`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-03`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-04`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-05`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-06`. Source rows: 0. Target rows: 3. This group is missing on the source. Mismatch.

### Amount Sum

Status: Mismatch

Column `amount`, scale 2. Source sum: 110517.18. Target sum: 110597.93. Difference: 80.75. Tolerance: 0. Mismatch.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source sum: 24088.22. Target sum: 24088.22. Difference: 0.00. Match.
- `txn_date`=`2024-03-02`. Source sum: 27138.71. Target sum: 27138.71. Difference: 0.00. Match.
- `txn_date`=`2024-03-03`. Source sum: 19067.29. Target sum: 19067.29. Difference: 0.00. Match.
- `txn_date`=`2024-03-04`. Source sum: 18979.63. Target sum: 18979.63. Difference: 0.00. Match.
- `txn_date`=`2024-03-05`. Source sum: 21243.33. Target sum: 21243.33. Difference: 0.00. Match.
- `txn_date`=`2024-03-06`. Source sum: none. Target sum: 80.75. Difference: none. This group is missing on the source. Mismatch.

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

Status: Mismatch

Compared columns, in hash order: `account_id`, `amount`, `currency`, `txn_date`, `txn_type`.

Rows with a null in any key column are excluded.
Counts are by distinct key value.
When a key is duplicated, the row with the smallest row hash is used.

Example limit: 20.

Missing keys: 0.

Extra keys: 3.

- `txn_id`=`TXN900001`
- `txn_id`=`TXN900002`
- `txn_id`=`TXN900003`

Changed keys: 0.

### Changed Columns

Status: Match

Example limit: 20.

Each count is the number of distinct keys whose value differs. Examples are listed in sorted order. When a key is duplicated, the compared row is the one with the smallest row hash.

No compared column differs.
