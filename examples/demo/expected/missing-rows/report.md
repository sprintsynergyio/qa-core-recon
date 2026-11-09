# Reconciliation Report

## Result

Status: Mismatch

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: a6e040dc19396e6c4140e9c29b749221fdbad781b8e41516e1c080527f85ce41

## Row Counts

Source: 200
Target: 160

## Checks

### Row Count

Status: Mismatch

Source rows: 200. Target rows: 160.

Grouped by `txn_date`.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source rows: 40. Target rows: 0. This group is missing on the target. Mismatch.
- `txn_date`=`2024-03-02`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-03`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-04`. Source rows: 40. Target rows: 40. Match.
- `txn_date`=`2024-03-05`. Source rows: 40. Target rows: 40. Match.

### Amount Sum

Status: Mismatch

Column `amount`, scale 2. Source sum: 110517.18. Target sum: 86428.96. Difference: 24088.22. Tolerance: 0. Mismatch.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source sum: 24088.22. Target sum: none. Difference: none. This group is missing on the target. Mismatch.
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

Status: Mismatch

Compared columns, in hash order: `account_id`, `amount`, `currency`, `txn_date`, `txn_type`.

Rows with a null in any key column are excluded.
Counts are by distinct key value.
When a key is duplicated, the row with the smallest row hash is used.

Example limit: 20.

Missing keys: 40.
The first 20 of 40 are listed in sorted order.

- `txn_id`=`TXN000001`
- `txn_id`=`TXN000006`
- `txn_id`=`TXN000011`
- `txn_id`=`TXN000016`
- `txn_id`=`TXN000021`
- `txn_id`=`TXN000026`
- `txn_id`=`TXN000031`
- `txn_id`=`TXN000036`
- `txn_id`=`TXN000041`
- `txn_id`=`TXN000046`
- `txn_id`=`TXN000051`
- `txn_id`=`TXN000056`
- `txn_id`=`TXN000061`
- `txn_id`=`TXN000066`
- `txn_id`=`TXN000071`
- `txn_id`=`TXN000076`
- `txn_id`=`TXN000081`
- `txn_id`=`TXN000086`
- `txn_id`=`TXN000091`
- `txn_id`=`TXN000096`

Extra keys: 0.

Changed keys: 0.

### Changed Columns

Status: Match

Example limit: 20.

Each count is the number of distinct keys whose value differs. Examples are listed in sorted order. When a key is duplicated, the compared row is the one with the smallest row hash.

No compared column differs.
