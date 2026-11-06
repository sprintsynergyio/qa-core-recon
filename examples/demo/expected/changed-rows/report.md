# Reconciliation Report

## Result

Status: Mismatch

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: 5de354b8584172d80ae315b0a466e7896e40c6164b04beefe93b3143ce777500

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

Status: Mismatch

Column `amount`, scale 2. Source sum: 110517.18. Target sum: 110523.43. Difference: 6.25. Tolerance: 0. Mismatch.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source sum: 24088.22. Target sum: 24089.47. Difference: 1.25. Mismatch.
- `txn_date`=`2024-03-02`. Source sum: 27138.71. Target sum: 27139.96. Difference: 1.25. Mismatch.
- `txn_date`=`2024-03-03`. Source sum: 19067.29. Target sum: 19068.54. Difference: 1.25. Mismatch.
- `txn_date`=`2024-03-04`. Source sum: 18979.63. Target sum: 18980.88. Difference: 1.25. Mismatch.
- `txn_date`=`2024-03-05`. Source sum: 21243.33. Target sum: 21244.58. Difference: 1.25. Mismatch.

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

Extra keys: 0.

Changed keys: 5.

- `txn_id`=`TXN000001`
- `txn_id`=`TXN000002`
- `txn_id`=`TXN000003`
- `txn_id`=`TXN000004`
- `txn_id`=`TXN000005`

### Changed Columns

Status: Mismatch

Example limit: 20.

Each count is the number of distinct keys whose value differs. Examples are listed in sorted order. When a key is duplicated, the compared row is the one with the smallest row hash.

Column `amount`. Distinct keys that differ: 5.

- `txn_id`=`TXN000001`. Source: `143.06`. Target: `144.31`. Source token: `decimal:6:143.06`. Target token: `decimal:6:144.31`.
- `txn_id`=`TXN000002`. Source: `-562.37`. Target: `-561.12`. Source token: `decimal:7:-562.37`. Target token: `decimal:7:-561.12`.
- `txn_id`=`TXN000003`. Source: `1347.58`. Target: `1348.83`. Source token: `decimal:7:1347.58`. Target token: `decimal:7:1348.83`.
- `txn_id`=`TXN000004`. Source: `1638.15`. Target: `1639.40`. Source token: `decimal:7:1638.15`. Target token: `decimal:6:1639.4`.
- `txn_id`=`TXN000005`. Source: `611.98`. Target: `613.23`. Source token: `decimal:6:611.98`. Target token: `decimal:6:613.23`.

Column `txn_type`. Distinct keys that differ: 5.

- `txn_id`=`TXN000001`. Source: `payment`. Target: `adjustment`. Source token: `string:7:payment`. Target token: `string:10:adjustment`.
- `txn_id`=`TXN000002`. Source: `refund`. Target: `adjustment`. Source token: `string:6:refund`. Target token: `string:10:adjustment`.
- `txn_id`=`TXN000003`. Source: `transfer`. Target: `adjustment`. Source token: `string:8:transfer`. Target token: `string:10:adjustment`.
- `txn_id`=`TXN000004`. Source: `fee`. Target: `adjustment`. Source token: `string:3:fee`. Target token: `string:10:adjustment`.
- `txn_id`=`TXN000005`. Source: `payment`. Target: `adjustment`. Source token: `string:7:payment`. Target token: `string:10:adjustment`.
