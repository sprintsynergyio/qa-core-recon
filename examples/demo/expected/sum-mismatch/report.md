# Reconciliation Report

## Result

Status: Mismatch

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: 941f566d39b36363fe0cb08d9fd76b6661167ae68bbc4cd7d74595bbba1e0d67

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

Column `amount`, scale 2. Source sum: 110517.18. Target sum: 110617.18. Difference: 100.00. Tolerance: 0. Mismatch.

Groups, in sorted order:

- `txn_date`=`2024-03-01`. Source sum: 24088.22. Target sum: 24108.22. Difference: 20.00. Mismatch.
- `txn_date`=`2024-03-02`. Source sum: 27138.71. Target sum: 27158.71. Difference: 20.00. Mismatch.
- `txn_date`=`2024-03-03`. Source sum: 19067.29. Target sum: 19087.29. Difference: 20.00. Mismatch.
- `txn_date`=`2024-03-04`. Source sum: 18979.63. Target sum: 18999.63. Difference: 20.00. Mismatch.
- `txn_date`=`2024-03-05`. Source sum: 21243.33. Target sum: 21263.33. Difference: 20.00. Mismatch.

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

Changed keys: 200.
The first 20 of 200 are listed in sorted order.

- `txn_id`=`TXN000001`
- `txn_id`=`TXN000002`
- `txn_id`=`TXN000003`
- `txn_id`=`TXN000004`
- `txn_id`=`TXN000005`
- `txn_id`=`TXN000006`
- `txn_id`=`TXN000007`
- `txn_id`=`TXN000008`
- `txn_id`=`TXN000009`
- `txn_id`=`TXN000010`
- `txn_id`=`TXN000011`
- `txn_id`=`TXN000012`
- `txn_id`=`TXN000013`
- `txn_id`=`TXN000014`
- `txn_id`=`TXN000015`
- `txn_id`=`TXN000016`
- `txn_id`=`TXN000017`
- `txn_id`=`TXN000018`
- `txn_id`=`TXN000019`
- `txn_id`=`TXN000020`

### Changed Columns

Status: Mismatch

Example limit: 20.

Each count is the number of distinct keys whose value differs. Examples are listed in sorted order. When a key is duplicated, the compared row is the one with the smallest row hash.

Column `amount`. Distinct keys that differ: 200.
The first 20 of 200 are listed in sorted order.

- `txn_id`=`TXN000001`. Source: `143.06`. Target: `143.56`. Source token: `decimal:6:143.06`. Target token: `decimal:6:143.56`.
- `txn_id`=`TXN000002`. Source: `-562.37`. Target: `-561.87`. Source token: `decimal:7:-562.37`. Target token: `decimal:7:-561.87`.
- `txn_id`=`TXN000003`. Source: `1347.58`. Target: `1348.08`. Source token: `decimal:7:1347.58`. Target token: `decimal:7:1348.08`.
- `txn_id`=`TXN000004`. Source: `1638.15`. Target: `1638.65`. Source token: `decimal:7:1638.15`. Target token: `decimal:7:1638.65`.
- `txn_id`=`TXN000005`. Source: `611.98`. Target: `612.48`. Source token: `decimal:6:611.98`. Target token: `decimal:6:612.48`.
- `txn_id`=`TXN000006`. Source: `-375.17`. Target: `-374.67`. Source token: `decimal:7:-375.17`. Target token: `decimal:7:-374.67`.
- `txn_id`=`TXN000007`. Source: `1947.65`. Target: `1948.15`. Source token: `decimal:7:1947.65`. Target token: `decimal:7:1948.15`.
- `txn_id`=`TXN000008`. Source: `854.55`. Target: `855.05`. Source token: `decimal:6:854.55`. Target token: `decimal:6:855.05`.
- `txn_id`=`TXN000009`. Source: `618.54`. Target: `619.04`. Source token: `decimal:6:618.54`. Target token: `decimal:6:619.04`.
- `txn_id`=`TXN000010`. Source: `-1932.24`. Target: `-1931.74`. Source token: `decimal:8:-1932.24`. Target token: `decimal:8:-1931.74`.
- `txn_id`=`TXN000011`. Source: `1278.19`. Target: `1278.69`. Source token: `decimal:7:1278.19`. Target token: `decimal:7:1278.69`.
- `txn_id`=`TXN000012`. Source: `1010.79`. Target: `1011.29`. Source token: `decimal:7:1010.79`. Target token: `decimal:7:1011.29`.
- `txn_id`=`TXN000013`. Source: `916.47`. Target: `916.97`. Source token: `decimal:6:916.47`. Target token: `decimal:6:916.97`.
- `txn_id`=`TXN000014`. Source: `-229.05`. Target: `-228.55`. Source token: `decimal:7:-229.05`. Target token: `decimal:7:-228.55`.
- `txn_id`=`TXN000015`. Source: `1903.19`. Target: `1903.69`. Source token: `decimal:7:1903.19`. Target token: `decimal:7:1903.69`.
- `txn_id`=`TXN000016`. Source: `1934.52`. Target: `1935.02`. Source token: `decimal:7:1934.52`. Target token: `decimal:7:1935.02`.
- `txn_id`=`TXN000017`. Source: `1992.74`. Target: `1993.24`. Source token: `decimal:7:1992.74`. Target token: `decimal:7:1993.24`.
- `txn_id`=`TXN000018`. Source: `-991.24`. Target: `-990.74`. Source token: `decimal:7:-991.24`. Target token: `decimal:7:-990.74`.
- `txn_id`=`TXN000019`. Source: `1339.41`. Target: `1339.91`. Source token: `decimal:7:1339.41`. Target token: `decimal:7:1339.91`.
- `txn_id`=`TXN000020`. Source: `1080.60`. Target: `1081.10`. Source token: `decimal:6:1080.6`. Target token: `decimal:6:1081.1`.
