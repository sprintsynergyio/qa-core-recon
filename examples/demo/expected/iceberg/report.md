# Reconciliation Report

## Result

Status: Match

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: 13131c4fc545c9583c145465c8379a8ee46f5277dd8f7b54a093e11e9a4aaf8b

## Row Counts

Source: 200
Target: 200

## Checks

### Row Count

Status: Match

Source rows: 200. Target rows: 200.

Grouped by txn_date.

Groups, in sorted order:

- txn_date=2024-03-01. Source rows: 40. Target rows: 40. Match.
- txn_date=2024-03-02. Source rows: 40. Target rows: 40. Match.
- txn_date=2024-03-03. Source rows: 40. Target rows: 40. Match.
- txn_date=2024-03-04. Source rows: 40. Target rows: 40. Match.
- txn_date=2024-03-05. Source rows: 40. Target rows: 40. Match.

### Amount Sum

Status: Match

Column amount, scale 2. Source sum: 110517.18. Target sum: 110517.18. Difference: 0.00. Tolerance: 0. Match.

Groups, in sorted order:

- txn_date=2024-03-01. Source sum: 24088.22. Target sum: 24088.22. Difference: 0.00. Match.
- txn_date=2024-03-02. Source sum: 27138.71. Target sum: 27138.71. Difference: 0.00. Match.
- txn_date=2024-03-03. Source sum: 19067.29. Target sum: 19067.29. Difference: 0.00. Match.
- txn_date=2024-03-04. Source sum: 18979.63. Target sum: 18979.63. Difference: 0.00. Match.
- txn_date=2024-03-05. Source sum: 21243.33. Target sum: 21243.33. Difference: 0.00. Match.

### Null Keys

Status: Match

Source rows with a null key: 0.
Target rows with a null key: 0.

### Duplicate Keys

Status: Match

Example limit: 20.

Source duplicate keys: 0.

Target duplicate keys: 0.
