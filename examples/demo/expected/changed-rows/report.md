# Reconciliation Report

## Result

Status: Mismatch

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: 93de347bb7fa641d4e1bee1a9248c87876e476557a90cb02cabbf2c6de029fdf

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

Status: Mismatch

Column amount, scale 2. Source sum: 110517.18. Target sum: 110523.43. Difference: 6.25. Tolerance: 0. Mismatch.

Groups, in sorted order:

- txn_date=2024-03-01. Source sum: 24088.22. Target sum: 24089.47. Difference: 1.25. Mismatch.
- txn_date=2024-03-02. Source sum: 27138.71. Target sum: 27139.96. Difference: 1.25. Mismatch.
- txn_date=2024-03-03. Source sum: 19067.29. Target sum: 19068.54. Difference: 1.25. Mismatch.
- txn_date=2024-03-04. Source sum: 18979.63. Target sum: 18980.88. Difference: 1.25. Mismatch.
- txn_date=2024-03-05. Source sum: 21243.33. Target sum: 21244.58. Difference: 1.25. Mismatch.

### Null Keys

Status: Match

Source rows with a null key: 0.
Target rows with a null key: 0.

### Duplicate Keys

Status: Match

Example limit: 20.

Source duplicate keys: 0.

Target duplicate keys: 0.
