# Reconciliation Report

## Result

Status: Mismatch

Tool: qa-core-recon
Version: 0.1.0
Spec Hash: ed9c8b7f2f787831073ef59a465a9625ce9b4fe64fe8e6bb2eaf654c18bef7c2

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

Column amount, scale 2. Source sum: 110517.18. Target sum: 110617.18. Difference: 100.00. Tolerance: 0. Mismatch.

Groups, in sorted order:

- txn_date=2024-03-01. Source sum: 24088.22. Target sum: 24108.22. Difference: 20.00. Mismatch.
- txn_date=2024-03-02. Source sum: 27138.71. Target sum: 27158.71. Difference: 20.00. Mismatch.
- txn_date=2024-03-03. Source sum: 19067.29. Target sum: 19087.29. Difference: 20.00. Mismatch.
- txn_date=2024-03-04. Source sum: 18979.63. Target sum: 18999.63. Difference: 20.00. Mismatch.
- txn_date=2024-03-05. Source sum: 21243.33. Target sum: 21263.33. Difference: 20.00. Mismatch.

### Null Keys

Status: Match

Source rows with a null key: 0.
Target rows with a null key: 0.

### Duplicate Keys

Status: Match

Example limit: 20.

Source duplicate keys: 0.

Target duplicate keys: 0.
