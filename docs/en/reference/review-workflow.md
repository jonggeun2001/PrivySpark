# Legacy Review Apply Workflow

`privyspark review apply` is the legacy workflow where an operator edits `scan_results` directly and generates a fingerprint-based exact allowlist JSONL file.

The default offline review workflow is now the recurring review state described in the Korean reference [offline-review-collector.md](../../ko/reference/offline-review-collector.md). Scans that use `--review-state-root` apply only `entry_type=recurring` false-positive state. Legacy exact entries are not used for suppression.

## Legacy Command

```bash
privyspark review apply \
  --scan-results /abs/output/excel/scan_results.xlsx \
  --input-root /abs/input \
  --allowlist /abs/review/allowlist.jsonl \
  --reviewer reviewer@example.com
```

This command still writes exact entries for backward file-generation compatibility, but the recurring collector is the recommended workflow for repeated batch false positives.

## Recommended Workflow

```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output \
  --review-state-root /abs/review-state
```

Upload returned response JSON files into `/abs/review-state/inbox/*.json`. The next `scan --review-state-root /abs/review-state` automatically collects them before scanning. If any response is invalid or another collect holds `/abs/review-state/.collect.lock`, the scan fails before the scan work starts. When detections make the review larger than 2MB, `review.html` becomes an index and `review-part-*.html` files are generated; reviewers create one response JSON from each part file and upload all of them to the inbox. For Excel editing, reviewers can download the CSV from the review file, import the decrypted CSV file, or paste the TSV clipboard text copied from Excel. CSV upload preserves quoted commas and embedded line breaks; TSV paste uses tabs and row breaks, preserving embedded line breaks when Excel wraps the cell in double quotes.
