# False-Positive Review Workflow

## Goal
- Reduce repeated noise from the same reviewed false positives.
- The review key is `(file_identifier, column_name, pii_type)`.
- If file metadata or checksum changes, the previous false-positive decision is automatically invalidated.

## 1. Initial Scan
```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output \
  --output-format excel
```

Operators only need to edit these columns in `scan_results`.
- `review_status`: `pending`, `false_positive`, `true_positive`
- `review_reason`: free-form note

## 2. Create or Update the Allowlist
```bash
privyspark review apply \
  --scan-results /abs/output/excel/scan_results.xlsx \
  --input-root /abs/input \
  --allowlist /abs/review/allowlist.jsonl \
  --reviewer reviewer@example.com
```

What this command does:
- Reads only rows where `review_status=false_positive`
- Resolves each `file_identifier` back to the current input file
- Expands directory identifiers only through the recorded `review_scope_file_identifiers`
- For directory review rows, requires recorded `review_scope_file_fingerprints` and compares each scoped file fingerprint before staging
- For non-directory rows, validates the recorded `file_size` and `file_mtime_epoch_ms`, then calculates current `CRC32`
- Upserts the latest review for the same key

Keys from the same `scan_results` input that are now marked `true_positive` or `pending` are removed from the existing allowlist.

With `--dry-run`, the command only reports staged entry counts and does not write the file.

## 3. Re-Scan
```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output-next \
  --allowlist /abs/review/allowlist.jsonl
```

During the next scan:
- If the allowlist key matches the current `(file_identifier, column_name, pii_type)` and
- `file_size`, `file_mtime_epoch_ms`, and `CRC32` still match,
- the result row is removed from the final `scan_results`.

If metadata or checksum changed, the row stays and carries:
- `review_status=pending`
- `review_reason=<previous note>`
- `review_invalidated=true`

## Notes
- Ignore rules and allowlists solve different problems. Ignore rules skip files before scanning, while allowlists suppress only reviewed false positives after detection.
- Archive entries keep the `<archive>!<entry>` identifier format and Excel sheets keep `<workbook>#<sheet>`.
- Password-protected archives, multi-volume RAR archives, and RAR5 archives are recorded explicitly in `scan_errors` during scanning.
