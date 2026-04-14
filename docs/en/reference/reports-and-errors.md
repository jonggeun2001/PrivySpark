# Reports and Errors

## Final Output Paths
- Result reports:
  - `<output>/parquet/scan_results`
  - `<output>/csv/scan_results`
- Error reports:
  - `<output>/parquet/scan_errors`
  - `<output>/csv/scan_errors`

PrivySpark always writes final outputs in both Parquet and CSV. The `_progress` directory is only for operational visibility and is not the public output contract.

## Result Fields
- `dataset_path`
- `scan_timestamp`
- `file_identifier`
- `column_name`
- `pii_type`
- `match_count`
- `match_ratio`
- `non_null_match_ratio`
- `confidence`

## `file_identifier` Rules
- The default is the input-relative path.
- Promotion to a directory-level identifier only happens when exact split confirms identical schemas, there are no pre-scan errors, and directory-level aggregation is allowed for the multi-file group.
- The input-root directory group uses `.`.
- Archive entries use `<archive>!<entry>`.
- Excel sheets use `<workbook>#<sheet>`.
- Single-file groups and logical inputs keep file or logical identifiers.

Directory-level promotion is intentionally strict so the semantic unit of a result row does not drift. Aggregating too early would make result interpretation ambiguous when schema drift or pre-scan errors exist.

## Ratio Fields
- `match_ratio` is based on sampled rows.
- `non_null_match_ratio` uses only non-null values in the column as its denominator.
- `full_column` only changes how `match_count` is computed. The denominator for `match_ratio` and `confidence` still uses sampled row count.
- `confidence` currently equals `match_ratio`.
- Both values are rounded to two decimal places.

## Error Reports
- File and group failures are accumulated without aborting the entire scan.
- Read errors caused by file replacement or deletion are retried before being recorded.
- Corrupt JSON, nested archives, unsafe archive paths, and unsupported inputs that fail magic-byte/text fallback are recorded as explicit errors.

## In-Progress `_progress` Path
- Intermediate shards may be written under `<output>/_progress/<run_id>/results/*.jsonl`, `errors/*.jsonl`, and `meta/completions/*.jsonl`.
- Clean completions produce completion markers without result or error rows.
- On normal completion, PrivySpark merges `_progress` into final Parquet/CSV reports and removes `_progress/<run_id>`.

The separate progress path serves two purposes: it exposes already completed work during long scans, and it keeps partial results away from the final consumer-facing report locations.

## Security Guarantees
- Raw PII values are never stored.
- Only aggregated metadata and error metadata are written.
