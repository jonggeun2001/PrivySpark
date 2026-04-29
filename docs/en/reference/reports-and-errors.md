# Reports and Errors

## Final Output Paths
- Default result reports:
  - `<output>/parquet/scan_results`
- Default error reports:
  - `<output>/parquet/scan_errors`
- With `--output-format csv`:
  - `<output>/csv/scan_results`
  - `<output>/csv/scan_errors`
- With `--output-format excel`:
  - `<output>/excel/scan_results.xlsx`
  - `<output>/excel/scan_errors.xlsx`

`--output-format` can be repeated and supports `parquet`, `csv`, and `excel`. The default is `parquet`. The `_progress` directory is only for operational visibility and is not the public output contract.

## Result Fields
- `dataset_path`
- `scan_timestamp`
- `file_identifier`
- `column_name`
- `pii_type`
- `match_count`
- `sampled_row_count`
- `match_ratio`
- `non_empty_match_ratio`
- `confidence`
- `sample_raw_value`
- `sample_matched_fragment`
- `file_size`
- `file_mtime_epoch_ms`
- `hive_table_fqn`
- `review_status`
- `review_reason`
- `review_invalidated`
- `review_scope_file_identifiers`
- `review_scope_file_fingerprints`

`scan_results.scan_timestamp` is the UTC ISO-8601 time when each result row is actually materialized, not a fixed CLI start timestamp. Long-running scans and multi-group scans can therefore contain different values across result rows.

## `hive_table_fqn` Rules
- Lookup is enabled only when `--hive-metastore-jdbc-url`, `--hive-metastore-user`, and `--hive-metastore-password-file` are all provided.
- When enabled, the driver queries Hive Metastore `DBS`/`TBLS`/`SDS` tables once through the configured JDBC driver class and broadcasts a normalized URI-prefix index of table-level `LOCATION` values. The default driver class is `org.mariadb.jdbc.Driver`; set `--hive-metastore-jdbc-driver-class` or Spark conf `spark.privyspark.hiveMetastore.jdbcDriverClass` to override it. CLI values take precedence over Spark conf.
- If a result row's input file path is under a registered table `LOCATION`, PrivySpark writes the matched `db.table` value into `hive_table_fqn`.
- When table `LOCATION` values overlap, PrivySpark uses normalized-URI longest-prefix matching. Duplicate prefixes of the same length use deterministic ordering.
- Archive entry and Excel sheet identifiers are looked up by their host archive/workbook path, stripping `<archive>!<entry>` and `<workbook>#<sheet>` suffixes.
- If the options are omitted, JDBC connection/query/password-file reading fails, or no table matches, the field is an empty string `""`.
- Partition-level `LOCATION` overrides are not enumerated in this version. Only table-level `LOCATION` is used.

## `file_identifier` Rules
- The default is the input-relative path.
- Promotion to a directory-level identifier only happens when exact split confirms identical schemas, there are no pre-scan errors, and directory-level aggregation is allowed for the multi-file group.
- The input-root directory group uses `.`.
- Archive entries use `<archive>!<entry>`.
- Excel sheets use `<workbook>#<sheet>`.
- Single-file directories below the input root may promote to directory identifiers when the same safety checks pass.
- A single file at the input root and logical inputs keep file or logical identifiers.

Directory-level promotion is intentionally strict so the semantic unit of a result row does not drift. Aggregating too early would make result interpretation ambiguous when schema drift or pre-scan errors exist.

## Review Fields
- `file_size` stores the representative byte size for the row. File-level rows keep the file size, while directory-level rows keep the sum of included file sizes.
- `file_mtime_epoch_ms` stores the representative last-modified time in epoch milliseconds. Directory-level rows keep the maximum mtime across included files.
- `review_status` defaults to `pending`. Operators can edit it to `false_positive` or `true_positive`.
- `review_reason` stores the operator note. It should be filled when a row is marked `false_positive`.
- `review_invalidated=true` means the same `(dataset_path, file_identifier, column_name, pii_type)` tuple existed in the allowlist before, but the current file metadata and checksum no longer match and the row should be reviewed again.
- `review_scope_file_identifiers` stores the concrete file identifiers included in a directory-level row. It is encoded as a `|`-delimited string, and `review apply` expands only this recorded scope.
- `review_scope_file_fingerprints` stores the recorded per-file fingerprint snapshot for directory-level rows. It uses an internal encoded string format and `review apply` requires every scoped file fingerprint to match before staging a false-positive review.
- When `--allowlist` is not provided, all review fields stay at their default values.

## Ratio Fields
- `match_ratio` is based on sampled rows.
- `sampled_row_count` is the post-sampling row count that was actually scanned.
- `non_empty_match_ratio` uses only non-empty values in the column as its denominator.
- Empty means `null` or a value whose `trim(column)` is blank.
- `full_column` only changes how `match_count` is computed. `confidence` is still calculated against non-empty values for the column.
- `confidence` is the lower bound of the 95% Wilson score interval (z=1.96) for `match_count / non_empty_count`. Smaller samples are penalized more conservatively, and larger samples converge toward `non_empty_match_ratio`.
- `sample_matched_fragment` stores one raw fragment that actually matched the regex and validator path.
- `sample_raw_value` stores only the matched fragment plus up to 50 characters of surrounding context on each side.
- Both values are rounded to two decimal places.

## Error Reports
- File and group failures are accumulated without aborting the entire scan.
- Read errors caused by file replacement or deletion are retried before being recorded.
- Corrupt JSON, nested archives, unsafe archive paths, password-protected archives, multi-volume RAR archives, RAR5 archives, and unsupported inputs that fail magic-byte/text fallback are recorded as explicit errors.

## In-Progress `_progress` Path
- Intermediate shards may be written under `<output>/_progress/<run_id>/results/*.jsonl`, `errors/*.jsonl`, and `meta/completions/*.jsonl`.
- While a task is running, `<output>/_progress/<run_id>/in-flight/*.json` may contain one marker per active group, file, or allowlist snapshot rescan.
- In-flight markers are operational diagnostics only. Completed work and recoverable failures remove their markers, while unrecovered group/file failures that end the Spark application as `FAILED` preserve the marker.
- In-flight marker filenames preserve filesystem-safe UTF-8 letters/digits plus `.`, `_`, and `-`; path separators and other characters are replaced with `_`. The original `identifier` remains in the marker JSON body.
- Clean completions produce completion markers without result or error rows.
- On normal completion, PrivySpark merges `_progress` into the selected final output formats and removes `_progress/<run_id>`.

The separate progress path serves two purposes: it exposes already completed work during long scans, and it keeps partial results away from the final consumer-facing report locations.

## Sample Value Storage Policy
- `scan_results` stores one raw sample to make each result row easier to interpret.
- `sample_matched_fragment` keeps the exact detected fragment.
- `sample_raw_value` keeps only bounded context around that fragment instead of the entire cell.
- Error reports still contain metadata only.
