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

`--output-format` can be repeated and supports `parquet`, `csv`, and `excel`. The default is `parquet`.

Explicit formats replace the default selection. For example, `--output-format csv` writes CSV only. Use `--output-format parquet --output-format csv` when both are needed. The `_progress` directory is only for operational visibility and is not the public output contract.

## Reruns and Output Replacement

Rerunning with the same `<output>` writes fresh reports under `_report_staging/<id>`, backs up existing outputs, then promotes the new outputs. Success also removes format directories not selected for the new run. Use a separate output path per run to retain history. On promotion failure, the writer attempts to restore the previous outputs; if restoration also fails, it preserves the backup staging path and logs `report_output_rollback_failed`.

Parquet/CSV outputs are directories containing Spark `part-*` files. CSV includes a header; Excel writes `scan_results` and `scan_errors` sheets in separate `.xlsx` files. Empty results still retain the selected formats and result/error schemas.

## Result Fields

- `dataset_path`
- `scan_timestamp`
- `file_identifier`
- `column_name`
- `pii_type`
- `match_count`
- `sampled_row_count`
- `non_empty_value_count`
- `match_ratio`
- `non_empty_match_ratio`
- `confidence`
- `sample_raw_value`
- `sample_matched_fragment`
- `file_size`
- `file_mtime_epoch_ms`
- `hive_table_fqn`
- `aggregated`
- `aggregated_file_count`
- `aggregated_partition_count`
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
- When the input `--path` exactly matches a registered table-level `LOCATION` and no ignore match is found, that scan group is read through `spark.table("db.table")`, and both raw progress and final results use the table-root identifier. If an ignore match is found, PrivySpark keeps the existing physical file scan path. If Spark Catalog cannot resolve the same `db.table`, the failure is recorded in `scan_errors`.
- Final `scan_results` groups rows that have `hive_table_fqn` by `dataset_path`, `hive_table_fqn`, `column_name`, and `pii_type`. Repeated partition/file rows become one table-level row, `match_count`, `sampled_row_count`, and `non_empty_value_count` are summed, and `match_ratio`, `non_empty_match_ratio`, and `confidence` are recalculated.
- Table-level rows store the partition-stripped table-root identifier in `file_identifier` and expose the grouped size through `aggregated=true`, `aggregated_file_count`, and `aggregated_partition_count`. Results without Hive mapping keep the existing file or directory identifier unit.
- When table `LOCATION` values overlap, PrivySpark uses normalized-URI longest-prefix matching. Duplicate prefixes of the same length use deterministic ordering.
- Archive entry and Excel sheet identifiers are looked up by their host archive/workbook path, stripping `<archive>!<entry>` and `<workbook>#<sheet>` suffixes.
- If the options are omitted, JDBC connection/query/password-file reading fails, or no table matches, the field is an empty string `""`.
- Partition-level `LOCATION` overrides are not enumerated in this version. Only table-level `LOCATION` is used.

## `file_identifier` Rules

- The default is the input-relative path.
- Promotion to a directory-level identifier only happens when exact split confirms identical schemas, there are no pre-scan errors, and directory-level aggregation is allowed for the multi-file group.
- Sampled `text` groups and sampled Parquet/ORC/Avro groups that pass bounded schema validation keep file-level identifiers on the batch path because they have not been promoted through exact-split directory aggregation.
- The input-root directory group uses `.`.
- Partition, bucket, and skew/list-bucketing layout directories are treated as layout metadata for grouping, so eligible rows identify the normalized table path rather than each physical layout subdirectory.
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
- `review_invalidated` is a compatibility field for legacy exact-fingerprint mismatches. The current recurring-only matcher does not invalidate reviews based on size, mtime, or checksum and does not set this field to `true` in new scans.
- `review_scope_file_identifiers` stores the concrete file identifiers included in a directory-level or Hive table-level row. Each identifier is UTF-8 URL-encoded before joining with `|`, and `review apply` expands only this recorded scope.
- `review_scope_file_fingerprints` stores the recorded per-file fingerprint snapshot for directory-level or Hive table-level rows. It uses an internal encoded string format and `review apply` requires every scoped file fingerprint to match before staging a false-positive review.
- New scans use `pending`, an empty string, and `false` for `review_status`, `review_reason`, and `review_invalidated`. Findings matching recurring entries from `--allowlist` or `--review-state-root` are removed from the results. Fingerprint/scope fields and legacy `review apply` file-generation compatibility remain available.

## Ratio Fields

- `match_ratio` is based on sampled rows.
- `sampled_row_count` is the post-sampling row count that was actually scanned.
- `non_empty_value_count` is the number of non-empty values used as the denominator for `non_empty_match_ratio` and `confidence`.
- `non_empty_match_ratio` uses only non-empty values in the column as its denominator.
- Empty means `null` or a value whose `trim(column)` is blank.
- `full_column` only changes how `match_count` is computed. `confidence` is still calculated against non-empty values for the column.
- `confidence` is the lower bound of the 95% Wilson score interval (z=1.96) for `match_count / non_empty_count`. Smaller samples are penalized more conservatively, and larger samples converge toward `non_empty_match_ratio`.
- `sample_matched_fragment` stores one raw fragment that actually matched the regex.
- `sample_raw_value` stores only the matched fragment plus up to 50 characters of surrounding context on each side.
- `match_ratio`, `non_empty_match_ratio`, and `confidence` are numbers between `0` and `1`, rounded to two decimal places with `HALF_UP`. Sample strings are not rounded. The review HTML separately displays `match_count / sampled_row_count * 100` as a percentage.

## Error Reports

- Recoverable file and group failures are accumulated while processing continues. Unrecovered failures can still terminate the application.
- Read errors caused by file replacement or deletion are retried before being recorded.
- Corrupt JSON, nested archives, unsafe archive paths, password-protected archives, multi-volume RAR archives, RAR5 archives, extensions excluded from probing, and inputs that fail magic-byte/CSV/text fallback are recorded as explicit errors.

The `scan_errors` fields are `dataset_path`, `scan_timestamp`, `file_identifier`, and `error_message`. Zero-byte inputs and files deleted between discovery and pre-scan do not create error rows.

## In-Progress `_progress` Path

- Intermediate shards may be written under `<output>/_progress/<run_id>/results/*.jsonl`, `errors/*.jsonl`, and `meta/completions/*.jsonl`.
- File fallback scans flush progress shards when the group finishes by default. `_progress` remains the final merge source, but it is not a per-file live-tail contract; set `spark.privyspark.progress.flushMode=file` if shards must appear immediately after each file completes.
- While a task is running, `<output>/_progress/<run_id>/in-flight/*.json` may contain group and allowlist snapshot rescan markers. File markers are off by default; enable them with `spark.privyspark.progress.fileMarker.enabled=true`.
- In-flight markers are operational diagnostics only. Completed work and recoverable failures remove their markers, while unrecovered group/file failures that end the Spark application as `FAILED` preserve the marker.
- In-flight marker filenames preserve filesystem-safe UTF-8 letters/digits plus `.`, `_`, and `-`; path separators and other characters are replaced with `_`. The original `identifier` remains in the marker JSON body.
- Clean completions produce completion markers without result or error rows.
- On normal completion, PrivySpark merges `_progress`, applies final Hive table-level result aggregation, writes the selected final output formats, and removes `_progress/<run_id>`. Intermediate `_progress` JSONL files are raw diagnostic shards, not the final consumer contract.

The separate progress path serves two purposes: it exposes already completed work during long scans, and it keeps partial results away from the final consumer-facing report locations.

## Sample Value Storage Policy

- `scan_results` stores one raw sample to make each result row easier to interpret.
- `sample_matched_fragment` keeps the exact detected fragment.
- `sample_raw_value` keeps only bounded context around that fragment instead of the entire cell.
- Error reports store paths, timestamps, identifiers, and `error_message` without dedicated sample fields. The error message may include a reader exception message.

`--review-sample-mode` applies only to samples embedded in the generated HTML and exported from it as CSV/response JSON. Default `masked` partially masks the detected fragment while retaining surrounding context; `raw` passes through the stored sample and `none` emits empty sample strings. Original Parquet/CSV/Excel `scan_results` retain the raw-sample policy above in every mode. This option does not automatically mask other sensitive values in the surrounding context.
