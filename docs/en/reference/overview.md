# Product Overview

PrivySpark is a Spark-based batch scanner that detects potential PII in a dataset path and writes aggregated result and error reports.

## Scope
- The public commands are `privyspark scan`, `privyspark review apply`, and `privyspark review collect`.
- Input paths must be absolute paths or URIs.
- Supported formats are `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, and archive families `zip`, `jar`, `tar`, `tar.gz/tgz`, `tar.bz2/tbz2`, `tar.xz/txz`, `tar.zst/tzst`, `7z`, and `rar`.
- Direct text-style data files (`csv`, `json/jsonl/ndjson`) wrapped by `gzip` or `bzip2` are passed through to Spark/Hadoop readers using the original path.
- CSV-like inputs are automatically probed for delimiter and header shape. Files without extensions and unsupported extensions are probed for `parquet`/`orc` magic bytes first; UTF-8 CSV-like inputs are promoted to `csv`, and remaining UTF-8 or EUC-KR text-like inputs are normalized into the internal `text` format.
- Only binary-looking unsupported inputs are recorded as `Unsupported file format`.
- `--ignore` and `--ignore-file` define scan exclusions by basename or input-root-relative path.
- `suppressions:` or `--suppress`, `--suppression-file` remove only selected `(column, pii_type)` result pairs.
- `--review-state-root` automatically collects `inbox/*.json` before the scan starts, updates cumulative offline-review state, applies the state allowlist, and writes `<output>/review/review.html` and `<output>/review/review.xlsm` by default. `--review-html-dir` can point the review file output directory to a separate absolute path or URI.

## Detection Model
- Detection uses ruleset-based regexes directly.
- Suppression keeps the rule enabled while filtering only the noisy column/type combinations.
- Invalid regexes are rejected during ruleset loading before the scan starts.
- Aggregated results include `match_count`, `sampled_row_count`, `match_ratio`, `non_empty_match_ratio`, `confidence`, `sample_raw_value`, and `sample_matched_fragment`.
- `sample_raw_value` stores only the matched fragment plus up to 50 characters of surrounding context on each side, not the entire cell value.

## Sampling and Scan Units
- `--sample-ratio` is row sampling.
- `--file-sample-ratio` selects a stable hash-ranked subset of files inside both batch scans and file-fallback scans.
- File sampling only applies when the group has more files than `--file-sample-min-files`.
- File sampling is a separate option so row sampling semantics stay intact while still reducing file reads for small-file-heavy groups and reflecting the operational concern that certain data may be concentrated in a single file. Review fingerprints for file-sampled groups cover only the sampled file scope.

## Outputs
- Result report: `scan_results`
- Error report: `scan_errors`
- Output formats: Parquet + CSV
- Offline review, when enabled: `<output>/review/review.html` and `<output>/review/review.xlsm` by default, or `review.html`/`review.xlsm` under the directory configured by `--review-html-dir`
- During long scans, intermediate JSONL shards may appear under `<output>/_progress/<run_id>`, but they are not the final consumer contract.

## Sample Datasets
- Input handling sample bundles are documented in [../../../samples/input-cases/README.md](../../../samples/input-cases/README.md).
- Regenerate them with `./gradlew generateSampleDatasets`.

## Next
- Input formats and grouping: [input-formats.md](input-formats.md)
- Rulesets and detection constraints: [rules-and-detection.md](rules-and-detection.md)
- Reports and errors: [reports-and-errors.md](reports-and-errors.md)
- False-positive review workflow: [review-workflow.md](review-workflow.md)
