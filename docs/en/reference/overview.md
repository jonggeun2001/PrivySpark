# Product Overview

PrivySpark is a Spark-based batch scanner that detects potential PII in a dataset path and writes aggregated result and error reports.

## Scope
- The product exposes a single entrypoint: `privyspark scan`.
- Input paths must be absolute paths or URIs.
- Supported formats are `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, and `jar`.
- Files without extensions and unsupported extensions are probed for `parquet`/`orc` magic bytes first. Text-like inputs are normalized into the internal `text` format.
- Only binary-looking unsupported inputs are recorded as `Unsupported file format`.

## Detection Model
- Detection uses ruleset-based regexes plus strict validators for selected PII types.
- Invalid regexes are rejected during ruleset loading before the scan starts.
- Aggregated results include `match_count`, `sampled_row_count`, `match_ratio`, `non_null_match_ratio`, `confidence`, `sample_raw_value`, and `sample_matched_fragment`.
- `sample_raw_value` stores only the matched fragment plus up to 50 characters of surrounding context on each side, not the entire cell value.

## Sampling and Scan Units
- `--sample-ratio` is row sampling.
- `--file-sample-ratio` uniformly samples files inside a batch-capable group.
- File sampling is a separate option so row sampling semantics stay intact while still reducing file reads for small-file-heavy groups and reflecting the operational concern that certain data may be concentrated in a single file.

## Outputs
- Result report: `scan_results`
- Error report: `scan_errors`
- Output formats: Parquet + CSV
- During long scans, intermediate JSONL shards may appear under `<output>/_progress/<run_id>`, but they are not the final consumer contract.

## Sample Datasets
- Input handling sample bundles are documented in [../../../samples/input-cases/README.md](../../../samples/input-cases/README.md).
- Regenerate them with `./gradlew generateSampleDatasets`.

## Next
- Input formats and grouping: [input-formats.md](input-formats.md)
- Rulesets and detection constraints: [rules-and-detection.md](rules-and-detection.md)
- Reports and errors: [reports-and-errors.md](reports-and-errors.md)
