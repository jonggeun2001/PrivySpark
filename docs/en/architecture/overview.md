# Architecture Overview

## Goals
- Scan large datasets reliably with Spark-based batch processing.
- Improve efficiency through input expansion and grouping without losing identifier semantics.
- Continue processing as much as possible even when some files or groups fail.

## Components
- `Cli.scala`: CLI arguments and default execution options
- `FormatDetector.scala`: first-stage format detection by extension
- `RulesetLoader.scala`: built-in and external ruleset loading and validation
- `DriverLogger.scala`: driver log level parsing and structured log format
- `DetectionAggregator.scala`: metric aggregation and fallback strategies
- `PrivySparkApp.scala`: input expansion, grouping, exact split, scan orchestration, progress/final report writing
- `Models.scala`: result, error, and ruleset models

## Development and Verification Tools
- `src/test/scala/io/github/jonggeun2001/privyspark/SampleDatasetGenerator.scala`: sample dataset generator for reproducing input-handling branches
- `generateSampleDatasets` and `packageSampleDatasets` in `build.gradle.kts`: regeneration and release packaging tasks for sample datasets

## Processing Flow
1. validate input path
2. load ruleset and pre-validate regexes
3. collect physical files
4. expand archive entries and workbook sheets, probe magic bytes, normalize text fallback
5. build first-pass groups by `(directory, format)`
6. sample a representative file for schema detection
7. perform schema-aware split and determine whether directory identifiers are safe
8. exact-split revalidate sampled multi-file groups before scanning
9. acquire `<output>/_progress-preparing.json`, prepare `<output>/_progress/<run_id>`, clean stale progress
10. batch scan non-sampled batch-capable groups, optionally with file sampling
11. direct-file scan non-sampled `xlsx` groups
12. fall back to file-level scanning if a normal group batch scan fails
13. write progress JSONL shards when a group or file completes
14. merge progress JSONL into final `scan_results` and `scan_errors`, then remove `_progress/<run_id>`

## Operational Invariants
- `scan_results` stores two interpretation aids: `sample_matched_fragment` keeps the detected fragment itself, and `sample_raw_value` keeps only up to 50 characters of surrounding context on each side.
- `--pre-scan-parallelism` applies to input expansion, format probing, and schema split.
- Effective pre-scan parallelism is bounded by the discovered file count and the safety ceiling `64`.
- `xlsx` file-level scans also flow through `scanGroupByFile`, so they consume CLI `--file-parallelism` or `spark.privyspark.fileParallelism`.
- `--file-sample-ratio` only applies to batch-capable group scans and uniformly samples at least one file using `ceil(fileCount * ratio)`.
- When `--file-sample-ratio` is active, `--sample-ratio < 1.0` is ignored for that batch-capable group and a warning is logged.
- Sampled groups are never promoted to directory-level identifiers before exact-split validation.
- Archive and Excel logical inputs keep their own identifiers.
- The public output contract remains `parquet/scan_results`, `parquet/scan_errors`, `csv/scan_results`, and `csv/scan_errors`.
- Clean completions also emit `meta/completions` markers.
- `_progress` is cleaned based on staleness when the next run starts. There is no shutdown hook cleanup.

## Why It Works This Way
- Keeping `_progress` separate from final outputs preserves both observability and final report integrity.
- Cleanup happens on the next run instead of a shutdown hook because forced YARN termination and `kill -9` make shutdown hooks unreliable.
- `_progress-preparing.json` exists so concurrent startup cannot delete another run's freshly created progress root before the active marker is ready.
- The owner run can self-heal an unreadable `active-run.json` from `meta/run.json` so a damaged marker does not unnecessarily kill a live run.
