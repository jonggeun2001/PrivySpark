# Architecture Overview

## Goals
- Scan large datasets reliably with Spark-based batch processing.
- Improve efficiency through input expansion and grouping without losing identifier semantics.
- Continue processing as much as possible even when some files or groups fail.

## Components
- `cli/Cli.scala`: CLI arguments and default execution options
- `format/FormatDetector.scala`: first-stage format detection by extension
- `format/CompressionStreams.scala`: codec wrapping for direct compressed text-style files and compressed tar streams
- `RulesetLoader.scala`: built-in/external ruleset loading, suppression loading, and regex validation
- `util/DriverLogger.scala`: driver log level parsing and structured log format
- `detect/DetectionAggregator.scala`: metric aggregation and fallback strategies
- `hive/HiveTableLookup.scala`: Hive table `LOCATION` normalization, longest-prefix lookup indexing, and broadcast creation
- `scan/DirectoryScanner.scala`, `scan/GroupScanner.scala`: input expansion, grouping, and scan execution
- `report/ReportWriter.scala`: final report writing and format-specific outputs
- `PrivySparkApp.scala`: input expansion, grouping, exact split, scan orchestration, progress/final report writing
- `Models.scala`: result, error, and ruleset models

## Development and Verification Tools
- `src/test/scala/io/github/jonggeun2001/privyspark/SampleDatasetGenerator.scala`: sample dataset generator for reproducing input-handling branches
- `generateSampleDatasets` and `packageSampleDatasets` in `build.gradle.kts`: regeneration and release packaging tasks for sample datasets

## Processing Flow
1. validate input path
2. load ruleset, pre-validate regexes, and merge ruleset/CLI suppressions
3. when Hive lookup is enabled, enumerate Hive Catalog table `LOCATION` values once on the driver and create a broadcast index
4. collect physical files and apply ignore-pattern filtering
5. expand archive entries and workbook sheets from workbook metadata, pass through direct compressed text-style inputs, probe magic bytes, detect CSV dialects, normalize text fallback, and filter ignored archive entries
6. build first-pass groups by `(directory, format)`
7. sample a representative file for schema detection
8. perform schema-aware split and determine whether directory identifiers are safe
9. exact-split revalidate sampled multi-file groups before scanning
10. acquire `<output>/_progress-preparing.json`, prepare `<output>/_progress/<run_id>`, clean stale progress
11. batch scan non-sampled batch-capable groups, optionally with file sampling
12. direct-file scan non-sampled `xlsx` groups
13. fall back to file-level scanning if a normal group batch scan fails
14. create in-flight markers while group/file/allowlist work is active, then write progress JSONL shards when a group or file completes
15. merge progress JSONL into final `scan_results` and `scan_errors`, then remove `_progress/<run_id>`

## Operational Invariants
- `scan_results` stores two interpretation aids: `sample_matched_fragment` keeps the detected fragment itself, and `sample_raw_value` keeps only up to 50 characters of surrounding context on each side.
- `--pre-scan-parallelism` applies to directory discovery, input expansion, format probing, and schema split.
- `--ignore` and `--ignore-file` apply immediately after physical file discovery and again during archive entry expansion.
- Suppression is applied during `DetectionAggregator.buildMetrics`, before metric planning, so excluded `(column, pii_type)` pairs never materialize result rows.
- Directory discovery uses breadth-first traversal and parallelizes `listStatus` per BFS level, capped by the safety ceiling `64`.
- After file discovery, effective pre-scan parallelism is bounded by the discovered file count and the safety ceiling `64`.
- Hive lookup enumerates only table-level `LOCATION` values and falls back to an empty mapping with a warning on failure. `hive_table_fqn` is intentionally excluded from review snapshot comparison payloads.
- `xlsx` pre-scan lightly parses workbook metadata and header row XML on the driver to plan visible sheets and schema signatures. Sheet body row/cell reads are deferred to the executor-side StAX scan path.
- `xlsx` file-level scans also flow through `scanGroupByFile`, so they consume CLI `--file-parallelism` or `spark.privyspark.fileParallelism`.
- `--file-sample-ratio` applies to both batch scans and file-fallback scans, but only when a group has more files than `--file-sample-min-files`; when it does apply, PrivySpark uniformly samples at least one file using `ceil(fileCount * ratio)`.
- When file sampling actually applies, `--sample-ratio < 1.0` is ignored for that group and a warning is logged.
- Sampled groups are never promoted to directory-level identifiers before exact-split validation.
- Archive and Excel logical inputs keep their own identifiers.
- The public output contract defaults to `parquet/scan_results` and `parquet/scan_errors`, and CLI `--output-format` can additionally materialize `csv/...` and `excel/*.xlsx`.
- Clean completions also emit `meta/completions` markers.
- In-flight markers under `_progress/<run_id>/in-flight` are best-effort diagnostics for currently active work. Completed work and recoverable failures delete markers; unrecovered group/file failures that make the application `FAILED` preserve them.
- In-flight marker filenames preserve filesystem-safe UTF-8 letters/digits plus `.`, `_`, and `-`; path separators and other characters are replaced with `_`.
- `_progress` is cleaned based on staleness when the next run starts. There is no shutdown hook cleanup.

## Why It Works This Way
- Keeping `_progress` separate from final outputs preserves both observability and final report integrity.
- In-flight markers expose current bottleneck work and the last active group/file work at application failure while preserving the completed-progress JSONL contract.
- Cleanup happens on the next run instead of a shutdown hook because forced YARN termination and `kill -9` make shutdown hooks unreliable.
- `_progress-preparing.json` exists so concurrent startup cannot delete another run's freshly created progress root before the active marker is ready.
- The owner run can self-heal an unreadable `active-run.json` from `meta/run.json` so a damaged marker does not unnecessarily kill a live run.
