# Execution and Operations

## Execution Model
- The public commands are `privyspark scan` and `privyspark review apply`.
- Input and output paths must be absolute paths or URIs.
- The default target runtime is Spark on YARN cluster mode.
- The build artifact is a Shadow fat JAR (`*-all.jar`).

## `scan` CLI Arguments
- `--path <ABS_PATH_OR_URI>`: input path
- `--output <ABS_PATH_OR_URI>`: output path
- `--output-format <parquet|csv|excel>`: repeatable final output format option, default `parquet`
- `--ruleset <default|path>`: built-in ruleset or external path
- `--sample-ratio <(0.0, 1.0]>`: row sampling ratio, default `0.2`
- `--file-sample-ratio <(0.0, 1.0]>`: batch group file sampling ratio, unset by default
- `--file-sample-min-files <INT>`: minimum group size required before file sampling applies, default `10`, `>= 1`
- `--pre-scan-parallelism <INT>`: parallelism for directory discovery, file pre-scan expansion, and schema split, `> 0`
- `--group-parallelism <INT>`: group scan parallelism, `> 0`
- `--file-parallelism <INT>`: file fallback scan parallelism, `> 0`
- `--excel-max-rows-in-memory <INT>`: spark-excel `maxRowsInMemory` reader option for `xlsx` reads, `> 0`
- `--ignore <PATTERN>`: repeatable gitignore-style glob ignore pattern
- `--ignore-file <PATH>`: line-based ignore pattern file path, with `#` comments and blank lines ignored
- `--allowlist <ABS_PATH_OR_URI>`: false-positive suppression allowlist JSONL path
- `--suppress <column:pii_type>`: repeatable false-positive suppression rule
- `--suppression-file <PATH>`: line-based suppression file path, with `#` comments and blank lines ignored

## `review apply` CLI Arguments
- `--scan-results <ABS_PATH_OR_URI>`: edited `scan_results` input path. `csv`, `parquet`, and `xlsx` (`scan_results` sheet) are supported.
- `--input-root <ABS_PATH_OR_URI>`: original scan input root
- `--allowlist <ABS_PATH_OR_URI>`: allowlist JSONL path to create or update
- `--reviewer <STRING>`: reviewer identifier
- `--dry-run`: calculates staged entries without writing the output file

## Ignore Patterns
- Patterns without `/` match basenames. Example: `_SUCCESS`, `*.crc`
- Patterns with `/` match input-root-relative paths. Example: `backup/**`, `logs/2025/*.gz`
- A leading `/` is treated as an input-root anchor. Example: `/backup/**`, `/logs/`
- Patterns ending with `/` are treated as directory patterns and exclude the full subtree. Example: `logs/`
- Archive entries also apply the same rules against the entry-relative logical path under `<archive>!<entry>`.
- v1 does not support negate patterns such as `!pattern`.
- `--ignore-file` is read through Hadoop `FileSystem`. In YARN cluster mode, client-local files must be distributed first with `--files` or `PRIVYSPARK_SPARK_FILES`, then referenced through the distributed alias.

The ignore filter runs before pre-scan so low-value inputs such as `_SUCCESS`, `.crc`, log dumps, or backup directories do not inflate I/O, error rows, or report noise.

Allowlists are intentionally different from ignore rules. Ignore rules skip files before scanning, while allowlists suppress only reviewed false positives at the `(file_identifier, column_name, pii_type)` level after detection.

## Suppression
- Suppression removes only a specific `(column, pii_type)` result pair. Column names are matched case-insensitively by exact equality.
- `--suppress` only accepts the `column:pii_type` format.
- `--suppression-file` is read through Hadoop `FileSystem`. In YARN cluster mode, distribute client-local files first with `--files` or `PRIVYSPARK_SPARK_FILES`, then reference the distributed alias.
- CLI suppressions are union-merged with ruleset YAML `suppressions:`.

## Parallelism
- CLI values are passed directly into application logic.
- When omitted, PrivySpark uses `spark.privyspark.preScanParallelism`, `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism`, or the application defaults (`4`, `4`, `3`).
- Pre-scan parallelism covers directory discovery, input expansion, format probing, and group schema split.
- During directory discovery, the pool is capped by the safety ceiling `64` and the number of directories in the current BFS level. After discovery, effective pre-scan parallelism is still bounded by discovered file count and the safety ceiling `64`.
- Group and file parallelism control how many scan tasks the driver submits concurrently.

These settings do not directly guarantee executor fan-out. Actual executor distribution still depends on input partitioning, Spark scheduling, and dynamic allocation backlog.

## Excel Reader Configuration
- When `--excel-max-rows-in-memory` is set, PrivySpark passes it to the spark-excel reader as `maxRowsInMemory` for `xlsx` schema detection and actual scans.
- When the CLI option is omitted, PrivySpark uses the `spark.privyspark.excel.maxRowsInMemory` Spark conf only if it is present.
- This setting reduces memory pressure by enabling the streaming reader path for large workbooks; it does not make a single `xlsx` sheet row-splittable across executors.

## Sampling
- `--sample-ratio` is non-deterministic row sampling.
- When `sampleRatio >= 1.0`, no row sampling is applied.
- `--file-sample-ratio` uniformly samples files inside both batch scan and file-fallback group scans.
- File sampling only applies when the group has more files than `--file-sample-min-files`. Groups at or below the threshold still scan every file.
- When sampling applies, the sampled file count is `ceil(fileCount * fileSampleRatio)` with a minimum of one file.
- When file sampling actually applies and `--sample-ratio < 1.0` is also provided, row sampling is ignored for that group and `group_scan_row_sampling_ignored` is logged.

Uniform random file sampling was chosen because the operational concern was file-level concentration risk. Size-weighted sampling would bias toward large files and could amplify concentration instead of reflecting it.

## Driver Logging
- Driver log level can be configured through `PRIVYSPARK_DEBUG`, `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG`, or `-Dprivyspark.debug`.
- Supported values are `error`, `warn`, `info`, `debug`, and `off`.
- The default is `warn`.
- For backward compatibility, `true` maps to `debug` and `false` maps to `warn`.
- Log format: `[PrivySpark][LEVEL][ISO-8601 UTC timestamp] event key=value...`

`info` exposes high-level lifecycle events such as `scan_start`, `scan_plan_ready`, and `scan_complete`. `debug` adds detailed events for file discovery, pre-scan execution, grouping, and `_progress` lifecycle.

When ignore rules apply, events such as `scan_directory_file_ignored` and `archive_entry_skipped reason=ignored` are emitted, and `ignored_files` is included in `scan_directory_files_discovered`, `scan_directory_pre_scan_execute_complete`, and `scan_complete`.

## `_progress` Handling
- In-progress shards are written as JSONL under `<output>/_progress/<run_id>/results`, `errors`, and `meta/completions`.
- Before setup, PrivySpark acquires `<output>/_progress-preparing.json`.
- Once setup is ready, it switches to `_progress/active-run.json` with heartbeat updates.
- On the next run, only stale heartbeats, `FAILED` markers, or stale preparing locks are cleaned up.
- A recent `RUNNING` heartbeat or fresh preparing lock causes a conflict failure instead of cleanup.
- If `active-run.json` becomes unreadable, the owner run can self-heal it from `meta/run.json`.

This design keeps long-running progress observable without mixing partial output into the final consumer-facing result paths.

## Releases
- GitHub Release is triggered by pushing a `v*` tag or bare semver tag.
- The release workflow runs `./gradlew clean shadowJar packageSampleDatasets`.
- Release assets are `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, and `privyspark-<tag>-sample-datasets.zip`.
