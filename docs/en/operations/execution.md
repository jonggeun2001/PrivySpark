# Execution and Operations

## Execution Model
- `privyspark scan` is the single public entrypoint.
- Input and output paths must be absolute paths or URIs.
- The default target runtime is Spark on YARN cluster mode.
- The build artifact is a Shadow fat JAR (`*-all.jar`).

## CLI Arguments
- `--path <ABS_PATH_OR_URI>`: input path
- `--output <ABS_PATH_OR_URI>`: output path
- `--output-format <parquet|csv|excel>`: repeatable final output format option, default `parquet`
- `--ruleset <default|path>`: built-in ruleset or external path
- `--sample-ratio <(0.0, 1.0]>`: row sampling ratio, default `0.2`
- `--file-sample-ratio <(0.0, 1.0]>`: batch group file sampling ratio, unset by default
- `--pre-scan-parallelism <INT>`: parallelism for directory discovery, file pre-scan expansion, and schema split, `> 0`
- `--group-parallelism <INT>`: group scan parallelism, `> 0`
- `--file-parallelism <INT>`: file fallback scan parallelism, `> 0`
- `--ignore <PATTERN>`: repeatable gitignore-style glob ignore pattern
- `--ignore-file <PATH>`: line-based ignore pattern file path, with `#` comments and blank lines ignored

## Ignore Patterns
- Patterns without `/` match basenames. Example: `_SUCCESS`, `*.crc`
- Patterns with `/` match input-root-relative paths. Example: `backup/**`, `logs/2025/*.gz`
- A leading `/` is treated as an input-root anchor. Example: `/backup/**`, `/logs/`
- Patterns ending with `/` are treated as directory patterns and exclude the full subtree. Example: `logs/`
- Archive entries also apply the same rules against the entry-relative logical path under `<archive>!<entry>`.
- v1 does not support negate patterns such as `!pattern`.
- `--ignore-file` is read through Hadoop `FileSystem`. In YARN cluster mode, client-local files must be distributed first with `--files` or `PRIVYSPARK_SPARK_FILES`, then referenced through the distributed alias.

The ignore filter runs before pre-scan so low-value inputs such as `_SUCCESS`, `.crc`, log dumps, or backup directories do not inflate I/O, error rows, or report noise.

## Parallelism
- CLI values are passed directly into application logic.
- When omitted, PrivySpark uses `spark.privyspark.preScanParallelism`, `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism`, or the application defaults (`4`, `4`, `3`).
- Pre-scan parallelism covers directory discovery, input expansion, format probing, and group schema split.
- During directory discovery, the pool is capped by the safety ceiling `64` and the number of directories in the current BFS level. After discovery, effective pre-scan parallelism is still bounded by discovered file count and the safety ceiling `64`.
- Group and file parallelism control how many scan tasks the driver submits concurrently.

These settings do not directly guarantee executor fan-out. Actual executor distribution still depends on input partitioning, Spark scheduling, and dynamic allocation backlog.

## Sampling
- `--sample-ratio` is non-deterministic row sampling.
- When `sampleRatio >= 1.0`, no row sampling is applied.
- `--file-sample-ratio` uniformly samples files inside a batch-capable group.
- The sampled file count is `ceil(fileCount * fileSampleRatio)` with a minimum of one file.
- When `--file-sample-ratio` is active and `--sample-ratio < 1.0` is also provided, row sampling is ignored for that group and `group_scan_row_sampling_ignored` is logged.

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
