# Execution and Operations

## Execution Model
- The public commands are `privyspark scan`, `privyspark review apply`, and `privyspark review collect`.
- Input and output paths must be absolute paths or URIs.
- Input filenames may contain spaces and Spark glob-special characters (`*`, `?`, `[`, `]`, `{`, `}`); PrivySpark treats them as literal filenames. Glob syntax applies only to `--ignore` and `--ignore-file` patterns.
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
- `--excel-max-rows-in-memory <INT>`: compatibility option for the previous spark-excel scan reader path, `> 0`; explicitly setting it logs a warning and no longer affects `xlsx` scan reads
- `--excel-byte-array-max-override <INT>`: Apache POI byte array allocation max override, default `300000000`, `> 0`
- `--ignore <PATTERN>`: repeatable gitignore-style glob ignore pattern
- `--ignore-file <PATH>`: line-based ignore pattern file path, with `#` comments and blank lines ignored
- `--allowlist <ABS_PATH_OR_URI>`: false-positive suppression allowlist JSONL path
- `--review-state-root <ABS_PATH_OR_URI>`: cumulative offline-review state root. Before the scan starts, collects `<review-state-root>/inbox/*.json`, updates `<review-state-root>/current`, then applies `<review-state-root>/current/allowlist.jsonl` and writes `<output>/review/review.html` by default. Review HTML files are capped at 2MB each; oversized reviews are split into a `review.html` index and `review-part-0001.html` style part files
- `--review-html-dir <ABS_PATH_OR_URI>`: offline review HTML output directory. Defaults to `<output>/review`, with `review.html` fixed as the entry filename
- `--review-sample-mode <raw|masked|none>`: sample display mode for `review.html`, default `masked`
- `--suppress <column:pii_type>`: repeatable false-positive suppression rule
- `--suppression-file <PATH>`: line-based suppression file path, with `#` comments and blank lines ignored
- `--hive-metastore-jdbc-url <JDBC_URL>`: Hive Metastore JDBC URL, for example `jdbc:mariadb://hms-db.internal:3306/metastore`
- `--hive-metastore-user <USER>`: metastore read-only user
- `--hive-metastore-password-file <ABS_PATH_OR_URI>`: file whose first line contains the password. `hdfs://`, `s3a://`, `file://`, and absolute paths are supported
- `--hive-metastore-jdbc-driver-class <CLASS>`: Hive Metastore JDBC driver class. When the CLI value is omitted, PrivySpark uses the `spark.privyspark.hiveMetastore.jdbcDriverClass` Spark conf; when both are omitted, it defaults to `org.mariadb.jdbc.Driver`

## `review apply` CLI Arguments
- `--scan-results <ABS_PATH_OR_URI>`: edited `scan_results` input path. `csv`, `parquet`, and `xlsx` (`scan_results` sheet) are supported.
- `--input-root <ABS_PATH_OR_URI>`: original scan input root
- `--allowlist <ABS_PATH_OR_URI>`: allowlist JSONL path to create or update
- `--reviewer <STRING>`: reviewer identifier
- `--dry-run`: calculates staged entries without writing the output file

## `review collect` CLI Arguments
- `--review-state-root <ABS_PATH_OR_URI>`: state root where response JSON files are read and cumulative review state is written

`review collect` reads only `<review-state-root>/inbox/*.json` and updates `allowlist.jsonl`, `action_plan.jsonl`, `finding_status.jsonl`, and `response_ledger.jsonl` under `<review-state-root>/current`. Review owners can create JSON directly in `review.html`; when the review is split into 2MB part files, they create one response JSON from each `review-part-*.html` file and place all returned JSON files in the inbox. For Excel editing, they download a CSV from the review file, edit it, import the decrypted CSV back into the page, or paste the TSV clipboard text copied from Excel, and then create the JSON. CSV upload preserves quoted commas and embedded line breaks as cell content. TSV paste is applied using tabs and row breaks, and embedded line breaks remain cell content when Excel wraps that cell in double quotes. `--scan-results` is no longer required. A later scan with the same `--review-state-root` runs this collect step automatically before scanning. If any response is invalid, current state is not updated and the command fails. If `<review-state-root>/.collect.lock` already exists, the command fails to prevent concurrent state updates; the lock is removed after collect finishes.

## Ignore Patterns
- Patterns without `/` match basenames. Example: `_SUCCESS`, `*.crc`
- Patterns with `/` match input-root-relative paths. Example: `backup/**`, `logs/2025/*.gz`
- A leading `/` is treated as an input-root anchor. Example: `/backup/**`, `/logs/`
- Patterns ending with `/` are treated as directory patterns and exclude the full subtree. Example: `logs/`
- Archive entries also apply the same rules against the entry-relative logical path under `<archive>!<entry>`.
- v1 does not support negate patterns such as `!pattern`.
- `--ignore-file` is read through Hadoop `FileSystem`. In YARN cluster mode, client-local files must be distributed first with `--files` or `PRIVYSPARK_SPARK_FILES`, then referenced through the distributed alias.

The ignore filter runs before pre-scan so low-value inputs such as `_SUCCESS`, `.crc`, log dumps, or backup directories do not inflate I/O, error rows, or report noise.

Allowlists are intentionally different from ignore rules. Ignore rules skip files before scanning, while allowlists suppress only reviewed recurring false positives after detection. When Hive mapping exists the key is `(scan_path, hive_table_fqn, column_name, pii_type)`; otherwise the key is `(scan_path, file_identifier_pattern, column_name, pii_type)`. New recurring responses require exact `column_name` and `pii_type` values; `*` wildcards are rejected for those fields.

## Suppression
- Suppression removes only a specific `(column, pii_type)` result pair. Column names are matched case-insensitively by exact equality.
- `--suppress` only accepts the `column:pii_type` format.
- `--suppression-file` is read through Hadoop `FileSystem`. In YARN cluster mode, distribute client-local files first with `--files` or `PRIVYSPARK_SPARK_FILES`, then reference the distributed alias.
- CLI suppressions are union-merged with ruleset YAML `suppressions:`.

## Hive Table Lookup
- Hive table lookup is enabled only when `--hive-metastore-jdbc-url`, `--hive-metastore-user`, and `--hive-metastore-password-file` are all provided. Supplying only one or two options is a CLI error. Supplying none logs `hive_lookup_inactive`, and `hive_table_fqn` remains `""`.
- When enabled, the driver queries Hive Metastore `DBS`/`TBLS`/`SDS` once through the configured JDBC driver class and broadcasts a table-level `LOCATION` prefix index. If a result row's physical input path falls under a table prefix, `scan_results.hive_table_fqn` is filled with `db.table`.
- The password file is read through Hadoop `FileSystem`. Shared URIs such as `hdfs://` do not require extra YARN `--files` distribution. Client-local files must still be distributed first with `--files` or `PRIVYSPARK_SPARK_FILES`, then referenced by the distributed alias.
- JDBC driver JARs are not packaged in the Shadow JAR. The default driver class is `org.mariadb.jdbc.Driver`; set `--hive-metastore-jdbc-driver-class` or Spark conf `spark.privyspark.hiveMetastore.jdbcDriverClass` when using another driver. CLI values take precedence over Spark conf. To use Hive table lookup, install the driver on the cluster common classpath, or submit it through Spark `--jars` by setting `PRIVYSPARK_JARS=/path/to/driver.jar`. In environments that allow Maven package resolution, `PRIVYSPARK_PACKAGES=org.mariadb.jdbc:mariadb-java-client:3.4.1` is also supported.
- For MariaDB/MySQL compatible drivers or JDBC URLs, PrivySpark applies `connectTimeout=5000` and `socketTimeout=30000` when the URL does not define them. For other drivers, configure driver-specific timeout parameters directly in the JDBC URL.
- If JDBC connection, password-file reading, or metastore query fails, PrivySpark logs `hive_lookup_disabled` and continues with an empty mapping. Successful index creation logs `hive_lookup_ready size=<N>`.
- Archive entries and Excel sheets are looked up by the host archive/workbook path after stripping `<archive>!<entry>` and `<workbook>#<sheet>` suffixes.
- Partition-level `LOCATION` overrides are not supported yet. PrivySpark uses only table-level `LOCATION`.

## Parallelism
- CLI values are passed directly into application logic.
- When omitted, PrivySpark uses `spark.privyspark.preScanParallelism`, `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism`, or the application defaults (`32`, `16`, `8`).
- Pre-scan parallelism covers directory discovery, input expansion, format probing, and group schema split.
- During directory discovery, the pool is capped by the safety ceiling `64` and the number of directories in the current BFS level. After discovery, effective pre-scan parallelism is still bounded by discovered file count and the safety ceiling `64`.
- Group and file parallelism control how many scan tasks the driver submits concurrently.
- `spark.privyspark.driverRpcConcurrency` adds a second cap for driver-side HDFS/RPC-like scan work. The default is `48` for group/file/snapshot scan paths and `64` for pre-scan paths. Set it to `0` to disable this safety gate. Group dispatch parallelism is also reduced to this cap so batch group scans cannot bypass it.
- `spark.privyspark.progress.flushMode` controls the progress JSONL write unit for file fallback scans. The default `group` buffers per-file results and errors in memory and flushes results/errors/completions shards once when the group finishes. Set it to `file` to restore immediate per-file progress shard writes.

These settings do not directly guarantee executor fan-out. Actual executor distribution still depends on input partitioning, Spark scheduling, and dynamic allocation backlog.

## Retry and HDFS Refresh
- File read retry now attempts up to three times and uses exponential backoff from a 200ms base with jitter, reducing simultaneous retry waves from many driver threads.
- Before retrying, Spark catalog refresh targets the original file paths by default. Parent directory refresh is disabled by default because it can trigger expensive NameNode `listStatus` calls on large directories.
- Set `spark.privyspark.retry.refreshParent=true` to opt back into parent directory refresh if an environment depends on the previous behavior.

## Excel Reader Configuration
- During `xlsx` pre-scan, the driver lightly parses workbook metadata and header row XML to build visible sheet lists and schema signatures; sheet body row/cell contents are handled by the executor-side StAX streamer.
- `--excel-max-rows-in-memory` is retained for CLI compatibility with the previous spark-excel scan reader. When explicitly set, PrivySpark logs `excel_max_rows_in_memory_unused` and does not use the value for scan reads.
- The `spark.privyspark.excel.maxRowsInMemory` Spark conf also no longer affects executor-side `xlsx` scans.
- When `--excel-byte-array-max-override` is set, PrivySpark applies Apache POI `IOUtils.setByteArrayMaxOverride`. This is retained for POI-backed paths such as Excel report writing.
- When the CLI option is omitted, PrivySpark uses the `spark.privyspark.excel.byteArrayMaxOverride` Spark conf, and if that conf is also absent it applies the default value `300000000`.
- The executor-side `xlsx` streamer reads one workbook sheet in one Spark task. It does not make a single sheet row-splittable across executors, and it intentionally avoids cache/persist, so repeated actions reread the workbook zip.
- Workbook ZIP entry iteration uses an API compatible with older `commons-compress` versions bundled by Spark/Hadoop runtimes. Operators do not need to override the cluster common classpath to avoid `NoSuchMethodError` during `xlsx` scans.
- The Shadow fat JAR relocates `commons-compress` into a PrivySpark-internal package. This keeps POI-based Excel report write paths on the bundled compatible copy even when Spark/Hadoop exposes an older `commons-compress` first.

## Sampling
- `--sample-ratio` is non-deterministic row sampling.
- When `sampleRatio >= 1.0`, no row sampling is applied.
- `--file-sample-ratio` selects a stable hash-ranked subset of files inside both batch scan and file-fallback group scans.
- File sampling only applies when the group has more files than `--file-sample-min-files`. Groups at or below the threshold still scan every file.
- When sampling applies, the sampled file count is `ceil(fileCount * fileSampleRatio)` with a minimum of one file.
- When file sampling actually applies and `--sample-ratio < 1.0` is also provided, row sampling is ignored for that group and `group_scan_row_sampling_ignored` is logged.
- Review fingerprints for file-sampled group rows cover only the sampled files that were actually scanned.

Stable hash-ranked file sampling keeps the same subset for the same group and file set, which prevents review scopes from drifting between runs when data has not changed. It still avoids size weighting because the operational concern is file-level concentration risk; size-weighted sampling would bias toward large files and could amplify concentration instead of reflecting it.

## Driver Logging
- Driver log level can be configured through `PRIVYSPARK_DEBUG`, `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG`, or `-Dprivyspark.debug`.
- Supported values are `error`, `warn`, `info`, `debug`, and `off`.
- The default is `warn`.
- For backward compatibility, `true` maps to `debug` and `false` maps to `warn`.
- Log format: `[PrivySpark][LEVEL][ISO-8601 local driver timestamp with offset] event key=value...`
- The default timestamp pattern is `uuuu-MM-dd'T'HH:mm:ss.SSSXXX`, so fractional seconds are always fixed to three millisecond digits (`ss.SSS`).
- Override the timestamp pattern with `PRIVYSPARK_DEBUG_TIMESTAMP_PATTERN` or `-Dprivyspark.debug.timestampPattern=<DateTimeFormatter pattern>`. Blank or invalid patterns fall back to the default.

`info` exposes high-level lifecycle events such as `scan_start`, `scan_plan_ready`, and `scan_complete`. `debug` adds detailed events for file discovery, pre-scan execution, grouping, and `_progress` lifecycle.

To diagnose driver TCP connection growth during group scan, inspect `group_scan_tcp_snapshot` events at `debug` level. On Linux/YARN drivers, PrivySpark reads the current JVM's `/proc` TCP socket fds and records `tcp_fd_count`, `tcp_states`, `tcp_remote_ports_top`, `tcp_established_remote_ports_top`, and `tcp_established_remote_endpoints_top`. On environments without `/proc`, such as macOS, it records `tcp_snapshot_available=false` with a reason. Compare `phase=batch_action_start|batch_action_complete`, `action=sampled_rows_by_file|aggregate_matches|sample_matches|count_non_empty`, `phase=review_snapshot_stage_*`, and `phase=file_spark_action_*` chronologically to isolate which Spark action or review snapshot step increases connections.

When connection growth appears during schema detection, inspect `read_schema_source_tcp_snapshot` events as well. These events record `read_schema_source_start|read_schema_source_complete|read_schema_source_error` phases with `format` and `file`, so you can compare whether `ESTABLISHED` connections opened after `read_schema_source_start` remain after completion and whether the endpoints point to HDFS DataNodes or Spark executor/RPC ports.

When ignore rules apply, events such as `scan_directory_file_ignored` and `archive_entry_skipped reason=ignored` are emitted, and `ignored_files` is included in `scan_directory_files_discovered`, `scan_directory_pre_scan_execute_complete`, and `scan_complete`.

If a file is discovered and then deleted before pre-scan probing, the file is skipped and logged as `scan_directory_file_skipped reason=not_found`. It is included in `skipped_files` for `scan_directory_pre_scan_execute_complete`, not in `scan_errors`.

## `_progress` Handling
- In-progress shards are written as JSONL under `<output>/_progress/<run_id>/results`, `errors`, and `meta/completions`.
- File fallback scans flush progress at group granularity by default. In this mode, per-file completed rows may not appear under `_progress` until the group finishes, and a driver failure causes that group to be rerun on the next attempt.
- Running group and allowlist snapshot tasks create temporary JSON markers under `<output>/_progress/<run_id>/in-flight`. File-level markers are disabled by default to avoid create/delete pressure during small-file scans; set `spark.privyspark.progress.fileMarker.enabled=true` to restore file-level in-flight visibility.
- Each in-flight marker includes `runId`, `scope`, `identifier`, `threadName`, `startedAtEpochMs`, and available scan metadata such as `format` and `schemaSignature`.
- In-flight marker filenames preserve filesystem-safe UTF-8 letters/digits plus `.`, `_`, and `-`; path separators and other characters are replaced with `_`. The original `identifier` remains in the JSON body.
- In-flight markers are removed for completed work and recoverable failures. Unrecovered group/file failures that make the Spark application end as `FAILED` preserve their markers so operators can inspect the last active work.
- Before setup, PrivySpark acquires `<output>/_progress-preparing.json`.
- Once setup is ready, it switches to `_progress/active-run.json` with heartbeat updates. Heartbeats are updated by a periodic task outside the progress shard write hot path.
- On the next run, only stale heartbeats, `FAILED` markers, or stale preparing locks are cleaned up.
- A recent `RUNNING` heartbeat or fresh preparing lock causes a conflict failure instead of cleanup.
- If `active-run.json` becomes unreadable, the owner run can self-heal it from `meta/run.json`.

This design keeps long-running progress observable without mixing partial output into the final consumer-facing result paths.

## Releases
- GitHub Release is triggered by pushing a `v*` tag or bare semver tag.
- The release workflow runs `./gradlew clean shadowJar packageSampleDatasets`.
- Release assets are `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, `privyspark-<tag>-sample-datasets.zip`, `privyspark-<tag>-review-response-example.html`, and `privyspark-<tag>-review-response-viewer.html`.
- `privyspark-<tag>-review-response-example.html` is a self-contained example for checking the offline owner review response JSON download, CSV edit/import, and Excel cell copy/paste flow. Production files are generated at `<scan-output>/review/review.html` after running `scan --review-state-root`.
- `privyspark-<tag>-review-response-viewer.html` is a self-contained operator page for inspecting a collected `response-<scan-path>-YYYYMMDD-HHMMSS.json` locally by choosing, dragging and dropping, or pasting the file contents, including envelope metadata, validation messages, and per-finding decisions.
