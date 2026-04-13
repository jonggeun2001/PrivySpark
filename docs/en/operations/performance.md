# Performance Guide

## Runtime Characteristics
PrivySpark performance usually breaks down into four stages:

1. file discovery and pre-scan
2. schema sampling and group split
3. group or file scan execution
4. final merge and report writing

The actual bottleneck depends on input distribution. Small-file-heavy inputs tend to amplify pre-scan and partition fan-out cost, while wide schemas amplify detection expression cost.

## Optimizations Already Implemented
- Pre-scan parallelism is reused for format probing and group schema split.
- CSV body reads run with `inferSchema=false`.
- `DetectionAggregator` uses batched aggregation instead of one Spark job per metric.
- The legacy fallback threshold is raised to `50,000` expressions, and the fallback still uses smaller aggregation batches rather than per-metric counts.
- `_progress` acts as the final merge source so long scans do not require the driver to retain all result row payloads in memory.

## Small-File-Heavy Inputs
- `--pre-scan-parallelism` is the first lever for probe and schema-split latency.
- `--group-parallelism` and `--file-parallelism` increase driver-side concurrent submissions, but they do not directly guarantee executor distribution.
- `--file-sample-ratio` can be more effective than `--sample-ratio` for small-file-heavy batch-capable groups because it reduces the number of files read at all.

Uniform random file sampling is not only a performance feature. It also preserves file-level concentration risk better than size-weighted sampling, which would over-bias large files.

## When `scan_directory_structure_start` Is Slow
This phase is usually driver-side work:

- recursive file listing
- pre-scan execution
- input expansion and initial grouping
- schema split

When `scan_directory_files_discovered` to `scan_directory_initial_groups_ready` is slow, the likely cost centers are:

- per-file `getFileStatus`
- probing unknown or extensionless inputs
- large `Future` submission and collection
- grouping and sorting by `(directory, format)`

## When Detection Aggregation Is Slow
`DetectionAggregator` runs a global aggregate across the sampled DataFrame. Even though the result may be a single row, it still scans all sampled partitions.

- More input partitions means more aggregate tasks.
- Wide schemas combined with many rules increase metric count.
- `column_hints` can reduce metric count by limiting rules to relevant columns.

## Spark/YARN Operating Notes
- With dynamic allocation enabled, short jobs may not create enough backlog to scale executors out.
- Increasing application-level parallelism alone may still lead to limited executor fan-out if the scheduler stays FIFO or backlog remains small.
- For large batch groups, input partitioning and Spark file partition settings matter as much as PrivySpark CLI knobs.
- Enable `info` or `debug` driver logs to separate pre-scan, grouping, and progress-merge bottlenecks.

## Tuning Priority
1. For small-file-heavy inputs, start with `--pre-scan-parallelism`, `--file-sample-ratio`, and Spark file partition settings.
2. If group count is high, tune `--group-parallelism`.
3. If fallback file scans are common, tune `--file-parallelism`.
4. For wide schemas, reduce unnecessary metrics with `column_hints` and ruleset cleanup.
5. For long scans, inspect `_progress` and structured driver logs together to isolate the slow stage.
