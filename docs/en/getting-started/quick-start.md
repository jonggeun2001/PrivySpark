# Quick Start

## Prerequisites
- Spark `3.5.3`
- Scala `2.12`
- JVM bytecode target `1.8`
- YARN cluster runtime

PrivySpark assumes Spark runtime libraries are provided by the cluster. Application dependencies are packaged into the Shadow fat JAR.

## Build

```bash
./gradlew clean shadowJar
```

The main artifact is `build/libs/*-all.jar`.

## Test

```bash
./gradlew test
```

To regenerate the bundled sample datasets:

```bash
./gradlew generateSampleDatasets
./gradlew packageSampleDatasets
```

## Basic Run

```bash
PRIVYSPARK_DEBUG=info \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --output-format parquet \
  --ruleset default \
  --sample-ratio 0.2
```

`--path` and `--output` only accept absolute paths or URIs.

## Parallelism and Sampling Example

```bash
PRIVYSPARK_DEBUG=debug \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --output-format parquet \
  --output-format csv \
  --ruleset default \
  --sample-ratio 0.2 \
  --file-sample-ratio 0.1 \
  --file-sample-min-files 10 \
  --pre-scan-parallelism 32 \
  --group-parallelism 16 \
  --file-parallelism 8 \
  --excel-max-rows-in-memory 2048 \
  --suppress prdctcd:driver_license_number \
  --ignore "_SUCCESS" \
  --ignore "backup/**"
```

`--file-sample-ratio` only applies when a group has more files than `--file-sample-min-files`. Once file sampling actually applies to a group, `--sample-ratio < 1.0` is ignored for that group and a warning is logged. This avoids changing the sampling basis twice.

## Ignore Pattern Example

```bash
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ignore "_SUCCESS" \
  --ignore "*.crc" \
  --ignore "/backup/**" \
  --ignore-file scan.ignore
```

For YARN cluster runs, distribute a client-local ignore file first.

```bash
PRIVYSPARK_SPARK_FILES=/abs/path/scan.ignore#scan.ignore \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ignore-file scan.ignore
```

`--ignore-file` is a UTF-8 text file. Blank lines and `#` comments are ignored. HDFS and object-store URIs can also be passed directly.

## Suppression Example

```bash
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --suppress prdctcd:driver_license_number \
  --suppression-file scan.suppressions
```

`--suppression-file` is also UTF-8 text. Each line uses `column:pii_type`, and blank lines plus `#` comments are ignored. If the ruleset YAML already defines `suppressions:`, the CLI entries are union-merged with them.

## Distributing a Custom Ruleset

For YARN cluster runs, distribute the custom ruleset file together with the job:

```bash
PRIVYSPARK_SPARK_FILES=/abs/path/my-rules.yaml#my-rules.yaml \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset my-rules.yaml
```

Direct `spark-submit` is also supported:

```bash
spark-submit \
  --class io.github.jonggeun2001.privyspark.PrivySparkApp \
  --master yarn \
  --deploy-mode cluster \
  --files /abs/path/my-rules.yaml#my-rules.yaml \
  /abs/path/privyspark-<version>-all.jar \
  scan --path hdfs:///data/input --output hdfs:///data/output --ruleset my-rules.yaml
```

## Output Paths
- Default final results: `<output>/parquet/scan_results`
- Default final errors: `<output>/parquet/scan_errors`
- With `--output-format csv`: `<output>/csv/scan_results`, `<output>/csv/scan_errors`
- With `--output-format excel`: `<output>/excel/scan_results.xlsx`, `<output>/excel/scan_errors.xlsx`
- In-progress data: `<output>/_progress/<run_id>`

`--output-format` can be repeated and supports `parquet`, `csv`, and `excel`. The default is `parquet`.

The `_progress` directory is only for observability during long scans. Consumers should rely on the selected final report formats.
