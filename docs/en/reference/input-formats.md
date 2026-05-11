# Input Formats and Normalization

## Supported Inputs
- Extension-first support: `csv`, `json`, `jsonl`, `ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`, `tar`, `tar.gz`, `tgz`, `tar.bz2`, `tbz2`, `tar.xz`, `txz`, `tar.zst`, `tzst`, `7z`, `rar`
- Spaces in physical file paths are accepted, and Spark glob-special characters (`*`, `?`, `[`, `]`, `{`, `}`) are passed to schema detection and scan readers as literal path characters. This handling is separate from ignore pattern syntax and applies only to actual filenames.
- Direct text-style data files (`csv`, `json`, `jsonl`, `ndjson`) with outer `gz` or `bz2` wrappers are passed through to Spark/Hadoop readers using the original path. Examples: `customers.csv.gz`, `events.json.bz2`
- Files without extensions and most unsupported extensions are probed for `parquet` and `orc` magic bytes first. A small set of obviously non-data binary extensions such as `pdf` or `jpg` are classified as unsupported without probing.
- If magic bytes do not match but UTF-8 text has a stable delimiter plus header/data structure, the input is promoted to the internal `csv` format and scanned by column. If CSV dialect detection fails or the text is too ambiguous, UTF-8 or EUC-KR text-like input is normalized into the internal `text` format and scanned as a single `value` column.
- UTF-8 text that uses ASCII information separators (`0x1C`-`0x1F`, for example RS-delimited files) can be treated as CSV when the separator is stable across rows; remaining text that does not yield a CSV dialect stays in text fallback.
- Only binary-looking unsupported inputs are recorded as `Unsupported file format`.
- Zero-byte physical files are skipped during pre-scan.
- Physical files matching `--ignore` or `--ignore-file` patterns are excluded before pre-scan.
- Directory discovery reuses listed file length and modification time during pre-scan, so small-file-heavy inputs do not issue another file-status RPC for every discovered file before format expansion. Long pre-scans also emit progress logs on a time throttle in addition to the file-count interval.

The text/CSV fallback exists because extension-based filtering alone would reject too many real-world log and dump files. At the same time, forcing every unknown binary into text mode would create noise, so PrivySpark separates those cases with magic-byte checks, text encoding probing, and CSV dialect probing.

## Archive Handling
- `zip`, `jar`, `tar`, `tar.gz/tgz`, `tar.bz2/tbz2`, `tar.xz/txz`, `tar.zst/tzst`, `7z`, and `rar` are pre-scanned and staged entry-by-entry before scanning.
- Archive expansion is limited to one level.
- Nested archives are rejected instead of recursively expanded.
- Zero-byte archive entries are skipped without staging or error rows.
- Archive entries matching ignore patterns are logged as `archive_entry_skipped reason=ignored` and are not staged.
- Archive identifiers use the `<archive>!<entry>` format.
- Password-protected archives, multi-volume RAR archives, and RAR5 archives are reported explicitly in `scan_errors` without staging.

## Excel Handling
- `xlsx` workbooks are expanded into sheet-level logical inputs.
- During pre-scan, the driver lightly parses workbook metadata and header row XML to build visible sheet lists and schema signatures; it does not read sheet body row/cell contents.
- Empty visible sheets are skipped without results or errors after header-based schema detection. Hidden and very hidden sheets are excluded.
- Sheet identifiers use the `<workbook>#<sheet>` format.
- Actual scans run through an executor-side StAX sheet row streamer. Shared strings are loaded for the lifetime of the task on the executor, while sheet XML rows are streamed.
- Workbook ZIP entry iteration uses an API compatible with older `commons-compress` versions already provided by Spark/Hadoop runtimes, so `xlsx` scans do not require overriding the cluster `commons-compress` classpath.
- `--excel-max-rows-in-memory` is accepted only for compatibility with the previous spark-excel scan reader. Explicitly setting it logs `excel_max_rows_in_memory_unused` and does not affect scan reads.
- `--excel-byte-array-max-override` or `spark.privyspark.excel.byteArrayMaxOverride` adjusts the Apache POI byte array allocation limit for POI-backed paths such as Excel report writing. When both are omitted, PrivySpark applies the default value `300000000`.

## Grouping and Scan Units
- The base scan unit is a file.
- Inputs are grouped first by `(directory, format)`.
- Before that grouping, Hive/Spark layout directories such as `key=value` partitions, `bucket_00000` or `bucket-00000` bucket folders, and `__HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME__` skew/list-bucketing folders are normalized to the nearest non-layout parent within the input root.
- A representative-file schema sample and exact split are then used to refine `schemaSignature` and split groups again if necessary.
- Only multi-file groups with confirmed identical schemas can be promoted to a directory-level identifier.
- Archive entries and workbook sheets always keep their logical identifiers.

## Schema Sampling
- Multi-file groups may sample a single representative file first.
- Group-level schema split reuses `--pre-scan-parallelism` on the driver.
- Sampled groups are revalidated with exact split before batch scanning.
- CSV exact split also rechecks header drift.

This separate schema sampling phase exists for two reasons. First, directory-level aggregation must not hide schema drift. Second, reading every file upfront for exact split would inflate pre-scan cost, so the representative-file path trades cost against correctness before exact revalidation.

## CSV Header Handling
- CSV delimiters are detected automatically. Built-in candidates include comma, tab, semicolon, pipe, colon, ASCII information separator, plus consistent 2-3 character non-alphanumeric delimiters such as `||` and `|~|`.
- `.csv` files are still dialect-probed, so tab-, semicolon-, or pipe-delimited files with a `.csv` extension are read with the detected dialect.
- Unsupported extensions such as `.txt`, `.log`, `.data`, and extensionless text are promoted to `csv` when a stable CSV dialect is detected with at least a header plus two data rows and an additional header or structured-value signal.
- With a header, PrivySpark uses a header-name signature.
- Without a header, it uses a column-count signature (`cols:N`).
- Known CSV inputs can still resolve ambiguous two-line cases as header-bearing CSV, but unsupported text is not promoted to CSV from two lines alone.

## Corrupt Inputs and Fallback
- JSON files that only produce corrupt records are recorded as corrupt inputs.
- Sampled multi-file groups are exact-split revalidated before batch scanning.
- When a normal group batch scan fails, PrivySpark falls back to file-level scanning without another schema resplit.
- File replacement or deletion during reads is retried for a limited number of attempts.
