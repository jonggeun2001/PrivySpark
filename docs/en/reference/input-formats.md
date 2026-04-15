# Input Formats and Normalization

## Supported Inputs
- Extension-first support: `csv`, `json`, `jsonl`, `ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`
- Files without extensions and unsupported extensions are probed for `parquet` and `orc` magic bytes first.
- If magic bytes do not match but the content looks like UTF-8 text, the input is normalized into the internal `text` format and scanned through Spark's single-column `text` reader.
- UTF-8 text that uses ASCII information separators (`0x1C`-`0x1F`, for example RS-delimited files) still counts as text fallback input instead of binary.
- Only binary-looking unsupported inputs are recorded as `Unsupported file format`.
- Zero-byte physical files are skipped during pre-scan.

The text fallback exists because extension-based filtering alone would reject too many real-world log and dump files. At the same time, forcing every unknown binary into text mode would create noise, so PrivySpark separates those cases with magic-byte checks plus UTF-8 probing.

## Archive Handling
- `zip` and `jar` are pre-scanned and only supported inner files are staged and scanned.
- Archive expansion is limited to one level.
- Nested `zip`/`jar` entries are rejected instead of recursively expanded.
- Zero-byte archive entries are skipped without staging or error rows.
- Archive identifiers use the `<archive>!<entry>` format.

## Excel Handling
- `xlsx` workbooks are expanded into sheet-level logical inputs.
- Empty sheets are skipped.
- Sheet identifiers use the `<workbook>#<sheet>` format.

## Grouping and Scan Units
- The base scan unit is a file.
- Inputs are grouped first by `(directory, format)`.
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
- With a header, PrivySpark uses a header-name signature.
- Without a header, it uses a column-count signature (`cols:N`).
- Ambiguous two-line plain-text cases are treated as header-bearing CSV.

## Corrupt Inputs and Fallback
- JSON files that only produce corrupt records are recorded as corrupt inputs.
- Sampled multi-file groups are exact-split revalidated before batch scanning.
- When a normal group batch scan fails, PrivySpark falls back to file-level scanning without another schema resplit.
- File replacement or deletion during reads is retried for a limited number of attempts.
