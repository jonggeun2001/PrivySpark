# Extended Input Format Support Design

## Goal

Expand input handling so PrivySpark can scan `avro`, `xlsx`, and archive-wrapped datasets (`zip`, `jar`), while also probing unknown extensions as plain text before classifying them as unsupported.

## Scope

- Add extension detection for `avro`, `xlsx`, `zip`, and `jar`.
- Read `avro` files through Spark's Avro datasource.
- Read `xlsx` workbooks through `spark-excel`, treating each sheet as a separate scan target.
- Expand `zip` and `jar` files during pre-scan, extract supported nested entries to a staging area, and scan them through the existing pipeline.
- Probe unknown extensions as text and scan them with Spark's text reader when the content looks textual.
- Preserve report identifiers for nested or workbook-backed sources, for example `archive.zip!nested/users.csv` and `book.xlsx#Sheet1`.
- Update README and PRD documents to reflect the supported input formats and text fallback behavior.

## Non-Goals

- No changes to result or error report schemas.
- No delimiter inference or structured parsing for generic text fallback beyond line-based scanning.
- No support for encrypted archives or password-protected Excel workbooks.
- No attempt to treat arbitrary binary formats as structured datasets beyond the text probe.

## Architecture

- Keep the existing scan flow centered on pre-scan, `(directory, format)` grouping, schema splitting, and `readSource`/`readSchemaSource`.
- Extend pre-scan with an ingest normalization step:
  - archives are expanded into staging files plus original identifiers
  - Excel sheets are represented as logical scan targets with sheet-qualified identifiers
  - unknown extensions are probed and, if textual, normalized to an internal `text` format
- Add datasource-backed readers for `avro`, `xlsx`, and `text` so the rest of the scan pipeline can stay consistent.

## Data Flow

### Archive inputs

- When pre-scan sees a `zip` or `jar`, it opens the archive and iterates file entries.
- Supported nested files are extracted to a staging directory under the work path.
- Each extracted entry keeps its origin identifier in the form `<archive>!<entry>`.
- Nested entries with unknown extensions go through the same text probe as top-level unknown files.
- Archive-level failures produce `ScanError` rows for the archive; per-entry failures produce `ScanError` rows for the specific nested identifier without aborting the entire archive.

### Avro inputs

- Avro files are grouped and schema-split like the existing columnar formats.
- `readSource` and `readSchemaSource` load Avro via Spark's Avro datasource so batching and sampling continue to work as today.

### Excel inputs

- `.xlsx` files are loaded with `spark-excel`.
- Each visible, non-empty sheet becomes an independent logical scan target identified as `<workbook>#<sheet>`.
- Schema detection and scanning operate at the sheet level because sheets within a workbook may differ.

### Unknown extensions

- Unknown extensions are probed from a small prefix of bytes.
- Files with BOM-free textual content, low control-character density, and no binary markers are treated as `text`.
- Text fallback uses Spark's text reader and scans the single `value` column.
- Files that fail the text probe remain unsupported and land in the error report.

## Validation

- Extend format detection tests for the new extensions.
- Add app-level regressions for:
  - Avro scanning success
  - Xlsx sheet scanning success
  - Zip/jar expansion with nested supported files
  - Unknown-extension text fallback success
  - Binary unknown-extension rejection
- Run `./gradlew test`.
