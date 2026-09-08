# Offline Review Collector

This guide explains how to distribute self-contained `review.html` files without a server and collect response JSON into cumulative review state. The [Korean version](../../ko/reference/offline-review-collector.md) is the canonical reference.

## Workflow

1. Run a scan with a shared review state root.

```bash
bin/privyspark-submit scan \
  --path hdfs:///user/username \
  --output hdfs:///privyspark/output/20260430 \
  --review-state-root hdfs:///review-state-root
```

2. The owner opens `<scan-output>/review/review.html`, or `review.html` under `--review-html-dir`, enters their employee identifier, and marks each finding false positive or true positive. For Excel editing, use `엑셀 편집용 CSV 다운로드`, edit decisions/reasons/plans/dates, then use `복호화한 CSV 불러오기` or paste TSV copied from Excel. If corporate security software encrypts CSV files, decrypt the CSV before importing it.
3. Upload the downloaded `response-<scan-path>-YYYYMMDD-HHMMSS.json` to `<review-state-root>/inbox/*.json`.
4. Run the next scan with the same `--review-state-root`. Before scanning, it collects the inbox and updates `current`. Matching recurring false positives are suppressed; true positives remain visible with their remediation state.

Collection uses context inside the response JSON. `--scan-results` is a deprecated compatibility argument that does not affect collection, although a supplied value must still pass absolute-path/URI validation. To collect without scanning:

```bash
bin/privyspark-submit review collect \
  --review-state-root hdfs:///review-state-root
```

## State Layout

```text
review-state-root/
  .collect.lock
  inbox/
    response-*.json
  current/
    allowlist.jsonl
    action_plan.jsonl
    finding_status.jsonl
    response_ledger.jsonl
```

- `allowlist.jsonl`: recurring false-positive exclusions.
- `action_plan.jsonl`: true-positive remediation plans.
- `finding_status.jsonl`: status summary for the latest collected responses and retained action plans.
- `response_ledger.jsonl`: an audit snapshot of the latest response per finding in this inbox collection, not an append-only history of every response.

Both automatic collection and `review collect` create `<review-state-root>/.collect.lock` while updating state. An existing lock makes the command fail. Normal completion and validation failure release the lock.

The scan uses `current/allowlist.jsonl` for suppression. `action_plan.jsonl` does not hide findings. An explicit `--allowlist` is combined with the state allowlist.

## Recollection and Retention

- The collector reads only lowercase `.json` files directly under the inbox; it does not recurse into subdirectories. It does not move or delete input files, so files left there are reread on every collection.
- Within one collection, it selects the greatest `responded_at` instant for each `finding_key`. A newly accepted scope replaces the corresponding allowlist/action-plan state, while unrelated entries remain.
- An older response can overwrite an existing scope in a later collection. Preserve successfully collected originals separately, and avoid leaving only old responses for that scope in the inbox.
- An empty inbox is valid. Existing recurring entries and action plans are retained, but the response ledger can become empty because it represents this collection. Keep response originals or separate archives for a complete audit history.
- All responses are validated before temporary state is written and promoted. Existing `current` files use individual `.bak` backup/restore operations; the four files are not a single transaction across I/O failures.
- A forced process termination can leave `.collect.lock` behind. There is no automatic expiration; an operator must first verify that no collect/scan is active before removing a stale lock.

## Recurring False-Positive Matching

Fingerprint/CRC-based exact entries do not suppress findings. Recurring matching supports batch files that change size, mtime, and checksum while representing the same logical column.

With Hive mapping:

```text
normalized_scan_path + hive_table_fqn + column_name + pii_type
```

Without Hive mapping:

```text
normalized_scan_path + file_identifier_pattern + column_name + pii_type
```

New recurring responses use case-sensitive exact column and PII-type values. For file patterns, `*` matches any string including path separators; other regex-special characters are literal. The HTML does not invent a wildcard: it uses the displayed `file_identifier`.

Repeated slashes in HDFS URI paths are normalized: `hdfs:///user/name` and `hdfs:////user/name` identify the same scan path.

## False-Positive Example

```json
{
  "schema_version": 1,
  "scan_path": "hdfs:///user/username",
  "responder": "owner1",
  "responded_at": "2026-04-30T10:00:00Z",
  "responses": [
    {
      "finding_key": "sha256:...",
      "finding_hash": "sha256:...",
      "file_identifier": "daily/customers/part-000.parquet",
      "hive_database": "mart",
      "hive_table": "customers",
      "hive_table_fqn": "mart.customers",
      "column_name": "test_email",
      "pii_type": "email",
      "sample_row_count": 1000,
      "match_count": 12,
      "non_empty_match_ratio": 0.12,
      "decision": "false_positive",
      "false_positive_reason": "테스트 계정 이메일 컬럼",
      "expires_at": "9999-12-31"
    }
  ]
}
```

The HTML does not show an expiry field. It automatically sets `expires_at` to the internal permanent-recurring value `9999-12-31`.

The collected Hive-mapped allowlist entry is:

```json
{"entry_type":"recurring","scan_path":"hdfs:///user/username","hive_table_fqn":"mart.customers","file_identifier_pattern":"","column_name":"test_email","pii_type":"email","reason":"테스트 계정 이메일 컬럼","reviewer":"owner1","reviewed_at":"2026-04-30T10:00:00Z","expires_at":"9999-12-31","source_finding_key":"sha256:...","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}
```

A later finding with the same scan path, Hive table, column, and PII type is suppressed even if its checksum changes. Manually maintained entries with an expired `expires_at` are not applied.

Without Hive mapping, use `file_identifier_pattern`:

```json
{"entry_type":"recurring","scan_path":"hdfs:///user/username","hive_table_fqn":"","file_identifier_pattern":"daily/customers/*.parquet","column_name":"test_email","pii_type":"email","reason":"반복 생성되는 테스트 데이터","reviewer":"owner1","reviewed_at":"2026-04-30T10:00:00Z","expires_at":"9999-12-31","source_finding_key":"sha256:...","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}
```

## True-Positive Example

```json
{
  "schema_version": 1,
  "scan_path": "hdfs:///user/username",
  "responder": "owner1",
  "responded_at": "2026-04-30T10:00:00Z",
  "responses": [
    {
      "finding_key": "sha256:...",
      "finding_hash": "sha256:...",
      "file_identifier": "daily/customers/part-000.parquet",
      "hive_database": "mart",
      "hive_table": "customers",
      "hive_table_fqn": "mart.customers",
      "column_name": "customer_phone",
      "pii_type": "phone_number",
      "sample_row_count": 1000,
      "match_count": 830,
      "non_empty_match_ratio": 0.83,
      "decision": "true_positive",
      "action_plan": "마스킹 적용",
      "action_due_date": "2026-05-15"
    }
  ]
}
```

True positives are accumulated in `action_plan.jsonl` and stay visible in later scans. When the same finding recurs, the `기존 조치 상태` column shows the prior plan, due date, and employee identifier. A `삭제 처리` plan before its due date displays `삭제 조치 필요`; an overdue plan displays `조치 기한 초과`.

## Validation Rules

- `schema_version` must be `1`.
- The envelope requires `scan_path`, `responder`, `responded_at`, and a non-empty `responses` array.
- `responder` must contain lowercase letters and digits only (`[a-z0-9]+`), without surrounding whitespace.
- `responded_at` must be an ISO-8601 instant.
- Every item requires `finding_key`, `column_name`, `pii_type`, and `decision`.
- False positives require `false_positive_reason` and `expires_at`; the HTML supplies `9999-12-31` automatically.
- New recurring false positives require exact `column_name` and `pii_type`; `*` is rejected in these fields.
- Without Hive mapping, a false positive requires `file_identifier_pattern` or `file_identifier`.
- True positives require `action_plan` and `action_due_date`.
- `expires_at` and `action_due_date` must be real `YYYY-MM-DD` dates. The collector parses dates; the browser enforces the separate today-through-30-days input/download window.
- Non-recurring scopes such as `allowlist_scope=exact` are rejected.

Downloaded responses also contain `sample_matched_fragment` and `sample_raw_value` helpers for the operator [response viewer](../../../samples/offline-review/review-response-viewer.html). The collector does not use these fields for state decisions.

If any response is invalid, the collector fails without updating `current`. When automatic collection fails, scan work does not start.

## review.html

Each review is self-contained: open it in a browser and download response JSON without server calls. The UTF-8 file budget is 2MiB (2,097,152 bytes). Larger reviews produce a `review.html` part index and `review-part-0001.html`, `review-part-0002.html`, etc. Each part contains only its own findings; submit one response JSON per part to the inbox.

Separate columns show path, Hive table, column name, PII type, sampled rows, match count, detection percentage, matched fragment/raw context, decision, existing action state, false-positive reason, remediation plan, and due date. Hive findings with the same table, column, and PII type are grouped into one row. The path is the partition-stripped table root, with badges for grouped partitions/files. Detection percentage is `match_count / sampled_row_count * 100`, shown to two decimal places; matched fragments and context are separated by actual line breaks.

CSV/TSV imports update only decision, false-positive reason, action plan, and due date by `finding_key`, independently of sort order. A new table-level grouping produces a new key, so do not reuse old CSV/TSV with a newly generated review. Import decrypted CSV when corporate software encrypts saved files. CSV preserves quoted commas and line breaks; TSV uses tabs/row breaks and preserves embedded line breaks inside Excel-quoted cells.

False positives always generate recurring responses: there are no exact/pattern selectors or expiry inputs. The browser rejects empty employee identifiers or characters outside lowercase letters/digits and focuses that field. True-positive due dates must be today through 30 days from today.

`--review-sample-mode raw|masked|none` defaults to `masked`. Masked mode partially masks the matched fragment while retaining surrounding context; none emits empty sample strings. CSV/response JSON exported by that HTML use the same display data. These modes do not change original `scan_results` samples or mask every sensitive value in the surrounding context.

Before download, every finding must have a decision and the corresponding required reason/plan/date. Invalid cells are highlighted and the first error is focused. Bulk true-positive/false-positive actions affect only rows already marked with that decision. Sorting and scrolling preserve form state while rows near the viewport are rendered.

If a single finding cannot fit the HTML budget or the part index exceeds the limit, HTML generation fails instead of truncating content. Regeneration removes existing `review-part-*.html` files in the same review directory, so use separate directories to retain review history.

## Relationship to Legacy review apply

`bin/privyspark-submit review apply` still generates a legacy exact allowlist file from edited `scan_results`. Use `review collect` for recurring offline state. Exact fingerprint entries are not used for scan suppression.

Existing `entry_type=pattern` files have a compatibility path that converts them into recurring entries on read. This retains legacy field wildcards, but new collector responses cannot introduce those wildcards.
