# PrivySpark PRD (MVP v0.1)

## 1. Goal
Build a Spark-based scanner to detect potential personal data (PII) from a user-provided dataset path and generate a report.

## 2. Functional Requirements

### 2.1 Input
- User provides dataset location via CLI (`--path`).
- File format is unknown in advance.
- Scanner should operate on any Spark-supported filesystem.

### 2.2 Supported Detection Types (initial)
- Name
- Phone number
- Email
- Resident registration number
- Address
- Bank account number
- Credit card number
- Passport number
- IP address
- Extensible for additional PII types later

### 2.3 Detection Method
- Regex-based detection only for MVP.

### 2.4 Detection Granularity
- Column-level reporting.

### 2.5 Quality Priority
- Minimize false positives.

### 2.6 Execution Mode
- One-time batch run.

### 2.7 Output
- Report written to an output path (`--output`) on Spark-supported filesystem.

### 2.8 Post-Detection Action
- Reporting only (no automatic masking, blocking, or remediation).

## 3. Non-Goals (MVP)
- Real-time/streaming detection
- ML/NLP-based classification
- Automatic masking/redaction
- Pipeline failure/blocking action

## 4. Proposed CLI (draft)
```bash
privyspark scan \
  --path <input_path> \
  --output <report_path> \
  --format auto \
  --ruleset default \
  --sample-ratio 0.2
```

## 5. Output Schema (draft)
- dataset_path
- scan_timestamp
- file_identifier
- column_name
- pii_type
- match_count
- match_ratio
- confidence

## 6. Future Extensions
- Additional rule packs
- Configurable confidence threshold per pii_type
- Incremental partition scanning
- Optional alert integration
