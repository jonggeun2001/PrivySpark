# PrivySpark

PrivySpark is an open-source Scala + Spark batch scanner that detects potential PII in datasets.

## MVP Scope
- One-time batch execution
- Input path from CLI (`--path`)
- Works with any Spark-supported filesystem (HDFS/S3/ADLS/GCS/local, etc.)
- Regex-based detection
- Column-level reporting
- Optimized to minimize false positives
- Report output to a Spark-supported filesystem path (`--output`)
- Reporting only (no blocking, no masking)

## Status
Planning/PRD stage. Implementation will continue in a separate development environment.

## Planned CLI (draft)
```bash
privyspark scan \
  --path <input_path> \
  --output <report_path> \
  --format auto \
  --ruleset default
```
