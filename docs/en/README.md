# PrivySpark Documentation

This directory contains the public English documentation for PrivySpark. The Korean documentation is the canonical source and may be updated first.

- Korean documentation: [../ko/README.md](../ko/README.md)

## Getting Started
- [getting-started/quick-start.md](getting-started/quick-start.md): build, test, YARN submission, custom ruleset distribution

## Reference
- [reference/overview.md](reference/overview.md): scope and feature summary
- [reference/input-formats.md](reference/input-formats.md): input formats, archive/xlsx handling, grouping, fallback behavior
- [reference/rules-and-detection.md](reference/rules-and-detection.md): rulesets, `match_type`, built-in detection constraints, aggregation
- [reference/reports-and-errors.md](reference/reports-and-errors.md): result reports, error reports, sample-value storage policy
- [reference/review-workflow.md](reference/review-workflow.md): false-positive review, allowlist creation, and re-scan suppression
- Korean reference [../ko/reference/offline-review-collector.md](../ko/reference/offline-review-collector.md): serverless owner review, response JSON collection, and cumulative review state

## Architecture
- [architecture/overview.md](architecture/overview.md): component map, processing flow, operational invariants

## Operations
- [operations/execution.md](operations/execution.md): CLI options, parallelism, sampling, logging, progress handling, releases
- [operations/performance.md](operations/performance.md): runtime characteristics and tuning guidance
