# PrivySpark Documentation

This directory contains the public English documentation for PrivySpark. The Korean documentation is the canonical source; both language trees describe the same current behavior.

- Korean documentation: [../ko/README.md](../ko/README.md)

## Getting Started

- [getting-started/quick-start.md](getting-started/quick-start.md): build, test, YARN submission, custom ruleset distribution

## Reference

- [reference/overview.md](reference/overview.md): scope and feature summary
- [reference/input-formats.md](reference/input-formats.md): input formats, archive/xlsx handling, grouping, fallback behavior
- [reference/rules-and-detection.md](reference/rules-and-detection.md): rulesets, `match_type`, built-in detection constraints, aggregation
- [reference/reports-and-errors.md](reference/reports-and-errors.md): result reports, error reports, sample-value storage policy
- [reference/review-workflow.md](reference/review-workflow.md): legacy exact-allowlist file generation and migration to recurring review
- [reference/offline-review-collector.md](reference/offline-review-collector.md): serverless owner review, response JSON collection, cumulative state, validation, and retention; [Korean reference](../ko/reference/offline-review-collector.md)

## Architecture

- [architecture/overview.md](architecture/overview.md): component map, processing flow, operational invariants

## Operations

- [operations/execution.md](operations/execution.md): CLI options, parallelism, sampling, logging, progress handling, releases
- [operations/performance.md](operations/performance.md): runtime characteristics and tuning guidance

## Development and Samples

- [Code map](../dev/CODE_MAP.md): package responsibilities and source entry points (Korean).
- [Input cases](../../samples/input-cases/README.md) and [offline review examples](../../samples/offline-review/README.md).
