# Name Detection Removal Design

## Goal

Remove the `name` PII type entirely so PrivySpark no longer ships, accepts, or documents name detection.

## Scope

- Remove the default `name` rule from `config/rules/default.yaml`.
- Reject custom rulesets that define `pii_type: name`.
- Remove loader branches that only existed for the old name-detection implementation.
- Update user-facing documentation to remove name detection from supported types and ruleset examples.

## Non-Goals

- No changes to non-`name` PII types.
- No output schema changes.
- No unrelated refactoring in scan or aggregation flow.

## Validation

- Add a regression test that proves the default ruleset no longer includes `name`.
- Add a regression test that proves a custom ruleset with `pii_type: name` fails to load.
- Run `./gradlew test`.
