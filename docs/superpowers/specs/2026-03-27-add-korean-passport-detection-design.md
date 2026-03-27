# Korean Passport Detection Design

## Goal

Promote `passport_number` from a bare default rule to a verified feature that detects Korean passport numbers and rejects common substring false positives.

## Scope

- Keep `passport_number` in the default ruleset and tighten its regex for Korean passport numbers only.
- Add regression coverage for default ruleset loading and end-to-end scan results.
- Update README and PRD documents to state that `passport_number` targets Korean passports only.

## Non-Goals

- No support for foreign passport formats.
- No changes to output schema or unrelated PII rules.

## Validation

- Add a loader regression that confirms the default ruleset still ships `passport_number`.
- Add an app-level regression that verifies valid Korean passport numbers are detected while alphanumeric-adjacent false positives are not.
- Run `./gradlew test`.
