# Regex-Only Driver License Detection Design

## Summary

Remove the dedicated `DriverLicenseNumberValidator` helper and all `driver_license_number`
special-case branches so that detection behavior is defined only by the active ruleset regex.
After this change, aggregation and sample extraction will treat `driver_license_number` the same
way as every other PII rule.

## Goals

- Make the ruleset regex the single source of truth for `driver_license_number`.
- Remove runtime branches that revalidate driver license matches outside the ruleset.
- Keep aggregation and sample extraction behavior aligned with the actual regex match result.
- Update tests and documentation so they describe regex-based behavior only.

## Non-Goals

- Re-design the default `driver_license_number` regex beyond what is needed for consistency.
- Introduce a new generic validation framework for other rule types.
- Change unrelated aggregation, sampling, or ruleset-loading behavior.

## Current Problem

`driver_license_number` is the only built-in rule that does not rely only on its configured regex.
The current code:

- uses a dedicated `DriverLicenseNumberValidator` helper,
- builds a separate SQL predicate path for aggregation,
- re-checks sample fragments with strict validator logic before storing them,
- documents this two-stage behavior as an intentional exception.

This creates a split contract:

- the ruleset regex says one thing,
- runtime detection can silently narrow it,
- tests are spread across regex assertions and validator-specific assertions.

That makes rule behavior harder to reason about and harder to change safely.

## Decision

Use the ruleset regex as the only runtime contract for `driver_license_number`.

The detection engine will:

- compile the configured regex,
- count values using the same `matches` / `find` logic used for other rules,
- extract `sample_matched_fragment` directly from the regex match,
- stop performing any driver-license-specific post-filtering.

If a false positive or false negative exists after this change, the fix belongs in the ruleset
regex and its tests, not in a separate validator layer.

## Implementation Design

### 1. Remove dedicated driver-license helper code

- Delete `detect/DriverLicenseNumberValidator.scala`.
- Delete the standalone validator spec.
- Remove driver-license-specific constants derived from that helper.

### 2. Unify aggregation behavior

In `DetectionAggregator`:

- remove the `driver_license_number` branch from metric predicate creation,
- remove the SQL `regexp_extract_all` + predicate validation path,
- let all rules use the same regex-based predicate logic.

### 3. Unify sample extraction behavior

In `DetectionAggregator`:

- remove the special extraction path for `driver_license_number`,
- let sample extraction use the same regex matcher path used by other rule types,
- store the fragment that the regex actually matched.

### 4. Move behavior checks to regex contract tests

Testing shifts from helper-focused validation to rule-focused validation:

- `RulesetLoaderSpec` keeps the regex contract for valid and invalid driver license examples.
- `DetectionAggregatorSpec` verifies aggregation and sample extraction use regex results directly.
- `PrivySparkAppSpec` keeps an end-to-end sample fragment assertion, now based on regex-only behavior.

Tests that only existed to prove validator-side filtering will be removed or rewritten.

## Expected Behavior Changes

- A `driver_license_number` value matched by the configured regex will now be counted and sampled
  without additional runtime rejection.
- Values that were previously filtered only by `DriverLicenseNumberValidator` may become detectable
  if the regex still matches them.

This is intentional. Regex quality is now the only control surface.

## Risks and Mitigations

### Risk: Existing regex is broader than the old runtime validator

Mitigation:

- keep and strengthen driver-license regex contract tests in `RulesetLoaderSpec`,
- run targeted aggregator and application tests covering current examples,
- if behavior changes are unwanted, tighten the ruleset regex instead of adding runtime branches back.

### Risk: Documentation drifts from runtime behavior

Mitigation:

- update both Korean and English detection reference docs,
- update performance docs to remove references to the driver-license-only path.

## Files Expected To Change

- `src/main/scala/io/github/jonggeun2001/privyspark/detect/DetectionAggregator.scala`
- `src/main/scala/io/github/jonggeun2001/privyspark/detect/DriverLicenseNumberValidator.scala`
- `src/test/scala/io/github/jonggeun2001/privyspark/DriverLicenseNumberValidatorSpec.scala`
- `src/test/scala/io/github/jonggeun2001/privyspark/DetectionAggregatorSpec.scala`
- `src/test/scala/io/github/jonggeun2001/privyspark/PrivySparkAppSpec.scala`
- `src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala`
- `docs/ko/reference/rules-and-detection.md`
- `docs/en/reference/rules-and-detection.md`
- `docs/ko/operations/performance.md`
- `docs/en/operations/performance.md`

## Verification Strategy

- Run focused unit tests for ruleset loading, aggregation, app-level scanning, and any replacement
  coverage for the deleted validator spec.
- Run the repository standard test entrypoint after updating or generating
  `scripts/verify-worktree.sh`.
- Confirm documentation no longer references driver-license-only runtime validation.
