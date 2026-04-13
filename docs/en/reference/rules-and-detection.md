# Rulesets and Detection

## Detection Model
- PrivySpark uses regex-based candidate detection plus strict validators for selected PII types.
- Results are aggregated at column level or file level.
- Invalid regexes fail immediately during ruleset loading.

Rulesets are validated before scanning so long-running jobs do not fail late because of a malformed regex. In practice, a start-up failure is safer and easier to operate than a delayed runtime failure.

## Default Ruleset
- Default file: `config/rules/default.yaml`
- Built-in default PII types:
  - phone number
  - email
  - resident registration number
  - foreign registration number
  - driver license number
  - address
  - bank account number
  - credit card number
  - Korean passport number
  - IP address

## Custom Ruleset Contract
- Each rule must include `pii_type` and `regex`.
- `column_hints` is optional and limits the rule to matching column names.
- `match_type` is optional and defaults to `value`.
- Supported `match_type` values are `value` and `full_column`.

## Unsupported Rule Shapes
- `pii_type: name`
- `validator` field
- `__KOREAN_NAME_RULE_REGEX__` placeholders

## `match_type`
- `value`: counts values that match the regex.
- `full_column`: evaluates each non-empty value as a full regex match.
- The internal `text` fallback format also treats each line as a single value and applies `full_column` as a full-line match.

`full_column` exists because exact-value formats such as resident registration numbers behave very differently from substring detection inside free-form text. Mixing both behaviors under one mode would increase false positives.

## Type-Specific Constraints
- `phone_number`: supports domestic `010`/`011`/`016`/`017`/`018`/`019` patterns and `+82 10...`-style international forms.
- `resident_registration_number`: supports hyphenated and compact forms, including a 1-digit gender/century short form.
- `resident_registration_number`: the default ruleset only constrains month `01`-`12` and day `01`-`31`, and rejects matches inside longer numeric tokens.
- `driver_license_number`: supports hyphenated and compact forms, validates only legacy 10-digit and current 12-digit formats, and only allows current region codes `11`-`26`, `28`.
- `passport_number`: only matches the Korean passport format and avoids substrings inside longer alphanumeric tokens.

## Aggregation Strategy
- The primary path uses batched aggregation with `agg`.
- When expression count exceeds the threshold (`50,000`), PrivySpark falls back to smaller aggregation batches.
- If aggregation still fails, it switches to a safe legacy fallback.
- File-level aggregation uses an internal dynamic file-identifier column to avoid collisions with user columns.

Batched aggregation is the default because running `filter().count()` per metric would explode Spark job counts. Grouping metrics into batches reduces scan repetition and scheduler overhead.
