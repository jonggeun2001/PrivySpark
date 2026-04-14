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
- `email`: adds token boundaries and requires a final alphabetic TLD of at least two characters to reduce suffix-style and malformed-domain false positives.
- `resident_registration_number`: supports hyphenated and compact forms, including a 1-digit gender/century short form.
- `resident_registration_number`: the default ruleset only constrains month `01`-`12` and day `01`-`31`, and rejects matches inside longer numeric tokens.
- `foreign_registration_number`: mirrors the resident-registration month/day constraints and only allows foreign-registration codes `5`-`8` in the seventh digit.
- `driver_license_number`: accepts legacy 10-digit numbers, current 12-digit numbers, and pre-July-2-2014 Korean region-name formats such as `서울 00 - 123456 - 01` and `부산0012345601`. Current numeric region codes are still limited to `11`-`26`, `28`, and Korean region-name forms are limited to the KoROAD notice list: `서울`, `부산`, `경기`, `강원`, `충북`, `충남`, `전북`, `전남`, `경북`, `경남`, `제주`, `대구`, `인천`, `광주`, `대전`, and `울산`. A strict validator still rechecks the final candidate.
- `address`: remains relatively conservative because Korean address strings vary heavily in real datasets. Tightening it too aggressively would increase misses faster than it reduces false positives.
- `bank_account_number`: keeps hyphenated account-number detection, but tightens segment lengths to avoid obvious date-like patterns such as `YYYY-MM-DD`.
- `credit_card_number`: is limited to common 16-digit issuer prefixes, keeps Mastercard 2-series inside the `2221`-`2720` range, and avoids matches inside larger numeric tokens.
- `passport_number`: only matches the Korean passport format, avoids substrings inside longer alphanumeric tokens, and rejects obviously abnormal `00000000` serials.
- `ip_address`: keeps IPv4 range checks, avoids substrings inside longer dotted numeric tokens such as `10.0.0.1.5`, and still matches common sentence-ending forms such as `192.168.0.1.`.

The default-ruleset tightening strategy is intentionally asymmetric. Korean identifiers with a stable public format are constrained more aggressively, while high-variation types are tightened mainly at token boundaries. The goal is to reduce false positives without turning normal field variations into widespread false negatives.

## Aggregation Strategy
- The primary path uses batched aggregation with `agg`.
- When expression count exceeds the threshold (`50,000`), PrivySpark falls back to smaller aggregation batches.
- If aggregation still fails, it switches to a safe legacy fallback.
- File-level aggregation uses an internal dynamic file-identifier column to avoid collisions with user columns.

Batched aggregation is the default because running `filter().count()` per metric would explode Spark job counts. Grouping metrics into batches reduces scan repetition and scheduler overhead.
