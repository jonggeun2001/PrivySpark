# Driver License Number Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `driver_license_number` as a bundled default detection type with strict validation for legacy 10-digit and current 12-digit Korean driver license numbers.

**Architecture:** Keep the YAML surface stable by leaving public `validator` support disabled. Add a dedicated Scala helper for driver-license normalization and strict validation, then wire it into `DetectionAggregator` as an internal built-in validator for `driver_license_number` rules.

**Tech Stack:** Scala 2.12, Spark SQL, ScalaTest, Gradle, YAML ruleset, Markdown docs

---

### Task 1: Lock in the default ruleset expectation with failing tests

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala`
- Create: `src/test/scala/io/github/jonggeun2001/privyspark/DriverLicenseNumberValidatorSpec.scala`

- [ ] **Step 1: Write the failing ruleset test**

```scala
val driverLicenseRule = rules.find(_.piiType == "driver_license_number")
assert(driverLicenseRule.nonEmpty)
assert(driverLicenseRule.get.regex == "(?<![0-9])(?:[0-9]{10}|[0-9]{12}|[0-9]{2}-[0-9]{6}-[0-9]{2}|[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2})(?![0-9])")
```

- [ ] **Step 2: Write the failing validator unit tests**

```scala
assert(DriverLicenseNumberValidator.isValid("11-12-345678-90"))
assert(DriverLicenseNumberValidator.isValid("111234567890"))
assert(DriverLicenseNumberValidator.isValid("12-345678-90"))
assert(!DriverLicenseNumberValidator.isValid("10-12-345678-90"))
assert(!DriverLicenseNumberValidator.isValid("11123456789"))
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.RulesetLoaderSpec --tests io.github.jonggeun2001.privyspark.DriverLicenseNumberValidatorSpec`
Expected: FAIL because the rule and validator do not exist yet.

### Task 2: Implement the strict validator and aggregator integration

**Files:**
- Create: `src/main/scala/io/github/jonggeun2001/privyspark/DriverLicenseNumberValidator.scala`
- Modify: `src/main/scala/io/github/jonggeun2001/privyspark/DetectionAggregator.scala`
- Modify: `config/rules/default.yaml`

- [ ] **Step 1: Add the validator helper**

```scala
object DriverLicenseNumberValidator {
  def isValid(raw: String): Boolean = ???
}
```

- [ ] **Step 2: Wire the validator into metric predicates**

Use a built-in validator lookup by `piiType` so `driver_license_number` metrics require both candidate regex match and validator pass.

- [ ] **Step 3: Add the bundled regex**

```yaml
- pii_type: driver_license_number
  regex: '(?<![0-9])(?:[0-9]{10}|[0-9]{12}|[0-9]{2}-[0-9]{6}-[0-9]{2}|[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2})(?![0-9])'
```

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.RulesetLoaderSpec --tests io.github.jonggeun2001.privyspark.DriverLicenseNumberValidatorSpec`
Expected: PASS

### Task 3: Add aggregation regression coverage

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/DetectionAggregatorSpec.scala`

- [ ] **Step 1: Write the failing aggregation regression**

Create a small DataFrame with:
- valid 12-digit current license values
- valid 10-digit legacy values
- regex-matching but invalid region/length values

Assert that only the strictly valid values are counted for `driver_license_number`.

- [ ] **Step 2: Run the single spec and verify RED/GREEN**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.DetectionAggregatorSpec`
Expected: FAIL before integration is complete, PASS after.

### Task 4: Sync product and architecture docs

**Files:**
- Modify: `README.md`
- Modify: `docs/PRD-Functional.md`
- Modify: `docs/PRD-Architecture.md`

- [ ] **Step 1: Update default detection scope**

Add `운전면허번호` / `driver_license_number` to the bundled detection list.

- [ ] **Step 2: Document strict validation behavior**

Describe that driver license detection accepts hyphenated or digits-only input and validates legacy 10-digit / current 12-digit formats with current-region restrictions for 12-digit numbers.

### Task 5: Full verification and branch workflow

**Files:**
- Modify: repository state only

- [ ] **Step 1: Run full verification**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test`
Expected: PASS

- [ ] **Step 2: Review the diff**

Run: `git diff --stat && git diff`

- [ ] **Step 3: Commit and push**

```bash
git add config/rules/default.yaml src/main/scala/io/github/jonggeun2001/privyspark/DriverLicenseNumberValidator.scala src/main/scala/io/github/jonggeun2001/privyspark/DetectionAggregator.scala src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala src/test/scala/io/github/jonggeun2001/privyspark/DriverLicenseNumberValidatorSpec.scala src/test/scala/io/github/jonggeun2001/privyspark/DetectionAggregatorSpec.scala README.md docs/PRD-Functional.md docs/PRD-Architecture.md docs/superpowers/specs/2026-03-30-driver-license-number-design.md docs/superpowers/plans/2026-03-30-driver-license-number.md
git commit -m "feat: add driver license number detection"
git push -u origin feature/add-driver-license-number-detection
```

- [ ] **Step 4: Open PR, pass review gate, merge, clean up**

Use the `worktree-flow` review/merge steps for the new branch.
