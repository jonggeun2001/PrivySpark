# Foreign Registration Number Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `foreign_registration_number` as a bundled default regex rule and document the expanded default detection scope.

**Architecture:** The change stays inside the existing ruleset-driven design. No detection pipeline code changes are needed because bundled rules already flow through `RulesetLoader` into `DetectionAggregator`; the work is a default ruleset update, one regression test, and synchronized product docs.

**Tech Stack:** Scala 2.12, ScalaTest, Gradle, YAML ruleset, Markdown docs

---

### Task 1: Lock in the bundled ruleset expectation with a failing test

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala`
- Test: `src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
test("loads default ruleset") {
  val rules = RulesetLoader.load("default")
  assert(rules.nonEmpty)
  assert(rules.exists(_.piiType == "email"))
  assert(rules.exists(_.piiType == "foreign_registration_number"))
  assert(rules.forall(_.columnHints.isEmpty))
  assert(rules.forall(_.matchType == "value"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.RulesetLoaderSpec`
Expected: FAIL because the bundled default ruleset does not yet contain `foreign_registration_number`.

- [ ] **Step 3: Write minimal implementation**

```yaml
- pii_type: foreign_registration_number
  regex: '[0-9]{6}-?[5-8][0-9]{6}'
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.RulesetLoaderSpec`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala config/rules/default.yaml
git commit -m "feat: add foreign registration number default rule"
```

### Task 2: Synchronize user-facing documentation with the new default rule

**Files:**
- Modify: `README.md`
- Modify: `docs/PRD-Functional.md`
- Modify: `docs/PRD-Architecture.md`
- Test: `README.md`

- [ ] **Step 1: Write the failing documentation expectation**

Review the default detection type lists and identify every place that still omits foreign registration numbers.

- [ ] **Step 2: Update the docs minimally**

Add `외국인 등록번호` / `foreign_registration_number` to the documented default detection scope without changing unrelated requirements.

- [ ] **Step 3: Run full regression verification**

Run: `./gradlew test`
Expected: PASS

- [ ] **Step 4: Inspect the diff**

Run: `git diff --stat && git diff -- src/test/scala/io/github/jonggeun2001/privyspark/RulesetLoaderSpec.scala config/rules/default.yaml README.md docs/PRD-Functional.md docs/PRD-Architecture.md`
Expected: Only the intended ruleset, test, and docs changes appear.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/PRD-Functional.md docs/PRD-Architecture.md
git commit -m "docs: update default detection scope for foreign registration number"
```

### Task 3: Finish the branch workflow

**Files:**
- Modify: none
- Test: repository CI / PR state

- [ ] **Step 1: Push branch**

Run: `git push -u origin feature/add-foreign-registration-number-detection`

- [ ] **Step 2: Open or update the PR**

Run: `gh pr create --base main --head feature/add-foreign-registration-number-detection --title "외국인 등록번호 기본 검출 규칙 추가" --body-file /tmp/pr_body_ko.md`

- [ ] **Step 3: Run independent review gate**

Run: `bash /Users/jonggeun/.agents/skills/worktree-flow/scripts/review_gate.sh "/Users/jonggeun/IdeaProjects/PrivySpark-worktrees/add-foreign-registration-number-detection" "feature/add-foreign-registration-number-detection" "main"`

- [ ] **Step 4: Wait for CI**

Run: `gh pr checks "$(gh pr view feature/add-foreign-registration-number-detection --json number --template '{{.number}}')" --watch`

- [ ] **Step 5: Merge and clean up**

Run: `gh pr merge "$(gh pr view feature/add-foreign-registration-number-detection --json number --template '{{.number}}')" --squash --delete-branch`
