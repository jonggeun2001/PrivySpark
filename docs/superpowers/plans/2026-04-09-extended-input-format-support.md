# Extended Input Format Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Avro, Excel, archive-expanded, and text-probed input support without changing PrivySpark's report schema.

**Architecture:** Extend pre-scan into a normalization layer that can expand archives, represent workbook sheets as logical files, and downgrade unknown extensions to a `text` scan path when their bytes look textual. Keep downstream grouping, schema detection, sampling, and aggregation on the existing scan pipeline by adding reader support for `avro`, `xlsx`, and `text`.

**Tech Stack:** Scala 2.12, Spark SQL 3.5.x, Spark Avro datasource, spark-excel, ScalaTest, Gradle, Markdown docs

---

### Task 1: Lock in format-detection expectations with failing tests

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/FormatDetectorSpec.scala`

- [ ] **Step 1: Add failing detector assertions**

```scala
assert(FormatDetector.infer("/data/input.avro").contains("avro"))
assert(FormatDetector.infer("/data/input.xlsx").contains("xlsx"))
assert(FormatDetector.infer("/data/archive.zip").contains("zip"))
assert(FormatDetector.infer("/data/archive.jar").contains("jar"))
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.FormatDetectorSpec`
Expected: FAIL because the new extensions are not detected yet.

### Task 2: Add focused app regressions before implementation

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/PrivySparkAppSpec.scala`

- [ ] **Step 1: Add a failing Avro regression**

Create a small Spark-written Avro dataset with a value matching a default rule and assert the scan reports that match.

- [ ] **Step 2: Add a failing Xlsx regression**

Create a workbook fixture with at least one populated sheet and assert the output identifier includes `#SheetName`.

- [ ] **Step 3: Add a failing archive regression**

Create a zip or jar containing a supported nested file and assert the output identifier includes `archive.ext!nested/file.csv`.

- [ ] **Step 4: Add text-fallback regressions**

Add one unknown-extension text file that should scan successfully and one binary-like file that should still produce `Unsupported file format`.

- [ ] **Step 5: Run the focused app spec and verify RED**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.PrivySparkAppSpec`
Expected: FAIL because the new readers and pre-scan behavior do not exist yet.

### Task 3: Add datasource dependencies and minimal format detection

**Files:**
- Modify: `build.gradle.kts`
- Modify: `src/main/scala/io/github/jonggeun2001/privyspark/FormatDetector.scala`

- [ ] **Step 1: Add Spark Avro and spark-excel dependencies**

Use Spark/Scala-compatible artifacts that work with the repository's Spark `3.5.x` and Scala `2.12` targets, and keep them in the Shadow JAR path for offline deployment.

- [ ] **Step 2: Extend `FormatDetector`**

```scala
else if (lower.endsWith(".avro")) Some("avro")
else if (lower.endsWith(".xlsx")) Some("xlsx")
else if (lower.endsWith(".zip")) Some("zip")
else if (lower.endsWith(".jar")) Some("jar")
```

- [ ] **Step 3: Re-run the detector spec and verify GREEN**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.FormatDetectorSpec`
Expected: PASS

### Task 4: Implement ingest normalization for archives and text fallback

**Files:**
- Modify: `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`

- [ ] **Step 1: Introduce logical source metadata**

Add internal metadata that can preserve:
- the physical path Spark will read
- the logical report identifier
- the effective format
- optional workbook sheet name

- [ ] **Step 2: Expand `scanDirectoryStructure`**

Handle `zip` and `jar` by extracting nested supported files to staging and enqueueing them as logical scan targets.

- [ ] **Step 3: Add text probing**

Probe unknown extensions from a limited byte prefix and normalize text-like files to the internal `text` format.

- [ ] **Step 4: Preserve identifiers through grouping and scanning**

Ensure result and error rows use archive-entry and workbook-sheet identifiers rather than staging paths.

- [ ] **Step 5: Re-run the focused app spec**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.PrivySparkAppSpec`
Expected: fewer failures, with remaining failures isolated to missing reader support if any.

### Task 5: Implement Avro, Xlsx, and text readers

**Files:**
- Modify: `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`

- [ ] **Step 1: Extend `readSource`**

Add reader branches for:
- `avro` via Spark Avro
- `xlsx` via `spark-excel`
- `text` via Spark text reader

- [ ] **Step 2: Extend `readSchemaSource`**

Mirror the same format branches for schema detection.

- [ ] **Step 3: Keep CSV-specific header logic isolated**

Do not let the new formats reuse CSV header detection or schema assumptions.

- [ ] **Step 4: Re-run the focused app spec and verify GREEN**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test --tests io.github.jonggeun2001.privyspark.PrivySparkAppSpec`
Expected: PASS

### Task 6: Sync product and architecture docs

**Files:**
- Modify: `README.md`
- Modify: `docs/PRD-Functional.md`
- Modify: `docs/PRD-Architecture.md`

- [ ] **Step 1: Update supported input format lists**

Document `avro`, `xlsx`, `zip`, and `jar`.

- [ ] **Step 2: Document archive expansion and text fallback**

Explain that archive containers are expanded during pre-scan and unknown extensions are scanned as plain text when they appear textual.

### Task 7: Full verification and branch workflow

**Files:**
- Modify: repository state only

- [ ] **Step 1: Run full verification**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home PATH="$JAVA_HOME/bin:$PATH" ./gradlew test`
Expected: PASS

- [ ] **Step 2: Review the diff**

Run: `git diff --stat && git diff`

- [ ] **Step 3: Commit and push**

```bash
git add build.gradle.kts src/main/scala/io/github/jonggeun2001/privyspark/FormatDetector.scala src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala src/test/scala/io/github/jonggeun2001/privyspark/FormatDetectorSpec.scala src/test/scala/io/github/jonggeun2001/privyspark/PrivySparkAppSpec.scala README.md docs/PRD-Functional.md docs/PRD-Architecture.md docs/superpowers/specs/2026-04-09-extended-input-format-support-design.md docs/superpowers/plans/2026-04-09-extended-input-format-support.md
git commit -m "feat: support avro excel archive and text inputs"
git push -u origin feature/support-archive-avro-xlsx-text-formats
```

- [ ] **Step 4: Open PR, pass review gate, merge, clean up**

Use the `worktree-flow` review/merge steps for the new branch.
