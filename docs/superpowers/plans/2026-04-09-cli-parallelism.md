# CLI Parallelism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `privyspark scan`가 그룹/파일 병렬도 값을 직접 인자로 받아 기존 병렬도 해석 로직에 반영하도록 만든다.

**Architecture:** CLI는 병렬도 옵션을 파싱하고 검증만 수행한다. 런타임은 CLI 값을 직접 `scanGroups`/`scanGroupByFile` 경로로 전달하고, 값이 없을 때만 기존 `resolveGroupParallelism`/`resolveFileParallelism` fallback을 사용한다.

**Tech Stack:** Scala 2.12, scopt, Spark 3.5, ScalaTest, bash submit script, markdown docs

---

### Task 1: CLI 계약 고정

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/CliSpec.scala`

- [ ] **Step 1: Write the failing test**

CLI 기본값과 새 옵션 파싱, 잘못된 병렬도 거부를 검증하는 테스트를 추가한다.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.CliSpec`
Expected: 새 필드/옵션이 없어 실패

- [ ] **Step 3: Write minimal implementation**

`CliConfig`와 `Cli.scala`에 새 옵션을 추가하고 `> 0` 검증을 넣는다.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.CliSpec`
Expected: PASS

### Task 2: 런타임 병렬도 연결

**Files:**
- Modify: `src/test/scala/io/github/jonggeun2001/privyspark/ParallelismConfigSpec.scala`
- Modify: `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`

- [ ] **Step 1: Write the failing test**

CLI 설정이 런타임 병렬도 튜플로 정규화되는지 검증하는 테스트를 추가한다.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.ParallelismConfigSpec`
Expected: helper 부재 또는 동작 불일치로 FAIL

- [ ] **Step 3: Write minimal implementation**

CLI 병렬도 값을 직접 호출 경로에 전달하는 helper와 배선을 추가한다.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.ParallelismConfigSpec`
Expected: PASS

### Task 3: 사용자 문서와 제출 스크립트 정렬

**Files:**
- Modify: `bin/privyspark-submit`
- Modify: `README.md`
- Modify: `docs/PRD-Functional.md`
- Modify: `docs/PRD-Architecture.md`

- [ ] **Step 1: Update usage and requirement docs**

새 CLI 옵션 이름, 기본 동작, 유효성, CLI 우선순위를 문서에 반영한다.

- [ ] **Step 2: Run focused verification**

Run: `./gradlew test --tests io.github.jonggeun2001.privyspark.CliSpec --tests io.github.jonggeun2001.privyspark.ParallelismConfigSpec`
Expected: PASS

- [ ] **Step 3: Run full verification**

Run: `./gradlew test`
Expected: PASS

- [ ] **Step 4: Commit**

Run:
```bash
git add src/main/scala/io/github/jonggeun2001/privyspark/Cli.scala \
  src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala \
  src/test/scala/io/github/jonggeun2001/privyspark/CliSpec.scala \
  src/test/scala/io/github/jonggeun2001/privyspark/ParallelismConfigSpec.scala \
  bin/privyspark-submit README.md docs/PRD-Functional.md docs/PRD-Architecture.md \
  docs/superpowers/specs/2026-04-09-cli-parallelism-design.md \
  docs/superpowers/plans/2026-04-09-cli-parallelism.md
git commit -m "feat: add cli parallelism options"
```
