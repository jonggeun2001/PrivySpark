# 성능 개선 플랜 A: DetectionAggregator 중심 (탐지 엔진 최적화)

> **담당:** Person A
> **기준 커밋:** `659f526` (withFileReadRetry 추가)

## 수정 대상 파일

| 파일 | Phase |
|------|-------|
| `src/main/scala/io/github/jonggeun2001/privyspark/DetectionAggregator.scala` | A-1, A-2 |
| `src/main/scala/io/github/jonggeun2001/privyspark/model/Models.scala` | A-3 |
| `config/rules/default.yaml` | A-3 |
| `src/test/scala/io/github/jonggeun2001/privyspark/DetectionAggregatorSpec.scala` | A-2, A-3 |

Person B의 `PrivySparkApp.scala`와 파일 충돌 없음.

---

## A-Phase 1: Quick Win

### A-1. Debug 로깅 플래그 캐싱

**파일:** `DetectionAggregator.scala:19-27`

**문제:** `isDebugLoggingEnabled`가 `logDebug()` 호출마다 `sys.props.get` + `sys.env.get` 수행. 스캔 중 수십~수백회 호출.

**변경:**
```scala
// 기존 (line 19-27): 매번 sys.props/sys.env 조회
private def isDebugLoggingEnabled: Boolean = {
  val rawValue = sys.props.get(DebugPropertyName).orElse(sys.env.get(DebugEnvName))
  ...
}

// 변경 후: 최초 1회만 조회, 이후 캐싱
@volatile private var _debugCached: java.lang.Boolean = _

private def isDebugLoggingEnabled: Boolean = {
  val cached = _debugCached
  if (cached != null) return cached.booleanValue()
  val result = {
    val rawValue = sys.props.get(DebugPropertyName).orElse(sys.env.get(DebugEnvName))
    rawValue.exists { value =>
      value.trim.toLowerCase match {
        case "1" | "true" | "yes" | "on" => true
        case _ => false
      }
    }
  }
  _debugCached = java.lang.Boolean.valueOf(result)
  result
}

// 테스트 재설정용
private[privyspark] def resetDebugCache(): Unit = { _debugCached = null }
```

**위험:** 매우 낮음
**검증:** 기존 debug 로깅 테스트 통과 확인

---

## A-Phase 2: 핵심 개선

### A-2. 레거시 폴백을 배치 단위 집계로 대체

**파일:** `DetectionAggregator.scala:13, 224-229, 256-278`

**문제:**
- `legacyFallbackThreshold`(기본 10,000) 초과 시 `aggregateLegacy`가 메트릭당 개별 `filter().count()` 실행
- 9규칙 × 1,112컬럼 = 10,008 메트릭 → **10,008개 개별 Spark job 생성**
- `aggregateByFileLegacy`는 더 심각: 메트릭당 `filter().groupBy().count().collect()`

**변경 1 - threshold 상향 (line 13):**
```scala
// 기존
final case class AggregationConfig(maxExpressionsPerAgg: Int = 400, legacyFallbackThreshold: Int = 10000)

// 변경 후
final case class AggregationConfig(maxExpressionsPerAgg: Int = 400, legacyFallbackThreshold: Int = 50000)
```

**변경 2 - `aggregateLegacy` (line 224-229):**
```scala
// 기존: 메트릭당 개별 Spark job
private def aggregateLegacy(sampledDf: DataFrame, metrics: Seq[Metric]): Seq[MatchCount] = {
  metrics.flatMap { metric =>
    val count = sampledDf.filter(metric.predicate).count()
    if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count)) else None
  }
}

// 변경 후: 작은 배치 크기로 집계
private def aggregateLegacy(sampledDf: DataFrame, metrics: Seq[Metric]): Seq[MatchCount] = {
  aggregateInBatches(sampledDf, metrics, maxExpressionsPerAgg = 50)
}
```

**변경 3 - `aggregateByFileLegacy` (line 256-278):**
```scala
// 기존: 메트릭당 filter+groupBy+count+collect
private def aggregateByFileLegacy(
  sampledDf: DataFrame,
  fileIdentifierColumn: String,
  metrics: Seq[Metric]
): Seq[FileMatchCount] = {
  metrics.flatMap { metric =>
    val groupedRows = sampledDf
      .filter(metric.predicate)
      .groupBy(col(fileIdentifierColumn))
      .count()
      .collect()
    ...
  }
}

// 변경 후: 배치 집계 재사용
private def aggregateByFileLegacy(
  sampledDf: DataFrame,
  fileIdentifierColumn: String,
  metrics: Seq[Metric]
): Seq[FileMatchCount] = {
  aggregateByFileInBatches(sampledDf, fileIdentifierColumn, metrics, maxExpressionsPerAgg = 50)
}
```

**효과:** 9,000 Spark jobs → 180 jobs (**50배 감소**)
**위험:** 낮음-중간. 배치 집계는 이미 primary path로 검증됨

**테스트 변경 사항:**
- `DetectionAggregatorSpec`의 legacy fallback 테스트에서 threshold 값 업데이트
- 결과 정확성 검증 로직은 그대로 유지

---

## A-Phase 3: 추가 최적화

### A-3. 컬럼명 기반 규칙 사전 필터링 (opt-in)

**파일:** `Models.scala`, `DetectionAggregator.scala:186-198`, `default.yaml`

**문제:** 모든 컬럼에 모든 규칙 적용 → O(rows × cols × rules). 9규칙 × 100컬럼 = 900 메트릭.

**변경 1 - `Models.scala`에 필드 추가:**
```scala
// 기존
final case class PiiRule(piiType: String, regex: String)

// 변경 후
final case class PiiRule(piiType: String, regex: String, columnHints: Seq[String] = Seq.empty)
```

**변경 2 - `buildMetrics` 필터링 로직 추가 (line 186-198):**
```scala
private def buildMetrics(columns: Seq[String], rules: Seq[PiiRule]): Seq[Metric] = {
  columns.zipWithIndex.flatMap {
    case (columnName, columnIndex) =>
      val lowerColName = columnName.toLowerCase
      rules.zipWithIndex.flatMap {
        case (rule, ruleIndex) =>
          // columnHints가 비어있으면 기존 동작 유지 (모든 컬럼 검사)
          val shouldTest = rule.columnHints.isEmpty ||
            rule.columnHints.exists(hint => lowerColName.contains(hint.toLowerCase))
          if (shouldTest) {
            val alias = s"m_${columnIndex}_${ruleIndex}"
            val valueColumn = col(columnName).cast(StringType)
            val predicate = valueColumn.isNotNull && valueColumn.rlike(rule.regex)
            Some(Metric(alias = alias, columnName = columnName, piiType = rule.piiType, predicate = predicate))
          } else None
      }
  }
}
```

**변경 3 - `default.yaml` 힌트 추가 (선택사항):**
```yaml
# 예시: email 규칙에 컬럼명 힌트
- pii_type: email
  regex: '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
  column_hints:
    - email
    - mail
    - e_mail
    - contact
```

**효과:** 힌트 설정 시 메트릭 수 2-5배 감소
**위험:** 중간. 빈 힌트는 기존 동작 그대로 유지 (false negative 없음)

---

## 검증 명령

```bash
# Phase별 단위 테스트
./gradlew test --tests "*.DetectionAggregatorSpec"

# 전체 회귀 테스트 (Person B 작업과 merge 후)
./gradlew test
```

## 병합 순서

1. **A-1, A-2** 작업 → PR 생성
2. Person B의 B-1~B-4 PR과 동시 merge 가능 (충돌 없음)
3. 통합 `./gradlew test` 검증
4. **A-3** 작업 → 별도 PR (Person B의 B-5, B-6과 동시 진행)
