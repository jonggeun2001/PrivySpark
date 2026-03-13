# 성능 개선 플랜 B: PrivySparkApp 중심 (스캔 파이프라인 최적화)

> **담당:** Person B
> **기준 커밋:** `659f526` (withFileReadRetry 추가)

## 수정 대상 파일

| 파일 | Phase |
|------|-------|
| `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala` | B-1~B-7 전체 |
| `src/test/scala/io/github/jonggeun2001/privyspark/PrivySparkAppSpec.scala` | B-5, B-6 |

Person A의 `DetectionAggregator.scala`, `Models.scala`, `default.yaml`와 파일 충돌 없음.

---

## B-Phase 1: Quick Wins

### B-1. CSV `inferSchema` 제거

**파일:** `PrivySparkApp.scala:981`

**문제:** `readSource`에서 CSV를 `inferSchema=true`로 읽으면 Spark가 전체 데이터를 한번 더 스캔하여 컬럼 타입 추론. 하지만 `DetectionAggregator.buildMetrics` (line 192)에서 모든 컬럼을 `col(columnName).cast(StringType)`으로 강제 변환하므로 타입 추론은 완전히 낭비.

**변경:**
```scala
// PrivySparkApp.scala readSource 메서드 내 csv case
case "csv" =>
  spark.read
    .option("header", "true")
    .option("inferSchema", "false")  // "true" → "false"
    .option("mode", "PERMISSIVE")
    .csv(filePaths: _*)
```

**효과:** CSV I/O ~2배 감소. `readSchemaSource`는 이미 `inferSchema=false` 사용 중이므로 일관성도 확보.
**위험:** 매우 낮음

---

### B-2. Non-blocking unpersist

**파일:** `PrivySparkApp.scala:893, 943`

**문제:** `unpersist(blocking = true)`가 `withFileReadRetry` finally 블록에서 동기 대기. 파일별 폴백 루프 (`scanGroupByFile`, line 690)에서 매 파일마다 블로킹 발생.

**변경:**
```scala
// line 893 (scanGroupBatch finally 블록)
sampledDf.unpersist(blocking = false)  // blocking = true → false

// line 943 (scanFileMetrics finally 블록)
sampledDf.unpersist(blocking = false)  // blocking = true → false
```

**효과:** 파일별 폴백 시 파일당 50-200ms 절약. 500파일 처리 시 최대 100초 절감.
**위험:** 낮음. Spark storage manager가 비동기 eviction 처리.

---

### B-3. Report DataFrame 캐싱

**파일:** `PrivySparkApp.scala:1007-1026`

**문제:** `resultDf`/`errorDf`를 Parquet+CSV로 4번 write하는데 캐싱 없이 매번 재생성.

**변경:**
```scala
// writeReports 메서드 내
val resultDf = spark.createDataset(results).toDF().cache()
val errorDf = spark.createDataset(errors).toDF().cache()

resultDf.write.mode("overwrite").parquet(resultParquetPath)
errorDf.write.mode("overwrite").parquet(errorParquetPath)
resultDf.write.option("header", "true").mode("overwrite").csv(resultCsvPath)
errorDf.write.option("header", "true").mode("overwrite").csv(errorCsvPath)

resultDf.unpersist(blocking = false)
errorDf.unpersist(blocking = false)
```

**효과:** 소규모. 대용량 결과(10만건+) 시 효과 있음.
**위험:** 매우 낮음

---

### B-4. Debug 로깅 플래그 캐싱

**파일:** `PrivySparkApp.scala:56-63`

**문제:** `isDebugLoggingEnabled`가 매 `logDebug()` 호출마다 `sys.props.get` + `sys.env.get` 수행.

**변경:** Person A의 A-1과 동일한 패턴 적용:
```scala
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

private[privyspark] def resetDebugCache(): Unit = { _debugCached = null }
```

**위험:** 매우 낮음

---

## B-Phase 2: 핵심 개선

### B-5. CSV 스키마 추론 고속화 (Hadoop API 직접 헤더 읽기)

**파일:** `PrivySparkApp.scala:467-549`

**문제:** `splitGroupBySchema`가 파일마다 `inferSchemaSignature` → `withFileReadRetry` → `readSchemaSource` → Spark DataFrame 생성. 500개 CSV 파일 = **500번 Spark read** (재시도 포함 최대 1,000번).

**변경 1 - 새 private 메서드 추가:**
```scala
import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

private def inferCsvHeaderSignature(
  conf: org.apache.hadoop.conf.Configuration,
  filePath: String
): Either[String, String] = {
  try {
    val path = new Path(filePath)
    val fs = path.getFileSystem(conf)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(
        new InputStreamReader(stream, StandardCharsets.UTF_8)
      )
      val headerLine = reader.readLine()
      if (headerLine == null || headerLine.trim.isEmpty) {
        Left("Empty or missing CSV header")
      } else {
        // CSV 헤더 파싱: 인용부호 처리 포함
        val fields = parseHeaderFields(headerLine)
        val normalized = fields.map(_.trim.toLowerCase)
        Right(normalized.mkString("|"))
      }
    } finally {
      stream.close()
    }
  } catch {
    case NonFatal(e) => Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
  }
}

// 기본 CSV 헤더 파싱 (인용부호 처리)
private def parseHeaderFields(line: String): Seq[String] = {
  val result = scala.collection.mutable.ArrayBuffer.empty[String]
  var i = 0
  val sb = new StringBuilder
  while (i < line.length) {
    if (line(i) == '"') {
      i += 1
      while (i < line.length && line(i) != '"') { sb.append(line(i)); i += 1 }
      if (i < line.length) i += 1 // closing quote
    } else if (line(i) == ',') {
      result += sb.toString(); sb.clear(); i += 1
    } else {
      sb.append(line(i)); i += 1
    }
  }
  result += sb.toString()
  result.toSeq
}
```

**변경 2 - `splitGroupBySchema`에 CSV 분기 추가 (line 482-509):**
```scala
group.filePaths.foreach { filePath =>
  val result = if (group.format == "csv") {
    inferCsvHeaderSignature(spark.sparkContext.hadoopConfiguration, filePath)
  } else {
    inferSchemaSignature(spark, group.format, filePath)
  }
  result match {
    case Right(schemaSignature) =>
      val groupedFiles = filesBySchema.getOrElseUpdate(schemaSignature, ArrayBuffer.empty[String])
      groupedFiles += filePath
      ...
    case Left(errorMessage) =>
      ...
  }
}
```

**효과:** 스키마 탐지 단계 수분 → **1초 미만** (500 파일 기준). Spark job overhead 완전 제거.
**위험:** 중간. `parseHeaderFields`의 인용부호 처리 edge case 테스트 필요.

**테스트 추가:**
```scala
// PrivySparkAppSpec에 추가
"inferCsvHeaderSignature should match inferSchemaSignature" in {
  // 일반 헤더, 인용부호 포함 헤더, 특수문자 헤더 등 케이스별 결과 비교
}
```

---

### B-6. 그룹 병렬 처리

**파일:** `PrivySparkApp.scala:305-326`

**문제:** `scanPlan.groups.foreach`로 전체 그룹을 순차 처리. 20개 그룹이면 합산 실행시간만큼 대기.

**변경 - `runScan` 메서드 내 `foreach` 블록 교체:**
```scala
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration.Duration

val parallelism = math.min(
  scanPlan.groups.size,
  spark.sparkContext.getConf.getInt("spark.privyspark.groupParallelism", 4)
)

if (parallelism <= 1 || scanPlan.groups.size <= 1) {
  // 단일 그룹이거나 parallelism=1이면 기존 순차 처리
  scanPlan.groups.foreach { group =>
    val (groupResults, groupErrors) = scanGroup(spark, config.inputPath, group, rules, config.sampleRatio, timestamp)
    results ++= groupResults
    errors ++= groupErrors
  }
} else {
  val pool = Executors.newFixedThreadPool(parallelism)
  implicit val ec = ExecutionContext.fromExecutor(pool)
  try {
    val futures = scanPlan.groups.map { group =>
      Future {
        logDebug("group_scan_dispatch", "directory" -> group.directoryPath, "format" -> group.format)
        scanGroup(spark, config.inputPath, group, rules, config.sampleRatio, timestamp)
      }
    }
    val allGroupResults = Await.result(Future.sequence(futures), Duration.Inf)
    allGroupResults.foreach { case (groupResults, groupErrors) =>
      results ++= groupResults
      errors ++= groupErrors
    }
  } finally {
    pool.shutdown()
  }
}
```

**효과:** 벽시계 시간 ~1/N (N=parallelism). 20그룹 + parallelism=4 → 약 5배 빠름.
**위험:** 중간-높음.
- SparkSession은 thread-safe (concurrent job submission 공식 지원)
- `withFileReadRetry`의 `refreshReadPaths`, `pauseBeforeRetry`는 thread-safe
- 메모리 압박: parallelism 제한으로 완화
- YARN 클러스터 권장 설정: `spark.scheduler.mode=FAIR`

**테스트 추가:**
```scala
// 다중 그룹 환경에서 순차/병렬 결과 동일성 검증
"parallel group scan should produce same results as sequential" in { ... }
```

---

## B-Phase 3: 추가 최적화

### B-7. 파일별 폴백 병렬화

**파일:** `PrivySparkApp.scala:690-711`

**문제:** `scanGroupByFile`이 파일별 순차 `scanFileMetrics` 호출. 1,000+ 파일 폴백 시 순차 실행.

**변경:** B-6과 동일한 `Future` 패턴 적용. thread-safe 컬렉션 사용:
```scala
import java.util.concurrent.ConcurrentLinkedQueue

val fileParallelism = math.min(
  group.filePaths.size,
  spark.sparkContext.getConf.getInt("spark.privyspark.fileParallelism", 3)
)

val successQueue = new ConcurrentLinkedQueue[FileScanMetrics]()
val errorQueue = new ConcurrentLinkedQueue[ScanError]()

if (fileParallelism <= 1) {
  // 기존 순차 처리
} else {
  val pool = Executors.newFixedThreadPool(fileParallelism)
  implicit val ec = ExecutionContext.fromExecutor(pool)
  try {
    val futures = group.filePaths.map { filePath =>
      Future {
        scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp) match {
          case Right(metrics) => successQueue.add(metrics)
          case Left(error) => errorQueue.add(error)
        }
      }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  } finally {
    pool.shutdown()
  }
}
```

**의존성:** B-6 이후 구현 (병렬화 인프라 공유)

---

## 검증 명령

```bash
# Phase 1 단위 테스트
./gradlew test --tests "*.PrivySparkAppSpec"

# 전체 회귀 테스트 (Person A 작업과 merge 후)
./gradlew test
```

## 병합 순서

1. **B-1 ~ B-4** 작업 → PR 생성
2. Person A의 A-1, A-2 PR과 동시 merge 가능 (충돌 없음)
3. 통합 `./gradlew test` 검증
4. **B-5, B-6** 작업 → 별도 PR (Person A의 A-3과 동시 진행)
5. 최종 merge 후 통합 검증
6. **B-7** → Phase 3 마무리 PR
