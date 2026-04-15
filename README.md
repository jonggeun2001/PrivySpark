# PrivySpark

PrivySpark는 Spark 기반 배치 스캐너입니다. 데이터셋에서 잠재적 개인정보(PII)를 ruleset 기반 정규식과 타입별 strict validator로 탐지하고, 최종 결과를 `scan_results`와 `scan_errors` 리포트로 생성합니다.

한국어 문서가 기준 문서입니다. 영어 문서는 공개 사용자용 대응본으로 함께 제공합니다.

- 한국어 문서: [docs/ko/README.md](docs/ko/README.md)
- English documentation: [docs/en/README.md](docs/en/README.md)

## 핵심 기능
- 입력 경로는 절대경로 또는 URI만 허용합니다.
- 지원 입력은 `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`입니다.
- 무확장자 파일과 미지원 확장자 파일은 `parquet`/`orc` 매직바이트를 우선 판별하고, 바이너리처럼 보이지 않는 UTF-8 텍스트는 내부 `text` 포맷으로 정규화해 스캔합니다.
- 0바이트 파일과 0바이트 archive entry는 포맷 판별과 오류 리포트 대상에서 제외하고 건너뜁니다.
- row sampling(`--sample-ratio`)과 batch group용 file sampling(`--file-sample-ratio`)을 분리해 제어할 수 있습니다.
- `--ignore`, `--ignore-file`로 gitignore 스타일 glob 패턴을 지정해 파일/아카이브 엔트리를 pre-scan 전에 제외할 수 있습니다.
- 실행 중에는 `<output>/_progress/<run_id>` 아래에 group/file 완료 단위 JSONL progress를 남기고, 정상 종료 시 최종 Parquet/CSV 리포트로 merge한 뒤 정리합니다.

## 빠른 시작
빌드:

```bash
./gradlew clean shadowJar
```

테스트:

```bash
./gradlew test
```

YARN cluster 실행:

```bash
PRIVYSPARK_DEBUG=info \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --sample-ratio 0.2 \
  --file-sample-ratio 0.1 \
  --pre-scan-parallelism 6 \
  --group-parallelism 8 \
  --file-parallelism 4 \
  --ignore "_SUCCESS" \
  --ignore "backup/**"
```

자세한 실행 절차와 옵션은 [docs/ko/getting-started/quick-start.md](docs/ko/getting-started/quick-start.md), [docs/ko/operations/execution.md](docs/ko/operations/execution.md)에 정리돼 있습니다.

## 문서 구조
- 시작하기: [docs/ko/getting-started/quick-start.md](docs/ko/getting-started/quick-start.md), [docs/en/getting-started/quick-start.md](docs/en/getting-started/quick-start.md)
- 제품/기능 개요: [docs/ko/reference/overview.md](docs/ko/reference/overview.md), [docs/en/reference/overview.md](docs/en/reference/overview.md)
- 입력 포맷과 정규화: [docs/ko/reference/input-formats.md](docs/ko/reference/input-formats.md), [docs/en/reference/input-formats.md](docs/en/reference/input-formats.md)
- ruleset과 탐지 모델: [docs/ko/reference/rules-and-detection.md](docs/ko/reference/rules-and-detection.md), [docs/en/reference/rules-and-detection.md](docs/en/reference/rules-and-detection.md)
- 출력과 오류 리포트: [docs/ko/reference/reports-and-errors.md](docs/ko/reference/reports-and-errors.md), [docs/en/reference/reports-and-errors.md](docs/en/reference/reports-and-errors.md)
- 아키텍처: [docs/ko/architecture/overview.md](docs/ko/architecture/overview.md), [docs/en/architecture/overview.md](docs/en/architecture/overview.md)
- 운영과 릴리즈: [docs/ko/operations/execution.md](docs/ko/operations/execution.md), [docs/en/operations/execution.md](docs/en/operations/execution.md)
- 성능 가이드: [docs/ko/operations/performance.md](docs/ko/operations/performance.md), [docs/en/operations/performance.md](docs/en/operations/performance.md)

## 샘플 데이터셋
- 재현 가능한 입력 케이스 번들은 [samples/input-cases/README.md](samples/input-cases/README.md)에 있습니다.
- `./gradlew generateSampleDatasets`는 현재 입력 처리 경로를 재현하는 샘플 케이스를 다시 생성합니다.
- `./gradlew packageSampleDatasets`는 샘플 번들을 `build/distributions/privyspark-sample-datasets.zip`으로 패키징하고, 릴리즈 자산에서는 `privyspark-<tag>-sample-datasets.zip`으로 배포합니다.

## 소스 구조
- `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`: 입력 확장, 그룹화, 스캔 오케스트레이션, progress/최종 리포트 저장
- `src/main/scala/io/github/jonggeun2001/privyspark/Cli.scala`: CLI 파싱과 실행 옵션 정의
- `src/main/scala/io/github/jonggeun2001/privyspark/DetectionAggregator.scala`: 규칙별 집계와 fallback 전략
- `src/main/scala/io/github/jonggeun2001/privyspark/DriverLogger.scala`: driver 로그 레벨과 공통 로그 포맷
- `src/main/scala/io/github/jonggeun2001/privyspark/FormatDetector.scala`: 지원 포맷 판별
- `src/main/scala/io/github/jonggeun2001/privyspark/config/RulesetLoader.scala`: 기본/외부 ruleset 로딩과 검증
- `src/main/scala/io/github/jonggeun2001/privyspark/model/Models.scala`: ruleset, 결과, 오류 모델

## 릴리즈
- 태그 `v*` 또는 bare semver(`0.1.3`) 푸시 시 GitHub Actions가 Shadow fat JAR를 빌드해 Release 자산으로 업로드합니다.
- 결과물은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, `privyspark-<tag>-sample-datasets.zip` 형식입니다.
- `default-rules.yaml`은 YARN 제출 시 함께 배포할 수 있는 예시 ruleset 파일입니다.
