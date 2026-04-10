# PrivySpark

PrivySpark는 Spark 기반 배치 스캐너로, 데이터셋에서 잠재적 개인정보(PII)를 정규식으로 탐지해 결과 리포트와 오류 리포트를 생성합니다.

## MVP 요약
- 일회성 배치 실행
- 입력/출력 경로는 절대경로 또는 URI만 허용
- 지원 입력: `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`
- 무확장자 파일과 미지원 확장자 파일은 앞부분 매직바이트로 `parquet`, `orc`를 우선 판별하고, 바이너리처럼 보이지 않는 텍스트 입력은 단일 `value` 컬럼의 내부 `text` 포맷으로 스캔합니다. 바이너리로 보이는 입력만 `Unsupported file format`으로 기록합니다.
- 0바이트 빈 파일과 0바이트 archive entry는 포맷 판별과 오류 리포트 대상에서 제외하고 그대로 skip합니다.
- `--pre-scan-parallelism`은 파일 확장/포맷 판별뿐 아니라 그룹별 schema split 단계에도 적용됩니다.
- 탐지 방식: ruleset 기반 regex + 일부 타입의 내장 strict validator, invalid regex는 ruleset 로드 단계에서 즉시 실패
- 출력: Parquet + CSV (`scan_results`, `scan_errors`)
- 샘플링과 앱 레벨 병렬도 조정 지원

## 빠른 명령
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
PRIVYSPARK_DEBUG=debug \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --sample-ratio 0.2 \
  --pre-scan-parallelism 6 \
  --group-parallelism 8 \
  --file-parallelism 4
```

`PRIVYSPARK_DEBUG` / `-Dprivyspark.debug`는 driver 로그 레벨 설정으로 동작합니다. 지원값은 `error`, `warn`, `info`, `debug`이며 `off`로 driver 로그를 끌 수 있습니다. 기본값은 `warn`이고, 하위호환으로 `true`는 `debug`, `false`는 `warn`으로 해석합니다.

driver 로그는 `[PrivySpark][LEVEL][ISO-8601 UTC timestamp] event key=value...` 형식으로 통일됩니다. `info` 레벨에서는 `scan_start`, `scan_plan_ready`, `scan_complete` 같은 상위 실행 lifecycle이 남고, `debug` 레벨에서는 `scanDirectoryStructure`의 파일 발견, pre-scan 실행, pre-scan 후처리, 초기 그룹화 단계에 대한 duration/progress 로그까지 함께 남습니다.

## 문서 맵
- 기능 요구사항 허브: [docs/PRD-Functional.md](docs/PRD-Functional.md)
- 아키텍처 허브: [docs/PRD-Architecture.md](docs/PRD-Architecture.md)
- MVP 상세 문서 인덱스: [docs/mvp/README.md](docs/mvp/README.md)
- 성능 검토 메모: [docs/PERF-PLAN-A.md](docs/PERF-PLAN-A.md), [docs/PERF-PLAN-B.md](docs/PERF-PLAN-B.md)

## 소스 구조
- `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`: 입력 확장, 그룹화, 스캔 오케스트레이션, 리포트 저장
- `src/main/scala/io/github/jonggeun2001/privyspark/Cli.scala`: CLI 파싱과 실행 옵션 정의
- `src/main/scala/io/github/jonggeun2001/privyspark/DetectionAggregator.scala`: 규칙별 집계와 fallback 전략
- `src/main/scala/io/github/jonggeun2001/privyspark/FormatDetector.scala`: 지원 포맷 판별
- `src/main/scala/io/github/jonggeun2001/privyspark/config/RulesetLoader.scala`: 기본/외부 ruleset 로딩과 검증
- `src/main/scala/io/github/jonggeun2001/privyspark/model/Models.scala`: ruleset, 결과, 오류 모델

## 릴리즈
- 태그 `v*` 또는 bare semver(`0.1.3`) 푸시 시 GitHub Actions가 Shadow fat JAR를 빌드해 Release 자산으로 업로드합니다.
- 결과물은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml` 형식입니다.
- `default-rules.yaml`은 배포 예시 ruleset 파일이며, YARN 제출 시 `--files /abs/path/default-rules.yaml#default-rules.yaml` 또는 `PRIVYSPARK_SPARK_FILES=/abs/path/default-rules.yaml#default-rules.yaml`로 함께 전달할 수 있습니다.
