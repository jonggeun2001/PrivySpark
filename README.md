# PrivySpark

PrivySpark는 Spark 기반 배치 스캐너로, 데이터셋에서 잠재적 개인정보(PII)를 정규식으로 탐지해 결과 리포트와 오류 리포트를 생성합니다.

## MVP 요약
- 일회성 배치 실행
- 입력/출력 경로는 절대경로 또는 URI만 허용
- 지원 입력: `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`
- 무확장자 파일과 미지원 확장자 파일은 앞부분 매직바이트로 `parquet`, `orc`를 추가 판별하고, 일치하지 않으면 `Unsupported file format`으로 기록
- 탐지 방식: ruleset 기반 regex + 일부 타입의 내장 strict validator
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
PRIVYSPARK_DEBUG=true \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --sample-ratio 0.2 \
  --group-parallelism 8 \
  --file-parallelism 4
```

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
