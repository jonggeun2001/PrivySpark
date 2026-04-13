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
- 최종 출력: Parquet + CSV (`scan_results`, `scan_errors`)
- 실행 중에는 `<output>/_progress/<run_id>` 아래에 group/file 완료 단위 임시 JSONL 결과를 남기고, 탐지/오류가 없더라도 `meta/completions` marker를 기록합니다. 정상 종료 시 최종 리포트로 merge한 뒤 삭제합니다. startup race를 막기 위해 `_progress` 루트보다 먼저 `<output>/_progress-preparing.json` lock을 잡고, 준비가 끝나면 `_progress/active-run.json` heartbeat marker로 전환합니다. 다음 실행은 `FAILED` 또는 stale heartbeat로 판정된 `_progress`만 정리합니다. 최근 heartbeat의 `RUNNING` marker나 fresh preparing lock이 남아 있으면 충돌로 실패해 다른 실행의 progress를 지우지 않습니다. `active-run.json`이 깨져도 owner run은 `meta/run.json`을 근거로 다음 heartbeat에서 marker를 self-heal합니다.
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

샘플 입력 케이스 번들 재생성:
```bash
./gradlew generateSampleDatasets
```

샘플 입력 케이스 zip 패키징:
```bash
./gradlew packageSampleDatasets
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
  --file-sample-ratio 0.1 \
  --pre-scan-parallelism 6 \
  --group-parallelism 8 \
  --file-parallelism 4
```

`PRIVYSPARK_DEBUG` / `-Dprivyspark.debug`는 driver 로그 레벨 설정으로 동작합니다. 지원값은 `error`, `warn`, `info`, `debug`이며 `off`로 driver 로그를 끌 수 있습니다. 기본값은 `warn`이고, 하위호환으로 `true`는 `debug`, `false`는 `warn`으로 해석합니다.

driver 로그는 `[PrivySpark][LEVEL][ISO-8601 UTC timestamp] event key=value...` 형식으로 통일됩니다. field 값에 공백, 개행, `=` 같은 문자가 있으면 quote/escape해 구조를 유지합니다. `info` 레벨에서는 `scan_start`, `scan_plan_ready`, `scan_complete` 같은 상위 실행 lifecycle이 남고, `scan_start`의 병렬도 필드는 `configured_*` 이름으로 요청값 또는 `spark_conf_or_default` 상태를 기록합니다. `debug` 레벨에서는 `scanDirectoryStructure`의 파일 발견, pre-scan 실행, pre-scan 후처리, 초기 그룹화 단계에 대한 duration/progress 로그와 `_progress` 준비/쓰기/merge 로그까지 함께 남습니다. `--file-sample-ratio`가 적용된 group batch scan에서 `--sample-ratio < 1.0`이 함께 들어오면 row sampling은 무시되고 `group_scan_row_sampling_ignored` 경고가 driver 로그에 남습니다.

`--file-sample-ratio`는 group batch scan에서 파일을 균등 무작위로 추출합니다. 이 옵션을 추가한 이유는 작은 파일이 많은 입력에서 task/I/O를 직접 줄이기 위해서이기도 하지만, 더 중요한 배경은 특정 데이터가 한 파일에 몰릴 수 있다는 운영 우려입니다. 파일 크기 가중치 기반 샘플링은 큰 파일을 더 자주 선택하므로, 데이터 집중이 특정 파일에 몰린 경우 그 분포를 그대로 강화할 수 있습니다. 반대로 균등 무작위 파일 추출은 각 파일을 같은 확률로 보므로 파일 단위 concentration risk를 과도하게 편향시키지 않습니다.

중간 `_progress` 경로를 추가한 이유는 긴 스캔에서 최종 `scan_results`/`scan_errors`가 완성되기 전에도 이미 끝난 group/file 단위 결과를 바로 확인할 수 있게 하려는 것입니다. 탐지/오류가 없는 clean completion도 `meta/completions` marker를 남기는 이유는 운영자가 `아직 처리 중`과 `탐지 없이 완료`를 구분할 수 있어야 하기 때문입니다. 진행 중 경로를 별도로 둔 이유는 최종 리포트 소비자가 부분 결과를 완성본으로 오해하지 않게 하기 위해서이고, 종료 훅 대신 다음 실행 시작 시 cleanup을 택한 이유는 YARN 강제 종료나 `kill -9` 같은 상황에서 훅 신뢰도가 낮기 때문입니다. 추가로 `_progress-preparing.json`을 둔 이유는 `_progress` 디렉토리를 active marker보다 먼저 만들 수밖에 없는 준비 구간에서 concurrent startup이 서로의 fresh root를 지우지 않게 하기 위해서입니다. preparing lock은 짧은 setup window만 보호하고, 준비가 끝나면 heartbeat 기반 `active-run.json`으로 전환합니다. marker가 깨졌을 때는 owner run이 `meta/run.json`으로 self-heal하지만, `run.json`이 `FAILED`로 바뀐 뒤에는 늦게 끝난 sibling task가 marker를 `RUNNING`으로 되살리지 못하도록 막습니다.

## 샘플 데이터셋
- 재현 가능한 입력 케이스 번들은 [samples/input-cases/README.md](samples/input-cases/README.md)에 있습니다.
- `./gradlew generateSampleDatasets`로 `csv/json/jsonl/ndjson/parquet/orc/avro/xlsx/zip/jar`, extensionless magic-byte, text fallback, zero-byte skip, unsupported/error archive 케이스를 다시 생성할 수 있습니다.
- `./gradlew packageSampleDatasets`는 체크인된 `samples/input-cases` 번들을 그대로 `build/distributions/privyspark-sample-datasets.zip`으로 묶고, GitHub Release에서는 `privyspark-<tag>-sample-datasets.zip` 자산으로 함께 배포합니다.
- 케이스별 기대 결과/오류는 `samples/input-cases/scenario-manifest.tsv`에 정리됩니다.

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
- 결과물은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, `privyspark-<tag>-sample-datasets.zip` 형식입니다.
- `default-rules.yaml`은 배포 예시 ruleset 파일이며, YARN 제출 시 `--files /abs/path/default-rules.yaml#default-rules.yaml` 또는 `PRIVYSPARK_SPARK_FILES=/abs/path/default-rules.yaml#default-rules.yaml`로 함께 전달할 수 있습니다.
- `privyspark-<tag>-sample-datasets.zip`은 압축 해제 시 `input-cases/` 루트를 기준으로 샘플 ruleset, manifest, 입력 케이스 파일 트리를 제공합니다.
