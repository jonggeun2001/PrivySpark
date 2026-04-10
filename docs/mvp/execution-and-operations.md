# 실행과 운영

## 실행 모델
- 명령은 `privyspark scan` 단일 진입점입니다.
- 입력/출력 경로는 절대경로 또는 URI만 허용합니다.
- Spark on YARN cluster 실행을 기본 전제로 합니다.
- 빌드 산출물은 Shadow fat JAR(`*-all.jar`)입니다.

## CLI 인자
- `--path <ABS_PATH_OR_URI>`: 입력 경로
- `--output <ABS_PATH_OR_URI>`: 출력 경로
- `--ruleset <default|path>`: 규칙셋 경로 또는 `default`
- `--sample-ratio <(0.0, 1.0]>`: 샘플링 비율, 기본 `0.2`
- `--pre-scan-parallelism <INT>`: 파일 pre-scan 확장 병렬도, `> 0` and `<= driver CPU 수`
- `--group-parallelism <INT>`: 그룹 스캔 병렬도, `> 0`
- `--file-parallelism <INT>`: 파일 폴백 스캔 병렬도, `> 0`

## 병렬도
- CLI에서 병렬도 값을 주면 해당 값이 앱 로직에 직접 전달됩니다.
- CLI 값을 생략하면 `spark.privyspark.preScanParallelism`, `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism` 또는 앱 기본값(`4`, `4`, `3`)을 사용합니다.
- pre-scan 병렬도는 파일 단위 입력 확장과 포맷 판별 경로에 적용됩니다.
- pre-scan 병렬도는 driver CPU 수를 넘길 수 없고, 초과 값은 CLI와 Spark conf fallback 모두 거부됩니다.
- 그룹 병렬도는 `scanGroups` 경로에 적용됩니다.
- 파일 병렬도는 일반 `scanGroupByFile` fallback 경로에 적용됩니다.
- batch scan을 지원하지 않아 direct file scan으로 내려가는 `xlsx` 그룹은 현재 CLI `--file-parallelism`이 아니라 Spark conf 또는 기본값 경로를 사용합니다.

## 샘플링
- 샘플링은 비결정적 랜덤 방식입니다.
- `sampleRatio >= 1.0`이면 샘플링 없이 전체를 사용합니다.
- `match_ratio`, `confidence`의 분모는 샘플링된 행 수입니다.

## 빌드와 실행
빌드:
```bash
./gradlew clean shadowJar
```

테스트:
```bash
./gradlew test
```

제출 스크립트:
```bash
bin/privyspark-submit scan --path /abs/input --output /abs/output --ruleset default
```

직접 실행:
```bash
spark-submit \
  --class io.github.jonggeun2001.privyspark.PrivySparkApp \
  --master yarn \
  --deploy-mode cluster \
  --files /abs/path/config/rules/default.yaml#default-rules.yaml \
  /abs/path/privyspark-<version>-all.jar \
  scan --path hdfs:///data/input --output hdfs:///data/output
```

커스텀 ruleset을 YARN cluster에서 사용할 때는 ruleset 파일도 함께 배포해야 합니다.

제출 스크립트에서 기본 ruleset 대신 커스텀 ruleset 배포:
```bash
PRIVYSPARK_SPARK_FILES=/abs/path/my-rules.yaml#my-rules.yaml \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset my-rules.yaml
```

직접 실행에서 커스텀 ruleset 배포:
```bash
spark-submit \
  --class io.github.jonggeun2001.privyspark.PrivySparkApp \
  --master yarn \
  --deploy-mode cluster \
  --files /abs/path/my-rules.yaml#my-rules.yaml \
  /abs/path/privyspark-<version>-all.jar \
  scan --path hdfs:///data/input --output hdfs:///data/output --ruleset my-rules.yaml
```

## 운영 로그
- 기본 로그에는 스캔 요약과 fallback 원인이 출력됩니다.
- `PRIVYSPARK_DEBUG=true` 또는 `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG=true`, `-Dprivyspark.debug=true`가 설정되면 debug 이벤트가 추가됩니다.
- debug 로그에는 플랜 수립, 그룹/파일 스캔 진행, 리포트 저장 단계가 포함됩니다.

## 릴리즈
- GitHub Release는 `v*` 또는 bare semver 태그 푸시로 트리거됩니다.
- Release 자산 파일명은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`입니다.
- `default-rules.yaml`은 클러스터 제출 시 함께 배포할 수 있는 예시 기본 ruleset 파일입니다.
