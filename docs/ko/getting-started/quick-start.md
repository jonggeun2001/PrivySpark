# 빠른 시작

## 전제 조건
- Spark `3.5.3`
- Scala `2.12`
- JVM 바이트코드 타겟 `1.8`
- YARN cluster 실행 환경

PrivySpark는 클러스터 제공 Spark 런타임을 전제로 하고, 애플리케이션 의존성은 Shadow fat JAR에 포함합니다.

## 빌드

```bash
./gradlew clean shadowJar
```

생성 산출물은 `build/libs/*-all.jar`입니다.

## 테스트

```bash
./gradlew test
```

샘플 데이터셋을 다시 만들려면 아래 명령을 사용합니다.

```bash
./gradlew generateSampleDatasets
./gradlew packageSampleDatasets
```

## 기본 실행

```bash
PRIVYSPARK_DEBUG=info \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --output-format parquet \
  --ruleset default \
  --sample-ratio 0.2
```

`--path`, `--output`은 절대경로 또는 URI만 허용합니다.

## 병렬도와 샘플링 예시

```bash
PRIVYSPARK_DEBUG=debug \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --output-format parquet \
  --output-format csv \
  --ruleset default \
  --sample-ratio 0.2 \
  --file-sample-ratio 0.1 \
  --file-sample-min-files 10 \
  --pre-scan-parallelism 32 \
  --group-parallelism 16 \
  --file-parallelism 8 \
  --suppress prdctcd:driver_license_number \
  --ignore "_SUCCESS" \
  --ignore "backup/**"
```

`--file-sample-ratio`는 그룹 파일 수가 `--file-sample-min-files`보다 클 때만 적용됩니다. 실제 파일 샘플링이 적용된 그룹에서는 `--sample-ratio < 1.0`이 무시되고 warning 로그가 남습니다. 이유는 파일 샘플링 후 다시 row sampling을 적용하면 샘플 기준이 이중으로 바뀌어 결과 해석이 불명확해지기 때문입니다.

## ignore 패턴 예시

```bash
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ignore "_SUCCESS" \
  --ignore "*.crc" \
  --ignore "/backup/**" \
  --ignore-file scan.ignore
```

YARN cluster에서 client 로컬 ignore 파일을 쓰려면 먼저 배포해야 합니다.

```bash
PRIVYSPARK_SPARK_FILES=/abs/path/scan.ignore#scan.ignore \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ignore-file scan.ignore
```

`--ignore-file`은 UTF-8 텍스트 파일이며, 빈 줄과 `#` 주석을 무시합니다. HDFS나 object-store URI를 직접 넘기는 것도 가능합니다.

## suppression 예시

```bash
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --suppress prdctcd:driver_license_number \
  --suppression-file scan.suppressions
```

`--suppression-file`도 UTF-8 텍스트 파일이며, 각 줄은 `column:pii_type` 형식입니다. 빈 줄과 `#` 주석을 무시합니다. ruleset YAML에 `suppressions:`가 있으면 CLI suppression과 union으로 합쳐집니다.

## 커스텀 ruleset 배포

YARN cluster 실행에서 커스텀 ruleset을 사용할 때는 ruleset 파일도 함께 배포해야 합니다.

```bash
PRIVYSPARK_SPARK_FILES=/abs/path/my-rules.yaml#my-rules.yaml \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset my-rules.yaml
```

직접 `spark-submit`을 사용할 수도 있습니다.

```bash
spark-submit \
  --class io.github.jonggeun2001.privyspark.PrivySparkApp \
  --master yarn \
  --deploy-mode cluster \
  --files /abs/path/my-rules.yaml#my-rules.yaml \
  /abs/path/privyspark-<version>-all.jar \
  scan --path hdfs:///data/input --output hdfs:///data/output --ruleset my-rules.yaml
```

## 결과 확인
- 기본 최종 결과: `<output>/parquet/scan_results`
- 기본 최종 오류: `<output>/parquet/scan_errors`
- `--output-format csv` 추가 시: `<output>/csv/scan_results`, `<output>/csv/scan_errors`
- `--output-format excel` 추가 시: `<output>/excel/scan_results.xlsx`, `<output>/excel/scan_errors.xlsx`
- 실행 중 progress: `<output>/_progress/<run_id>`

`--output-format`은 반복 지정 가능하고 지원값은 `parquet`, `csv`, `excel`입니다. 기본값은 `parquet`입니다.

progress 경로는 관측용 임시 경로입니다. 최종 소비자는 항상 선택한 최종 리포트 포맷을 기준으로 봐야 합니다.
