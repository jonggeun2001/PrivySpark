# 빠른 시작

## 전제 조건

- Spark `3.5.3`
- Scala `2.12`
- JVM 바이트코드 타겟 `1.8`
- 빌드·테스트용 JDK 17과 `JAVA_HOME` 설정. Gradle 9.x 실행에는 JDK 17 이상이 필요하며, 릴리즈 워크플로우는 JDK 21을 사용합니다. 바이트코드 타겟과 빌드 JDK는 별개입니다. [Gradle 호환성](https://docs.gradle.org/current/userguide/compatibility.html#java_runtime)
- YARN cluster 실행 환경

PrivySpark는 클러스터 제공 Spark 런타임을 전제로 하고, 애플리케이션 의존성은 Shadow fat JAR에 포함합니다.

저장소의 명령 예시는 저장소 루트에서 실행합니다. 실제 제출 스크립트는 `bin/privyspark-submit`이며, Spark 런타임과 `spark-submit`은 별도로 준비되어 있어야 합니다.

## 릴리즈 JAR 사용

직접 빌드하는 대신 [GitHub Releases](https://github.com/jonggeun2001/PrivySpark/releases)에서 `privyspark-<tag>-all.jar`와 checksum을 받을 수 있습니다. 저장소 루트에서 `PRIVYSPARK_APP_JAR=/abs/path/privyspark-<tag>-all.jar`를 지정하면 아래 실행 예제에 해당 JAR를 사용합니다. 기본 ruleset 파일은 제출 스크립트가 `--files`로 배포하며 JAR의 내장 리소스로 읽는 방식이 아닙니다. 환경 변수와 네트워크가 차단된 클러스터의 제출 방법은 [실행과 운영](../operations/execution.md#제출-스크립트와-설정-파일)을 참고합니다.

## 빌드

```bash
./gradlew clean shadowJar
```

생성 산출물은 `build/libs/*-all.jar`입니다.

## 테스트

```bash
bash scripts/verify-worktree.sh
```

Scala/Spark 테스트와 브라우저 리뷰 로직 테스트를 함께 실행합니다. JavaScript 테스트에는 Node.js 18 이상이 필요하며 추가 npm 패키지는 사용하지 않습니다. Scala 테스트만 실행하려면 `./gradlew test`, JavaScript 테스트만 실행하려면 `node --test src/test/js/*.test.cjs`를 사용합니다. Node.js는 스캐너 실행에는 필요하지 않습니다.

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
  --excel-byte-array-max-override 300000000 \
  --suppress prdctcd:driver_license_number \
  --ignore "_SUCCESS" \
  --ignore "backup/**"
```

`--file-sample-ratio`는 그룹 파일 수가 `--file-sample-min-files`보다 클 때만 적용됩니다. 같은 그룹/파일 집합에서는 해시 기반 파일 subset이 반복 실행마다 유지됩니다. 실제 파일 샘플링이 적용된 그룹에서는 `--sample-ratio < 1.0`이 무시되고 warning 로그가 남습니다. 이유는 파일 샘플링 후 다시 row sampling을 적용하면 샘플 기준이 이중으로 바뀌어 결과 해석이 불명확해지기 때문입니다. review fingerprint는 실제 스캔된 sampled file scope만 대상으로 기록됩니다.

`--excel-max-rows-in-memory`는 과거 spark-excel scan reader 호환용으로만 받습니다. 실제 `xlsx` scan은 executor task의 StAX 스트리머를 사용하므로, 이 값을 명시하면 warning 로그를 남기고 scan에는 사용하지 않습니다.

`--excel-byte-array-max-override`를 생략하면 `spark.privyspark.excel.byteArrayMaxOverride` Spark conf를 사용하고, 이 conf도 없으면 기본값 `300000000`이 적용됩니다.

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
PRIVYSPARK_SPARK_FILES=/abs/path/scan.ignore#scan.ignore,config/rules/default.yaml#default-rules.yaml \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ignore-file scan.ignore
```

`--ignore-file`은 UTF-8 텍스트 파일이며, 빈 줄과 `#` 주석을 무시합니다. HDFS나 object-store URI를 직접 넘기는 것도 가능합니다.

## suppression 예시

```bash
PRIVYSPARK_SPARK_FILES=/abs/path/scan.suppressions#scan.suppressions,config/rules/default.yaml#default-rules.yaml \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --suppress prdctcd:driver_license_number \
  --suppression-file scan.suppressions
```

`--suppression-file`도 UTF-8 텍스트 파일이며, 각 줄은 `column:pii_type` 형식입니다. 빈 줄과 `#` 주석을 무시합니다. ruleset YAML에 `suppressions:`가 있으면 CLI suppression과 union으로 합쳐집니다. ruleset YAML에서는 같은 `pii_type`에 여러 컬럼을 묶기 위해 `columns: [col1, col2]`를 사용할 수 있습니다.

`PRIVYSPARK_SPARK_FILES`는 기본 배포 목록을 대체하므로 위 ignore/suppression 예제는 `default-rules.yaml`도 함께 배포합니다. 여러 파일은 쉼표로 구분합니다.

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
  "/abs/path/privyspark-<version>-all.jar" \
  scan --path hdfs:///data/input --output hdfs:///data/output --ruleset my-rules.yaml
```

## 결과 확인

- 기본 최종 결과: `<output>/parquet/scan_results`
- 기본 최종 오류: `<output>/parquet/scan_errors`
- `--output-format csv` 지정 시: `<output>/csv/scan_results`, `<output>/csv/scan_errors`
- `--output-format excel` 지정 시: `<output>/excel/scan_results.xlsx`, `<output>/excel/scan_errors.xlsx`
- 실행 중 progress: `<output>/_progress/<run_id>`

`--output-format`은 반복 지정 가능하고 지원값은 `parquet`, `csv`, `excel`입니다. 기본값은 `parquet`입니다.

명시적으로 포맷을 지정하면 그 목록만 생성합니다. 예를 들어 `--output-format csv`만 지정하면 Parquet는 생성하지 않습니다. 둘 다 필요하면 `--output-format parquet --output-format csv`를 사용합니다.

progress 경로는 관측용 임시 경로입니다. 최종 소비자는 항상 선택한 최종 리포트 포맷을 기준으로 봐야 합니다.

담당자 검토까지 이어가려면 [오프라인 리뷰 collector](../reference/offline-review-collector.md)를 참고합니다.
