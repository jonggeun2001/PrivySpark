# PrivySpark

PrivySpark는 Spark 기반 배치 스캐너입니다. 데이터셋에서 잠재적 개인정보(PII)를 ruleset 기반 정규식과 타입별 strict validator로 탐지하고, 최종 결과를 `scan_results`와 `scan_errors` 리포트로 생성합니다.

한국어 문서가 기준 문서입니다. 영어 문서는 공개 사용자용 대응본으로 함께 제공합니다.

- 한국어 문서: [docs/ko/README.md](docs/ko/README.md)
- English documentation: [docs/en/README.md](docs/en/README.md)

## 핵심 기능
- 입력 경로는 절대경로 또는 URI만 허용합니다.
- 실제 파일명에 공백과 Spark glob 특수문자(`*`, `?`, `[`, `]`, `{`, `}`)가 포함되어도 schema detection과 scan reader에는 literal path로 전달합니다.
- 지원 입력은 `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`와 archive 계열 `zip`, `jar`, `tar`, `tar.gz/tgz`, `tar.bz2/tbz2`, `tar.xz/txz`, `tar.zst/tzst`, `7z`, `rar`입니다.
- `gzip`, `bzip2`로 감싼 direct text-style data file(`*.csv.gz`, `*.json.bz2` 등)은 원본 경로를 그대로 Spark/Hadoop reader에 전달합니다.
- CSV 계열 입력은 콤마뿐 아니라 탭, 세미콜론, 파이프, 콜론, ASCII 정보 구분자, 일부 다중문자 구분자(`||`, `|~|` 등)를 자동 감지해 컬럼 단위로 스캔합니다.
- 무확장자 파일과 미지원 확장자 파일은 `parquet`/`orc` 매직바이트를 우선 판별하고, 안정적인 구분자와 헤더/데이터 구조가 확인되는 UTF-8 텍스트만 내부 `csv` 포맷으로 승격합니다. 그 외 바이너리처럼 보이지 않는 UTF-8 또는 EUC-KR 텍스트는 내부 `text` 포맷으로 정규화해 스캔합니다.
- 0바이트 파일과 0바이트 archive entry는 포맷 판별과 오류 리포트 대상에서 제외하고 건너뜁니다.
- password-protected archive, multi-volume RAR, RAR5 archive는 `scan_errors`에 명시적으로 기록합니다.
- row sampling(`--sample-ratio`)과 file sampling(`--file-sample-ratio`)을 분리해 제어할 수 있고, file sampling은 같은 그룹/파일 집합에서 안정적인 해시 기반 subset을 선택합니다. `--file-sample-min-files`로 파일 샘플링을 적용할 최소 그룹 크기(기본 `10`)를 조정할 수 있습니다.
- `--ignore`, `--ignore-file`로 gitignore 스타일 glob 패턴을 지정해 파일/아카이브 엔트리를 pre-scan 전에 제외할 수 있습니다.
- ruleset `suppressions:` 또는 `--suppress`, `--suppression-file`로 특정 `(column, pii_type)` 조합만 결과에서 제외할 수 있습니다.
- `--review-state-root`로 누적 오프라인 리뷰 state를 적용하고 기본 `<output>/review/review.html`과 `<output>/review/review.xlsm`을 생성할 수 있습니다. `--review-html-dir`을 지정하면 해당 디렉토리 아래 `review.html`/`review.xlsm`로 출력 위치를 바꿀 수 있습니다. 회수한 response JSON은 다음 `scan --review-state-root` 시작 시 자동 수집되어 누적 allowlist/action plan에 반영됩니다.
- 실행 중에는 `<output>/_progress/<run_id>` 아래에 group/file 완료 단위 JSONL progress와 현재 실행 중인 작업의 `in-flight` marker를 남기고, 정상 종료 시 선택된 최종 출력 포맷으로 merge한 뒤 정리합니다. Spark application이 `FAILED`로 끝나는 미복구 group/file 실패에서는 당시 marker를 보존합니다.
- `scan_results`에는 집계 지표와 함께 `sample_raw_value`, `sample_matched_fragment` 1건을 저장합니다. `sample_raw_value`는 매치 주변 앞뒤 최대 50자 문맥만 남깁니다.
- Hive Metastore JDBC 옵션을 지정하면 table `LOCATION`과 입력 파일 경로를 longest-prefix로 매칭해 `scan_results.hive_table_fqn`에 `db.table`을 기록합니다. 기본 driver class는 `org.mariadb.jdbc.Driver`이며 `--hive-metastore-jdbc-driver-class` 또는 `spark.privyspark.hiveMetastore.jdbcDriverClass`로 변경할 수 있습니다.

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
  --output-format parquet \
  --output-format csv \
  --ruleset default \
  --sample-ratio 0.2 \
  --file-sample-ratio 0.1 \
  --file-sample-min-files 10 \
  --pre-scan-parallelism 6 \
  --group-parallelism 8 \
  --file-parallelism 4 \
  --excel-byte-array-max-override 300000000 \
  --suppress prdctcd:driver_license_number \
  --ignore "_SUCCESS" \
  --ignore "backup/**" \
  --hive-metastore-jdbc-url "jdbc:mariadb://hms-db.internal:3306/metastore" \
  --hive-metastore-user privyspark_ro \
  --hive-metastore-password-file hdfs:///etc/secrets/metastore.pw \
  --hive-metastore-jdbc-driver-class org.mariadb.jdbc.Driver
```

`--file-sample-ratio`는 그룹 파일 수가 `--file-sample-min-files`보다 클 때만 적용됩니다. 같은 그룹/파일 집합에서는 해시 기반 선택 결과가 반복 실행마다 유지되며, 실제 파일 샘플링이 적용된 그룹에서는 `--sample-ratio < 1.0` row sampling을 무시하고 warning 로그를 남깁니다. 이때 review fingerprint scope는 실제 선택되어 스캔된 파일 subset을 기준으로 기록됩니다.

`xlsx` 실제 scan은 Spark executor task 안에서 StAX 기반 sheet row 스트리머로 처리합니다. `--excel-max-rows-in-memory`는 과거 spark-excel scan reader 호환용으로만 받으며, 명시하면 warning 로그를 남기고 실제 scan에는 사용하지 않습니다.

`--excel-byte-array-max-override`는 Apache POI `IOUtils.setByteArrayMaxOverride` 값입니다. 생략하면 `spark.privyspark.excel.byteArrayMaxOverride` Spark conf를 사용하고, 이 conf도 없으면 기본값 `300000000`을 적용합니다.

`xlsx` pre-scan은 드라이버에서 workbook metadata와 header row XML만 경량 파싱해 visible sheet 목록과 schema signature를 만들고, sheet body row/cell 내용은 executor task의 StAX 스트리머에서 처리합니다. 빈 visible sheet는 header 기반 schema detection 이후 결과/오류 없이 건너뜁니다.

오탐 제외를 파일로 관리하려면 `--suppression-file`에 UTF-8 텍스트 파일을 넘길 수 있습니다. 파일 형식은 줄 단위 `column:pii_type`이며 빈 줄과 `#` 주석을 무시합니다. YARN cluster에서 client 로컬 suppression 파일을 쓰려면 `PRIVYSPARK_SPARK_FILES` 또는 `--files`로 먼저 배포해야 합니다.

자세한 실행 절차와 옵션은 [docs/ko/getting-started/quick-start.md](docs/ko/getting-started/quick-start.md), [docs/ko/operations/execution.md](docs/ko/operations/execution.md)에 정리돼 있습니다.

서버 없이 담당자 검토를 받는 흐름은 스캔에 `--review-state-root`를 추가해 컬럼 헤더 정렬이 가능한 `review.html`과 Excel 담당자용 `review.xlsm`을 만들고, 회수한 JSON을 `<review-state-root>/inbox`에 둔 뒤 다음 스캔을 실행하는 방식입니다. 다음 `scan --review-state-root`는 스캔 본 작업 전에 자동으로 `inbox/*.json`을 수집하고, invalid response가 하나라도 있거나 collect lock이 이미 있으면 스캔을 시작하지 않고 실패합니다. HTML/XLSM을 scan output 밖에 배치해야 하면 scan 실행에 `--review-html-dir /abs/reviews`를 추가합니다. 같은 finding이 정탐으로 계속 검출되면 다음 리뷰 파일의 `기존 조치 상태` 컬럼에 이전 조치 계획과 예정일이 표시됩니다. `review.xlsm`은 매크로를 허용한 뒤 `review.json 생성` 버튼으로 JSON 파일을 내려받아 `inbox`에 넣습니다.

자세한 구조는 [docs/ko/reference/offline-review-collector.md](docs/ko/reference/offline-review-collector.md)에 있습니다.

## 코드베이스 사용법

### 가장 자주 쓰는 개발 명령

```bash
# 저장소 기준 표준 검증
bash scripts/verify-worktree.sh

# 전체 테스트
./gradlew test

# fat JAR 재생성
./gradlew clean shadowJar
```

- 로컬 개발과 CI 전 확인은 `bash scripts/verify-worktree.sh`를 기준으로 맞춥니다.
- 스캔 실행 시 `--path`, `--output`은 절대경로 또는 URI만 허용합니다.
- `--output-format`은 반복 지정 가능하고, 기본값은 `parquet`입니다. 지원값은 `parquet`, `csv`, `excel`입니다.
- `--suppress`는 반복 지정 가능하며 `column:pii_type` 형식입니다. `--suppression-file`은 같은 형식을 줄 단위로 읽고, ruleset `suppressions:`와 union으로 합쳐집니다.
- Hive table 매핑은 `--hive-metastore-jdbc-url`, `--hive-metastore-user`, `--hive-metastore-password-file` 세 옵션을 모두 지정한 경우에만 활성화됩니다. 기본 JDBC driver class는 `org.mariadb.jdbc.Driver`이고, MySQL 등 다른 driver를 쓰면 `--hive-metastore-jdbc-driver-class <CLASS>` 또는 Spark conf `spark.privyspark.hiveMetastore.jdbcDriverClass`로 지정합니다. CLI 값이 Spark conf보다 우선합니다. JDBC driver JAR는 fat JAR에 포함하지 않으므로 cluster classpath에 두거나 `PRIVYSPARK_JARS=/path/to/driver.jar`로 함께 제출합니다. driver가 없거나 JDBC 접속/query가 실패하면 warning 후 `hive_table_fqn`은 빈 문자열로 남습니다.
- Shadow fat JAR는 `commons-compress`를 앱 내부 패키지로 relocate합니다. 따라서 Spark/Hadoop 런타임의 구버전 `commons-compress`가 먼저 잡혀도 `review.xlsm` 생성 같은 POI 기반 workbook write 경로가 런타임 `NoSuchMethodError`에 영향을 받지 않습니다.
- 기본 ruleset은 [config/rules/default.yaml](config/rules/default.yaml)에 있습니다.

### 결과를 확인하는 위치
- 기본 최종 리포트는 `<output>/parquet/scan_results`, `<output>/parquet/scan_errors`에 저장됩니다.
- `--output-format csv`를 지정하면 `<output>/csv/scan_results`, `<output>/csv/scan_errors`가 추가로 생성됩니다.
- `--output-format excel`을 지정하면 `<output>/excel/scan_results.xlsx`, `<output>/excel/scan_errors.xlsx`가 추가로 생성됩니다.
- 실행 중 progress는 `<output>/_progress/<run_id>` 아래 JSONL로 쌓이고, 실행 중인 group/file/allowlist 작업은 `in-flight/*.json` marker로 관찰할 수 있습니다.
- `in-flight` marker 파일명은 파일명에 안전한 UTF-8 문자/숫자를 보존하고, 경로 구분자와 그 외 문자는 `_`로 치환합니다.
- Spark application이 `FAILED`로 종료된 경우 미복구 group/file 실패 marker는 삭제하지 않아 마지막 진행 중 작업을 확인할 수 있습니다.
- `_progress`는 진행 중 임시 경로이고, 최종 출력 계약은 선택된 `parquet`, `csv`, `excel` 산출물입니다.
- 샘플 값 정책과 리포트 컬럼 의미는 [docs/ko/reference/reports-and-errors.md](docs/ko/reference/reports-and-errors.md)에서 확인합니다.

### 어디를 수정해야 하는지 빠르게 찾기
- `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`
  - 입력 확장, 그룹화, 스캔 오케스트레이션, progress/최종 리포트 저장
- `src/main/scala/io/github/jonggeun2001/privyspark/cli/Cli.scala`
  - CLI 파싱과 실행 옵션 정의
- `src/main/scala/io/github/jonggeun2001/privyspark/detect/DetectionAggregator.scala`
  - 규칙별 집계, sample 값 추출, fallback regroup 전략
- `src/main/scala/io/github/jonggeun2001/privyspark/format/FormatDetector.scala`
  - 지원 포맷 판별
- `src/main/scala/io/github/jonggeun2001/privyspark/config/RulesetLoader.scala`
  - 기본/외부 ruleset 및 suppression 로딩과 검증
- `src/main/scala/io/github/jonggeun2001/privyspark/model/Models.scala`
  - ruleset, suppression, 결과, 오류 모델
- `src/test/scala/io/github/jonggeun2001/privyspark`
  - 기능별 ScalaTest 스펙

### 수정 흐름 추천
1. 현재 상태를 `./gradlew test` 또는 `bash scripts/verify-worktree.sh`로 먼저 확인합니다.
2. ruleset 변경이면 [config/rules/default.yaml](config/rules/default.yaml)과 관련 문서를 함께 수정합니다.
3. 입력 포맷 처리 변경이면 `format/FormatDetector.scala`, `scan/DirectoryScanner.scala`, `PrivySparkApp.scala`, 입력 포맷 문서를 같이 봅니다.
4. 집계나 출력 스키마 변경이면 `detect/DetectionAggregator.scala`, `model/Models.scala`, `report/ReportWriter.scala`, 관련 테스트를 같이 봅니다.
5. 변경 후 테스트를 다시 돌리고, 필요하면 `bin/privyspark-submit`으로 실제 스캔을 재현합니다.

## 문서 구조
- 시작하기: [docs/ko/getting-started/quick-start.md](docs/ko/getting-started/quick-start.md), [docs/en/getting-started/quick-start.md](docs/en/getting-started/quick-start.md)
- 제품/기능 개요: [docs/ko/reference/overview.md](docs/ko/reference/overview.md), [docs/en/reference/overview.md](docs/en/reference/overview.md)
- 입력 포맷과 정규화: [docs/ko/reference/input-formats.md](docs/ko/reference/input-formats.md), [docs/en/reference/input-formats.md](docs/en/reference/input-formats.md)
- ruleset과 탐지 모델: [docs/ko/reference/rules-and-detection.md](docs/ko/reference/rules-and-detection.md), [docs/en/reference/rules-and-detection.md](docs/en/reference/rules-and-detection.md)
- 출력과 오류 리포트: [docs/ko/reference/reports-and-errors.md](docs/ko/reference/reports-and-errors.md), [docs/en/reference/reports-and-errors.md](docs/en/reference/reports-and-errors.md)
- 오프라인 리뷰 collector: [docs/ko/reference/offline-review-collector.md](docs/ko/reference/offline-review-collector.md)
- 아키텍처: [docs/ko/architecture/overview.md](docs/ko/architecture/overview.md), [docs/en/architecture/overview.md](docs/en/architecture/overview.md)
- 운영과 릴리즈: [docs/ko/operations/execution.md](docs/ko/operations/execution.md), [docs/en/operations/execution.md](docs/en/operations/execution.md)
- 성능 가이드: [docs/ko/operations/performance.md](docs/ko/operations/performance.md), [docs/en/operations/performance.md](docs/en/operations/performance.md)

## 샘플
- 재현 가능한 입력 케이스 번들은 [samples/input-cases/README.md](samples/input-cases/README.md)에 있습니다.
- 오프라인 리뷰 응답 HTML 예시는 [samples/offline-review/review-response-example.html](samples/offline-review/review-response-example.html)에 있습니다.
- 회수한 오프라인 리뷰 응답 JSON을 파일 선택, 드래그앤드롭, 원문 붙여넣기로 확인하는 운영자 HTML은 [samples/offline-review/review-response-viewer.html](samples/offline-review/review-response-viewer.html)에 있습니다.
- `./gradlew generateSampleDatasets`는 현재 입력 처리 경로를 재현하는 샘플 케이스를 다시 생성합니다.
- `./gradlew packageSampleDatasets`는 샘플 번들을 `build/distributions/privyspark-sample-datasets.zip`으로 패키징하고, 릴리즈 자산에서는 `privyspark-<tag>-sample-datasets.zip`으로 배포합니다.

## 소스 구조
- `src/main/scala/io/github/jonggeun2001/privyspark/PrivySparkApp.scala`: 입력 확장, 그룹화, 스캔 오케스트레이션, progress/최종 리포트 저장
- `src/main/scala/io/github/jonggeun2001/privyspark/cli/`: CLI 파싱과 경로 검증
- `src/main/scala/io/github/jonggeun2001/privyspark/scan/`: 입력 확장, pre-scan, 그룹 스캔, 캐시
- `src/main/scala/io/github/jonggeun2001/privyspark/format/`: 포맷 판별, CSV 추론, workbook 헬퍼
- `src/main/scala/io/github/jonggeun2001/privyspark/hive/`: Hive Metastore JDBC table location lookup
- `src/main/scala/io/github/jonggeun2001/privyspark/detect/`: 규칙 집계와 strict validator
- `src/main/scala/io/github/jonggeun2001/privyspark/report/`: 출력 포맷, JSON codec, 리포트 쓰기
- `src/main/scala/io/github/jonggeun2001/privyspark/review/`: review apply, offline review HTML/XLSM, collector, allowlist 처리
- `src/main/scala/io/github/jonggeun2001/privyspark/fsio/`: staging 경로 관리와 재시도 I/O
- `src/main/scala/io/github/jonggeun2001/privyspark/util/`: driver 로그, 병렬도, 식별자 유틸리티
- `src/main/scala/io/github/jonggeun2001/privyspark/config/RulesetLoader.scala`: 기본/외부 ruleset 로딩과 검증
- `src/main/scala/io/github/jonggeun2001/privyspark/model/Models.scala`: ruleset, 결과, 오류 모델

## 릴리즈
- 태그 `v*` 또는 bare semver(`0.1.3`) 푸시 시 GitHub Actions가 Shadow fat JAR를 빌드해 Release 자산으로 업로드합니다.
- 결과물은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, `privyspark-<tag>-sample-datasets.zip`, `privyspark-<tag>-review-response-example.html`, `privyspark-<tag>-review-response-viewer.html` 형식입니다.
- `default-rules.yaml`은 YARN 제출 시 함께 배포할 수 있는 예시 ruleset 파일입니다.
