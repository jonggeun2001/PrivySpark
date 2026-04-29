# 실행과 운영

## 실행 모델
- 공개 명령은 `privyspark scan`, `privyspark review apply`, `privyspark review collect`입니다.
- 입력/출력 경로는 절대경로 또는 URI만 허용합니다.
- 입력 파일명에 공백과 Spark glob 특수문자(`*`, `?`, `[`, `]`, `{`, `}`)가 포함되어도 실제 파일명으로 처리합니다. glob 문법은 `--ignore`, `--ignore-file` 패턴에만 적용됩니다.
- Spark on YARN cluster 실행을 기본 전제로 합니다.
- 빌드 산출물은 Shadow fat JAR(`*-all.jar`)입니다.

## `scan` CLI 인자
- `--path <ABS_PATH_OR_URI>`: 입력 경로
- `--output <ABS_PATH_OR_URI>`: 출력 경로
- `--output-format <parquet|csv|excel>`: 반복 지정 가능한 최종 출력 포맷, 기본 `parquet`
- `--ruleset <default|path>`: 규칙셋 경로 또는 `default`
- `--sample-ratio <(0.0, 1.0]>`: row sampling 비율, 기본 `0.2`
- `--file-sample-ratio <(0.0, 1.0]>`: batch group scan 파일 샘플링 비율, 기본 미설정
- `--file-sample-min-files <INT>`: file sampling을 적용할 최소 그룹 파일 수, 기본 `10`, `>= 1`
- `--pre-scan-parallelism <INT>`: 디렉터리 discovery, 파일 pre-scan 확장, schema split 병렬도, `> 0`
- `--group-parallelism <INT>`: 그룹 스캔 병렬도, `> 0`
- `--file-parallelism <INT>`: 파일 폴백 스캔 병렬도, `> 0`
- `--excel-max-rows-in-memory <INT>`: 과거 spark-excel scan reader 호환용 옵션, `> 0`; 명시하면 warning을 남기며 현재 `xlsx` scan에는 사용하지 않음
- `--excel-byte-array-max-override <INT>`: Apache POI byte array allocation 상한 override, 기본 `300000000`, `> 0`
- `--ignore <PATTERN>`: 반복 지정 가능한 gitignore 스타일 glob ignore 패턴
- `--ignore-file <PATH>`: 줄 단위 ignore 패턴 파일 경로, `#` 주석과 빈 줄 무시
- `--allowlist <ABS_PATH_OR_URI>`: false positive suppression allowlist JSONL 경로
- `--review-state-root <ABS_PATH_OR_URI>`: 누적 오프라인 리뷰 state root. `<review-state-root>/current/allowlist.jsonl`을 적용하고 기본 `<output>/review/review.html`을 생성
- `--review-html-dir <ABS_PATH_OR_URI>`: 오프라인 리뷰 HTML 출력 디렉토리. 미지정 시 `<output>/review`, 파일명은 `review.html` 고정
- `--review-sample-mode <raw|masked|none>`: `review.html` 검출 샘플 표시 방식, 기본 `masked`
- `--suppress <column:pii_type>`: 반복 지정 가능한 오탐 제외 규칙
- `--suppression-file <PATH>`: 줄 단위 suppression 파일 경로, `#` 주석과 빈 줄 무시
- `--hive-metastore-jdbc-url <JDBC_URL>`: Hive Metastore JDBC URL, 예: `jdbc:mariadb://hms-db.internal:3306/metastore`
- `--hive-metastore-user <USER>`: Metastore read-only 계정
- `--hive-metastore-password-file <ABS_PATH_OR_URI>`: password 첫 줄을 읽을 파일. `hdfs://`, `s3a://`, `file://`, 절대경로를 지원
- `--hive-metastore-jdbc-driver-class <CLASS>`: Hive Metastore JDBC driver class. CLI 값을 생략하면 `spark.privyspark.hiveMetastore.jdbcDriverClass` Spark conf를 사용하고, 이 conf도 없으면 기본값 `org.mariadb.jdbc.Driver`를 적용

## `review apply` CLI 인자
- `--scan-results <ABS_PATH_OR_URI>`: 담당자가 편집한 `scan_results` 입력 경로. `csv`, `parquet`, `xlsx(scan_results sheet)`를 지원합니다.
- `--input-root <ABS_PATH_OR_URI>`: 원본 스캔 대상 루트 경로
- `--allowlist <ABS_PATH_OR_URI>`: 생성 또는 갱신할 allowlist JSONL 경로
- `--reviewer <STRING>`: 검토자 식별자
- `--dry-run`: 실제 파일 기록 없이 반영 예정 엔트리 수만 계산

## `review collect` CLI 인자
- `--scan-results <ABS_PATH_OR_URI>`: 현재 스캔의 `scan_results` 경로. `csv`, `parquet`, `xlsx(scan_results sheet)`를 지원합니다.
- `--review-state-root <ABS_PATH_OR_URI>`: response JSON을 읽고 누적 state를 갱신할 root 경로

`review collect`는 `<review-state-root>/inbox/*.json`을 읽어 `<review-state-root>/current` 아래의 `allowlist.jsonl`, `action_plan.jsonl`, `finding_status.jsonl`, `response_ledger.jsonl`을 갱신합니다. 다음 스캔은 같은 `--review-state-root`를 지정해 누적 오탐 allowlist를 반영합니다.

## Ignore 패턴
- `/`가 없는 패턴은 basename 기준으로 매칭합니다. 예: `_SUCCESS`, `*.crc`
- `/`가 있는 패턴은 입력 루트 기준 상대 경로로 매칭합니다. 예: `backup/**`, `logs/2025/*.gz`
- 선행 `/`는 입력 루트 anchor로 해석합니다. 예: `/backup/**`, `/logs/`
- `/`로 끝나는 패턴은 디렉터리 매칭으로 간주하고 하위 전체를 제외합니다. 예: `logs/`
- archive entry도 `<archive>!<entry>` 논리 식별자에서 entry 상대 경로 기준으로 같은 ignore 규칙을 적용합니다.
- v1 범위에서는 `!pattern` negate 문법을 지원하지 않습니다.
- `--ignore-file`은 Hadoop `FileSystem`으로 읽습니다. YARN cluster에서 client 로컬 파일을 쓰려면 `--files` 또는 `PRIVYSPARK_SPARK_FILES`로 먼저 배포한 뒤 alias 경로를 `--ignore-file`에 넘겨야 합니다.

ignore 필터를 pre-scan 전에 적용하는 이유는 `_SUCCESS`, `.crc`, 로그, 백업 파일처럼 스캔 가치가 낮은 입력 때문에 불필요한 I/O, 오류 리포트, 결과 노이즈가 늘어나는 것을 막기 위해서입니다.

allowlist는 ignore와 역할이 다릅니다. ignore는 pre-scan 전에 파일 자체를 제외하고, allowlist는 탐지 이후 `(dataset_path, file_identifier, column_name, pii_type)` 단위 false positive만 suppress합니다.

## Suppression
- suppression은 특정 `(column, pii_type)` 결과만 제외합니다. 컬럼명은 대소문자를 무시하고 exact match 합니다.
- `--suppress`는 `column:pii_type` 형식만 허용합니다.
- `--suppression-file`은 Hadoop `FileSystem`으로 읽습니다. YARN cluster에서 client 로컬 파일을 쓰려면 `--files` 또는 `PRIVYSPARK_SPARK_FILES`로 먼저 배포한 뒤 alias 경로를 `--suppression-file`에 넘겨야 합니다.
- CLI suppression은 ruleset YAML의 `suppressions:`와 union으로 합쳐집니다.

## Hive table lookup
- Hive table lookup은 `--hive-metastore-jdbc-url`, `--hive-metastore-user`, `--hive-metastore-password-file` 세 옵션을 모두 지정한 경우에만 활성화됩니다. 셋 중 1~2개만 지정하면 CLI 오류로 종료하고, 모두 생략하면 `hive_lookup_inactive` 로그 후 `hive_table_fqn`은 모두 `""`입니다.
- 활성화되면 실행 초기에 driver가 설정된 JDBC driver class로 Hive Metastore `DBS`/`TBLS`/`SDS`를 1회 조회하고 table-level `LOCATION` prefix 인덱스를 broadcast 합니다. 결과 row의 물리 입력 경로가 해당 prefix 하위이면 `scan_results.hive_table_fqn`에 `db.table`을 기록합니다.
- password 파일은 Hadoop `FileSystem`으로 읽습니다. `hdfs://` 같은 공유 URI를 쓰면 YARN cluster에서 별도 `--files` 배포가 필요 없습니다. client 로컬 파일을 쓰려면 다른 로컬 설정 파일과 마찬가지로 `--files` 또는 `PRIVYSPARK_SPARK_FILES`로 배포한 alias를 지정해야 합니다.
- JDBC driver JAR는 Shadow JAR에 포함하지 않습니다. 기본 driver class는 `org.mariadb.jdbc.Driver`이고, 다른 driver를 쓰면 `--hive-metastore-jdbc-driver-class` 또는 `spark.privyspark.hiveMetastore.jdbcDriverClass` Spark conf로 class name을 지정합니다. CLI 값이 Spark conf보다 우선합니다. Hive table lookup을 쓰려면 cluster 공통 classpath에 driver를 설치하거나, 제출 시 `PRIVYSPARK_JARS=/path/to/driver.jar`처럼 driver JAR를 Spark `--jars`로 함께 전달합니다. Maven package resolution을 허용하는 환경에서는 `PRIVYSPARK_PACKAGES=org.mariadb.jdbc:mariadb-java-client:3.4.1`도 사용할 수 있습니다.
- URL에 timeout이 없으면 기본 `connectTimeout=5000`, `socketTimeout=30000`을 적용합니다.
- JDBC 접속, password 파일 읽기, metastore query가 실패하면 `hive_lookup_disabled` warning을 남기고 빈 매핑으로 계속 진행합니다. 정상 인덱스 준비 시 `hive_lookup_ready size=<N>` info 로그가 남습니다.
- archive entry와 Excel sheet는 `<archive>!<entry>`, `<workbook>#<sheet>`에서 host archive/workbook path만 lookup 합니다.
- partition별 `LOCATION` override는 현재 지원하지 않습니다. table-level `LOCATION`만 사용합니다.

## 병렬도
- CLI 값을 주면 해당 값이 앱 로직에 직접 전달됩니다.
- CLI 값을 생략하면 `spark.privyspark.preScanParallelism`, `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism` 또는 앱 기본값(`4`, `4`, `3`)을 사용합니다.
- pre-scan 병렬도는 디렉터리 discovery, 파일 단위 입력 확장, 포맷 판별, 그룹별 schema split 경로에 적용됩니다.
- 디렉터리 discovery 단계에서는 BFS 레벨의 디렉터리 수와 safety ceiling `64` 기준으로 풀 크기가 제한되고, discovery 이후 pre-scan 병렬도는 기존처럼 파일 수와 safety ceiling `64` 기준으로 축소됩니다.
- 그룹 병렬도와 파일 병렬도는 driver가 동시에 제출하는 작업 수를 제어합니다.

여기서 중요한 점은 앱 레벨 병렬도가 곧 executor 수를 직접 보장하는 것은 아니라는 점입니다. 실제 executor 분산은 입력 파티션 수, Spark scheduler, dynamic allocation backlog에 함께 영향을 받습니다.

## Excel reader 설정
- `xlsx` pre-scan은 드라이버에서 workbook metadata와 header row XML만 경량 파싱해 visible sheet 목록과 schema signature를 만들고, sheet body row/cell 내용은 Spark executor task의 StAX 스트리머에서 처리합니다.
- `--excel-max-rows-in-memory`는 이전 spark-excel scan reader와의 CLI 호환을 위해 유지합니다. 값을 지정하면 `excel_max_rows_in_memory_unused` warning 로그를 남기고 scan 동작에는 사용하지 않습니다.
- `spark.privyspark.excel.maxRowsInMemory` Spark conf도 현재 executor-side `xlsx` scan에는 영향을 주지 않습니다.
- `--excel-byte-array-max-override`를 지정하면 Apache POI `IOUtils.setByteArrayMaxOverride` 값을 적용합니다. 이 설정은 POI 기반 Excel report writing 등 POI 사용 경로를 위한 호환 설정입니다.
- CLI 값을 생략하면 `spark.privyspark.excel.byteArrayMaxOverride` Spark conf를 사용하고, 이 conf도 없으면 기본값 `300000000`을 적용합니다.
- executor-side `xlsx` 스트리머는 한 workbook sheet를 하나의 Spark task에서 읽습니다. 단일 시트 자체를 row 단위로 split해서 여러 executor가 나눠 읽게 만들지는 않으며, cache/persist도 추가하지 않아 여러 action에서는 workbook zip을 다시 읽습니다.
- workbook ZIP 엔트리 순회는 Spark/Hadoop 런타임에 번들된 구버전 `commons-compress`와 호환되는 API를 사용합니다. 따라서 `xlsx` scan의 `NoSuchMethodError` 회피를 위해 cluster 공통 classpath를 별도로 덮어쓸 필요가 없습니다.

## 샘플링
- `--sample-ratio`는 비결정적 row sampling입니다.
- `sampleRatio >= 1.0`이면 row sampling 없이 전체 행을 사용합니다.
- `--file-sample-ratio`는 batch scan 경로와 file fallback scan 경로에서 그룹 내부 파일을 균등 무작위로 추출합니다.
- file sampling은 그룹 파일 수가 `--file-sample-min-files`보다 클 때만 적용합니다. 임계값 이하 그룹은 전체 파일을 그대로 스캔합니다.
- 샘플 파일 수는 `ceil(fileCount * fileSampleRatio)`이며, sampling이 적용된 그룹에서는 최소 1개 파일을 항상 선택합니다.
- `--file-sample-ratio`가 실제로 적용되고 동시에 `--sample-ratio < 1.0`이면 row sampling을 무시하고 `group_scan_row_sampling_ignored` warning 로그를 남깁니다.

균등 무작위 파일 추출을 택한 이유는 특정 데이터가 한 파일에 몰려 있을 가능성을 과소평가하지 않기 위해서입니다. 파일 크기 가중치 방식은 큰 파일을 더 자주 뽑아 concentration risk를 강화할 수 있습니다.

## Driver 로그
- `PRIVYSPARK_DEBUG`, `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG`, `-Dprivyspark.debug`로 driver 로그 레벨을 설정할 수 있습니다.
- 지원값은 `error`, `warn`, `info`, `debug`, `off`입니다.
- 기본값은 `warn`입니다.
- 하위호환으로 `true`는 `debug`, `false`는 `warn`으로 해석합니다.
- 로그 포맷은 `[PrivySpark][LEVEL][ISO-8601 UTC timestamp] event key=value...`입니다.

`info` 레벨은 `scan_start`, `scan_plan_ready`, `scan_complete` 같은 상위 lifecycle을 보여주고, `debug` 레벨은 파일 발견, pre-scan 실행, 그룹화, progress 준비/쓰기/merge 같은 상세 이벤트를 추가로 남깁니다.

ignore가 적용되면 `scan_directory_file_ignored`, `archive_entry_skipped reason=ignored` 같은 이벤트와 함께 `ignored_files` 집계가 `scan_directory_files_discovered`, `scan_directory_pre_scan_execute_complete`, `scan_complete`에 포함됩니다.

## `_progress` 경로 운영
- 진행 중 shard는 `<output>/_progress/<run_id>/results`, `errors`, `meta/completions` 아래 JSONL로 기록됩니다.
- 실행 중인 group, file, allowlist snapshot 작업은 `<output>/_progress/<run_id>/in-flight` 아래 임시 JSON marker를 생성합니다.
- 각 in-flight marker에는 `runId`, `scope`, `identifier`, `threadName`, `startedAtEpochMs`와 가능한 경우 `format`, `schemaSignature` 같은 스캔 메타데이터가 들어갑니다.
- in-flight marker 파일명은 파일명에 안전한 UTF-8 문자/숫자와 `.`, `_`, `-`를 보존하고, 경로 구분자와 그 외 문자는 `_`로 치환합니다. 원본 `identifier`는 JSON 본문에 유지됩니다.
- 완료된 작업과 처리 가능한 실패의 in-flight marker는 삭제됩니다. Spark application을 `FAILED`로 끝내는 미복구 group/file 실패는 marker를 보존해 마지막 진행 중 작업을 확인할 수 있게 합니다.
- setup 시작 전에는 `<output>/_progress-preparing.json` lock을 먼저 획득합니다.
- 준비가 끝나면 `_progress/active-run.json` heartbeat marker로 전환합니다.
- 다음 실행은 stale heartbeat, `FAILED` marker, stale preparing lock만 cleanup 대상으로 봅니다.
- 최근 heartbeat의 `RUNNING` marker나 fresh preparing lock이 남아 있으면 충돌로 실패합니다.
- unreadable `active-run.json`은 owner run이 `meta/run.json`을 근거로 self-heal합니다.

이 구조를 택한 이유는 긴 스캔의 중간 결과를 바로 확인하게 하면서도, 최종 리포트 소비자가 부분 결과를 완성본으로 오해하지 않게 하기 위해서입니다.

## 릴리즈
- GitHub Release는 `v*` 또는 bare semver 태그 푸시로 트리거됩니다.
- Release workflow는 `./gradlew clean shadowJar packageSampleDatasets`를 실행합니다.
- 릴리즈 자산은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, `privyspark-<tag>-sample-datasets.zip`, `privyspark-<tag>-review-response-example.html`, `privyspark-<tag>-review-response-viewer.html`입니다.
- `privyspark-<tag>-review-response-example.html`은 오프라인 리뷰 담당자가 response JSON 다운로드 흐름을 확인할 수 있는 self-contained 예시 파일입니다. 실제 운영 파일은 `scan --review-state-root` 실행 후 `<scan-output>/review/review.html`에 생성됩니다.
- `privyspark-<tag>-review-response-viewer.html`은 회수한 `response-YYYYMMDD-HHMMSS.json`을 운영자가 로컬에서 열어 envelope 메타데이터, 검증 메시지, finding별 판정을 확인하는 self-contained 파일입니다.
