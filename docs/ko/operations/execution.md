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
- `--sample-ratio <(0.0, 1.0]>`: row sampling 비율, 기본 `0.2`
- `--file-sample-ratio <(0.0, 1.0]>`: batch group scan 파일 샘플링 비율, 기본 미설정
- `--pre-scan-parallelism <INT>`: 파일 pre-scan 확장과 schema split 병렬도, `> 0`
- `--group-parallelism <INT>`: 그룹 스캔 병렬도, `> 0`
- `--file-parallelism <INT>`: 파일 폴백 스캔 병렬도, `> 0`
- `--ignore <PATTERN>`: 반복 지정 가능한 gitignore 스타일 glob ignore 패턴
- `--ignore-file <PATH>`: 줄 단위 ignore 패턴 파일 경로, `#` 주석과 빈 줄 무시

## Ignore 패턴
- `/`가 없는 패턴은 basename 기준으로 매칭합니다. 예: `_SUCCESS`, `*.crc`
- `/`가 있는 패턴은 입력 루트 기준 상대 경로로 매칭합니다. 예: `backup/**`, `logs/2025/*.gz`
- 선행 `/`는 입력 루트 anchor로 해석합니다. 예: `/backup/**`, `/logs/`
- `/`로 끝나는 패턴은 디렉터리 매칭으로 간주하고 하위 전체를 제외합니다. 예: `logs/`
- archive entry도 `<archive>!<entry>` 논리 식별자에서 entry 상대 경로 기준으로 같은 ignore 규칙을 적용합니다.
- v1 범위에서는 `!pattern` negate 문법을 지원하지 않습니다.
- `--ignore-file`은 Hadoop `FileSystem`으로 읽습니다. YARN cluster에서 client 로컬 파일을 쓰려면 `--files` 또는 `PRIVYSPARK_SPARK_FILES`로 먼저 배포한 뒤 alias 경로를 `--ignore-file`에 넘겨야 합니다.

ignore 필터를 pre-scan 전에 적용하는 이유는 `_SUCCESS`, `.crc`, 로그, 백업 파일처럼 스캔 가치가 낮은 입력 때문에 불필요한 I/O, 오류 리포트, 결과 노이즈가 늘어나는 것을 막기 위해서입니다.

## 병렬도
- CLI 값을 주면 해당 값이 앱 로직에 직접 전달됩니다.
- CLI 값을 생략하면 `spark.privyspark.preScanParallelism`, `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism` 또는 앱 기본값(`4`, `4`, `3`)을 사용합니다.
- pre-scan 병렬도는 파일 단위 입력 확장, 포맷 판별, 그룹별 schema split 경로에 적용됩니다.
- pre-scan 병렬도는 파일 수와 safety ceiling `64` 기준으로 축소됩니다.
- 그룹 병렬도와 파일 병렬도는 driver가 동시에 제출하는 작업 수를 제어합니다.

여기서 중요한 점은 앱 레벨 병렬도가 곧 executor 수를 직접 보장하는 것은 아니라는 점입니다. 실제 executor 분산은 입력 파티션 수, Spark scheduler, dynamic allocation backlog에 함께 영향을 받습니다.

## 샘플링
- `--sample-ratio`는 비결정적 row sampling입니다.
- `sampleRatio >= 1.0`이면 row sampling 없이 전체 행을 사용합니다.
- `--file-sample-ratio`는 batch-capable group scan에서 그룹 내부 파일을 균등 무작위로 추출합니다.
- 샘플 파일 수는 `ceil(fileCount * fileSampleRatio)`이며 최소 1개 파일은 항상 선택합니다.
- `--file-sample-ratio`가 설정되고 동시에 `--sample-ratio < 1.0`이면 batch-capable group scan에서는 row sampling을 무시하고 `group_scan_row_sampling_ignored` warning 로그를 남깁니다.

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
- setup 시작 전에는 `<output>/_progress-preparing.json` lock을 먼저 획득합니다.
- 준비가 끝나면 `_progress/active-run.json` heartbeat marker로 전환합니다.
- 다음 실행은 stale heartbeat, `FAILED` marker, stale preparing lock만 cleanup 대상으로 봅니다.
- 최근 heartbeat의 `RUNNING` marker나 fresh preparing lock이 남아 있으면 충돌로 실패합니다.
- unreadable `active-run.json`은 owner run이 `meta/run.json`을 근거로 self-heal합니다.

이 구조를 택한 이유는 긴 스캔의 중간 결과를 바로 확인하게 하면서도, 최종 리포트 소비자가 부분 결과를 완성본으로 오해하지 않게 하기 위해서입니다.

## 릴리즈
- GitHub Release는 `v*` 또는 bare semver 태그 푸시로 트리거됩니다.
- Release workflow는 `./gradlew clean shadowJar packageSampleDatasets`를 실행합니다.
- 릴리즈 자산은 `privyspark-<tag>-all.jar`, `privyspark-<tag>-all.jar.sha256`, `default-rules.yaml`, `privyspark-<tag>-sample-datasets.zip`입니다.
