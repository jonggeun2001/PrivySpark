# PrivySpark 아키텍처 PRD (MVP v0.1)

## 1. 아키텍처 목표
- Spark 기반 배치 처리로 대용량 데이터셋을 안정적으로 스캔한다.
- 파일 단위 결과 정확성을 유지하면서 디렉토리/포맷 그룹화로 처리 효율을 확보한다.
- 일부 파일/그룹 실패 시 전체 작업을 중단하지 않고 가능한 범위를 계속 처리한다.

## 2. 실행/배포 환경
- Spark on YARN cluster (`--master yarn --deploy-mode cluster`)
- Spark 버전 타겟: `3.5.3`
- Scala 버전 타겟: `2.12`
- JVM 바이트코드 타겟: `1.8`
- 클러스터 외부 네트워크 차단 환경 지원
- 배포 아티팩트: Shadow fat JAR(`*-all.jar`)
- 기본 규칙셋은 `spark-submit --files`로 드라이버에 배포

### 2.1 구현 컴포넌트 매핑
- `Cli.scala`: `privyspark scan` CLI 파싱과 `sample-ratio` 유효성 검증
- `PathValidator.scala`: 입력/출력 경로의 절대경로/URI 판별
- `FormatDetector.scala`: `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar` 확장자 판별
- `RulesetLoader.scala`: 기본 ruleset 탐색, 커스텀 ruleset 파싱, 금지 필드 검증
- `DetectionAggregator.scala`: 규칙별 metric 구성, `match_type` 처리, 집계/폴백 경로
- `DriverLicenseNumberValidator.scala`: `driver_license_number` 내장 strict validator
- `PrivySparkApp.scala`: 선스캔, archive/xlsx/text 입력 정규화, 그룹화, exact split, 재시도, 리포트 저장 오케스트레이션
- `Models.scala`: `PiiRule`, `ScanResult`, `ScanError` 스키마 정의

## 3. 스캔 처리 아키텍처

### 3.1 플로우
1. 입력 경로 검증(절대경로/URI)
2. 디렉토리 구조 선스캔 및 파일 목록 수집
3. archive 엔트리 확장, workbook 시트 확장, unknown-extension text probe 수행
4. `(directory, format)` 기준 1차 그룹화
5. 그룹 내 대표 파일 1개 기준 스키마 샘플링
6. 그룹 단위 배치 스캔 수행
7. sampled group 실패 시 전체 파일 exact split 후 서브그룹 재스캔
8. 그룹 실패 시 파일 단위 폴백
9. 결과/오류 리포트 저장

### 3.2 그룹 전략
- 그룹 키: `directoryPath`, `format`, `schemaSignature`
- 다중 파일 그룹은 대표 파일 1개로 스키마를 샘플링하고 `schemaSampled=true`로 표시한다
- sampled group은 exact split으로 동질성이 확인되기 전까지 `useDirectoryIdentifier=false`로 유지한다
- archive 내부 파일과 Excel 시트는 논리 입력(`archive!entry`, `workbook#sheet`) 기준 식별자를 유지하며 디렉토리 식별자로 승격하지 않는다
- sampled multi-file group은 batch scan 전에 exact split으로 재확인하고, 단일 동일-스키마 그룹이면 `useDirectoryIdentifier=true`로 복원한다
- CSV는 exact split 단계에서 대표 파일의 헤더 유무 판정을 전체에 전파하지 않도록 헤더 드리프트도 함께 재확인한다
- exact split 이후에도 batch 읽기가 실패하면 파일 단위 폴백으로 전환하고, 여러 서브그룹으로 갈라지면 `useDirectoryIdentifier=false`를 유지한다
- JSON 스키마 판별 또는 배치 읽기 결과가 Spark 내부 corrupt record 컬럼만 남기면 해당 파일/배치는 손상 입력으로 간주하고 내부 Spark 예외 대신 PrivySpark 오류 메시지로 전환한다
- CSV는 헤더 유무를 자동 감지한다. 헤더가 있으면 헤더 순서를 유지한 시그니처를 만들고, 헤더가 없으면 컬럼 수 기반 시그니처(`cols:N`)를 사용한다. plain-text 2행 tie-case는 header 쪽으로 처리한다
- unknown-extension text fallback은 단일 `value` 컬럼 스키마로 취급한다
- exact split으로 동일 스키마가 확인되고 pre-scan 오류가 없는 단일 디렉토리 그룹이면 결과 `file_identifier`는 파일명이 아니라 디렉토리 상대경로를 사용하며, 입력 루트 디렉토리 그룹은 `.`로 표기

### 3.3 탐지 집계 전략
- 기본: 배치 집계(`agg`) 기반 정규식 매칭 카운트 계산
- 기본/커스텀 ruleset 모두 `pii_type: name`은 지원하지 않으며, 포함 시 로드 단계에서 실패한다.
- 기본/커스텀 ruleset 모두 제거된 `validator` 필드와 `__KOREAN_NAME_RULE_REGEX__` 내부 참조를 지원하지 않으며, 포함 시 로드 단계에서 실패한다.
- 기본 ruleset은 전화번호, 이메일, 주민등록번호, 외국인 등록번호, 운전면허번호, 주소, 계좌번호, 카드번호, 한국 여권번호, IP를 기본 탐지 대상으로 포함한다.
- 기본 `resident_registration_number`는 하이픈 포함/미포함 입력 모두에서 성별/세기 코드 1자리만 있는 축약형과 전체 형식을 모두 허용하고, 더 긴 숫자 토큰 내부 substring 매치는 제외한다.
- 기본 `driver_license_number`는 candidate regex에 매칭된 값에 대해 하이픈 정규화 후 구형 10자리 또는 현행 12자리 형식만 통과시키는 내장 strict validator를 적용한다. 현행 12자리는 지역코드 `11`~`26`, `28`만 허용한다.
- 기본 ruleset의 `passport_number`는 한국 여권번호 형식만 대상으로 하고, 다른 영숫자 토큰 내부 substring 매치는 제외한다.
- 규칙이 `column_hints`를 가지면 컬럼명 힌트와 매칭되는 컬럼에만 metric을 생성하고, 힌트가 없으면 모든 컬럼에 적용한다.
- 규칙의 `match_type`이 `value`면 행 단위 regex 일치 개수를 집계하고, `full_column`이면 비어 있지 않은 값 전체가 regex를 만족하는 컬럼/파일에 대해서만 결과를 생성한다.
- 보호 장치:
  - 파일 교체/삭제로 인한 읽기 실패 시 경로 메타데이터 refresh 후 제한 횟수 내 재시도
  - 표현식 임계치(기본 `50,000`) 초과 시 소배치 집계 기반 fallback 경로로 전환
  - 집계 예외 발생 시 per-metric legacy fallback 경로로 전환
- 파일 식별 컬럼은 내부 동적 이름을 사용해 원본 컬럼 충돌 방지

## 4. 출력 아키텍처
- 결과 리포트
  - Parquet: `<output>/parquet/scan_results` (`coalesce(1)`로 단일 data part file 저장)
  - CSV: `<output>/csv/scan_results` (`coalesce(1)`로 단일 data part file 저장)
- `file_identifier`는 입력 경로 기준 상대경로를 사용하며, archive 내부 파일은 `<archive>!<entry>`, Excel 시트는 `<workbook>#<sheet>` 형식을 사용한다. pre-scan 오류가 없는 단일 디렉토리 그룹만 디렉토리 상대경로로 집계하며, 입력 루트 디렉토리 그룹은 `.`를 사용한다.
- 오류 리포트
  - Parquet: `<output>/parquet/scan_errors` (`coalesce(1)`로 단일 data part file 저장)
  - CSV: `<output>/csv/scan_errors` (`coalesce(1)`로 단일 data part file 저장)
- `match_ratio`, `confidence`는 결과 생성 시 소수점 둘째 자리까지 반올림한다.
- 저장 데이터는 집계 메타데이터만 포함하고 원문 PII는 저장하지 않음

## 5. 운영 특성
- 로그는 스캔 요약(`scanned_files`, `groups`, `detections`, `errors`)과 폴백 원인/실행 경로를 드라이버 로그에 출력하고, `bin/privyspark-submit`의 `PRIVYSPARK_DEBUG=true` 또는 `spark-submit`의 `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG=true`/`-Dprivyspark.debug=true`가 설정되면 debug 진행 이벤트(플랜 수립, 스키마 분할, 그룹/파일 스캔, 리포트 저장)를 추가로 출력
- 실패 허용 전략: 파일/그룹 단위 오류를 누적 기록하고 나머지 처리를 지속
- 그룹 스캔 병렬도는 `spark.privyspark.groupParallelism`(기본 `4`), 파일 폴백 병렬도는 `spark.privyspark.fileParallelism`(기본 `3`)으로 조정한다
- report DataFrame은 cache 후 Parquet/CSV 저장을 재사용하고, `sampledDf`/report DataFrame 해제는 non-blocking `unpersist(false)`를 사용한다
