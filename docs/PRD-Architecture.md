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

## 3. 스캔 처리 아키텍처

### 3.1 플로우
1. 입력 경로 검증(절대경로/URI)
2. 디렉토리 구조 선스캔 및 파일 목록 수집
3. `(directory, format)` 기준 1차 그룹화
4. 그룹 내 대표 파일 1개 기준 스키마 샘플링
5. 그룹 단위 배치 스캔 수행
6. sampled group 실패 시 전체 파일 exact split 후 서브그룹 재스캔
7. 그룹 실패 시 파일 단위 폴백
8. 결과/오류 리포트 저장

### 3.2 그룹 전략
- 그룹 키: `directoryPath`, `format`, `schemaSignature`
- 다중 파일 그룹은 대표 파일 1개로 스키마를 샘플링하고 `schemaSampled=true`로 표시한다
- sampled group은 exact split으로 동질성이 확인되기 전까지 `useDirectoryIdentifier=false`로 유지한다
- CSV sampled group은 대표 파일의 헤더 유무 판정을 전체에 전파하지 않도록 batch scan 전에 exact split으로 재확인한다
- sampled group 배치 읽기 실패 시 전체 파일 exact split으로 재분류하고, 여러 서브그룹으로 갈라지면 `useDirectoryIdentifier=false`로 강등한다
- CSV는 헤더 유무를 자동 감지한다. 헤더가 있으면 헤더 순서를 유지한 시그니처를 만들고, 헤더가 없으면 컬럼 수 기반 시그니처(`cols:N`)를 사용한다. plain-text 2행 tie-case는 header 쪽으로 처리한다
- exact split으로 동일 스키마가 확인되고 pre-scan 오류가 없는 단일 디렉토리 그룹이면 결과 `file_identifier`는 파일명이 아니라 디렉토리 상대경로를 사용하며, 입력 루트 디렉토리 그룹은 `.`로 표기

### 3.3 탐지 집계 전략
- 기본: 배치 집계(`agg`) 기반 정규식 매칭 카운트 계산
- 기본/커스텀 ruleset 모두 `pii_type: name`은 지원하지 않으며, 포함 시 로드 단계에서 실패한다.
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
- `file_identifier`는 입력 경로 기준 상대경로를 사용하며, pre-scan 오류가 없는 단일 디렉토리 그룹은 디렉토리 상대경로로 집계한다. 입력 루트 디렉토리 그룹은 `.`를 사용한다.
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
