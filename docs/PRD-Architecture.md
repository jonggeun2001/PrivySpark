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
4. 그룹 내 파일별 스키마 시그니처 기준 2차 분할
5. 그룹 단위 배치 스캔 수행
6. 그룹 실패/과대 그룹 시 파일 단위 폴백
7. 결과/오류 리포트 저장

### 3.2 그룹 전략
- 그룹 키: `directoryPath`, `format`, `schemaSignature`
- CSV는 헤더 순서를 유지한 시그니처를 사용해 컬럼 매핑 왜곡을 방지
- 과대 그룹(`MaxFilesPerGroupBatchScan`)은 드라이버 메모리 위험 회피를 위해 파일 단위로 전환

### 3.3 탐지 집계 전략
- 기본: 배치 집계(`agg`) 기반 정규식 매칭 카운트 계산
- 보호 장치:
  - 표현식 임계치 초과 시 legacy 경로 폴백
  - 집계 예외 발생 시 legacy 경로 폴백
- 파일 식별 컬럼은 내부 동적 이름을 사용해 원본 컬럼 충돌 방지

## 4. 출력 아키텍처
- 결과 리포트
  - Parquet: `<output>/parquet/scan_results`
  - CSV: `<output>/csv/scan_results`
- 오류 리포트
  - Parquet: `<output>/parquet/scan_errors`
  - CSV: `<output>/csv/scan_errors`
- 저장 데이터는 집계 메타데이터만 포함하고 원문 PII는 저장하지 않음

## 5. 운영 특성
- 로그는 스캔 요약(`scanned_files`, `groups`, `detections`, `errors`)과 폴백 원인을 출력
- 실패 허용 전략: 파일/그룹 단위 오류를 누적 기록하고 나머지 처리를 지속
