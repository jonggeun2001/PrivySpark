# PrivySpark PRD (MVP v0.1)

## 1. 목표
사용자가 지정한 데이터 경로를 스캔해 잠재적 PII를 탐지하고, 파일/컬럼 단위 리포트를 생성한다.

## 2. 기능 요구사항

### 2.1 CLI
- 명령: `privyspark scan`
- 인자
  - `--path <ABS_PATH_OR_URI>`: 입력 경로 (필수)
  - `--output <ABS_PATH_OR_URI>`: 출력 경로 (필수)
  - `--ruleset <default|path>`: 규칙셋 (기본 `default`)
  - `--sample-ratio <0.0~1.0>`: 샘플링 비율 (기본 `0.2`)
- `--path`, `--output`이 절대경로/URI가 아니면 즉시 실패.

### 2.2 실행 환경
- Spark on YARN cluster (`--master yarn --deploy-mode cluster`)
- Spark 버전 타겟: `3.5.3`
- Scala 버전 타겟: `2.12`
- JVM 바이트코드 타겟: `1.8`
- 클러스터 외부 네트워크 차단 환경을 지원해야 함
- 배포 아티팩트는 Shadow fat JAR(`*-all.jar`)를 기본으로 사용
- 기본 규칙셋 파일은 `spark-submit --files`로 드라이버에 배포

### 2.3 입력 처리
- 포맷 인자는 받지 않음.
- 확장자 기반 자동 감지(`csv`, `json/jsonl/ndjson`, `parquet`).
- 미지원 확장자는 해당 파일을 실패로 기록하고 오류 리포트에 포함.
- 스캔 단위는 파일 단위.

### 2.4 탐지
- MVP는 정규식 매칭만 사용.
- 규칙셋은 외부 파일에서 로드.
- 기본 규칙셋 `config/rules/default.yaml` 제공.
- 탐지 타입은 한국 포맷 중심(이름, 전화번호, 이메일, 주민번호, 주소, 계좌번호, 카드번호, 여권번호, IP).

### 2.5 샘플링
- 기본값 `0.2`.
- 비결정적 랜덤 샘플링(seed 고정 없음).

### 2.6 출력
- 포맷: Parquet + CSV(Spark 기본 포맷).
- 결과 리포트는 아래 필드 포함:
  - `dataset_path`, `scan_timestamp`, `file_identifier`, `column_name`, `pii_type`, `match_count`, `match_ratio`, `confidence`
- MVP의 `confidence = match_ratio`.
- 실제 매칭값(원문 PII)은 저장하지 않음.

### 2.7 오류 처리 및 종료 코드
- 파일별 실패는 전체 중단 없이 계속 처리.
- 실패 파일은 별도 오류 리포트로 저장.
- 종료 코드는 실행 성공/실패 기준이며, PII 발견 여부와 무관.

## 3. 비목표 (MVP)
- 스트리밍/실시간 탐지
- ML/NLP 기반 분류
- 자동 마스킹/차단
- false positive 고도화(체크디짓/컨텍스트 점수 등)

## 4. MVP 완료 기준 (Acceptance Criteria)
1. 절대경로/URI 검증이 동작하고, 상대경로 입력 시 실패한다.
2. YARN cluster 모드에서 실행 가능하다.
3. 파일 단위 정규식 탐지가 동작하고 `match_count`, `match_ratio`, `confidence`를 생성한다.
4. 결과를 Parquet + CSV로 저장한다.
5. 일부 파일 실패 시 오류 리포트를 남기고 나머지 파일 처리를 계속한다.
