# PrivySpark 기능 PRD (MVP v0.1)

## 1. 목표
사용자가 지정한 데이터 경로를 스캔해 잠재적 PII를 탐지하고, 파일 또는 디렉토리 그룹/컬럼 단위 리포트를 생성한다.

## 2. 기능 요구사항

### 2.1 CLI
- 명령: `privyspark scan`
- 인자
  - `--path <ABS_PATH_OR_URI>`: 입력 경로 (필수)
  - `--output <ABS_PATH_OR_URI>`: 출력 경로 (필수)
  - `--ruleset <default|path>`: 규칙셋 (기본 `default`)
  - `--sample-ratio <0.0~1.0>`: 샘플링 비율 (기본 `0.2`)
- `--path`, `--output`이 절대경로/URI가 아니면 즉시 실패.

### 2.2 입력 처리
- 포맷 인자는 받지 않음.
- 확장자 기반 자동 감지(`csv`, `json/jsonl/ndjson`, `parquet`, `orc`).
- 미지원 확장자는 해당 파일을 실패로 기록하고 오류 리포트에 포함.
- 스캔 단위는 파일 단위를 기본으로 하며, 동일 스키마 파일이 하나의 디렉토리 그룹이면 디렉토리 식별자로 결과를 집계할 수 있다.
- CSV 스키마 그룹핑은 전체 파일 타입 추론이 아니라 헤더 라인 파싱으로 판단한다.

### 2.3 탐지
- MVP는 정규식 매칭만 사용.
- 규칙셋은 외부 파일에서 로드.
- 기본 규칙셋 `config/rules/default.yaml` 제공.
- 규칙은 커스텀 ruleset에서 선택적으로 `column_hints`를 가질 수 있으며, 지정 시 컬럼명에 힌트가 포함된 컬럼에만 적용한다. `column_hints`가 비어 있으면 모든 컬럼을 검사한다.
- 탐지 타입은 한국 포맷 중심(이름, 전화번호, 이메일, 주민번호, 주소, 계좌번호, 카드번호, 여권번호, IP).

### 2.4 샘플링
- 기본값 `0.2`.
- 비결정적 랜덤 샘플링(seed 고정 없음).

### 2.5 출력
- 포맷: Parquet + CSV(Spark 기본 포맷, 각 출력 경로는 단일 data part file로 저장).
- 결과 리포트는 아래 필드 포함:
  - `dataset_path`, `scan_timestamp`, `file_identifier`, `column_name`, `pii_type`, `match_count`, `match_ratio`, `confidence`
- `file_identifier`는 입력 경로 기준 상대경로를 사용하며, 동일 스키마 파일이 pre-scan 오류 없이 하나의 디렉토리 그룹이면 해당 디렉토리 상대경로를 사용한다. 입력 루트 디렉토리 그룹은 `.`를 사용한다.
- `match_ratio`, `confidence`는 소수점 둘째 자리까지 반올림하며, MVP의 `confidence = match_ratio`.
- 실제 매칭값(원문 PII)은 저장하지 않음.

### 2.6 오류 처리 및 종료 코드
- 파일별 실패는 전체 중단 없이 계속 처리.
- 실패 파일은 별도 오류 리포트로 저장.
- 스키마 판별/그룹 배치 스캔/파일 폴백 스캔 중 파일이 일시적으로 교체되거나 삭제되어 읽기 오류가 나면 내부 재시도 후 계속 진행하고, 재시도 이후에도 실패하면 해당 파일/그룹 오류로 기록한다.
- 운영자는 `spark.privyspark.groupParallelism`, `spark.privyspark.fileParallelism` 설정으로 그룹/파일 폴백 병렬도를 조정할 수 있다.
- `bin/privyspark-submit` 사용 시 `PRIVYSPARK_DEBUG=true`, `spark-submit` 직접 실행 시 `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG=true` 또는 `-Dprivyspark.debug=true`가 설정되면 드라이버 debug 로그에는 스캔 계획, 그룹/파일 스캔 진행, 폴백 여부를 남겨 운영 중 분석 진행상황과 버그 확인이 가능해야 한다.
- 종료 코드는 실행 성공/실패 기준이며, PII 발견 여부와 무관.

## 3. 비목표 (MVP)
- 스트리밍/실시간 탐지
- ML/NLP 기반 분류
- 자동 마스킹/차단
- false positive 고도화(체크디짓/컨텍스트 점수 등)

## 4. MVP 완료 기준 (Acceptance Criteria)
1. 절대경로/URI 검증이 동작하고, 상대경로 입력 시 실패한다.
2. 파일 또는 단일 디렉토리 그룹 기준 정규식 탐지가 동작하고 `match_count`, `match_ratio`, `confidence`를 생성한다.
3. 결과를 Parquet + CSV로 저장한다.
4. 일부 파일 실패 시 오류 리포트를 남기고 나머지 파일 처리를 계속한다.
