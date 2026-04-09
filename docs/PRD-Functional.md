# PrivySpark 기능 PRD (MVP)

## 목표
사용자가 지정한 데이터 경로를 스캔해 잠재적 PII를 탐지하고, 결과 리포트와 오류 리포트를 생성합니다.

## 기능 기준선
- 실행 진입점은 `privyspark scan` 단일 명령입니다.
- 입력은 절대경로 또는 URI만 허용합니다.
- 지원 포맷은 코드 기준으로 `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`이며, 그 외 확장자는 text probe 후 plain text fallback 여부를 결정합니다.
- 탐지는 ruleset 기반 regex + 일부 타입의 strict validator 조합입니다.
- 출력은 Parquet + CSV 2종이며 `scan_results`, `scan_errors`를 함께 생성합니다.
- 일부 파일/그룹 실패는 전체 작업을 중단시키지 않고 누적 기록합니다.

## 상세 문서 맵
- 실행, 샘플링, 병렬도, 릴리즈: [mvp/execution-and-operations.md](mvp/execution-and-operations.md)
- 입력 포맷, archive/xlsx/text fallback, 그룹화: [mvp/input-normalization.md](mvp/input-normalization.md)
- ruleset, 탐지 타입, `match_type`, 집계 fallback: [mvp/detection-and-rules.md](mvp/detection-and-rules.md)
- 출력 스키마, `file_identifier`, 오류 리포트: [mvp/reporting-and-failures.md](mvp/reporting-and-failures.md)
- 전체 MVP 문서 인덱스: [mvp/README.md](mvp/README.md)

## Acceptance Criteria
1. 절대경로/URI 검증이 동작하고 상대경로 입력은 거부됩니다.
2. 현재 지원 포맷과 text fallback 경로가 소스 기준으로 문서화돼 있습니다.
3. ruleset 기반 탐지와 결과 필드(`match_count`, `match_ratio`, `confidence`) 의미가 문서화돼 있습니다.
4. 일부 파일 실패 시 오류 리포트를 남기고 나머지 처리를 계속하는 동작이 문서화돼 있습니다.
