# 출력과 오류 처리

## 결과 리포트
- Parquet: `<output>/parquet/scan_results`
- CSV: `<output>/csv/scan_results`
- 두 출력 모두 `coalesce(1)`로 단일 data part file 저장 경로를 사용합니다.

## 결과 필드
- `dataset_path`
- `scan_timestamp`
- `file_identifier`
- `column_name`
- `pii_type`
- `match_count`
- `match_ratio`
- `confidence`

## file_identifier 규칙
- 기본은 입력 경로 기준 상대경로입니다.
- 디렉토리 상대경로 승격은 exact split으로 동일 스키마가 확인되고, pre-scan 오류가 없고, 디렉토리 식별자 승격이 허용된 다중 파일 그룹에서만 일어납니다.
- plain text fallback, archive 내부 파일, Excel 시트, 단일 파일 그룹은 파일 또는 논리 입력 식별자를 유지합니다.
- 입력 루트 디렉토리 그룹은 `.`를 사용합니다.
- archive 내부 파일은 `<archive>!<entry>` 형식을 사용합니다.
- Excel 시트는 `<workbook>#<sheet>` 형식을 사용합니다.

## 확률 필드
- `match_ratio`는 샘플링된 행 기준 비율입니다.
- `confidence`는 현재 MVP에서 `match_ratio`와 동일한 값입니다.
- 두 값 모두 소수점 둘째 자리까지 반올림합니다.

## 오류 리포트
- Parquet: `<output>/parquet/scan_errors`
- CSV: `<output>/csv/scan_errors`
- 일부 파일/그룹 실패는 전체 작업을 중단시키지 않고 누적 기록합니다.

## 오류 처리 원칙
- 실패 파일은 오류 리포트에 남기고 나머지 처리를 계속합니다.
- 파일 교체/삭제로 인한 읽기 오류는 내부 재시도 후 실패 시 기록합니다.
- 손상 JSON, nested archive, unsafe archive path, binary unknown-extension 등은 명시적 오류로 기록합니다.

## 보안 원칙
- 원문 PII 값은 저장하지 않습니다.
- 저장 대상은 집계 메타데이터와 오류 메타데이터만입니다.
