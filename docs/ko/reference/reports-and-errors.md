# 결과와 오류 리포트

## 최종 출력 경로
- 결과 리포트:
  - `<output>/parquet/scan_results`
  - `<output>/csv/scan_results`
- 오류 리포트:
  - `<output>/parquet/scan_errors`
  - `<output>/csv/scan_errors`

최종 출력은 Parquet와 CSV를 함께 제공합니다. 임시 `_progress` 경로는 운영 관측용이며, 최종 출력 계약은 아닙니다.

## 결과 필드
- `dataset_path`
- `scan_timestamp`
- `file_identifier`
- `column_name`
- `pii_type`
- `match_count`
- `sampled_row_count`
- `match_ratio`
- `non_empty_match_ratio`
- `confidence`
- `sample_raw_value`
- `sample_matched_fragment`

## `file_identifier` 규칙
- 기본은 입력 경로 기준 상대경로입니다.
- 동일 스키마가 exact split으로 확인되고, pre-scan 오류가 없고, 다중 파일 그룹의 디렉토리 승격이 허용된 경우에만 디렉토리 식별자로 승격합니다.
- 입력 루트 디렉토리 그룹은 `.`를 사용합니다.
- archive 내부 파일은 `<archive>!<entry>` 형식을 사용합니다.
- Excel 시트는 `<workbook>#<sheet>` 형식을 사용합니다.
- 입력 루트 아래의 단일 파일 디렉토리는 위 조건을 만족하면 디렉토리 식별자로 승격할 수 있습니다.
- 입력 루트 자체의 단일 파일과 논리 입력은 파일 또는 논리 입력 식별자를 유지합니다.

`file_identifier` 승격 조건을 엄격하게 둔 이유는 결과 해석의 기준 단위를 흐리지 않기 위해서입니다. 디렉토리 단위 집계는 편하지만, 스키마 드리프트나 pre-scan 오류가 있는 상태에서 무리하게 합치면 결과 의미가 달라집니다.

## 비율 필드
- `match_ratio`는 샘플링된 행 기준 비율입니다.
- `sampled_row_count`는 실제 탐지에 사용된 샘플링 후 행 수입니다.
- `non_empty_match_ratio`는 해당 컬럼에서 비어 있지 않은 값만 분모로 사용한 비율입니다.
- 비어 있는 값은 `null`이거나 `trim(column)` 결과가 blank인 값입니다.
- `full_column`도 `match_count` 기준만 달라질 뿐, `match_ratio`와 `confidence`의 분모는 동일하게 샘플링된 행 수입니다.
- `confidence`는 현재 구현에서 `match_ratio`와 동일한 값입니다.
- `sample_matched_fragment`는 실제 regex/validator가 검출한 원문 조각 1건입니다.
- `sample_raw_value`는 그 조각이 포함된 셀에서 앞뒤 최대 50자 문맥만 잘라 저장한 값입니다.
- 두 값 모두 소수점 둘째 자리까지 반올림합니다.

## 오류 리포트
- 일부 파일/그룹 실패는 전체 작업을 중단시키지 않고 누적 기록합니다.
- 파일 교체/삭제로 인한 읽기 오류는 재시도 후 실패 시 기록합니다.
- 손상 JSON, nested archive, unsafe archive path, 매직바이트 불일치 무확장자/미지원 확장자 입력 등은 명시적 오류로 기록합니다.

## 진행 중 progress 경로
- 진행 중 임시 shard는 `<output>/_progress/<run_id>/results/*.jsonl`, `errors/*.jsonl`, `meta/completions/*.jsonl`에 기록될 수 있습니다.
- clean completion은 탐지나 오류 row 없이 completion marker만 남깁니다.
- 정상 종료 시 `_progress` 내용을 merge해 최종 Parquet/CSV를 만들고 `_progress/<run_id>`를 삭제합니다.

progress 경로를 별도로 둔 이유는 두 가지입니다. 첫째, 긴 스캔에서 이미 끝난 범위의 결과를 바로 확인할 수 있어야 합니다. 둘째, 최종 리포트 소비자가 부분 결과를 완성본으로 오해하지 않게 해야 합니다.

## 샘플 값 저장 정책
- `scan_results`는 결과 해석을 돕기 위해 원문 샘플 1건을 저장합니다.
- `sample_matched_fragment`는 실제 검출된 조각 그대로 저장합니다.
- `sample_raw_value`는 셀 전체 원문 대신, 검출 조각 주변 앞뒤 최대 50자 문맥만 저장합니다.
- 오류 리포트는 계속 메타데이터만 저장합니다.
