# 결과와 오류 리포트

## 최종 출력 경로
- 기본 결과 리포트:
  - `<output>/parquet/scan_results`
- 기본 오류 리포트:
  - `<output>/parquet/scan_errors`
- `--output-format csv` 추가 시:
  - `<output>/csv/scan_results`
  - `<output>/csv/scan_errors`
- `--output-format excel` 추가 시:
  - `<output>/excel/scan_results.xlsx`
  - `<output>/excel/scan_errors.xlsx`

`--output-format`은 반복 지정 가능하고 지원값은 `parquet`, `csv`, `excel`입니다. 기본값은 `parquet`입니다. 임시 `_progress` 경로는 운영 관측용이며, 최종 출력 계약은 아닙니다.

## 결과 필드
- `dataset_path`
- `scan_timestamp`
- `file_identifier`
- `column_name`
- `pii_type`
- `match_count`
- `sampled_row_count`
- `non_empty_value_count`
- `match_ratio`
- `non_empty_match_ratio`
- `confidence`
- `sample_raw_value`
- `sample_matched_fragment`
- `file_size`
- `file_mtime_epoch_ms`
- `hive_table_fqn`
- `aggregated`
- `aggregated_file_count`
- `aggregated_partition_count`
- `review_status`
- `review_reason`
- `review_invalidated`
- `review_scope_file_identifiers`
- `review_scope_file_fingerprints`

`scan_results.scan_timestamp`는 CLI 시작 시각 고정값이 아니라, 각 결과 row가 실제로 만들어진 시점의 UTC ISO-8601 시각입니다. 따라서 장시간 스캔이나 다중 그룹 스캔에서는 결과 row마다 값이 달라질 수 있습니다.

## `hive_table_fqn` 규칙
- `--hive-metastore-jdbc-url`, `--hive-metastore-user`, `--hive-metastore-password-file` 세 옵션을 모두 지정한 경우에만 활성화됩니다.
- 활성화되면 driver가 설정된 JDBC driver class로 Hive Metastore `DBS`/`TBLS`/`SDS` 테이블을 1회 조회하고, table-level `LOCATION`을 정규화 URI prefix 인덱스로 broadcast 합니다. 기본 driver class는 `org.mariadb.jdbc.Driver`이며 `--hive-metastore-jdbc-driver-class` 또는 `spark.privyspark.hiveMetastore.jdbcDriverClass`로 변경할 수 있습니다. CLI 값이 Spark conf보다 우선합니다.
- 결과 row의 입력 파일 경로가 등록된 table `LOCATION` 하위에 있으면 `db.table` 형식으로 `hive_table_fqn`을 채웁니다.
- 최종 `scan_results`는 `hive_table_fqn`이 있는 결과를 `dataset_path`, `hive_table_fqn`, `column_name`, `pii_type` 단위로 묶습니다. 파티션/파일별 반복 row는 테이블 단위 row 1개가 되고, `match_count`, `sampled_row_count`, `non_empty_value_count`는 합산한 뒤 `match_ratio`, `non_empty_match_ratio`, `confidence`를 다시 계산합니다.
- 테이블 단위로 묶인 row는 `file_identifier`에 파티션 세그먼트를 제거한 테이블 루트 식별자를 기록하고, `aggregated=true`, `aggregated_file_count`, `aggregated_partition_count`로 묶인 규모를 표시합니다. Hive 매핑이 없는 결과는 기존 파일/디렉토리 식별자 단위를 유지합니다.
- 여러 table `LOCATION`이 겹치면 정규화된 URI 기준 longest-prefix match를 사용합니다. 같은 길이의 중복 prefix는 deterministic 정렬 결과를 사용합니다.
- archive entry와 Excel sheet 식별자는 `<archive>!<entry>`, `<workbook>#<sheet>`에서 host archive/workbook path만 떼어 lookup 합니다.
- 옵션이 미지정됐거나, JDBC 접속/쿼리/password 파일 읽기가 실패했거나, 매칭되는 table이 없으면 빈 문자열 `""`을 기록합니다.
- partition별 `LOCATION` override는 현재 열거하지 않습니다. table-level `LOCATION`만 사용합니다.

## `file_identifier` 규칙
- 기본은 입력 경로 기준 상대경로입니다.
- 동일 스키마가 exact split으로 확인되고, pre-scan 오류가 없고, 다중 파일 그룹의 디렉토리 승격이 허용된 경우에만 디렉토리 식별자로 승격합니다.
- sampled `text` group과 bounded schema validation을 통과한 sampled Parquet/ORC/Avro group은 exact split 디렉토리 집계로 승격된 상태가 아니므로 batch 경로에서 파일 식별자를 유지합니다.
- 입력 루트 디렉토리 그룹은 `.`를 사용합니다.
- partition, bucket, skew/list-bucketing layout 디렉토리는 그룹화용 물리 레이아웃 메타데이터로 취급하므로, 승격 가능한 row는 각 layout 하위 디렉토리 대신 정규화된 테이블 경로를 식별자로 사용합니다.
- archive 내부 파일은 `<archive>!<entry>` 형식을 사용합니다.
- Excel 시트는 `<workbook>#<sheet>` 형식을 사용합니다.
- 입력 루트 아래의 단일 파일 디렉토리는 위 조건을 만족하면 디렉토리 식별자로 승격할 수 있습니다.
- 입력 루트 자체의 단일 파일과 논리 입력은 파일 또는 논리 입력 식별자를 유지합니다.

`file_identifier` 승격 조건을 엄격하게 둔 이유는 결과 해석의 기준 단위를 흐리지 않기 위해서입니다. 디렉토리 단위 집계는 편하지만, 스키마 드리프트나 pre-scan 오류가 있는 상태에서 무리하게 합치면 결과 의미가 달라집니다.

## Review 필드
- `file_size`는 해당 row를 대표하는 파일 바이트 크기입니다. 파일 식별자 row는 파일 크기, 디렉토리 식별자 row는 포함된 파일 크기 합계를 기록합니다.
- `file_mtime_epoch_ms`는 해당 row를 대표하는 파일의 마지막 수정 시각(epoch milliseconds)입니다. 디렉토리 식별자 row는 포함된 파일 중 최대 mtime을 기록합니다.
- `review_status` 기본값은 `pending`입니다. 운영 검토에서 `false_positive`, `true_positive`로 편집할 수 있습니다.
- `review_reason`은 검토 사유 텍스트입니다. `false_positive` 판정 시 필수로 채우는 것을 권장합니다.
- `review_invalidated=true`는 이전 allowlist와 같은 `(dataset_path, file_identifier, column_name, pii_type)` 조합이 있었지만, 현재 파일 메타데이터와 checksum이 달라져 재검토가 필요함을 의미합니다.
- `review_scope_file_identifiers`는 디렉토리 또는 Hive 테이블 집계 row가 실제로 포함한 concrete file identifier 목록입니다. `|` 구분 문자열로 저장되고 `review apply`는 이 목록만 allowlist로 전개합니다.
- `review_scope_file_fingerprints`는 디렉토리 또는 Hive 테이블 집계 row의 파일별 fingerprint snapshot입니다. 내부 인코딩 문자열로 저장되고 `review apply`는 scope 안의 모든 fingerprint가 일치할 때만 false positive를 staged 합니다.
- `--allowlist`를 쓰지 않으면 review 관련 필드는 기본값만 채워집니다.

## 비율 필드
- `match_ratio`는 샘플링된 행 기준 비율입니다.
- `sampled_row_count`는 실제 탐지에 사용된 샘플링 후 행 수입니다.
- `non_empty_value_count`는 해당 컬럼에서 비어 있지 않아 `non_empty_match_ratio`와 `confidence` 계산 분모로 사용된 값 수입니다.
- `non_empty_match_ratio`는 해당 컬럼에서 비어 있지 않은 값만 분모로 사용한 비율입니다.
- 비어 있는 값은 `null`이거나 `trim(column)` 결과가 blank인 값입니다.
- `full_column`도 `match_count` 기준만 달라질 뿐, `confidence`는 여전히 해당 컬럼의 non-empty 값 기준으로 계산됩니다.
- `confidence`는 `match_count / non_empty_count`의 95% Wilson score 신뢰구간 하한(z=1.96)입니다. 표본이 작을수록 보수적으로 낮아지고, 표본이 커질수록 `non_empty_match_ratio`에 수렴합니다.
- `sample_matched_fragment`는 실제 regex/validator가 검출한 원문 조각 1건입니다.
- `sample_raw_value`는 그 조각이 포함된 셀에서 앞뒤 최대 50자 문맥만 잘라 저장한 값입니다.
- 두 값 모두 소수점 둘째 자리까지 반올림합니다.

## 오류 리포트
- 일부 파일/그룹 실패는 전체 작업을 중단시키지 않고 누적 기록합니다.
- 파일 교체/삭제로 인한 읽기 오류는 재시도 후 실패 시 기록합니다.
- 손상 JSON, nested archive, unsafe archive path, password-protected archive, multi-volume RAR, RAR5 archive, 매직바이트 불일치 무확장자/미지원 확장자 입력 등은 명시적 오류로 기록합니다.

## 진행 중 progress 경로
- 진행 중 임시 shard는 `<output>/_progress/<run_id>/results/*.jsonl`, `errors/*.jsonl`, `meta/completions/*.jsonl`에 기록될 수 있습니다.
- file fallback scan은 기본적으로 group 종료 시 progress shard를 flush합니다. 따라서 `_progress`는 최종 merge 소스이지만 file별 실시간 tail 계약은 아니며, 파일 완료 즉시 shard가 필요하면 `spark.privyspark.progress.flushMode=file`을 사용합니다.
- 작업이 실행 중일 때는 `<output>/_progress/<run_id>/in-flight/*.json`에 활성 group, file, allowlist snapshot rescan별 marker가 있을 수 있습니다.
- in-flight marker는 운영 진단용입니다. 완료된 작업과 처리 가능한 실패는 marker를 삭제하지만, Spark application을 `FAILED`로 끝내는 미복구 group/file 실패는 marker를 보존합니다.
- in-flight marker 파일명은 파일명에 안전한 UTF-8 문자/숫자와 `.`, `_`, `-`를 보존하고, 경로 구분자와 그 외 문자는 `_`로 치환합니다. 원본 `identifier`는 marker JSON 본문에 유지됩니다.
- clean completion은 탐지나 오류 row 없이 completion marker만 남깁니다.
- 정상 종료 시 `_progress` 내용을 merge하고 Hive 테이블 단위 최종 집계를 적용해 선택된 최종 출력 포맷을 만든 뒤 `_progress/<run_id>`를 삭제합니다. `_progress`의 중간 JSONL은 디버깅용 원시 shard이며 최종 소비 계약이 아닙니다.

progress 경로를 별도로 둔 이유는 두 가지입니다. 첫째, 긴 스캔에서 이미 끝난 범위의 결과를 바로 확인할 수 있어야 합니다. 둘째, 최종 리포트 소비자가 부분 결과를 완성본으로 오해하지 않게 해야 합니다.

## 샘플 값 저장 정책
- `scan_results`는 결과 해석을 돕기 위해 원문 샘플 1건을 저장합니다.
- `sample_matched_fragment`는 실제 검출된 조각 그대로 저장합니다.
- `sample_raw_value`는 셀 전체 원문 대신, 검출 조각 주변 앞뒤 최대 50자 문맥만 저장합니다.
- 오류 리포트는 계속 메타데이터만 저장합니다.
