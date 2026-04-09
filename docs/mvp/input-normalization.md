# 입력 정규화와 스캔 단위

## 지원 입력
- 확장자 기반 우선 지원: `csv`, `json`, `jsonl`, `ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`
- 미지원 확장자는 text probe를 먼저 수행합니다.
- text로 판단되면 plain text로 읽고, binary로 판단되면 오류 리포트에 기록합니다.

## archive 처리
- `zip`, `jar`는 내부 엔트리를 선스캔한 뒤 지원 포맷 파일만 staging 후 스캔합니다.
- archive 확장은 1단계까지만 허용합니다.
- nested `zip`/`jar` 엔트리는 재귀 처리하지 않고 오류로 남깁니다.
- archive 내부 식별자는 `<archive>!<entry>` 형식을 사용합니다.

## Excel 처리
- `xlsx`는 workbook을 시트 단위 논리 입력으로 확장합니다.
- 빈 시트는 제외하고 내용이 있는 시트만 스캔합니다.
- 시트 식별자는 `<workbook>#<sheet>` 형식을 사용합니다.

## 그룹화와 스캔 단위
- 기본 스캔 단위는 파일입니다.
- `(directory, format, schemaSignature)` 기준으로 그룹을 만듭니다.
- exact split으로 동일 스키마가 확인된 디렉토리 그룹만 디렉토리 식별자로 승격할 수 있습니다.
- archive 엔트리와 Excel 시트는 논리 입력 식별자를 유지하며 디렉토리 식별자로 승격하지 않습니다.

## 스키마 샘플링
- 다중 파일 그룹은 대표 파일 1개로 스키마를 먼저 샘플링할 수 있습니다.
- sampled group은 batch scan 전에 exact split으로 다시 검증합니다.
- CSV는 exact split 단계에서 헤더 유무 드리프트도 다시 확인합니다.

## CSV 헤더 처리
- 헤더가 있으면 헤더명 기반 시그니처를 사용합니다.
- 헤더가 없으면 컬럼 수 기반 시그니처(`cols:N`)를 사용합니다.
- plain-text 2행 tie-case는 header 쪽으로 처리합니다.

## 손상 입력과 fallback
- JSON이 corrupt record만 생성하면 해당 파일은 손상 입력으로 기록합니다.
- 그룹 batch scan 실패 시 exact split 재시도 후, 그래도 실패하면 파일 단위 fallback으로 전환합니다.
- 읽기 중 파일 교체/삭제가 발생하면 제한된 횟수 내에서 재시도합니다.
