# 입력 포맷과 정규화

## 지원 입력
- 확장자 기반 우선 지원: `csv`, `json`, `jsonl`, `ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`
- 무확장자 파일과 미지원 확장자 파일은 앞부분 매직바이트로 `parquet`, `orc`를 먼저 판별합니다.
- 매직바이트가 일치하지 않더라도 UTF-8 텍스트처럼 보이는 입력은 내부 `text` 포맷으로 정규화해 Spark `text` reader의 단일 `value` 컬럼으로 스캔합니다.
- UTF-8 텍스트 안에서 ASCII 정보 구분자(`0x1C`-`0x1F`, 예: RS 구분 파일)가 자주 등장해도 text fallback 입력으로 처리합니다.
- 바이너리처럼 보이는 입력만 `Unsupported file format`으로 오류 리포트에 기록합니다.
- 0바이트 physical file은 pre-scan에서 즉시 건너뜁니다.

이 text fallback을 둔 이유는 확장자만으로 텍스트 로그나 덤프를 배제하면 실제 운영 입력을 지나치게 많이 놓치기 때문입니다. 반대로 아무 바이너리나 텍스트로 강제 처리하면 노이즈가 커지므로, 매직바이트와 UTF-8 probe를 함께 사용해 경계를 분리합니다.

## Archive 처리
- `zip`, `jar`는 내부 엔트리를 선스캔한 뒤 지원 포맷 파일만 staging 후 스캔합니다.
- archive 확장은 1단계까지만 허용합니다.
- nested `zip`/`jar` 엔트리는 재귀 처리하지 않고 오류로 남깁니다.
- 0바이트 archive entry는 staging이나 오류 리포트 없이 건너뜁니다.
- archive 내부 식별자는 `<archive>!<entry>` 형식을 사용합니다.

## Excel 처리
- `xlsx`는 workbook을 시트 단위 논리 입력으로 확장합니다.
- 빈 시트는 제외하고, 내용이 있는 시트만 스캔합니다.
- 시트 식별자는 `<workbook>#<sheet>` 형식을 사용합니다.

## 그룹화와 스캔 단위
- 기본 스캔 단위는 파일입니다.
- 먼저 `(directory, format)` 기준으로 1차 그룹을 만듭니다.
- 이후 대표 파일 스키마 샘플링과 exact split으로 `schemaSignature`를 보강하거나 그룹을 다시 나눕니다.
- 동일 스키마가 확인된 다중 파일 그룹만 디렉토리 식별자로 승격할 수 있습니다.
- archive 엔트리와 Excel 시트는 논리 입력 식별자를 유지합니다.

## 스키마 샘플링
- 다중 파일 그룹은 대표 파일 1개로 스키마를 먼저 샘플링할 수 있습니다.
- 그룹별 schema split은 `--pre-scan-parallelism`을 재사용해 driver 측에서 병렬 수행합니다.
- sampled group은 batch scan 전에 exact split으로 다시 검증합니다.
- CSV는 exact split 단계에서 헤더 유무 드리프트도 다시 확인합니다.

스키마 샘플링을 별도 단계로 둔 이유는 두 가지입니다. 첫째, 같은 디렉토리라도 실제 스키마 드리프트가 있을 수 있으므로 디렉토리 단위 집계를 바로 적용하면 식별자 의미가 깨질 수 있습니다. 둘째, 모든 파일을 처음부터 exact split으로 읽으면 pre-scan 비용이 커지므로 대표 파일 샘플링으로 비용과 정확도 사이를 먼저 조정합니다.

## CSV 헤더 처리
- 헤더가 있으면 헤더명 기반 시그니처를 사용합니다.
- 헤더가 없으면 컬럼 수 기반 시그니처(`cols:N`)를 사용합니다.
- plain-text 2행 tie-case는 header 쪽으로 처리합니다.

## 손상 입력과 fallback
- JSON이 corrupt record만 생성하면 해당 파일은 손상 입력으로 기록합니다.
- sampled multi-file group은 batch scan 전에 exact split으로 먼저 재검증합니다.
- 일반 group에서 batch scan이 실패하면 별도 schema resplit 없이 파일 단위 fallback으로 전환합니다.
- 읽기 중 파일 교체/삭제가 발생하면 제한된 횟수 내에서 재시도합니다.
