# PrivySpark 기능 PRD (MVP)

## 목표
사용자가 지정한 데이터 경로를 스캔해 잠재적 PII를 탐지하고, 결과 리포트와 오류 리포트를 생성합니다.

## 기능 기준선
- 실행 진입점은 `privyspark scan` 단일 명령입니다.
- 입력은 절대경로 또는 URI만 허용합니다.
- 지원 포맷은 코드 기준으로 `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`입니다.
- 확장자가 없는 파일과 미지원 확장자 파일은 앞부분 매직바이트로 `parquet`, `orc`를 우선 판별하고, 텍스트처럼 보이는 입력은 단일 `value` 컬럼의 내부 `text` 포맷으로 스캔합니다. 바이너리처럼 보이는 입력만 `Unsupported file format`으로 기록합니다.
- 0바이트 빈 파일과 0바이트 archive entry는 포맷 판별과 오류 리포트 대상에서 제외하고 skip합니다.
- 실행 옵션으로 pre-scan, group, file fallback 병렬도를 조정할 수 있으며, pre-scan 병렬도는 파일 확장/포맷 판별과 그룹별 schema split에 적용됩니다.
- 실행 옵션으로 row sampling(`--sample-ratio`)과 group batch scan용 file sampling(`--file-sample-ratio`)을 구분해 조정할 수 있습니다.
- `--file-sample-ratio`는 그룹 내 파일을 균등 무작위로 추출하며, 함께 `--sample-ratio < 1.0`이 주어지면 batch group scan에서는 row sampling을 무시하고 경고 로그를 남깁니다.
- driver 로그 레벨은 `PRIVYSPARK_DEBUG` / `privyspark.debug`로 `error`, `warn`, `info`, `debug`를 설정할 수 있고, 기본값은 `warn`입니다. `true`/`false`는 각각 `debug`/`warn`으로 해석됩니다.
- 저장소에는 현재 입력 처리 분기를 재현하는 샘플 데이터셋 번들과 재생성 task(`generateSampleDatasets`)를 포함할 수 있습니다.
- release 자산에는 샘플 데이터셋 번들을 압축한 `privyspark-<tag>-sample-datasets.zip`를 포함할 수 있습니다.
- 탐지는 ruleset 기반 regex + 일부 타입의 strict validator 조합입니다.
- 출력은 Parquet + CSV 2종이며 `scan_results`, `scan_errors`를 함께 생성합니다.
- 일부 파일/그룹 실패는 전체 작업을 중단시키지 않고 누적 기록합니다.

## 상세 문서 맵
- 실행, 샘플링, 병렬도, 릴리즈: [mvp/execution-and-operations.md](mvp/execution-and-operations.md)
- 입력 포맷, archive/xlsx, magic-byte 판별, 그룹화: [mvp/input-normalization.md](mvp/input-normalization.md)
- ruleset, 탐지 타입, `match_type`, 집계 fallback: [mvp/detection-and-rules.md](mvp/detection-and-rules.md)
- 출력 스키마, `file_identifier`, 오류 리포트: [mvp/reporting-and-failures.md](mvp/reporting-and-failures.md)
- 전체 MVP 문서 인덱스: [mvp/README.md](mvp/README.md)

## Acceptance Criteria
1. 절대경로/URI 검증이 동작하고 상대경로 입력은 거부됩니다.
2. 현재 지원 포맷과 unknown input의 magic-byte/text fallback 처리 경로가 소스 기준으로 문서화돼 있습니다.
3. ruleset 기반 탐지와 결과 필드(`match_count`, `match_ratio`, `confidence`) 의미가 문서화돼 있습니다.
4. 일부 파일 실패 시 오류 리포트를 남기고 나머지 처리를 계속하는 동작이 문서화돼 있습니다.
5. file sampling이 필요한 배경과 row sampling 무시 조건이 문서화돼 있습니다.
