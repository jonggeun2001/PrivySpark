# 제품 개요

PrivySpark는 Spark 기반 배치 스캐너로, 지정한 데이터 경로에서 잠재적 개인정보를 탐지하고 집계된 결과 리포트와 오류 리포트를 생성합니다.

## 지원 범위
- 실행 명령은 `privyspark scan` 단일 진입점입니다.
- 입력 경로는 절대경로 또는 URI만 허용합니다.
- 지원 포맷은 `csv`, `json/jsonl/ndjson`, `parquet`, `orc`, `avro`, `xlsx`와 archive 계열 `zip`, `jar`, `tar`, `tar.gz/tgz`, `tar.bz2/tbz2`, `tar.xz/txz`, `tar.zst/tzst`, `7z`, `rar`입니다.
- `gzip`, `bzip2`, `xz`, `zstd`로 감싼 direct data file은 원본 경로를 그대로 Spark/Hadoop reader에 전달합니다.
- 무확장자 파일과 미지원 확장자 파일은 `parquet`/`orc` 매직바이트를 우선 검사하고, 텍스트처럼 보이면 내부 `text` 포맷으로 정규화해 스캔합니다.
- 바이너리처럼 보이는 미지원 입력만 `Unsupported file format` 오류로 기록합니다.
- `--ignore`, `--ignore-file`은 파일명 또는 입력 루트 기준 상대 경로로 스캔 제외 대상을 정의합니다.

## 탐지 모델
- 탐지는 ruleset 기반 regex 결과를 그대로 사용합니다.
- invalid regex는 ruleset 로드 단계에서 즉시 거부합니다.
- 집계 결과는 `match_count`, `sampled_row_count`, `match_ratio`, `non_empty_match_ratio`, `confidence`, `sample_raw_value`, `sample_matched_fragment`를 포함합니다.
- `sample_raw_value`는 매치가 발생한 셀의 전체 원문이 아니라, 매치 조각 기준 앞뒤 최대 50자 문맥만 저장합니다.

## 샘플링과 스캔 단위
- `--sample-ratio`는 row sampling입니다.
- `--file-sample-ratio`는 batch scan과 file fallback scan에서 그룹 내 파일을 균등 무작위로 추출합니다.
- file sampling은 그룹 파일 수가 `--file-sample-min-files`보다 클 때만 적용합니다.
- file sampling을 별도 옵션으로 분리한 이유는 row sampling 의미를 유지하면서도 작은 파일이 많은 입력에서 읽는 파일 수를 줄이고, 특정 데이터가 한 파일에 몰릴 수 있다는 운영 우려를 파일 단위로 반영하기 위해서입니다.

## 결과물
- 결과 리포트: `scan_results`
- 오류 리포트: `scan_errors`
- 출력 형식: Parquet + CSV
- 긴 스캔에서는 `<output>/_progress/<run_id>` 아래에 중간 JSONL shard가 기록될 수 있지만, 최종 소비 경로는 아닙니다.

## 샘플 데이터셋
- 입력 처리 케이스 번들은 [../../../samples/input-cases/README.md](../../../samples/input-cases/README.md)에 있습니다.
- 재생성 명령은 `./gradlew generateSampleDatasets`입니다.

## 다음 문서
- 입력 포맷과 그룹화: [input-formats.md](input-formats.md)
- ruleset과 탐지 제약: [rules-and-detection.md](rules-and-detection.md)
- 결과/오류 리포트: [reports-and-errors.md](reports-and-errors.md)
