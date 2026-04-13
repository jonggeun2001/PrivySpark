# 샘플 입력 케이스 번들

이 디렉토리는 현재 소스 기준 입력 처리 branches를 재현하는 샘플 데이터셋 번들입니다.

- 생성기 엔트리: `io.github.jonggeun2001.privyspark.SampleDatasetGenerator`
- 재생성 명령: `./gradlew generateSampleDatasets`
- 생성 산출물:
  - `sample-rules.yaml`: 샘플 검증용 최소 ruleset
  - `scenario-manifest.tsv`: 케이스별 기대 결과/오류 manifest
  - `files/`: 실제 스캔 대상 파일/디렉토리

`files/` 아래 binary 파일은 생성기로 다시 만들어지는 산출물이므로 수동 편집하지 않습니다.

## 사용 방법

단일 케이스를 스캔하려면 `scenario-manifest.tsv`의 `relative_path` 값을 입력 경로로 사용합니다.

예시:

```bash
ROOT="$(pwd)/samples/input-cases"
CASE_PATH="$ROOT/files/flat/csv/customers.csv"

bin/privyspark-submit \
  scan \
  --path "$CASE_PATH" \
  --output /abs/output \
  --ruleset "$ROOT/sample-rules.yaml" \
  --sample-ratio 1.0
```

## 포함 케이스

| case_id | path | 기대 동작 |
| --- | --- | --- |
| `flat_csv` | `files/flat/csv/customers.csv` | flat CSV detect |
| `flat_json` | `files/flat/json/records.json` | flat JSON detect |
| `flat_jsonl` | `files/flat/jsonl/events.jsonl` | flat JSONL detect |
| `flat_ndjson` | `files/flat/ndjson/events.ndjson` | flat NDJSON detect |
| `flat_parquet` | `files/flat/parquet/contacts.parquet` | flat Parquet detect |
| `flat_orc` | `files/flat/orc/contacts.orc` | flat ORC detect |
| `flat_avro` | `files/flat/avro/contacts.avro` | flat Avro detect |
| `flat_xlsx` | `files/flat/xlsx/contacts.xlsx` | workbook sheet detect, empty sheet skip |
| `flat_text_extension` | `files/flat/text-extension/notes.log` | unsupported extension text fallback |
| `flat_text_no_extension` | `files/flat/text-no-extension/notes` | extensionless text fallback |
| `flat_extensionless_parquet` | `files/flat/extensionless-parquet/payload` | extensionless Parquet magic-byte detect |
| `flat_extensionless_orc` | `files/flat/extensionless-orc/payload` | extensionless ORC magic-byte detect |
| `flat_parq_alias` | `files/flat/parquet-alias/contacts.parq` | unsupported extension + Parquet magic-byte detect |
| `archive_zip_mixed` | `files/archive/mixed.zip` | zip 내부 csv + extensionless parquet + text fallback + zero-byte entry skip |
| `archive_jar_jsonl` | `files/archive/mixed.jar` | jar 내부 jsonl detect |
| `archive_zero_byte_entry` | `files/archive/zero-byte-entry.zip` | archive 내부 zero-byte entry skip |
| `edge_zero_byte_sibling` | `files/edge/zero-byte-sibling` | top-level zero-byte sibling skip |
| `edge_unsupported_binary` | `files/edge/unsupported-binary/Bytecode.class` | unsupported binary error |
| `edge_broken_workbook` | `files/edge/broken-workbook/broken.xlsx` | broken workbook error |
| `edge_nested_archive` | `files/edge/nested-archive/nested.zip` | nested archive reject |
| `edge_unsafe_archive` | `files/edge/unsafe-archive/unsafe.zip` | unsafe archive path reject |

기대 결과 건수와 오류 메시지 fragment는 `scenario-manifest.tsv`를 기준으로 검증합니다.
