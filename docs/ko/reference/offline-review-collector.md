# Offline Review Collector

이 문서는 서버 없이 `review.html`을 담당자에게 전달하고, 회수한 response JSON으로 누적 review state를 운영하는 흐름을 설명합니다.

## 운영 흐름

1. 스캔 실행 시 공통 review state root를 지정합니다.

```bash
privyspark scan \
  --path hdfs:///user/username \
  --output hdfs:///privyspark/output/20260430 \
  --review-state-root hdfs:///review-state-root
```

2. 담당자는 `<scan-output>/review/review.html` 또는 `--review-html-dir`에 생성된 `review.html`에서 각 finding을 오탐/정탐으로 판정합니다.

3. 다운로드한 `response-<scan-path>-YYYYMMDD-HHMMSS.json`을 `<review-state-root>/inbox/*.json`에 업로드합니다.

4. collector를 실행합니다.

```bash
privyspark review collect \
  --review-state-root hdfs:///review-state-root
```

5. 다음 스캔은 같은 `--review-state-root`를 지정합니다. 오탐은 recurring allowlist에 매칭되면 결과에서 제외되고, 정탐은 제외하지 않고 조치 상태만 누적됩니다.

`review collect`는 response JSON 자체에 포함된 컨텍스트를 사용합니다. `--scan-results`는 더 이상 필요하지 않으며, 지정해도 recurring 수집 판단에는 사용하지 않습니다.

## State 구조

collector는 `<review-state-root>/current` 아래 파일을 갱신합니다.

```text
review-state-root/
  inbox/
    response-*.json
  current/
    allowlist.jsonl
    action_plan.jsonl
    finding_status.jsonl
    response_ledger.jsonl
```

- `allowlist.jsonl`: 오탐으로 확정된 recurring 제외 규칙
- `action_plan.jsonl`: 정탐 조치 계획
- `finding_status.jsonl`: 최근 수집 응답과 기존 조치 계획의 상태 요약
- `response_ledger.jsonl`: 수집된 응답 감사 로그

`scan`은 `current/allowlist.jsonl`만 suppress 판단에 사용합니다. `action_plan.jsonl`은 finding을 숨기지 않습니다.

## Recurring 오탐 기준

exact fingerprint/CRC 기반 allowlist는 지원하지 않습니다. 매일 교체되는 배치 파일처럼 파일 크기, mtime, checksum이 바뀌는 데이터에서도 같은 논리 컬럼 오탐을 제외하기 위해 recurring 기준만 사용합니다.

Hive 매핑이 있으면 다음 키로 매칭합니다.

```text
normalized_scan_path + hive_table_fqn + column_name + pii_type
```

Hive 매핑이 없으면 다음 키로 매칭합니다.

```text
normalized_scan_path + file_identifier_pattern + column_name + pii_type
```

HDFS URI는 path slash 개수를 정규화합니다. 예를 들어 `hdfs:///user/name`과 `hdfs:////user/name`은 같은 scan path로 취급합니다.

## 오탐 응답 예시

```json
{
  "schema_version": 1,
  "scan_path": "hdfs:///user/username",
  "responder": "owner@example.com",
  "responded_at": "2026-04-30T10:00:00Z",
  "responses": [
    {
      "finding_key": "sha256:...",
      "finding_hash": "sha256:...",
      "file_identifier": "daily/customers/part-000.parquet",
      "hive_database": "mart",
      "hive_table": "customers",
      "hive_table_fqn": "mart.customers",
      "column_name": "test_email",
      "pii_type": "email",
      "sample_row_count": 1000,
      "match_count": 12,
      "non_empty_match_ratio": 0.12,
      "decision": "false_positive",
      "false_positive_reason": "테스트 계정 이메일 컬럼",
      "expires_at": "2026-12-31"
    }
  ]
}
```

수집 후 `allowlist.jsonl`에는 다음처럼 저장됩니다.

```json
{"entry_type":"recurring","scan_path":"hdfs:///user/username","hive_table_fqn":"mart.customers","file_identifier_pattern":"","column_name":"test_email","pii_type":"email","reason":"테스트 계정 이메일 컬럼","reviewer":"owner@example.com","reviewed_at":"2026-04-30T10:00:00Z","expires_at":"2026-12-31","source_finding_key":"sha256:...","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}
```

다음 스캔에서 같은 `scan_path`, `hive_table_fqn`, `column_name`, `pii_type`가 검출되면 파일 checksum이 달라도 제외됩니다. `expires_at`이 지난 항목은 적용하지 않습니다.

Hive 매핑이 없으면 `file_identifier_pattern`을 사용합니다.

```json
{"entry_type":"recurring","scan_path":"hdfs:///user/username","hive_table_fqn":"","file_identifier_pattern":"daily/customers/*.parquet","column_name":"test_email","pii_type":"email","reason":"반복 생성되는 테스트 데이터","reviewer":"owner@example.com","reviewed_at":"2026-04-30T10:00:00Z","expires_at":"2026-12-31","source_finding_key":"sha256:...","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}
```

## 정탐 응답 예시

```json
{
  "schema_version": 1,
  "scan_path": "hdfs:///user/username",
  "responder": "owner@example.com",
  "responded_at": "2026-04-30T10:00:00Z",
  "responses": [
    {
      "finding_key": "sha256:...",
      "finding_hash": "sha256:...",
      "file_identifier": "daily/customers/part-000.parquet",
      "hive_database": "mart",
      "hive_table": "customers",
      "hive_table_fqn": "mart.customers",
      "column_name": "customer_phone",
      "pii_type": "phone_number",
      "sample_row_count": 1000,
      "match_count": 830,
      "non_empty_match_ratio": 0.83,
      "decision": "true_positive",
      "action_plan": "마스킹 적용",
      "action_due_date": "2026-05-15"
    }
  ]
}
```

정탐은 `action_plan.jsonl`에 누적되지만 다음 스캔에서 숨기지 않습니다. 같은 finding이 계속 검출되면 review 화면 또는 state 파일에서 조치 계획을 확인할 수 있습니다.

## 검증 규칙

collector는 response JSON에 대해 다음을 검증합니다.

- `schema_version`은 `1`
- envelope의 `scan_path`, `responder`, `responded_at`, `responses`는 필수
- `responded_at`은 ISO-8601 instant
- 각 response의 `finding_key`, `column_name`, `pii_type`, `decision`은 필수
- 오탐은 `false_positive_reason`, `expires_at` 필수
- Hive 매핑이 없는 오탐은 `file_identifier_pattern` 또는 `file_identifier` 필수
- 정탐은 `action_plan`, `action_due_date` 필수
- `expires_at`, `action_due_date`는 `YYYY-MM-DD`
- `allowlist_scope=exact` 등 recurring이 아닌 scope는 거부

## review.html

`review.html`은 self-contained HTML입니다. 서버 호출 없이 브라우저에서 열고 응답 JSON을 다운로드합니다.

표는 경로, Hive 테이블, 컬럼명, 개인정보 유형, 샘플 행 수, 검출 건수, 비어있지 않은 값 대비 검출 비율, 샘플, 판정, 오탐 사유, 오탐 만료일, 정탐 조치 계획, 조치 예정일을 분리된 컬럼으로 표시합니다.

오탐 선택 시에는 recurring 응답만 생성합니다. exact/pattern 선택지는 표시하지 않습니다. 정탐 조치 예정일은 오늘부터 30일 이내만 선택할 수 있습니다.

## 기존 review apply와의 관계

`privyspark review apply`는 사람이 편집한 `scan_results`에서 legacy exact allowlist 파일을 만드는 단일 파일 워크플로우입니다. recurring-only offline review state에서는 `review collect`를 사용합니다.

현재 scan의 offline review suppress 판단은 `entry_type=recurring` state만 적용합니다. legacy exact entry는 suppress 판단에 사용하지 않습니다.
