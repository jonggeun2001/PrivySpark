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

2. 담당자는 `<scan-output>/review/review.html` 또는 `--review-html-dir`에 생성된 `review.html`에서 응답자사번을 입력하고 각 finding을 오탐/정탐으로 판정합니다. Excel 검토가 필요하면 `엑셀 편집용 CSV 다운로드`로 CSV를 내려받아 판정/사유/계획/예정일을 편집한 뒤 `복호화한 CSV 불러오기`로 다시 가져오거나, Excel에서 전체 복사한 TSV 클립보드 내용을 붙여넣어 반영합니다. 사내 보안 솔루션이 CSV 파일을 암호화한 경우 암호화 해제한 CSV 파일을 임포트해야 합니다.

3. 다운로드한 `response-<scan-path>-YYYYMMDD-HHMMSS.json`을 `<review-state-root>/inbox/*.json`에 업로드합니다.

4. 다음 스캔을 같은 `--review-state-root`로 실행합니다. scan 명령은 본 스캔을 시작하기 전에 `<review-state-root>/inbox/*.json`을 자동 수집해 `<review-state-root>/current`를 갱신합니다. 오탐은 recurring allowlist에 매칭되면 결과에서 제외되고, 정탐은 제외하지 않고 조치 상태만 누적됩니다.

`review collect`는 response JSON 자체에 포함된 컨텍스트를 사용합니다. `--scan-results`는 더 이상 필요하지 않으며, 지정해도 recurring 수집 판단에는 사용하지 않습니다.

## State 구조

collector는 `<review-state-root>/current` 아래 파일을 갱신합니다.

```text
review-state-root/
  .collect.lock
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

`scan --review-state-root`와 `review collect`는 state 갱신 중 `<review-state-root>/.collect.lock`을 생성합니다. 이미 lock 파일이 있으면 동시 갱신을 막기 위해 명령이 실패합니다. 수집이 정상 종료되거나 검증 실패로 중단되면 lock 파일은 삭제됩니다.

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
  "responder": "owner1",
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
      "expires_at": "9999-12-31"
    }
  ]
}
```

`review.html`은 오탐 만료일 입력란을 표시하지 않고, 영구 반복 제외를 나타내는 내부값 `9999-12-31`을 자동으로 넣습니다.

수집 후 `allowlist.jsonl`에는 다음처럼 저장됩니다.

```json
{"entry_type":"recurring","scan_path":"hdfs:///user/username","hive_table_fqn":"mart.customers","file_identifier_pattern":"","column_name":"test_email","pii_type":"email","reason":"테스트 계정 이메일 컬럼","reviewer":"owner1","reviewed_at":"2026-04-30T10:00:00Z","expires_at":"9999-12-31","source_finding_key":"sha256:...","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}
```

다음 스캔에서 같은 `scan_path`, `hive_table_fqn`, `column_name`, `pii_type`가 검출되면 파일 checksum이 달라도 제외됩니다. 수동으로 작성한 state에서 `expires_at`이 지난 항목은 적용하지 않습니다.

Hive 매핑이 없으면 `file_identifier_pattern`을 사용합니다.

```json
{"entry_type":"recurring","scan_path":"hdfs:///user/username","hive_table_fqn":"","file_identifier_pattern":"daily/customers/*.parquet","column_name":"test_email","pii_type":"email","reason":"반복 생성되는 테스트 데이터","reviewer":"owner1","reviewed_at":"2026-04-30T10:00:00Z","expires_at":"9999-12-31","source_finding_key":"sha256:...","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}
```

## 정탐 응답 예시

```json
{
  "schema_version": 1,
  "scan_path": "hdfs:///user/username",
  "responder": "owner1",
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

정탐은 `action_plan.jsonl`에 누적되지만 다음 스캔에서 숨기지 않습니다. 같은 finding이 계속 검출되면 `review.html`의 `기존 조치 상태` 컬럼에 이전 조치 계획, 예정일, 응답자사번이 표시됩니다. 예를 들어 기존 계획이 `삭제 처리`이고 예정일이 지나지 않았으면 `삭제 조치 필요`로 보이고, 예정일이 지났으면 `조치 기한 초과`로 표시됩니다.

## 검증 규칙

collector는 response JSON에 대해 다음을 검증합니다.

- `schema_version`은 `1`
- envelope의 `scan_path`, `responder`, `responded_at`, `responses`는 필수
- `responded_at`은 ISO-8601 instant
- 각 response의 `finding_key`, `column_name`, `pii_type`, `decision`은 필수
- 오탐은 `false_positive_reason`, `expires_at` 필수. `review.html`은 `expires_at`을 영구 반복 제외 내부값 `9999-12-31`로 자동 생성
- 신규 recurring 오탐의 `column_name`, `pii_type`은 exact 값만 허용하며 `*` wildcard는 거부
- Hive 매핑이 없는 오탐은 `file_identifier_pattern` 또는 `file_identifier` 필수
- 정탐은 `action_plan`, `action_due_date` 필수
- `expires_at`, `action_due_date`는 `YYYY-MM-DD`
- `allowlist_scope=exact` 등 recurring이 아닌 scope는 거부

`review.html`이 생성하는 response JSON은 운영자가 회수 파일을 해석할 수 있도록 `sample_matched_fragment`, `sample_raw_value` 보조 필드를 함께 담습니다. collector는 이 두 필드를 review state 판단에 사용하지 않으며, `review-response-viewer.html`에서 샘플/추출값 표시용으로 사용합니다.

invalid response가 하나라도 있으면 collector는 `<review-state-root>/current`를 갱신하지 않고 실패합니다. `scan --review-state-root`에서 자동 수집 중 같은 실패가 발생하면 스캔 본 작업을 시작하지 않습니다.

## review.html

`review.html`은 self-contained HTML입니다. 서버 호출 없이 브라우저에서 열고 응답 JSON을 다운로드합니다. 파일별 최대 크기는 2MB로 고정되며, 탐지가 많아 이 크기를 넘으면 `review.html`은 part 목록 인덱스가 되고 실제 검토 화면은 `review-part-0001.html`, `review-part-0002.html`처럼 분할됩니다. 각 part 파일은 자기 범위의 finding만 담으므로 각 파일에서 응답 JSON을 따로 생성해 모두 `<review-state-root>/inbox`에 제출합니다.

표는 경로, Hive 테이블, 컬럼명, 개인정보 유형, 샘플 행 수, 검출 건수, `검출비율(%)`, `검출샘플(검출값/데이터)`, 판정, 기존 조치 상태, 오탐 사유, 정탐 조치 계획, 조치 예정일을 분리된 컬럼으로 표시합니다. Hive 매핑이 있는 finding은 같은 `hive_table_fqn`, 컬럼, 개인정보 유형이면 파티션/파일별로 반복 표시하지 않고 한 행으로 묶습니다. 이때 경로는 파티션 세그먼트를 제거한 테이블 루트로 보이며, 경로 옆 배지는 묶인 파티션 수와 파일 수를 표시합니다. 검출 비율은 `검출 건수 / 샘플 행 수 * 100`으로 계산해 소수점 둘째 자리까지 표시하고, 검출 샘플은 검출값과 원본 데이터 컨텍스트를 실제 줄바꿈으로 분리합니다.

Excel 편집이 필요하면 `review.html`에서 CSV를 다운로드해 편집한 뒤 다시 불러오거나, Excel에서 전체 복사한 TSV 클립보드 내용을 붙여넣습니다. CSV/TSV 임포트는 `finding_key` 기준으로 판정, 오탐 사유, 정탐 조치 계획, 조치 예정일만 반영하며, 정렬 순서가 달라도 됩니다. Hive 테이블 단위로 묶인 행은 새 `finding_key`를 사용하므로 이전에 내려받은 CSV/TSV는 새 리뷰 파일에 재사용하지 않습니다. 사내 보안 솔루션이 CSV 저장 파일을 암호화하는 환경에서는 암호화 해제한 CSV 파일을 임포트해야 합니다. CSV 파일 업로드는 따옴표로 감싼 쉼표와 줄바꿈을 셀 내용으로 유지하고, TSV 붙여넣기는 탭과 줄바꿈을 기준으로 반영합니다. Excel이 줄바꿈 포함 셀을 큰따옴표로 감싼 경우 줄바꿈은 셀 내용으로 유지됩니다.

오탐 선택 시에는 recurring 응답만 생성합니다. exact/pattern 선택지와 오탐 만료일 입력란은 표시하지 않습니다. 응답자사번이 비어 있거나 소문자 영어/숫자 외 문자를 포함하면 response JSON을 생성하지 않고 입력란에 포커스합니다. 정탐 조치 예정일은 오늘부터 30일 이내만 선택할 수 있습니다.

## 기존 review apply와의 관계

`privyspark review apply`는 사람이 편집한 `scan_results`에서 legacy exact allowlist 파일을 만드는 단일 파일 워크플로우입니다. recurring-only offline review state에서는 `review collect`를 사용합니다.

현재 scan의 offline review suppress 판단은 `entry_type=recurring` state만 적용합니다. legacy exact entry는 suppress 판단에 사용하지 않습니다.
