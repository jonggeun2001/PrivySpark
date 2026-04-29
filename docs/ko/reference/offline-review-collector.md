# 오프라인 리뷰와 누적 Collector

## 목적
이 문서는 서버를 새로 구동하지 않고 개인정보 검출 결과를 담당자에게 검토받는 흐름을 정의합니다.

핵심 목표는 다음과 같습니다.
- 스캔 결과 output 영역에는 검토용 `review.html`만 추가로 생성합니다.
- 담당자는 `review.html`을 로컬 브라우저에서 열어 검출 샘플을 확인하고 response JSON을 생성합니다.
- 사내 시스템은 `review.html` 전달과 response JSON 회수만 담당합니다.
- PrivySpark collector는 회수된 response JSON을 읽어 누적 review state를 갱신합니다.
- 다음 스캔은 누적 review state의 allowlist를 적용합니다.

## 비목표
- PrivySpark가 담당자 알림 메일, 사내 결재, 사내 티켓 시스템을 직접 운영하지 않습니다.
- 스캔 output 아래에 response/state를 누적 저장하지 않습니다.
- 담당자가 사람이 읽는 `task_id`, `campaign_id`, `target_id`를 직접 관리하지 않습니다.
- 정탐은 suppress하지 않습니다. 정탐은 조치 계획 추적 대상으로만 관리합니다.

## 출력 경로 원칙
스캔 output은 기본적으로 기존 리포트와 검토용 HTML만 포함합니다.

```text
<scan-output>/
  parquet/
    scan_results
    scan_errors
  csv/
    scan_results
    scan_errors
  excel/
    scan_results.xlsx
    scan_errors.xlsx
  review/
    review.html
```

`review.html`은 담당자 입력 도구입니다. response JSON과 collector state는 이 경로에 저장하지 않습니다.

scan output과 HTML 전달 경로를 분리해야 하는 운영 환경에서는 scan 실행 시 `--review-html-dir <ABS_PATH_OR_URI>`를 지정합니다. 이때 HTML은 지정한 디렉토리의 `review.html`로 생성되고 기본 `<scan-output>/review/review.html`은 만들지 않습니다.

누적 response와 state는 별도 root에서 관리합니다.

```text
<review-state-root>/
  inbox/
    response-001.json
    response-002.json
  current/
    allowlist.jsonl
    action_plan.jsonl
    finding_status.jsonl
    response_ledger.jsonl
```

운영자는 이번 스캔의 `scan_results` 경로와 누적 `<review-state-root>`만 지정합니다.

```bash
privyspark review collect \
  --scan-results <scan-output>/parquet/scan_results \
  --review-state-root <review-state-root>
```

다음 스캔은 같은 누적 state root를 사용합니다.

```bash
privyspark scan \
  --path <scan-path> \
  --output <next-scan-output> \
  --review-state-root <review-state-root>
```

`--review-state-root`를 받은 scan은 내부적으로 `<review-state-root>/current/allowlist.jsonl`을 적용하고, 기본 `<scan-output>/review/review.html`을 추가 생성합니다. `--review-html-dir`를 지정하면 해당 디렉토리의 `review.html`에 HTML을 생성합니다. `action_plan.jsonl`은 suppress에 쓰지 않고 collector의 `finding_status.jsonl` 계산에 사용합니다.

## 식별자 정책
사람이 관리하는 별도 campaign/task ID를 만들지 않습니다.

기준 입력은 실제 스캔 대상 경로입니다.

```text
scan_path = scan CLI의 --path 값
file_identifier = scan_results.file_identifier
```

collector와 HTML 생성기는 `scan_path`를 정규화한 뒤 finding key를 계산합니다.

```text
finding_key = sha256(
  normalized_scan_path + "|" +
  file_identifier + "|" +
  column_name + "|" +
  pii_type
)
```

`finding_key`는 스캔 차수를 넘어 같은 검출 항목을 추적하기 위한 안정 키입니다.

구버전 HTML로 만든 응답을 거부하기 위해 현재 검출 증거 요약도 해시합니다.

```text
finding_hash = sha256(
  finding_key + "|" +
  streaming_digest(sorted evidence file_identifier, file_size, file_mtime_epoch_ms, checksum, scan_timestamp, match metrics)
)
```

`finding_hash`는 같은 테이블/컬럼/PII 타입이라도 현재 검출 scope나 검출량/비율이 바뀌었는지 확인하는 값입니다. response JSON의 `finding_hash`가 현재 `scan_results`에서 재계산한 값과 다르면 collector는 해당 응답을 reject합니다.

`scan_results_fingerprint`는 HTML이 어떤 scan result에서 생성됐는지 확인하는 run-level fingerprint입니다.

```text
scan_results_fingerprint = sha256(
  sorted(finding_key, finding_hash)
)
```

collector는 response JSON의 `scan_results_fingerprint`가 현재 `--scan-results`에서 재계산한 값과 다르면 해당 response 파일 전체를 reject합니다.

## Review HTML
`review.html`은 self-contained HTML 파일입니다. 서버 호출 없이 로컬 브라우저에서 열 수 있어야 합니다.

GitHub Release에는 `privyspark-<tag>-review-response-example.html` 예시 파일을 함께 제공합니다. 이 파일은 더미 finding으로 response JSON 다운로드 흐름을 확인하기 위한 샘플이며, 실제 운영 검토에는 각 스캔이 생성한 `<scan-output>/review/review.html`을 사용합니다.

GitHub Release에는 `privyspark-<tag>-review-response-viewer.html`도 함께 제공합니다. 운영자는 회수한 `response-YYYYMMDD-HHMMSS.json`을 이 파일로 로컬에서 열어 envelope 메타데이터, schema/fingerprint 유무, finding별 판정과 allowlist/action plan 입력값을 확인할 수 있습니다. 이 파일은 JSON 확인용이며 collector state를 갱신하지 않습니다.

HTML에는 현재 `scan_results`에서 만든 finding 목록과 검출 샘플을 포함합니다. 담당자는 테이블 헤더를 클릭해 finding 목록을 정렬할 수 있고, 같은 헤더를 다시 클릭하면 정렬 방향이 반전됩니다. 같은 `file_identifier` 경로에서 여러 PII가 검출되더라도 개인정보 유형별 판정이 다를 수 있으므로 각 finding은 별도 row로 표시합니다.

브라우저 렌더링 비용을 낮추기 위해 `review.html`은 입력 상태를 DOM이 아닌 페이지 내부 상태에 보관하고, 화면 근처의 row만 실제 입력 필드로 hydrate합니다. 화면 밖 row는 가벼운 placeholder로 유지되지만, 입력한 판정/사유/조치 계획은 response JSON 생성 시 전체 finding 기준으로 포함됩니다. 정렬과 일괄 삭제 계획 적용도 이 내부 상태를 기준으로 처리하므로 화면에 보이지 않는 row의 입력값이 누락되지 않습니다.

결정 값은 다음 중 하나입니다.
- `false_positive`: 실제 개인정보가 아니므로 다음 스캔에서 suppress합니다.
- `true_positive`: 실제 개인정보이므로 조치 계획을 등록합니다.

HTML 표는 검토자가 빠르게 훑을 수 있도록 주요 값을 별도 컬럼으로 보여줍니다.
- `경로`, `Hive 테이블`, `컬럼명`, `개인정보 유형`: 검출 위치와 PII 타입을 각각 독립 컬럼으로 표시합니다. `개인정보 유형`은 `email`, `driver_license_number` 같은 내부 값을 `이메일`, `운전면허번호`처럼 한글명으로 표시합니다. `finding_key`는 response 생성과 검증에는 사용하지만 화면에는 hidden 값으로만 보관합니다.
- `샘플 행 수`, `검출 건수`, `검출 비율`: 검토 판단에 필요한 핵심 지표를 개별 컬럼으로 표시합니다. `confidence`는 response JSON 검증용 데이터에는 남지만 표의 지표 컬럼에는 노출하지 않습니다.
- `판정`: 오탐/정탐 판정만 선택합니다.
- `오탐 제외 범위`: 오탐일 때만 `exact` 또는 `pattern`을 선택합니다.
- `오탐 사유`, `경로 패턴`, `컬럼명 패턴`, `개인정보 유형 패턴`, `패턴 만료일`, `정탐 조치 계획`, `조치 예정일`: 기존 사유/계획 입력 영역을 필드별 컬럼으로 나눠 입력합니다.

반복되는 입력 힌트는 row마다 표시하지 않고 표 상단의 `검토 안내`에 한 번만 표시합니다. 개인정보 유형 패턴은 한글명 또는 원본 `pii_type` 값으로 입력할 수 있으며, HTML은 다운로드 시 한글명을 원본 `pii_type` 값으로 변환해 collector 스키마를 유지합니다. 여러 파일 증거가 있는 finding을 `pattern` 오탐으로 처리할 때 경로 패턴을 비우면 HTML이 response JSON 생성을 막아 collector reject를 줄입니다.

판정 선택 전에는 사유/계획 입력 영역을 숨깁니다. `false_positive`를 선택하면 오탐 사유와 pattern 필드만 표시하고, `true_positive`를 선택하면 조치 계획과 조치 예정일만 표시합니다.

삭제 처리 대상 정탐이 여러 건이면 상단의 삭제 예정일을 입력한 뒤 `일괄 삭제 계획 등록`을 누릅니다. 이 버튼은 현재 `true_positive`로 선택된 finding에 `action_plan=삭제 처리`와 입력한 조치 예정일을 채웁니다.

오탐 입력 필수값:
- 오탐 사유
- allowlist scope: `exact` 또는 `pattern`
- `pattern`인 경우 `file_identifier_pattern`, `column_name_pattern`, `pii_type_pattern` 중 하나 이상과 만료일

정탐 입력 필수값:
- 조치 계획
- 조치 예정일
- 담당자 또는 응답자 식별자

## 검출 샘플 표시
담당자가 판단할 수 있도록 `review.html`은 검출 샘플을 보여줍니다.

finding 요약에는 다음 필드를 표시합니다.
- `file_identifier`
- `hive_table_fqn`
- `hive_database`
- `hive_table`
- `column_name`
- `pii_type`
- `sampled_row_count`
- `match_count`
- `non_empty_match_ratio`

`finding_key`, `finding_hash`, `match_ratio`, `confidence`는 HTML 내부 데이터와 response 검증에는 사용하지만 검토 표의 일반 표시 컬럼으로는 노출하지 않습니다.

상세 영역에는 evidence sample을 표시합니다.
- `file_identifier`
- `sample_matched_fragment`
- `sample_raw_value`
- `match_count`
- `confidence`

하나의 finding이 여러 파일에서 검출될 수 있으므로 HTML은 최대 N개의 샘플만 보여줍니다. 기본값은 5개를 권장합니다.

샘플 노출 원칙:
- HTML에는 담당자 판단을 위해 샘플을 포함합니다.
- 메일 본문에는 샘플을 넣지 않습니다.
- response JSON에는 샘플을 넣지 않습니다.
- 누적 state에는 샘플 원문을 저장하지 않습니다.
- 필요한 경우 `finding_key`로 현재 또는 과거 `scan_results`에서 샘플을 다시 조회합니다.

오프라인 리뷰 HTML 관련 scan 옵션은 다음과 같습니다.

```text
--review-sample-mode raw|masked|none
--review-html-dir <ABS_PATH_OR_URI>
```

권장 기본값은 `masked`입니다.
- `raw`: `scan_results`의 값을 그대로 표시합니다.
- `masked`: 가운데 일부를 마스킹해 표시합니다.
- `none`: 샘플을 표시하지 않습니다.

## Response JSON
담당자가 HTML에서 제출 버튼을 누르면 response JSON 파일을 다운로드합니다. HTML 파일 자체를 회신받아 파싱하지 않습니다.

response JSON은 하나의 파일 안에 여러 finding 응답을 담을 수 있는 envelope 구조입니다. 담당자가 HTML에서 여러 항목을 한 번에 검토하면 `responses` 배열에 여러 응답이 들어갑니다. 화면에 표시되는 개인정보 유형은 한글명이지만 response JSON의 `pii_type`과 `pii_type_pattern`은 collector가 쓰는 원본 타입 값을 유지합니다.

```json
{
  "schema_version": 1,
  "scan_path": "hdfs:///warehouse/project_db.db",
  "scan_results_fingerprint": "sha256...",
  "responder": "owner@example.com",
  "responded_at": "2026-04-27T10:00:00Z",
  "responses": [
    {
      "finding_key": "sha256...",
      "finding_hash": "sha256...",
      "file_identifier": "project_db/customer/part-000.parquet",
      "column_name": "customer_no",
      "pii_type": "driver_license_number",
      "decision": "false_positive",
      "false_positive_reason": "내부 주문번호 포맷이 운전면허번호 규칙과 충돌",
      "allowlist_scope": "exact",
      "file_identifier_pattern": null,
      "column_name_pattern": null,
      "pii_type_pattern": null,
      "expires_at": null,
      "action_plan": null,
      "action_due_date": null
    },
    {
      "finding_key": "sha256...",
      "finding_hash": "sha256...",
      "file_identifier": "project_db/customer/part-001.parquet",
      "column_name": "email",
      "pii_type": "email",
      "decision": "true_positive",
      "false_positive_reason": null,
      "allowlist_scope": null,
      "file_identifier_pattern": null,
      "column_name_pattern": null,
      "pii_type_pattern": null,
      "expires_at": null,
      "action_plan": "컬럼 마스킹 적용 후 접근권한 재점검",
      "action_due_date": "2026-05-10"
    }
  ]
}
```

pattern allowlist response 예시는 다음과 같습니다.

```json
{
  "finding_key": "sha256...",
  "finding_hash": "sha256...",
  "file_identifier": "project_db/customer/part-000.parquet",
  "column_name": "temp_driver_no",
  "pii_type": "driver_license_number",
  "decision": "false_positive",
  "false_positive_reason": "temp_* 컬럼은 내부 테스트 식별자",
  "allowlist_scope": "pattern",
  "file_identifier_pattern": "project_db/customer/*",
  "column_name_pattern": "temp_*",
  "pii_type_pattern": "driver_license_number",
  "expires_at": "2026-07-31",
  "action_plan": null,
  "action_due_date": null
}
```

사내 시스템은 이 JSON 파일을 `<review-state-root>/inbox`에 업로드합니다. collector는 `inbox`의 모든 response를 읽되 현재 `--scan-results`와 맞는 응답만 처리합니다.

## Collector 동작
collector는 idempotent batch job이어야 합니다. 같은 `scan_results`와 같은 `inbox`로 여러 번 실행해도 같은 `current` state가 나와야 합니다.

처리 순서:
1. `--scan-results`를 읽어 현재 finding 목록을 재구성합니다.
2. 각 finding의 `finding_key`, `finding_hash`, `scan_results_fingerprint`를 계산합니다.
3. `<review-state-root>/inbox/*.json`을 읽습니다.
4. response JSON envelope schema와 `responses[]` 항목별 필수값을 검증합니다.
5. response의 `scan_path`, `scan_results_fingerprint`, `finding_key`, `finding_hash`가 현재 scan result와 맞는지 검증합니다.
6. 같은 finding에 여러 유효 응답이 있으면 envelope의 `responded_at`이 가장 최신인 응답을 채택합니다.
7. 기존 `<review-state-root>/current`와 새 응답을 merge합니다.
8. 새 state를 임시 경로에 쓴 뒤 원자적으로 `current`를 교체합니다.

reject 사유 예시:
- JSON 파싱 실패
- 알 수 없는 `schema_version`
- `responses`가 비어 있음
- `responder`가 비어 있음
- `responded_at`이 ISO-8601 instant가 아님
- 현재 scan result에 없는 `finding_key`
- `scan_results_fingerprint` mismatch
- `finding_hash` mismatch
- `false_positive`인데 `false_positive_reason`이 비어 있음
- `true_positive`인데 `action_plan` 또는 `action_due_date`가 비어 있거나 `action_due_date`가 `YYYY-MM-DD`가 아님
- `pattern` allowlist인데 `expires_at`이 비어 있음
- `pattern` allowlist인데 pattern 필드가 모두 비어 있음
- 금지된 wildcard 사용

## 오탐 반영
오탐은 allowlist로 누적합니다.

### Exact allowlist
`allowlist_scope=exact`는 기존 allowlist 의미를 유지합니다.

HTML에서는 일반 담당자 기본 선택지로 안내합니다. 단, finding의 fingerprint metadata가 완전하지 않으면 collector가 exact allowlist 응답을 거부하므로 HTML 힌트에 그 사실을 표시합니다.

매칭 기준:
- `dataset_path`
- `file_identifier`
- `column_name`
- `pii_type`
- `file_size`
- `file_mtime_epoch_ms`
- checksum

다음 스캔에서 모든 fingerprint가 일치하면 해당 finding은 `scan_results`에서 suppress됩니다. 파일 metadata 또는 checksum이 바뀌면 suppress하지 않고 재검토 대상으로 남깁니다.

### Pattern allowlist
`allowlist_scope=pattern`은 반복 오탐을 줄이기 위한 확장입니다.

HTML에서는 반복 오탐용 확장 선택지로 안내합니다. pattern은 fingerprint 검증을 하지 못하므로 `false_positive_reason`, `expires_at`, 하나 이상의 pattern 필드를 요구합니다. 여러 파일 증거가 있는 finding은 collector가 `file_identifier_pattern`도 요구할 수 있으므로 표 상단 검토 안내에서 pattern 입력 예시를 함께 제공합니다.

예시:

```json
{
  "entry_type": "pattern",
  "dataset_path": "hdfs:///warehouse/project_db.db",
  "file_identifier_pattern": "project_db/customer/*",
  "column_name_pattern": "temp_*",
  "pii_type_pattern": "driver_license_number",
  "reason": "temp_* 컬럼은 내부 테스트 식별자",
  "reviewer": "owner@example.com",
  "reviewed_at": "2026-04-27T10:00:00Z",
  "expires_at": "2026-07-31",
  "source_finding_key": "sha256..."
}
```

Wildcard 정책:
- `file_identifier_pattern`, `column_name_pattern`, `pii_type_pattern`에 `*`를 허용합니다.
- `dataset_path=*`는 금지합니다.
- `pii_type=*`는 기본 금지합니다. 필요하면 별도 운영자 승인 정책을 둡니다.
- pattern allowlist는 fingerprint 검증을 하지 못하므로 `reason`, `reviewer`, `expires_at`을 필수로 둡니다.
- `expires_at`이 지난 pattern entry는 다음 스캔에서 적용하지 않고 재검토 대상으로 남깁니다.

pattern allowlist의 `*`는 glob 의미입니다. 정규식으로 해석하지 않습니다.

## 정탐 반영
정탐은 allowlist에 넣지 않습니다. `action_plan.jsonl`에 누적합니다.

```text
action_plan.jsonl
- finding_key
- scan_path
- file_identifier
- hive_database
- hive_table
- hive_table_fqn
- column_name
- pii_type
- action_plan
- action_due_date
- responder
- responded_at
- status
```

다음 스캔에서 같은 `finding_key`가 다시 검출되면 기존 action plan과 연결합니다.

collector가 쓰는 `finding_status.jsonl`의 상태 계산:
- 다시 검출됨 + 예정일 전: `remediation_planned`
- 다시 검출됨 + 예정일 초과: `overdue`
- 더 이상 검출되지 않음: `remediated_candidate`
- 운영 확인 완료: `verified`

정탐 상태는 suppress에 영향을 주지 않습니다. 정탐은 계속 검출되어야 하며, 조치 완료 여부는 다음 스캔 결과와 action plan 비교로 판단합니다.

## 다음 스캔 반영
다음 스캔은 누적 state root를 입력으로 받습니다.

```bash
privyspark scan \
  --path hdfs:///warehouse/project_db.db \
  --output /scan/output/project_db_20260501 \
  --review-state-root /privyspark/review-state
```

scan은 `<review-state-root>/current/allowlist.jsonl`을 읽어 오탐을 suppress합니다.

정탐 action plan은 다음 용도로 사용합니다.
- collector의 `finding_status.jsonl`에서 예정일 초과 finding 표시
- 다음 collector 실행에서 더 이상 검출되지 않은 finding을 `remediated_candidate`로 표시

## 운영 절차
1. 스캔을 실행합니다.

   ```bash
   privyspark scan \
     --path <scan-path> \
     --output <scan-output> \
     --review-state-root <review-state-root>
   ```

2. 생성된 `<scan-output>/review/review.html` 또는 `--review-html-dir`로 지정한 디렉토리의 `review.html` 파일을 담당자에게 전달합니다.
3. 담당자는 HTML에서 검출 샘플을 확인하고 response JSON을 생성합니다. 같은 경로에 여러 개인정보 유형이 있어도 finding별 row에서 별도로 판정/사유/계획을 입력합니다. 여러 정탐 finding을 삭제 처리해야 하면 `true_positive`로 선택한 뒤 일괄 삭제 계획 등록으로 같은 삭제 계획을 채울 수 있습니다.
4. 사내 시스템은 response JSON을 `<review-state-root>/inbox`에 업로드합니다.
5. collector를 실행합니다.

   ```bash
   privyspark review collect \
     --scan-results <scan-output>/parquet/scan_results \
     --review-state-root <review-state-root>
   ```

6. collector 결과를 확인합니다.
   - `<review-state-root>/current/allowlist.jsonl`
   - `<review-state-root>/current/action_plan.jsonl`
   - `<review-state-root>/current/finding_status.jsonl`
   - `<review-state-root>/current/response_ledger.jsonl`

7. 다음 스캔도 같은 `<review-state-root>`를 사용합니다.

## 보안과 감사
- 검출 샘플은 HTML에만 포함하고 메일 본문과 response JSON에는 포함하지 않습니다.
- response JSON 원본은 삭제하지 않습니다.
- collector는 accepted 처리 이력을 `response_ledger.jsonl`에 남깁니다. 현재 `scan_results`와 맞지 않는 response는 state에 반영하지 않고 driver 로그의 rejected count로 확인합니다.
- pattern allowlist는 반드시 만료일을 가져야 합니다.
- broad wildcard는 기본적으로 제한합니다.
- 구버전 HTML 또는 오래된 scan result에 대한 응답은 `finding_hash` mismatch로 거부합니다.
- state 갱신은 임시 경로에 쓴 뒤 rename하는 방식으로 원자성을 보장합니다.

## 기존 review apply와의 관계
기존 `privyspark review apply`는 사람이 편집한 `scan_results`에서 `review_status=false_positive` row를 읽어 exact allowlist를 만드는 흐름입니다.

오프라인 collector 흐름은 이보다 넓은 운영 모델입니다.
- 입력은 response JSON입니다.
- 오탐 exact allowlist와 pattern allowlist를 모두 생성합니다.
- 정탐 action plan을 누적합니다.
- 여러 스캔에 걸친 누적 state를 유지합니다.

기존 `review apply`는 단일 파일 기반 수동 검토에 계속 사용할 수 있습니다. 서버 없는 담당자 검토와 누적 상태 운영은 `review collect` 흐름을 기준으로 합니다.
