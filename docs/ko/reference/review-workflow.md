# False Positive 검토 워크플로우

## 목적
- 반복적으로 같은 false positive가 다시 올라오는 노이즈를 줄입니다.
- 검토 단위는 `(file_identifier, column_name, pii_type)` 입니다.
- 파일 메타데이터와 checksum이 바뀌면 기존 false positive 판정은 자동으로 무효화됩니다.

## 1. 1차 스캔
```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output \
  --output-format excel
```

담당자는 `scan_results`에서 다음 컬럼만 편집하면 됩니다.
- `review_status`: `pending`, `false_positive`, `true_positive`
- `review_reason`: 자유 텍스트

## 2. Allowlist 생성 또는 갱신
```bash
privyspark review apply \
  --scan-results /abs/output/excel/scan_results.xlsx \
  --input-root /abs/input \
  --allowlist /abs/review/allowlist.jsonl \
  --reviewer reviewer@example.com
```

동작 요약:
- `review_status=false_positive` row만 읽습니다.
- 각 row의 `file_identifier`를 실제 파일로 역해석합니다.
- directory identifier는 `review_scope_file_identifiers`에 기록된 concrete file identifier만 allowlist로 전개합니다.
- 현재 파일의 `file_size`, `file_mtime_epoch_ms`, `CRC32`를 계산합니다.
- 같은 key가 이미 있으면 최신 review로 덮어씁니다.

같은 `scan_results` 파일에서 `true_positive` 또는 `pending`으로 바뀐 key는 기존 allowlist에서 제거됩니다.

`--dry-run`을 주면 파일은 쓰지 않고 staged entry 수만 계산합니다.

## 3. 재스캔
```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output-next \
  --allowlist /abs/review/allowlist.jsonl
```

재스캔 시 동작:
- allowlist key와 현재 `(file_identifier, column_name, pii_type)`가 일치하고
- `file_size`, `file_mtime_epoch_ms`, `CRC32`도 같으면
- 해당 result row는 최종 `scan_results`에서 빠집니다.

메타데이터나 checksum이 바뀌면 row를 유지하고 다음 값을 남깁니다.
- `review_status=pending`
- `review_reason=<이전 사유>`
- `review_invalidated=true`

## 주의사항
- ignore와 allowlist는 다릅니다. ignore는 파일 자체를 스캔에서 제외하고, allowlist는 이미 검토된 false positive만 suppress합니다.
- archive entry는 `<archive>!<entry>`, Excel sheet는 `<workbook>#<sheet>` 식별자를 유지합니다.
- password-protected archive, multi-volume RAR, RAR5 archive는 스캔 단계에서 `scan_errors`에 명시적으로 기록됩니다.
