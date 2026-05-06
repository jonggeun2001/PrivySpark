# Legacy Review Apply 워크플로우

`privyspark review apply`는 사람이 `scan_results`를 직접 편집한 뒤 legacy exact allowlist JSONL을 생성하는 오래된 워크플로우입니다.

현재 오프라인 리뷰 운영의 기본 흐름은 [offline-review-collector.md](offline-review-collector.md)의 recurring review state입니다. `scan --review-state-root`가 적용하는 누적 오탐 제외는 `entry_type=recurring`만 사용하며, legacy exact entry는 suppress 판단에 사용하지 않습니다.

## Legacy 명령

```bash
privyspark review apply \
  --scan-results /abs/output/excel/scan_results.xlsx \
  --input-root /abs/input \
  --allowlist /abs/review/allowlist.jsonl \
  --reviewer reviewer@example.com
```

이 명령은 `review_status=false_positive` row를 읽고 파일 fingerprint 기반 exact entry를 씁니다. 이 형식은 이전 버전과의 파일 생성 호환을 위해 남아 있지만, 반복 배치 오탐 운영에는 적합하지 않습니다.

## 권장 흐름

서버 없이 담당자에게 `review.html` 또는 `review.xlsm`을 전달하고 다음 스캔에서 반복 오탐을 제외하려면 다음 흐름을 사용합니다.

```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output \
  --review-state-root /abs/review-state

privyspark review collect \
  --review-state-root /abs/review-state
```

회수한 response JSON은 `/abs/review-state/inbox/*.json`에 업로드합니다. `review.xlsm`을 사용한 경우 Excel 파일 자체가 아니라 통합 문서의 `review.json 생성` 버튼으로 만든 JSON 파일을 업로드합니다.
