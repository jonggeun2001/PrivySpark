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

서버 없이 담당자에게 `review.html`을 전달하고 다음 스캔에서 반복 오탐을 제외하려면 다음 흐름을 사용합니다.

```bash
privyspark scan \
  --path /abs/input \
  --output /abs/output \
  --review-state-root /abs/review-state
```

회수한 response JSON은 `/abs/review-state/inbox/*.json`에 업로드합니다. 다음 `scan --review-state-root /abs/review-state`는 스캔 시작 전에 자동으로 수집을 실행합니다. invalid response가 있거나 다른 collect가 `/abs/review-state/.collect.lock`을 보유 중이면 스캔은 시작하지 않고 실패합니다. 탐지가 많아 `review.html`이 2MB를 넘으면 `review.html`은 인덱스가 되고 `review-part-*.html`이 생성됩니다. 각 part 파일에서 응답 JSON을 따로 생성해 모두 inbox에 업로드합니다. Excel 편집이 필요하면 review 파일에서 CSV를 내려받아 편집하고, 사내 보안 솔루션이 CSV를 암호화한 경우 암호화 해제한 CSV를 다시 불러오거나 Excel에서 전체 복사한 TSV 클립보드 내용을 붙여넣은 뒤 JSON 파일을 생성해 업로드합니다. CSV 업로드는 따옴표로 감싼 쉼표와 줄바꿈을 셀 내용으로 유지하고, TSV 붙여넣기는 탭과 줄바꿈을 기준으로 반영합니다.
