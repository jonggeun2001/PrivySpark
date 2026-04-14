# Todo

- [x] `non_null_match_ratio` 영향 범위 확인
- [x] failing test 추가: blank/null 제외 분모 규칙
- [x] failing test 확인
- [x] 스키마/계산 로직을 `non_empty_match_ratio`로 변경
- [x] CSV/JSON/문서 출력 이름 변경
- [x] 관련 테스트 실행 및 결과 확인
- [x] 문서 업데이트
- [x] 최종 검증 실행
- [ ] 리뷰 지적 반영 커밋, 푸시, PR 업데이트

## Review

- [x] 요구사항 대비 rename 반영 확인
- [x] `trim(column)` blank와 `null`이 모두 비어있는 값으로 처리되는지 확인
- [x] `value` 매칭 분자도 비어 있지 않은 값 기준으로 제한되는지 확인
- [x] legacy `_progress` 결과의 `non_null_match_ratio`가 merge 시 복구되는지 확인
- [ ] 최종 보고에 검증 결과와 직접 반영 작업 여부 기록
