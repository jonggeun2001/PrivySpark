# Todo

- [x] `DetectionAggregator`의 driver license 집계 UDF 사용 위치 확인
- [x] worktree/branch 생성 및 작업 환경 준비
- [x] failing test 추가: analyzed expression에서 `ScalaUDF` 제거 조건
- [x] failing test 추가: batched aggregate와 safe legacy fallback 동치성
- [x] failing test 추가: 앞선 invalid match 뒤의 later valid match 수용
- [x] `driver_license_number` 집계를 `regexp_extract_all` 기반 SQL predicate로 치환
- [x] validator 상수 가시성 정리
- [x] 문서 업데이트
- [x] 관련 테스트 실행 및 결과 확인
- [x] 최종 검증 실행
- [ ] 커밋, 푸시, PR 생성/업데이트

## Review

- [x] aggregate analyzed expression에 `ScalaUDF`가 남지 않는지 확인
- [x] partial/full-column 모두 safe legacy fallback과 결과가 맞는지 확인
- [x] custom regex에서 invalid-first/later-valid semantics가 유지되는지 확인
- [ ] 최종 보고에 검증 결과와 직접 반영 작업 여부 기록
