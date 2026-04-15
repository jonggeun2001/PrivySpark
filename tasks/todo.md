# Todo

- [x] 설계 문서와 현재 코드 구조 대조
- [x] worktree/branch 생성 및 작업 환경 준비
- [x] failing test 추가: CLI `--ignore`, `--ignore-file`
- [x] failing test 추가: `IgnoreMatcher` glob/path 매칭
- [x] failing test 추가: 디렉터리/아카이브 ignore 적용
- [x] `CliConfig`와 parser에 ignore 옵션 추가
- [x] `IgnoreMatcher` 신규 구현
- [x] `scanDirectoryStructure`와 archive 확장 경로에 ignore 적용
- [x] ignore 관련 로그/summary 카운트 반영
- [x] 문서 업데이트
- [x] 관련 테스트 실행 및 결과 확인
- [x] 최종 검증 실행
- [ ] 커밋, 푸시, PR 생성/업데이트

## Review

- [x] basename 패턴과 relative-path 패턴이 모두 동작하는지 확인
- [x] `--ignore-file`에서 주석/빈 줄이 무시되는지 확인
- [x] 단일 파일 입력과 디렉터리 입력 모두에서 ignore가 조기 적용되는지 확인
- [x] archive entry ignore 시 `archive_entry_skipped reason=ignored`가 남는지 확인
- [ ] 최종 보고에 검증 결과와 직접 반영 작업 여부 기록
