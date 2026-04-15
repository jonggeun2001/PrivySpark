# Lessons

- 단일 파일 디렉토리 승격 조건을 바꿀 때는 세 경우를 반드시 분리해서 생각한다: 디렉토리 입력, 입력 루트 그룹, 직접 파일 입력. 이 셋을 섞으면 `.` 승격이나 부모 디렉토리 승격이 의도치 않게 퍼진다.
- ratio 스키마 rename은 계산 함수만 바꾸면 끝나지 않는다. case class 필드, JSON 직렬화, CSV 컬럼명, progress merge, 문서, 테스트 fixture를 같이 바꿔야 누락이 없다.
- 진행 중 스캔용 progress 스키마를 바꿀 때는 merge reader에 구 필드 fallback을 남겨야 한다. 그렇지 않으면 배포 중 재시작 시 누적된 JSONL progress가 새 버전에서 끊긴다.
- glob ignore 기능은 매칭 엔진만 추가해서 끝나지 않는다. CLI 옵션, 파일 discovery, archive entry 확장, summary 로그, 문서, 테스트를 한 번에 묶어야 실제 운영 노이즈 감소로 이어진다.
- DriverLogger는 `*`, `!` 같은 문자를 가진 값을 자동으로 quote 하므로, 로그 기반 테스트는 `key=value` 전체 literal보다 핵심 token 존재 여부로 검증하는 편이 안정적이다.
