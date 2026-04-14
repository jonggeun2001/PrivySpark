# Lessons

- 단일 파일 디렉토리 승격 조건을 바꿀 때는 세 경우를 반드시 분리해서 생각한다: 디렉토리 입력, 입력 루트 그룹, 직접 파일 입력. 이 셋을 섞으면 `.` 승격이나 부모 디렉토리 승격이 의도치 않게 퍼진다.
- ratio 스키마 rename은 계산 함수만 바꾸면 끝나지 않는다. case class 필드, JSON 직렬화, CSV 컬럼명, progress merge, 문서, 테스트 fixture를 같이 바꿔야 누락이 없다.
- 진행 중 스캔용 progress 스키마를 바꿀 때는 merge reader에 구 필드 fallback을 남겨야 한다. 그렇지 않으면 배포 중 재시작 시 누적된 JSONL progress가 새 버전에서 끊긴다.
