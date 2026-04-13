# PrivySpark MVP 문서 맵

이 디렉토리는 현재 소스 기준 MVP 문서를 책임 단위로 분할한 문서 모음입니다.

## 읽는 순서
1. [execution-and-operations.md](execution-and-operations.md)
2. [input-normalization.md](input-normalization.md)
3. [detection-and-rules.md](detection-and-rules.md)
4. [reporting-and-failures.md](reporting-and-failures.md)

## 소스 매핑
- `Cli.scala` -> 실행 인자, 샘플링, 병렬도: [execution-and-operations.md](execution-and-operations.md)
- `PrivySparkApp.scala` -> 입력 확장, 그룹화, 스캔 흐름, 리포트 저장: [input-normalization.md](input-normalization.md), [reporting-and-failures.md](reporting-and-failures.md)
- `DetectionAggregator.scala` -> 탐지 집계, 배치/legacy fallback: [detection-and-rules.md](detection-and-rules.md)
- `RulesetLoader.scala`, `DriverLicenseNumberValidator.scala` -> ruleset 검증, 타입별 제약: [detection-and-rules.md](detection-and-rules.md)
- `Models.scala` -> 결과/오류 스키마: [reporting-and-failures.md](reporting-and-failures.md)

## 문서 원칙
- 현재 구현된 동작만 기록합니다.
- 중복 설명은 줄이고, 상세 규칙은 가장 가까운 책임 문서 한 곳에 둡니다.
- `docs/PRD-Functional.md`, `docs/PRD-Architecture.md`는 이 문서 묶음으로 연결되는 허브 역할을 합니다.

## 샘플 번들
- 입력 처리 케이스 재현용 샘플 데이터셋은 [../../samples/input-cases/README.md](../../samples/input-cases/README.md)에 있습니다.
