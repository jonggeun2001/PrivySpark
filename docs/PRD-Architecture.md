# PrivySpark 아키텍처 PRD (MVP)

## 아키텍처 목표
- Spark 기반 배치 처리로 대용량 데이터셋을 안정적으로 스캔합니다.
- 입력 확장과 그룹화로 처리 효율을 확보하되, 결과 식별자 의미를 유지합니다.
- 일부 파일/그룹 실패가 있어도 가능한 범위를 계속 처리합니다.

## 구현 컴포넌트 맵
- `Cli.scala`: 실행 인자와 기본 실행 옵션
- `FormatDetector.scala`: 확장자 기반 포맷 식별
- `RulesetLoader.scala`: 기본/외부 ruleset 로딩과 검증
- `DetectionAggregator.scala`: 규칙별 집계와 fallback 전략
- `DriverLicenseNumberValidator.scala`: 운전면허번호 strict validator
- `PrivySparkApp.scala`: 입력 확장, 그룹화, exact split, 스캔 orchestration, 리포트 저장
- `Models.scala`: 결과/오류/규칙 모델

## 처리 플로우
1. 입력 경로 검증
2. 물리 파일 수집
3. archive 엔트리 확장, workbook 시트 확장, unknown-extension text probe
4. `(directory, format)` 기준 1차 그룹화
5. 대표 파일 기준 스키마 샘플링
6. schema-aware split 및 디렉토리 식별자 승격 가능성 판정
7. sampled multi-file group이면 exact split 재검증 후 재분류된 그룹 스캔
8. non-sampled group이면 그룹 batch scan
9. 일반 group batch 실패 시 파일 단위 fallback
10. 결과/오류 리포트 저장

## 상세 문서 맵
- 실행 환경과 운영 옵션: [mvp/execution-and-operations.md](mvp/execution-and-operations.md)
- 입력 정규화와 그룹 전략: [mvp/input-normalization.md](mvp/input-normalization.md)
- 탐지, validator, 집계 전략: [mvp/detection-and-rules.md](mvp/detection-and-rules.md)
- 출력 스키마와 실패 처리: [mvp/reporting-and-failures.md](mvp/reporting-and-failures.md)
- 전체 MVP 문서 인덱스: [mvp/README.md](mvp/README.md)

## 운영 불변 조건
- 원문 PII는 저장하지 않습니다.
- CLI 병렬도 값이 있으면 앱 로직 전달값이 우선합니다.
- sampled group은 exact split 검증 전까지 디렉토리 식별자로 승격하지 않습니다.
- archive와 Excel 논리 입력은 자체 식별자를 유지합니다.
