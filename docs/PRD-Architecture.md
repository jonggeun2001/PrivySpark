# PrivySpark 아키텍처 PRD (MVP)

## 아키텍처 목표
- Spark 기반 배치 처리로 대용량 데이터셋을 안정적으로 스캔합니다.
- 입력 확장과 그룹화로 처리 효율을 확보하되, 결과 식별자 의미를 유지합니다.
- 일부 파일/그룹 실패가 있어도 가능한 범위를 계속 처리합니다.

## 구현 컴포넌트 맵
- `Cli.scala`: 실행 인자와 기본 실행 옵션
- `FormatDetector.scala`: 확장자 기반 1차 포맷 식별
- `RulesetLoader.scala`: 기본/외부 ruleset 로딩과 검증
- `DriverLogger.scala`: driver 로그 레벨 해석, 공통 로그 포맷, fatal/error 강제 출력
- `DetectionAggregator.scala`: 규칙별 집계와 fallback 전략
- `SampleDatasetGenerator.scala`: 입력 처리 케이스 재현용 샘플 데이터셋 생성
- `build.gradle.kts`의 `packageSampleDatasets`: 샘플 데이터셋 번들을 release용 zip으로 패키징
- `DriverLicenseNumberValidator.scala`: 운전면허번호 strict validator
- `PrivySparkApp.scala`: 입력 확장, 그룹화, exact split, 스캔 orchestration, 리포트 저장
- `Models.scala`: 결과/오류/규칙 모델

## 처리 플로우
1. 입력 경로 검증
2. ruleset 로드와 regex 사전 검증
3. 물리 파일 수집
4. archive 엔트리 확장, workbook 시트 확장, 무확장자/미지원 확장자 `parquet/orc` magic-byte 판별, text fallback 정규화
5. `(directory, format)` 기준 1차 그룹화
6. 대표 파일 기준 스키마 샘플링
7. schema-aware split 및 디렉토리 식별자 승격 가능성 판정
8. sampled multi-file group이면 exact split 재검증 후 재분류된 그룹 스캔
9. non-sampled group 중 batch-capable group이면 필요 시 균등 무작위 file sampling 후 그룹 batch scan
10. non-sampled `xlsx` group은 direct file scan
11. 일반 group batch 실패 시 파일 단위 fallback
12. 결과/오류 리포트 저장

## 상세 문서 맵
- 실행 환경과 운영 옵션: [mvp/execution-and-operations.md](mvp/execution-and-operations.md)
- 입력 정규화와 그룹 전략: [mvp/input-normalization.md](mvp/input-normalization.md)
- 탐지, validator, 집계 전략: [mvp/detection-and-rules.md](mvp/detection-and-rules.md)
- 출력 스키마와 실패 처리: [mvp/reporting-and-failures.md](mvp/reporting-and-failures.md)
- 전체 MVP 문서 인덱스: [mvp/README.md](mvp/README.md)

## 운영 불변 조건
- 원문 PII는 저장하지 않습니다.
- CLI 병렬도 값이 있으면 `scanDirectoryStructure`, `scanGroups`, 일반 파일 fallback 경로의 앱 로직 전달값이 우선합니다.
- `--pre-scan-parallelism`은 파일 단위 입력 확장, 포맷 판별, 그룹별 schema split 경로에 적용합니다.
- `--pre-scan-parallelism`과 `spark.privyspark.preScanParallelism`은 `> 0`이면 허용하고, 최종 적용값은 파일 수와 pre-scan safety ceiling `64` 기준으로 축소합니다.
- 기본 pre-scan 병렬도는 I/O 중심 작업 특성을 고려해 `4`를 유지합니다.
- explicit pre-scan 병렬도에서 driver CPU 기반 상한을 제거한 이유는 archive expansion, header probe, workbook metadata listing이 대부분 짧은 blocking I/O여서 운영자가 workload에 맞춰 코어 수보다 높은 동시성을 선택할 수 있게 하기 위함입니다.
- 동시에 무제한 스레드 생성을 막기 위해 pre-scan 실행 스레드는 고정 safety ceiling `64`로 제한합니다.
- pre-scan은 0바이트 physical file을 즉시 skip하고, archive 내부 0바이트 entry도 staging이나 오류 리포트 없이 제외합니다.
- batch scan을 지원하지 않는 `xlsx` direct file scan 경로는 현재 CLI `--file-parallelism` 전달 대상이 아닙니다.
- `--file-sample-ratio`는 현재 batch-capable group scan에만 적용하며, group 내부 파일 목록에서 `ceil(fileCount * ratio)` 수만큼 최소 1개를 균등 무작위 추출합니다.
- `--file-sample-ratio`를 도입한 이유는 작은 파일이 많은 그룹에서 task fan-out을 직접 줄이기 위해서이기도 하지만, 더 중요한 요구는 특정 데이터가 한 파일에 몰릴 가능성을 배제하지 않는 샘플링이 필요했기 때문입니다. 파일 크기 가중치 기반 선택은 큰 파일 편향을 강화할 수 있으므로, 현재 구현은 파일 단위 concentration risk를 과소평가하지 않도록 균등 무작위 추출을 사용합니다.
- batch-capable group scan에서 `--file-sample-ratio`가 설정되면 `--sample-ratio < 1.0`은 무시하고 warning 로그를 남깁니다. 파일 샘플링 후 다시 row sampling을 적용하면 샘플 기준이 이중으로 바뀌어 결과 해석이 모호해지기 때문입니다.
- sampled group은 exact split 검증 전까지 디렉토리 식별자로 승격하지 않습니다.
- archive와 Excel 논리 입력은 자체 식별자를 유지합니다.
