# Code Map

이 문서는 코드 탐색용 인덱스입니다. 동작 계약은 `docs/ko/`, `docs/en/`, `README.md`의 사용자/운영 문서를 기준으로 합니다.

## 패키지 요약

- `cli/`: scopt 기반 CLI 파싱, 경로 validation, 실행 옵션 모델.
- `config/`: ruleset, suppression, ignore matcher 설정 로딩과 검증.
- `detect/`: PII 규칙별 Spark aggregation, non-empty count, sample value 수집.
- `format/`: 입력 포맷 감지, CSV dialect/header 추론, Excel workbook streaming, 압축 스트림 처리.
- `fsio/`: Hadoop `FileSystem` 기반 안전한 경로 교체, retry IO helper.
- `hive/`: Hive Metastore JDBC table location 조회, longest-prefix lookup 인덱스, scan result `hive_table_fqn` 해석.
- `model/`: scan result/error, ruleset, scan plan, pre-scan/group/report ADT.
- `progress/`: run marker, progress JSONL, in-flight marker, stale run cleanup.
- `report/`: scan result/error를 parquet/csv/excel 산출물로 저장.
- `review/`: offline review HTML, response collect/apply, allowlist/action plan state.
- `scan/`: directory discovery, source expansion, grouping, file sampling, batch/file scan orchestration.
- `scan/archive/`: archive format dispatch, staging safety, entry loop, zip/tar/7z/rar handlers.
- `util/`: driver logging, parallelism 설정, path identifier 정규화.

## 핵심 파일 포인터

- `PrivySparkApp.scala`: `main` L36, `runMain` L40, `runScan` L161.
- `scan/DirectoryScanner.scala`: `discoverPhysicalFiles` L55, `scanDirectoryStructure` L108, schema split fast path L501.
- `scan/archive/ArchiveExpanders.scala`: archive format dispatch L36, unsupported/read failure handling L60.
- `scan/archive/ArchiveStaging.scala`: archive format constants L6, safe staging path resolution L19.
- `scan/GroupScanCoordinator.scala`: `scanGroups` L17, sampled/file fallback L173, batch fallback policy L246.
- `scan/FileSampling.scala`: deterministic file sampling L6.
- `hive/HiveTableFqnResolver.scala`: scan result `hive_table_fqn` 단일 해석 helper L5.
- `review/ReviewHtmlWriter.scala`: `normalizeSampleMode` L29, public `write` overloads L34-L68, `writeFindings` L84, JSON/sample rendering L885.
- `detect/DetectionAggregator.scala`: public `aggregate` L80, `aggregateByFile` L89, sample collection L124, metric planning L145.
- `report/ReportWriter.scala`: public `writeReports` overloads L17-L51, format writer L147.
- `model/Models.scala`: `PiiRule` L14, `ScanResult` L32, `ScanError` L55.
- `model/ScanPlanModels.scala`: `ScanFileEntry` L20, `ScanGroup` L32, `DirectoryScanPlan` L49, `PreScanFileOutcome` L74, `ReportFormatPaths` L107.

## 호출 트레이스

```text
main
  -> runMain
  -> runScan
  -> DirectoryScanner.scanDirectoryStructure
  -> ProgressRunManager.prepareProgressRun
  -> GroupScanCoordinator.scanGroups
  -> DetectionAggregator
  -> ProgressRunManager.mergeProgressReports
  -> ReportWriter.writeReports
  -> afterReportWrite callback
  -> ReviewHtmlWriter.write (reviewStateRoot 설정 시)
```

## 데이터 모델 위치

- 리포트 스키마: `model/Models.scala`의 `ScanResult`, `ScanError`.
- 규칙 스키마: `model/Models.scala`의 `PiiRule`, `Ruleset`, `YamlRuleset`.
- 스캔 입력 계획: `model/ScanPlanModels.scala`의 `ScanFileEntry`, `ScanGroup`, `DirectoryScanPlan`.
- pre-scan 결과: `model/ScanPlanModels.scala`의 `PreScanFileOutcome`.
- 출력 포맷 경로: `model/ScanPlanModels.scala`의 `ReportFormatPaths`.

## 갱신 규칙

- 대형 파일 분할, public entrypoint 이동, 패키지 추가/삭제가 있으면 이 문서를 함께 갱신합니다.
- 라인 포인터는 exact 계약이 아니라 빠른 진입용입니다. 근처 심볼을 찾는 기준으로 유지합니다.
- 앱 버전은 `build.gradle.kts:12`가 진실 소스입니다. 코드 맵에는 고정 버전을 쓰지 않습니다.
