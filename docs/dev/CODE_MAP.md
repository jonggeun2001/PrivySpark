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
- `review/fingerprint/`: review apply fingerprint resolution for flat files, workbook sheets, archive entries, CRC32 streams.
- `review/collect/`: offline review response envelope read/validate/build/write helpers and collect lock handling.
- `scan/`: scan pipeline orchestration, directory scan, source expansion, grouping, group route/fallback policy, file sampling, batch/file scan orchestration.
- `scan/archive/`: archive format dispatch, staging safety, entry loop, zip/tar/7z/rar handlers.
- `scan/discovery/`: physical file discovery, pre-scan expansion, schema split/finalization helpers.
- `util/`: driver logging, driver TCP socket 진단, RPC concurrency gate, parallelism 설정, path identifier 정규화.

## 핵심 파일 포인터

- `PrivySparkApp.scala`: `main` L13, `runMain` L17, scan auto collect dispatch L49, default scan dispatch L79, Excel compatibility warning L92.
- `cli/CliArgumentValidator.scala`: command path validation L6, absolute path error logging L51.
- `config/SuppressionParser.scala`: parsed suppression ADT L15, CLI/file suppression parsing L17/L25, unknown pii warning L35.
- `scan/ScanPipeline.scala`: summary/hooks ADT L22/L36, `run` orchestration L41, report merge/review hook L204.
- `scan/DirectoryScanner.scala`: `scanDirectoryStructure` L23, pre-scan collect/group build L141, schema split/finalization delegate L219.
- `scan/archive/ArchiveExpanders.scala`: archive format dispatch L36, unsupported/read failure handling L60.
- `scan/archive/ArchiveStaging.scala`: archive format constants L6, safe staging path resolution L19.
- `scan/discovery/DirectoryDiscovery.scala`: `resolvePreScanProgressInterval` L13, `discover` L17.
- `scan/discovery/PreScanExecutor.scala`: CSV dialect refinement L29, `runPreScan` L50.
- `scan/discovery/SchemaGroupSplitter.scala`: `splitAndFinalize` L24, schema split scheduling L120, file schema task executor L140, `splitGroupBySchemaFast` L152, `splitGroupBySchema` L302.
- `scan/GroupScanCoordinator.scala`: `scanGroups` L17, route dispatch L221, sampled batch schema validation L319, batch fallback invocation L308, compatibility delegates L361/L399.
- `scan/GroupFileScanner.scala`: `scanGroupByFile` L25, file progress buffer setup L89, file progress record helper L96, group buffer flush L309.
- `scan/GroupScanRouter.scala`: group route ADT L6, `routeOf` L15.
- `scan/GroupScanFallbackPolicy.scala`: batch failure fallback executor L7.
- `scan/FileSampling.scala`: deterministic file sampling L6.
- `hive/HiveTableFqnResolver.scala`: scan result `hive_table_fqn` 단일 해석 helper L5.
- `review/FileIdentifierResolver.scala`: fingerprint resolver dispatch L14.
- `review/fingerprint/PathFingerprintResolver.scala`: flat file/directory fingerprint resolution L11, input path resolution L31.
- `review/fingerprint/WorkbookFingerprintResolver.scala`: workbook identifier parsing L11, sheet fingerprint resolution L33.
- `review/fingerprint/ArchiveFingerprintResolver.scala`: archive identifier parsing L21, archive format dispatch L30.
- `review/fingerprint/Crc32Stream.scala`: CRC32 stream calculation L16, temporary local archive helper L54.
- `review/ReviewCollectCommand.scala`: collect ADT L12-L65, lock/strict validation/write orchestration L68.
- `review/collect/ReviewCollectLock.scala`: collect lock path L31, atomic lock acquire L34, release L55.
- `review/collect/ResponseEnvelopeReader.scala`: inbox JSON read L13, response envelope parse L30.
- `review/collect/ResponseValidator.scala`: envelope validation L9, response item validation L23.
- `review/collect/ReviewStateBuilder.scala`: current state build L23, recurring/action-plan retention helpers L85.
- `review/collect/ReviewStateWriter.scala`: action plan load L20, state write L47, atomic replace L60.
- `review/ReviewHtmlWriter.scala`: `normalizeSampleMode`, public `write` overloads, 2MB review HTML split, `writeFindings`.
- `review/ReviewHtmlRenderer.scala`: resource template/script loading, review part metadata rendering, split index rendering.
- `review/ReviewSampleMasker.scala`: finding JSON rendering L6, sample masking L17.
- `review/ReviewActionPlanStatus.scala`: action plan state load/match L20, UI status label L75.
- `src/main/resources/review/review.html.template`: offline review HTML/CSS shell and `${REVIEW_DATA_JSON}` placeholder.
- `src/main/resources/review/review.js`: offline review browser state, sorting, validation, and response download logic.
- `detect/DetectionAggregator.scala`: fault injector plug-point L37, public `aggregate` L52, `aggregateByFile` L61, sample collection L96, metric planning L117.
- `fsio/RetryIO.scala`: file read retry 정책, exponential backoff/jitter, retry 전 Spark catalog refresh 대상 제어.
- `progress/ProgressIO.scala`: flush mode 설정 L19-L44, progress JSONL write L46/L108.
- `progress/ProgressBuffer.scala`: group 단위 progress buffering L11, enqueue L23, flush L30.
- `progress/ProgressRunManager.scala`: progress run prepare L21, merge L88-L101, active heartbeat L154-L181.
- `util/DriverTcpConnectionSnapshot.scala`: Linux `/proc` 기반 driver TCP socket snapshot capture와 `group_scan_tcp_snapshot` debug log helper.
- `util/RpcGate.scala`: `spark.privyspark.driverRpcConcurrency` 기반 driver-side RPC성 작업 동시성 gate.
- `report/WriteReportsRequest.scala`: report write request ADT L5.
- `report/ReportWriter.scala`: request 기반 `writeReports` L17, 호환용 Seq writer L100, format writer L128.
- `model/Models.scala`: `PiiRule` L14, `ScanResult` L32, `ScanError` L55.
- `model/ScanPlanModels.scala`: `ScanFileEntry` L20, `ScanGroup` L32, `DirectoryScanPlan` L49, `PreScanFileOutcome` L74, `ReportFormatPaths` L107.

## 호출 트레이스

```text
main
  -> runMain
  -> ScanPipeline.run
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
