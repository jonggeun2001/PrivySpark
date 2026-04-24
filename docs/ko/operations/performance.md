# 성능 가이드

## 현재 성능 특성
PrivySpark 성능은 크게 네 구간으로 나뉩니다.

1. 파일 발견과 pre-scan
2. 스키마 샘플링과 그룹 split
3. group/file scan
4. 결과 merge와 리포트 저장

실제 병목은 입력 분포에 따라 달라집니다. 작은 파일이 매우 많으면 pre-scan과 파티션 fan-out이, 넓은 스키마 테이블에서는 탐지 집계 표현식 수가 더 크게 작용합니다.

## 현재 구현이 이미 적용하는 최적화
- pre-scan 병렬도는 BFS 디렉터리 discovery, 파일 확장, 포맷 판별, 그룹별 schema split에 재사용됩니다.
- `xlsx` 실제 scan은 spark-excel/POI DataFrame reader 대신 executor-side StAX 스트리머를 사용해 driver가 workbook body와 POI workbook 객체를 들고 있지 않도록 합니다.
- CSV 본문 읽기는 `inferSchema=false`로 동작합니다.
- CSV dialect 감지는 파일 앞부분의 non-blank 라인 일부만 사용하며, 비기본 dialect가 있는 그룹은 파일별 read option 보존을 위해 exact split/file scan 경로로 처리합니다.
- `DetectionAggregator`는 메트릭별 개별 job 대신 batched aggregation을 기본 경로로 사용합니다.
- `driver_license_number`도 다른 규칙과 동일한 regex 기반 predicate 경로를 사용하므로, 타입별 Scala UDF나 추가 validator 없이 Catalyst/codegen 경로를 유지합니다.
- legacy fallback threshold는 `50,000` 표현식으로 올려져 있고, 초과 시에도 메트릭당 개별 count 대신 소배치 집계를 사용합니다.
- sample raw-value fallback도 배치화되어 있습니다. dataset 경로는 `when(...)` projection을 chunk 단위로 처리하고, file 경로는 파일별 `first(when(...))` 집계를 chunk 단위로 묶어서 메트릭마다 Spark job을 따로 내지 않습니다.
- `_progress`를 최종 집계 소스로 사용해 long scan에서 driver가 모든 결과 row payload를 끝까지 들고 있지 않도록 합니다.
- sampled scan과 최종 리포트 저장 경로는 Spark storage cache를 사용하지 않습니다. dynamic allocation 환경에서 cached executor가 YARN 자원을 오래 점유하지 않게 하기 위한 선택입니다.
- 기본 driver-side 병렬도는 I/O 바운드 pre-scan fan-out을 기준으로 `--pre-scan-parallelism 32`, `--group-parallelism 16`, `--file-parallelism 8`에서 시작합니다. `pre-scan`은 안전 상한 `64`를 유지합니다.

## 작은 파일이 많은 입력
- `--pre-scan-parallelism`은 디렉터리 discovery, 파일 probe, schema split 대기 시간을 줄이는 1차 옵션입니다.
- `--group-parallelism`과 `--file-parallelism`은 driver 제출 동시성을 늘리지만, executor 분산을 직접 보장하지는 않습니다.
- `--file-sample-ratio`는 읽는 파일 수 자체를 줄이므로, 작은 파일이 매우 많을 때 `--sample-ratio`보다 직접적인 효과가 날 수 있습니다.
- 기본 `--file-sample-min-files 10` 때문에 작은 그룹은 sampling 대상이 아닙니다. 더 작은 그룹에도 file sampling을 적용하려면 임계값을 낮춰야 합니다.

균등 무작위 파일 샘플링을 둔 이유는 성능만이 아니라 데이터 concentration risk를 보존하기 위해서입니다. 특정 데이터가 한 파일에 몰린 경우를 운영적으로 배제하지 않는다는 요구가 있었기 때문에, 파일 크기 가중치 대신 각 파일을 같은 확률로 뽑습니다.

## `scan_directory_structure_start` 이후가 느릴 때
이 구간은 보통 driver 쪽 작업입니다.

- 파일 목록 재귀 수집
- pre-scan 실행
- 입력 확장과 초기 그룹화
- schema split

특히 `scan_directory_files_discovered` 이후 `scan_directory_initial_groups_ready`가 느리면 파일 수가 많을 때 다음 비용이 커질 수 있습니다.

- 파일별 `getFileStatus`
- 미지원 확장자/무확장자 probe
- CSV dialect probe
- `xlsx` workbook metadata 및 header row XML 기반 visible sheet/schema 확장
- 대량 `Future` 제출과 결과 수집
- `(directory, format)` 그룹화와 정렬

## 탐지 집계가 느릴 때
`DetectionAggregator`의 전역 aggregate는 결과가 1행이어도 샘플된 DataFrame 전체 파티션을 읽습니다. 그래서 task 수는 `head()`가 아니라 입력 파티션 수와 배치 수에 의해 결정됩니다.

- 파티션 수가 많으면 aggregate task 수도 많아집니다.
- wide schema + 많은 rules는 메트릭 수를 늘립니다.
- `column_hints`가 있으면 필요한 컬럼에만 규칙을 적용해 메트릭 수를 줄일 수 있습니다.

## Spark/YARN 운영 팁
- dynamic allocation이 켜져 있어도 작은 job이 짧게 끝나면 executor scale-out이 제한될 수 있습니다.
- 반대로 cached block이 executor에 남아 있으면 scale-in도 늦어질 수 있습니다. PrivySpark는 이 문제를 줄이기 위해 sampled scan과 report write 경로에서 storage cache를 제거했습니다.
- 앱 레벨 병렬도만 올려도 Spark scheduler가 FIFO이거나 backlog가 작으면 executor fan-out이 기대보다 작을 수 있습니다.
- 큰 그룹 batch scan에서는 input partitioning과 Spark 파일 파티션 설정도 함께 봐야 합니다.
- `info`/`debug` driver 로그를 켜면 pre-scan, grouping, progress merge 구간을 단계별로 확인할 수 있습니다.

## 튜닝 우선순위
1. 입력이 작은 파일 위주면 `--pre-scan-parallelism`, `--file-sample-ratio`, Spark 파일 파티션 설정부터 조정
2. 그룹 수가 많으면 `--group-parallelism` 조정
3. 파일 fallback이 많으면 `--file-parallelism` 조정
4. wide schema면 `column_hints`와 ruleset 구성을 먼저 정리
5. 장시간 스캔은 `_progress`와 driver 로그를 함께 보면서 병목 구간을 분리
