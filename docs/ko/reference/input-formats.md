# 입력 포맷과 정규화

## 지원 입력
- 확장자 기반 우선 지원: `csv`, `json`, `jsonl`, `ndjson`, `parquet`, `orc`, `avro`, `xlsx`, `zip`, `jar`, `tar`, `tar.gz`, `tgz`, `tar.bz2`, `tbz2`, `tar.xz`, `txz`, `tar.zst`, `tzst`, `7z`, `rar`
- physical file 경로의 공백은 그대로 허용하고, Spark glob 특수문자(`*`, `?`, `[`, `]`, `{`, `}`)는 schema detection과 scan reader에 literal path로 전달합니다. 이 처리는 ignore 패턴 문법과 별개로 실제 파일명에만 적용됩니다.
- direct text-style data file(`csv`, `json`, `jsonl`, `ndjson`)에 붙은 outer compression wrapper `gz`, `bz2`는 원본 경로 그대로 Spark/Hadoop reader에 전달합니다. 예: `customers.csv.gz`, `events.json.bz2`
- 무확장자 파일과 대부분의 미지원 확장자 파일은 앞부분 매직바이트로 `parquet`, `orc`를 먼저 판별합니다. 다만 `pdf`, `jpg` 같은 명확한 비데이터 바이너리 확장자는 probe 없이 바로 미지원 입력으로 분류합니다.
- 매직바이트가 일치하지 않더라도 UTF-8 텍스트에서 안정적인 구분자와 헤더/데이터 구조가 확인되면 내부 `csv` 포맷으로 승격해 컬럼 단위로 스캔합니다. CSV dialect 감지에 실패하거나 텍스트 구조가 모호하면 UTF-8 또는 EUC-KR 텍스트처럼 보이는 입력은 내부 `text` 포맷으로 정규화해 단일 `value` 컬럼으로 스캔합니다.
- UTF-8 텍스트 안에서 ASCII 정보 구분자(`0x1C`-`0x1F`, 예: RS 구분 파일)가 안정적인 컬럼 구분자로 반복되면 CSV로 처리할 수 있고, CSV dialect 감지에 실패한 나머지 텍스트는 text fallback 입력으로 처리합니다.
- 바이너리처럼 보이는 입력만 `Unsupported file format`으로 오류 리포트에 기록합니다.
- 0바이트 physical file은 pre-scan에서 즉시 건너뜁니다.
- `--ignore`, `--ignore-file` 패턴에 매칭된 physical file은 pre-scan 전에 제외합니다.
- 디렉토리 discovery에서 얻은 파일 길이와 수정 시간을 pre-scan에서 재사용하므로, small file이 많은 입력에서도 format expansion 전에 발견된 파일마다 file-status RPC를 다시 호출하지 않습니다. 긴 pre-scan에서는 파일 수 기준 interval 외에도 시간 기준 throttle로 progress 로그를 남깁니다.

이 text/CSV fallback을 둔 이유는 확장자만으로 텍스트 로그나 덤프를 배제하면 실제 운영 입력을 지나치게 많이 놓치기 때문입니다. 반대로 아무 바이너리나 텍스트로 강제 처리하면 노이즈가 커지므로, 매직바이트와 텍스트 인코딩 probe, CSV dialect probe를 함께 사용해 경계를 분리합니다.

## Archive 처리
- `zip`, `jar`, `tar`, `tar.gz/tgz`, `tar.bz2/tbz2`, `tar.xz/txz`, `tar.zst/tzst`, `7z`, `rar`는 내부 엔트리를 선스캔한 뒤 staging 후 스캔합니다.
- archive 확장은 1단계까지만 허용합니다.
- nested archive 엔트리는 재귀 처리하지 않고 오류로 남깁니다.
- 0바이트 archive entry는 staging이나 오류 리포트 없이 건너뜁니다.
- ignore 패턴에 매칭된 archive entry는 `archive_entry_skipped reason=ignored` 로그만 남기고 staging하지 않습니다.
- archive 내부 식별자는 `<archive>!<entry>` 형식을 사용합니다.
- password-protected archive, multi-volume RAR, RAR5 archive는 staging 없이 `scan_errors`에 명시적으로 남깁니다.

## Excel 처리
- `xlsx`는 workbook을 시트 단위 논리 입력으로 확장합니다.
- pre-scan은 드라이버에서 workbook metadata와 header row XML만 경량 파싱해 visible sheet 목록과 schema signature를 만들고, sheet body row/cell 내용은 읽지 않습니다.
- 빈 visible sheet는 header 기반 schema detection 이후 결과/오류 없이 건너뜁니다. hidden/veryHidden sheet는 제외합니다.
- 시트 식별자는 `<workbook>#<sheet>` 형식을 사용합니다.
- 실제 scan은 Spark executor task 안에서 StAX 기반 sheet row 스트리머로 수행합니다. sharedStrings는 task 수명 동안 executor 메모리에 적재하고, sheet XML은 row 단위로 스트리밍합니다.
- workbook ZIP 엔트리 순회는 Spark/Hadoop 런타임에 이미 포함된 구버전 `commons-compress`와 호환되는 API를 사용하므로, `xlsx` scan을 위해 cluster classpath의 `commons-compress`를 별도로 교체할 필요가 없습니다.
- `--excel-max-rows-in-memory`는 과거 spark-excel scan reader 호환용으로만 받습니다. 값을 명시하면 `excel_max_rows_in_memory_unused` warning을 남기며 실제 scan에는 사용하지 않습니다.
- `--excel-byte-array-max-override` 또는 `spark.privyspark.excel.byteArrayMaxOverride`를 설정하면 Apache POI byte array allocation 상한을 조정합니다. 이 설정은 Excel report writing 등 POI 사용 경로를 위한 호환 설정입니다. 둘 다 생략하면 기본값 `300000000`을 적용합니다.

## 그룹화와 스캔 단위
- 기본 스캔 단위는 파일입니다.
- 먼저 `(directory, format)` 기준으로 1차 그룹을 만듭니다.
- 이 그룹화 전에 `key=value` partition 디렉토리, `bucket_00000` 또는 `bucket-00000` bucket 디렉토리, `__HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME__` skew/list-bucketing 디렉토리는 입력 루트 안에서 가장 가까운 비-layout 상위 경로로 정규화합니다.
- 이후 대표 파일 스키마 샘플링으로 `schemaSignature`를 보강합니다. exact split은 sampled CSV/JSON group과 파일별 스키마 확정이 필요한 fallback 경로에서 수행합니다.
- 동일 스키마가 확인된 다중 파일 그룹만 디렉토리 식별자로 승격할 수 있습니다.
- archive 엔트리와 Excel 시트는 논리 입력 식별자를 유지합니다.

## 스키마 샘플링
- 다중 파일 그룹은 대표 파일 1개로 스키마를 먼저 샘플링할 수 있습니다.
- 그룹별 schema split은 `--pre-scan-parallelism`을 재사용해 driver 측에서 병렬 수행합니다.
- sampled CSV/JSON group은 스캔 전에 exact split으로 다시 검증합니다.
- sampled `text`, Parquet, ORC, Avro group은 batch 전 exact schema validation을 건너뛰고 batch 경로에서 파일 식별자를 유지합니다.
- CSV는 fallback 또는 명시적 exact 경로에서 exact split에 도달한 경우 헤더 유무 드리프트도 다시 확인합니다.

스키마 샘플링을 별도 단계로 둔 이유는 두 가지입니다. 첫째, 같은 디렉토리라도 실제 스키마 드리프트가 있을 수 있으므로 디렉토리 단위 집계를 바로 적용하면 식별자 의미가 깨질 수 있습니다. 둘째, 모든 파일을 처음부터 exact split으로 읽으면 pre-scan 비용이 커지므로 대표 파일 샘플링으로 비용과 정확도 사이를 조정하고 non-CSV/non-JSON small-file group은 driver-side exact split 없이 더 저렴한 batch 경로를 사용합니다.

## CSV 헤더 처리
- CSV 구분자는 자동 감지합니다. 기본 후보는 콤마, 탭, 세미콜론, 파이프, 콜론, ASCII 정보 구분자이며, `||`, `|~|`처럼 라인 간 반복이 일정한 2-3글자 비영숫자 구분자도 후보로 사용합니다.
- `.csv` 확장자 파일도 실제 내용이 탭/세미콜론/파이프 등으로 구분되어 있으면 감지된 dialect로 읽습니다.
- `.txt`, `.log`, `.data`, 무확장자 등 미지원 확장자 텍스트도 헤더와 최소 2개 데이터 행에서 CSV dialect가 안정적으로 감지되고 추가 헤더 또는 구조화 값 신호가 있어야 `csv`로 승격합니다.
- 헤더가 있으면 헤더명 기반 시그니처를 사용합니다.
- 헤더가 없으면 컬럼 수 기반 시그니처(`cols:N`)를 사용합니다.
- 이미 CSV로 분류된 입력의 2행 tie-case는 header 쪽으로 처리할 수 있지만, 미지원 확장자 텍스트는 2행만으로 CSV로 승격하지 않습니다.

## 손상 입력과 fallback
- JSON이 corrupt record만 생성하면 해당 파일은 손상 입력으로 기록합니다.
- sampled CSV/JSON multi-file group은 스캔 전에 exact split으로 먼저 재검증합니다.
- sampled non-CSV/non-JSON batch-capable group은 바로 batch scan으로 진행하며, batch read가 실패하면 fallback 정책에 따라 exact split 재검증 후 다시 스캔할 수 있습니다.
- 일반 group에서 batch scan이 실패하면 별도 schema resplit 없이 파일 단위 fallback으로 전환합니다.
- 읽기 중 파일 교체/삭제가 발생하면 제한된 횟수 내에서 재시도합니다.
