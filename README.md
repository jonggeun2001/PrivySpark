# PrivySpark

PrivySpark는 Spark 기반 배치 스캐너로, 데이터셋에서 잠재적 개인정보(PII)를 정규식으로 탐지해 리포트를 생성합니다.

## 현재 범위 (MVP v0.1)
- 일회성 배치 실행
- 입력/출력 경로는 절대경로(또는 URI)만 허용
- 파일 단위 스캔을 기본으로 하되, exact-confirmed 동일 스키마 파일 묶음은 디렉토리 그룹 기준으로 식별 가능
- 디렉토리 구조 선스캔 후 `(디렉토리, 포맷)` 그룹 단위로 배치 처리
- 그룹 내부는 대표 파일 1개로 스키마를 우선 샘플링하고, sampled group은 파일 식별자를 유지한 채 배치 읽기를 시도한다. 다만 CSV sampled group은 헤더 유무 드리프트를 막기 위해 batch scan 전에 exact split으로 재확인한다. sampled group 배치 읽기 실패 시 전체 파일 exact split 후 재시도한다.
- `file_identifier`는 입력 경로 기준 상대경로를 사용하고, exact split으로 동일 스키마가 확인된 디렉토리 그룹만 디렉토리 상대경로를 사용한다. 입력 루트 디렉토리 그룹은 충돌 방지를 위해 `.`로 표기한다.
- 외부 규칙 파일 기반 정규식 탐지 (선택적 `column_hints`/`validator` 지원 + 배치 집계 + 메트릭 50,000 초과 시 소배치 폴백 + 집계 예외 시 안전 legacy 폴백)
- `bin/privyspark-submit` 사용 시 `PRIVYSPARK_DEBUG=true`를 지정하거나, `spark-submit` 직접 실행 시 `spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG=true` 또는 `-Dprivyspark.debug=true`를 지정하면 드라이버 debug 로그에 스캔 계획, 스키마 분할, 그룹/파일 스캔, 리포트 저장 진행사항을 기록
- 그룹/집계 폴백 발생 시 원인과 실행 경로를 드라이버 로그에 기록
- 지원 확장자: `csv`, `json`, `jsonl`, `ndjson`, `parquet`, `orc` (그 외 포맷은 오류 리포트로 분류)
- CSV는 헤더 유무를 자동 감지한다. 헤더가 있으면 헤더명 기반 시그니처를 사용하고, 헤더가 없으면 컬럼 수 기반 시그니처와 Spark 기본 `_c0`, `_c1`, ... 컬럼명을 사용한다. plain-text 2행 tie-case는 header 쪽으로 보수 처리한다.
- 샘플링 지원(`--sample-ratio`, 기본값 `0.2`, 비결정적 랜덤)
- 결과 출력: Parquet + CSV (Spark 기본 포맷)
- 실패 파일은 스킵하고 별도 오류 리포트 생성
- PII 원문값 저장 금지(파일/컬럼/집계 정보만 저장)

## 프로젝트 구조
- `src/main/scala/io/github/jonggeun2001/privyspark`: 애플리케이션 코드
- `src/test/scala/io/github/jonggeun2001/privyspark`: 테스트 코드
- `src/test/resources/datasets`: 검증용 테스트 데이터셋
- `config/rules/default.yaml`: 기본 규칙셋
- `bin/privyspark-submit`: YARN cluster 제출 스크립트
- `docs/PRD-Functional.md`: 기능 요구사항 문서
- `docs/PRD-Architecture.md`: 아키텍처 요구사항 문서

## 빌드 타겟 버전
- Spark: `3.5.3`
- Scala: `2.12`
- JVM 바이트코드 타겟: `1.8`

## 빌드
```bash
./gradlew clean shadowJar
```

## 테스트
```bash
./gradlew test
```

## YARN Cluster 실행
```bash
PRIVYSPARK_DEBUG=true \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --sample-ratio 0.2
```

스크립트는 `spark-submit --master yarn --deploy-mode cluster`를 기본 사용합니다.
오프라인 YARN 환경 대응을 위해 기본적으로 `--packages`를 사용하지 않으며, Shadow fat JAR(`*-all.jar`)를 제출합니다.
또한 기본 규칙 파일(`config/rules/default.yaml`)을 `--files`로 YARN 드라이버에 배포합니다.

## spark-submit 직접 실행
```bash
spark-submit \
  --class io.github.jonggeun2001.privyspark.PrivySparkApp \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.yarn.appMasterEnv.PRIVYSPARK_DEBUG=true \
  --files /abs/path/config/rules/default.yaml#default-rules.yaml \
  /abs/path/privyspark-v0.1.1-all.jar \
  scan \
  --path hdfs:///data/input \
  --output hdfs:///data/privyspark-report \
  --ruleset default \
  --sample-ratio 0.2
```

커스텀 ruleset 사용 시 `--files /abs/path/my-rules.yaml#my-rules.yaml`와 `--ruleset my-rules.yaml`를 함께 지정합니다.
debug 로그가 필요 없으면 `PRIVYSPARK_DEBUG`를 생략하면 됩니다.
debug 로그를 끄더라도 스캔 요약과 fallback 로그는 계속 출력됩니다.


## GitHub Release 산출물
- 태그 `v*` 또는 bare semver(`0.1.3` 형식) 푸시 시 GitHub Actions가 `./gradlew clean shadowJar`를 실행합니다.
- 릴리즈 자산 파일명은 태그를 포함한 `privyspark-<tag>-all.jar` 및 `privyspark-<tag>-all.jar.sha256` 형식으로 업로드됩니다.

예시:
```bash
git tag v0.1.0
git push origin v0.1.0

git tag 0.1.3
git push origin 0.1.3
```

## 규칙셋 파일 형식
`config/rules/default.yaml` 예시:
```yaml
rules:
  - pii_type: name
    regex: '(남궁|선우|독고|사공|제갈|황보|김|이|박|최|정|강|조|윤|장|임|한|오|서|신|권|황|안|송|류|전|홍|고|문|양|손|배|백|허|유|남|심|노|하|곽|성|차|주|우|구|민|진|나|지|엄|채|원|천|방|공|현|함|변|염|여|추|도|석|선|설|마|길|연|위|표|명|기|반|라|왕|금|옥|육|인|맹|제|모|탁|국|어|은|편|봉|피|경|사|가)[가-힣]{1,2}'
    validator: korean_name_dict
  - pii_type: email
    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}'
    column_hints:
      - email
      - mail
```

`column_hints`는 커스텀 ruleset에서 선택적으로 사용하는 필드입니다. 지정하면 컬럼명에 해당 힌트가 포함된 컬럼에만 규칙을 적용하고, 생략하면 기존처럼 모든 컬럼을 검사합니다.

`validator`도 선택 필드이며 현재는 `korean_name_dict`만 지원합니다. 이 validator는 정규식으로 먼저 성씨 prefix 기반 후보를 좁힌 뒤, 셀 안의 이름 후보 substring을 한국인 이름 음절 사전으로 다시 검증해 `"김치찌개"`, `"이사회"` 같은 오탐을 줄입니다. 기본 ruleset의 `name` 규칙이 이 validator를 사용합니다.
