# PrivySpark

PrivySpark는 Spark 기반 배치 스캐너로, 데이터셋에서 잠재적 개인정보(PII)를 정규식으로 탐지해 리포트를 생성합니다.

## 현재 범위 (MVP v0.1)
- 일회성 배치 실행
- 입력/출력 경로는 절대경로(또는 URI)만 허용
- 파일 단위 스캔
- 외부 규칙 파일 기반 정규식 탐지
- 지원 확장자: `csv`, `json`, `jsonl`, `ndjson`, `parquet` (그 외 포맷은 오류 리포트로 분류)
- 샘플링 지원(`--sample-ratio`, 기본값 `0.2`, 비결정적 랜덤)
- 결과 출력: Parquet + Excel
- 실패 파일은 스킵하고 별도 오류 리포트 생성
- PII 원문값 저장 금지(파일/컬럼/집계 정보만 저장)

## 프로젝트 구조
- `src/main/scala/io/github/jonggeun2001/privyspark`: 애플리케이션 코드
- `src/test/scala/io/github/jonggeun2001/privyspark`: 테스트 코드
- `config/rules/default.yaml`: 기본 규칙셋
- `bin/privyspark-submit`: YARN cluster 제출 스크립트
- `docs/PRD.md`: 요구사항 문서

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


## GitHub Release 산출물
- 태그 `v*` 푸시 시 GitHub Actions가 `./gradlew clean shadowJar`를 실행하고 Shadow JAR(`*-all.jar`) + SHA256 파일을 Release 자산으로 업로드합니다.
- 수동 실행은 GitHub Actions의 `Release Artifact` 워크플로우에서 **이미 존재하는 태그**(`tag`)를 입력해 실행합니다.

예시:
```bash
git tag v0.1.0
git push origin v0.1.0
```

## 규칙셋 파일 형식
`config/rules/default.yaml` 예시:
```yaml
rules:
  - pii_type: email
    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}'
```
