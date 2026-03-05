# Repository Guidelines

## 프로젝트 구조 및 모듈 구성
- `src/main/scala/io/github/jonggeun2001/privyspark`: 앱 코드
  - `PrivySparkApp.scala`: 엔트리포인트
  - `config/RulesetLoader.scala`: 외부 규칙셋 로더
  - `model/Models.scala`: 리포트/규칙 데이터 모델
- `src/test/scala/io/github/jonggeun2001/privyspark`: 단위 테스트
- `config/rules/default.yaml`: 기본 정규식 규칙셋
- `bin/privyspark-submit`: YARN cluster 제출 스크립트
- `docs/PRD.md`: 제품 요구사항

## 빌드, 테스트, 개발 명령어
- `./gradlew clean shadowJar`: Shadow fat JAR 생성(`build/libs/*-all.jar`)
- `./gradlew test`: 테스트 실행
- `bin/privyspark-submit scan --path /abs/input --output /abs/output --ruleset default --sample-ratio 0.2`: YARN cluster 실행
- `rg --files`: 파일 구조 빠른 탐색
- `git tag v0.1.0 && git push origin v0.1.0`: Release Artifact 워크플로우 트리거

`--path`, `--output`은 절대경로(또는 URI)만 허용합니다.

## 코딩 스타일 및 네이밍 규칙
- Scala 2.12 기준, 들여쓰기 2칸.
- 패키지 루트는 `io.github.jonggeun2001.privyspark`를 유지.
- 클래스/오브젝트는 PascalCase, 메서드/변수는 camelCase.
- 모델 필드는 리포트 스키마와 동일한 snake_case를 유지(예: `match_ratio`).
- 복잡한 처리(예: 파일 포맷 분기, 에러 처리)는 작은 함수로 분리.

## 테스트 가이드
- 프레임워크: ScalaTest (`AnyFunSuite`).
- 파일명: `*Spec.scala`.
- 최소 포함 항목:
  - 경로 검증(절대/상대)
  - 규칙셋 로드 실패/성공
  - 포맷 미지원 파일 오류 처리
- 실행: `./gradlew test`.

## 커밋 및 PR 가이드
- Conventional Commits 사용: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
- PR에는 다음을 포함:
  - 변경 목적과 범위
  - CLI/출력 스키마 영향
  - 테스트 결과(`./gradlew test`) 또는 미실행 사유
  - 요구사항 변경 시 `README.md`와 `docs/PRD.md` 동시 반영

## Release 운영
- 워크플로우: `.github/workflows/release-artifact.yml`
- 트리거: `v*` 태그 푸시 또는 수동 실행(`workflow_dispatch`)
- 수동 실행 시 입력한 `tag`를 checkout하여 해당 태그 커밋 기준으로 릴리즈 자산 생성
- 결과물: `build/libs/*-all.jar`, `build/libs/*-all.jar.sha256`를 GitHub Release에서 다운로드 가능

## 오프라인 배포
- 기본 제출 스크립트는 `--packages`를 사용하지 않습니다.
- 클러스터 외부 네트워크가 차단된 환경에서는 Shadow fat JAR를 사용합니다.
- Spark 런타임 라이브러리는 클러스터 제공을 전제로 하며(`compileOnly`), 앱 의존성은 Shadow JAR에 포함됩니다.

## 보안 및 설정 주의사항
- 리포트에 실제 PII 원문값은 저장하지 않습니다.
- 규칙셋 변경 시 성능 영향(정규식 비용)과 오탐 리스크를 PR 설명에 명시하세요.
