# PrivySpark 문서

이 디렉토리는 PrivySpark 공개 문서의 한국어 기준본입니다. 기능 설명, 아키텍처, 운영 절차, 성능 가이드를 사용자 관점으로 정리합니다.

영어 대응 문서는 [../en/README.md](../en/README.md)에서 볼 수 있습니다.

## 시작하기
- [getting-started/quick-start.md](getting-started/quick-start.md): 빌드, 테스트, YARN 제출, 커스텀 ruleset 배포

## 참조
- [reference/overview.md](reference/overview.md): 지원 범위와 핵심 동작 요약
- [reference/input-formats.md](reference/input-formats.md): 입력 포맷, archive/xlsx 처리, 그룹화, fallback
- [reference/rules-and-detection.md](reference/rules-and-detection.md): ruleset 규칙, `match_type`, 탐지 타입, 집계 전략
- [reference/reports-and-errors.md](reference/reports-and-errors.md): 최종 출력, 오류 리포트, 샘플 값 저장 정책
- [reference/review-workflow.md](reference/review-workflow.md): false positive 검토, allowlist 생성, 재스캔 suppression
- [reference/offline-review-collector.md](reference/offline-review-collector.md): 서버 없는 담당자 리뷰, response JSON 회수, 누적 review state 운영

## 아키텍처
- [architecture/overview.md](architecture/overview.md): 컴포넌트 맵, 처리 플로우, 운영 불변 조건

## 운영
- [operations/execution.md](operations/execution.md): CLI 옵션, 병렬도, 샘플링, 로그, progress 경로, 릴리즈
- [operations/performance.md](operations/performance.md): 현재 성능 특성, 튜닝 포인트, Spark/YARN 운영 주의사항
