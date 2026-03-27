# 외국인 등록번호 기본 검출 규칙 추가 설계

## 목표
- PrivySpark 기본 ruleset이 외국인 등록번호 형식도 기본 PII 타입으로 탐지하도록 확장한다.

## 배경
- 현재 기본 ruleset은 주민등록번호, 전화번호, 이메일 등 한국 포맷 중심의 기본 규칙을 제공한다.
- 외국인 등록번호는 별도 `pii_type`으로 리포트에 구분되어야 하며, 이번 범위는 별도 validator나 체크디짓 없이 기본 정규식 규칙 추가로 한정한다.

## 설계 결정

### 1. 탐지 방식
- `config/rules/default.yaml`에 `foreign_registration_number` 규칙을 추가한다.
- 구현 방식은 기존 아키텍처를 따른다. `RulesetLoader`와 `DetectionAggregator`는 규칙의 `pii_type`과 `regex`를 일반화된 방식으로 처리하므로 신규 코드 경로는 필요 없다.

### 2. 정규식 범위
- 외국인 등록번호는 `YYMMDD-?5/6/7/8XXXXXX` 형식의 기본 패턴을 대상으로 한다.
- 하이픈 유무는 모두 허용한다.
- 이번 변경에서는 체크디짓 검증, 주민등록번호 규칙과의 관계 재정의, 후처리 validator는 도입하지 않는다.

### 3. 테스트 범위
- 기본 ruleset 로드 테스트가 `foreign_registration_number` 규칙 존재를 검증하도록 확장한다.
- 목적은 기본 번들 규칙 누락 회귀를 막는 것이다.

### 4. 문서 범위
- `README.md`, `docs/PRD-Functional.md`, `docs/PRD-Architecture.md`에 기본 탐지 타입 목록을 갱신한다.
- 사용자 관점에서 기본 제공 탐지 범위가 바뀌므로 요구사항 문서와 사용 문서를 함께 맞춘다.

## 제외 범위
- 체크디짓 또는 국적/성별 코드 정합성 검증
- 커스텀 validator 재도입
- 탐지 집계 로직 변경
- 출력 스키마 변경

## 검증 계획
- `RulesetLoaderSpec`에 기본 ruleset 회귀 테스트를 추가한다.
- 전체 회귀는 `./gradlew test`로 확인한다.
