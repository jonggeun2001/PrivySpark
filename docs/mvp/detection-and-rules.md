# 탐지와 규칙셋

## 탐지 모델
- MVP는 regex 기반 후보 탐지를 사용합니다.
- 필요한 타입에는 추가 strict validator를 적용합니다.
- 결과 집계는 컬럼 단위 또는 파일 단위로 수행됩니다.

## 기본 ruleset
- 기본 파일: `config/rules/default.yaml`
- 기본 탐지 타입: 전화번호, 이메일, 주민등록번호, 외국인 등록번호, 운전면허번호, 주소, 계좌번호, 카드번호, 한국 여권번호, IP

## 커스텀 ruleset 규칙
- 각 rule은 `pii_type`, `regex`를 포함해야 합니다.
- `column_hints`는 선택 항목이며, 지정 시 힌트가 포함된 컬럼에만 규칙을 적용합니다.
- `match_type`은 선택 항목이며 기본값은 `value`입니다.
- invalid regex는 ruleset 로드 단계에서 즉시 거부되며, 스캔 시작 전에 `IllegalArgumentException`으로 실패합니다.

## 지원하지 않는 규칙
- `pii_type: name`
- `validator` 필드
- `__KOREAN_NAME_RULE_REGEX__` 내부 참조

## match_type
- `value`: regex에 매칭되는 값 개수를 집계합니다.
- `full_column`: 비어 있지 않은 값 전체가 regex를 만족하는 컬럼/파일에 대해서만 결과를 생성합니다.
- 내부 `text` fallback 포맷에서는 자유 형식 텍스트 한 줄 전체가 값이 되므로, `full_column`도 `value`처럼 부분 매치 집계로 처리합니다.

## 타입별 제약
- `phone_number`: 국내 `010`/`011`/`016`/`017`/`018`/`019` 형식과 `+82 10...` 계열 국제 표기(`+82-10-1234-5678`, `+821012345678`)를 검출
- `resident_registration_number`: 하이픈 포함/미포함 입력 모두 허용, 성별/세기 코드 1자리 축약형 허용, 더 긴 숫자 토큰 내부 substring 제외
- `resident_registration_number`: 기본 ruleset은 월 `01`~`12`, 일 `01`~`31` 범위만 허용
- `driver_license_number`: 하이픈 포함/미포함 입력 모두 허용, 구형 10자리 또는 현행 12자리만 strict 검증, 현행 지역코드는 `11`~`26`, `28`만 허용
- `passport_number`: 한국 여권번호 형식만 검출, 영숫자 토큰 내부 substring 제외

## 집계 전략
- 기본 경로는 batched aggregation(`agg`)입니다.
- 표현식 수가 임계치(`50,000`)를 넘으면 소배치 fallback으로 전환합니다.
- 집계 예외가 나면 safe legacy fallback으로 전환합니다.
- 파일 단위 집계 시에는 내부 동적 파일 식별 컬럼을 추가해 원본 컬럼 충돌을 피합니다.
