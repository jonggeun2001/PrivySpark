# ruleset과 탐지 모델

## 탐지 방식
- 탐지는 regex 기반 후보 탐지와 타입별 strict validator 조합입니다.
- 결과 집계는 컬럼 단위 또는 파일 단위로 수행됩니다.
- invalid regex는 ruleset 로드 단계에서 즉시 실패합니다.

ruleset을 로드할 때 regex를 미리 검증하는 이유는 스캔이 한참 진행된 뒤에 잘못된 정규식 때문에 실패하는 상황을 막기 위해서입니다. 긴 배치 작업에서는 시작 전 실패가 운영적으로 훨씬 낫습니다.

## 기본 ruleset
- 기본 파일: `config/rules/default.yaml`
- 기본 탐지 타입:
  - 전화번호
  - 이메일
  - 주민등록번호
  - 외국인등록번호
  - 운전면허번호
  - 주소
  - 계좌번호
  - 카드번호
  - 한국 여권번호
  - IP 주소

## 커스텀 ruleset 규칙
- 각 rule은 `pii_type`, `regex`를 포함해야 합니다.
- `column_hints`는 선택 항목이며, 지정 시 힌트가 포함된 컬럼에만 적용합니다.
- `match_type`은 선택 항목이며 기본값은 `value`입니다.
- 허용 `match_type` 값은 `value`, `full_column`입니다.

## 지원하지 않는 규칙
- `pii_type: name`
- `validator` 필드
- `__KOREAN_NAME_RULE_REGEX__` 내부 참조

## `match_type`
- `value`: regex에 매칭되는 값 개수를 집계합니다.
- `full_column`: 각 비어 있지 않은 값을 regex 전체 일치 기준으로 평가합니다.
- 내부 `text` fallback 포맷에서도 각 줄 전체를 하나의 값으로 보고 `full_column`의 전체 일치 규칙을 그대로 적용합니다.

`full_column`을 따로 둔 이유는 주민등록번호처럼 값 전체가 특정 포맷이어야 하는 탐지와, 자유 텍스트 안 substring 검출을 같은 규칙으로 처리하면 오탐이 크게 늘어나기 때문입니다.

## 타입별 제약
- `phone_number`: 국내 `010`/`011`/`016`/`017`/`018`/`019`와 `+82 10...` 계열 국제 표기를 검출합니다.
- `resident_registration_number`: 하이픈 포함/미포함 입력을 허용하고, 성별/세기 코드 1자리 축약형도 허용합니다.
- `resident_registration_number`: 기본 ruleset은 월 `01`~`12`, 일 `01`~`31` 범위만 허용하고, 더 긴 숫자 토큰 내부 substring은 제외합니다.
- `driver_license_number`: 하이픈 포함/미포함 입력을 허용하고, 구형 10자리 또는 현행 12자리만 strict 검증합니다. 현행 지역코드는 `11`~`26`, `28`만 허용합니다.
- `passport_number`: 한국 여권번호 형식만 검출하며, 영숫자 토큰 내부 substring은 제외합니다.

## 집계 전략
- 기본 경로는 batched aggregation(`agg`)입니다.
- 표현식 수가 임계치(`50,000`)를 넘으면 소배치 fallback으로 전환합니다.
- 집계 예외가 나면 safe legacy fallback으로 전환합니다.
- 파일 단위 집계 시 내부 동적 파일 식별 컬럼을 추가해 원본 컬럼 충돌을 피합니다.

배치 집계를 기본으로 두는 이유는 메트릭마다 `filter().count()`를 반복하면 Spark job 수가 급격히 늘기 때문입니다. 현재 구현은 메트릭을 묶어서 처리해 스캔 횟수와 scheduler 오버헤드를 줄입니다.
