# 운전면허번호 strict 검출 설계

## 목표
- PrivySpark 기본 ruleset에 `driver_license_number` 검출을 추가하고, 구형 10자리/현행 12자리 운전면허번호를 strict 검증으로 집계한다.

## 배경
- 현재 기본 ruleset은 전화번호, 이메일, 주민등록번호, 외국인 등록번호 등 주요 한국 식별자를 포함하지만 운전면허번호는 기본 탐지 대상에 없다.
- 사용자 요구사항은 하이픈 포함/미포함 입력을 모두 허용하면서 구형 10자리와 현행 12자리 운전면허번호를 엄격히 검증하는 것이다.
- 현재 공개 YAML `validator` 필드는 제거된 상태이므로, 이번 변경은 외부 ruleset 계약을 다시 열지 않고 코드 내부의 내장 validator로 해결한다.

## 설계 결정

### 1. 기본 규칙 추가
- `config/rules/default.yaml`에 `driver_license_number` 후보 regex를 추가한다.
- 후보 regex는 하이픈 포함/미포함 입력을 모두 허용하되 숫자 경계를 둔다.

### 2. strict validator 방식
- `driver_license_number`에 대해서만 코드 내부의 전용 validator를 적용한다.
- validator는 값을 정규화해 하이픈을 제거한 뒤 아래 규칙만 통과시킨다.
  - 구형 번호: 10자리
  - 현행 번호: 12자리, 앞 2자리는 허용 지역코드 `11`~`26`, `28`
- 공개적으로 확인 가능한 자료에서는 별도 체크섬 규칙이 확인되지 않아, 이번 strict 검증은 길이/형식/지역코드 검증으로 제한한다.

### 3. 코드 구조
- 운전면허번호 검증 로직은 `DetectionAggregator` 내부에 직접 넣지 않고 별도 helper 객체로 분리한다.
- `DetectionAggregator`는 rule의 `piiType`에 따라 optional validator를 조회하고, regex predicate에 validator predicate를 추가한다.
- 이 방식은 최근 제거된 YAML `validator` 설정을 되살리지 않으면서도 향후 다른 내장 검증기를 추가할 여지를 남긴다.

### 4. 테스트 범위
- 기본 ruleset 로딩 테스트에서 `driver_license_number` 규칙 존재와 regex를 고정한다.
- validator 단위 테스트에서 정상/비정상 구형/현행 번호를 검증한다.
- 집계기 회귀 테스트에서 regex 후보는 맞지만 strict validator를 통과하지 못하는 값이 집계되지 않는지 확인한다.

### 5. 문서 범위
- `README.md`, `docs/PRD-Functional.md`, `docs/PRD-Architecture.md`에 기본 탐지 타입과 strict 검증 방식을 반영한다.

## 제외 범위
- 공개 YAML `validator` 필드 재도입
- 체크섬 기반 검증
- 운전면허번호 외 다른 PII 타입의 검증 방식 변경

## 참고
- Microsoft Learn, South Korea driver's license number:
  https://learn.microsoft.com/en-us/purview/sit-defn-south-korea-drivers-license-number
