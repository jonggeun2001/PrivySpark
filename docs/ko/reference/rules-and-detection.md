# ruleset과 탐지 모델

## 탐지 방식

- 탐지는 ruleset regex 결과를 집계와 sample 추출에 그대로 사용합니다. 타입별 추가 strict validator나 checksum 검증은 수행하지 않으며, `validator` 필드는 로드 단계에서 거부합니다.
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

### 값 전체가 64자리 16진수인 경우

기본 ruleset의 모든 타입은 원문 값 전체가 `\A[0-9A-Fa-f]{64}\z`에 일치하면 탐지하지 않습니다. SHA-256 해시 문자열 내부의 숫자를 개인정보로 오탐하는 것을 줄이기 위한 조건이며, 대문자·소문자·숫자만으로 구성된 값도 포함합니다.

| 원문 값 | 처리 |
| --- | --- |
| 정확히 64자리 ASCII 16진수 | 모든 기본 타입에서 제외 |
| 63자리 또는 65자리 16진수 | 기존 규칙 적용 |
| 64자리 16진수 앞뒤에 공백·개행이 있는 값 | 기존 규칙 적용 |
| `0x`, `0X`, `hash=` 접두사 또는 다른 문장이 붙은 값 | 기존 규칙 적용 |

공백을 제거하거나 접두사를 해석하지 않고 값 전체로 판단합니다. 이 조건은 SHA-256으로 생성됐다는 증명이 아닌 문자열 형식 검사이므로, 같은 형태의 일반 식별자도 제외됩니다. 문장 속 해시 토큰을 따로 제외하지 않으므로 혼합 텍스트에서는 해시 내부 오탐이 남을 수 있습니다.

각 기본 정규식 전체를 아래처럼 감싸서 구현합니다. 뒤쪽의 제외 조건은 부분 검색이 값 중간에서 시작하더라도 값 전체의 64자리 조건을 검사하며, 기존 매치 조각과 위치는 유지합니다.

```regex
(?:기존정규식)(?<!\A(?=[0-9A-Fa-f]{64}\z)[0-9A-Fa-f]{0,64})
```

제외된 값은 탐지 건수와 sample 후보에 포함되지 않지만, 비어 있지 않은 값의 전체 개수에는 그대로 포함됩니다. 이 정책은 `config/rules/default.yaml`에만 있으며 엔진·규칙 스키마·의존성은 변경하지 않습니다. 커스텀 ruleset에는 자동 적용하지 않습니다.

회귀 검증은 `RulesetLoaderSpec`에서 모든 기본 규칙의 제외 조건과 경계 입력을, `DetectionAggregatorSpec`에서 실제 Spark `sha2(..., 256)` 결과의 집계·sample 제외와 커스텀 규칙 적용을 확인합니다. 전체 검증은 `bash scripts/verify-worktree.sh`로 실행합니다.

## 커스텀 ruleset 규칙

- top-level `rules`에는 최소 한 개의 규칙이 있어야 하며, 각 rule은 비어 있지 않은 `pii_type`, `regex`를 포함해야 합니다.
- `column_hints`는 선택 항목이며, 컬럼명과 힌트를 trim/소문자로 정규화한 뒤 힌트 하나 이상이 부분 문자열로 포함된 컬럼에만 적용합니다.
- `match_type`은 선택 항목이며 기본값은 `value`입니다.
- top-level `suppressions`는 선택 항목이며 특정 `(column, pii_type)` 결과를 제외합니다. 각 항목은 단일 `column` 또는 여러 `columns`를 사용할 수 있습니다.
- 허용 `match_type` 값은 `value`, `full_column`입니다.

## 오탐 제외 (Suppressions)

특정 컬럼이 특정 탐지 타입에만 반복적으로 오탐되는 경우, ruleset은 그대로 유지한 채 결과만 제외할 수 있습니다. suppression은 컬럼명 대소문자를 무시한 exact match와 `pii_type` exact match를 함께 써서 적용합니다.

### YAML 설정

```yaml
rules:
  - pii_type: driver_license_number
    regex: '...'

suppressions:
  - columns: [tr_dt, trade_time]
    pii_type: driver_license_number
  - column: PRDCTCD
    pii_type: phone_number
```

`column`에도 YAML 배열을 쓸 수 있지만, 하나의 suppression 항목이 여러 컬럼으로 펼쳐질 때는 `columns`를 권장합니다.

### CLI 로 추가 suppression

- `--suppress <column:pii_type>`는 반복 지정 가능합니다.
- `--suppression-file <path>`는 UTF-8 텍스트 파일을 줄 단위 `column:pii_type` 형식으로 읽고, 빈 줄과 `#` 주석을 무시합니다.
- CLI/file 형식에서는 마지막 `:`를 구분자로 사용하므로 컬럼명 안에 `:`가 포함돼도 표현할 수 있습니다.
- CLI suppression은 ruleset `suppressions:`를 대체하지 않고 union으로 합쳐집니다.
- YARN cluster에서 client 로컬 suppression 파일을 쓰려면 `--files` 또는 `PRIVYSPARK_SPARK_FILES`로 먼저 배포한 뒤 alias를 `--suppression-file`에 넘겨야 합니다.

예시:

```bash
PRIVYSPARK_SPARK_FILES=/abs/path/scan.suppressions#scan.suppressions,config/rules/default.yaml#default-rules.yaml \
bin/privyspark-submit \
  scan \
  --path /abs/input \
  --output /abs/output \
  --ruleset default \
  --suppress prdctcd:driver_license_number \
  --suppression-file scan.suppressions
```

### 매칭 규칙

- 컬럼명은 trim 후 소문자로 정규화한 값으로 exact match 합니다.
- `pii_type`은 trim 후 exact match 합니다.
- ruleset YAML의 `column`/`columns` 배열은 컬럼별 `(column, pii_type)` suppression으로 펼쳐집니다.
- suppression은 metric plan 생성 전에 적용되므로, 제외된 조합은 `match_count`, `match_ratio`, `confidence`, `sample_raw_value`, `sample_matched_fragment` 결과 자체가 생기지 않습니다.
- 같은 컬럼이라도 suppression에 지정되지 않은 다른 `pii_type`은 계속 탐지됩니다.

### 동작과 한계

- ruleset에 없는 `pii_type`을 suppression에 적어도 스캔을 실패시키지 않고 warning만 남깁니다. 같은 suppression 파일을 여러 ruleset에 재사용하기 위한 동작입니다.
- suppression은 value-regex 기반 예외나 컬럼명 glob/regex 매칭을 지원하지 않습니다.
- suppression 변경 후에는 기존 결과 파일을 신뢰하지 말고 스캔을 다시 실행해야 합니다.

## 지원하지 않는 규칙

- `pii_type: name`
- `validator` 필드
- `__KOREAN_NAME_RULE_REGEX__` 내부 참조

## `match_type`

- `value`: regex에 부분 일치하는 비어 있지 않은 값의 개수를 집계합니다. 한 셀에서 여러 번 일치해도 그 행/컬럼은 1건입니다.
- `full_column`: 각 비어 있지 않은 값을 regex 전체 일치 기준으로 평가합니다.
- 내부 `text` fallback 포맷에서도 각 줄 전체를 하나의 값으로 보고 `full_column`의 전체 일치 규칙을 그대로 적용합니다.

`full_column`을 따로 둔 이유는 주민등록번호처럼 값 전체가 특정 포맷이어야 하는 탐지와, 자유 텍스트 안 substring 검출을 같은 규칙으로 처리하면 오탐이 크게 늘어나기 때문입니다.

## 타입별 제약

- `phone_number`: 국내 `010`/`011`/`016`/`017`/`018`/`019`와 `+82-10-...` 또는 하이픈 없는 `+8210...` 계열 국제 표기를 검출합니다. 기본 regex는 번호 내부 공백을 허용하지 않습니다.
- `email`: 토큰 경계를 적용하고, 마지막 TLD는 영문 2자 이상으로 제한해 잘못 붙은 suffix나 비정상 도메인 오탐을 줄입니다.
- `resident_registration_number`: 하이픈 포함/미포함 입력을 허용하고, 성별/세기 코드 1자리 축약형도 허용합니다.
- `resident_registration_number`: 기본 ruleset은 월 `01`~`12`, 일 `01`~`31` 범위만 허용하고, 더 긴 숫자 토큰 내부 substring은 제외합니다. 하이픈 없는 7자리 축약형에는 아래의 16진수 경계 조건도 적용합니다.
- `foreign_registration_number`: 주민등록번호와 같은 방식으로 월 `01`~`12`, 일 `01`~`31` 범위를 제한하고, 7번째 자리는 외국인등록번호 코드 `5`~`8`만 허용합니다.
- `driver_license_number`: regex 단계에서 구형 하이픈 포함 10자리 형식, 현행 12자리 형식, 그리고 2014년 7월 2일 이전 지역명 표기(`서울 00 - 123456 - 01`, `부산0012345601` 등)를 허용합니다. 현행 숫자 지역코드는 `11`~`26`, `28`만 허용하고, 지역명 표기는 한국도로교통공단 안내에 나온 `서울`, `부산`, `경기`, `강원`, `충북`, `충남`, `전북`, `전남`, `경북`, `경남`, `제주`, `대구`, `인천`, `광주`, `대전`, `울산`만 허용합니다. 기본 ruleset은 구형 숫자-only 10자리 형식을 제외해 `full_column`에서 다른 10자리 숫자 식별자와 충돌하는 오탐을 줄이고, `27-12-345678-90` 같은 잘못된 현행 형식 내부에서만 구형 하이픈 형식 substring 재매치를 막습니다. 런타임 집계와 sample 추출은 이제 설정된 regex 결과를 그대로 따릅니다.
- `address`: 주소는 자유 서술 편차가 커서 기본 ruleset에서도 상대적으로 보수적으로 유지합니다. 다른 타입처럼 과도하게 조이면 정상 한국 주소 누락이 빠르게 늘어납니다.
- `bank_account_number`: 하이픈 포함 계좌번호 형식을 유지하되 `YYYY-MM-DD` 같은 날짜 패턴이 계좌번호로 오탐되는 경우를 줄이도록 세그먼트 길이를 조정했습니다.
- `credit_card_number`: 흔한 16자리 카드 issuer prefix 중심으로 제한하고, Mastercard 2-series는 `2221`~`2720` 범위만 허용하며, 더 긴 숫자 토큰 내부 substring은 제외합니다.
- `passport_number`: 한국 여권번호 형식만 검출하며, 영숫자 토큰 내부 substring과 `00000000` 같은 비정상 serial은 제외합니다.
- `ip_address`: IPv4 범위를 유지하면서 `10.0.0.1.5` 같은 더 긴 dotted token 내부 substring은 제외하되, 문장 끝 `192.168.0.1.` 같은 일반 표기는 계속 검출합니다.

이 기본 ruleset 조정 방향은 모든 타입을 주민등록번호처럼 과도하게 조이는 것이 아니라, 형식 규격이 명확한 한국 식별자는 더 강하게 제한하고, 변형이 많은 타입은 경계 조건 위주로만 보강하는 쪽을 택한 것입니다. 오탐을 줄이되 정상값 누락이 급격히 늘어나는 변화는 피하려는 의도입니다.

### 주민등록번호 7자리 축약형의 16진수 경계

연속된 16진수 난수 안의 숫자 7자리가 주민등록번호로 오탐되는 경우를 줄이기 위해, 기본 ruleset은 하이픈 없는 7자리 축약형의 바로 앞뒤가 `0-9`, `a-f`, `A-F`이면 매치하지 않습니다. 정규식의 `(?<![0-9A-Fa-f])`와 `(?![0-9A-Fa-f])`로 표현하며, 바로 앞의 `0x`·`0X` 접두사는 `(?<!0[xX])`로 별도 제외합니다.

13자리 전체형과 하이픈 있는 7자리 축약형은 기존 숫자 경계를 유지하되, 위의 값 전체 64자리 16진수 제외 조건은 모든 분기에 적용합니다. 이 구분은 `config/rules/default.yaml`의 정규식 분기 안에 있으며, 검출 엔진의 타입별 예외나 ruleset 스키마를 추가하지 않습니다. 커스텀 ruleset은 계속 작성한 정규식 그대로 동작합니다.

다음은 모두 합성 예시이며, 기본 `value` 매칭 기준입니다.

| 입력 | 주민등록번호 매치 |
| --- | --- |
| `9012251`, `rrn=9012251`, `주민번호9012251입니다` | 유지 |
| `901225-1`, `ab901225-1cd` | 유지 |
| `ab9012251234567cd`, `ab901225-1234567cd` | 유지 |
| `ab9012251cd`, `9012251ABCDEF` | 제외 |
| `0x9012251`, `0X9012251` | 제외 |

이는 주변 문자에 따른 휴리스틱입니다. `code9012251`도 바로 앞의 `e` 때문에 제외되므로 영문에 붙인 실제 축약형이 누락될 수 있습니다. 난수가 독립된 7자리 값인 경우에는 실제 축약형과 구분할 수 없으며, `deadbeef-9012251-cafebabe` 같은 하이픈으로 나뉜 식별자는 이 경계 검사로 제외하지 않습니다. 한 셀에 제외 대상과 다른 정상 매치가 함께 있으면 정상 매치는 계속 집계와 sample 추출에 사용합니다.

회귀 검증은 `RulesetLoaderSpec`에서 기본 정규식 경계와 기존 형식 보존을, `DetectionAggregatorSpec`에서 집계·sample 추출과 커스텀 주민등록번호 정규식 적용을 확인합니다. 전체 검증은 `bash scripts/verify-worktree.sh`로 실행합니다.

## 집계 전략

- 기본 경로는 batched aggregation(`agg`)이며, 기본 배치당 최대 표현식 수는 `400`입니다.
- 표현식 수가 임계치(`50,000`)를 넘으면 소배치 fallback으로 전환합니다.
- 집계 예외가 나면 safe legacy fallback으로 전환합니다.
- 파일 단위 집계 시 내부 동적 파일 식별 컬럼을 추가해 원본 컬럼 충돌을 피합니다.

배치 집계를 기본으로 두는 이유는 메트릭마다 `filter().count()`를 반복하면 Spark job 수가 급격히 늘기 때문입니다. 현재 구현은 메트릭을 묶어서 처리해 스캔 횟수와 scheduler 오버헤드를 줄입니다.
