# CLI Parallelism Design

## Goal
- 사용자가 앱 실행 인자로 그룹 스캔 병렬도와 파일 폴백 병렬도를 직접 지정할 수 있게 한다.

## Scope
- `privyspark scan` CLI에 `--group-parallelism`, `--file-parallelism` 옵션을 추가한다.
- 두 옵션은 양의 정수만 허용한다.
- 런타임에서는 CLI 병렬도 값을 `scanGroups`와 `scanGroupByFile` 호출 경로에 직접 전달하고, 값이 없을 때만 기존 Spark conf 기반 병렬도 해석을 사용한다.
- `bin/privyspark-submit`, `README.md`, `docs/PRD-Functional.md`, `docs/PRD-Architecture.md`에 새 옵션을 반영한다.

## Design
- `CliConfig`에 `groupParallelism`, `fileParallelism` 필드를 추가하고 기본값은 미지정 상태를 표현하는 `None`으로 둔다.
- `Cli.scala`에서 두 옵션을 파싱하고 `> 0` 검증을 수행한다.
- `PrivySparkApp.scala`는 CLI 병렬도 값을 `(groupParallelism, fileParallelism)` 튜플로 정규화하고, `runScan -> scanGroups -> scanGroup -> scanGroupByFile` 경로로 직접 전달한다.
- 실제 병렬도 계산 함수(`resolveGroupParallelism`, `resolveFileParallelism`)는 그대로 두어 기존 동작과 테스트 자산을 최대한 재사용한다.

## Testing
- CLI 테스트에서 기본값, 옵션 파싱, 유효성 실패를 검증한다.
- 앱 단위 테스트에서 CLI 병렬도 정규화 helper를 검증한다.

## Risks
- CLI와 Spark conf 우선순위가 모호해질 수 있다. 이번 변경에서는 CLI가 주어지면 직접 전달값이 우선하고, 미지정 시 Spark conf/default를 사용하도록 명시한다.
- 병렬도 `1`은 합법 값이므로 순차 실행과 구분해 허용해야 한다.
