# Earie Go 서버 작업 지침

## 프로젝트

Go 모듈은 temp-ws이며 go.mod는 Go 1.24.2를 지정한다. WebSocket 게임 서버와 음악 조회 코드가 있다.
- cmd/server: HTTP/WebSocket 진입점과 의존성 조립.
- internal/client: 연결별 송수신.
- internal/room: Hub, Room 이벤트 루프, 참가자/게임 조합.
- internal/game: 라운드·정답·카드 등 게임 규칙.
- internal/message: 메시지 봉투와 payload.
- internal/music: 음악 조건·PostgreSQL 조회.
- cmd/populate: 데이터 적재. 일반 구현 검증 목적으로 실행하지 않는다.

## 작업 시작

저장소 상태와 기존 변경을 확인한다. 사용자의 untracked 파일과 빌드 산출물을 임의로 삭제하거나 커밋하지 않는다.
랜덤 매칭은 docs/specs/random-matchmaking/README.md와 할당된 tasks 항목을 먼저 읽는다. Draft 계약을 구현 확정으로 취급하지 않는다.
작업별 전용 worktree를 사용하고 선행 PR 병합/계약 버전이 확인된 작업만 시작한다. 브랜치 기본 접두사는 codex/다.
다른 에이전트가 담당하는 파일이나 계약을 임의로 변경하지 않는다. 필요하면 영향을 설명하고 조정한다.

## 구현 원칙

기존 패키지 경계를 따른다. 상태 소유자와 동시성 경계를 명시한다. Room 상태는 이벤트 루프를 통해 변경한다.
채널 종료·goroutine 종료·context 취소·중복 정리·끊어진 연결을 다룬다. 비동기 결과를 기다리며 중앙 매칭 루프를 막지 않는다.
변경한 Go 파일에 gofmt를 적용한다. 관련 오류를 처리하고 현재 golangci-lint 규칙을 준수한다.
사용자가 정하지 않은 제품 정책을 구현 편의로 추가하지 않는다.

## 검증

저장소 루트에서 실행한다.
- go test ./... : 단위/회귀 테스트.
- go test -race ./... : 동시성 변경 시 race 검증.
- go build ./... : 전체 빌드.
- golangci-lint run : .golangci.yml 기반 린트 (현재 CI v2.1.6).
현재 GitHub Actions는 빌드/린트만 실행한다. CI 성공을 테스트 실행 증거로 쓰지 않는다.
테스트는 *_test.go에 작성하고 경쟁 상황은 이벤트 순서를 제어해 재현한다. 시간 sleep에 기대지 않는다.
환경 문제로 실행하지 못한 검증은 통과로 기록하지 않는다.

## 로컬 실행과 비밀

서버 실행에는 CATALOG_PATH(서버 catalog.json)와 SONG_MANIFEST_PATH(같은 버전 manifest)가 필요하다. DB와 DATABASE_URL은 필요 없다. 누락·오류면 서버가 시작하지 않으며 synthetic fixture로 자동 대체하지 않는다.
run.sh는 .env가 있으면 읽고, 두 변수를 export해 go run ./cmd/server를 실행한다. 둘 중 하나라도 없으면 실행하지 않는다.
DATABASE_URL은 cmd/populate처럼 DB를 직접 다루는 작업에만 쓴다.
.env, DB 연결 문자열, 인증 정보와 음악 원본 데이터를 문서/로그/커밋에 노출하지 않는다. 실제 DB 수정이나 populate 실행은 해당 작업에 명시적으로 포함된 경우에만 한다.

## PR

[Feat], [Fix], [Refactor], [Test], [Chore] 등 기존 접두사를 사용한다. 관련 이슈, 요구사항 ID, 변경 이유, 테스트 결과를 작성한다.
작업 완료는 구현·검증을 의미하고 자동 병합/배포를 의미하지 않는다. 사용자 또는 작업 지시의 권한을 따른다.
