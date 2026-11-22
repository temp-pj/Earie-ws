# 랜덤 매칭 서버 통합 검증 (T9)

이 문서는 [#36](https://github.com/temp-pj/Earie-ws/issues/36) T9의 Go 서버 독립 통합 검증을 다시 실행하는 방법과 각 테스트가 보장하는 범위를 기록한다. 기준 명세는 `docs/specs/random-matchmaking/`(계약 SHA `7683c2507dad2c61da83bbede40651a3330c6fde`)이며 이 문서는 명세를 바꾸지 않는다.

T9 통과는 **서버 독립 구현 검증**이다. iOS 연동, 실제 곡·분류 검수(T10), 실음원 재생, 운영 배포 검증이 아니다.

## 실행

저장소 루트에서 실행한다. DB, `.env`, `DATABASE_URL`, 네트워크 음악 서비스는 필요 없다.

```sh
export GOTOOLCHAIN=go1.24.2          # go.mod 버전
go build ./...
go test -count=1 ./...
go test -race -count=1 ./...
golangci-lint run                     # CI와 같은 v2.1.6

# 통합 테스트만
go test -race -count=1 -run TestIntegration -v ./cmd/server/
```

golangci-lint v2.1.6이 없으면 `GOTOOLCHAIN=go1.24.2 GOBIN=<dir> go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6`로 설치한다. CI(`.github/workflows/ci.yml`)는 build → `go test -race -count=1 ./...` → lint 순서로 실행한다. 현재 트리거는 main/develop 대상 push/PR뿐이라 `feature/random-matchmaking` 대상 PR에서는 실행되지 않는다.

반복 실행 시 macOS에서는 `-count`가 매우 크면(통합 테스트 ×100 이상) 임시 포트가 TIME_WAIT로 소진되어 `connect: can't assign requested address`가 날 수 있다. 이는 호스트 제약이며 서버 실패가 아니다. 반복 검증은 `-race -count=30` 정도로 나눠 실행한다.

## 하네스 구성 (`cmd/server/integration_*_test.go`)

- **실제 조립과 socket**: `newMatchingRegistry`(운영과 같은 조립 함수)와 `handleWebSocket`을 `httptest.Server`에 올리고 `/ws?protocolVersion=1`로 `coder/websocket` 클라이언트 2~11개를 접속한다. 테스트 전용 생산 코드 분기는 없다. 조립 함수의 `room.Option` 인자로만 의존성을 주입한다.
- **synthetic catalog**: `docs/specs/random-matchmaking/fixtures/catalog.json`·`manifest.json`(`test-v1`, 가상 곡 20개)을 운영 loader(`loadMatchingCatalog`)로 읽는다. 정답은 같은 공개 fixture의 제목에서 얻는다.
- **주입 MusicProvider**(`controlledSongs`): 지정 카테고리의 곡 조회를 테스트가 붙잡았다가 성공/실패로 완료한다. 나머지는 catalog로 바로 통과한다. starting 경합, 조회 실패, 늦은 결과, 멈춘 Room 격리를 재현한다.
- **fake clock**(`room.WithClock`): preload 30초 제한, COUNT_DOWN, 라운드 제한 시간은 테스트가 `Advance`할 때만 진행한다. 벽시계 COUNT_DOWN이 끼어들지 않는다.
- **wire 계약 검사**: 모든 송수신은 실제 `message.Encode`/`Decode`를 통과한다. 수신 시 서버 timestamp, 현재 membership의 roomID(WELCOME·ROOM_LEFT 제외), 같은 연결의 playerID 불변을 자동 검사한다. WELCOME은 requestID, 자신·host 포함, 중복 없는 players, `waiting`/8/`test-v1`, any 없는 확정 category를 검사한다.
- **순서 동기화(sleep 없음)**: 
  - lobby barrier: 모르는 ID의 MATCH_CANCEL → MATCH_CANCELLED. 해당 연결의 이전 matchmaker 입력 처리 완료를 증명한다.
  - room barrier: waiting 외 상태나 비방장의 START_GAME → 송신자에게만 ROOM_COMMAND_FAILED. Room actor FIFO의 이전 명령 처리 완료를 증명한다.
  - handler 종료 관찰: 운영 handler를 감싼 래퍼가 반환(= `Registry.Disconnect` 완료)을 알려 준다. 연결 종료가 이후 입력보다 먼저 matchmaker에 들어갔음을 보장한다.
  - "배정되지 않음"은 시간 대기가 아니라 이후의 긍정 증거(다른 상대와의 WELCOME 내용)로 검증한다.
- **제한 시간**: 서버별 context 20초, 종료 대기 5초. 실패는 무한 대기 대신 타임아웃으로 드러난다. goroutine 정리는 개수를 deadline까지 다시 확인하며 실패 시 stack을 덤프한다.

## 완료 기준과 테스트

| T9/요구사항 | 테스트 | 확인 내용 |
|---|---|---|
| 2명 신규·기존 방 충원·불일치 대기·우선순위 (MATCH-01~03, CAT-03~05, ROOM-01) | `TestIntegrationCompatibilityAndPriority` | 불일치 대기, 가장 이른 호환 대기자와 쌍, 접수 순서 방장, any/all 해소, 2010s≠all, 기존 방 충원, 생성 순서가 빠른 방 우선, 취소 후 새 접수 순서 |
| 9명 이상, 방당 8명, 추가 대기/새 방 | `TestIntegrationCapacityWithElevenConnections`, `TestIntegrationLastSeatRace` | 11 연결; 9번째 대기 후 새 방; 빈자리는 오래된 방; 마지막 자리 동시 요청 시 정확히 1명(×5 반복) |
| 중복 메시지 (MATCH-04) | `TestIntegrationDuplicateRequests` | 같은 ID 재전송 응답, ALREADY_MATCHING/ALREADY_IN_ROOM, WELCOME 재전송 동일, 연타 시 PLAYER_JOINED 1회 |
| 오류 응답 (CAT-06, DATA-01, CONN-03) | `TestIntegrationInvalidCriteriaAndReplay`, `TestIntegrationRequestIDLimitClosesWithPolicyViolation` | 버전 불일치·all 축·허용 외 조합·미지원 태그 → INVALID_CRITERIA 및 재전송 동일; 연결 유지; 4096 초과 ID → 1008 |
| 취소/재요청과 늦은 등록 완료 (CANCEL-01~03) | `TestIntegrationCancelTombstones`, `TestIntegrationCancelDuringAssignmentThenRequest` | 반복 취소, tombstone 비부활, matched 취소 후 정리, 오래된 LEAVE 무해; R1 요청·취소·R2 연속 전송 ×20에서 MATCH_CANCELLED(R1) 이후에만 WELCOME(R2), 늦은 R1 결과 격리 |
| 연결 종료 정리 (CONN-02) | `TestIntegrationDisconnectCleanup` | 대기/배정 중(결정적 변형 포함 ×10)/대기방 방장/게임 중 연결 종료 |
| 참가 등록과 시작 경합, WELCOME 선행 (ROOM-02, ROOM-06) | `TestIntegrationJoinBeforeAndAfterStart` | 시작 전 등록자는 게임 포함; starting 중 요청은 무알럿 대기 후 다른 방; 동시 전송 ×10의 양쪽 결과; provider가 방 category 수신 |
| 시작 실패·중복 시작 (design 7) | `TestIntegrationStartFailures` | INVALID_STATE·단일 조회, INTERNAL_ERROR 후 waiting 재수용, NOT_HOST, 1명 NOT_ENOUGH_PLAYERS, starting 중 이탈·늦은 결과 폐기 |
| 방장 승계, 1/0명 (ROOM-03) | `TestIntegrationHostSuccessionAndEmptyRoom` | 접수 순서 승계, 1명 방 유지·충원·시작 불가, 0명 삭제 후 후보 제외, 반복 LEAVE |
| 게임 중 인원 감소, 프리로드 중 이탈, 조기 종료 (ROOM-04) | `TestIntegrationDeparturesWhilePlaying` | ready 재평가로 1회 라운드 시작, 방장 승계 후 계속, 1명 GAME_ABORTED(INSUFFICIENT_PLAYERS), result 재시작·충원 없음, 이전 방 늦은 명령 무시 |
| 프리로드 무응답 (design 7, FAIL-01) | `TestIntegrationPreloadTimeout` | fake clock 30초 경계에서만 GAME_ABORTED(INTERNAL_ERROR) |
| 10곡 정상 종료, LEAVE 후 같은 연결 재매칭 (ROOM-05, CONN-01/04, GAME-01~02) | `TestIntegrationTenRoundGameAndRematch` | 10개 서로 다른 ISRC, 정답/오답/시간초과, 점수·승자 기존 규칙, 구 START_GAME 필드 무시, result에서 재시작/요청/충원 거부, 퇴장 tombstone, 같은 playerID로 새 방 |
| 반복 실행 후 정리, actor 격리 | `TestIntegrationRepeatedCyclesAndActorIsolation`, `TestIntegrationShutdownWithActiveRooms` | 조회가 멈춘 Room과 무관하게 두 그룹 전체 주기 ×3 병렬 진행 후 정상 시작; goroutine이 유휴 서버 수준으로 복귀; waiting/starting/playing Room이 있어도 context 취소 시 종료, socket 종료, 서버 생성 전 goroutine 수로 복귀 |

기존 소켓 테스트(`main_test.go`, `matchmaking_test.go`, `cancellation_test.go`, `game_test.go`)는 protocolVersion/room query HTTP 400, binary/잘못된 JSON/알 수 없는 type/초과 frame 종료 코드, 같은 ID 내용 변경 1008, KICK 후 재매칭을 계속 담당한다.

## socket 테스트와 actor 단위 테스트의 보장 범위

socket 테스트는 wire 메시지만으로 판정하며 실제 goroutine·채널·socket 경합을 거친다. 다만 경합 순서를 강제할 수 없는 경로는 결과의 불변식만 검사하고, 실행마다 어떤 순서가 나왔는지 로그(`admitted/refused` 등)로 남긴다.

다음은 외부 wire로 순서를 강제할 수 없어 **actor 단위 테스트가 보장**한다(`internal/room`, `internal/matchmaking`, `internal/session`, `internal/client`).

- 오래된 snapshot으로 starting Room에 보낸 Register의 거절: `TestGatewayCapacityAndStartOrder`, `TestStartGamePermissionAndState`, `TestRealRoomStartRegistrationOrdering`. socket에서는 START와 같은 turn에 accepting=false snapshot이 나가므로 이 창이 거의 열리지 않는다(변이 검사로 확인).
- 등록/제거 5초 timeout quarantine, Done 지연, drain 전 재배정 금지, terminal Room 늦은 이벤트: `TestTimeoutQuarantineWaitsForDoneAndDrain`, `TestInitialPairTimeoutAndStalledDone`, `TestTerminalRoomIgnoresAllLateEvents` 등 (controlled clock).
- 이벤트 큐·명령 큐 backpressure, 연결 outbound 256 frame/1 MiB, ping 20초/pong 10초/write 10초: `TestEventQueueAtomicBackpressure`, `TestGatewayBackpressureAndContextTermination`, `TestConnectionOutboundLimits`, `TestConnectionWorkersUseDeadlinesAndTerminate`.
- 곡 조회 5초 기술 timeout은 실시간이라 socket 테스트에서 기다리지 않는다. 조회 실패 경로는 주입 오류로 검증한다.

## 알려진 사항

- `internal/game`의 `TestFullGameCycle`, `TestEndRound_WithWinner`, `TestSubmitAnswer`, `TestGenerateStartTime`은 develop(`2de775a`)에서도 실패한다. 2026-06-27 정답 공백 처리(`3497b5e`)와 시작 시간 ms→초(`e539513`) 변경 후 테스트가 갱신되지 않았다. 게임 규칙 소유 범위라 T9에서 수정하지 않았다. 해결 전에는 `go test ./...`와 CI test 단계가 실패한다.
- 이 하네스는 실제 음원 재생, iOS 클라이언트, 운영 카탈로그·DB를 검증하지 않는다. `/ws?protocolVersion=1`은 구 앱과 호환되지 않는다.
