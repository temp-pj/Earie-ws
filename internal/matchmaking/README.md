# T3 매칭·배정 및 T4 취소·실패 보장

이슈 #33, REQ-MATCH-01~04 / REQ-ROOM-01,02,06. 기준은
`e7696eec6fa9f1b443c5a5b9162c96152c6dd742`이며 T1 PR #39와 T2 PR #38의
`feature/random-matchmaking` 병합을 확인했다. T0 PR #37의 공통 값 계약과
wire v1을 변경하지 않는다.

## 상태 소유권과 조립

`New(ctx, gateway, catalog, events)` → `Registry()`를 endpoint에 주입하고
`Run()`을 한 번 실행한다. `cmd/server.newMatchingRegistry`가 실제 조립 경로다.
Matchmaker 루프만 요청·접수 순서·pending token·방 snapshot을 변경한다.
Registry는 연결/FIFO/membership 참조, Room은 실제 참가 map과 생성 순서를 소유한다.
Match/Disconnected는 입력을 enqueue하며 Room 결과를 기다리지 않는다.

각 scheduling pass는 waiting 요청을 접수 순서로 방문한다. 공개된 기존 방을
생성 순서로 먼저 탐색하고, 없으면 T2 ResolvePair로 호환성과 실제 pool을 검사해
가장 이른 대기자와 비공개 방을 만든다. assigning 전환 후 결과가 오기 전에는
같은 요청을 재등록하지 않는다. 동일 활성 ID 재전송은 기존 순서를 유지한다.
등록 거절/enqueue 실패는 해당 roomID+revision을 제외하고 다음 후보를 탐색한다.
최초 쌍 실패도 순서를 유지하되 같은 실패를 무한 반복하지 않는다. 새 요청이나
공개 방 갱신이 오면 해당 쌍을 다시 검토한다.

등록 결과의 token·member를 확인한 뒤 Registry.Apply에서 WELCOME과 후속 wire
이벤트를 함께 처리한다. 최초 쌍은 두 결과와 연결 유효성을 처리하고, 실패한
WELCOME/끊어진 연결의 scoped Remove 완료를 소비한 뒤 Publish한다. 0명은 닫는다.
더 높은 revision만 반영하며 Published 전/Closed 후 snapshot은 후보가 되지 않는다.
Done watcher와 단일 이벤트 소비자가 종료된 방의 FIFO를 drain한 뒤 참조를 회수한다.
컨텍스트 종료 시 Room을 닫고 Done을 기다리며 이 시점에는 신규 배정을 중단한다.

## T4 요청 기록과 실패 정리

이슈 #34, REQ-CANCEL-01~03 / REQ-MATCH-04 및 design 4,8을 구현한다.
T3 PR #40 병합 후 `209636109a5ea969f166a253d78eb980356d20f8`에서 시작했다.
공통 actor 값/인터페이스와 wire v1은 변경하지 않는다.

연결별 history가 모든 requestID의 원래 payload와 terminal 응답을 보관하고,
활성 요청 map은 waiting/assigning/matched 하나만 가리킨다. 취소는 활성 요청에서
즉시 빠지지만 진행 중 등록/제거가 끝날 때까지 history에 남아 같은 연결의 새
요청 배정을 막는다. 새 ID는 즉시 새 순서와 MATCH_WAITING을 받는다. 같은 ID의
변경된 payload 또는 연결당 4096개 ID 이후 신규 ID는 writer에서 1008로 닫는다.
모르는 ID 취소도 tombstone을 생성하며, 이후 요청으로 다시 활성화하지 않는다.

취소·퇴장·disconnect는 요청별 하나의 scoped Remove를 공유한다. Registry의
정리 입력이 취소 완료보다 늦게 소비되면 이미 확인된 제거로 해당 leave만
완료하고 Room에 새 명령/타이머를 만들지 않는다. 완료는 room/player/request와
불변 token을 검사하며, 이전 요청 정리가 새 활성 요청 또는 membership을
지우지 않는다. 정상 퇴장 완료는 cancelled tombstone으로 축소한다.

Matchmaker 단일 루프의 timer가 등록/제거(및 방 공개) 결과를 5초 제한으로
감시한다. 결과가 없거나 제거 enqueue가 실패하면 방을 격리하고 Close한다.
격리 이후 결과/snapshot은 적용하지 않는다. Done과 FIFO drain을 확인한 뒤
유효 assigning 요청은 INTERNAL_ERROR, 취소 요청은 MATCH_CANCELLED로 끝낸다.
이미 입장한 사용자는 연결을 종료한다. 취소된 membership과 그 pending leave는
scoped 정리 후 연결 재사용을 유지한다. Close 후에도 동일 기술 제한 5초 동안
Done이 오지 않으면 운영 오류를 기록하고 영향 연결을 종료하며, 정리 증명 없이
배정/취소 완료를 통지하지 않는다. Done이 오면 남은 기록을 회수한다.

clock/event 단위 테스트는 시간을 직접 전진시키고 결과 순서를 제어한다.
실제 WebSocket 테스트는 취소 직후 같은 연결 재요청, terminal replay 및 1008
정책 종료까지 확인한다. 모든 테스트 데이터는 synthetic test-v1이다.

## 실행 데이터

기존 DATABASE_URL 외에 서버 catalog.json의 `CATALOG_PATH`와 같은 버전 manifest의
`SONG_MANIFEST_PATH`를 설정한다. 서버 시작 시 T2 loader로 검증하며 누락/오류 시
시작하지 않는다. synthetic fixture로 자동 대체하지 않는다. 테스트는 공개
`test-v1` fixture를 명시적으로 주입하며 DB를 조회·변경하지 않는다.

## 후속 범위와 제한

- T7: RoomCommander 주입, 실제 게임 시작·종료와 곡 조회 연결. 현재 게임 명령은
  미주입 상태다. 시작 경합 테스트는 T1 BeginStart admission 장벽만 검증한다.
- T9: 전체 게임 WebSocket 통합 검증. T3 socket 테스트는 실제 조립 경로의
  대기·최초 쌍·충원·WELCOME 순서·명시 퇴장 후 재매칭까지만 검증한다.

이 중간 브랜치는 전체 기능 완료나 출시 상태가 아니다. 실제 음원·운영 분류·iOS는
검증하지 않았다. 기존 internal/game 테스트 실패는 T4에서도 변경하지 않는다.
