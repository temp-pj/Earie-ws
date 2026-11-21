# T3 매칭·배정 연결

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

## 실행 데이터

기존 DATABASE_URL 외에 서버 catalog.json의 `CATALOG_PATH`와 같은 버전 manifest의
`SONG_MANIFEST_PATH`를 설정한다. 서버 시작 시 T2 loader로 검증하며 누락/오류 시
시작하지 않는다. synthetic fixture로 자동 대체하지 않는다. 테스트는 공개
`test-v1` fixture를 명시적으로 주입하며 DB를 조회·변경하지 않는다.

## 후속 범위와 제한

- T4: MATCH_CANCEL, terminal tombstone/4096 ID 한도, payload 변경 처리,
  취소 직후 재요청 장벽, 5초 operation timeout/quarantine 및 장애 재처리 보장.
  현재 MATCH_CANCEL은 오류를 반환해 연결을 닫으며 취소 완료로 응답하지 않는다.
  퇴장 완료 요청의 terminal 기록도 아직 유지하지 않는다.
- T7: RoomCommander 주입, 실제 게임 시작·종료와 곡 조회 연결. 현재 게임 명령은
  미주입 상태다. 시작 경합 테스트는 T1 BeginStart admission 장벽만 검증한다.
- T9: 전체 게임 WebSocket 통합 검증. T3 socket 테스트는 실제 조립 경로의
  대기·최초 쌍·충원·WELCOME 순서·명시 퇴장 후 재매칭까지만 검증한다.

이 중간 브랜치는 전체 기능 완료나 출시 상태가 아니다. 실제 음원·운영 분류·iOS는
검증하지 않았다. 기존 internal/game 테스트 실패는 T3에서 변경하지 않는다.
