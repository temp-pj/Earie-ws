# Go 서버 랜덤 매칭 기술 설계 v1.1

이번 대상은 Go 서버다. 이 문서는 구현자가 임의로 바꾸지 않을 서버 구조·순서 보장 기준이다. private helper 이름은 구현자 재량이며 외부 메시지는 protocol.md를 따른다.

## 1. 코드 출발점과 변경 경계

현재 cmd/server/main.go는 /ws 즉시 Hub.CreateRoom/FindRoom 후 Register한다. client.Run은 하나의 Room incoming 채널에 고정된다. Room.removeClient는 Client.Send를 닫는다. 이 세 동작을 분리해야 한다.
Hub map 보호는 유지하되 Room 상태를 Hub mutex만으로 보호한다고 가정하지 않는다. 기존 게임의 판정/타이머 정책은 유지하고 카탈로그 풀에서 10곡을 공급한다.

## 2. 패키지와 소유권

| 패키지 | 소유권/역할 |
|---|---|
| internal/client | socket reader/writer, 연결 context, 서버 발급 playerID, 직렬 outbound queue |
| internal/session (신규) | 연결 registry, 명령 라우팅, membership 참조, 종료 및 전송 실패 전달 |
| internal/matchmaking (신규) | 단일 이벤트 루프, request 상태/순서, 배정, 취소 장벽, Room snapshot |
| internal/matchmaking/criteria (신규) | 순수 Category 호환/해소 함수; network/Room import 없음 |
| internal/room | Hub registry/생성 순서, Room actor 및 참가/게임 상태 |
| internal/music | Category, catalog 검증, pool/곡 저장소 |
| internal/message | 외부 JSON DTO |
| internal/sessioncontract (신규) | actor 간 값 타입·명령/결과 계약; 위 구현 패키지를 import하지 않음 |

의존 고리 방지: Room은 Matchmaker 구현을 import하지 않는다. 주입된 event sink로 sessioncontract 값만 전달한다. Matchmaker는 주입된 Room gateway를 사용한다. cmd/server가 조립한다.

## 3. actor 간 계약

Room gateway는 다음 비동기 명령을 제공한다. 결과는 요청의 roomID/requestID를 포함하며 성공·거절/정리 완료 중 하나로 끝난다.

- CreateInitialPair: 확정 Category, catalogVersion, 두 member(playerID,requestID,arrivalSequence). Hub가 유일 roomID/creationSequence를 발급하고 아직 비공개 Room 생성.
- Register: roomID, member. Room이 waiting, 정원(8), 중복 membership을 검증하고 같은 actor turn에서 등록한다.
- Remove: roomID, playerID, requestID. 일치하는 membership만 삭제. 없으면 성공한 정리로 취급한다.
- Close: Room context 취소 후 Done을 기다린다. Done은 추가 명령/등록/이벤트가 발생하지 않음을 보장한다.

Room 참가 map은 playerID를 key로 사용하되 값에 requestID와 arrivalSequence를 포함한다. 제거 비교에 playerID만 사용하지 않는다.
Matchmaker의 request key는 (connectionID,requestID)다. 연결 내부 식별 connectionID는 외부에 노출하지 않는다. 서버 process 전역 단조 증가 uint64로 arrivalSequence/creationSequence를 부여하며 벽시계 timestamp를 사용하지 않는다.

## 4. 요청 상태와 중복

waiting → assigning → matched, assigning 거절 → waiting(기존 순서 유지).
취소는 어느 상태에서도 cancelled로 전환한다. 실패는 failed terminal 상태다. cancelled/failed/퇴장된 matched는 결과 재전송에 필요한 tombstone만 남긴다. 연결 종료 시 관련 작업 정리 후 전부 제거한다.
하나의 활성 요청만 허용한다. cancelled 요청의 cleanupPending은 활성 요청 수에서 제외한다. 새 요청은 접수할 수 있지만 이전 제거/등록 수습이 완료될 때까지 dispatchEligible=false로 유지한다.
새 요청을 이미 방에 있는 사용자가 보내면 ALREADY_IN_ROOM. 살아 있는 다른 요청이 있으면 ALREADY_MATCHING. 같은 ID 중복 응답은 protocol.md를 따른다.
각 요청에는 하나의 pending room operation만 둔다. 추가 외부 assignmentID는 도입하지 않는다. 내부 콜백은 command의 불변 토큰/세대값으로 식별해서 늦거나 중복된 완료를 두 번 적용하지 않는다.

## 5. 매칭 순서

요청 접수, 방 공개/빈자리 변경, 취소 정리 완료, 등록 거절 시 스케줄링 pass를 실행한다. 이벤트당 각 eligible waiting 요청을 접수 순서로 한 번 방문한다.
각 요청은 정렬된 호환 기존 방을 먼저 탐색한다. 없으면 자신 외 호환 waiting 중 가장 이른 요청과 pair를 만든다. 양쪽 단독 유효성 외에도 해소 Category의 pool이 10곡 이상인지 확인한다. 해당하지 않는 상대를 건너뛰며 조건을 완화하지 않는다.
요청을 선택하는 actor turn에서 assigning으로 바꾸므로 다른 pass에서 중복 선택하지 않는다.
Register 결과가 full/started/closed면 최신 snapshot을 반영하고 요청을 waiting으로 복귀한다. 같은 roomID+revision에 실패한 후보는 다음 revision까지 재시도하지 않는다. 기존 방이 사용 불가하면 다른 후보/대기자를 탐색한다. pass가 무한 반복되지 않아야 한다.

## 6. Room 상태와 이벤트 순서

Room 내부 단계: initializing → waiting → starting → playing → result → closed. starting은 외부 admission을 막는 내부 상태이며 공개 상태는 playing으로 표시한다.
Room마다 uint64 revision을 증가시킨다. 모든 등록 결과·snapshot·wire event·정리 결과를 같은 Room의 순서 있는 event stream으로 내보낸다. 같은 actor turn의 event batch는 쪼개 다른 순서로 발행하지 않는다.
Matchmaker/세션 dispatch는 이 stream을 순서대로 소비한다. pending 참가자의 등록 결과에서 request가 유효하면 WELCOME을 연결 outbound queue에 먼저 넣고 이후 그 방 이벤트를 보낸다. Room이 직접 WELCOME/socket write를 하지 않는다.
취소라면 WELCOME은 보내지 않고 request-scoped Remove를 보낸다. 이미 큐에 들어간 WELCOME과 취소 경합은 iOS requestID 검사로 막는다. 연결 outbound queue는 FIFO이며 writer는 하나다.
Room snapshot은 roomID,creationSequence,category,catalogVersion,playerCount,maxPlayers,accepting,revision을 갖는다. Matchmaker는 더 높은 revision만 반영한다.
close 이벤트 후 roomID를 terminal로 기록하고 늦은 snapshot을 무시한다. Room ID는 재사용하지 않는다. Done 및 해당 Room 이벤트 drain 후 terminal 기록을 회수해 무제한 누적하지 않는다.
초기 쌍은 한 turn에서 전원 등록 또는 전원 실패한다. 두 등록 결과를 처리하고 취소 상태를 반영한 뒤 비공개 Room을 공개한다. 한 명 유효하면 남은 membership 상태에 맞춰 공개, 0명이면 삭제한다.

## 7. 시작·퇴장·정리

Room에서 START_GAME 처리 시 host, waiting, 실제 등록 인원>=2를 검사한다. 성공 시 즉시 starting으로 전환해 admission을 닫는다. 시작 이후 등록 요청은 거절한다.
확정 pool에서 서로 다른 ISRC 10개를 무작위로 고르고 기존 게임을 초기화한다. 시간은 기존 기본값 30초, category/count는 서버 권위다. DB/manifest 조회 실패 또는 10곡 미달이면 waiting으로 복귀, snapshot 갱신 및 ROOM_COMMAND_FAILED(INTERNAL_ERROR). GAME_STARTED는 게임 초기화 성공 후 내보낸다.
starting 중 호스트/참가자 퇴장도 actor에서 처리한다. 인원이 2명 미만이면 시작 작업 세대를 무효화하고 waiting(0명이면 closed)으로 복귀한다. 늦은 조회 결과는 버린다. 시작 버튼 요청자에게 아직 연결되어 있으면 NOT_ENOUGH_PLAYERS를 보낸다.
playing에서 1명은 game timer/audio 준비 흐름을 정지하고 result로 전환, GAME_ABORTED(INSUFFICIENT_PLAYERS). 0명은 즉시 close. 퇴장 시 ready map에서 제거하고 남은 모두 ready라면 라운드를 한 번만 시작한다. 라운드 세대/번호를 확인해 중복 timer나 READY가 다음 라운드에 영향을 주지 않게 한다.
Round preload가 30초 내 완료되지 않으면 게임을 GAME_ABORTED(INTERNAL_ERROR)로 종료한다. 이는 연결된 무응답 참가자로 인한 무한 대기 방지용이며 음악 제한 시간/정답 규칙 변경이 아니다. 주입 clock으로 검증한다.
정상 게임 결과 정책은 기존 구현 유지. 결과에서 각자 LEAVE_ROOM 가능, 재시작/충원 없음.
Room membership 제거는 연결의 Send 채널을 닫지 않는다. 연결 계층만 writer 수명을 종료한다.

## 8. Room 종료·무응답

DB/외부 작업은 actor 밖에서 수행하고 context를 전달한다. actor loop는 네트워크 send, DB, 다른 actor 응답으로 block하지 않는다. 명령 큐 등록과 result delivery는 context/Done을 함께 처리한다.
등록/제거 명령 결과의 기술 타임아웃은 5초다. 타임아웃만으로 같은 요청을 다른 방에 배정하지 않는다. 해당 Room을 accepting=false로 만들고 context 취소→Done→이벤트 drain→모든 membership 정리 확인까지 기존 작업을 격리한다.
그 뒤 유효한 배정 요청은 INTERNAL_ERROR로 종료하고, 취소된 요청은 MATCH_CANCELLED로 완료한다. 이미 입장한 사용자들은 연결을 종료해 공통 연결 실패 안내/로비 경로를 따른다. 정상 game abort와 서버 actor 장애를 구분한다.
Done이 끝나지 않으면 해당 요청의 새 배정/취소 완료를 거짓으로 통지하지 않는다. 운영 오류 기록과 해당 연결 종료를 수행하고 격리 정리를 계속한다. 중단 가능한 actor 구현 및 테스트로 이 상태를 예방한다.
반복 cleanup과 late result는 멱등 처리한다. 폐기된 Room의 결과로 새 membership을 생성하지 않는다.

## 9. 연결 정책

서버는 연결별 20초마다 WebSocket ping, pong 대기 10초, outbound write 10초 제한을 둔다. 유휴 로비라는 이유로 의도적으로 닫지는 않는다. 감지 시점까지 사용자가 남을 수 있고 즉시 감지를 보장하지 않는다.
후속 클라이언트 기대 동작(이번 구현 제외): iOS는 포그라운드에서 살아 있는 연결을 재사용한다. 백그라운드에서는 지속 연결/자동 복구를 보장하지 않는다. 복귀 시 실패가 감지되면 알럿 후 로비, 다음 명시적 매칭에서 새 연결. 앱 재실행은 로비에서 시작한다.
연결별 outbound queue는 최대 256 frames 또는 1 MiB 중 먼저 도달하는 상한을 적용하고, 새 frame을 넣을 수 없으면 느린 수신자로 연결 종료한다. Room/Matchmaker actor에서 큐 여유를 기다리지 않는다. client.Send를 다른 goroutine이 쓰는 중 닫는 방식 대신 context와 단일 writer의 소유권으로 종료한다.

## 10. 후속 iOS 연동 경계 — 이번 구현 제외

기존 iOS App/Lobby/Room 및 Client 구조는 이번 작업에서 수정하지 않는다. 이전에 논의한 App 단일 구독·requestID 필터·로비 전이는 후속 iOS 설계에서 구체화한다.
서버는 protocol.md의 requestID/roomID, WELCOME 선행, cancel 완료, 연결 재사용 계약을 테스트 클라이언트에 동일하게 제공한다. 특정 Swift Reducer 구조에 의존하지 않는다.
이번 Go 테스트 클라이언트는 wire 메시지만 사용하며 서버의 private state를 통해 성공을 위조하지 않는다. actor 단위 테스트에서 내부 상태를 관찰하는 것과 socket 통합 검증을 구분한다.

## 11. 검증/출고

protocol fixture는 이번에 Go decoder/encoder와 테스트 WebSocket 클라이언트에서 검증한다. 서버는 controlled event/clock 및 -race로 동시성을 검증한다. iOS TestStore 및 실기기 검증은 후속 단계이며 이번 서버 완료 조건에서 제외한다.
서버 통합 브랜치 중간 커밋은 출시하지 않는다. protocolVersion=1 endpoint는 구 앱 연결을 거절하므로 이번에는 격리된 서버 테스트 환경에서 검증하고, 후속 출시 단계에서 staging 서버/iOS를 함께 검증한다. 최종 배포에는 구 앱 서비스 중단/업데이트 안내가 필요하며 자동 배포 권한은 이 명세에 없다.
T9는 실제 WebSocket·synthetic MusicProvider로 서버 독립 end-to-end를 검증하는 병합 게이트다. T10 실제 카탈로그/Apple Music 검수와 후속 iOS 연동은 별도 출시 게이트이며 T9 착수·서버 코드 병합을 막지 않는다.
