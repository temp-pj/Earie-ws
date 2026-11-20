# 랜덤 매칭 WebSocket 계약 v1

이 문서의 필드/순서가 서버 및 후속 클라이언트 구현 기준이다. 이번에는 Go 서버와 WebSocket 테스트 클라이언트만 구현한다. iOS 관련 문구는 미래 소비자 계약이며 이번 앱 구현 지시가 아니다. 실제 음악 분류 목록은 catalogVersion으로 고정된 데이터이며 프로토콜 enum을 임의로 늘리지 않는다.

## 연결과 봉투

엔드포인트: /ws?protocolVersion=1. room query는 허용하지 않는다. protocolVersion 누락/다른 값 또는 room query가 있으면 upgrade 이전 HTTP 400. 기존 앱과 wire-compatible하지 않으며 통합 테스트용 환경에 양쪽을 함께 적용한다.
모든 메시지는 UTF-8 JSON text frame이다. 최대 크기 64 KiB. binary/초과 frame은 1003/1009로 종료. 잘못된 JSON, 필수 키 누락/타입 오류/UUID 형식 오류는 1008 종료. 알 수 없는 type은 1008 종료한다. 알 수 없는 객체 필드는 무시한다.

| 필드 | C→S | S→C |
|---|---|---|
| type | 필수 string | 필수 string |
| payload | 필수 object, 빈 경우 {} | 필수 object, 빈 경우 {} |
| timestamp | 선택, 있으면 integer; 순서 판단에 사용 안 함 | 필수 int64, Unix milliseconds |

별도 명시가 없는 필드는 필수이며 null 금지다. 모든 ID는 소문자 canonical UUID 문자열이다. iOS requestID는 UUID v4, 서버 roomID/playerID도 UUID v4. playerID는 연결당 서버가 발급하며 WELCOME에서 알려준다. 클라이언트가 playerID로 권한을 주장할 수 없다. 새 연결은 새 사용자이며 기존 상태 복구 없음.

## Category 공통 타입

요청과 방에서 같은 객체를 사용한다. 객체의 필드는 모두 필수다.

| 필드 | 타입 | 값 |
|---|---|---|
| decade | string | any, all, 또는 카탈로그가 제공하는 YYYYs(예: 2010s) |
| artistType | string | any, all, solo, group |
| gender | string | any, all, male, female |
| genre | string | any, all, 또는 카탈로그 genres의 ID |

any = 상대 조건 수용. all = 지원 카탈로그의 해당 축 전체 범위. country/가수 필드는 없다.
요청 UI는 decade에만 all을 제공한다. artistType/gender/genre의 all 요청은 INVALID_CRITERIA다. 이 축의 all은 양쪽 any를 합친 확정 방에서만 사용한다. 방 Category에는 any 금지. decade의 any+any와 all+any는 모두 all로 확정되며 같은 방 조건으로 취급한다.

decade YYYYs는 YYYY-01-01부터 YYYY+9-12-31까지다. all은 전 세계/전체 DB가 아니라 해당 catalogVersion의 검수된 풀 전체다.

## 배포 카탈로그

공통 파일 계약: catalog.json. 배열은 순서가 UI 표시 순서이며 빈 배열은 서비스 이용 불가를 뜻한다. version은 1~64자의 [A-Za-z0-9._-] 문자열이다. iOS 번들에 포함하고 서버 시작 시 같은 파일을 읽는다. 매칭용 새 REST API/카탈로그 요청 메시지를 만들지 않는다.

| 키 | 타입/의미 |
|---|---|
| version | 비어 있지 않은 string; 내용이 바뀌면 반드시 변경 |
| decades | YYYYs string[]; 중복 없음 |
| genres | {id:string,label:string}[]; ID는 [a-z][a-z0-9_]*, any/all 예약어 제외 |
| allowedRequests | Category[]; UI에서 선택 가능한 완전한 조합, any 포함 가능 |
| pools | {category:Category,songIDs:string[]}[]; category에 any 없음, songIDs는 서로 다른 ISRC |

풀은 동일 버전의 서버 song manifest에 연결된다. manifest는 ISRC를 key로 곡 조회에 필요한 MBID/title/artist/releaseDate/duration/startTime을 제공하며 각 ISRC가 실제 재생 가능한지 T10에서 확인한다. 정답/곡 목록은 iOS에 보내지 않는다. iOS 카탈로그 번들에는 pools의 각 원소를 {category:Category,songCount:integer>=10}로 바꾼 projection을 배포해 정답 풀 노출을 줄인다. version/decades/genres/allowedRequests는 동일하고 서버는 원본을 사용한다.
manifest JSON 최상위는 {catalogVersion:string,songs:[...]}다. song 항목은 isrc:string(12자리 표준 ISRC, 대문자), mbid:UUID, title:string, artist:string, releaseDate:YYYY-MM-DD, duration:integer 초, startTime:integer 초다. title/artist는 비어 있지 않아야 하고 duration>=40, 0<=startTime<=duration-30을 만족한다. startTime은 기존 랜덤 시작점 구현에 제공하는 검수 가능한 기본값이며 게임은 기존 시작점 생성 방식을 유지한다. 각 ISRC는 한 번만 존재한다. 양쪽 카탈로그와 manifest의 catalogVersion/version은 일치해야 한다.
각 pool은 완전한 category와 10개 이상 서로 다른 ISRC를 가지며 category가 같으면 중복 pool을 두지 않는다. 모든 songID가 manifest에 있어야 한다.
allowedRequests는 해당 요청을 충족하는 10개 이상 ISRC의 유효 pool이 있을 때만 포함한다. 양쪽 요청을 결합한 Category에도 pool(10곡 이상)이 있어야 매칭 가능하다. 요청 단독 유효성과 쌍의 결합 유효성을 분리한다.
UI: 기본은 전부 any. 선택 변경 후 allowedRequests에 존재하는 완전 조합만 허용한다. 다른 현재 선택을 자동 변경하지 않고 불가능한 후보를 비활성화한다. 전부 any조차 유효하지 않으면 매칭 비활성 및 이용 불가 표시.
버전 불일치 MATCH_REQUEST는 INVALID_CRITERIA. 앱은 '음악 조건 정보가 변경됐어요. 앱 업데이트를 확인해 주세요.' 안내 후 로비. 버전 mismatch 자동 완화/다른 풀 대체 없음.

## 메시지 payload

| type | 방향 | payload 필드 |
|---|---|---|
| MATCH_REQUEST | C→S | requestID:UUID, catalogVersion:string, category:Category |
| MATCH_WAITING | S→C | requestID:UUID |
| MATCH_CANCEL | C→S | requestID:UUID |
| MATCH_CANCELLED | S→C | requestID:UUID |
| MATCH_FAILED | S→C | requestID:UUID, code:오류 enum |
| WELCOME | S→C | requestID:UUID, roomID:UUID, playerID:UUID, hostID:UUID, players:UUID[], roomState:"waiting", maxPlayers:8, category:Category, catalogVersion:string |
| LEAVE_ROOM | C→S | roomID:UUID |
| ROOM_LEFT | S→C | roomID:UUID |
| START_GAME | C→S | roomID:UUID |
| ROOM_COMMAND_FAILED | S→C | roomID:UUID, command:"START_GAME", code:오류 enum |
| GAME_ABORTED | S→C | roomID:UUID, reason:"INSUFFICIENT_PLAYERS" 또는 "INTERNAL_ERROR" |

WELCOME.players는 해당 참가자를 포함하며 hostID가 목록에 있어야 한다. 서버 접수 순서로 정렬한다. 정상 배정에는 MATCH_WAITING이 선행하지 않아도 된다.
MATCH_FAILED.code: INVALID_CRITERIA, INTERNAL_ERROR, ALREADY_MATCHING, ALREADY_IN_ROOM.
ROOM_COMMAND_FAILED.code: NOT_HOST, NOT_ENOUGH_PLAYERS, INVALID_STATE, INTERNAL_ERROR.
ALREADY_MATCHING은 다른 유효 요청이 살아 있는데 새 ID 요청을 받은 경우 새 ID에 응답한다. 기존 요청/순서는 유지한다. ALREADY_IN_ROOM도 새로운 요청만 거절한다.

## 기존 게임 메시지

PLAYER_JOINED/PLAYER_LEFT/ HOST_CHANGED는 기존 playerID에 roomID를 추가한다. PLAYER_KICKED는 roomID를 추가하며 kicking 기능 자체는 이번 UI에 추가하지 않는다. 처리 시 현재 방에서 제거하고 로비로 복귀한다.
GAME_STARTED payload는 {roomID}. ROOM_COMMAND_FAILED와 GAME_ABORTED는 이 버전에서 필요한 추가 이벤트이며 새 게임 판정 규칙은 아니다. PRELOAD_SONG/ROUND_STARTED/COUNT_DOWN/WRONG_ANSWER/ROUND_RESULT/GAME_OVER의 기존 필드는 유지하고 roomID를 추가한다. 이름은 기존 코드 그대로 COUNT_DOWN, ROUND_STARTED를 사용한다.
READY_TO_PLAY는 {roomID,roundNumber:integer>=1}, SUBMIT_ANSWER는 {roomID,answer:string}, KICK_PLAYER는 {roomID,targetPlayerID:UUID}. 각 명령은 실제 연결 membership과 일치할 때만 처리한다. 이전 방의 늦은 게임 명령은 무시한다.
START_GAME의 이전 category/trackCount/timeLimit 필드는 더 이상 사용하지 않는다. 추가되어도 서버는 무시하며 권위 있는 방 설정을 사용한다. 중복 시작은 INVALID_STATE로 실패하고 중복 게임을 만들지 않는다.
모든 roomID가 현재 방과 다르면 iOS에서 무시한다. 조기 종료는 GAME_OVER로 위장하지 않고 GAME_ABORTED로 보낸다. 기존 승자/점수 판정 변경 없음.

## 멱등성과 순서

- 서버는 연결별 requestID 기록을 연결 종료까지 보관한다. 완료 기록은 tombstone으로 줄인다. 한 연결 최대 4096개 서로 다른 ID 접수 후 추가 신규 요청은 1008 종료한다. 이는 무제한 메모리 증가 방지 한도이며 단위 테스트한다.
- 같은 requestID+같은 내용 재전송: 새 순서/등록 없이 waiting 또는 assigning이면 MATCH_WAITING, matched면 저장된 WELCOME, cancelled면 정리 후 MATCH_CANCELLED, failed면 저장된 MATCH_FAILED를 재전송한다. 퇴장 완료된 matched 요청은 cancelled tombstone으로 전환한다.
- 같은 ID로 내용 변경은 1008 종료. UUID 형식은 맞지만 카테고리 값/버전이 잘못되면 INVALID_CRITERIA.
- 모르는 ID 취소는 cancelled tombstone 생성 후 MATCH_CANCELLED. 이후 그 ID의 MATCH_REQUEST를 새 요청으로 살리지 않는다. 다른 연결의 ID로 타 사용자의 상태를 수정할 수 없다.
- 취소 정리 중 새 요청은 새 순서로 접수하여 MATCH_WAITING을 보내되 이전 membership 정리 완료 전에는 배정하지 않는다.
- LEAVE_ROOM 반복/이미 없는 방 요청은 ROOM_LEFT로 확인한다. 다른 현재 방은 제거하지 않는다. iOS도 현재 pending leave ID와 같은 확인만 처리한다.
- MATCH_CANCELLED는 등록 중 결과를 포함한 정리 완료 후 전송한다. cancel 이후 이미 큐에 있던 WELCOME은 도착할 수 있으므로 iOS의 requestID 검사도 필수다.
- 새 참가자의 WELCOME은 그 사용자에게 전송되는 해당 방의 다른 이벤트보다 앞선다. Room이 직접 socket에 쓰지 않고 설계의 순서 보장 경로를 사용한다.

## 예시

```json
{"type":"MATCH_REQUEST","payload":{"requestID":"11111111-1111-4111-8111-111111111111","catalogVersion":"test-v1","category":{"decade":"2010s","artistType":"any","gender":"any","genre":"korean_ballad"}}}
```

```json
{"type":"WELCOME","payload":{"requestID":"11111111-1111-4111-8111-111111111111","roomID":"22222222-2222-4222-8222-222222222222","playerID":"33333333-3333-4333-8333-333333333333","hostID":"33333333-3333-4333-8333-333333333333","players":["33333333-3333-4333-8333-333333333333","44444444-4444-4444-8444-444444444444"],"roomState":"waiting","maxPlayers":8,"category":{"decade":"2010s","artistType":"all","gender":"all","genre":"korean_ballad"},"catalogVersion":"test-v1"},"timestamp":1795132800000}
```

예시의 test-v1/korean_ballad는 테스트용이며 실제 선택지 출시를 의미하지 않는다.

| 시나리오 | 순서 |
|---|---|
| 대기→배정 | MATCH_REQUEST → MATCH_WAITING → WELCOME |
| 즉시 배정 | MATCH_REQUEST → WELCOME |
| 대기 취소 | MATCH_CANCEL → 대기 제거 → MATCH_CANCELLED |
| 등록 중 취소 | MATCH_CANCEL → 등록 결과 수신 → membership 제거 확인 → MATCH_CANCELLED |
| 취소 후 재요청 | R1 취소 → R2 접수/WAITING → R1 정리 완료 → R2 배정 가능 |
| 유효하지 않은 조건 | MATCH_REQUEST → MATCH_FAILED(INVALID_CRITERIA) |
| 방 나가기 | LEAVE_ROOM → 참가 제거 → ROOM_LEFT |

이 예시는 구조 검증용이다. 후속 클라이언트도 재사용할 공통 fixture 파일을 서버 T0에서 만들고 JSON 파싱 및 모든 필수 필드/잘못된 사례를 검사한다.
