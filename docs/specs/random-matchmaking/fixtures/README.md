# T0 v1 공통 계약과 공개 fixture

이 디렉터리는 이슈 #30의 서버 계약 검증 데이터다. 기준 wire 계약은
`7683c2507dad2c61da83bbede40651a3330c6fde`의 protocol.md이며, 작업 시작 통합 SHA는
`8bed7d69b9648ce4789a14548c535043e9203afc`다.

## Synthetic 데이터 경계

`test-v1`은 테스트 전용 버전이다. 모든 ISRC/MBID, 제목, 아티스트, 정답은 만든 값이며
실제 음원 조회·재생 가능성·분류 적합성을 주장하지 않는다. `ZZTST26…`는 형식 검증용
문자열이며 실제 할당된 ISRC가 아니다. `korean_ballad`와 `synthetic_pop`도 운영 선택지를
확정하지 않는다. DB, 원본 음악 데이터, 비밀 파일을 사용하지 않았다.

`catalog.json`은 서버 예시, `manifest.json`은 그 20개 가상 곡의 manifest,
`catalog-projection.json`은 곡 ID/정답을 제외한 공개 형식 예시다. projection 파일은
계약 데이터일 뿐 iOS 앱/번들 구현이 아니다. `catalog-unavailable.json`은 빈 배열로
이용 불가를 나타내는 예시다. 실제 출고 카탈로그와 실음원 검수는 별도 T10 작업이다.

## Fixture 형식과 실행

- `messages-valid.json`: `{name,direction,message,canonical?}` 배열. direction은
  `client` 또는 `server`. canonical이 있으면 알 수 없는 필드를 제거한 인코딩 결과다.
- `messages-invalid.json`: `{name,base,path,op,value?,direction?}` 배열. base는 정상
  fixture의 name, path는 JSON 키 또는 0부터 시작하는 배열 인덱스 문자열 목록이다.
  op가 `remove`면 키 삭제, `set`이면 value로 교체한다. 별도 `{name,direction,raw}`는
  잘못된 JSON 텍스트 사례다. 모든 사례는 `ErrInvalidMessage`(향후 close 1008) 대상이다.
- `criteria-invalid.json`: 구조적으로 정상인 MATCH_REQUEST. `reason:value`는 값 형식
  오류, `reason:catalog`는 버전/카탈로그 미지원 조건이다. 이들은 1008 대상이 아니며
  후속 요청 검증에서 `MATCH_FAILED/INVALID_CRITERIA`로 처리해야 한다.
- `categories.json`, `versions.json`: 요청/확정 방의 any/all 구분, enum, 태그 문법,
  버전 길이/문자 제한에 대한 정상·오류 사례다.
- `catalog-invalid.json`: `{name,file,path,op,value?}` 변형 레시피다. 원본 3개 파일을
  새로 읽은 뒤 지정 파일 하나에만 적용한다. 필수/null/타입, 중복, 버전 불일치,
  manifest 참조, 10곡 미달, 날짜/길이/시작점, projection 정보 노출 사례를 포함한다.

저장소 루트에서 `go test ./internal/message/v1 ./internal/music/v1 ./internal/sessioncontract`로
실행한다. 메시지 테스트는 실제 Decode/Encode API를 통과하며 모든 필수 객체 필드의
누락/null/타입 오류 레시피 존재 여부도 검사한다. catalog 테스트는 DTO roundtrip과
**테스트 전용 fixture 감사 함수**로 예시의 무결성을 확인한다. T2의 운영 loader·검증
구현을 대신하지 않으며 T2에서 같은 오류 사례를 실제 loader에도 연결해야 한다.

## 확정 shared API와 파일 소유권

| 파일 | 후속 작업에서 사용할 API |
|---|---|
| `internal/music/v1/category.go` | `Category`, Any/All/축 상수, `TracksPerGame`, `ValidateRequest`, `ValidateResolved`, `ValidCatalogVersion` |
| `internal/music/v1/catalog.go` | `Catalog`, `Genre`, `Pool`, `CatalogProjection`, `PoolProjection`, `Manifest`, `ManifestSong` |
| `internal/message/v1/payloads.go` | 모든 v1 Type/오류 상수, payload DTO, `ProtocolVersion`, `MaxFrameBytes`, `MaxRequestIDs`, `MaxPlayers` |
| `internal/message/v1/codec.go` | `Message`, `Direction`, `Decode`, `Encode`, `ErrInvalidMessage`, `ValidID` |
| `internal/sessioncontract/contract.go` | 아래 actor 계약, 상태/sequence/token 값, 연결·타임아웃 한도 |

v1 패키지의 package 이름은 각각 `music`, `message`다. 기존 런타임은 계속 구 패키지를
사용한다. T1/T2는 v1 import 경로를 사용하고, T7에서 구 프로토콜을 제거한다. 요청 DTO,
카탈로그, actor snapshot/초기 쌍 명령은 모두 **동일한 `music/v1.Category`**를 참조한다.
Category 값 검증은 문법과 any/all 제약까지만 확인한다. 지원 태그/버전 일치,
allowedRequests, 두 요청 해소 및 pool 적합성은 T2 책임이다.

wire 입력은 `json.Unmarshal` 단독 사용 대신 `message.Decode(data, ClientToServer)`를
사용한다. 성공한 `Message.Payload`는 해당 DTO의 포인터다. 필수/null/타입/UUID 오류와
잘못된 방향/type은 `errors.Is(err, ErrInvalidMessage)`로 식별한다. 알 수 없는 객체
필드는 무시하고 canonical UUID v4만 연결·방·요청 ID로 허용한다. timestamp 포인터는
없음과 0을 구분하며 서버 메시지에는 반드시 지정한다. `Encode`는 DTO 값/포인터를 받고
type과 payload의 조합도 검증한다. 프레임 종류·크기·endpoint 협상은 T1 전송 계층 책임이다.

기존 게임의 빈 winner는 그대로 허용한다. PLAYER_KICKED와 GAME_STARTED의 payload는
`{roomID}`다. START_GAME의 옛 category/trackCount/timeLimit는 알 수 없는 필드로 무시한다.
게임 점수·승자·시간 정책은 변경하지 않는다.

## Actor 동기화 계약

- `RequestKey`는 내부 connectionID/requestID, `Member`는 playerID/requestID/arrivalSequence다.
  arrivalSequence/creationSequence는 process 전역 단조 순서이며 wire timestamp와 무관하다.
- `RoomGateway`의 CreateInitialPair/Register/Remove/Publish는 비차단 접수만 수행한다.
  nil은 접수 성공이고 완료는 `EventSink`로 전달한다. 오류이면 미접수다. 접수 후 context
  취소만으로 등록 실패/정리 완료를 추론하지 않는다. `OperationToken`은 내부 세대 식별자다.
- 최초 쌍은 `[2]Member`와 단일 `InitialPairResult`로 전원 성공/실패를 표현한다.
  `CreateInitialPair`는 Hub가 발급한 roomID/creationSequence/Done을 `RoomHandle`로 즉시
  반환한다. 첫 등록 결과가 오지 않아도 이 handle로 방을 격리·Close할 수 있다. handle은
  등록 성공이나 공개를 의미하지 않는다. 결과의 두 requestID를 처리하고 필요한
  scoped Remove 완료를 받은 후 `Publish`한다. 0명은 Close, 1명은 남은 membership으로 공개한다.
- `EventBatch`의 roomID/revision은 모든 결과에 적용된다. 같은 actor turn의 등록 결과,
  snapshot, wire event, 정리 결과를 분할하지 않는다. 이벤트 slice 순서와 방별 FIFO를 유지한다.
  등록 결과의 `Admission`과 batch snapshot으로 중앙 dispatcher가 WELCOME을 먼저 생성한다.
- `WireEvent`는 수신 membership 목록과 v1 DTO를 담는다. Room은 WELCOME이나 socket write를
  직접 수행하지 않는다. 전달한 slice/map/DTO 포인터의 소유권은 수신자에게 넘어가므로
  발행자는 이후 수정하지 않거나 미리 복사해야 한다.
- Remove는 roomID/playerID/requestID가 일치할 때만 제거한다. `RemovalResult`는 이미 없는
  membership도 성공한 정리로 확인한다. 늦은 결과는 토큰과 요청을 함께 비교해야 한다.
- Close는 취소를 시작하고 Done 채널을 즉시 반환한다. Done 종료와 해당 방의 큐 drain을
  모두 확인해야 quarantine/terminal 기록을 해제한다. `Closed` 이벤트나 5초 timeout만으로
  정리 완료를 선언하지 않는다. 반복 Close/늦은 결과 처리는 T1/T4에서 멱등 구현한다.
- `RoomStarting.PublicState()`는 playing이다. initializing/closed는 admission용 내부 상태이며
  WELCOME에 보내지 않는다. snapshot은 더 높은 revision만 적용하고 Closed 후 늦은 것은 버린다.

여기서는 actor, queue, timer, network를 구현하지 않는다. 비차단·취소·drain·WELCOME 순서의
실제 동시성 보장은 T1/T3/T4의 controlled-event 테스트, 게임 수명은 T7, socket 통합은 T9에서
검증한다. T0 PR의 병합 확인 전에는 T1/T2 착수 조건이 충족되지 않는다.
