 <div align="center">
  <h1>Earie Server</h1>
  <p><strong>Real-time WebSocket Server for Earie</strong></p>
  <p>실시간 멀티플레이 음악 퀴즈 Earie의<br/>방 관리, 라운드 진행, 정답 판정을 담당하는 Go 서버</p>
  <p>
    <img src="https://img.shields.io/badge/Go_1.24-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go 1.24" />
    <img src="https://img.shields.io/badge/WebSocket-333333?style=for-the-badge" alt="WebSocket" />
    <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  </p>
</div>

<br/>

## How It Works

<img width="800" alt="server-sequence-dark" src="https://github.com/user-attachments/assets/8c732b01-e12d-4cae-9622-372817cc8c56" />


### Message Protocol

모든 메시지는 `type`, `payload`, `timestamp`로 구성된 JSON 형식입니다.

```json
{
  "type": "SUBMIT_ANSWER",
  "payload": {},
  "timestamp": 1728600000000
}
```

| 방향 | 타입 | 설명 |
|:---|:---|:---|
| Client → Server | `START_GAME` | 방장의 게임 시작 요청 |
| Client → Server | `READY_TO_PLAY` | 곡 로드 완료 알림 |
| Client → Server | `SUBMIT_ANSWER` | 답 제출 |
| Client → Server | `KICK_PLAYER` | 방장의 플레이어 강퇴 요청 |
| Server → Client | `WELCOME` | 입장한 플레이어에게 방 정보 전달 |
| Server → Client | `PLAYER_JOINED` · `PLAYER_LEFT` | 플레이어 입장 · 퇴장 알림 |
| Server → Client | `HOST_CHANGED` | 방장 변경 알림 |
| Server → Client | `PLAYER_KICKED` | 강퇴 결과 알림 |
| Server → Client | `GAME_STARTED` | 게임 시작 알림 |
| Server → Client | `PRELOAD_SONG` | 다음 곡과 시작 지점 전달 |
| Server → Client | `ROUND_STARTED` | 라운드 시작 알림 |
| Server → Client | `COUNT_DOWN` | 라운드 남은 시간 전달 |
| Server → Client | `WRONG_ANSWER` | 오답 알림 |
| Server → Client | `ROUND_RESULT` | 라운드 결과 전달 |
| Server → Client | `GAME_OVER` | 최종 게임 결과 전달 |

<br/>

## Architecture
<img width="800" alt="server-architecture-dark" src="https://github.com/user-attachments/assets/0a5922c2-9e59-4cc8-901e-0ee3dbcf106c" />


각 `Room`은 하나의 `Run` 루프에서 방 상태 변경을 순차적으로 처리합니다. WebSocket 연결의 읽기·쓰기와 방 상태 관리를 분리하고, channel을 통해 연결 이벤트와 메시지를 Room에 전달합니다. `Hub`는 방의 생성·조회·삭제를 담당하며, 게임 규칙은 별도 패키지로 분리했습니다.

### Package Structure

| 패키지 | 역할 |
|:---|:---|
| `cmd/server/` | 서버 진입점, HTTP 서버, `/ws` 핸들러, DB 연결 |
| `internal/client/` | WebSocket 연결 및 읽기·쓰기 |
| `internal/room/` | Hub와 Room 상태 관리, 이벤트 루프 |
| `internal/game/` | 라운드 진행, 글자 카드 생성, 정답 판정 |
| `internal/message/` | 메시지 타입과 payload 정의 |
| `internal/music/` | 곡 데이터 조회 |

<br/>

## Tests

전체 테스트는 다음 명령으로 실행할 수 있습니다.

```bash
go test ./...
```

| 패키지 | 테스트 수 | 검증 범위 |
|:---|:---:|:---|
| `internal/game` | 17 | 글자 카드 생성, 라운드 진행, 시간 초과, 정답 판정, 남은 시간 계산, 시작 지점 |
| `internal/room` | 9 | Hub 방 생성 · 조회 · 삭제, 동시 접근, Room 등록 · 해제 · 브로드캐스트 |
