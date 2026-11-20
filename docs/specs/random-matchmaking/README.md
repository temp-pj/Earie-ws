# Go 서버 랜덤 매칭 실행 명세 v1.1

이번 실행 범위는 Go 서버만이다. iOS 환경 준비·구현·이슈·브랜치는 시작하지 않는다. 서버 안에서 T1과 T2를 병렬 실행한다.
문서 범위 버전: v1.1 / WebSocket 계약 버전: v1. 이번 변경은 클라이언트와의 통신 형식을 변경하지 않는다.

## 읽는 순서

1. [requirements.md](requirements.md): 서버 요구사항과 미래 앱 동작의 구분.
2. [protocol.md](protocol.md): 서버와 WebSocket 테스트 클라이언트의 계약.
3. [design.md](design.md): Go 구조·상태 소유권·경합 처리.
4. [tasks.md](tasks.md): 서버 작업 범위·의존성·검증·에이전트 지시문.

## 현재 산출물과 다음 행동

서버 기본 지침은 develop에 반영되어 있다. feature/random-matchmaking에 명세를 커밋·push했고 상위 이슈 [#29](https://github.com/temp-pj/Earie-ws/issues/29) 및 하위 이슈 #30~#36을 생성했다. 실제 하위 이슈/blocked-by 관계를 설정했다. Orca 구현 실행·구현 PR·하위 작업 브랜치는 아직 시작하지 않았다. 아래 1~3번 준비는 완료됐으며 다음은 4번 T0 실행이다. 실제 상태는 시작 시 GitHub에서 재확인한다.

1. 이 채팅에서 서버 AGENTS.md/CLAUDE.md만 develop 커밋·push 요청. 보호 규칙이 요구할 때만 설정 PR을 쓴다.
2. 이 채팅에서 서버 상위 이슈 및 feature/random-matchmaking 생성, 명세 커밋·push 요청.
3. 이 채팅에서 T0/T1/T2/T3/T4/T7/T9 서버 하위 이슈 생성 요청. 의존성 기록, T0만 Ready. T5/T6/T8은 만들지 않는다.
4. Orca에서 temp-ws 프로젝트와 T0 전용 worktree를 열어 구현 실행. Cursor로 코드를 볼 때도 해당 worktree를 연다.
5. 이 채팅에서 T0 PR 검토 요청. 수정은 원래 Orca 작업에 전달. 검토 후 명시적으로 기능 통합 브랜치 병합 요청.
6. T0 병합 후 Orca에서 T1/T2를 각 전용 worktree로 병렬 실행. 두 PR 병합 후 T3 시작.
7. T3 → T4 → T7 → T9는 앞 작업 병합 후 순차 실행. 모든 하위 PR 대상은 feature/random-matchmaking.
8. T9 서버 검증 완료 후 기능 통합 브랜치 → develop 최종 PR. iOS 연동·실음원 검수·출시는 후속 단계다.

이슈는 미리 만들되 브랜치는 Ready 시 최신 통합 커밋에서 만든다. 문서는 자동 실행 설정이 아니다. 첫 사이클은 이 채팅의 상태 확인과 Orca 수동 작업 시작으로 운영한다. 자동 후속 실행은 Orca 설치 기능을 확인하고 별도로 설정한다.

## 문서 소유권과 변경

원본: Earie-ws의 docs/specs/random-matchmaking. iOS 저장소의 기존 안내는 후속 개발용이며 이번에 앱 작업을 시작하라는 의미가 아니다.
서버 저장소: https://github.com/temp-pj/Earie-ws (로컬 temp-ws).
기능 정책은 requirements, JSON은 protocol, 구조는 design, 실행 범위는 tasks가 담당한다. 작업자가 계약을 단독 변경하지 않는다.
초기 구현 계약 커밋 SHA는 7683c2507dad2c61da83bbede40651a3330c6fde다. 실행 시 최신 통합 브랜치 SHA도 함께 기록한다. GitHub 이슈가 실행 상태 원본이다.

## 서버 완료와 출시를 구분

T9는 Go 테스트와 실제 WebSocket 테스트 클라이언트, synthetic 곡 공급자로 독립 검증한다. Swift/Xcode/실제 iPhone이 필요하지 않다.
T10 실제 곡·분류 검수는 별도 출시 준비다. T10/iOS 준비가 없어도 서버 코드를 개발·테스트·develop 병합할 수 있다. 그것을 음악 재생·앱 연동·production 출시 검증 완료로 표현하지 않는다.
알려진 wire 비호환(/ws?protocolVersion=1) 때문에 이번 서버를 구 iOS가 사용하는 환경에 자동 배포하지 않는다.

## 도구

Cursor 루트 지침: https://cursor.com/docs/rules
Orca 병렬 worktree: https://orca.dev/
하나의 worktree에는 하나의 코드 작성 에이전트만 둔다. 이 채팅은 GitHub 준비/상태 확인/PR 리뷰, Orca는 구현 실행, Cursor는 해당 작업 코드 확인 역할로 사용한다.
