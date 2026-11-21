# T2 카탈로그 조회 API

이슈 #32 / REQ-CAT-03~06, REQ-DATA-01~02, REQ-GAME-02.
기준 통합 커밋: `8ad11c460a78610b280955858be2f0b8515afa9e` (T0 PR #37 병합).
wire v1과 `internal/music/v1`의 공통 Category·Catalog·Manifest 타입을 그대로 사용한다.

## 후속 작업 연결

1. 시작 시 `music.LoadCatalog(catalogReader, manifestReader)`로 같은 버전의 서버 파일을
   읽는다. 실패 시 store는 nil이며 `errors.Is(err, music.ErrInvalidCatalog)`로 구분한다.
   reader의 파일 열기/닫기는 호출자가 소유한다. 실제 DB나 환경 변수가 필요하지 않다.
2. `Available()`은 선택 배열, pool과 기본 전부 any 요청이 제공되는지 확인한다.
   빈 배열 카탈로그는 로딩 가능하지만 매칭 요청은 거절한다.
3. 요청 접수 시 `ValidateRequest(version, category)`를 호출한다. 정확한 allowedRequests
   조합 및 버전이 필요하다. `ErrInvalidCriteria`를 MATCH_FAILED/INVALID_CRITERIA로 매핑한다.
4. 신규 쌍은 `ResolvePair(version, a, b)`로 결정한다. 오류는 요청 자체가 유효하지 않음을
   뜻한다. `(zero, false, nil)`은 서로 불일치하거나 확정 pool이 없다는 뜻으로, 다음 후보를
   탐색하거나 대기를 유지한다. 조건을 완화하거나 다른 pool을 대신 선택하지 않는다.
5. 기존 방 충원은 `CanJoin(version, request, resolved)`로 확인한다. 방의 version을 전달한다.
   Room은 확정 Category를 변경하지 않는다.
6. 게임 시작 작업은 `FetchSongs(ctx, version, resolved)`로 확정 pool의 서로 다른 ISRC
   10개를 무작위 조회한다. 반환 타입은 `[]musicv1.ManifestSong`이며 기본 startTime도 보존한다.
   기존 game의 Song 변환·시작점 생성과 Room 연결은 T7에서 수행한다.

`criteria.MergeRequests`와 `criteria.CanJoin`은 공통 Category의 문법과 호환 관계만
확인하는 순수 함수다. 지원 태그/allowedRequests/실제 pool 검증은 위 store API가 담당한다.
Matchmaker·cmd/server의 의존성 조립은 T3, socket 통합은 T9 범위다.

## 데이터와 소유권

로더는 필수/non-null 필드, 버전, 태그, manifest 메타데이터와 참조, pool당 고유 ISRC
10개 이상을 검사한다. T0의 오류 fixture와 계약에 따라 manifest/pool 내 중복 ISRC는
거절하며 중복 행을 곡 수에 가산하지 않는다. 제목이 같은 다른 ISRC는 별도 곡이다.
여러 pool이 같은 ISRC를 참조하는 것은 허용된다.

로딩 완료 후 snapshot은 불변이다. 요청과 pool 조회는 메모리에서 수행하며 goroutine이나
DB 작업을 만들지 않는다. projection과 조회 곡은 복사본이므로 여러 actor에서 함께 사용해도
내부 상태를 변경할 수 없다. 요청/방의 생명주기·취소 처리는 이 API 밖의 담당 작업이 소유한다.

`Projection()`은 표시 순서와 고유 곡 수를 보존하고 곡 ID/정답을 포함하지 않는다.
`ValidateProjection(reader)`는 공개 파일이 해당 snapshot과 일치하는지 검증한다.
알 수 없는 파일 필드를 거절하여 projection의 songIDs 등 정보 노출도 검출한다.

공개 `test-v1` fixture를 실제 로더·wire decoder·조건 판정·곡 조회에 연결해 검증한다.
실제 음원 재생과 분류의 적합성, 운영 카탈로그 제작·검수는 별도 T10 범위다.

## 검증 명령

저장소 루트에서 Go 1.24.2와 Go 1.24.2로 빌드한 golangci-lint v2.1.6을 사용한다.

```sh
GOTOOLCHAIN=go1.24.2 go test -count=1 ./internal/matchmaking/criteria ./internal/music/...
GOTOOLCHAIN=go1.24.2 go test -race -count=1 ./...
GOTOOLCHAIN=go1.24.2 go test -count=1 ./...
GOTOOLCHAIN=go1.24.2 go build ./...
GOTOOLCHAIN=go1.24.2 golangci-lint run
```
