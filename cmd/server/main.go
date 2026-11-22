package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/coder/websocket"
	_ "github.com/lib/pq"

	"temp-ws/internal/client"
	"temp-ws/internal/matchmaking"
	message "temp-ws/internal/message/v1"
	"temp-ws/internal/music"
	"temp-ws/internal/room"
	"temp-ws/internal/session"
)

func main() {
	db := mustConnectDB()
	defer func() { _ = db.Close() }()

	catalog, err := loadMatchingCatalog(os.Getenv("CATALOG_PATH"), os.Getenv("SONG_MANIFEST_PATH"))
	if err != nil {
		log.Fatal("매칭 카탈로그 로드 실패: CATALOG_PATH 및 SONG_MANIFEST_PATH의 검증된 파일이 필요합니다")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry, _ := newMatchingRegistry(ctx, catalog)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket(registry))

	log.Println("서버 시작: localhost:8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal("서버 에러:", err)
	}
}

// Both production and socket tests use this assembly. Synthetic fixtures are
// supplied by tests only; missing deployment data never falls back to test-v1.
// The catalog is the default song provider; options are applied after it, so
// integration tests inject a song provider or clock through this same path
// instead of a test-only branch. Production passes no options.
func newMatchingRegistry(ctx context.Context, catalog *music.CatalogStore, options ...room.Option) (*session.Registry, <-chan struct{}) {
	events := session.NewEventQueue(ctx, 256)
	gateway := room.NewGateway(ctx, events, append([]room.Option{room.WithSongs(catalog)}, options...)...)
	matcher := matchmaking.New(ctx, gateway, catalog, events.Batches())
	go matcher.Run()
	return matcher.Registry(), matcher.Done()
}

func loadMatchingCatalog(catalogPath, manifestPath string) (*music.CatalogStore, error) {
	catalog, err := os.Open(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	defer func() { _ = catalog.Close() }()
	manifest, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer func() { _ = manifest.Close() }()
	return music.LoadCatalog(catalog, manifest)
}

func mustConnectDB() *sql.DB {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL 환경변수가 설정되지 않음")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("DB 연결 실패:", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal("DB Ping 실패:", err)
	}

	log.Println("DB 연결 성공!")
	return db
}

func handleWebSocket(registry *session.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil || len(query["protocolVersion"]) != 1 || query.Get("protocolVersion") != message.ProtocolVersion || query.Has("room") {
			http.Error(w, "protocolVersion=1 required; room query is unsupported", http.StatusBadRequest)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		cli := client.NewConnection(r.Context(), conn)
		connection := registry.Connect(cli.ID, cli)
		defer registry.Disconnect(connection.ID)
		cli.Run(func(msg message.Message) error { return registry.Route(connection.ID, msg) })
	}
}
