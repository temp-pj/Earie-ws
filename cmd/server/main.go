package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/coder/websocket"
	_ "github.com/lib/pq"

	"temp-ws/internal/client"
	message "temp-ws/internal/message/v1"
	"temp-ws/internal/room"
	"temp-ws/internal/session"
)

func main() {
	db := mustConnectDB()
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	events := session.NewEventQueue(ctx, 256)
	gateway := room.NewGateway(ctx, events)
	registry := session.NewRegistry(ctx, gateway, nil, nil)
	// T3 injects the match handler and ordered event consumer. No room is
	// allocated by a connection, and the old direct-room endpoint is disabled.

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket(registry))

	log.Println("서버 시작: localhost:8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal("서버 에러:", err)
	}
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
