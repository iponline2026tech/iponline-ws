package controllers

// connection_test · saludo de punta a punta (14.2) con un servidor httptest y coder/websocket como cliente:
// welcome con los canales correctos, ticket inválido 4001, Origin inválido 4003, sin hello 4400, y eventos solo después del hello

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/core/server"
	"importadoraRealtime/src/features/access/dtos"
	"importadoraRealtime/src/features/connection/services"
	ticketRepos "importadoraRealtime/src/features/tickets/repositories"

	"github.com/coder/websocket"
)

type fakeTickets struct{}

func (fakeTickets) Redeem(_ context.Context, ticket string) (ticketRepos.Identity, error) {
	if ticket != "bueno" {
		return ticketRepos.Identity{}, ticketRepos.ErrTicketUsed
	}
	company := int64(1)
	return ticketRepos.Identity{ActorType: "employee", ActorID: 3, CompanyID: &company, SessionID: 77}, nil
}

type fakeAccess struct{}

func (fakeAccess) Build(_ context.Context, id ticketRepos.Identity) (dtos.AccessContext, error) {
	if id.ActorID != 3 {
		return dtos.AccessContext{}, errors.New("no esperado")
	}
	company := int64(1)
	return dtos.AccessContext{ActorType: "employee", ActorID: 3, CompanyID: &company, WarehouseIDs: []int64{1}, SessionID: 77,
		Permissions: map[string]bool{"transfers:view": true}}, nil
}

func startServer(t *testing.T) (*httptest.Server, *hub.Hub) {
	t.Helper()
	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.Run(ctx)
	svc := services.NewConnectionService(fakeTickets{}, fakeAccess{}, h, services.Options{
		QueueSize: 16, PingInterval: time.Hour, PongTimeout: time.Second, HelloTimeout: 300 * time.Millisecond,
	}, func() int64 { return 9000 })
	ctrl := NewConnectionController(svc, server.NewOrigins([]string{"http://localhost:5173"}), server.NewIPLimiter(100), protocol.MaxClientMessage)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", ctrl.Connect)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, h
}

func dial(t *testing.T, srv *httptest.Server, ticket, origin string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?ticket=" + ticket
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {origin}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func readEnvelope(t *testing.T, conn *websocket.Conn) protocol.Envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("se esperaba un mensaje: %v", err)
	}
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func closeCode(t *testing.T, conn *websocket.Conn) websocket.StatusCode {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return websocket.CloseStatus(err)
		}
	}
}

func TestWelcomeWithChannelsThenLiveEvents(t *testing.T) {
	srv, h := startServer(t)
	conn := dial(t, srv, "bueno", "http://localhost:5173")

	welcome := readEnvelope(t, conn)
	var data struct {
		CurrentSeq      int64    `json:"currentSeq"`
		Channels        []string `json:"channels"`
		ContractVersion int      `json:"contractVersion"`
	}
	_ = json.Unmarshal(welcome.Data, &data)
	if welcome.Type != "system.welcome" || welcome.Seq != 0 || data.CurrentSeq != 9000 || data.ContractVersion != 1 ||
		!reflect.DeepEqual(data.Channels, []string{"company:1", "warehouse:1", "actor:employee:3"}) {
		t.Fatalf("welcome: %+v %+v", welcome, data)
	}

	// un evento que llega ANTES del hello se encola y sale después
	company := int64(1)
	h.Dispatch([]events.StoredEvent{{Seq: 9001, ID: "e1", Type: "transfer.created", CompanyID: &company, WarehouseIDs: []int64{1, 3},
		Permissions: []string{"transfers:view"}, Actor: events.Actor{Type: "employee", Name: "Jhon"}, Payload: json.RawMessage(`{"transfer":{}}`), OccurredAt: time.Now()}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","v":1,"lastSeq":null}`)); err != nil {
		t.Fatal(err)
	}
	if ev := readEnvelope(t, conn); ev.Type != "transfer.created" || ev.Seq != 9001 {
		t.Fatalf("después del hello debe llegar el evento encolado: %+v", ev)
	}
	if h.Connections() != 1 {
		t.Fatalf("la conexión debe estar registrada: %d", h.Connections())
	}
}

func TestInvalidTicketCloses4001(t *testing.T) {
	srv, _ := startServer(t)
	if code := closeCode(t, dial(t, srv, "usado", "http://localhost:5173")); code != protocol.CloseTicketInvalid {
		t.Fatalf("ticket inválido → 4001, llegó %d", code)
	}
}

func TestInvalidOriginCloses4003(t *testing.T) {
	srv, _ := startServer(t)
	if code := closeCode(t, dial(t, srv, "bueno", "http://localhost:5174")); code != protocol.CloseOriginRejected {
		t.Fatalf("Origin no permitido → 4003, llegó %d", code)
	}
}

func TestNoHelloCloses4400(t *testing.T) {
	srv, h := startServer(t)
	conn := dial(t, srv, "bueno", "http://localhost:5173")
	_ = readEnvelope(t, conn) // welcome
	if code := closeCode(t, conn); code != protocol.CloseProtocolError {
		t.Fatalf("sin hello a tiempo → 4400, llegó %d", code)
	}
	for i := 0; i < 100 && h.Connections() != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if h.Connections() != 0 {
		t.Fatal("al cerrar se quita del hub")
	}
}

func TestUnknownMessageCloses4400(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv, "bueno", "http://localhost:5173")
	_ = readEnvelope(t, conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"subscribe","channel":"company:2"}`))
	if code := closeCode(t, conn); code != protocol.CloseProtocolError {
		t.Fatalf("mensaje desconocido → 4400, llegó %d", code)
	}
}
