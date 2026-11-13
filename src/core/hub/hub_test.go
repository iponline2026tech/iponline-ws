package hub

// hub_test · canales por actor, filtro de eventos, cola acotada y el hub: registro concurrente, cierre por sesión, límite de pestañas,
// cola llena y reparto solo a quien corresponde (con un socket falso: no abre sockets reales)
// ojo: -race no corre en Windows sin gcc; estas pruebas meten muchas goroutines para cubrir la concurrencia y conviene correr -race en Linux

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/features/access/dtos"

	"github.com/coder/websocket"
)

func i64(n int64) *int64   { return &n }
func str(s string) *string { return &s }

func employee(id, company, warehouse, session int64, perms ...string) dtos.AccessContext {
	p := map[string]bool{}
	for _, k := range perms {
		p[k] = true
	}
	return dtos.AccessContext{ActorType: "employee", ActorID: id, CompanyID: i64(company), WarehouseIDs: []int64{warehouse}, SessionID: session, Permissions: p}
}

func admin(id, company, session int64, warehouses ...int64) dtos.AccessContext {
	return dtos.AccessContext{ActorType: "admin", ActorID: id, CompanyID: i64(company), WarehouseIDs: warehouses, SessionID: session, IsAdmin: true, Permissions: map[string]bool{}}
}

func superadmin(id, session int64) dtos.AccessContext {
	return dtos.AccessContext{ActorType: "superadmin", ActorID: id, SessionID: session, IsSuperadmin: true, Permissions: map[string]bool{}}
}

func TestChannelsFor(t *testing.T) {
	cases := map[string]struct {
		access dtos.AccessContext
		want   []string
	}{
		"empleado":   {employee(18, 1, 7, 1), []string{"company:1", "warehouse:7", "actor:employee:18"}},
		"admin":      {admin(4, 1, 1, 1, 2, 3), []string{"company:1", "warehouse:1", "warehouse:2", "warehouse:3", "actor:admin:4"}},
		"superadmin": {superadmin(1, 1), []string{"platform", "actor:superadmin:1"}},
	}
	for name, c := range cases {
		if got := ChannelsFor(c.access); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %v, esperaba %v", name, got, c.want)
		}
	}
	// decisión 2: aunque el superadmin tenga compañía de contexto, no entra a canales de compañía
	sa := superadmin(1, 1)
	sa.CompanyID = i64(1)
	if got := ChannelsFor(sa); !reflect.DeepEqual(got, []string{"platform", "actor:superadmin:1"}) {
		t.Errorf("superadmin con contexto: %v", got)
	}
}

func TestAllowed(t *testing.T) {
	seller := employee(3, 1, 1, 10, "transfers:view")
	other := employee(5, 1, 2, 11, "transfers:view")
	ev := func(company *int64, warehouses []int64, perms ...string) events.StoredEvent {
		return events.StoredEvent{Type: "x", CompanyID: company, WarehouseIDs: warehouses, Permissions: perms}
	}
	cases := []struct {
		name   string
		access dtos.AccessContext
		event  events.StoredEvent
		want   bool
	}{
		{"otra compañía", seller, ev(i64(2), nil), false},
		{"otra bodega en un evento de bodega", seller, ev(i64(1), []int64{3}, "transfers:view"), false},
		{"bodega de destino del traslado", other, ev(i64(1), []int64{1, 2}, "transfers:view"), true},
		{"misma compañía sin el permiso", seller, ev(i64(1), nil, "stock:view"), false},
		{"uno de los permisos alcanza", seller, ev(i64(1), nil, "stock:view", "transfers:view"), true},
		{"sin permisos requeridos", seller, ev(i64(1), nil), true},
		{"admin pasa todos los permisos", admin(4, 1, 20, 1, 2), ev(i64(1), []int64{2}, "payments:view"), true},
		{"admin de otra compañía no", admin(4, 2, 20, 9), ev(i64(1), nil), false},
		{"superadmin en evento de plataforma", superadmin(1, 30), events.StoredEvent{Platform: true, CompanyID: i64(1), Permissions: []string{"admins:view"}}, true},
		{"superadmin no recibe eventos de compañía", superadmin(1, 30), ev(i64(1), nil), false},
		{"evento personal de otra persona", seller, events.StoredEvent{ActorChannel: str("employee:5")}, false},
		{"evento personal propio", seller, events.StoredEvent{ActorChannel: str("employee:3")}, true},
	}
	for _, c := range cases {
		if got := Allowed(c.access, c.event); got != c.want {
			t.Errorf("%s: %v, esperaba %v", c.name, got, c.want)
		}
	}
}

func TestQueueFull(t *testing.T) {
	q := newQueue(2)
	if !q.TryPush(message{}) || !q.TryPush(message{}) || q.TryPush(message{}) {
		t.Fatal("la tercera debe fallar en una cola de 2")
	}
}

// fakeSocket registra lo que se escribe y con qué código se cerró
type fakeSocket struct {
	mu     sync.Mutex
	writes [][]byte
	closed chan websocket.StatusCode
}

func newFakeSocket() *fakeSocket { return &fakeSocket{closed: make(chan websocket.StatusCode, 1)} }

func (f *fakeSocket) Read(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
func (f *fakeSocket) Write(_ context.Context, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, data)
	return nil
}
func (f *fakeSocket) Ping(context.Context) error { return nil }
func (f *fakeSocket) Close(code websocket.StatusCode, _ string) error {
	select {
	case f.closed <- code:
	default:
	}
	return nil
}

func runHub(t *testing.T, maxPerActor int) *Hub {
	h := New(maxPerActor)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.Run(ctx)
	return h
}

// eventually espera hasta 2 s a que se cumpla la condición (Register regresa antes de que el hub termine de agregar)
func eventually(t *testing.T, ok func() bool, msg string) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

func waitClosed(t *testing.T, c *Client) websocket.StatusCode {
	t.Helper()
	select {
	case <-c.Done():
		return c.CloseCode()
	case <-time.After(2 * time.Second):
		t.Fatalf("la conexión %s no se cerró", c.ID)
		return 0
	}
}

func TestRegisterUnregisterConcurrently(t *testing.T) {
	h := runHub(t, 50)
	var wg sync.WaitGroup
	clients := make([]*Client, 200)
	for i := range clients {
		clients[i] = NewClient(fmt.Sprint(i), newFakeSocket(), employee(int64(i%40), 1, 1, int64(i)), 8)
	}
	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) { defer wg.Done(); h.Register(c) }(c)
	}
	wg.Wait()
	eventually(t, func() bool { return h.Connections() == 200 }, "deben registrarse las 200")
	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) { defer wg.Done(); h.Unregister(c); h.Unregister(c) }(c) // quitar dos veces no rompe
	}
	wg.Wait()
	eventually(t, func() bool { return h.Connections() == 0 }, "deben quitarse todas")
}

func TestCloseSessionOnlyThatSession(t *testing.T) {
	h := runHub(t, 5)
	tab1 := NewClient("a", newFakeSocket(), employee(3, 1, 1, 100), 8)
	tab2 := NewClient("b", newFakeSocket(), employee(3, 1, 1, 100), 8)
	other := NewClient("c", newFakeSocket(), employee(3, 1, 1, 101), 8)
	for _, c := range []*Client{tab1, tab2, other} {
		h.Register(c)
	}
	h.CloseSession(100, protocol.CloseSessionRevoked)
	if waitClosed(t, tab1) != protocol.CloseSessionRevoked || waitClosed(t, tab2) != protocol.CloseSessionRevoked {
		t.Fatal("las dos pestañas de la sesión 100 deben cerrar con 4401")
	}
	select {
	case <-other.Done():
		t.Fatal("la otra sesión no debe cerrarse")
	default:
	}
	eventually(t, func() bool { return h.Connections() == 1 }, "debe quedar 1 conexión")
}

func TestSixthTabClosesOldest(t *testing.T) {
	h := runHub(t, 5)
	var tabs []*Client
	for i := 0; i < 6; i++ {
		c := NewClient(fmt.Sprint(i), newFakeSocket(), employee(3, 1, 1, int64(i)), 8)
		tabs = append(tabs, c)
		h.Register(c)
	}
	if waitClosed(t, tabs[0]) != protocol.CloseTooManyConnections {
		t.Fatal("la más vieja debe cerrar con 4429")
	}
	eventually(t, func() bool { return h.Connections() == 5 }, "deben quedar 5 conexiones")
}

func TestDispatchFiltersAndOverflows(t *testing.T) {
	h := runHub(t, 5)
	inW1 := NewClient("w1", newFakeSocket(), employee(3, 1, 1, 1, "transfers:view"), 8)
	inW3 := NewClient("w3", newFakeSocket(), employee(6, 1, 3, 2, "transfers:view"), 8)
	noPerm := NewClient("np", newFakeSocket(), employee(7, 1, 1, 3), 8)
	slow := NewClient("slow", newFakeSocket(), employee(8, 1, 1, 4, "transfers:view"), 1)
	for _, c := range []*Client{inW1, inW3, noPerm, slow} {
		h.Register(c)
	}
	transfer := func(seq int64) events.StoredEvent {
		return events.StoredEvent{Seq: seq, ID: fmt.Sprint(seq), Type: "transfer.status_changed", CompanyID: i64(1),
			WarehouseIDs: []int64{1, 2}, Permissions: []string{"transfers:view"}, Payload: []byte(`{}`)}
	}
	h.Dispatch([]events.StoredEvent{transfer(1), transfer(2)})
	if waitClosed(t, slow) != protocol.CloseQueueOverflow {
		t.Fatal("la cola de 1 debe llenarse con 2 eventos y cerrar con 4408")
	}
	if len(inW1.q.ch) != 2 || len(inW3.q.ch) != 0 || len(noPerm.q.ch) != 0 {
		t.Fatalf("reparto: w1=%d w3=%d sinPermiso=%d", len(inW1.q.ch), len(inW3.q.ch), len(noPerm.q.ch))
	}
	if h.QueueOverflows() != 1 || h.LastProcessedSeq() != 2 {
		t.Fatalf("métricas: overflows=%d lastSeq=%d", h.QueueOverflows(), h.LastProcessedSeq())
	}
	first, second := <-inW1.q.ch, <-inW1.q.ch
	if first.seq != 1 || second.seq != 2 {
		t.Fatal("se entregan en orden de seq")
	}
}

func TestCloseAllOnShutdown(t *testing.T) {
	h := runHub(t, 5)
	a := NewClient("a", newFakeSocket(), employee(3, 1, 1, 1), 8)
	b := NewClient("b", newFakeSocket(), admin(4, 1, 2, 1), 8)
	h.Register(a)
	h.Register(b)
	h.CloseAll(protocol.CloseGoingAway)
	if waitClosed(t, a) != protocol.CloseGoingAway || waitClosed(t, b) != protocol.CloseGoingAway {
		t.Fatal("el apagado cierra todo con 1001")
	}
}

func TestWriteLoopSkipsAlreadySent(t *testing.T) {
	sock := newFakeSocket()
	c := NewClient("x", sock, employee(3, 1, 1, 1), 8)
	c.lastSent = 5
	c.q.TryPush(message{seq: 4, data: []byte("viejo")})
	c.q.TryPush(message{seq: 6, data: []byte("nuevo")})
	c.q.TryPush(message{seq: 0, data: []byte("sistema")})
	ctx, cancel := context.WithCancel(context.Background())
	go c.WriteLoop(ctx, time.Hour, time.Second)
	time.Sleep(100 * time.Millisecond)
	cancel()
	sock.mu.Lock()
	defer sock.mu.Unlock()
	if len(sock.writes) != 2 || string(sock.writes[0]) != "nuevo" || string(sock.writes[1]) != "sistema" {
		t.Fatalf("debe saltar lo ya enviado (seq ≤ 5): %q", sock.writes)
	}
}
