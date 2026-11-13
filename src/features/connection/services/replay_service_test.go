package services

// replay_service_test · pruebas de recuperación por lastSeq contra PostgreSQL real: exacta, filtrada por alcance/permisos,
// resync por purga o exceso de eventos, y entrega ordenada de eventos en vivo encolados durante la recuperación
// conecta con: replay_service.go, connection_service.go, events/repositories/event_repo.go y core/hub
// ojo: solo corre si TEST_DATABASE_URL está definida (base importadora_test); limpia las filas que inserta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/features/access/dtos"
	eventRepos "importadoraRealtime/src/features/events/repositories"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL no está definida")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func cleanReplayEvents(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	ctx := context.Background()
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock(20261129)`); err != nil {
		lockConn.Release()
		t.Fatal(err)
	}
	var before int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE(MAX(seq), 0) FROM realtime_events`).Scan(&before)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM realtime_events WHERE seq > $1`, before)
		_, _ = lockConn.Exec(ctx, `SELECT pg_advisory_unlock(20261129)`)
		lockConn.Release()
	})
	return before
}

type captureSocket struct {
	mu     sync.Mutex
	msgs   []protocol.Envelope
	closed chan websocket.StatusCode
}

func newCaptureSocket() *captureSocket {
	return &captureSocket{closed: make(chan websocket.StatusCode, 1)}
}

func (c *captureSocket) Read(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *captureSocket) Write(_ context.Context, data []byte) error {
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err == nil {
		c.mu.Lock()
		c.msgs = append(c.msgs, env)
		c.mu.Unlock()
	}
	return nil
}
func (c *captureSocket) Ping(context.Context) error { return nil }
func (c *captureSocket) Close(code websocket.StatusCode, _ string) error {
	select {
	case c.closed <- code:
	default:
	}
	return nil
}

func (c *captureSocket) received() []protocol.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]protocol.Envelope, len(c.msgs))
	copy(out, c.msgs)
	return out
}

func empAccess(id, company, warehouse int64, perms ...string) dtos.AccessContext {
	p := map[string]bool{}
	for _, k := range perms {
		p[k] = true
	}
	return dtos.AccessContext{
		ActorType:    "employee",
		ActorID:      id,
		CompanyID:    &company,
		WarehouseIDs: []int64{warehouse},
		SessionID:    1,
		Permissions:  p,
	}
}

func insertEventRow(t *testing.T, pool *pgxpool.Pool, companyID *int64, warehouses []int64, eventType string, perms []string) int64 {
	t.Helper()
	if warehouses == nil {
		warehouses = []int64{}
	}
	if perms == nil {
		perms = []string{}
	}
	var seq int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload)
		VALUES ($1, $2, $3, $4, 'employee', 3, 'Juan', $5, '{}'::jsonb)
		RETURNING seq`, companyID, warehouses, eventType, perms, companyID == nil).Scan(&seq)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func i64(v int64) *int64 { return &v }

func TestReplayExact(t *testing.T) {
	pool := testPool(t)
	cleanReplayEvents(t, pool)
	ctx := context.Background()

	// compañía propia de la prueba: otros paquetes insertan eventos de la compañía 1 al mismo tiempo
	var company int64
	if err := pool.QueryRow(ctx, `INSERT INTO companies (name) VALUES ($1) RETURNING id`, fmt.Sprintf("replay-prueba-%d", time.Now().UnixNano())).Scan(&company); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM companies WHERE id = $1", company) })

	// Insertar 10 eventos y recuperar desde el seq anterior al primero
	var seqs []int64
	for i := 0; i < 10; i++ {
		seqs = append(seqs, insertEventRow(t, pool, i64(company), nil, "sale.created", nil))
	}
	lastSeq := seqs[0] - 1
	currentSeq := seqs[len(seqs)-1]

	sock := newCaptureSocket()
	client := hub.NewClient("r1", sock, empAccess(3, company, 1), 64)
	svc := NewReplayService(eventRepos.NewEventRepository(pool), 5000)

	if err := svc.Replay(ctx, client, lastSeq, currentSeq, nil); err != nil {
		t.Fatal(err)
	}

	got := sock.received()
	if len(got) != 11 {
		t.Fatalf("esperaba 10 eventos + system.replay_completed (11), llegaron %d", len(got))
	}
	for i := 0; i < 10; i++ {
		if got[i].Seq != seqs[i] {
			t.Fatalf("evento %d: seq %d, esperaba %d", i, got[i].Seq, seqs[i])
		}
	}
	last := got[10]
	if last.Type != "system.replay_completed" || last.Seq != 0 {
		t.Fatalf("último mensaje debe ser system.replay_completed: %+v", last)
	}
	var body struct {
		FromSeq int64 `json:"fromSeq"`
		ToSeq   int64 `json:"toSeq"`
		Count   int   `json:"count"`
	}
	_ = json.Unmarshal(last.Data, &body)
	if body.FromSeq != lastSeq+1 || body.ToSeq != currentSeq || body.Count != 10 {
		t.Fatalf("datos de replay_completed inesperados: %+v", body)
	}
}

func TestReplayFilteredByScope(t *testing.T) {
	pool := testPool(t)
	cleanReplayEvents(t, pool)
	ctx := context.Background()

	s1 := insertEventRow(t, pool, i64(1), []int64{1}, "transfer.created", []string{"transfers:view"}) // sí (misma bodega y permiso)
	_ = insertEventRow(t, pool, nil, nil, "entity.created", nil)                                      // no (evento de plataforma, sin company_id)
	_ = insertEventRow(t, pool, i64(1), []int64{3}, "transfer.created", []string{"transfers:view"})   // no (otra bodega)
	s4 := insertEventRow(t, pool, i64(1), nil, "sale.created", []string{"orders:view"})               // no (falta orders:view)

	sock := newCaptureSocket()
	client := hub.NewClient("r2", sock, empAccess(3, 1, 1, "transfers:view"), 64)
	svc := NewReplayService(eventRepos.NewEventRepository(pool), 5000)

	if err := svc.Replay(ctx, client, s1-1, s4, nil); err != nil {
		t.Fatal(err)
	}

	got := sock.received()
	if len(got) != 2 || got[0].Seq != s1 || got[1].Type != "system.replay_completed" {
		t.Fatalf("solo debía llegar el evento permitido (%d) y replay_completed; llegaron: %+v", s1, got)
	}
	var body struct {
		Count int `json:"count"`
	}
	_ = json.Unmarshal(got[1].Data, &body)
	if body.Count != 1 {
		t.Fatalf("count = %d, esperaba 1", body.Count)
	}
}

func TestResyncWhenPurged(t *testing.T) {
	pool := testPool(t)
	cleanReplayEvents(t, pool)
	ctx := context.Background()

	// Asegurar que exista al menos una fila con seq >= 10
	s1 := insertEventRow(t, pool, i64(1), nil, "sale.created", nil)
	s2 := insertEventRow(t, pool, i64(1), nil, "sale.created", nil)
	repo := eventRepos.NewEventRepository(pool)
	oldest, err := repo.OldestSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if oldest < 2 {
		// Si la tabla empezaba en 1, borramos s1 para que oldest sea >= 2
		_, _ = pool.Exec(ctx, `DELETE FROM realtime_events WHERE seq = $1`, s1)
		oldest, _ = repo.OldestSeq(ctx)
	}

	sock := newCaptureSocket()
	client := hub.NewClient("r3", sock, empAccess(3, 1, 1), 64)
	svc := NewReplayService(repo, 5000)

	// Pedimos lastSeq = oldest - 2 (el evento oldest - 1 ya fue purgado)
	if err := svc.Replay(ctx, client, oldest-2, s2, nil); err != nil {
		t.Fatal(err)
	}

	got := sock.received()
	if len(got) != 1 || got[0].Type != "system.resync_required" {
		t.Fatalf("esperaba system.resync_required, llegó: %+v", got)
	}
	var body struct {
		Reason     string `json:"reason"`
		CurrentSeq int64  `json:"currentSeq"`
	}
	_ = json.Unmarshal(got[0].Data, &body)
	if body.Reason != "gap_too_large" || body.CurrentSeq != s2 {
		t.Fatalf("resync inesperado: %+v", body)
	}
}

func TestResyncWhenTooMany(t *testing.T) {
	pool := testPool(t)
	cleanReplayEvents(t, pool)
	ctx := context.Background()

	sock := newCaptureSocket()
	client := hub.NewClient("r4", sock, empAccess(3, 1, 1), 64)
	svc := NewReplayService(eventRepos.NewEventRepository(pool), 100) // maxReplay = 100

	if err := svc.Replay(ctx, client, 10, 200, nil); err != nil {
		t.Fatal(err)
	}

	got := sock.received()
	if len(got) != 1 || got[0].Type != "system.resync_required" {
		t.Fatalf("esperaba system.resync_required por exceso de eventos, llegó: %+v", got)
	}
}

func TestLiveEventsQueuedDuringReplay(t *testing.T) {
	pool := testPool(t)
	cleanReplayEvents(t, pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := hub.New(5)
	go h.Run(ctx)

	s1 := insertEventRow(t, pool, i64(1), nil, "sale.created", nil)
	s2 := insertEventRow(t, pool, i64(1), nil, "sale.stage_changed", nil)

	sock := newCaptureSocket()
	client := hub.NewClient("r5", sock, empAccess(3, 1, 1), 64)
	h.Register(client)
	h.ReloadPermissions("nadie:0", nil) // sincronizar registro

	// Mientras el cliente está en recuperación (hasta currentSeq = s2), el hub despacha en vivo s2 (duplicado) y s2+1 (nuevo)
	company := int64(1)
	h.Dispatch([]events.StoredEvent{
		{Seq: s2, ID: "dup", Type: "sale.stage_changed", CompanyID: &company, Payload: json.RawMessage(`{}`), OccurredAt: time.Now()},
		{Seq: s2 + 1, ID: "live", Type: "sale.payment_changed", CompanyID: &company, Payload: json.RawMessage(`{}`), OccurredAt: time.Now()},
	})
	h.ReloadPermissions("nadie:0", nil) // asegurar que quedaron encolados en client.q

	svc := NewReplayService(eventRepos.NewEventRepository(pool), 5000)
	if err := svc.Replay(ctx, client, s1-1, s2, nil); err != nil {
		t.Fatal(err)
	}

	// Arranca WriteLoop (paso 8): debe saltar s2 (ya enviado en replay) y entregar s2+1 después de replay_completed
	client.MarkLive()
	go client.WriteLoop(ctx, time.Hour, time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(sock.received()) < 4 {
		time.Sleep(15 * time.Millisecond)
	}
	got := sock.received()
	if len(got) != 4 {
		t.Fatalf("esperaba 4 mensajes (s1, s2, replay_completed, s2+1), llegaron %d: %+v", len(got), got)
	}
	if got[0].Seq != s1 || got[1].Seq != s2 || got[2].Type != "system.replay_completed" || got[3].Seq != s2+1 {
		t.Fatalf("orden incorrecto: [%d (%s), %d (%s), %d (%s), %d (%s)]",
			got[0].Seq, got[0].Type, got[1].Seq, got[1].Type, got[2].Seq, got[2].Type, got[3].Seq, got[3].Type)
	}
}
