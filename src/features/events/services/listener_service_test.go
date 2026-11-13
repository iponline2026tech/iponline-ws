package services

// listener_service_test · pruebas del listener contra PostgreSQL real: entrega en orden, commits fuera de orden (17.3),
// watermark, retroceso por respaldo restaurado, caída y reconexión de LISTEN, y lectura por lotes de 500
// conecta con: listener_service.go, events/repositories/event_repo.go, core/db/listen_conn.go y core/hub
// ojo: solo corre si TEST_DATABASE_URL está definida (base importadora_test); limpia las filas que inserta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"importadoraRealtime/src/core/db"
	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/features/access/dtos"
	"importadoraRealtime/src/features/events/repositories"

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

// cleanRealtimeEvents serializa entre paquetes con un candado de PostgreSQL y borra al final lo insertado por la prueba.
func cleanRealtimeEvents(t *testing.T, pool *pgxpool.Pool) {
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
}

func insertCompanyEvent(t *testing.T, pool *pgxpool.Pool, companyID int64, eventType string) int64 {
	t.Helper()
	ctx := context.Background()
	var seq int64
	err := pool.QueryRow(ctx, `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload)
		VALUES ($1, '{}', $2, '{}', 'employee', 3, 'Juan', FALSE, '{}'::jsonb)
		RETURNING seq`, companyID, eventType).Scan(&seq)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `SELECT pg_notify('erp_events', $1)`, fmt.Sprint(seq))
	return seq
}

type memSocket struct {
	mu     sync.Mutex
	msgs   []protocol.Envelope
	closed chan websocket.StatusCode
}

func newMemSocket() *memSocket {
	return &memSocket{closed: make(chan websocket.StatusCode, 1)}
}

func (m *memSocket) Read(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
func (m *memSocket) Write(_ context.Context, data []byte) error {
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err == nil {
		m.mu.Lock()
		m.msgs = append(m.msgs, env)
		m.mu.Unlock()
	}
	return nil
}
func (m *memSocket) Ping(context.Context) error { return nil }
func (m *memSocket) Close(code websocket.StatusCode, _ string) error {
	select {
	case m.closed <- code:
	default:
	}
	return nil
}

func (m *memSocket) received() []protocol.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]protocol.Envelope, len(m.msgs))
	copy(out, m.msgs)
	return out
}

func employeeAccess(id, company, warehouse, session int64, perms ...string) dtos.AccessContext {
	p := map[string]bool{}
	for _, k := range perms {
		p[k] = true
	}
	return dtos.AccessContext{
		ActorType:    "employee",
		ActorID:      id,
		CompanyID:    &company,
		WarehouseIDs: []int64{warehouse},
		SessionID:    session,
		Permissions:  p,
	}
}

func startLiveClient(t *testing.T, h *hub.Hub, ac dtos.AccessContext, queueSize int) (*hub.Client, *memSocket) {
	t.Helper()
	sock := newMemSocket()
	c := hub.NewClient(fmt.Sprintf("c-%d", time.Now().UnixNano()), sock, ac, queueSize)
	c.MarkLive()
	h.Register(c)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c.WriteLoop(ctx, time.Hour, time.Second)
	// sincronizar con la goroutine del hub haciendo una operación vacía
	h.ReloadPermissions("nadie:0", nil)
	return c, sock
}

func waitForCount(t *testing.T, sock *memSocket, want int, timeout time.Duration) []protocol.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := sock.received()
		if len(got) >= want {
			return got
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("esperaba %d mensajes y llegaron %d", want, len(sock.received()))
	return nil
}

func TestDeliversInOrder(t *testing.T) {
	pool := testPool(t)
	cleanRealtimeEvents(t, pool)

	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	repo := repositories.NewEventRepository(pool)
	listener := NewListenerService(repo, db.NewListenDialer(os.Getenv("TEST_DATABASE_URL")), h, NewControlHandler(h, nil), ListenerOptions{
		BatchSize:         500,
		PollInterval:      50 * time.Millisecond,
		WatermarkInterval: time.Hour,
	})
	if err := listener.Init(ctx); err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10), 256)

	var expected []int64
	for i := 0; i < 100; i++ {
		expected = append(expected, insertCompanyEvent(t, pool, 1, "sale.created"))
	}

	got := waitForCount(t, sock, 100, 5*time.Second)
	for i := 0; i < 100; i++ {
		if got[i].Seq != expected[i] {
			t.Fatalf("posición %d: llegó seq %d, esperaba %d", i, got[i].Seq, expected[i])
		}
	}
}

// TestOutOfOrderCommit es la prueba obligatoria 17.3:
// Tx A toma seq 10 sin confirmar; Tx B toma seq 11 y confirma → el 11 NO se entrega mientras A siga abierta;
// al confirmar A, se entregan el 10 y el 11 en ese orden.
func TestOutOfOrderCommit(t *testing.T) {
	pool := testPool(t)
	cleanRealtimeEvents(t, pool)

	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	repo := repositories.NewEventRepository(pool)
	listener := NewListenerService(repo, db.NewListenDialer(os.Getenv("TEST_DATABASE_URL")), h, NewControlHandler(h, nil), ListenerOptions{
		BatchSize:         500,
		PollInterval:      30 * time.Millisecond,
		WatermarkInterval: time.Hour,
	})
	if err := listener.Init(ctx); err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10), 64)

	// 1. Transacción A inserta sin confirmar
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txA.Rollback(ctx) }()

	var seqA int64
	err = txA.QueryRow(ctx, `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload)
		VALUES (1, '{}', 'sale.created', '{}', 'employee', 3, 'Juan', FALSE, '{"tx":"A"}'::jsonb)
		RETURNING seq`).Scan(&seqA)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Transacción B inserta y confirma
	seqB := insertCompanyEvent(t, pool, 1, "sale.stage_changed")
	if seqB <= seqA {
		t.Fatalf("seqB (%d) debe ser mayor que seqA (%d)", seqB, seqA)
	}

	// 3. Dar tiempo a varios ciclos del listener y comprobar que B NO se entregó
	listener.Trigger()
	time.Sleep(150 * time.Millisecond)
	if got := sock.received(); len(got) != 0 {
		t.Fatalf("mientras Tx A esté abierta no debe entregarse el evento B; llegaron: %+v", got)
	}

	// 4. Confirmar Tx A
	if _, err := txA.Exec(ctx, `SELECT pg_notify('erp_events', $1)`, fmt.Sprint(seqA)); err != nil {
		t.Fatal(err)
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// 5. Deben llegar A y luego B en ese orden
	got := waitForCount(t, sock, 2, 3*time.Second)
	if got[0].Seq != seqA || got[1].Seq != seqB {
		t.Fatalf("orden incorrecto tras confirmar A: llegaron [%d, %d], esperaba [%d, %d]", got[0].Seq, got[1].Seq, seqA, seqB)
	}
}

func TestWatermarkSent(t *testing.T) {
	pool := testPool(t)
	cleanRealtimeEvents(t, pool)

	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	repo := repositories.NewEventRepository(pool)
	listener := NewListenerService(repo, nil, h, nil, ListenerOptions{
		BatchSize:         500,
		PollInterval:      25 * time.Millisecond,
		WatermarkInterval: 60 * time.Millisecond,
	})
	if err := listener.Init(ctx); err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	// Cliente en bodega 1; insertamos un evento exclusivo de la bodega 3 (el cliente no recibe el evento, pero sí el watermark)
	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10), 64)
	var seqOther int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload)
		VALUES (1, '{3}', 'transfer.created', '{}', 'employee', 3, 'Juan', FALSE, '{}'::jsonb)
		RETURNING seq`).Scan(&seqOther); err != nil {
		t.Fatal(err)
	}

	got := waitForCount(t, sock, 1, 2*time.Second)
	if got[0].Type != "system.watermark" || got[0].Seq != 0 {
		t.Fatalf("esperaba system.watermark con seq 0, llegó: %+v", got[0])
	}
	var wm struct {
		Seq int64 `json:"seq"`
	}
	_ = json.Unmarshal(got[0].Data, &wm)
	if wm.Seq < seqOther {
		t.Fatalf("watermark seq = %d, esperaba >= %d", wm.Seq, seqOther)
	}
}

type fakeResetReader struct {
	mu      sync.Mutex
	visible int64
}

func (f *fakeResetReader) ReadVisibleAfter(context.Context, int64, int) ([]events.StoredEvent, error) {
	return nil, nil
}
func (f *fakeResetReader) CurrentVisibleSeq(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.visible, nil
}

func TestBackupRestoreResets(t *testing.T) {
	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	reader := &fakeResetReader{visible: 500}
	listener := NewListenerService(reader, nil, h, nil, ListenerOptions{
		BatchSize:         500,
		PollInterval:      time.Hour,
		WatermarkInterval: time.Hour,
	})
	if err := listener.Init(ctx); err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10), 16)

	// La tabla vuelve atrás (ej. pg_restore dejó visibleSeq = 120 < 500)
	reader.mu.Lock()
	reader.visible = 120
	reader.mu.Unlock()
	listener.Trigger()

	got := waitForCount(t, sock, 1, 2*time.Second)
	if got[0].Type != "system.resync_required" {
		t.Fatalf("esperaba system.resync_required, llegó: %+v", got[0])
	}
	var body struct {
		Reason     string `json:"reason"`
		CurrentSeq int64  `json:"currentSeq"`
	}
	_ = json.Unmarshal(got[0].Data, &body)
	if body.Reason != "backup_restored" || body.CurrentSeq != 120 || listener.LastProcessedSeq() != 120 {
		t.Fatalf("resync inesperado: %+v lastProcessed=%d", body, listener.LastProcessedSeq())
	}
}

type droppableListenConn struct {
	inner db.ListenConn
	drop  chan struct{}
}

func (d *droppableListenConn) WaitForNotification(ctx context.Context) error {
	done := make(chan error, 1)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		done <- d.inner.WaitForNotification(wctx)
	}()
	select {
	case <-d.drop:
		cancel()
		_ = d.inner.Close(context.Background())
		return fmt.Errorf("conexión LISTEN cortada en prueba")
	case err := <-done:
		return err
	}
}

func (d *droppableListenConn) Close(ctx context.Context) error {
	return d.inner.Close(ctx)
}

func TestRecoversAfterListenDrop(t *testing.T) {
	pool := testPool(t)
	cleanRealtimeEvents(t, pool)

	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	baseDial := db.NewListenDialer(os.Getenv("TEST_DATABASE_URL"))
	dropFirst := make(chan struct{})
	var dials int
	var mu sync.Mutex
	dial := func(ctx context.Context) (db.ListenConn, error) {
		c, err := baseDial(ctx)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		dials++
		n := dials
		mu.Unlock()
		if n == 1 {
			return &droppableListenConn{inner: c, drop: dropFirst}, nil
		}
		return c, nil
	}

	repo := repositories.NewEventRepository(pool)
	listener := NewListenerService(repo, dial, h, nil, ListenerOptions{
		BatchSize:         500,
		PollInterval:      time.Hour, // desactivar poll frecuente para probar que la reconexión de LISTEN recupera
		WatermarkInterval: time.Hour,
	})
	if err := listener.Init(ctx); err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10), 64)
	time.Sleep(80 * time.Millisecond)

	// Cortar la primera conexión LISTEN e insertar eventos mientras reconecta
	close(dropFirst)
	seq1 := insertCompanyEvent(t, pool, 1, "sale.created")
	seq2 := insertCompanyEvent(t, pool, 1, "sale.stage_changed")

	got := waitForCount(t, sock, 2, 3*time.Second)
	if got[0].Seq != seq1 || got[1].Seq != seq2 {
		t.Fatalf("tras reconectar LISTEN deben llegar [%d, %d], llegaron [%d, %d]", seq1, seq2, got[0].Seq, got[1].Seq)
	}
	if !listener.Healthy(time.Now()) {
		t.Fatal("el listener debe quedar sano tras reconectar")
	}
}

func TestBatchRead(t *testing.T) {
	pool := testPool(t)
	cleanRealtimeEvents(t, pool)

	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	repo := repositories.NewEventRepository(pool)
	listener := NewListenerService(repo, nil, h, nil, ListenerOptions{
		BatchSize:         500,
		PollInterval:      time.Hour,
		WatermarkInterval: time.Hour,
	})
	if err := listener.Init(ctx); err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10), 2048)

	// Insertar 1000 eventos en una sola sentencia y despertar al listener una vez
	_, err := pool.Exec(ctx, `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload)
		SELECT 1, '{}', 'sale.created', '{}', 'employee', 3, 'Juan', FALSE, '{}'::jsonb
		FROM generate_series(1, 1000)`)
	if err != nil {
		t.Fatal(err)
	}
	listener.Trigger()

	got := waitForCount(t, sock, 1000, 5*time.Second)
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("fuera de orden en %d: %d <= %d", i, got[i].Seq, got[i-1].Seq)
		}
	}
}
