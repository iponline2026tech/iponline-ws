package services

// purge_service_test · prueba contra PostgreSQL real que PurgeOlderThan borra solo los eventos anteriores a la ventana de retención
// conecta con: purge_service.go y events/repositories/event_repo.go
// ojo: solo corre si TEST_DATABASE_URL está definida (base importadora_test); limpia las filas que inserta

import (
	"context"
	"fmt"
	"testing"
	"time"

	"importadoraRealtime/src/features/events/repositories"
	ticketRepos "importadoraRealtime/src/features/tickets/repositories"
)

func TestPurgeRemovesOnlyOldEvents(t *testing.T) {
	pool := testPool(t)
	cleanRealtimeEvents(t, pool)
	ctx := context.Background()

	var oldSeq, recentSeq int64
	err := pool.QueryRow(ctx, `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload, created_at)
		VALUES (1, '{}', 'sale.created', '{}', 'employee', 3, 'Juan', FALSE, '{}'::jsonb, now() - interval '72 hours')
		RETURNING seq`).Scan(&oldSeq)
	if err != nil {
		t.Fatal(err)
	}
	err = pool.QueryRow(ctx, `
		INSERT INTO realtime_events (company_id, warehouse_ids, type, permissions, actor_type, actor_id, actor_name, platform, payload, created_at)
		VALUES (1, '{}', 'sale.created', '{}', 'employee', 3, 'Juan', FALSE, '{}'::jsonb, now() - interval '1 hour')
		RETURNING seq`).Scan(&recentSeq)
	if err != nil {
		t.Fatal(err)
	}

	svc := NewPurgeService(repositories.NewEventRepository(pool), ticketRepos.NewTicketRepository(pool), 48*time.Hour, time.Hour)
	deleted, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted < 1 {
		t.Fatalf("esperaba borrar al menos 1 fila antigua, borró %d", deleted)
	}

	var countOld, countRecent int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM realtime_events WHERE seq = $1`, oldSeq).Scan(&countOld)
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM realtime_events WHERE seq = $1`, recentSeq).Scan(&countRecent)
	if countOld != 0 || countRecent != 1 {
		t.Fatalf("el evento de 72h debe borrarse (%d) y el de 1h conservarse (%d)", countOld, countRecent)
	}
}

func TestPurgeRemovesOnlyExpiredTickets(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	var session int64
	if err := pool.QueryRow(ctx, `INSERT INTO refresh_tokens (employee_id, token_hash, expires_at) VALUES (3, $1, now() + interval '1 day') RETURNING id`,
		fmt.Sprintf("prueba-purga-%d", time.Now().UnixNano())).Scan(&session); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM refresh_tokens WHERE id = $1", session) })
	insert := func(hash, ttl string) {
		if _, err := pool.Exec(ctx, `INSERT INTO realtime_tickets (ticket_hash, actor_type, actor_id, company_id, warehouse_id, session_id, expires_at)
			VALUES ($1, 'employee', 3, 1, 1, $2, now() + $3::interval)`, hash, session, ttl); err != nil {
			t.Fatal(err)
		}
	}
	old := fmt.Sprintf("purga-vieja-%d", time.Now().UnixNano())
	fresh := fmt.Sprintf("purga-reciente-%d", time.Now().UnixNano())
	insert(old, "-2 hours")
	insert(fresh, "-10 minutes")

	svc := NewPurgeService(repositories.NewEventRepository(pool), ticketRepos.NewTicketRepository(pool), 48*time.Hour, time.Hour)
	if _, err := svc.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var countOld, countFresh int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM realtime_tickets WHERE ticket_hash = $1`, old).Scan(&countOld)
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM realtime_tickets WHERE ticket_hash = $1`, fresh).Scan(&countFresh)
	if countOld != 0 || countFresh != 1 {
		t.Fatalf("el ticket vencido hace 2 h debe borrarse (%d) y el de 10 min conservarse (%d)", countOld, countFresh)
	}
}
