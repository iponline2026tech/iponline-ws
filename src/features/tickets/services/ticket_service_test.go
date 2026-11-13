package services

// ticket_service_test · contra PostgreSQL real: un ticket se consume UNA vez, el reusado y el vencido se rechazan con su motivo
// conecta con: ticket_service.go y tickets/repositories (Consume); el ticket lo emitiría la API (mismo sha256)
// ojo: solo corre si TEST_DATABASE_URL está definida (una base creada con startdb.pgsql, nunca la de trabajo); limpia lo que crea

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"importadoraRealtime/src/features/tickets/repositories"

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

// newSession crea una sesión del empleado 3 (Juan Felipe Montoya) y la borra al final (los tickets caen en cascada)
func newSession(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	err := pool.QueryRow(ctx, `INSERT INTO refresh_tokens (employee_id, token_hash, expires_at) VALUES (3, $1, now() + interval '1 day') RETURNING id`,
		fmt.Sprintf("prueba-ticket-%d", time.Now().UnixNano())).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM refresh_tokens WHERE id = $1", id) })
	return id
}

func insertTicket(t *testing.T, pool *pgxpool.Pool, ticket string, session int64, ttl string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO realtime_tickets (ticket_hash, actor_type, actor_id, company_id, warehouse_id, session_id, expires_at)
		VALUES ($1, 'employee', 3, 1, 1, $2, now() + $3::interval)`, Hash(ticket), session, ttl)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTicketIsConsumedOnce(t *testing.T) {
	pool := testPool(t)
	session := newSession(t, pool)
	svc := NewTicketService(repositories.NewTicketRepository(pool))
	ticket := fmt.Sprintf("valido-%d", time.Now().UnixNano())
	insertTicket(t, pool, ticket, session, "30 seconds")

	id, err := svc.Redeem(context.Background(), ticket)
	if err != nil || id.ActorType != "employee" || id.ActorID != 3 || id.SessionID != session || id.CompanyID == nil || *id.CompanyID != 1 {
		t.Fatalf("un ticket válido devuelve la identidad: %+v %v", id, err)
	}
	if _, err := svc.Redeem(context.Background(), ticket); !errors.Is(err, repositories.ErrTicketUsed) {
		t.Fatalf("reusarlo debe dar ErrTicketUsed: %v", err)
	}
}

func TestExpiredMissingAndEmptyTickets(t *testing.T) {
	pool := testPool(t)
	session := newSession(t, pool)
	svc := NewTicketService(repositories.NewTicketRepository(pool))
	expired := fmt.Sprintf("vencido-%d", time.Now().UnixNano())
	insertTicket(t, pool, expired, session, "-1 second")
	if _, err := svc.Redeem(context.Background(), expired); !errors.Is(err, repositories.ErrTicketExpired) {
		t.Fatalf("vencido debe dar ErrTicketExpired: %v", err)
	}
	if _, err := svc.Redeem(context.Background(), "no-existe"); !errors.Is(err, repositories.ErrTicketNotFound) {
		t.Fatalf("inexistente debe dar ErrTicketNotFound: %v", err)
	}
	if _, err := svc.Redeem(context.Background(), "  "); !errors.Is(err, ErrTicketMissing) {
		t.Fatalf("vacío debe dar ErrTicketMissing: %v", err)
	}
}

func TestHashMatchesTheAPI(t *testing.T) {
	// mismo valor que importadoraApi/src/features/realtime/services.HashTicket("abc")
	if Hash("abc") != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatal("el hash debe ser sha256 hex, igual que en la API")
	}
}
