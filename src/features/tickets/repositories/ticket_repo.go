package repositories

// ticket_repo.go · consume y purga tickets de realtime_tickets (los emite la API con POST /realtime/ticket)
// conecta con: tickets/services (lo usa); el usuario importadora_realtime solo puede leer, marcar used_at y borrar (migración 20261129)
// ojo: el consumo es UN solo UPDATE … WHERE used_at IS NULL AND expires_at > now(): dos conexiones con el mismo ticket no pasan las dos

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Identity es quién abre la conexión, según el ticket.
type Identity struct {
	ActorType   string
	ActorID     int64
	CompanyID   *int64
	WarehouseID *int64
	SessionID   int64
}

// Estado del ticket cuando no se pudo consumir.
var (
	ErrTicketNotFound = errors.New("ticket inexistente")
	ErrTicketUsed     = errors.New("ticket ya usado")
	ErrTicketExpired  = errors.New("ticket vencido")
)

// TicketRepository lee y marca tickets.
type TicketRepository struct{ db ticketQuerier }

type ticketQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// NewTicketRepository crea el repositorio.
func NewTicketRepository(db ticketQuerier) *TicketRepository { return &TicketRepository{db: db} }

// Consume marca el ticket como usado y devuelve la identidad; si no se puede, dice por qué.
func (r *TicketRepository) Consume(ctx context.Context, hash string) (Identity, error) {
	var id Identity
	err := r.db.QueryRow(ctx, `
		UPDATE realtime_tickets SET used_at = now()
		WHERE ticket_hash = $1 AND used_at IS NULL AND expires_at > now()
		RETURNING actor_type, actor_id, company_id, warehouse_id, session_id`, hash).
		Scan(&id.ActorType, &id.ActorID, &id.CompanyID, &id.WarehouseID, &id.SessionID)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Identity{}, err
	}
	var usedAt *time.Time
	var expiresAt time.Time
	if err := r.db.QueryRow(ctx, `SELECT used_at, expires_at FROM realtime_tickets WHERE ticket_hash = $1`, hash).Scan(&usedAt, &expiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Identity{}, ErrTicketNotFound
		}
		return Identity{}, err
	}
	if usedAt != nil {
		return Identity{}, ErrTicketUsed
	}
	return Identity{}, ErrTicketExpired
}

// PurgeExpired deletes tickets that expired more than one hour ago.
func (r *TicketRepository) PurgeExpiredTickets(ctx context.Context) (int64, error) {
	tag, err := r.db.Exec(ctx, `DELETE FROM realtime_tickets WHERE expires_at < now() - interval '1 hour'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
