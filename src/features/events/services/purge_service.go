package services

// purge_service.go · purga periódica de realtime_events más antiguos que REALTIME_RETENTION_HOURS (48 h por defecto) y de los tickets vencidos
// conecta con: events/repositories/event_repo.go (PurgeOlderThan) y main.go (arranca una vez y luego cada hora)
// ojo: si una conexión vuelve con un lastSeq ya purgado, replay_service responde system.resync_required(gap_too_large)

import (
	"context"
	"log/slog"
	"time"
)

const DefaultPurgeInterval = time.Hour

// EventPurger borra eventos antiguos.
type EventPurger interface {
	PurgeOlderThan(ctx context.Context, age time.Duration) (int64, error)
}

type TicketPurger interface {
	PurgeExpiredTickets(ctx context.Context) (int64, error)
}

// PurgeService ejecuta la limpieza periódica de realtime_events.
type PurgeService struct {
	repo      EventPurger
	tickets   TicketPurger
	retention time.Duration
	interval  time.Duration
}

// NewPurgeService crea el servicio de purga.
func NewPurgeService(repo EventPurger, tickets TicketPurger, retention, interval time.Duration) *PurgeService {
	if retention <= 0 {
		retention = 48 * time.Hour
	}
	if interval <= 0 {
		interval = DefaultPurgeInterval
	}
	return &PurgeService{repo: repo, tickets: tickets, retention: retention, interval: interval}
}

// RunOnce ejecuta una pasada de purga y registra cuántas filas borró.
func (s *PurgeService) RunOnce(ctx context.Context) (int64, error) {
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deleted, err := s.repo.PurgeOlderThan(pctx, s.retention)
	if err != nil {
		slog.Error("falló la purga de realtime_events", "error", err)
		return 0, err
	}
	tickets, err := s.tickets.PurgeExpiredTickets(pctx)
	if err != nil {
		slog.Error("falló la purga de realtime_tickets", "error", err)
		return deleted, err
	}
	slog.Info("purga completada", "deleted", deleted, "ticketsDeleted", tickets)
	return deleted, nil
}

// Run ejecuta una purga al arrancar y luego cada interval hasta que se cancele ctx.
func (s *PurgeService) Run(ctx context.Context) {
	_, _ = s.RunOnce(ctx)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.RunOnce(ctx)
		}
	}
}
