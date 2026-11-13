package services

// replay_service.go · paso 7b/7c del saludo (14.2): recupera eventos desde lastSeq hasta currentSeq o pide system.resync_required
// conecta con: connection_service.go (lo llama al recibir hello con lastSeq), events/repositories/event_repo.go, core/hub y core/protocol
// ojo: escribe directo con SendReplay antes de que arranque WriteLoop; al terminar deja lastSent = currentSeq para no duplicar lo encolado en vivo

import (
	"context"
	"log/slog"
	"time"

	"importadoraRealtime/src/core/contracts"
	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
)

const (
	ReplayTimeout  = 10 * time.Second
	ReplayPageSize = 500
)

// ReplayRepository es lo que la recuperación necesita de realtime_events.
type ReplayRepository interface {
	OldestSeq(ctx context.Context) (int64, error)
	ReadAfterForScope(ctx context.Context, afterSeq, upToSeq int64, scope contracts.ReplayScope, limit int) ([]events.StoredEvent, error)
}

// ReplayService ejecuta la recuperación por secuencia de una conexión que reconecta.
type ReplayService struct {
	repo      ReplayRepository
	maxReplay int
}

// NewReplayService crea el servicio de recuperación.
func NewReplayService(repo ReplayRepository, maxReplay int) *ReplayService {
	if maxReplay <= 0 {
		maxReplay = 5000
	}
	return &ReplayService{repo: repo, maxReplay: maxReplay}
}

// Replay envía los eventos pendientes en (lastSeq, currentSeq] y cierra con system.replay_completed, o envía system.resync_required.
func (s *ReplayService) Replay(ctx context.Context, client *hub.Client, lastSeq, currentSeq int64, log *slog.Logger) error {
	rctx, cancel := context.WithTimeout(ctx, ReplayTimeout)
	defer cancel()

	// Si la secuencia del servidor es menor que la del cliente, se restauró un respaldo mientras estaba desconectado.
	if currentSeq < lastSeq {
		return s.sendResync(rctx, client, "backup_restored", currentSeq, log)
	}

	// Paso 7b: demasiados eventos pendientes
	if currentSeq-lastSeq > int64(s.maxReplay) {
		return s.sendResync(rctx, client, "gap_too_large", currentSeq, log)
	}

	oldest, err := s.repo.OldestSeq(rctx)
	if err != nil {
		return err
	}
	if oldest > 0 && lastSeq < oldest-1 {
		return s.sendResync(rctx, client, "gap_too_large", currentSeq, log)
	}

	// Paso 7c: lectura paginada de 500 en 500 filtrada por alcance y permisos
	access := client.Access()
	scope := contracts.ReplayScope{
		CompanyID:    access.CompanyID,
		ActorKey:     access.ActorKey(),
		IsSuperadmin: access.IsSuperadmin,
	}

	cursor := lastSeq
	sent := 0
	for cursor < currentSeq {
		rows, err := s.repo.ReadAfterForScope(rctx, cursor, currentSeq, scope, ReplayPageSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, e := range rows {
			cursor = e.Seq
			if !hub.Allowed(access, e) {
				continue
			}
			encoded, err := protocol.EventEnvelope(e).Encode()
			if err != nil {
				return err
			}
			if err := client.SendReplay(rctx, e.Seq, encoded); err != nil {
				return err
			}
			sent++
		}
		if len(rows) < ReplayPageSize {
			break
		}
	}

	fromSeq := lastSeq + 1
	if lastSeq == currentSeq {
		fromSeq = currentSeq
	}
	completed, err := protocol.ReplayCompleted(fromSeq, currentSeq, sent)
	if err != nil {
		return err
	}
	if err := client.SendReplay(rctx, currentSeq, completed); err != nil {
		return err
	}
	if log != nil {
		log.Info("recuperación completada", "fromSeq", fromSeq, "toSeq", currentSeq, "count", sent)
	}
	return nil
}

func (s *ReplayService) sendResync(ctx context.Context, client *hub.Client, reason string, currentSeq int64, log *slog.Logger) error {
	msg, err := protocol.ResyncRequired(reason, currentSeq)
	if err != nil {
		return err
	}
	if err := client.SendReplay(ctx, currentSeq, msg); err != nil {
		return err
	}
	if log != nil {
		log.Info("recuperación requiere resync", "reason", reason, "currentSeq", currentSeq)
	}
	return nil
}
