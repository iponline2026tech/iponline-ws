package services

// ticket_service.go · paso 2 del saludo (14.2): sha256 del ticket recibido → consumirlo → identidad, o un error que termina en cierre 4001
// conecta con: tickets/repositories (Consume) y features/connection (lo llama al aceptar el socket)
// ojo: el hash es el mismo de la API (importadoraApi/src/features/realtime/services.HashTicket); el ticket en claro nunca se registra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"importadoraRealtime/src/features/tickets/repositories"
)

// ConsumeTimeout es el límite del paso 2 del saludo.
const ConsumeTimeout = 2 * time.Second

// ErrTicketMissing: la conexión llegó sin ticket.
var ErrTicketMissing = errors.New("falta el ticket")

// Store es lo que el servicio necesita del repositorio.
type Store interface {
	Consume(ctx context.Context, hash string) (repositories.Identity, error)
}

// TicketService consume tickets.
type TicketService struct{ store Store }

// NewTicketService crea el servicio.
func NewTicketService(store Store) *TicketService { return &TicketService{store: store} }

// Redeem consume el ticket y devuelve quién es.
func (s *TicketService) Redeem(ctx context.Context, ticket string) (repositories.Identity, error) {
	ticket = strings.TrimSpace(ticket)
	if ticket == "" {
		return repositories.Identity{}, ErrTicketMissing
	}
	ctx, cancel := context.WithTimeout(ctx, ConsumeTimeout)
	defer cancel()
	return s.store.Consume(ctx, Hash(ticket))
}

// Hash es el sha256 en hex del ticket.
func Hash(ticket string) string {
	sum := sha256.Sum256([]byte(ticket))
	return hex.EncodeToString(sum[:])
}
