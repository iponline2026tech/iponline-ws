package services

// connection_service.go · saludo de una conexión (sección 14.2, pasos 2 a 9): ticket → acceso → canales → hub → welcome → hello → live
// conecta con: features/tickets (Redeem), features/access (Build), core/hub (Register, cola y loops) y core/protocol (mensajes y cierres)
// ojo: desde el registro (paso 5) los eventos del cliente se ENCOLAN pero no salen hasta que llega el hello y termina la recuperación;
// la recuperación con lastSeq (paso 7b/7c) llega en E5: mientras tanto un hello con lastSeq pasa directo a live

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"time"

	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/logging"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/features/access/dtos"
	accessServices "importadoraRealtime/src/features/access/services"
	ticketRepos "importadoraRealtime/src/features/tickets/repositories"
	ticketServices "importadoraRealtime/src/features/tickets/services"
)

// Tickets consume el ticket de la conexión.
type Tickets interface {
	Redeem(ctx context.Context, ticket string) (ticketRepos.Identity, error)
}

// Access arma el acceso de la conexión.
type Access interface {
	Build(ctx context.Context, id ticketRepos.Identity) (dtos.AccessContext, error)
}

// Options son los tiempos y tamaños de la conexión (vienen de la configuración).
type Options struct {
	QueueSize    int
	PingInterval time.Duration
	PongTimeout  time.Duration
	HelloTimeout time.Duration
}

// ConnectionService atiende el saludo y la vida de cada conexión.
type ConnectionService struct {
	tickets    Tickets
	access     Access
	hub        *hub.Hub
	replay     *ReplayService
	opts       Options
	currentSeq func() int64 // último evento procesado (lo da el listener en E5)
}

// NewConnectionService crea el servicio.
func NewConnectionService(tickets Tickets, access Access, h *hub.Hub, opts Options, currentSeq func() int64) *ConnectionService {
	return &ConnectionService{tickets: tickets, access: access, hub: h, opts: opts, currentSeq: currentSeq}
}

// WithReplay conecta el servicio de recuperación para atender hello con lastSeq != nil.
func (s *ConnectionService) WithReplay(replay *ReplayService) *ConnectionService {
	s.replay = replay
	return s
}

// Handle atiende una conexión ya aceptada hasta que se cierra.
func (s *ConnectionService) Handle(ctx context.Context, sock hub.Socket, ticket string) {
	connID := newConnID()
	log := logging.WithConn(slog.Default(), connID)
	started := time.Now()

	// 2. ticket
	identity, err := s.tickets.Redeem(ctx, ticket)
	if err != nil {
		log.Info("conexión rechazada: ticket inválido", "reason", ticketReason(err))
		_ = sock.Close(protocol.CloseTicketInvalid, protocol.CloseReason[protocol.CloseTicketInvalid])
		return
	}
	// 3. acceso
	access, err := s.access.Build(ctx, identity)
	if err != nil {
		code := protocol.CloseGoingAway // error de base: que reintente con espera
		switch {
		case errors.Is(err, accessServices.ErrSessionInactive):
			code = protocol.CloseSessionRevoked
		case errors.Is(err, accessServices.ErrActorUnavailable):
			code = protocol.CloseAccountUnavailable
		default:
			log.Error("no se pudo armar el acceso", "error", err)
		}
		_ = sock.Close(code, protocol.CloseReason[code])
		return
	}

	// 4–5. canales y registro (desde aquí los eventos se encolan)
	client := hub.NewClient(connID, sock, access, s.opts.QueueSize)
	s.hub.Register(client)
	defer s.hub.Unregister(client)
	channels := hub.ChannelsFor(access)
	log = log.With("actor", access.ActorKey(), "channels", channels)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// ojo: corre antes que cancel (los defer van al revés): si se cancela primero, coder/websocket corta sin mandar el código de cierre
	defer client.WaitSocketClosed(5 * time.Second)
	go client.ReadLoop(ctx)

	// 6. welcome
	welcome, _ := protocol.Welcome(s.currentSeq(), channels, int(s.opts.PingInterval/time.Second))
	if err := client.SendNow(ctx, welcome); err != nil {
		client.Close(protocol.CloseGoingAway)
		return
	}

	// 7. hello
	var helloMsg protocol.ClientMessage
	select {
	case helloMsg = <-client.Hello():
	case <-time.After(s.opts.HelloTimeout):
		log.Info("conexión cerrada: no llegó hello a tiempo")
		client.Close(protocol.CloseProtocolError)
		return
	case <-client.Done():
		return
	}

	// 7b–7c. recuperación con lastSeq (system.replay_completed o system.resync_required)
	if helloMsg.LastSeq != nil && s.replay != nil {
		if err := s.replay.Replay(ctx, client, *helloMsg.LastSeq, s.currentSeq(), log); err != nil {
			log.Warn("falló la recuperación", "lastSeq", *helloMsg.LastSeq, "error", err)
			client.Close(protocol.CloseGoingAway)
			return
		}
	}

	// 8–9. live: se vacía lo encolado y se hace ping
	client.MarkLive()
	log.Info("conexión aceptada")
	go client.WriteLoop(ctx, s.opts.PingInterval, s.opts.PongTimeout)
	<-client.Done()
	log.Info("conexión cerrada", "code", int(client.CloseCode()), "durationSec", int(time.Since(started).Seconds()))
}

func ticketReason(err error) string {
	switch {
	case errors.Is(err, ticketServices.ErrTicketMissing):
		return "missing"
	case errors.Is(err, ticketRepos.ErrTicketUsed):
		return "used"
	case errors.Is(err, ticketRepos.ErrTicketExpired):
		return "expired"
	case errors.Is(err, ticketRepos.ErrTicketNotFound):
		return "not_found"
	}
	return "error"
}

func newConnID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
