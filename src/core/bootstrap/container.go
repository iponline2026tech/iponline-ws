package bootstrap

// container.go · arma las dependencias del servicio (repositorios, servicios, hub, listener, purga y controladores) y las conecta
// conecta con: core/config, core/db, core/hub, core/server y las features; lo usa router.go y lo crea main.go (mismo patrón que la API)
// ojo: E5 completa el contenedor con el listener de eventos, la recuperación por lastSeq, los eventos de control y la purga periódica

import (
	"importadoraRealtime/src/core/config"
	"importadoraRealtime/src/core/db"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/server"
	accessRepos "importadoraRealtime/src/features/access/repositories"
	accessServices "importadoraRealtime/src/features/access/services"
	connectionControllers "importadoraRealtime/src/features/connection/controllers"
	connectionServices "importadoraRealtime/src/features/connection/services"
	eventRepos "importadoraRealtime/src/features/events/repositories"
	eventServices "importadoraRealtime/src/features/events/services"
	healthControllers "importadoraRealtime/src/features/health/controllers"
	healthServices "importadoraRealtime/src/features/health/services"
	ticketRepos "importadoraRealtime/src/features/tickets/repositories"
	ticketServices "importadoraRealtime/src/features/tickets/services"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Container tiene todo lo que necesitan las rutas y main.go.
type Container struct {
	Cfg            *config.Config
	Pool           *pgxpool.Pool
	Hub            *hub.Hub
	Listener       *eventServices.ListenerService
	Purge          *eventServices.PurgeService
	HealthCtrl     *healthControllers.HealthController
	ConnectionCtrl *connectionControllers.ConnectionController
}

// NewContainer crea el contenedor.
func NewContainer(cfg *config.Config, pool *pgxpool.Pool) *Container {
	h := hub.New(cfg.MaxConnPerUser)

	ticketRepository := ticketRepos.NewTicketRepository(pool)
	tickets := ticketServices.NewTicketService(ticketRepository)
	access := accessServices.NewAccessService(accessRepos.NewAccessRepository(pool))
	eventsRepo := eventRepos.NewEventRepository(pool)

	control := eventServices.NewControlHandler(h, access)
	listener := eventServices.NewListenerService(eventsRepo, db.NewListenDialer(db.DSN(cfg)), h, control, eventServices.ListenerOptions{
		BatchSize:         eventServices.DefaultBatchSize,
		PollInterval:      eventServices.DefaultPollInterval,
		WatermarkInterval: cfg.Watermark,
	})
	purge := eventServices.NewPurgeService(eventsRepo, ticketRepository, cfg.Retention, eventServices.DefaultPurgeInterval)
	replay := connectionServices.NewReplayService(eventsRepo, cfg.MaxReplay)

	connection := connectionServices.NewConnectionService(tickets, access, h, connectionServices.Options{
		QueueSize:    cfg.MaxClientQueue,
		PingInterval: cfg.PingInterval,
		PongTimeout:  cfg.PongTimeout,
		HelloTimeout: cfg.HelloTimeout,
	}, listener.LastProcessedSeq).WithReplay(replay)

	health := healthServices.NewHealthService(pool, listener, h)
	return &Container{
		Cfg:            cfg,
		Pool:           pool,
		Hub:            h,
		Listener:       listener,
		Purge:          purge,
		HealthCtrl:     healthControllers.NewHealthController(health),
		ConnectionCtrl: connectionControllers.NewConnectionController(connection, server.NewOrigins(cfg.AllowedOrigins), server.NewIPLimiter(server.ConnectAttemptsPerMinute), cfg.MaxMessageSize),
	}
}
