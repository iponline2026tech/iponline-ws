package services

// health_service.go · estado del servicio para GET /health: base, listener, conexiones y retraso (sección 14.9 del plan)
// conecta con: core/db (Ping), core/hub y features/events (cuando existan: E4 y E5 los conectan por las interfaces de abajo)
// ojo: 503 si la base falla, si el listener lleva más de 30 s caído o si el retraso pasa de 1000 eventos durante más de 60 s

import (
	"context"
	"time"
)

// Pinger es la base de datos (el pool de pgx cumple esta interfaz).
type Pinger interface {
	Ping(ctx context.Context) error
}

// ListenerStatus lo da el listener de eventos (E5).
type ListenerStatus interface {
	Healthy(now time.Time) bool // false si lleva más de 30 s sin conexión LISTEN
	LastProcessedSeq() int64
	VisibleSeq() int64
	LagUnhealthy(now time.Time) bool // retraso de más de 1000 eventos por más de 60 s
}

// HubStats lo da el hub de conexiones (E4).
type HubStats interface {
	Connections() int
	QueueOverflows() int64
}

// Report es la respuesta de /health.
type Report struct {
	Status           string `json:"status"`
	DB               string `json:"db"`
	Listener         string `json:"listener"`
	Connections      int    `json:"connections"`
	LastProcessedSeq *int64 `json:"lastProcessedSeq"`
	VisibleSeq       *int64 `json:"visibleSeq"`
	LagSeq           *int64 `json:"lagSeq"`
	QueueOverflows   int64  `json:"queueOverflows"`
	ContractVersion  int    `json:"contractVersion"`
	UptimeSec        int64  `json:"uptimeSec"`
}

// ContractVersion es la versión del contrato que habla el servicio.
const ContractVersion = 1

// HealthService arma el Report.
type HealthService struct {
	db       Pinger
	listener ListenerStatus
	hub      HubStats
	started  time.Time
	now      func() time.Time
}

// NewHealthService crea el servicio; listener y hub pueden ser nil mientras no existan.
func NewHealthService(db Pinger, listener ListenerStatus, hub HubStats) *HealthService {
	return &HealthService{db: db, listener: listener, hub: hub, started: time.Now(), now: time.Now}
}

// Check devuelve el estado y si está sano (false → 503).
func (s *HealthService) Check(ctx context.Context) (Report, bool) {
	now := s.now()
	r := Report{Status: "ok", DB: "ok", Listener: "n/a", ContractVersion: ContractVersion, UptimeSec: int64(now.Sub(s.started).Seconds())}
	healthy := true

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if s.db == nil || s.db.Ping(pingCtx) != nil {
		r.DB, healthy = "down", false
	}
	if s.listener != nil {
		r.Listener = "ok"
		last, visible := s.listener.LastProcessedSeq(), s.listener.VisibleSeq()
		lag := visible - last
		r.LastProcessedSeq, r.VisibleSeq, r.LagSeq = &last, &visible, &lag
		if !s.listener.Healthy(now) {
			r.Listener, healthy = "down", false
		}
		if s.listener.LagUnhealthy(now) {
			healthy = false
		}
	}
	if s.hub != nil {
		r.Connections = s.hub.Connections()
		r.QueueOverflows = s.hub.QueueOverflows()
	}
	if !healthy {
		r.Status = "degraded"
	}
	return r, healthy
}
