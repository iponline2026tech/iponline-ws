package services

// listener_service.go · escucha LISTEN erp_events (con tick de seguridad cada 2 s), lee lotes visibles en orden de seq, aplica control_events,
// reparte al hub, detecta respaldos restaurados y envía system.watermark periódicamente (sección 14.7)
// conecta con: core/db/listen_conn.go, events/repositories/event_repo.go, control_events.go, core/hub y features/health (ListenerStatus)
// ojo: nunca interpreta huecos de seq como pérdida; el orden real entre transacciones concurrentes lo garantiza tx_id < pg_snapshot_xmin

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"importadoraRealtime/src/core/db"
	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
)

const (
	DefaultBatchSize    = 500
	DefaultPollInterval = 2 * time.Second
	UnhealthyDownAfter  = 30 * time.Second
	UnhealthyLagLimit   = 1000
	UnhealthyLagAfter   = 60 * time.Second
)

// EventReader es lo que el listener necesita del repositorio de eventos.
type EventReader interface {
	ReadVisibleAfter(ctx context.Context, afterSeq int64, limit int) ([]events.StoredEvent, error)
	CurrentVisibleSeq(ctx context.Context) (int64, error)
}

// ListenerOptions configura tiempos y tamaños del listener.
type ListenerOptions struct {
	BatchSize         int
	PollInterval      time.Duration
	WatermarkInterval time.Duration
}

// ListenerService coordina LISTEN, lectura por lotes, eventos de control y watermark.
type ListenerService struct {
	repo    EventReader
	dial    db.DialFunc
	hub     *hub.Hub
	control *ControlHandler
	opts    ListenerOptions

	notifyCh        chan struct{}
	lastProcessed   atomic.Int64
	visibleSeq      atomic.Int64
	disconnectedNs  atomic.Int64 // UnixNano desde que cayó LISTEN (0 = conectado)
	highLagSinceNs  atomic.Int64 // UnixNano desde que lagSeq > 1000 (0 = sin retraso alto)
	lastWatermarked int64
}

// NewListenerService crea el servicio del listener.
func NewListenerService(repo EventReader, dial db.DialFunc, h *hub.Hub, control *ControlHandler, opts ListenerOptions) *ListenerService {
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.WatermarkInterval <= 0 {
		opts.WatermarkInterval = 30 * time.Second
	}
	s := &ListenerService{
		repo:     repo,
		dial:     dial,
		hub:      h,
		control:  control,
		opts:     opts,
		notifyCh: make(chan struct{}, 1),
	}
	s.disconnectedNs.Store(time.Now().UnixNano())
	return s
}

// Init fija lastProcessed = CurrentVisibleSeq() antes de aceptar conexiones (paso 3 del arranque, 14.1).
func (s *ListenerService) Init(ctx context.Context) error {
	seq, err := s.repo.CurrentVisibleSeq(ctx)
	if err != nil {
		return err
	}
	s.lastProcessed.Store(seq)
	s.visibleSeq.Store(seq)
	s.lastWatermarked = seq
	s.hub.SetLastProcessedSeq(seq)
	return nil
}

// Run ejecuta el bucle de escucha y lectura hasta que se cancele ctx.
func (s *ListenerService) Run(ctx context.Context) {
	if s.dial != nil {
		go s.listenLoop(ctx)
	} else {
		s.disconnectedNs.Store(0)
	}

	pollTicker := time.NewTicker(s.opts.PollInterval)
	defer pollTicker.Stop()

	wmTicker := time.NewTicker(s.opts.WatermarkInterval)
	defer wmTicker.Stop()

	s.drain(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notifyCh:
			s.drain(ctx)
		case <-pollTicker.C:
			s.drain(ctx)
		case <-wmTicker.C:
			cur := s.lastProcessed.Load()
			if cur > s.lastWatermarked {
				s.hub.SendWatermark(cur)
				s.lastWatermarked = cur
			}
		}
	}
}

// Trigger despierta una lectura inmediata (útil en pruebas).
func (s *ListenerService) Trigger() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *ListenerService) listenLoop(ctx context.Context) {
	var backoff time.Duration
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := s.dial(ctx)
		if err != nil {
			s.markDisconnected()
			backoff = db.NextBackoff(backoff)
			slog.Warn("no se pudo conectar LISTEN; reintentando", "backoffMs", backoff.Milliseconds(), "error", err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			continue
		}
		if backoff > 0 {
			slog.Info("conexión LISTEN recuperada")
		}
		backoff = 0
		s.disconnectedNs.Store(0)
		s.Trigger() // cubrir notificaciones ocurridas durante la reconexión

		for {
			err := conn.WaitForNotification(ctx)
			if err != nil {
				_ = conn.Close(context.Background())
				if ctx.Err() != nil {
					return
				}
				s.markDisconnected()
				backoff = db.NextBackoff(0)
				slog.Warn("conexión LISTEN interrumpida; reconectando", "backoffMs", backoff.Milliseconds(), "error", err)
				sleepCtx(ctx, backoff)
				break
			}
			s.Trigger()
		}
	}
}

func (s *ListenerService) markDisconnected() {
	s.disconnectedNs.CompareAndSwap(0, time.Now().UnixNano())
}

func (s *ListenerService) drain(ctx context.Context) {
	visible, err := s.repo.CurrentVisibleSeq(ctx)
	if err == nil {
		s.visibleSeq.Store(visible)
		prev := s.lastProcessed.Load()
		if visible < prev {
			slog.Warn("la secuencia visible retrocedió (respaldo restaurado); se pide resync", "fromSeq", prev, "toSeq", visible)
			s.lastProcessed.Store(visible)
			s.lastWatermarked = visible
			s.hub.SetLastProcessedSeq(visible)
			s.hub.BroadcastResync(nil, "backup_restored", visible)
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}
		after := s.lastProcessed.Load()
		rows, err := s.repo.ReadVisibleAfter(ctx, after, s.opts.BatchSize)
		if err != nil {
			slog.Error("error leyendo realtime_events", "afterSeq", after, "error", err)
			break
		}
		if len(rows) == 0 {
			break
		}
		s.dispatchBatch(ctx, rows)
		last := rows[len(rows)-1].Seq
		s.lastProcessed.Store(last)
		s.hub.SetLastProcessedSeq(last)
		if last > s.visibleSeq.Load() {
			s.visibleSeq.Store(last)
		}
		if len(rows) < s.opts.BatchSize {
			break
		}
	}

	s.updateLagStatus()
}

func (s *ListenerService) dispatchBatch(ctx context.Context, rows []events.StoredEvent) {
	if s.control == nil {
		s.hub.Dispatch(rows)
		return
	}
	start := 0
	for i, e := range rows {
		if s.control.Handles(e.Type) {
			if i > start {
				s.hub.Dispatch(rows[start:i])
			}
			s.control.Handle(ctx, e)
			s.hub.Dispatch(rows[i : i+1])
			start = i + 1
		}
	}
	if start < len(rows) {
		s.hub.Dispatch(rows[start:])
	}
}

func (s *ListenerService) updateLagStatus() {
	lag := s.visibleSeq.Load() - s.lastProcessed.Load()
	if lag > UnhealthyLagLimit {
		s.highLagSinceNs.CompareAndSwap(0, time.Now().UnixNano())
	} else {
		s.highLagSinceNs.Store(0)
	}
}

// Healthy cumple health.ListenerStatus (false si lleva más de 30 s sin conexión LISTEN).
func (s *ListenerService) Healthy(now time.Time) bool {
	ns := s.disconnectedNs.Load()
	if ns == 0 {
		return true
	}
	return now.Sub(time.Unix(0, ns)) <= UnhealthyDownAfter
}

// LastProcessedSeq cumple health.ListenerStatus.
func (s *ListenerService) LastProcessedSeq() int64 { return s.lastProcessed.Load() }

// VisibleSeq cumple health.ListenerStatus.
func (s *ListenerService) VisibleSeq() int64 { return s.visibleSeq.Load() }

// LagUnhealthy cumple health.ListenerStatus (retraso > 1000 durante más de 60 s).
func (s *ListenerService) LagUnhealthy(now time.Time) bool {
	if s.visibleSeq.Load()-s.lastProcessed.Load() <= UnhealthyLagLimit {
		return false
	}
	ns := s.highLagSinceNs.Load()
	if ns == 0 {
		return false
	}
	return now.Sub(time.Unix(0, ns)) > UnhealthyLagAfter
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
