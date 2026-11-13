package hub

// metrics.go · contadores del hub con atomic (los lee /health sin pasar por la goroutine del hub)
// conecta con: hub.go (los actualiza) y features/health (HubStats)

import "sync/atomic"

type metrics struct {
	connections      atomic.Int64
	eventsDispatched atomic.Int64
	eventsDelivered  atomic.Int64
	queueOverflows   atomic.Int64
	lastProcessedSeq atomic.Int64
}

// Connections son las conexiones registradas ahora.
func (h *Hub) Connections() int { return int(h.m.connections.Load()) }

// QueueOverflows son las conexiones cerradas por cola llena desde el arranque.
func (h *Hub) QueueOverflows() int64 { return h.m.queueOverflows.Load() }

// EventsDelivered son los mensajes encolados a clientes desde el arranque.
func (h *Hub) EventsDelivered() int64 { return h.m.eventsDelivered.Load() }

// LastProcessedSeq es el último seq repartido (lo fija el listener en E5).
func (h *Hub) LastProcessedSeq() int64 { return h.m.lastProcessedSeq.Load() }
