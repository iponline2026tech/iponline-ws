package hub

// hub.go · registro central de conexiones: una goroutine dueña de los índices (por canal, por sesión y por persona) que recibe todo por canales
// conecta con: client.go, channels.go, filter.go, queue.go y metrics.go; lo usan features/connection (Register/Unregister) y
// features/events (Dispatch, CloseSession y recarga de permisos, E5)
// ojo: modelo de la sección 14.5: nadie más toca los mapas, así que no hay mutex en el estado. Cada sobre se serializa UNA vez por evento

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/protocol"

	"github.com/coder/websocket"
)

type set map[*Client]struct{}

type closeReq struct {
	key  any // sessionID (int64) o actorKey (string); nil = todos
	code websocket.StatusCode
	done chan struct{}
}

type hubOp struct {
	fn   func()
	done chan struct{}
}

// Hub reparte eventos a las conexiones.
type Hub struct {
	maxPerActor int

	register   chan *Client
	unregister chan *Client
	dispatch   chan []events.StoredEvent
	closes     chan closeReq
	ops        chan hubOp

	// estado: solo lo toca run()
	byChannel map[string]set
	bySession map[int64]set
	byActor   map[string]set
	channels  map[*Client][]string
	order     uint64

	m metrics
}

// New crea el hub (decisión 6: maxPerActor pestañas por persona).
func New(maxPerActor int) *Hub {
	return &Hub{
		maxPerActor: maxPerActor,
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		dispatch:    make(chan []events.StoredEvent, 64),
		closes:      make(chan closeReq),
		ops:         make(chan hubOp),
		byChannel:   map[string]set{},
		bySession:   map[int64]set{},
		byActor:     map[string]set{},
		channels:    map[*Client][]string{},
	}
}

// Run es la goroutine dueña del estado; termina con el contexto.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-h.register:
			h.add(c)
		case c := <-h.unregister:
			h.remove(c)
		case events := <-h.dispatch:
			h.deliver(events)
		case req := <-h.closes:
			h.closeMatching(req)
			close(req.done)
		case op := <-h.ops:
			op.fn()
			close(op.done)
		}
	}
}

func (h *Hub) do(fn func()) {
	op := hubOp{fn: fn, done: make(chan struct{})}
	h.ops <- op
	<-op.done
}

// SetLastProcessedSeq fija el último seq conocido por el servicio (al arrancar o al reiniciar tras un respaldo).
func (h *Hub) SetLastProcessedSeq(seq int64) { h.m.lastProcessedSeq.Store(seq) }

// Register agrega una conexión (si la persona ya tiene maxPerActor, cierra la más vieja con 4429).
func (h *Hub) Register(c *Client) { h.register <- c }

// Unregister quita una conexión (se puede llamar aunque ya no esté).
func (h *Hub) Unregister(c *Client) { h.unregister <- c }

// Dispatch reparte un lote de eventos ya ordenado por seq.
func (h *Hub) Dispatch(batch []events.StoredEvent) { h.dispatch <- batch }

// CloseSession cierra todas las pestañas de esa sesión (session.revoked, E5).
func (h *Hub) CloseSession(sessionID int64, code websocket.StatusCode) {
	h.closeAndWait(sessionID, code)
}

// CloseActor cierra todas las pestañas de una persona («employee:3»).
func (h *Hub) CloseActor(actorKey string, code websocket.StatusCode) { h.closeAndWait(actorKey, code) }

// CloseActorExcept cierra todas las pestañas de una persona salvo la sesión indicada (cambio de contraseña propio).
func (h *Hub) CloseActorExcept(actorKey string, exceptSessionID int64, code websocket.StatusCode) {
	h.do(func() {
		var targets []*Client
		for c := range h.byActor[actorKey] {
			if c.Access().SessionID != exceptSessionID {
				targets = append(targets, c)
			}
		}
		for _, c := range targets {
			c.Close(code)
			h.remove(c)
		}
	})
}

// ReloadPermissions reemplaza en vivo los permisos de todas las pestañas de una persona (permissions.changed, E5).
func (h *Hub) ReloadPermissions(actorKey string, perms map[string]bool) {
	h.do(func() {
		for c := range h.byActor[actorKey] {
			cp := make(map[string]bool, len(perms))
			for k, v := range perms {
				cp[k] = v
			}
			ac := c.Access()
			ac.Permissions = cp
			c.SetAccess(ac)
		}
	})
}

// UpdateAdminWarehouses recalcula los canales de bodega de los admins conectados de esa compañía (sección 14.3).
func (h *Hub) UpdateAdminWarehouses(companyID int64, warehouseIDs []int64) {
	h.do(func() {
		companyCh := fmt.Sprintf("company:%d", companyID)
		var admins []*Client
		for c := range h.byChannel[companyCh] {
			if c.Access().IsAdmin {
				admins = append(admins, c)
			}
		}
		for _, c := range admins {
			for _, ch := range h.channels[c] {
				removeFrom(h.byChannel, ch, c)
			}
			ac := c.Access()
			ids := make([]int64, len(warehouseIDs))
			copy(ids, warehouseIDs)
			ac.WarehouseIDs = ids
			c.SetAccess(ac)
			chans := ChannelsFor(ac)
			h.channels[c] = chans
			for _, ch := range chans {
				addTo(h.byChannel, ch, c)
			}
		}
	})
}

// BroadcastResync envía system.resync_required a las conexiones de una compañía (o a todas si companyID es nil).
func (h *Hub) BroadcastResync(companyID *int64, reason string, currentSeq int64) {
	data, err := protocol.ResyncRequired(reason, currentSeq)
	if err != nil {
		return
	}
	h.do(func() {
		var targets set
		if companyID != nil {
			targets = h.byChannel[fmt.Sprintf("company:%d", *companyID)]
		} else {
			targets = set{}
			for c := range h.channels {
				targets[c] = struct{}{}
			}
		}
		h.pushSystemTo(targets, data)
	})
}

// SendWatermark envía system.watermark { seq } a todos los clientes en estado live (sección 14.7).
func (h *Hub) SendWatermark(seq int64) {
	data, err := protocol.Watermark(seq)
	if err != nil {
		return
	}
	h.do(func() {
		targets := set{}
		for c := range h.channels {
			if c.IsLive() {
				targets[c] = struct{}{}
			}
		}
		h.pushSystemTo(targets, data)
	})
}

func (h *Hub) pushSystemTo(targets set, data []byte) {
	var overflowed []*Client
	for c := range targets {
		if !c.q.TryPush(message{seq: 0, data: data}) {
			overflowed = append(overflowed, c)
		}
	}
	for _, c := range overflowed {
		if _, still := h.channels[c]; !still {
			continue
		}
		h.m.queueOverflows.Add(1)
		slog.Warn("cola llena: se cierra la conexión", "connId", c.ID)
		c.Close(protocol.CloseQueueOverflow)
		h.remove(c)
	}
}

// CloseAll cierra todas las conexiones (apagado: 1001).
func (h *Hub) CloseAll(code websocket.StatusCode) { h.closeAndWait(nil, code) }

func (h *Hub) closeAndWait(key any, code websocket.StatusCode) {
	req := closeReq{key: key, code: code, done: make(chan struct{})}
	h.closes <- req
	<-req.done
}

func (h *Hub) add(c *Client) {
	a := c.Access()
	actor := a.ActorKey()
	// límite de pestañas: se va la más vieja (decisión 6)
	for len(h.byActor[actor]) >= h.maxPerActor {
		oldest := h.oldest(h.byActor[actor])
		slog.Info("demasiadas pestañas: se cierra la más vieja", "connId", oldest.ID, "actor", actor)
		oldest.Close(protocol.CloseTooManyConnections)
		h.remove(oldest)
	}
	h.order++
	c.order = h.order
	chans := ChannelsFor(a)
	h.channels[c] = chans
	for _, ch := range chans {
		addTo(h.byChannel, ch, c)
	}
	addTo(h.bySession, a.SessionID, c)
	addTo(h.byActor, actor, c)
	h.m.connections.Add(1)
}

func (h *Hub) remove(c *Client) {
	chans, ok := h.channels[c]
	if !ok {
		return
	}
	a := c.Access()
	for _, ch := range chans {
		removeFrom(h.byChannel, ch, c)
	}
	removeFrom(h.bySession, a.SessionID, c)
	removeFrom(h.byActor, a.ActorKey(), c)
	delete(h.channels, c)
	h.m.connections.Add(-1)
}

func (h *Hub) deliver(events []events.StoredEvent) {
	var overflowed []*Client
	for _, e := range events {
		h.m.eventsDispatched.Add(1)
		var data []byte
		targets := set{}
		for _, ch := range eventChannels(e) {
			for c := range h.byChannel[ch] {
				targets[c] = struct{}{}
			}
		}
		for c := range targets {
			if !Allowed(c.Access(), e) {
				continue
			}
			if data == nil {
				encoded, err := protocol.EventEnvelope(e).Encode()
				if err != nil {
					slog.Error("no se pudo serializar el evento", "seq", e.Seq, "type", e.Type, "error", err)
					break
				}
				data = encoded
			}
			if c.q.TryPush(message{seq: e.Seq, data: data}) {
				h.m.eventsDelivered.Add(1)
			} else {
				overflowed = append(overflowed, c)
			}
		}
		if e.Seq > h.m.lastProcessedSeq.Load() {
			h.m.lastProcessedSeq.Store(e.Seq)
		}
	}
	for _, c := range overflowed {
		if _, still := h.channels[c]; !still {
			continue
		}
		h.m.queueOverflows.Add(1)
		slog.Warn("cola llena: se cierra la conexión", "connId", c.ID)
		c.Close(protocol.CloseQueueOverflow)
		h.remove(c)
	}
}

func (h *Hub) closeMatching(req closeReq) {
	var targets []*Client
	switch key := req.key.(type) {
	case int64:
		for c := range h.bySession[key] {
			targets = append(targets, c)
		}
	case string:
		for c := range h.byActor[key] {
			targets = append(targets, c)
		}
	default:
		for c := range h.channels {
			targets = append(targets, c)
		}
	}
	for _, c := range targets {
		c.Close(req.code)
		h.remove(c)
	}
}

func (h *Hub) oldest(s set) *Client {
	list := make([]*Client, 0, len(s))
	for c := range s {
		list = append(list, c)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].order < list[j].order })
	return list[0]
}

func addTo[K comparable](idx map[K]set, key K, c *Client) {
	if idx[key] == nil {
		idx[key] = set{}
	}
	idx[key][c] = struct{}{}
}

func removeFrom[K comparable](idx map[K]set, key K, c *Client) {
	delete(idx[key], c)
	if len(idx[key]) == 0 {
		delete(idx, key)
	}
}
