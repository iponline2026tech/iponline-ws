package services

// control_events.go · aplica sobre el hub los efectos de sesión revocada, cambio de permisos, respaldo restaurado y bodegas de admins
// conecta con: listener_service.go (lo llama antes de repartir cada evento), core/hub y features/access (recarga de permisos y bodegas)
// ojo: el evento se procesa aquí ANTES de repartirse y también sigue hacia hub.Dispatch para que el front lo reciba si sigue conectado

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
)

// AccessReloader recarga permisos de empleados y bodegas activas de compañías desde la base.
type AccessReloader interface {
	ReloadPermissions(ctx context.Context, actorType string, actorID int64) (map[string]bool, error)
	ActiveWarehouses(ctx context.Context, companyID int64) ([]int64, error)
}

// ControlHandler actúa sobre el hub cuando llega un evento de control.
type ControlHandler struct {
	hub      *hub.Hub
	access   AccessReloader
	handlers map[string]func(context.Context, events.StoredEvent)
}

// NewControlHandler crea el manejador de eventos de control.
func NewControlHandler(h *hub.Hub, access AccessReloader) *ControlHandler {
	c := &ControlHandler{hub: h, access: access}
	c.handlers = map[string]func(context.Context, events.StoredEvent){
		"session.revoked":     func(_ context.Context, e events.StoredEvent) { c.handleSessionRevoked(e) },
		"permissions.changed": c.handlePermissionsChanged,
		"backup.restored": func(_ context.Context, e events.StoredEvent) {
			c.hub.BroadcastResync(e.CompanyID, "backup_restored", e.Seq)
		},
		"entity.created":        c.handleWarehouseChange,
		"entity.status_changed": c.handleWarehouseChange,
		"entity.archived":       c.handleWarehouseChange,
		"entity.restored":       c.handleWarehouseChange,
	}
	return c
}

type sessionRevokedPayload struct {
	SessionID       *int64 `json:"sessionId"`
	ExceptSessionID *int64 `json:"exceptSessionId"`
	Reason          string `json:"reason"`
}

type permissionsChangedPayload struct {
	ActorType   string   `json:"actorType"`
	ActorID     int64    `json:"actorId"`
	Permissions []string `json:"permissions"`
}

type entityEnvelopePayload struct {
	Entity struct {
		Kind string `json:"kind"`
	} `json:"entity"`
}

// Handle aplica el efecto de control del evento si corresponde.
func (c *ControlHandler) Handles(eventType string) bool {
	_, ok := c.handlers[eventType]
	return ok
}

func (c *ControlHandler) Handle(ctx context.Context, e events.StoredEvent) {
	if handler := c.handlers[e.Type]; handler != nil {
		handler(ctx, e)
	}
}

func (c *ControlHandler) handleSessionRevoked(e events.StoredEvent) {
	var p sessionRevokedPayload
	if len(e.Payload) > 0 {
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			slog.Warn("no se pudo leer session.revoked", "seq", e.Seq, "error", err)
			return
		}
	}
	if p.SessionID != nil {
		c.hub.CloseSession(*p.SessionID, protocol.CloseSessionRevoked)
		return
	}
	if e.ActorChannel != nil && *e.ActorChannel != "" {
		if p.ExceptSessionID != nil {
			c.hub.CloseActorExcept(*e.ActorChannel, *p.ExceptSessionID, protocol.CloseSessionRevoked)
		} else {
			c.hub.CloseActor(*e.ActorChannel, protocol.CloseSessionRevoked)
		}
	}
}

func (c *ControlHandler) handlePermissionsChanged(ctx context.Context, e events.StoredEvent) {
	var p permissionsChangedPayload
	if len(e.Payload) > 0 {
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			slog.Warn("no se pudo leer permissions.changed", "seq", e.Seq, "error", err)
			return
		}
	}
	actorKey := ""
	if p.ActorType != "" && p.ActorID > 0 {
		actorKey = fmt.Sprintf("%s:%d", p.ActorType, p.ActorID)
	} else if e.ActorChannel != nil {
		actorKey = *e.ActorChannel
	}
	if actorKey == "" {
		return
	}

	var perms map[string]bool
	if c.access != nil && p.ActorType != "" && p.ActorID > 0 {
		loaded, err := c.access.ReloadPermissions(ctx, p.ActorType, p.ActorID)
		if err != nil {
			slog.Warn("no se pudieron recargar permisos desde la BD; se usan los del evento", "actor", actorKey, "error", err)
		} else {
			perms = loaded
		}
	}
	if perms == nil {
		perms = make(map[string]bool, len(p.Permissions))
		for _, k := range p.Permissions {
			perms[k] = true
		}
	}
	c.hub.ReloadPermissions(actorKey, perms)
}

func (c *ControlHandler) handleWarehouseChange(ctx context.Context, e events.StoredEvent) {
	if e.CompanyID == nil || c.access == nil {
		return
	}
	var p entityEnvelopePayload
	if err := json.Unmarshal(e.Payload, &p); err != nil || p.Entity.Kind != "warehouse" {
		return
	}
	warehouses, err := c.access.ActiveWarehouses(ctx, *e.CompanyID)
	if err != nil {
		slog.Warn("no se pudieron recargar las bodegas activas de la compañía", "companyId", *e.CompanyID, "error", err)
		return
	}
	c.hub.UpdateAdminWarehouses(*e.CompanyID, warehouses)
}
