package protocol

// envelope.go · sobre v1: Envelope es el mensaje de transporte escrito en el socket.
// conecta con: core/events (modelo persistido), core/hub (decide a quién va) y features/connection (lo envía)
// ojo: el cuerpo (payload) viaja sin interpretarse; solo control_events.go decodifica los tipos que el servicio debe actuar

import (
	"encoding/json"
	"time"

	"importadoraRealtime/src/core/events"
)

// Version es la versión del contrato que habla el servicio.
const Version = 1

// Actor es quién hizo el cambio.
type Actor struct {
	Type string `json:"type"` // employee | admin | superadmin | system
	ID   *int64 `json:"id"`
	Name string `json:"name"`
}

// SystemActor firma los mensajes que genera este servicio.
var SystemActor = Actor{Type: "system", Name: "importadoraRealtime"}

// Envelope es el mensaje del servidor al cliente (mismo formato que RealtimeEnvelope del front y envelope.schema.json).
type Envelope struct {
	V             int             `json:"v"`
	Seq           int64           `json:"seq"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	OccurredAt    time.Time       `json:"occurredAt"`
	CompanyID     *int64          `json:"companyId"`
	WarehouseIDs  []int64         `json:"warehouseIds"`
	Actor         Actor           `json:"actor"`
	CorrelationID string          `json:"correlationId,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// EventEnvelope maps a persisted event to the outbound transport envelope.
func EventEnvelope(e events.StoredEvent) Envelope {
	warehouses := e.WarehouseIDs
	if warehouses == nil {
		warehouses = []int64{}
	}
	correlation := ""
	if e.CorrelationID != nil {
		correlation = *e.CorrelationID
	}
	return Envelope{V: Version, Seq: e.Seq, ID: e.ID, Type: e.Type, OccurredAt: e.OccurredAt.UTC(), CompanyID: e.CompanyID,
		WarehouseIDs: warehouses, Actor: Actor{Type: e.Actor.Type, ID: e.Actor.ID, Name: e.Actor.Name}, CorrelationID: correlation, Data: e.Payload}
}

// Encode serializa el sobre (el hub lo hace UNA vez por evento y reparte los mismos bytes).
func (e Envelope) Encode() ([]byte, error) { return json.Marshal(e) }
