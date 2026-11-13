package events

import (
	"encoding/json"
	"time"
)

// StoredEvent is a persisted event used for routing and replay.
type StoredEvent struct {
	Seq           int64
	ID            string
	Type          string
	CompanyID     *int64
	WarehouseIDs  []int64
	ActorChannel  *string
	Platform      bool
	Permissions   []string
	Actor         Actor
	CorrelationID *string
	Payload       json.RawMessage
	OccurredAt    time.Time
}

// Actor identifies who caused an event.
type Actor struct {
	Type string
	ID   *int64
	Name string
}
