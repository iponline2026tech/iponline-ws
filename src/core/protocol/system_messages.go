package protocol

// system_messages.go · mensajes que genera el propio servicio: system.welcome, replay_completed, resync_required y watermark
// conecta con: features/connection (welcome y recuperación) y core/hub (watermark y resync); contrato: grupo A del catálogo
// ojo: van con seq 0 (no salen de realtime_events) y actor system

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

func system(eventType string, data any) ([]byte, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return Envelope{V: Version, Seq: 0, ID: newID(), Type: eventType, OccurredAt: time.Now().UTC(), WarehouseIDs: []int64{},
		Actor: SystemActor, Data: body}.Encode()
}

// Welcome: conexión aceptada; currentSeq es el último evento procesado y channels los canales de la conexión.
func Welcome(currentSeq int64, channels []string, heartbeatSec int) ([]byte, error) {
	return system("system.welcome", map[string]any{"currentSeq": currentSeq, "channels": channels, "heartbeatSec": heartbeatSec, "contractVersion": Version})
}

// ReplayCompleted: terminó la recuperación pedida con hello.
func ReplayCompleted(fromSeq, toSeq int64, count int) ([]byte, error) {
	return system("system.replay_completed", map[string]any{"fromSeq": fromSeq, "toSeq": toSeq, "count": count})
}

// ResyncRequired: el cliente debe recargar desde HTTP (hueco grande, contrato nuevo o respaldo restaurado).
func ResyncRequired(reason string, currentSeq int64) ([]byte, error) {
	return system("system.resync_required", map[string]any{"reason": reason, "currentSeq": currentSeq})
}

// Watermark: avance de lo procesado aunque el cliente no haya recibido eventos (por filtro).
func Watermark(seq int64) ([]byte, error) {
	return system("system.watermark", map[string]any{"seq": seq})
}

// newID es un UUID v4.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
