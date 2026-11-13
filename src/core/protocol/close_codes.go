package protocol

// close_codes.go · códigos de cierre del websocket (tabla 14.6 del plan); son los mismos de REALTIME_CLOSE_CODES del front
// conecta con: core/hub y features/connection (cierran con estos) y contract/REALTIME-CONTRATO.md (sección 7)

import "github.com/coder/websocket"

const (
	CloseNormal             websocket.StatusCode = 1000 // el cliente cerró (logout)
	CloseGoingAway          websocket.StatusCode = 1001 // apagado del servicio: reconectar con espera
	ClosePingTimeout        websocket.StatusCode = 1011 // no respondió el ping: reconectar
	CloseTicketInvalid      websocket.StatusCode = 4001 // ticket usado, vencido o inexistente: pedir otro
	CloseOriginRejected     websocket.StatusCode = 4003 // Origin no permitido: no reconectar
	CloseProtocolError      websocket.StatusCode = 4400 // mensaje inválido, demasiado grande o sin hello a tiempo
	CloseSessionRevoked     websocket.StatusCode = 4401 // sesión cerrada o revocada: cerrar sesión en el front
	CloseAccountUnavailable websocket.StatusCode = 4403 // actor, compañía o bodega inactivos
	CloseQueueOverflow      websocket.StatusCode = 4408 // cliente lento: reconectar con lastSeq
	CloseTooManyConnections websocket.StatusCode = 4429 // más pestañas que REALTIME_MAX_CONN_PER_ACTOR: no reconectar en esa pestaña
)

// CloseReason es el texto corto que acompaña cada código (máximo 123 bytes en el protocolo).
var CloseReason = map[websocket.StatusCode]string{
	CloseNormal: "normal", CloseGoingAway: "going_away", ClosePingTimeout: "ping_timeout", CloseTicketInvalid: "ticket_invalid",
	CloseOriginRejected: "origin_rejected", CloseProtocolError: "protocol_error", CloseSessionRevoked: "session_revoked",
	CloseAccountUnavailable: "account_unavailable", CloseQueueOverflow: "queue_overflow", CloseTooManyConnections: "too_many_connections",
}
