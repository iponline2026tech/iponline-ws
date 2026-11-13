package controllers

// connection_controller.go · GET /ws?ticket=…: límite por IP, acepta el upgrade y valida el Origin (4003); el resto lo hace el servicio
// conecta con: connection_service (saludo), core/server (Origins, IPLimiter) y core/hub (WrapConn)
// ojo: el Origin se valida después del upgrade para poder cerrar con 4003 (el front entiende el código y no reintenta);
// el ticket nunca se registra en logs

import (
	"net/http"

	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/core/server"
	"importadoraRealtime/src/features/connection/services"

	"github.com/coder/websocket"
)

// ConnectionController atiende /ws.
type ConnectionController struct {
	service   *services.ConnectionService
	origins   *server.Origins
	limiter   *server.IPLimiter
	readLimit int64
}

// NewConnectionController crea el controlador.
func NewConnectionController(service *services.ConnectionService, origins *server.Origins, limiter *server.IPLimiter, readLimit int64) *ConnectionController {
	return &ConnectionController{service: service, origins: origins, limiter: limiter, readLimit: readLimit}
}

// Connect GET /ws
func (c *ConnectionController) Connect(w http.ResponseWriter, r *http.Request) {
	if !c.limiter.Allow(server.ClientIP(r)) {
		http.Error(w, "demasiados intentos de conexión; espera un minuto", http.StatusTooManyRequests)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // el Origin se revisa abajo con comparación exacta para responder 4003
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		return // Accept ya respondió el error HTTP
	}
	if !c.origins.Allow(r.Header.Get("Origin")) {
		_ = conn.Close(protocol.CloseOriginRejected, protocol.CloseReason[protocol.CloseOriginRejected])
		return
	}
	c.service.Handle(r.Context(), hub.WrapConn(conn, c.readLimit), r.URL.Query().Get("ticket"))
}
