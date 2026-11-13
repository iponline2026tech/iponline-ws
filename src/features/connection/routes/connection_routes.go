package routes

// connection_routes.go · registra GET /ws (el websocket del ERP)
// conecta con: connection_controller; lo llama core/bootstrap/router.go

import (
	"net/http"

	"importadoraRealtime/src/features/connection/controllers"
)

// Register agrega la ruta al mux.
func Register(mux *http.ServeMux, ctrl *controllers.ConnectionController) {
	mux.HandleFunc("GET /ws", ctrl.Connect)
}
