package routes

// health_routes.go · registra GET /health (lo usa el healthcheck de Railway)
// conecta con: health_controller; lo llama core/bootstrap/router.go

import (
	"net/http"

	"importadoraRealtime/src/features/health/controllers"
)

// Register agrega la ruta al mux.
func Register(mux *http.ServeMux, ctrl *controllers.HealthController) {
	mux.HandleFunc("GET /health", ctrl.Get)
}
