package controllers

// health_controller.go · responde GET /health con el Report en JSON: 200 si está sano, 503 si no
// conecta con: health_service (arma el estado); lo registra health_routes

import (
	"encoding/json"
	"net/http"

	"importadoraRealtime/src/features/health/services"
)

// HealthController atiende /health.
type HealthController struct {
	service *services.HealthService
}

// NewHealthController crea el controlador.
func NewHealthController(service *services.HealthService) *HealthController {
	return &HealthController{service: service}
}

// Get GET /health
func (c *HealthController) Get(w http.ResponseWriter, r *http.Request) {
	report, healthy := c.service.Check(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(report)
}
