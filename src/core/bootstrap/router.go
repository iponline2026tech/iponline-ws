package bootstrap

// router.go · registra las rutas de cada feature en el mux del servidor
// conecta con: container.go y las rutas de features (health y connection)

import (
	"net/http"

	connectionRoutes "importadoraRealtime/src/features/connection/routes"
	healthRoutes "importadoraRealtime/src/features/health/routes"
)

// RegisterRoutes conecta los controladores con el mux.
func RegisterRoutes(mux *http.ServeMux, c *Container) {
	healthRoutes.Register(mux, c.HealthCtrl)
	connectionRoutes.Register(mux, c.ConnectionCtrl)
}
