package hub

// filter.go · ¿esta conexión recibe este evento? Algoritmo 14.4 del plan: primero el canal, después los permisos (basta uno)
// conecta con: hub.go (dispatch) y la recuperación de E5 (aplica la misma regla a lo que relee de la base)
// ojo: la API ya guarda el alcance resuelto: sin warehouse_ids el evento es de toda la compañía; con warehouse_ids, solo de esas bodegas

import (
	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/features/access/dtos"
)

// Allowed decide si la conexión con ese acceso recibe el evento.
func Allowed(a dtos.AccessContext, e events.StoredEvent) bool {
	// 1. canal
	if e.ActorChannel != nil {
		return *e.ActorChannel == a.ActorKey()
	}
	inChannel := e.Platform && a.IsSuperadmin
	if !inChannel && e.CompanyID != nil && a.CompanyID != nil && *e.CompanyID == *a.CompanyID {
		inChannel = len(e.WarehouseIDs) == 0 || intersects(a.WarehouseIDs, e.WarehouseIDs)
	}
	if !inChannel {
		return false
	}
	// 2. permisos (lectura): el admin pasa todos en su compañía y el superadmin solo lee
	if len(e.Permissions) == 0 || a.IsAdmin || a.IsSuperadmin {
		return true
	}
	return a.HasAny(e.Permissions)
}

func intersects(a, b []int64) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}
