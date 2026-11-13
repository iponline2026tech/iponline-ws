package dtos

// access_context.go · lo que el hub necesita saber de cada conexión: quién es, de qué compañía y bodegas, su sesión y sus permisos
// conecta con: access/services (lo arma), core/hub (canales y filtro) y features/connection

import "strconv"

// AccessContext es la identidad efectiva de una conexión.
type AccessContext struct {
	ActorType    string // employee | admin | superadmin
	ActorID      int64
	CompanyID    *int64  // nil para el superadmin (decisión 2)
	WarehouseIDs []int64 // empleado: su bodega; admin: todas las activas de su compañía; superadmin: ninguna
	SessionID    int64
	Permissions  map[string]bool // «modulo:accion» (solo cuenta para empleados)
	IsAdmin      bool
	IsSuperadmin bool
}

// ActorKey identifica a la persona («employee:3»): agrupa sus pestañas y es su canal personal.
func (a AccessContext) ActorKey() string { return a.ActorType + ":" + strconv.FormatInt(a.ActorID, 10) }

// HasAny dice si tiene al menos uno de los permisos.
func (a AccessContext) HasAny(perms []string) bool {
	for _, p := range perms {
		if a.Permissions[p] {
			return true
		}
	}
	return false
}
