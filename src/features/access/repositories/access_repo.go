package repositories

// access_repo.go · ¿la sesión sigue activa?, ¿el actor, su compañía y su bodega están activos?, ¿qué permisos tiene? y bodegas del admin
// conecta con: access/services (arma el AccessContext); el usuario importadora_realtime tiene SELECT sobre estas tablas (migración 20261129)
// ojo: copia de importadoraApi/src/features/auth/repositories (IsSessionActive en auth_session_repo.go, IsActorAvailable +
// FindEmployeeByID/FindAdminByID/FindSuperadminByID y LoadPermissionsForActor en auth_repo.go) — si cambia allá, cambiar aquí

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Querier es lo que el repositorio necesita de la base (el pool de pgx lo cumple).
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// AccessRepository consulta sesiones, actores y permisos.
type AccessRepository struct{ db Querier }

// NewAccessRepository crea el repositorio.
func NewAccessRepository(db Querier) *AccessRepository { return &AccessRepository{db: db} }

// IsSessionActive: la sesión (refresh_tokens) no está revocada ni vencida. Igual que la API.
func (r *AccessRepository) IsSessionActive(ctx context.Context, sessionID int64) (bool, error) {
	var active bool
	err := r.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM refresh_tokens
			WHERE id = $1
			  AND revoked_at IS NULL
			  AND expires_at > CURRENT_TIMESTAMP
		)`, sessionID).Scan(&active)
	return active, err
}

// Availability es lo que hace falta para decidir si el actor puede conectarse.
type Availability struct {
	Active      bool   // la cuenta está activa (y su compañía, y la bodega del empleado)
	CompanyID   *int64 // compañía actual del actor (nil para el superadmin)
	WarehouseID *int64 // bodega actual del empleado
}

// ActorAvailability aplica la misma regla de IsActorAvailable de la API:
// superadmin: is_active; admin: is_active y su compañía activa; empleado: status y employment_status activos, su compañía activa
// y, si tiene bodega, la bodega activa.
func (r *AccessRepository) ActorAvailability(ctx context.Context, actorType string, actorID int64) (Availability, error) {
	var a Availability
	var err error
	switch actorType {
	case "superadmin":
		err = r.db.QueryRow(ctx, `SELECT is_active FROM superadmins WHERE id = $1`, actorID).Scan(&a.Active)
	case "admin":
		err = r.db.QueryRow(ctx, `
			SELECT a.is_active AND c.is_active, a.company_id
			FROM admins a
			JOIN companies c ON c.id = a.company_id
			WHERE a.id = $1`, actorID).Scan(&a.Active, &a.CompanyID)
	case "employee":
		err = r.db.QueryRow(ctx, `
			SELECT e.status = 'active' AND e.employment_status = 'active' AND c.is_active
			       AND (e.warehouse_id IS NULL OR COALESCE(w.is_active, FALSE)),
			       e.company_id, e.warehouse_id
			FROM employees e
			JOIN companies c ON c.id = e.company_id
			LEFT JOIN warehouses w ON w.id = e.warehouse_id
			WHERE e.id = $1 AND e.deleted_at IS NULL`, actorID).Scan(&a.Active, &a.CompanyID, &a.WarehouseID)
	default:
		return Availability{}, nil
	}
	if err == pgx.ErrNoRows {
		return Availability{}, nil
	}
	return a, err
}

// EmployeePermissions devuelve las llaves «modulo:accion» del empleado (igual que LoadPermissionsForActor para empleados).
func (r *AccessRepository) EmployeePermissions(ctx context.Context, employeeID int64) (map[string]bool, error) {
	rows, err := r.db.Query(ctx, `
		SELECT p.module, p.action
		FROM employee_permissions ep
		JOIN permissions p ON p.id = ep.permission_id
		WHERE ep.employee_id = $1`, employeeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	perms := map[string]bool{}
	for rows.Next() {
		var module, action string
		if err := rows.Scan(&module, &action); err != nil {
			return nil, err
		}
		perms[module+":"+action] = true
	}
	return perms, rows.Err()
}

// ActiveWarehouses son las bodegas activas de la compañía (los canales warehouse:{id} de un admin).
func (r *AccessRepository) ActiveWarehouses(ctx context.Context, companyID int64) ([]int64, error) {
	rows, err := r.db.Query(ctx, `SELECT id FROM warehouses WHERE company_id = $1 AND is_active ORDER BY id`, companyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
