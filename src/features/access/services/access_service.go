package services

// access_service.go · paso 3 del saludo (14.2): con la identidad del ticket arma el AccessContext (sesión activa, actor disponible,
// bodegas y permisos); lo vuelve a armar ante permissions.changed (E5)
// conecta con: access/repositories, tickets (Identity) y features/connection; errores → cierre 4401 (sesión) o 4403 (cuenta)
// ojo: la compañía y la bodega salen de la base, no del ticket: si cambiaron desde que se emitió, manda lo actual

import (
	"context"
	"errors"
	"time"

	"importadoraRealtime/src/features/access/dtos"
	"importadoraRealtime/src/features/access/repositories"
	ticketRepos "importadoraRealtime/src/features/tickets/repositories"
)

// BuildTimeout es el límite del paso 3 del saludo.
const BuildTimeout = 2 * time.Second

var (
	ErrSessionInactive  = errors.New("la sesión ya no está activa")
	ErrActorUnavailable = errors.New("la cuenta, su compañía o su bodega no están disponibles")
)

// Repository es lo que el servicio necesita de la base.
type Repository interface {
	IsSessionActive(ctx context.Context, sessionID int64) (bool, error)
	ActorAvailability(ctx context.Context, actorType string, actorID int64) (repositories.Availability, error)
	EmployeePermissions(ctx context.Context, employeeID int64) (map[string]bool, error)
	ActiveWarehouses(ctx context.Context, companyID int64) ([]int64, error)
}

// AccessService arma el AccessContext.
type AccessService struct{ repo Repository }

// NewAccessService crea el servicio.
func NewAccessService(repo Repository) *AccessService { return &AccessService{repo: repo} }

// Build arma el acceso de una conexión nueva.
func (s *AccessService) Build(ctx context.Context, id ticketRepos.Identity) (dtos.AccessContext, error) {
	ctx, cancel := context.WithTimeout(ctx, BuildTimeout)
	defer cancel()

	active, err := s.repo.IsSessionActive(ctx, id.SessionID)
	if err != nil {
		return dtos.AccessContext{}, err
	}
	if !active {
		return dtos.AccessContext{}, ErrSessionInactive
	}
	avail, err := s.repo.ActorAvailability(ctx, id.ActorType, id.ActorID)
	if err != nil {
		return dtos.AccessContext{}, err
	}
	if !avail.Active {
		return dtos.AccessContext{}, ErrActorUnavailable
	}

	ac := dtos.AccessContext{ActorType: id.ActorType, ActorID: id.ActorID, SessionID: id.SessionID, Permissions: map[string]bool{}}
	switch id.ActorType {
	case "superadmin":
		ac.IsSuperadmin = true // decisión 2: sin compañía ni bodegas
	case "admin":
		ac.IsAdmin = true
		ac.CompanyID = avail.CompanyID
		if avail.CompanyID != nil {
			if ac.WarehouseIDs, err = s.repo.ActiveWarehouses(ctx, *avail.CompanyID); err != nil {
				return dtos.AccessContext{}, err
			}
		}
	case "employee":
		ac.CompanyID = avail.CompanyID
		if avail.WarehouseID != nil {
			ac.WarehouseIDs = []int64{*avail.WarehouseID}
		}
		if ac.Permissions, err = s.repo.EmployeePermissions(ctx, id.ActorID); err != nil {
			return dtos.AccessContext{}, err
		}
	default:
		return dtos.AccessContext{}, ErrActorUnavailable
	}
	return ac, nil
}

// ReloadPermissions recarga de la base los permisos de un empleado (ante permissions.changed).
func (s *AccessService) ReloadPermissions(ctx context.Context, actorType string, actorID int64) (map[string]bool, error) {
	if actorType != "employee" {
		return map[string]bool{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, BuildTimeout)
	defer cancel()
	return s.repo.EmployeePermissions(ctx, actorID)
}

// ActiveWarehouses recarga las bodegas activas de una compañía (para recalcular canales de admins).
func (s *AccessService) ActiveWarehouses(ctx context.Context, companyID int64) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, BuildTimeout)
	defer cancel()
	return s.repo.ActiveWarehouses(ctx, companyID)
}
