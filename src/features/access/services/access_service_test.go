package services

// access_service_test · contra PostgreSQL real: el acceso de empleado (su bodega y sus permisos), admin (todas las bodegas de su compañía)
// y superadmin (sin compañía); una sesión revocada o una cuenta inactiva se rechazan
// ojo: solo corre si TEST_DATABASE_URL está definida (una base creada con startdb.pgsql); limpia lo que crea

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"importadoraRealtime/src/features/access/repositories"
	ticketRepos "importadoraRealtime/src/features/tickets/repositories"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL no está definida")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func session(t *testing.T, pool *pgxpool.Pool, column string, actorID int64) int64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	err := pool.QueryRow(ctx, `INSERT INTO refresh_tokens (`+column+`, token_hash, expires_at) VALUES ($1, $2, now() + interval '1 day') RETURNING id`,
		actorID, fmt.Sprintf("prueba-acceso-%d", time.Now().UnixNano())).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM refresh_tokens WHERE id = $1", id) })
	return id
}

func TestBuildAccessFromDatabase(t *testing.T) {
	pool := testPool(t)
	svc := NewAccessService(repositories.NewAccessRepository(pool))
	ctx := context.Background()

	// empleado 3: Juan Felipe Montoya, vendedor de Paseo Bolívar (bodega 1)
	emp, err := svc.Build(ctx, ticketRepos.Identity{ActorType: "employee", ActorID: 3, SessionID: session(t, pool, "employee_id", 3)})
	if err != nil || *emp.CompanyID != 1 || !reflect.DeepEqual(emp.WarehouseIDs, []int64{1}) || !emp.Permissions["catalog:view"] || emp.IsAdmin {
		t.Fatalf("empleado: %+v %v", emp, err)
	}

	// admin 1: todas las bodegas activas de la compañía 1
	adm, err := svc.Build(ctx, ticketRepos.Identity{ActorType: "admin", ActorID: 1, SessionID: session(t, pool, "admin_id", 1)})
	if err != nil || !adm.IsAdmin || *adm.CompanyID != 1 || !reflect.DeepEqual(adm.WarehouseIDs, []int64{1, 2, 3}) {
		t.Fatalf("admin: %+v %v", adm, err)
	}

	// superadmin: sin compañía ni bodegas (decisión 2)
	sa, err := svc.Build(ctx, ticketRepos.Identity{ActorType: "superadmin", ActorID: 1, SessionID: session(t, pool, "superadmin_id", 1)})
	if err != nil || !sa.IsSuperadmin || sa.CompanyID != nil || len(sa.WarehouseIDs) != 0 {
		t.Fatalf("superadmin: %+v %v", sa, err)
	}
}

func TestRevokedSessionAndInactiveAccount(t *testing.T) {
	pool := testPool(t)
	svc := NewAccessService(repositories.NewAccessRepository(pool))
	ctx := context.Background()

	revoked := session(t, pool, "employee_id", 3)
	_, _ = pool.Exec(ctx, "UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1", revoked)
	if _, err := svc.Build(ctx, ticketRepos.Identity{ActorType: "employee", ActorID: 3, SessionID: revoked}); !errors.Is(err, ErrSessionInactive) {
		t.Fatalf("sesión revocada → ErrSessionInactive: %v", err)
	}

	// empleado 4 inactivo durante la prueba
	sid := session(t, pool, "employee_id", 4)
	_, _ = pool.Exec(ctx, "UPDATE employees SET status = 'inactive' WHERE id = 4")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "UPDATE employees SET status = 'active' WHERE id = 4") })
	if _, err := svc.Build(ctx, ticketRepos.Identity{ActorType: "employee", ActorID: 4, SessionID: sid}); !errors.Is(err, ErrActorUnavailable) {
		t.Fatalf("cuenta inactiva → ErrActorUnavailable: %v", err)
	}
}
