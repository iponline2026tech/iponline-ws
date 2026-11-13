package services

// control_events_test · pruebas de session.revoked, permissions.changed, backup.restored y recálculo de bodegas de admins
// conecta con: control_events.go, listener_service.go y core/hub
// ojo: verifica que permissions.changed haga efecto ANTES de repartir el siguiente evento del mismo lote

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/core/hub"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/features/access/dtos"
)

type fakeAccessReloader struct {
	perms      map[int64]map[string]bool
	warehouses map[int64][]int64
}

func TestControlHandlerRegistryIsAuthoritative(t *testing.T) {
	h := NewControlHandler(nil, nil)
	for _, eventType := range []string{
		"session.revoked", "permissions.changed", "backup.restored",
		"entity.created", "entity.status_changed", "entity.archived", "entity.restored",
	} {
		if !h.Handles(eventType) {
			t.Errorf("%s debe estar registrado como evento de control", eventType)
		}
	}
	if h.Handles("sale.created") {
		t.Fatal("sale.created no debe tratarse como evento de control")
	}
}

func (f *fakeAccessReloader) ReloadPermissions(_ context.Context, _ string, actorID int64) (map[string]bool, error) {
	return f.perms[actorID], nil
}

func (f *fakeAccessReloader) ActiveWarehouses(_ context.Context, companyID int64) ([]int64, error) {
	return f.warehouses[companyID], nil
}

func TestSessionRevokedClosesTargetSession(t *testing.T) {
	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c1, _ := startLiveClient(t, h, employeeAccess(3, 1, 1, 100), 16)
	c2, _ := startLiveClient(t, h, employeeAccess(3, 1, 1, 101), 16)

	ctrl := NewControlHandler(h, nil)
	actorCh := "employee:3"
	ctrl.Handle(ctx, events.StoredEvent{
		Seq:          10,
		Type:         "session.revoked",
		ActorChannel: &actorCh,
		Payload:      json.RawMessage(`{"sessionId":100,"exceptSessionId":null,"reason":"logout"}`),
	})

	select {
	case <-c1.Done():
		if c1.CloseCode() != protocol.CloseSessionRevoked {
			t.Fatalf("c1 debía cerrar con 4401, cerró con %d", c1.CloseCode())
		}
	case <-time.After(time.Second):
		t.Fatal("c1 no se cerró")
	}

	select {
	case <-c2.Done():
		t.Fatal("c2 (sesión 101) no debía cerrarse")
	default:
	}

	// Revocar todas las sesiones del actor salvo la 101 (ej. PUT /auth/me/password)
	c3, _ := startLiveClient(t, h, employeeAccess(3, 1, 1, 102), 16)
	ctrl.Handle(ctx, events.StoredEvent{
		Seq:          11,
		Type:         "session.revoked",
		ActorChannel: &actorCh,
		Payload:      json.RawMessage(`{"sessionId":null,"exceptSessionId":101,"reason":"password_changed"}`),
	})
	select {
	case <-c3.Done():
		if c3.CloseCode() != protocol.CloseSessionRevoked {
			t.Fatalf("c3 debía cerrar con 4401, cerró con %d", c3.CloseCode())
		}
	case <-time.After(time.Second):
		t.Fatal("c3 no se cerró")
	}
	select {
	case <-c2.Done():
		t.Fatal("c2 (exceptSessionId=101) debía seguir viva")
	default:
	}
}

func TestPermissionsChangedTakesEffectBeforeNextEvent(t *testing.T) {
	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	// El empleado 3 empieza con transfers:view y catalog:view
	_, sock := startLiveClient(t, h, employeeAccess(3, 1, 1, 10, "transfers:view", "catalog:view"), 32)

	reloader := &fakeAccessReloader{
		perms: map[int64]map[string]bool{
			3: {"catalog:view": true}, // ya NO tiene transfers:view
		},
	}
	ctrl := NewControlHandler(h, reloader)
	listener := NewListenerService(&fakeResetReader{visible: 0}, nil, h, ctrl, ListenerOptions{
		BatchSize:         500,
		PollInterval:      time.Hour,
		WatermarkInterval: time.Hour,
	})

	company := int64(1)
	actorCh := "employee:3"
	batch := []events.StoredEvent{
		{
			Seq:          1,
			ID:           "ev-perm",
			Type:         "permissions.changed",
			ActorChannel: &actorCh,
			Payload:      json.RawMessage(`{"actorType":"employee","actorId":3,"permissions":["catalog:view"]}`),
			OccurredAt:   time.Now(),
		},
		{
			Seq:          2,
			ID:           "ev-transfer",
			Type:         "transfer.created",
			CompanyID:    &company,
			WarehouseIDs: []int64{1},
			Permissions:  []string{"transfers:view"},
			Payload:      json.RawMessage(`{}`),
			OccurredAt:   time.Now(),
		},
		{
			Seq:         3,
			ID:          "ev-catalog",
			Type:        "product.prices_changed",
			CompanyID:   &company,
			Permissions: []string{"catalog:view"},
			Payload:     json.RawMessage(`{}`),
			OccurredAt:  time.Now(),
		},
	}

	listener.dispatchBatch(ctx, batch)

	got := waitForCount(t, sock, 2, 2*time.Second)
	if len(got) != 2 || got[0].Type != "permissions.changed" || got[1].Type != "product.prices_changed" {
		t.Fatalf("debía recibir permissions.changed y product.prices_changed (saltando transfer.created sin permiso), recibió: %+v", got)
	}
}

func TestWarehouseChangeUpdatesAdminChannels(t *testing.T) {
	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	company := int64(1)
	adm := dtos.AccessContext{
		ActorType:    "admin",
		ActorID:      1,
		CompanyID:    &company,
		WarehouseIDs: []int64{1}, // empieza solo con la bodega 1
		SessionID:    50,
		IsAdmin:      true,
		Permissions:  map[string]bool{},
	}
	_, sock := startLiveClient(t, h, adm, 16)

	reloader := &fakeAccessReloader{
		warehouses: map[int64][]int64{
			1: {1, 2}, // se acaba de crear la bodega 2
		},
	}
	ctrl := NewControlHandler(h, reloader)
	ctrl.Handle(ctx, events.StoredEvent{
		Seq:       10,
		Type:      "entity.created",
		CompanyID: &company,
		Payload:   json.RawMessage(`{"entity":{"kind":"warehouse","id":2}}`),
	})

	// Ahora un evento exclusivo de la bodega 2 debe llegarle al admin
	h.Dispatch([]events.StoredEvent{{
		Seq:          11,
		ID:           "ev-w2",
		Type:         "transfer.created",
		CompanyID:    &company,
		WarehouseIDs: []int64{2},
		Permissions:  []string{"transfers:view"},
		Payload:      json.RawMessage(`{}`),
		OccurredAt:   time.Now(),
	}})

	got := waitForCount(t, sock, 1, 2*time.Second)
	if got[0].Seq != 11 {
		t.Fatalf("el admin debía recibir el evento de la nueva bodega 2; llegó %+v", got[0])
	}
}
