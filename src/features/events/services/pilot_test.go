package services

// pilot_test · prueba piloto E8 de entrega de eventos a los 4 actores del escenario 12:
// A (vendedor bodega 7), B (bodeguero bodega 7), C (bodeguero bodega 9) y D (admin)
// Verifica: traslados (origen y destino reciben, tercera bodega no), stock (permisos de catálogo/stock),
// cambio de estado de producto y revocación de sesión al desactivar bodega.

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

func TestPilotScenarioFourClients(t *testing.T) {
	h := hub.New(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	companyID := int64(3)

	// A: vendedor bodega 7
	accA := dtos.AccessContext{
		ActorType:    "employee",
		ActorID:      101,
		CompanyID:    &companyID,
		WarehouseIDs: []int64{7},
		SessionID:    1,
		Permissions:  map[string]bool{"catalog:view": true, "orders:view": true, "orders:create": true},
	}
	_, sockA := startLiveClient(t, h, accA, 16)

	// B: bodeguero bodega 7
	accB := dtos.AccessContext{
		ActorType:    "employee",
		ActorID:      102,
		CompanyID:    &companyID,
		WarehouseIDs: []int64{7},
		SessionID:    2,
		Permissions:  map[string]bool{"transfers:view": true, "stock:view": true},
	}
	_, sockB := startLiveClient(t, h, accB, 16)

	// C: bodeguero bodega 9
	accC := dtos.AccessContext{
		ActorType:    "employee",
		ActorID:      103,
		CompanyID:    &companyID,
		WarehouseIDs: []int64{9},
		SessionID:    3,
		Permissions:  map[string]bool{"transfers:view": true, "stock:view": true},
	}
	cliC, sockC := startLiveClient(t, h, accC, 16)

	// D: admin de la compañía
	accD := dtos.AccessContext{
		ActorType:    "admin",
		ActorID:      104,
		CompanyID:    &companyID,
		WarehouseIDs: []int64{7, 9, 12},
		SessionID:    4,
		IsAdmin:      true,
		Permissions:  map[string]bool{},
	}
	_, sockD := startLiveClient(t, h, accD, 16)

	// E: bodeguero de una tercera bodega ajena (bodega 12)
	accE := dtos.AccessContext{
		ActorType:    "employee",
		ActorID:      105,
		CompanyID:    &companyID,
		WarehouseIDs: []int64{12},
		SessionID:    5,
		Permissions:  map[string]bool{"transfers:view": true, "stock:view": true},
	}
	_, sockE := startLiveClient(t, h, accE, 16)

	// 1. TRASLADO: TR-42 creado entre bodega 7 (origen) y bodega 9 (destino)
	h.Dispatch([]events.StoredEvent{{
		Seq:          1,
		ID:           "tr-created-1",
		Type:         "transfer.created",
		CompanyID:    &companyID,
		WarehouseIDs: []int64{7, 9},
		Permissions:  []string{"transfers:view"},
		Payload:      json.RawMessage(`{"transfer":{"id":42,"serial":"TR-000042","status":"pending","originWarehouseId":7,"destinationWarehouseId":9}}`),
		OccurredAt:   time.Now(),
	}})

	// B (origen 7), C (destino 9) y D (admin) deben recibirlo
	gotB := waitForCount(t, sockB, 1, 2*time.Second)
	if gotB[0].Type != "transfer.created" {
		t.Fatalf("B debía recibir transfer.created; llegó %s", gotB[0].Type)
	}

	gotC := waitForCount(t, sockC, 1, 2*time.Second)
	if gotC[0].Type != "transfer.created" {
		t.Fatalf("C debía recibir transfer.created; llegó %s", gotC[0].Type)
	}

	gotD := waitForCount(t, sockD, 1, 2*time.Second)
	if gotD[0].Type != "transfer.created" {
		t.Fatalf("D debía recibir transfer.created; llegó %s", gotD[0].Type)
	}

	// E (bodega 12) NO debe recibir el traslado entre 7 y 9
	time.Sleep(100 * time.Millisecond)
	if len(sockE.received()) != 0 {
		t.Fatalf("E (bodega 12) no debía recibir transfer.created, recibió: %+v", sockE.received())
	}

	// 2. STOCK CHANGED: emitido al canal company con permisos catalog:view | stock:view | transfers:view
	h.Dispatch([]events.StoredEvent{{
		Seq:         2,
		ID:          "stk-changed-1",
		Type:        "stock.changed",
		CompanyID:   &companyID,
		Permissions: []string{"catalog:view", "stock:view", "transfers:view"},
		Payload:     json.RawMessage(`{"cause":{"type":"reservation"},"rows":[],"totals":[{"productId":88,"warehouseId":7,"sellable":15}]}`),
		OccurredAt:  time.Now(),
	}})

	// A (catalog:view), B (transfers:view/stock:view), C y D deben recibirlo
	gotA := waitForCount(t, sockA, 1, 2*time.Second)
	if gotA[0].Type != "stock.changed" {
		t.Fatalf("A debía recibir stock.changed; llegó %s", gotA[0].Type)
	}
	_ = waitForCount(t, sockB, 1, 2*time.Second)
	_ = waitForCount(t, sockC, 1, 2*time.Second)
	_ = waitForCount(t, sockD, 1, 2*time.Second)

	// 3. PRODUCT STATUS CHANGED (desactivar producto 88)
	h.Dispatch([]events.StoredEvent{{
		Seq:         3,
		ID:          "prd-status-1",
		Type:        "entity.status_changed",
		CompanyID:   &companyID,
		Permissions: []string{"catalog:view"},
		Payload:     json.RawMessage(`{"entity":{"kind":"product","id":88,"version":"2026-11-12T10:00:00Z"},"from":"active","to":"inactive","reason":"agotado"}`),
		OccurredAt:  time.Now(),
	}})

	// A (catalog:view) y D (admin) deben recibirlo
	// Para A es su segundo mensaje (tras stock.changed)
	gotA2 := waitForCount(t, sockA, 2, 2*time.Second)
	if gotA2[1].Type != "entity.status_changed" {
		t.Fatalf("A debía recibir entity.status_changed en msgs[1]; llegó %s", gotA2[1].Type)
	}
	// Para D es su tercer mensaje (transfer.created, stock.changed, entity.status_changed)
	gotD3 := waitForCount(t, sockD, 3, 2*time.Second)
	if gotD3[2].Type != "entity.status_changed" {
		t.Fatalf("D debía recibir entity.status_changed en msgs[2]; llegó %s", gotD3[2].Type)
	}

	// B y C NO tienen catalog:view, solo stock:view/transfers:view, así que no reciben entity.status_changed
	// (B recibió previamente transfer.created y stock.changed = 2 eventos)
	time.Sleep(100 * time.Millisecond)
	if len(sockB.received()) != 2 {
		t.Fatalf("B no debía recibir entity.status_changed sin catalog:view, tiene %d msgs", len(sockB.received()))
	}

	// 4. CONTROL: Desactivar bodega 9 revoca la sesión de C
	ctrl := NewControlHandler(h, nil)
	actorC := "employee:103"
	ctrl.Handle(ctx, events.StoredEvent{
		Seq:          4,
		Type:         "session.revoked",
		ActorChannel: &actorC,
		Payload:      json.RawMessage(`{"sessionId":3,"exceptSessionId":null,"reason":"warehouse_inactive"}`),
	})

	select {
	case <-cliC.Done():
		if cliC.CloseCode() != protocol.CloseSessionRevoked {
			t.Fatalf("C debía cerrar con 4401, cerró con %d", cliC.CloseCode())
		}
	case <-time.After(time.Second):
		t.Fatal("C no se cerró tras desactivar su bodega")
	}
}
