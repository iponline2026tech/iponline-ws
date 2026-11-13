package protocol

// protocol_test · los mensajes del cliente se validan estrictos y los que genera el servicio cumplen el contrato (JSON Schema de contract/)

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"importadoraRealtime/src/core/events"
)

func TestParseClientMessage(t *testing.T) {
	valid := map[string]ClientMessage{
		`{"type":"hello","v":1,"lastSeq":null}`: {Type: "hello"},
		`{"type":"hello","v":1,"lastSeq":42}`:   {Type: "hello", LastSeq: ptr(42)},
		`{"type":"pong","ts":1794495845123}`:    {Type: "pong", TS: 1794495845123},
	}
	for raw, want := range valid {
		got, err := ParseClientMessage([]byte(raw))
		if err != nil || got.Type != want.Type || got.TS != want.TS || (want.LastSeq == nil) != (got.LastSeq == nil) || (want.LastSeq != nil && *got.LastSeq != *want.LastSeq) {
			t.Errorf("%s → %+v, %v", raw, got, err)
		}
	}
	invalid := []string{
		`{"type":"hello","v":2,"lastSeq":null}`,       // otra versión
		`{"type":"hello","v":1}`,                      // sin lastSeq
		`{"type":"hello","v":1,"lastSeq":-1}`,         // negativo
		`{"type":"hello","v":1,"lastSeq":"7"}`,        // texto
		`{"type":"hello","v":1,"lastSeq":null,"x":1}`, // campo extra
		`{"type":"pong"}`,                             // sin ts
		`{"type":"subscribe","channel":"company:2"}`,  // tipo desconocido
		`no es json`,
		`{"type":"pong","ts":1,"pad":"` + strings.Repeat("x", MaxClientMessage) + `"}`, // demasiado grande
	}
	for _, raw := range invalid {
		if _, err := ParseClientMessage([]byte(raw)); err == nil {
			t.Errorf("debía rechazarse: %.60s", raw)
		}
	}
}

func ptr(n int64) *int64 { return &n }

// los mensajes de sistema validan contra envelope.schema.json y el schema de su tipo
func TestSystemMessagesMatchContract(t *testing.T) {
	contract := filepath.Join("..", "..", "..", "contract", "schemas")
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	compile := func(file string) *jsonschema.Schema {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		doc, _ := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		id := doc.(map[string]any)["$id"].(string)
		if err := compiler.AddResource(id, doc); err != nil {
			t.Fatal(err)
		}
		s, err := compiler.Compile(id)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	envelope := compile(filepath.Join(contract, "envelope.schema.json"))
	welcome, _ := Welcome(9000, []string{"company:1", "warehouse:1", "actor:employee:3"}, 25)
	replay, _ := ReplayCompleted(10, 20, 3)
	resync, _ := ResyncRequired("gap_too_large", 9000)
	watermark, _ := Watermark(9001)
	for eventType, msg := range map[string][]byte{"system.welcome": welcome, "system.replay_completed": replay, "system.resync_required": resync, "system.watermark": watermark} {
		body := compile(filepath.Join(contract, "events", eventType+".schema.json"))
		inst, _ := jsonschema.UnmarshalJSON(bytes.NewReader(msg))
		if err := envelope.Validate(inst); err != nil {
			t.Errorf("%s no cumple el sobre: %v", eventType, err)
		}
		if err := body.Validate(inst.(map[string]any)["data"]); err != nil {
			t.Errorf("%s no cumple su cuerpo: %v", eventType, err)
		}
		var env Envelope
		_ = json.Unmarshal(msg, &env)
		if env.Seq != 0 || env.Actor.Type != "system" || env.Type != eventType {
			t.Errorf("%s debe ir con seq 0 y actor system: %+v", eventType, env)
		}
	}
}

func TestStoredEventEnvelope(t *testing.T) {
	company, corr := int64(1), "req-1"
	env := EventEnvelope(events.StoredEvent{Seq: 9, ID: "a", Type: "stock.changed", CompanyID: &company, CorrelationID: &corr,
		Actor: events.Actor{Type: "employee", Name: "Juan"}, Payload: json.RawMessage(`{"x":1}`), OccurredAt: time.Now()})
	if env.V != 1 || env.WarehouseIDs == nil || env.CorrelationID != "req-1" || string(env.Data) != `{"x":1}` {
		t.Fatalf("sobre mal armado: %+v", env)
	}
}
