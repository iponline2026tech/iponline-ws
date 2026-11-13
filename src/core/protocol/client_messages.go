package protocol

// client_messages.go · los únicos mensajes que acepta el servidor: hello y pong (client-message.schema.json); cualquier otro cierra con 4400
// conecta con: core/hub/client.go (readLoop los lee) y features/connection (espera el hello)

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// MaxClientMessage es el tamaño máximo por defecto de un mensaje del cliente (REALTIME_MAX_MESSAGE_BYTES).
const MaxClientMessage = 4096

// ClientMessage es hello { v, lastSeq } o pong { ts }.
type ClientMessage struct {
	Type    string // hello | pong
	LastSeq *int64 // hello: null en la primera conexión
	TS      int64  // pong
}

// ErrProtocol: el mensaje no cumple el contrato.
var ErrProtocol = errors.New("mensaje del cliente inválido")

// ParseClientMessage valida el mensaje con JSON estricto: campos exactos del contrato, sin extras.
func ParseClientMessage(raw []byte) (ClientMessage, error) {
	if len(raw) > MaxClientMessage {
		return ClientMessage{}, fmt.Errorf("%w: %d bytes (máximo %d)", ErrProtocol, len(raw), MaxClientMessage)
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return ClientMessage{}, fmt.Errorf("%w: no es JSON", ErrProtocol)
	}
	switch head.Type {
	case "hello":
		// ojo: se lee como mapa porque «lastSeq": null y un lastSeq ausente se decodifican igual en un puntero
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return ClientMessage{}, fmt.Errorf("%w: hello no es un objeto", ErrProtocol)
		}
		for k := range fields {
			if k != "type" && k != "v" && k != "lastSeq" {
				return ClientMessage{}, fmt.Errorf("%w: hello con campo desconocido %q", ErrProtocol, k)
			}
		}
		var v int
		if err := json.Unmarshal(fields["v"], &v); err != nil || v != Version {
			return ClientMessage{}, fmt.Errorf("%w: hello con versión distinta de %d", ErrProtocol, Version)
		}
		lastSeq, present := fields["lastSeq"]
		if !present {
			return ClientMessage{}, fmt.Errorf("%w: hello sin lastSeq (usar null en la primera conexión)", ErrProtocol)
		}
		out := ClientMessage{Type: "hello"}
		if string(bytes.TrimSpace(lastSeq)) != "null" {
			var seq int64
			if err := json.Unmarshal(lastSeq, &seq); err != nil || seq < 0 {
				return ClientMessage{}, fmt.Errorf("%w: lastSeq debe ser entero ≥ 0 o null", ErrProtocol)
			}
			out.LastSeq = &seq
		}
		return out, nil
	case "pong":
		var m struct {
			Type string `json:"type"`
			TS   *int64 `json:"ts"`
		}
		if err := strict(raw, &m); err != nil {
			return ClientMessage{}, err
		}
		if m.TS == nil || *m.TS < 0 {
			return ClientMessage{}, fmt.Errorf("%w: pong sin ts", ErrProtocol)
		}
		return ClientMessage{Type: "pong", TS: *m.TS}, nil
	}
	return ClientMessage{}, fmt.Errorf("%w: tipo %q desconocido", ErrProtocol, head.Type)
}

func strict(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	return nil
}
