package hub

// client.go · una conexión: su acceso, sus canales, su cola de salida y dos goroutines (lectura: hello/pong; escritura: cola + ping)
// conecta con: hub.go (la registra y le encola mensajes), features/connection (la crea y espera su hello) y core/protocol
// ojo: el hub nunca escribe en el socket: solo encola; el writeLoop es el único que escribe. El ping es el del protocolo websocket
// (el navegador responde solo); sin respuesta en REALTIME_PONG_TIMEOUT_SEC → cierre 1011

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/features/access/dtos"

	"github.com/coder/websocket"
)

// WriteTimeout es el máximo para escribir un mensaje en el socket.
const WriteTimeout = 10 * time.Second

// Socket es lo que el cliente necesita del websocket (lo cumple wsSocket; las pruebas usan uno falso).
type Socket interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, data []byte) error
	Ping(ctx context.Context) error
	Close(code websocket.StatusCode, reason string) error
}

// Client es una conexión registrada (o por registrar) en el hub.
type Client struct {
	ID     string
	access atomic.Pointer[dtos.AccessContext]
	sock   Socket
	q      *queue
	order  uint64 // orden de registro: la más vieja se cierra primero si sobran pestañas

	hello     chan protocol.ClientMessage
	closed    chan struct{}
	sockDone  chan struct{} // se cierra cuando terminó el cierre del socket (con su código)
	closeOnce sync.Once
	closeCode atomic.Int32
	live      atomic.Bool
	lastSent  int64 // se actualiza en SendReplay (antes de WriteLoop) y luego solo lo toca WriteLoop
}

// NewClient crea el cliente de una conexión aceptada.
func NewClient(id string, sock Socket, access dtos.AccessContext, queueSize int) *Client {
	c := &Client{ID: id, sock: sock, q: newQueue(queueSize), hello: make(chan protocol.ClientMessage, 1), closed: make(chan struct{}), sockDone: make(chan struct{})}
	c.access.Store(&access)
	return c
}

// Access es el acceso vigente (se reemplaza entero ante permissions.changed).
func (c *Client) Access() dtos.AccessContext { return *c.access.Load() }

// SetAccess reemplaza el acceso (lo usa el hub al recargar permisos).
func (c *Client) SetAccess(a dtos.AccessContext) { c.access.Store(&a) }

// Hello entrega el primer hello del cliente.
func (c *Client) Hello() <-chan protocol.ClientMessage { return c.hello }

// Done se cierra cuando la conexión terminó.
func (c *Client) Done() <-chan struct{} { return c.closed }

// CloseCode es el código con que se cerró (0 si sigue abierta).
func (c *Client) CloseCode() websocket.StatusCode { return websocket.StatusCode(c.closeCode.Load()) }

// Close cierra la conexión una sola vez con el código del contrato.
func (c *Client) Close(code websocket.StatusCode) {
	c.closeOnce.Do(func() {
		c.closeCode.Store(int32(code))
		close(c.closed)
		// ojo: el cierre del socket espera la respuesta del otro lado (hasta 5 s); va aparte para no trabar al hub
		go func() {
			_ = c.sock.Close(code, protocol.CloseReason[code])
			close(c.sockDone)
		}()
	})
}

// WaitSocketClosed espera a que el cierre del socket termine (hasta max), para que el cliente reciba el código antes de cortar.
// ojo: si la conexión no se cerró por Close (por ejemplo se cortó la red), no hay nada que esperar
func (c *Client) WaitSocketClosed(max time.Duration) {
	select {
	case <-c.closed:
	default:
		return
	}
	select {
	case <-c.sockDone:
	case <-time.After(max):
	}
}

// SendNow escribe directo, antes de que arranque writeLoop (solo para system.welcome en el saludo).
func (c *Client) SendNow(ctx context.Context, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()
	return c.sock.Write(ctx, data)
}

// SendReplay escribe un mensaje durante la recuperación (antes de WriteLoop) y avanza lastSent si seq > 0.
func (c *Client) SendReplay(ctx context.Context, seq int64, data []byte) error {
	if err := c.SendNow(ctx, data); err != nil {
		return err
	}
	if seq > c.lastSent {
		c.lastSent = seq
	}
	return nil
}

// MarkLive marca la conexión como en vivo (terminó el saludo y la recuperación).
func (c *Client) MarkLive() { c.live.Store(true) }

// IsLive indica si la conexión ya está en estado live.
func (c *Client) IsLive() bool { return c.live.Load() }

// ReadLoop lee hasta que la conexión termine: el primer hello va a Hello(); pong es latido; cualquier otra cosa cierra con 4400.
func (c *Client) ReadLoop(ctx context.Context) {
	gotHello := false
	for {
		data, err := c.sock.Read(ctx)
		if err != nil {
			if errors.Is(err, errNotText) {
				c.Close(protocol.CloseProtocolError)
			} else if websocket.CloseStatus(err) == -1 {
				c.Close(protocol.CloseGoingAway) // se cortó sin cierre limpio
			} else {
				c.Close(protocol.CloseNormal)
			}
			return
		}
		msg, err := protocol.ParseClientMessage(data)
		if err != nil || (msg.Type == "hello" && gotHello) {
			c.Close(protocol.CloseProtocolError)
			return
		}
		if msg.Type == "hello" {
			gotHello = true
			c.hello <- msg
		}
	}
}

// WriteLoop vacía la cola en orden (saltando lo que ya salió por seq) y hace ping cada intervalo.
func (c *Client) WriteLoop(ctx context.Context, pingEvery, pongTimeout time.Duration) {
	ticker := time.NewTicker(pingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ctx.Done():
			return
		case m := <-c.q.ch:
			if m.seq > 0 && m.seq <= c.lastSent {
				continue // ya se envió en la recuperación
			}
			wctx, cancel := context.WithTimeout(ctx, WriteTimeout)
			err := c.sock.Write(wctx, m.data)
			cancel()
			if err != nil {
				c.Close(protocol.CloseGoingAway)
				return
			}
			if m.seq > c.lastSent {
				c.lastSent = m.seq
			}
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, pongTimeout)
			err := c.sock.Ping(pctx)
			cancel()
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					c.Close(protocol.ClosePingTimeout)
				}
				return
			}
		}
	}
}

// errNotText: el cliente mandó un mensaje binario (el contrato solo admite texto JSON).
var errNotText = errors.New("solo se aceptan mensajes de texto")

// wsSocket adapta *websocket.Conn a Socket.
type wsSocket struct{ conn *websocket.Conn }

// WrapConn adapta una conexión de coder/websocket con su límite de lectura.
func WrapConn(conn *websocket.Conn, readLimit int64) Socket {
	conn.SetReadLimit(readLimit)
	return &wsSocket{conn: conn}
}

func (s *wsSocket) Read(ctx context.Context) ([]byte, error) {
	typ, data, err := s.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, errNotText
	}
	return data, nil
}

func (s *wsSocket) Write(ctx context.Context, data []byte) error {
	return s.conn.Write(ctx, websocket.MessageText, data)
}

func (s *wsSocket) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

func (s *wsSocket) Close(code websocket.StatusCode, reason string) error {
	return s.conn.Close(code, reason)
}
