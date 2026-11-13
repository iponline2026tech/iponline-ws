package hub

// queue.go · cola acotada de salida de cada conexión (REALTIME_MAX_CLIENT_QUEUE); si se llena el cliente es lento y se cierra con 4408
// conecta con: client.go (writeLoop la vacía) y hub.go (dispatch la llena con TryPush, nunca bloquea)

// message es un sobre ya serializado y su seq (0 en los mensajes de sistema), para no reenviar lo que ya salió.
type message struct {
	seq  int64
	data []byte
}

type queue struct{ ch chan message }

func newQueue(size int) *queue { return &queue{ch: make(chan message, size)} }

// TryPush encola sin esperar; false si la cola está llena.
func (q *queue) TryPush(m message) bool {
	select {
	case q.ch <- m:
		return true
	default:
		return false
	}
}
