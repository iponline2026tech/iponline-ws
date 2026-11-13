package server

// limits.go · límite de intentos de conexión por IP (20 por minuto) para que nadie inunde el saludo; el de pestañas por persona lo aplica el hub
// conecta con: features/connection (lo consulta antes de aceptar el upgrade; si se pasa responde 429)

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ConnectAttemptsPerMinute es el máximo de intentos de conexión por IP en un minuto.
const ConnectAttemptsPerMinute = 20

// IPLimiter cuenta intentos por IP en una ventana deslizante de un minuto.
type IPLimiter struct {
	mu    sync.Mutex
	limit int
	hits  map[string][]time.Time
	now   func() time.Time
}

// NewIPLimiter crea el limitador.
func NewIPLimiter(limit int) *IPLimiter {
	return &IPLimiter{limit: limit, hits: map[string][]time.Time{}, now: time.Now}
}

// Allow registra un intento de esa IP y dice si todavía está dentro del límite.
func (l *IPLimiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	window := now.Add(-time.Minute)
	recent := l.hits[ip][:0]
	for _, t := range l.hits[ip] {
		if t.After(window) {
			recent = append(recent, t)
		}
	}
	if len(recent) >= l.limit {
		l.hits[ip] = recent
		return false
	}
	l.hits[ip] = append(recent, now)
	if len(l.hits) > 10000 { // limpieza simple para no crecer sin fin
		for k, v := range l.hits {
			if len(v) == 0 || !v[len(v)-1].After(window) {
				delete(l.hits, k)
			}
		}
	}
	return true
}

// ClientIP es la IP del cliente: la primera de X-Forwarded-For (Railway y los proxies la ponen) o la de la conexión.
func ClientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return strings.TrimSpace(strings.Split(fwd, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
