package server

// origin.go · ¿este Origin puede abrir el websocket? Comparación exacta contra REALTIME_ALLOWED_ORIGINS (esquema + host + puerto)
// conecta con: core/config (lista) y features/connection (lo consulta antes de aceptar el upgrade; si no coincide, cierre 4003)
// ojo: «*» solo llega aquí en desarrollo (validators.go lo rechaza en otro entorno)

import (
	"net/url"
	"strings"
)

// Origins es la lista de orígenes permitidos ya normalizada.
type Origins struct {
	any     bool
	allowed map[string]bool
}

// NewOrigins normaliza la lista de la configuración.
func NewOrigins(list []string) *Origins {
	o := &Origins{allowed: map[string]bool{}}
	for _, item := range list {
		if item == "*" {
			o.any = true
			continue
		}
		if n, ok := normalize(item); ok {
			o.allowed[n] = true
		}
	}
	return o
}

// Allow dice si el origen puede conectarse. Un origen vacío (cliente que no es navegador) no se permite.
func (o *Origins) Allow(origin string) bool {
	if origin == "" {
		return false
	}
	if o.any {
		return true
	}
	n, ok := normalize(origin)
	return ok && o.allowed[n]
}

// normalize deja esquema y host en minúsculas y agrega el puerto por defecto, para comparar exacto.
func normalize(origin string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return "", false
		}
	}
	return scheme + "://" + host + ":" + port, true
}
