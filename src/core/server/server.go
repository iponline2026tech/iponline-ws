package server

// server.go · servidor HTTP del servicio: GET /health y GET /ws, tiempos de espera y apagado ordenado
// conecta con: core/bootstrap/router.go (registra las rutas en el mux) y main.go (Start y Shutdown)
// ojo: sin WriteTimeout global porque cortaría los websockets; los límites de cada socket los pone su conexión (E4)

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// ShutdownTimeout es el máximo que espera el apagado ordenado.
const ShutdownTimeout = 10 * time.Second

// Server envuelve el http.Server y su mux.
type Server struct {
	Mux        *http.ServeMux
	httpServer *http.Server
	onShutdown []func(ctx context.Context)
}

// New crea el servidor en el puerto indicado.
func New(port string) *Server {
	mux := http.NewServeMux()
	return &Server{
		Mux: mux,
		httpServer: &http.Server{
			Addr:              ":" + port,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

// OnShutdown registra algo que se cierra al apagar (por ejemplo el hub, que cierra los sockets con 1001).
func (s *Server) OnShutdown(fn func(ctx context.Context)) { s.onShutdown = append(s.onShutdown, fn) }

// Start escucha en el puerto; devuelve cuando el servidor se detiene.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	slog.Info("servidor http escuchando", "addr", ln.Addr().String())
	if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown deja de aceptar conexiones, corre lo registrado en OnShutdown y espera hasta ShutdownTimeout.
func (s *Server) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ShutdownTimeout)
	defer cancel()
	err := s.httpServer.Shutdown(ctx)
	for _, fn := range s.onShutdown {
		fn(ctx)
	}
	return err
}
