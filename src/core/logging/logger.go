package logging

// logger.go · logs del servicio con log/slog: JSON en staging/producción y texto legible en desarrollo, con campos fijos
// conecta con: main.go (lo instala como logger por defecto) y el resto del servicio (WithConn, WithEvent)
// ojo: NUNCA se registran tickets, payloads completos ni correos (sección 14.9 del plan)

import (
	"log/slog"
	"os"
)

// New crea el logger según el entorno y lo deja como logger por defecto.
func New(development bool) *slog.Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if development {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(handler).With("service", "importadoraRealtime")
	slog.SetDefault(logger)
	return logger
}

// WithConn agrega el id de la conexión a cada línea.
func WithConn(l *slog.Logger, connID string) *slog.Logger { return l.With("connId", connID) }

// WithEvent agrega el seq y el tipo del evento a cada línea.
func WithEvent(l *slog.Logger, seq int64, eventType string) *slog.Logger {
	return l.With("seq", seq, "type", eventType)
}
