package db

// listen_conn.go · conexión dedicada (fuera del pool) para LISTEN erp_events y cálculo de espera creciente al reconectar
// conecta con: features/events/services/listener_service.go y core/config (DSN)
// ojo: una conexión del pgxpool no sirve para LISTEN porque el pool la presta y pierde el estado; siempre se abre con pgx.Connect

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// NotifyChannel es el canal de PostgreSQL donde la API publica el último seq confirmado.
const NotifyChannel = "erp_events"

const (
	MinBackoff = 500 * time.Millisecond
	MaxBackoff = 30 * time.Second
)

// ListenConn es lo que el listener necesita de la conexión dedicada (permite simular caídas en pruebas).
type ListenConn interface {
	WaitForNotification(ctx context.Context) error
	Close(ctx context.Context) error
}

// DialFunc abre una conexión ya suscrita a LISTEN erp_events.
type DialFunc func(ctx context.Context) (ListenConn, error)

type pgxListenConn struct {
	conn *pgx.Conn
}

func (c *pgxListenConn) WaitForNotification(ctx context.Context) error {
	_, err := c.conn.WaitForNotification(ctx)
	return err
}

func (c *pgxListenConn) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

// NewListenDialer devuelve un DialFunc que conecta a dsn con pgx.Connect y ejecuta LISTEN erp_events.
func NewListenDialer(dsn string) DialFunc {
	return func(ctx context.Context) (ListenConn, error) {
		connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return nil, fmt.Errorf("configuración LISTEN inválida: %w", err)
		}
		cfg.RuntimeParams["application_name"] = "importadoraRealtime_listen"

		conn, err := pgx.ConnectConfig(connCtx, cfg)
		if err != nil {
			return nil, fmt.Errorf("no se pudo abrir conexión LISTEN: %w", err)
		}
		if _, err := conn.Exec(connCtx, "LISTEN "+NotifyChannel); err != nil {
			_ = conn.Close(connCtx)
			return nil, fmt.Errorf("no se pudo ejecutar LISTEN %s: %w", NotifyChannel, err)
		}
		return &pgxListenConn{conn: conn}, nil
	}
}

// NextBackoff duplica la espera entre reconexiones desde 0,5 s hasta 30 s.
func NextBackoff(current time.Duration) time.Duration {
	if current < MinBackoff {
		return MinBackoff
	}
	next := current * 2
	if next > MaxBackoff {
		return MaxBackoff
	}
	return next
}

