package db

// postgres.go · pool de conexiones a PostgreSQL con el usuario importadora_realtime (solo lee, marca tickets y purga)
// conecta con: core/config (datos de conexión) y los repositorios de features (tickets, access, events); la conexión de LISTEN es aparte (E5)

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"importadoraRealtime/src/core/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect abre el pool y comprueba la base con un Ping de máximo 10 s.
func Connect(ctx context.Context, c *config.Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(DSN(c))
	if err != nil {
		return nil, fmt.Errorf("configuración de base inválida: %w", err)
	}
	pc.MaxConns = c.DBMaxConns
	pc.ConnConfig.RuntimeParams["application_name"] = "importadoraRealtime"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("no se pudo crear el pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo conectar con PostgreSQL (%s:%d/%s como %s): %w", c.DBHost, c.DBPort, c.DBName, c.DBUser, err)
	}
	return pool, nil
}

// DSN arma la cadena de conexión (la clave va escapada; nunca se registra en logs).
func DSN(c *config.Config) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.DBUser, c.DBPassword),
		Host:     fmt.Sprintf("%s:%d", c.DBHost, c.DBPort),
		Path:     "/" + c.DBName,
		RawQuery: "sslmode=" + url.QueryEscape(c.DBSSLMode),
	}
	return u.String()
}
