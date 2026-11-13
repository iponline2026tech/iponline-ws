package config

// validators.go · el servicio no arranca con configuración incompleta o peligrosa (orígenes abiertos en producción, sin BD, rangos fuera de la tabla 14.8)
// conecta con: loader.go (Load la llama) y validators_test.go

import (
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Validate revisa toda la configuración y devuelve el primer problema encontrado.
func Validate(c *Config) error {
	switch c.AppEnv {
	case "development", "staging", "production":
	default:
		return fmt.Errorf("APP_ENV debe ser development, staging o production (llegó %q)", c.AppEnv)
	}
	if port, err := strconv.Atoi(c.AppPort); err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("el puerto (PORT o APP_PORT) debe estar entre 1 y 65535 (llegó %q)", c.AppPort)
	}
	if c.DBHost == "" || c.DBName == "" || c.DBUser == "" {
		return fmt.Errorf("faltan datos de la base: DB_HOST, DB_DATABASE y DB_USER son obligatorias")
	}
	if c.DBPassword == "" && !c.IsDevelopment() {
		return fmt.Errorf("DB_PASSWORD es obligatoria fuera de desarrollo")
	}
	if c.DBUser == "postgres" && c.AppEnv == "production" {
		slog.Warn("el servicio realtime usa el usuario postgres en producción; usa importadora_realtime (mínimo privilegio)")
	}
	if err := validateOrigins(c); err != nil {
		return err
	}
	ranges := []struct {
		name     string
		value    int
		min, max int
	}{
		{"REALTIME_DB_MAX_CONNS", int(c.DBMaxConns), 2, 50},
		{"REALTIME_RETENTION_HOURS", int(c.Retention / time.Hour), 1, 720},
		{"REALTIME_MAX_REPLAY", c.MaxReplay, 100, 100000},
		{"REALTIME_MAX_CLIENT_QUEUE", c.MaxClientQueue, 16, 4096},
		{"REALTIME_MAX_CONN_PER_ACTOR", c.MaxConnPerUser, 1, 50},
		{"REALTIME_PING_SEC", int(c.PingInterval / time.Second), 5, 60},
		{"REALTIME_PONG_TIMEOUT_SEC", int(c.PongTimeout / time.Second), 2, 30},
		{"REALTIME_WATERMARK_SEC", int(c.Watermark / time.Second), 5, 300},
		{"REALTIME_HELLO_TIMEOUT_SEC", int(c.HelloTimeout / time.Second), 1, 30},
		{"REALTIME_MAX_MESSAGE_BYTES", int(c.MaxMessageSize), 512, 65536},
	}
	for _, r := range ranges {
		if r.value < r.min || r.value > r.max {
			return fmt.Errorf("%s debe estar entre %d y %d (llegó %d)", r.name, r.min, r.max, r.value)
		}
	}
	return nil
}

// validateOrigins: lista no vacía; «*» solo en desarrollo; cada origen es esquema + host (+ puerto), sin ruta.
func validateOrigins(c *Config) error {
	if len(c.AllowedOrigins) == 0 {
		return fmt.Errorf("REALTIME_ALLOWED_ORIGINS no puede estar vacía")
	}
	for _, o := range c.AllowedOrigins {
		if o == "*" {
			if !c.IsDevelopment() {
				return fmt.Errorf("REALTIME_ALLOWED_ORIGINS no puede ser «*» fuera de desarrollo")
			}
			continue
		}
		u, err := url.Parse(o)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.Trim(u.Path, "/") != "" {
			return fmt.Errorf("origen inválido %q: debe ser esquema y host, por ejemplo https://app.midominio.com", o)
		}
	}
	return nil
}
