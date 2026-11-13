package config

// loader.go · lee .env (si existe) y las variables de entorno, con los valores por defecto de la sección 14.8 del plan
// conecta con: config.go (llena Config) y validators.go (Load valida antes de devolver)
// ojo: en Railway el puerto llega en PORT; APP_PORT solo se usa si PORT no existe (decisión 10)

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Load lee la configuración y la valida; un error termina el proceso con un mensaje claro.
func Load() (*Config, error) {
	_ = godotenv.Load() // sin .env se usan solo las variables del entorno

	ints := map[string]int{}
	for key, def := range map[string]int{
		"DB_PORT": 5432, "REALTIME_DB_MAX_CONNS": 10, "REALTIME_RETENTION_HOURS": 48, "REALTIME_MAX_REPLAY": 5000,
		"REALTIME_MAX_CLIENT_QUEUE": 256, "REALTIME_MAX_CONN_PER_ACTOR": 5, "REALTIME_PING_SEC": 25, "REALTIME_PONG_TIMEOUT_SEC": 10,
		"REALTIME_WATERMARK_SEC": 30, "REALTIME_HELLO_TIMEOUT_SEC": 5, "REALTIME_MAX_MESSAGE_BYTES": 4096,
	} {
		v, err := envInt(key, def)
		if err != nil {
			return nil, err
		}
		ints[key] = v
	}

	cfg := &Config{
		AppEnv:         envStr("APP_ENV", "development"),
		AppPort:        envStr("PORT", envStr("APP_PORT", "3002")),
		DBHost:         envStr("DB_HOST", ""),
		DBPort:         ints["DB_PORT"],
		DBName:         envStr("DB_DATABASE", ""),
		DBUser:         envStr("DB_USER", "importadora_realtime"),
		DBPassword:     envStr("DB_PASSWORD", ""),
		DBSSLMode:      envStr("DB_SSLMODE", "disable"),
		DBMaxConns:     int32(ints["REALTIME_DB_MAX_CONNS"]),
		AllowedOrigins: parseCSV(envStr("REALTIME_ALLOWED_ORIGINS", "http://localhost:5173")),
		Retention:      time.Duration(ints["REALTIME_RETENTION_HOURS"]) * time.Hour,
		MaxReplay:      ints["REALTIME_MAX_REPLAY"],
		MaxClientQueue: ints["REALTIME_MAX_CLIENT_QUEUE"],
		MaxConnPerUser: ints["REALTIME_MAX_CONN_PER_ACTOR"],
		PingInterval:   time.Duration(ints["REALTIME_PING_SEC"]) * time.Second,
		PongTimeout:    time.Duration(ints["REALTIME_PONG_TIMEOUT_SEC"]) * time.Second,
		Watermark:      time.Duration(ints["REALTIME_WATERMARK_SEC"]) * time.Second,
		HelloTimeout:   time.Duration(ints["REALTIME_HELLO_TIMEOUT_SEC"]) * time.Second,
		MaxMessageSize: int64(ints["REALTIME_MAX_MESSAGE_BYTES"]),
	}
	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("configuración inválida: %w", err)
	}
	return cfg, nil
}

func envStr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s debe ser un número entero: %q", key, v)
	}
	return n, nil
}

func parseCSV(raw string) []string {
	out := []string{}
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
