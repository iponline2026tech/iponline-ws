package config

// validators_test · la configuración insegura o incompleta no deja arrancar el servicio

import (
	"strings"
	"testing"
	"time"
)

func valid() *Config {
	return &Config{
		AppEnv: "production", AppPort: "3002", DBHost: "db", DBPort: 5432, DBName: "importadora", DBUser: "importadora_realtime",
		DBPassword: "secreto", DBMaxConns: 10, AllowedOrigins: []string{"https://app.importadora.com"}, Retention: 48 * time.Hour,
		MaxReplay: 5000, MaxClientQueue: 256, MaxConnPerUser: 5, PingInterval: 25 * time.Second, PongTimeout: 10 * time.Second,
		Watermark: 30 * time.Second, HelloTimeout: 5 * time.Second, MaxMessageSize: 4096,
	}
}

func TestValidConfigPasses(t *testing.T) {
	if err := Validate(valid()); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidConfigFails(t *testing.T) {
	cases := map[string]func(c *Config){
		"falta la base":               func(c *Config) { c.DBHost = "" },
		"sin clave en producción":     func(c *Config) { c.DBPassword = "" },
		"orígenes vacíos":             func(c *Config) { c.AllowedOrigins = nil },
		"«*» en producción":           func(c *Config) { c.AllowedOrigins = []string{"*"} },
		"origen con ruta":             func(c *Config) { c.AllowedOrigins = []string{"https://app.com/ws"} },
		"origen sin esquema":          func(c *Config) { c.AllowedOrigins = []string{"app.com"} },
		"puerto fuera de rango":       func(c *Config) { c.AppPort = "70000" },
		"APP_ENV desconocido":         func(c *Config) { c.AppEnv = "prod" },
		"más de 50 pestañas":          func(c *Config) { c.MaxConnPerUser = 51 },
		"watermark fuera de rango":    func(c *Config) { c.Watermark = time.Second },
		"retención de más de 30 días": func(c *Config) { c.Retention = 721 * time.Hour },
		"mensajes demasiado pequeños": func(c *Config) { c.MaxMessageSize = 100 },
	}
	for name, mutate := range cases {
		c := valid()
		mutate(c)
		if err := Validate(c); err == nil {
			t.Errorf("%s: debía rechazarse", name)
		}
	}
}

func TestStarAllowedOnlyInDevelopment(t *testing.T) {
	c := valid()
	c.AppEnv, c.AllowedOrigins, c.DBPassword = "development", []string{"*"}, ""
	if err := Validate(c); err != nil {
		t.Fatalf("en desarrollo «*» y sin clave se permiten: %v", err)
	}
	c.AppEnv = "staging"
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "DB_PASSWORD") && !strings.Contains(err.Error(), "«*»") {
		t.Fatalf("en staging ya no: %v", err)
	}
}
