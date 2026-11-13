package config

// config.go · configuración tipada del servicio de tiempo real (solo datos; se lee en loader.go y se valida en validators.go)
// conecta con: main.go y core/bootstrap (la reparten); tabla de variables en PLAN-WEBSOCKET.md, sección 14.8

import "time"

// Config es toda la configuración del servicio.
type Config struct {
	AppEnv  string // development | staging | production
	AppPort string // PORT (Railway) o APP_PORT

	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string // importadora_realtime (mínimo privilegio)
	DBPassword string
	DBSSLMode  string
	DBMaxConns int32

	AllowedOrigins []string      // orígenes del front que pueden abrir el websocket (comparación exacta)
	Retention      time.Duration // vida de realtime_events (decisión 1: 48 h)
	MaxReplay      int           // máximo de eventos a reenviar con hello; más → system.resync_required
	MaxClientQueue int           // mensajes en cola por conexión; lleno → cierre 4408
	MaxConnPerUser int           // pestañas por usuario (decisión 6: 5)
	PingInterval   time.Duration
	PongTimeout    time.Duration
	Watermark      time.Duration // cada cuánto se envía system.watermark (decisión 7: 30 s)
	HelloTimeout   time.Duration // espera del primer mensaje hello
	MaxMessageSize int64         // tamaño máximo de un mensaje del cliente (bytes)
}

// IsDevelopment dice si corre en desarrollo (permite «*» en orígenes y logs legibles).
func (c *Config) IsDevelopment() bool { return c.AppEnv == "development" }
