# importadoraRealtime

Servicio Go independiente que mantiene actualizadas las pantallas del ERP mediante WebSocket. La API sigue siendo la autoridad de negocio: escribe los eventos y emite tickets de conexión; este servicio lee los eventos, valida y consume cada ticket una sola vez, aplica canales y permisos, y entrega los mensajes al front. Comparte PostgreSQL y el contrato con la API y el front, pero no importa ni forma parte del módulo de ninguno de ellos.

## Arquitectura y límites

```text
Front ── Bearer: POST /realtime/ticket ──► API
  ▲                                         │ guarda ticket de un uso
  └── wss://…/ws?ticket=… ──► Realtime    │ escribe realtime_events
                                 │           │ y notifica erp_events
                                 └── LISTEN/lee PostgreSQL ◄──┘
```

La API devuelve en el ticket la URL WebSocket configurada en `REALTIME_WS_URL`. El ticket vence pronto (30 s por defecto), se transmite en lugar del JWT y no se puede volver a usar. Tras conectar, el cliente envía `hello` con la última secuencia aplicada para recuperar eventos; el servicio filtra por ámbito y permisos antes de enviar. `system.watermark` permite avanzar la secuencia aunque haya eventos no visibles para ese actor.

Responsabilidades:

- **API (`importadoraApi`)**: autenticación HTTP, emisión del ticket, y escritura transaccional de eventos de negocio en `realtime_events`/`pg_notify('erp_events', ...)`.
- **Realtime (este módulo)**: consume y valida el ticket; administra conexiones y sus colas; escucha PostgreSQL, recupera eventos, filtra canales/permisos, emite mensajes de sistema y purga eventos antiguos y tickets vencidos.
- **Front (`importadoraInterfaz`)**: pide el ticket autenticado a la API, abre el WebSocket y aplica el contrato. No debe poner el token de sesión en la URL del WebSocket.
- **`contract/`**: formato compartido versionado. Los JSON Schema son la fuente del formato y los ejemplos son probados también por API y front.

La raíz `main.go` compone el servicio: carga y valida configuración, conecta PostgreSQL, crea el contenedor, inicializa el listener, ejecuta listener y purga en segundo plano, y arranca HTTP con apagado ordenado. `core/` contiene infraestructura reutilizable; `features/` organiza casos de uso. Las dependencias se pasan mediante interfaces/contenedor: handlers no deben implementar lógica de base ni la API debe depender del hub.

## Mapa del proyecto

| Ruta | Contenido |
|---|---|
| `main.go` | Composición, procesos en segundo plano y señales de apagado |
| `src/core/config/` | Carga de variables y validación de configuración |
| `src/core/db/` | Pool PostgreSQL y conexión `LISTEN` |
| `src/core/bootstrap/` | Contenedor de dependencias y registro de rutas |
| `src/core/server/` | Servidor HTTP, orígenes permitidos y límites |
| `src/core/hub/`, `src/core/protocol/` | Conexiones, suscripciones, colas, mensajes y códigos de cierre |
| `src/core/logging/`, `src/core/events/`, `src/core/contracts/` | Logging, eventos almacenados y alcance de recuperación |
| `src/features/access/`, `tickets/` | Contexto de acceso y consumo de tickets |
| `src/features/connection/` | `GET /ws`, autenticación de conexión, saludo y replay |
| `src/features/events/` | Listener, eventos de control y purga |
| `src/features/health/` | Healthcheck `GET /health` |
| `contract/schemas/`, `examples/` | Esquemas JSON y ejemplos del contrato v1 |
| `contract/scripts/validate.go` | Validador de los ejemplos del contrato |

## Go y dependencias

El módulo declara **Go 1.27** (`go.mod`). Usa `net/http` y `log/slog` de la biblioteca estándar, `github.com/coder/websocket` para WebSocket, `github.com/jackc/pgx/v5` para PostgreSQL y `github.com/joho/godotenv` para cargar `.env` local. El validador del contrato usa `github.com/santhosh-tekuri/jsonschema/v6`. No se necesita framework HTTP adicional.

## Desarrollo local

Requisitos: Go 1.27 o compatible con `go.mod`, PostgreSQL accesible, y el esquema/migración de realtime aplicado por la API. Ejecuta los comandos desde este directorio:

1. Prepara una base de desarrollo con los cambios de esquema de realtime de `importadoraApi`. Configura una cuenta local con los permisos requeridos por el servicio (lectura de eventos y datos de acceso, consumo de tickets y purga). En desarrollo se permite, por ejemplo, `DB_USER=postgres`; no uses una cuenta superusuario en producción.
2. Crea un `.env` local ignorado por Git con tus valores. No existe `.env.example` en este módulo. Plantilla sin credenciales:

   ```dotenv
   APP_ENV=development
   APP_PORT=3002
   DB_HOST=localhost
   DB_PORT=5432
   DB_DATABASE=importadora_dev
   DB_USER=postgres
   DB_PASSWORD=
   DB_SSLMODE=disable
   REALTIME_ALLOWED_ORIGINS=http://localhost:5173
   ```

   Usa la contraseña apropiada solo en tu `.env` local si PostgreSQL la exige. `godotenv` carga `.env` si existe; las variables ya definidas en el entorno prevalecen. Puedes reutilizar los valores de host, puerto y nombre de base de la API local, pero no copies sus credenciales: configura el usuario/contraseña de mínimo privilegio de Realtime por separado. Nunca publiques secretos ni los pongas en ejemplos o documentación.

   Para conectarlo al front local, configura en la API `REALTIME_WS_URL=ws://localhost:3002/ws`; la API devuelve esa dirección al solicitar el ticket. `VITE_WS_URL` del front es solo una URL de respaldo si el ticket no incluye `wsUrl`.
3. Arranca el servicio y comprueba su estado:

   ```powershell
   go run .
   # En otra terminal:
   Invoke-RestMethod http://localhost:3002/health
   ```

   El servidor escucha en `PORT` si está definido; si no, en `APP_PORT` (por defecto `3002`). `/health` devuelve `503` si PostgreSQL o el listener no están sanos.

## Configuración

Los valores siguientes corresponden a `src/core/config/loader.go` y `validators.go`. Los valores vacíos o solo con espacios usan el default. Los valores enteros no numéricos hacen fallar el arranque.

| Variable | Default | Requerido / validación |
|---|---:|---|
| `APP_ENV` | `development` | Solo `development`, `staging` o `production` |
| `PORT` | ausente | Si existe, prevalece sobre `APP_PORT`; puerto TCP de 1 a 65535 |
| `APP_PORT` | `3002` | Se usa si `PORT` está ausente; puerto de 1 a 65535 |
| `DB_HOST` | vacío | Obligatorio |
| `DB_PORT` | `5432` | Debe ser entero; `Validate` no impone un rango propio |
| `DB_DATABASE` | vacío | Obligatorio |
| `DB_USER` | `importadora_realtime` | Obligatorio tras aplicar default; en producción se advierte si es `postgres` |
| `DB_PASSWORD` | vacío | Obligatoria fuera de `development` |
| `DB_SSLMODE` | `disable` | Se pasa a pgx como `sslmode` |
| `REALTIME_DB_MAX_CONNS` | `10` | De 2 a 50 |
| `REALTIME_ALLOWED_ORIGINS` | `http://localhost:5173` | Lista separada por comas; no vacía; cada origen debe ser `http(s)://host[:puerto]` sin ruta. `*` solo se admite en desarrollo |
| `REALTIME_RETENTION_HOURS` | `48` | De 1 a 720 |
| `REALTIME_MAX_REPLAY` | `5000` | De 100 a 100000 |
| `REALTIME_MAX_CLIENT_QUEUE` | `256` | De 16 a 4096 |
| `REALTIME_MAX_CONN_PER_ACTOR` | `5` | De 1 a 50 |
| `REALTIME_PING_SEC` | `25` | De 5 a 60 |
| `REALTIME_PONG_TIMEOUT_SEC` | `10` | De 2 a 30 |
| `REALTIME_WATERMARK_SEC` | `30` | De 5 a 300 |
| `REALTIME_HELLO_TIMEOUT_SEC` | `5` | De 1 a 30 |
| `REALTIME_MAX_MESSAGE_BYTES` | `4096` | De 512 a 65536 |

`PORT` es el nombre que asigna Railway y tiene precedencia efectiva aunque el plan histórico detalle `APP_PORT`. En Railway configura los valores secretos en Variables, no en archivos del repositorio. Los detalles de mínimos privilegios y decisiones históricas están en [`PLAN-WEBSOCKET.md`](PLAN-WEBSOCKET.md), sección 14.

## Pruebas y contrato

```powershell
go build ./...
go vet ./...
go test ./...
go run ./contract/scripts/validate.go
```

Las pruebas unitarias corren sin servicio externo. Las pruebas que requieren PostgreSQL omiten su ejecución si falta `TEST_DATABASE_URL`; para ejecutarlas, apúntala a una base **de pruebas aislada**, con el esquema necesario, nunca a la base de trabajo:

```powershell
$env:TEST_DATABASE_URL = 'postgres://<usuario>:<clave>@localhost:5432/importadora_test?sslmode=disable'
go test ./...
```

El validador del contrato informa el resultado de los 48 ejemplos v1. Los contratos, mensajes, permisos y ejemplos completos están en [`contract/REALTIME-CONTRATO.md`](contract/REALTIME-CONTRATO.md) y el historial de cambios compatibles en [`contract/CHANGELOG.md`](contract/CHANGELOG.md).

## Coordinación y despliegue en Railway

La API y Realtime son servicios separados del mismo proyecto de Railway y usan el mismo PostgreSQL, pero **credenciales distintas**. En el servicio Realtime configura `DB_HOST`, `DB_PORT`, `DB_DATABASE` y el usuario/clave dedicado `importadora_realtime`; completa las variables de `src/core/config` para entorno de producción, en particular `APP_ENV=production` y el origen real del front en `REALTIME_ALLOWED_ORIGINS`. No establezcas `PORT` manualmente: Railway lo inyecta. El proceso enlaza `:<PORT>` (`:<APP_PORT>` localmente).

Configura el servicio con:

- Root Directory: `importadoraRealtime`.
- Build: `go build -o realtime .`; Start command: `./realtime`.
- Healthcheck: `/health`.
- Una réplica: el hub y el conteo de conexiones viven en memoria.
- Dominio público TLS de Railway para el WebSocket: `wss://<dominio-realtime>/ws`.

El front consume `VITE_REALTIME_ENABLED` (solo `false` o `0` lo desactiva; por defecto está habilitado) y `VITE_WS_URL` como respaldo si la respuesta de ticket no trae `wsUrl`. Para producción configura `REALTIME_WS_URL` en la API: es obligatoria allí y debe apuntar al dominio público `wss://` de Realtime.

Para coordinar el lanzamiento:

1. Aplica la migración de realtime de la API a PostgreSQL y configura allí el productor.
2. En la API, habilita sus eventos (`REALTIME_EVENTS_ENABLED`, default `true`) y configura `REALTIME_WS_URL=wss://<dominio-realtime>/ws`; el URL se devuelve al front junto con el ticket. El TTL del ticket (`REALTIME_TICKET_TTL_SEC`) es 30 segundos por defecto y acepta 10–120.
3. Despliega y verifica Realtime en `/health`.
4. Despliega el front con `VITE_REALTIME_ENABLED=true` y el origen público exacto incluido en `REALTIME_ALLOWED_ORIGINS`.

El front pide el ticket a la API mediante HTTP autenticado y luego se conecta al `wsUrl` recibido; no debe inferir el endpoint privado ni enviar el JWT por la URL. No se requiere nginx en Railway. Antes de activar el front verifica API → emisión del ticket → upgrade WebSocket → `/health`; si se revierte, desactiva primero Realtime en el front. La secuencia y el contexto completos están en la sección 18 de [`PLAN-WEBSOCKET.md`](PLAN-WEBSOCKET.md).

## Documentación del directorio

- `README.md`: guía vigente de uso, configuración y despliegue.
- `PLAN-WEBSOCKET.md`: especificación técnica y hoja de ruta; conserva contexto de diseño y fases ya realizadas.
- `contract/REALTIME-CONTRATO.md` y `contract/CHANGELOG.md`: contrato versionado y evolución de mensajes.

Para valores de configuración prevalecen `src/core/config/` y este README. Se retiraron las notas de traspaso y supervisión de fases ya completadas; su estado histórico no debe confundirse con la configuración operativa vigente.
