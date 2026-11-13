# Plan de tiempo real (websocket): integración completa

> Alcance:
> - **`importadoraRealtime/`** (nuevo, directorio aparte): servicio websocket en Go 1.27 + pgx v5 + `coder/websocket`.
> - **`importadoraApi/`** (Go 1.27 + Echo v4 + pgx v5 + PostgreSQL): solo escribe los eventos y emite el ticket.
> - **`importadoraInterfaz/`** (React 19 + Redux Toolkit 2 + redux-persist + Vite): se conecta y aplica los eventos.
> Se aplica **después** de `PLAN-AJUSTES-UI-PERMISOS.md`, porque depende de:
> - la Fase 5 (cada mutación actualiza su slice con la respuesta),
> - el punto 6.4 (el carrito del admin queda a nombre de su empleado ligado),
> - la Fase 8 (catálogo final de permisos `module:action`).
>
> Las reglas 0.1–0.7 de ese plan (validar antes de tocar, capas, comentarios, migraciones + `startdb.pgsql`, documentación, verificación) aplican igual aquí.
>
> **Estado revisado (12-nov-2026):** las etapas **E0–E8 están implementadas**; E8 cubre stock, traslados y cambios de estado. La siguiente etapa del plan es **E9 · Resto de dominios**. Para uso local, variables y despliegue consulta el [README vigente](README.md); el estado de Railway debe confirmarse contra la instancia real, no inferirse de las pruebas locales. Este archivo conserva la especificación y la hoja de ruta técnica.
>
> **Punto de partida:** este plan arranca cuando `../PLAN-AJUSTES-UI-PERMISOS.md` termina. Su estado final es la base de todo lo de aquí (sección 9.0). No corre en paralelo con ese plan ni lo modifica.

### Índice

| Sección | Qué responde |
|---|---|
| 0 | Por qué es un servicio aparte y qué hace cada directorio |
| 1–2 | Qué resuelve y las buenas prácticas aplicadas a nuestro stack |
| 3 | Flujo completo y puntos de entrada (E1–E7 en el backend, F1–F7 en el front) |
| 4 | Cómo se ve cada caso en la interfaz |
| 5 | Base de datos (tablas, rol y variables) |
| **6** | **Catálogo completo de los 48 eventos**, canales, quién consume cada uno y contrato TypeScript |
| **7** | **Contrato Go** del productor (API) y del sobre (servicio realtime) |
| **8** | **Estructura completa: cada carpeta, cada archivo y su responsabilidad** (realtime, API y front) |
| 9 | **Ruta de desarrollo**: punto de partida, etapa P de verificación y etapas E0–E11 con tareas por archivo, pruebas y criterio de salida |
| 10–11 | Peticiones HTTP que desaparecen y decisiones abiertas |
| 12 | Escenarios detallados (stock, traslados, activo ⇄ inactivo) y lista de aceptación |
| 13 | Qué se integra, cambia y retira en cada directorio |
| 14 | Especificación del servicio realtime (saludo, canales, filtro, hub, cierres, listener, configuración y seguridad) |
| 15 | Especificación del productor en la API (`Flush`, ruta → evento, respuestas completas y ticket) |
| 16 | Especificación del front (máquina de estados, adaptador, secuenciador, manejadores, avisos y carreras) |
| 17 | Estrategia de pruebas |
| 18 | Operación: local, despliegue, proxy, interruptores, monitoreo y respaldos |
| 19–20 | Riesgos y definición de terminado |

---

## 0. Arquitectura: el websocket es un servicio aparte (`importadoraRealtime/`)

> **Decisión del dueño:** el websocket **no** se construye dentro de `importadoraApi/` ni dentro de `importadoraInterfaz/`. Vive en un **directorio propio** al mismo nivel que ellos, con su propio módulo Go, su propio `main.go`, su propio `.env` y su propio puerto. Esta sección manda sobre cualquier otra parte del plan que diga lo contrario.

```
getion2import/
├── importadoraApi/          API HTTP (sin websocket): ejecuta los comandos y ESCRIBE los eventos en la BD
├── importadoraInterfaz/     Front: comandos por HTTP a la API y eventos por websocket al servicio realtime
├── importadoraRealtime/     NUEVO · servicio websocket: LEE los eventos de la BD y los reparte
├── Idea/
└── PLAN-*.md
```

### 0.1 Reparto de responsabilidades

| Responsabilidad | `importadoraApi` | `importadoraRealtime` | `importadoraInterfaz` |
|---|---|---|---|
| Ejecutar comandos (crear, editar, cambiar estado, anular) | **Sí** (como hoy) | **Nunca** | Los pide por HTTP |
| Crear el evento dentro de la transacción (`realtime_events` + `pg_notify`) | **Sí**: un paquete pequeño `core/events` (sin hub, sin sockets) | No | — |
| Emitir el ticket de conexión | **Sí**: `POST /realtime/ticket` (ya conoce la sesión y el token) | Lo **consume** (un solo uso) | Lo pide antes de conectar |
| Aceptar conexiones websocket, canales, filtro por permiso, ping, recuperación por `seq` | No | **Sí** | Se conecta a `VITE_WS_URL` |
| Escuchar `LISTEN erp_events` y leer `realtime_events` | No | **Sí** | — |
| Purgar eventos viejos | No | **Sí** | — |
| Dueño del esquema de BD (migraciones y `startdb.pgsql`) | **Sí** (sigue siendo el único) | No migra nada: solo lee | — |
| Conocer el cuerpo de cada evento | Sí (lo arma) | **No**: solo lee el sobre (compañía, bodegas, canal, permisos) y reenvía `payload` tal cual | Sí (lo aplica) |

### 0.2 Por qué así (buenas prácticas)

1. **La API no sabe que existe el websocket.** Solo escribe una fila más en su transacción. Si el servicio realtime se cae, la API sigue funcionando igual. Al volver, el servicio recupera los eventos desde la tabla por `seq`.
2. **La base de datos es el único punto de unión** (patrón *outbox*). No hay llamadas HTTP entre la API y el servicio realtime, así que tampoco hay eventos perdidos si una de esas llamadas fallara después del commit.
3. **El servicio realtime no tiene lógica de negocio.** Es un repartidor: no conoce stock, traslados ni estados, solo el sobre. Un evento nuevo no obliga a tocarlo, salvo que necesite un canal nuevo.
4. **Se escala y se despliega por separado.** Puede haber varias instancias (cada una con su `LISTEN`) sin tocar la API.
5. **Mínimo privilegio.** El servicio realtime se conecta con un usuario de PostgreSQL propio (`importadora_realtime`):
   - `SELECT` en `realtime_events` y en las tablas de sesión y permisos (`refresh_tokens`, `employees`, `admins`, `superadmins`, `employee_permissions`, `permissions`, `companies` y `warehouses`);
   - `UPDATE` solo en `realtime_tickets` (marcar el ticket como usado);
   - `DELETE` solo en `realtime_events` (purga).
   No puede escribir datos de negocio.
6. **Mismo stack y misma estructura** que la API (Go 1.27, pgx v5, carpetas `core/` y `features/` con `routes/controllers/services/repositories`, cabeceras de comentario), para que cualquiera del equipo lo lea igual. No necesita Echo: basta `net/http` con `github.com/coder/websocket`.

### 0.3 Cómo se autentica una conexión (sin compartir código entre módulos)

```
Front ── POST /realtime/ticket (Bearer) ──► API
          API: valida sesión → INSERT realtime_tickets (hash del ticket, actor, compañía, bodega, session_id, vence en 30 s)
          ◄── { ticket, wsUrl }
Front ── WS wss://realtime…/ws?ticket=… ──► importadoraRealtime
          realtime: UPDATE realtime_tickets SET used_at = now()
                    WHERE ticket_hash = $1 AND used_at IS NULL AND expires_at > now() RETURNING actor…   (un solo uso)
                    → valida que la sesión siga activa (refresh_tokens) y que el actor esté disponible
                    → carga sus permisos (misma consulta que el middleware de la API)
                    → calcula sus canales → envía system.welcome
```

- El servicio realtime **no** necesita `JWT_SECRET`: no lee el token, solo el ticket guardado en la BD.
- Las consultas de sesión y permisos se **replican** en `importadoraRealtime/src/features/access/repositories` (solo lectura) y se documenta en ambos lados que deben coincidir con `importadoraApi/src/core/middlewares/auth_middleware.go`. Una prueba de integración en el servicio realtime compara su resultado con el de la API para el mismo actor.

### 0.4 El contrato es compartido, el código no

- **Fuente única:** `importadoraRealtime/contract/` con `REALTIME-CONTRATO.md` y un JSON Schema por tipo de evento (`contract/schemas/*.schema.json`), más el del sobre.
- **Productor** (`importadoraApi/src/core/events/types.go`) y **consumidor** (`importadoraInterfaz/src/features/realtime/types/realtime.contract.ts`) tienen sus tipos propios (secciones 6 y 7). Cada lado tiene una prueba que valida ejemplos contra los JSON Schema, así que si alguien cambia un campo en un solo lado, la prueba falla.
- **No** se usa `replace` en `go.mod` para que la API importe el módulo realtime ni al revés: los tres directorios siguen independientes.

---

## 1. Qué resuelve y qué no

**Resuelve:** que todo cambio de estado de la operación aparezca **de inmediato**, igual y sin recargar, en cada pantalla, tabla, drawer, KPI y selector donde se vea. Vale para el usuario que hizo el cambio, para sus compañeros y para otras bodegas. Lo que debe quedar completamente cubierto:

- **Activo ⇄ inactivo** de cualquier entidad (clientes, empleados, productos, marcas, categorías, modelos, bodegas, ubicaciones, proveedores, transportadoras, roles, compañías y administradores).
- **Cantidades físicas**: disponible, reservado y vendible por ubicación y por bodega, con su causa (venta, traslado, compra, devolución, ajuste, conteo o reubicación).
- **Traslados entre bodegas**: creado → despachado (en tránsito) → recibido / con novedad / cancelado, más las cantidades que salen del origen y entran al destino.
- **Flujos de documentos**: etapas del pedido, facturas, pagos y abonos, caja, compras, devoluciones, cotizaciones, conteos y cartera.
- **Seguridad de la sesión**: sesión revocada, permisos cambiados y cuenta o compañía desactivada.

**No hace:**
- No reemplaza la carga inicial de cada pantalla ni los comandos. **Crear, editar, cambiar estado y anular siguen por HTTP**, que es donde ya están la validación, los permisos, el motivo obligatorio, la auditoría y las transacciones. El websocket **solo informa** lo que ya quedó confirmado en la base de datos.
- No sirve para subir archivos, exportar a Excel ni para reportes pesados.

---

## 2. Buenas prácticas aplicadas a nuestro stack

| # | Práctica | Cómo se aplica aquí |
|---|---|---|
| 1 | **Comandos por HTTP, eventos por websocket** (CQRS liviano) | Las rutas y servicios actuales no cambian su contrato. Cada servicio solo **registra** eventos dentro de su transacción. |
| 2 | **Un evento solo existe si la transacción se confirmó** (outbox transaccional) | El evento se escribe en `realtime_events` **dentro de la misma transacción** y se ejecuta `pg_notify`. PostgreSQL entrega el `NOTIFY` **solo al hacer commit**; si hay rollback, el evento desaparece con él. |
| 3 | **Una conexión por pestaña y canales asignados por el servidor** | Canales `company:{id}`, `warehouse:{id}` y `actor:{tipo}:{id}`, calculados desde la sesión. El cliente **no** elige canales: no puede escuchar lo que no le corresponde. |
| 4 | **Filtro por permiso al salir** | Cada tipo de evento declara el permiso que exige (catálogo de la Fase 8). El hub solo lo envía a quien lo tiene. Es la misma regla que la API. |
| 5 | **Valores absolutos, no diferencias** | `stock.changed` envía `available`, `reserved` y `sellable` finales, no «+3». Aplicarlo dos veces da el mismo resultado (idempotente) y nunca descuadra. |
| 6 | **Versión por entidad** | Cada evento trae `entity.version` (el `updated_at` de la fila). El reducer **ignora** lo que sea igual o más viejo que lo que ya tiene. Esto resuelve los duplicados (la pestaña que hizo el cambio recibe su propio evento), el desorden y la carrera entre la carga HTTP y el evento. |
| 7 | **Secuencia global para recuperar** | `seq` (BIGINT creciente). Al reconectar, el cliente envía su último `seq` y el servidor reenvía lo que falta. Si el hueco es demasiado grande, manda `resync.required`. |
| 8 | **Autenticación con ticket de un solo uso** | La API emite el ticket (`POST /realtime/ticket`, con Bearer, vence en 30 s, guardado como hash en `realtime_tickets`). El servicio realtime lo consume una sola vez en `/ws?ticket=…` (ver 0.3). El token **no** viaja en la URL (hoy `realtime.url.ts` lo pone en `?token=`, donde queda en los logs). |
| 9 | **Validación de `Origin`** | El servicio realtime valida el `Origin` contra su propia variable `REALTIME_ALLOWED_ORIGINS` (mismo valor que `CORS_ALLOWED_ORIGINS` de la API). |
| 10 | **Ping/pong y presión de salida** | Ping cada 25 s y cierre si no hay pong en 10 s. Cola de salida acotada por cliente (256 mensajes): si se llena, se corta y el cliente se recupera solo. |
| 11 | **Agrupar en ráfagas** | Un conteo o una recepción de compra puede tocar 300 ubicaciones. Los cambios de stock de una misma transacción se agrupan en **un** evento con lista (última versión por `stockId`), en lotes de máximo 200. |
| 12 | **El servidor no confía en el cliente** | El cliente solo envía `hello` (con su último `seq`) y `pong`. Cualquier otro mensaje se ignora. Tamaño máximo de mensaje entrante: 4 KB. |
| 13 | **Concurrencia optimista en ediciones** | Si otro usuario cambió el registro mientras alguien lo editaba, el `PUT` llega con `expectedVersion` y la API responde **409 `STALE_VERSION`**. El websocket muestra el aviso antes de que la persona guarde. |
| 14 | **Contrato versionado y escrito** | Campo `v: 1` en el sobre. `importadoraRealtime/contract/` (documento + JSON Schema) es la única fuente; los tipos Go de la API y los TS del front se validan contra ella (ver 0.4, 6 y 7). |

### Detalles propios de nuestro stack

- **La API casi no cambia su servidor.** Como el websocket vive en `importadoraRealtime/`, no hay que tocar el Gzip ni el Logger de Echo en `importadoraApi/src/core/server/server.go`; el gzip habría interferido con el «upgrade» del websocket si estuviera en la API. Lo único que se agrega es `middleware.RequestID()`, para el `correlationId`.
- **Librería (en `importadoraRealtime`):** `github.com/coder/websocket` (antes nhooyr): mantenida, funciona con `context` y `net/http`, y trae `AcceptOptions.OriginPatterns` y compresión opcional. Se fija la versión en su `go.mod` con `go get`. No hace falta Echo.
- **pgx v5:** `LISTEN` necesita una **conexión dedicada** fuera del pool (`pgxpool.Acquire` + `Conn().WaitForNotification(ctx)`), con reconexión. `pg_notify` tiene un límite de ~8000 bytes, así que se envía **solo el `seq`** y el listener lee los eventos de la tabla en lote (`WHERE seq > $último ORDER BY seq`).
- **Varias instancias:** cada instancia de `importadoraRealtime` tiene su listener y su hub. No se necesita Redis, y la API se escala por separado.
- **Redux:** el estado `realtime` **no** entra en la whitelist de `redux-persist` (`app/store/store.ts`). Las entidades que se actualizan por evento usan `createEntityAdapter` (lista normalizada por id), para que una sola actualización llegue a tabla, drawer, KPI y selectores a la vez.
- **Dependencias del front:** hoy `realtime.handlers.ts` importa `features/shop`. Para respetar `app → features → global → api` sin que `realtime` conozca todas las features, cada feature exporta sus manejadores desde su `index.ts` y `app/store` los registra (inversión de dependencia).
- **Vite:** `VITE_WS_URL` ya existe en `.env.example`. En desarrollo apunta al servicio realtime: `ws://localhost:3002/ws`. La API sigue en `:3001`.

---

## 3. Cómo queda todo: flujo completo y puntos de entrada

```
 PANTALLA (usuario A)                       API (Go / Echo)                                    PostgreSQL
 ─────────────────────                      ────────────────────────────────────────────       ───────────────────────
 clic «Despachar traslado»
   └─ thunk ─ HTTP PUT /transfers/42/dispatch ─►  route (RequirePermission transfers:edit)
                                                 controller
                                                 service ── BEGIN tx ───────────────────────►  UPDATE transfers ...
                                                   │  inventoryRepo.DeductStock ...  ───────►  UPDATE stock ... RETURNING
                                                   │  [E2] events.Stock(row)                   (se acumula en el buffer)
                                                   │  auditRepo.Insert ...  ────────────────►  INSERT audit_logs
                                                   │  [E1] events.Record(transfer.status_changed)
                                                   └─ [E3] events.Flush(tx) ────────────────►  INSERT realtime_events (seq 9001, 9002)
                                                                                               SELECT pg_notify('erp_events','9002')
                                                       COMMIT ─────────────────────────────►  (el NOTIFY se entrega aquí)
   ◄─ 200 { transfer actualizado } ────────────
   reducer aplica la respuesta (versión v7)
 ══════════════════════════════════════ importadoraRealtime (servicio aparte, :3002) ═════════════════════════════
                                                 [E4] listener LISTEN erp_events  ◄────────── NOTIFY '9002'
                                                       lee seq > 9000 en lote (usuario BD de solo lectura)
                                                 [E5] hub: canal → filtro por permiso → cola por cliente
                                                 [E6] /ws escribe el sobre JSON
 ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 NAVEGADOR (A, B en bodega origen, C en bodega destino)
 [F1] realtime.service (socket)   recibe el texto
 [F2] realtime.adapter            valida el sobre v1 y el tipo  → RealtimeEvent tipado
 [F3] realtime.sequencer          controla seq (huecos → hello/resync) y descarta duplicados
 [F4] realtime.router             tipo → manejadores registrados por cada feature
 [F5] <feature>.realtime.ts       despacha reducers: transferUpserted, stockRowsChanged …
                                  (A ignora transfer v7 porque ya la tiene; aplica el stock)
 [F6] selectores + UI             tabla, drawer, KPI y selectores se actualizan; resaltado de fila;
                                  toast solo para C («Traslado TR-000042 va en camino a tu bodega»)
```

### 3.1 Puntos de entrada en el backend (por dónde **entra** la información al websocket)

> E1–E3 y la emisión del ticket viven en **`importadoraApi`** (paquete `core/events`). E4–E7 viven en **`importadoraRealtime`**.

| Punto | Dónde | Qué captura | Por qué ahí |
|---|---|---|---|
| **E1 · Cambios de entidad** | **Dentro** de la función que se pasa a `db.WithTx(ctx, pool, func(tx) error {…})` (`core/db/postgres.go:47`), justo después del `UPDATE`/`INSERT` que cambia la entidad. Se usa la auditoría solo como **mapa** para encontrar todas las mutaciones (75 llamadas a `auditRepo.Insert`). | Crear, editar, **cambiar estado**, anular, archivar en papelera, restaurar y etapas | **Corrección tras revisar el código:** 66 de las 75 auditorías se escriben **después** del commit, con `s.pool` y con el error ignorado (por ejemplo `inventory_transfer_flow_service.go:74` y `:184`). Si el evento se emitiera ahí, podría salir aunque algo fallara, o perderse. Por eso va dentro de la transacción. Recomendación aprovechando la misma pasada: mover también esas auditorías dentro de la transacción. |
| **E2 · Cantidades físicas** | Repositorios que escriben `stock`: `inventory/repositories/inventory_repo.go` (`UpsertStockDelta`, `SetStockAvailable`, `ReserveStock`, `ReleaseReservation`, `DeductStock`), `returns/repositories/return_repo.go` y `warehouses/repositories/warehouse_location_repo.go` | Cada fila de stock que cambia, con sus valores finales | **Solo** estos tres archivos tocan la tabla `stock`. Todos devuelven la fila (`RETURNING`) o se ajustan para hacerlo. El repositorio entrega la fila al buffer de eventos: `events.Stock(row, cause)`. La causa (`movement_type`, `reference_type`, `reference_id`) sale del mismo `RecordMovement` del kardex (`audit_movements`). |
| **E3 · Cierre de la transacción** | **API:** `importadoraApi/src/core/events/buffer.go`, ligado al `context` de la transacción mediante el gancho de `db.WithTx` | Junta, agrupa (última versión por `stockId`) y guarda todo en `realtime_events` + `pg_notify` **antes del COMMIT** | Así nada se emite si la transacción falla. |
| **E4 · Listener** | **Realtime:** `importadoraRealtime/src/features/events/services/listener.go` (conexión dedicada con `LISTEN erp_events`) | Los `seq` confirmados | Funciona con una o varias instancias del servicio realtime, sin tocar la API. |
| **E5 · Hub** | **Realtime:** `importadoraRealtime/src/core/hub/` | Canales, filtro por permiso y colas por cliente | Un único lugar decide **quién** recibe cada evento. |
| **E6 · Conexión** | **API:** `POST /realtime/ticket` (`importadoraApi/src/features/realtime`, solo emite tickets). **Realtime:** `GET /ws` (`importadoraRealtime/src/features/connection`) | Autenticación, `hello`/recuperación, ping y cierre | Cada lado sigue su estructura de feature (`routes/controllers/services/repositories`). |
| **E7 · Sesión y seguridad** | **API** emite los eventos (`auth`: logout y revocar sesión; `staff`: desactivar empleado y guardar permisos; `companies` / `warehouses`: desactivar). **Realtime** los aplica a las conexiones abiertas. | `session.revoked`, `permissions.changed`, `account.status_changed` | El hub **cierra** las conexiones de una sesión revocada o de una cuenta inactiva y **vuelve a leer** los permisos de la conexión desde la BD cuando cambian. |

### 3.2 Puntos de entrada en el front (dónde **llega** y a qué afecta)

| Punto | Dónde | Responsabilidad |
|---|---|---|
| **F1 · Conexión** | `features/realtime/services/realtime.service.ts` (ya existe) | Abrir con ticket, reconectar con la espera creciente actual (`realtime.backoff.ts`) y enviar `hello { lastSeq }`. |
| **F2 · Adaptador** | `features/realtime/helpers/realtime.adapter.ts` (ya existe) | **Único** lugar que conoce el formato del servidor. Valida `v`, `type` y el cuerpo con guardas de tipo, y descarta lo inválido. Se retiran los nombres heredados `tipo`/`cuerpo`/`idCliente`. |
| **F3 · Secuenciador** | `features/realtime/helpers/realtime.sequencer.ts` (nuevo) | Guarda `lastSeq` y descarta repetidos (`seq <= lastSeq`). **No** detecta huecos: los saltos de `seq` son normales (filtro por canal y permiso, rollbacks); `system.watermark` avanza `lastSeq` y la recuperación es `hello { lastSeq }`. Ante `system.resync_required`, marca los slices como desactualizados. |
| **F4 · Enrutador** | `features/realtime/helpers/realtime.router.ts` (reemplaza `realtime.handlers.ts`) | Tabla `tipo → manejadores`, llena con lo que registra `app/store`. Un evento puede tener varios manejadores (por ejemplo `stock.changed` → tienda, inventario, traslados y carrito). |
| **F5 · Manejadores por feature** | `features/<feature>/realtime/<feature>.realtime.ts` (nuevo en cada feature), exportados por su `index.ts` | Convierten el evento en acciones de su slice: `upsert`, `statusChanged`, `removed` y `stockRowsChanged`. **No** llaman HTTP, salvo `invalidate` cuando el evento es solo un aviso. |
| **F6 · Interfaz** | Selectores existentes + `global/hooks/ui/useRecentlyChanged.ts` (nuevo) | Muestran el cambio y lo resaltan brevemente. En el drawer, desactivan las acciones que ya no aplican. Los toasts solo salen cuando el evento le compete al usuario. |
| **F7 · Estado de la conexión** | `features/realtime/components/RealtimeStatus.tsx`, montado en el `AppHeader` existente (no es un componente nuevo de `app/`) | Punto discreto: conectado / reconectando / sin conexión. Mientras no hay conexión, se revalida al volver a la pestaña. Sin botón «actualizar». |

---

## 4. Cómo se representa cada caso en la interfaz

### 4.1 Activo ⇄ inactivo (cualquier entidad)

Evento único: **`entity.status_changed`**, con `from`, `to`, `reason` y quién lo hizo. El estado se **normaliza** en el contrato a `active | inactive` (más otros valores propios como `discontinued` o `overdue`), aunque en BD unas tablas usan `is_active` y otras `status`.

| Dónde se ve | Qué pasa al llegar el evento |
|---|---|
| Tabla de la entidad | La fila cambia su `StatusSelect` y se resalta 1,5 s. Si el filtro activo es «Activos», la fila sale de la lista y el contador de la paginación se ajusta. |
| Drawer abierto de esa entidad | Cambia el estado mostrado. Si es un formulario de edición: aviso «Ana la desactivó: *motivo*» y las acciones que exigen «activo» se deshabilitan con tooltip. |
| Selectores de otros flujos (cliente en checkout, producto en catálogo y carrito, bodega en traslados, empleado vendedor, proveedor en compras) | Lo inactivo **desaparece** de las opciones. Si ya estaba elegido en un borrador (carrito o traslado), la línea se marca «no disponible» y no deja confirmar. |
| Producto inactivo en un carrito abierto | La línea se marca; la API ya lo rechaza al confirmar, y el front lo muestra antes. |
| La propia cuenta, su compañía o su bodega | Llega `account.status_changed` por el canal del actor, la conexión se cierra y aparece el mensaje de cuenta no disponible (el mismo de hoy: `ACCOUNT_UNAVAILABLE`). |

### 4.2 Cantidades físicas

Evento **`stock.changed`**, en lote, por fila de `stock` (bodega + ubicación), con valores **absolutos**, más el total por producto y bodega para no recalcular en el cliente.

| Dónde se ve | Qué pasa |
|---|---|
| Catálogo de la tienda (`shop.products`, `stockByWarehouse`) | Se fija el vendible por bodega. Si llega a 0, la tarjeta pasa a «agotado» y deshabilita agregar. |
| Carrito abierto | Si la cantidad pedida supera el nuevo vendible: aviso en la línea («quedan 2») y no deja confirmar. |
| Inventario (`StockScreen`), ubicaciones y conteos | La fila de la ubicación se actualiza y resalta. KPIs de bajo stock (`lowStock`: vendible del producto en la bodega < su mínimo por producto y bodega, o sin vendible; ver CHANGELOG del contrato 2026-11-13). |
| Traslados (`WarehouseProductsTable`, borrador) | El disponible del origen se actualiza; el borrador valida contra el nuevo valor. |
| Detalle de producto (drawer) | Se actualiza la tabla de ubicaciones. |

`cause` dice **por qué** cambió (`sale_out`, `transfer_out`, `transfer_in`, `purchase_in`, `return_in`, `adjustment`, `count_adjust`, `reservation`, `release` o `relocation`) y a qué documento pertenece. Así la interfaz puede enlazarlo («−3 por pedido PED-000881») y el kardex abierto agrega la línea.

### 4.3 Traslados entre bodegas

Estados de BD: `pending → in_transit → received | with_novelty`, y `cancelled` desde `pending`.

| Momento | Eventos emitidos (en la misma transacción) | Canales |
|---|---|---|
| Crear solicitud | `transfer.created` | bodega origen + bodega destino |
| Despachar | `transfer.status_changed (pending→in_transit)` + `stock.changed` en el origen (`transfer_out`) | origen + destino |
| Recibir completo | `transfer.status_changed (in_transit→received)` con `receivedQty` por línea + `stock.changed` en el destino (`transfer_in`) | origen + destino |
| Recibir con faltantes | `transfer.status_changed (→with_novelty)` con `noveltyNotes` y las líneas con diferencia | origen + destino |
| Cancelar | `transfer.status_changed (→cancelled)` con `cancelReason` (+ `stock.changed` si se liberaron reservas) | origen + destino |

En la interfaz:
- La lista de traslados de ambas bodegas mueve el registro de pestaña o filtro y lo resalta.
- El drawer de detalle abierto cambia sus botones según el nuevo estado: quien tenía «Despachar» lo ve deshabilitado con «Despachado por Luis».
- El destino recibe un toast cuando el traslado sale hacia su bodega; el origen, cuando fue recibido o tuvo novedad.

### 4.4 Pedido, caja, pagos y demás documentos

Todos siguen el patrón `<documento>.created` + `<documento>.status_changed` (o `sale.stage_changed`), con el catálogo completo en la sección 6.3, más los `stock.changed` y `credit.balance_changed` que la misma transacción provoque:

- **Pedido:** etapas `verification … sent / cancelled` (`sales.current_stage`). La tabla mueve el pedido de etapa; quien tenía el pedido abierto en una etapa que ya avanzó ve sus acciones deshabilitadas.
- **Factura:** `invoice.issued` / `invoice.annulled`.
- **Pago o abono:** `payment.status_changed` (`review → verified | issue | voided`) y conciliación bancaria (`pending → confirmed | ignored`).
- **Caja:** `cash.session_opened` / `cash.session_closed` / `cash.expense_recorded`.
- **Compras:** `draft → ordered → partial → received | voided`, más `stock.changed` (`purchase_in`) al recibir.
- **Devoluciones:** `pending → approved | rejected`, más `stock.changed` (`return_in`) y `credit.balance_changed` (saldo a favor).
- **Cotizaciones:** `draft … converted`.
- **Conteos:** `pending → verified → corroborated` (`count.status_changed`), el avance en `count.progress_changed` y, al corroborar, `stock.changed` (`count_adjust`).
- **Notificaciones:** `notification.created` reemplaza el sondeo de `useNotifications.ts:54`.

---

## 5. Base de datos

Migración `importadoraApi/src/core/db/migrations/2026112X_realtime-events.sql` (fecha posterior a la última existente) **y** el mismo bloque en `startdb.pgsql`:

```sql
-- realtime_events · eventos confirmados que el websocket reparte; se escriben en la misma transacción del cambio
CREATE TABLE IF NOT EXISTS realtime_events (
    seq             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id              UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tx_id           xid8 NOT NULL DEFAULT pg_current_xact_id(),          -- transacción que lo creó: el listener solo lee lo ya visible para todos (sección 14.7)
    company_id      BIGINT REFERENCES companies(id) ON DELETE CASCADE,   -- NULL: evento de plataforma (superadmin)
    warehouse_ids   BIGINT[] NOT NULL DEFAULT '{}',                      -- bodegas afectadas (canal warehouse:{id})
    actor_channel   VARCHAR(60),                                         -- 'employee:12' para eventos personales
    type            VARCHAR(60) NOT NULL,
    permissions     TEXT[] NOT NULL DEFAULT '{}',                        -- basta con uno: {'catalog:view','stock:view'} (filtro del hub)
    actor_type      VARCHAR(20) NOT NULL,
    actor_id        BIGINT,
    actor_name      VARCHAR(150) NOT NULL DEFAULT '',                   -- «Ana Pérez», para mostrar quién hizo el cambio
    platform        BOOLEAN NOT NULL DEFAULT FALSE,                     -- también al canal platform (superadmins)
    correlation_id  VARCHAR(64),
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_realtime_events_company_seq ON realtime_events (company_id, seq);
CREATE INDEX IF NOT EXISTS idx_realtime_events_created ON realtime_events (created_at);
```

En la misma migración, la tabla de tickets (la escribe la API y la consume el servicio realtime):

```sql
-- realtime_tickets · tickets de un solo uso para abrir el websocket; la API los emite y importadoraRealtime los consume
CREATE TABLE IF NOT EXISTS realtime_tickets (
    ticket_hash     CHAR(64) PRIMARY KEY,                                  -- sha256 del ticket; el ticket en claro nunca se guarda
    actor_type      VARCHAR(20) NOT NULL CHECK (actor_type IN ('employee', 'admin', 'superadmin')),
    actor_id        BIGINT NOT NULL,
    company_id      BIGINT REFERENCES companies(id) ON DELETE CASCADE,
    warehouse_id    BIGINT REFERENCES warehouses(id) ON DELETE CASCADE,
    session_id      BIGINT NOT NULL REFERENCES refresh_tokens(id) ON DELETE CASCADE,
    expires_at      TIMESTAMPTZ NOT NULL,
    used_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_realtime_tickets_expires ON realtime_tickets (expires_at);
```

Y el usuario de base de datos del servicio realtime (mínimo privilegio). La contraseña **no** va en el script: se define al desplegar.

```sql
-- importadora_realtime · usuario del servicio websocket: lee eventos, sesiones y permisos; solo marca tickets y purga eventos
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'importadora_realtime') THEN
    CREATE ROLE importadora_realtime LOGIN;
  END IF;
END $$;
GRANT SELECT, DELETE ON realtime_events TO importadora_realtime;
GRANT SELECT, UPDATE (used_at) ON realtime_tickets TO importadora_realtime;
GRANT DELETE ON realtime_tickets TO importadora_realtime;
GRANT SELECT ON refresh_tokens, employees, admins, superadmins, employee_permissions, permissions, companies, warehouses TO importadora_realtime;
```

Más:
- **`updated_at` como versión.** Toda tabla que emita eventos debe tener `updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP` y un trigger común `set_updated_at()` (`BEFORE UPDATE`). Hoy algunas solo tienen `created_at` (por ejemplo `brands` y `models`; revisa una por una). Se agrega en la misma migración y en `startdb.pgsql`.
- **Retención.** La hace el **servicio realtime**: al arrancar y cada hora borra eventos con más de **48 h** (`DELETE … WHERE created_at < now() - interval '48 hours'`) y tickets vencidos.
- **Variables nuevas:**
  - `importadoraApi/.env.example`: `REALTIME_TICKET_TTL_SEC=30` y `REALTIME_WS_URL=ws://localhost:3002/ws` (se devuelve con el ticket).
  - `importadoraRealtime/.env.example` (nuevo): `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USER=importadora_realtime`, `DB_PASSWORD`, `APP_ENV`, `APP_PORT=3002`, `REALTIME_ALLOWED_ORIGINS=http://localhost:5173`, `REALTIME_RETENTION_HOURS=48`, `REALTIME_MAX_CLIENT_QUEUE=256`, `REALTIME_PING_SEC=25` y `REALTIME_MAX_REPLAY=5000` (con un hueco mayor se manda `resync_required`).
  - `importadoraInterfaz/.env.example`: `VITE_WS_URL=ws://localhost:3002/ws`.

---

## 6. Catálogo completo de eventos y contrato TypeScript (front)

> Este catálogo sale de **todas las rutas que modifican datos** en `importadoraApi/src/features/*/routes` (revisadas una por una) y de los estados que define `startdb.pgsql`. Si una ruta modifica datos y no aparece aquí, el catálogo está incompleto: la prueba guardia de la sección 9 lo detecta.

### 6.1 Canales

| Canal | Quién queda suscrito (lo decide el servidor al conectar) | Qué viaja |
|---|---|---|
| `platform` | Superadmins (es su **único** canal de datos, decisión 2) | Compañías, administradores, respaldos y protector de pantalla de la plataforma |
| `company:{id}` | Todos los actores de la compañía. El superadmin **nunca** (no va atado a ninguna compañía). | Datos maestros, stock, precios, cartera y documentos de la compañía |
| `warehouse:{id}` | Empleados de esa bodega y admins de la compañía (todas sus bodegas) | Pedidos, traslados (origen **y** destino), caja, conteos y ubicaciones |
| `actor:{tipo}:{id}` | Solo esa persona (todas sus pestañas) | Su carrito, sus notificaciones, sus permisos, su cuenta y el cierre de su sesión |

Reglas:
- Un evento va a **uno o más** canales (`companyId`, `warehouseIds` y `actorChannel` del sobre).
- Después del canal se aplica el **filtro de permisos** (`permissions`, basta con tener uno).
- Los admins pasan todos los permisos de su compañía; el superadmin, solo los de lectura (igual que `RequirePermission` en la API).

### 6.2 Convención de nombres

- `<dominio>.<hecho_en_pasado>`, en inglés y en minúscula: `transfer.status_changed`, `stock.changed`.
- **Datos maestros** (entidades sin flujo de estados propio) usan los eventos genéricos `entity.*` con `entity.kind`. Así una tabla nueva de catálogo no necesita tipos nuevos.
- **Documentos con flujo** (pedido, traslado, compra, pago…) tienen eventos propios, porque llevan datos del flujo (líneas, montos, quién despachó o recibió).
- Todo evento con entidad lleva `entity.version` (`updated_at`) para descartar duplicados y eventos viejos.

### 6.3 Catálogo completo

#### A · Sistema y protocolo (no salen de la BD; los genera `importadoraRealtime`)

| Tipo | Cuándo | Canal | Cuerpo |
|---|---|---|---|
| `system.welcome` | Al aceptar la conexión | (solo esa conexión) | `currentSeq`, `channels`, `heartbeatSec`, `contractVersion` |
| `system.replay_completed` | Al terminar de reenviar los eventos pendientes tras un `hello` | (solo esa conexión) | `fromSeq`, `toSeq`, `count` |
| `system.resync_required` | Hueco mayor que `REALTIME_MAX_REPLAY`, contrato nuevo o **respaldo restaurado** | conexión o `company:{id}` / `platform` | `reason: 'gap_too_large' \| 'contract_changed' \| 'backup_restored'`, `currentSeq` |
| `system.watermark` | Cada `REALTIME_WATERMARK_SEC` si avanzó lo procesado (14.7) | (cada conexión `live`) | `seq` (el cliente avanza su `lastSeq` aunque no haya recibido eventos) |

#### B · Sesión y acceso (canal `actor:{tipo}:{id}`)

| Tipo | Lo emite (ruta de la API → servicio) | Cuerpo | Efecto en el front |
|---|---|---|---|
| `session.revoked` | `POST /auth/logout` (otras pestañas de esa sesión); `DELETE /employees/:id`, `/admins/:id`, `/companies/:id` y `/warehouses/:id` (desactivar); `PUT /employees/:id/credentials` y `PUT /auth/me/password` (cambio de contraseña) | `sessionId \| null`, `exceptSessionId \| null` (con `PUT /auth/me/password` la API deja viva la sesión que cambió la clave: esas conexiones **no** se cierran), `reason: 'logout' \| 'password_changed' \| 'account_inactive' \| 'company_inactive' \| 'warehouse_inactive'` | El servicio realtime **cierra** el socket (código 4401) y el front cierra la sesión con el aviso actual |
| `permissions.changed` | `PUT /employees/:id/permissions` | `actorType`, `actorId`, `permissions: string[]` | El servicio realtime recalcula los permisos de la conexión; el front actualiza `auth.permissions`, menú, acciones y guardias sin cerrar sesión |
| `account.updated` | `PUT /employees/:id`, `PUT /admins/:id` y `PUT /auth/me` (Fase 7) sobre la propia cuenta | `user` (misma forma que `GET /auth/me`), `version` | Encabezado, perfil y configuración muestran los datos nuevos |

#### C · Datos maestros (genéricos `entity.*`)

| Tipo | Lo emiten | Cuerpo |
|---|---|---|
| `entity.created` | Todo `POST` de creación de la tabla de abajo | `entity`, `row` (misma forma que `GET /:id`) |
| `entity.updated` | Todo `PUT` de edición (sin cambio de estado) | `entity`, `row`, `changedFields` |
| `entity.status_changed` | `PUT` con `status`/`isActive` y los `DELETE` que **desactivan** | `entity`, `from`, `to`, `reason`, `row` |
| `entity.archived` | `DELETE` que manda a la papelera (`recycle_bin`) | `entity`, `reason`, `recycleBinId` |
| `entity.restored` | `POST /recycle-bin/:id/restore` | `entity`, `row`, `recycleBinId`, `restoredBy` |
| `entity.deleted` | Borrados físicos de subregistros (códigos, ubicaciones, documentos y producto de proveedor) | `entity`, `parent` (`{kind, id}`) |

`entity.kind` posibles, con su ruta, canal y permiso:

| `kind` | Rutas que lo modifican | Canal | Permisos (cualquiera) |
|---|---|---|---|
| `company` | `POST/PUT/DELETE /companies` | `platform` + `company:{id}` | superadmin / (todos los de la compañía para `status_changed`) |
| `admin` | `POST/PUT/DELETE /admins` | `platform` + `company:{id}` | superadmin / admin |
| `warehouse` | `POST/PUT/DELETE /warehouses` | `company:{id}` | `warehouses:view`, `transfers:view`, `stock:view` |
| `warehouse_billing` | `PUT /warehouses/:id/facturation` | `company:{id}` | `warehouses:view`, `invoices:view` |
| `doc_series` | `PUT /doc-series/:id` | `company:{id}` | `warehouses:view` |
| `location` | `POST/PUT/DELETE /warehouses/:id/locations` | `warehouse:{id}` | `stock:view`, `transfers:view`, `transfers:create`, `products:view`, `purchases:view` |
| `customer` | `POST/PUT/DELETE /customers` | `company:{id}` | `customers:view`, `orders:create`, `quotations:view`, `credits:view` |
| `customer_document` | `POST /customers/:id/documents`, `…/renew`, `DELETE …/documents/:docId` | `company:{id}` | `customers:view` |
| `supplier` / `carrier` | `POST/PUT/DELETE /externals` (según su tipo) | `company:{id}` | `suppliers:view`, `purchases:view` |
| `brand` / `category` / `model` | `POST/PUT/DELETE /brands`, `/categories`, `/models` | `company:{id}` | `brands:view` / `categories:view` / `models:view`, `products:view`, `catalog:view` |
| `product` | `POST/PUT/DELETE /products` | `company:{id}` | `products:view`, `catalog:view`, `stock:view`, `transfers:view`, `purchases:view` |
| `product_code` | `POST/DELETE /products/:id/codes` | `company:{id}` | `products:view`, `catalog:view` |
| `product_supplier` | `POST/DELETE /products/:id/suppliers`, `PUT/DELETE /purchases/suppliers/:supplierId/products` | `company:{id}` | `products:view`, `purchases:view` |
| `role` | `POST/PUT/DELETE /roles` | `company:{id}` | `roles:view`, `employees:view` |
| `employee` | `POST/PUT/DELETE /employees` | `company:{id}` (+ `actor:employee:{id}` si es él mismo, ver B) | `employees:view` |
| `screensaver_settings` | `POST/PUT /settings/screensaver` | `platform` y/o `company:{id}` | (sin permiso: todos lo usan) |

#### D · Precios

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `product.prices_changed` | `PUT /products/:id` cuando cambian `product_prices` (y la edición de precios del catálogo) | `company:{id}` | `catalog:view`, `products:view`, `quotations:view` | `productId`, `prices: {p1..p4}`, `ivaRate`, `version` |

Efecto: catálogo, tarjetas y **carritos abiertos** (las líneas ya agregadas conservan su precio; se muestra un aviso si el precio de lista cambió antes de crear el pedido).

#### E · Inventario

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `stock.changed` | Todo escritor de stock (sección 12.1): ventas, traslados, ajustes (`POST /stock/adjust`), asignación (`/stock/assign`), reubicación (`/stock/relocate`), conteos, compras, devoluciones y stock inicial | `company:{id}` | `catalog:view`, `stock:view`, `transfers:view`, `products:view` | `cause`, `rows[]` (valores finales), `totals[]` |
| `transfer.created` | `POST /transfers` (uno por bodega de origen) | `warehouse:{origen}` + `warehouse:{destino}` | `transfers:view` | `transfer` (fila completa con líneas) |
| `transfer.status_changed` | `POST /transfers/:id/dispatch`, `/receive` y `/cancel` | `warehouse:{origen}` + `warehouse:{destino}` | `transfers:view` | `entity`, `serial`, `from`, `to` (`pending \| in_transit \| received \| with_novelty \| cancelled`), `lines[]` con `receivedQty`, `noveltyNotes`, `cancelReason`, `dispatchedBy`, `receivedBy` |
| `count.created` | `POST /counts` | `warehouse:{id}` | `stock:view` | `count` (fila completa) |
| `count.progress_changed` | `PUT /counts/:id/items` | `warehouse:{id}` | `stock:view` | `entity`, `items[]` (`productId`, `expectedQty`, `countedQty`), `countedLines`, `totalLines` |
| `count.status_changed` | `POST /counts/:id/verify` y `/corroborate` | `warehouse:{id}` | `stock:view` | `entity`, `from`, `to` (`pending \| verified \| corroborated`), `differences[]` |

#### F · Ventas: carrito, cotizaciones, pedidos y facturas

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `cart.updated` | `POST /carts`, `DELETE /carts/:id`, `POST/PUT/DELETE /carts/:id/items` | `actor:{tipo}:{id}` (solo su dueño) | — | `cart` (misma forma que la respuesta del carrito). Sincroniza las otras pestañas del mismo usuario. |
| `quotation.created` | `POST /quotations` | `warehouse:{id}` | `quotations:view` | `quotation` |
| `quotation.updated` | `PUT /quotations/:id` (líneas, cliente o vigencia) | `warehouse:{id}` | `quotations:view` | `entity`, `row`, `changedFields` |
| `quotation.status_changed` | Conversión a pedido y vencimiento a los 30 días (estados `pending \| converted \| expired`, 2026-11-13) | `company:{id}` | `quotations:view` | `entity`, `from`, `to` (`draft \| sent \| accepted \| rejected \| expired \| converted`), `saleId \| null` |
| `sale.created` | `POST /sales` | `warehouse:{id}` | `orders:view`, `invoices:view`, `cash:view` | `sale` (fila de la tabla de pedidos) |
| `sale.stage_changed` | `POST /sales/:id/verify`, `/verify-payment`, `/pick`, `/pack`, `/dispatch`, `/send` y `/cancel` | `warehouse:{id}` | `orders:view`, `cash:view` | `entity`, `serial`, `from`, `to` (`verification … sent \| cancelled`), `status` (`open \| completed \| cancelled`), `reason`, `takenBy` |
| `sale.payment_changed` | Pagos que afectan al pedido (`POST /payments`, `/batch`, `/verify`, `/void`, `/reject`, `/reopen`) | `warehouse:{id}` | `orders:view`, `cash:view` | `saleId`, `paidTotal`, `creditAmount`, `balance`, `paymentVerified`, `version` |
| `invoice.issued` | `POST /sales/:id/invoice` | `warehouse:{id}` | `invoices:view`, `orders:view` | `invoice` (serie, número DIAN, totales y `saleId`) |
| `invoice.annulled` | `POST /invoices/:id/annul` | `warehouse:{id}` | `invoices:view`, `orders:view` | `entity`, `saleId`, `reason` |

#### G · Pagos, cartera y conciliación

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `payment.created` | `POST /payments` y `POST /payments/batch` (uno por pago) | `company:{id}` | `payments:view`, `credits:view` | `payment` (fila de Abonos) |
| `payment.status_changed` | `POST /payments/:id/verify`, `/reject`, `/void` y `/reopen` | `company:{id}` | `payments:view`, `credits:view` | `entity`, `from`, `to` (`review \| verified \| issue \| voided`), `reason` |
| `credit.balance_changed` | Ventas a crédito, pagos, devoluciones con saldo a favor y `POST /wallet/adjust` | `company:{id}` | `credits:view`, `customers:view`, `orders:create` | `customerId`, `creditLimit`, `used`, `available`, `favorBalance`, `status` (`ok \| warning \| blocked`), `entry` (`charge \| payment \| return \| adjust`, monto y referencia), `version` |
| `bank_statement.imported` | `POST /bank-statements/upload-pdf` | `company:{id}` | `payments:view` | `statementId`, `bank`, `recordsCreated`, `period` (aviso: la tabla vuelve a pedir **una** vez su página visible) |
| `bank_record.status_changed` | `PATCH /bank-statements/records/:id/confirm` y `/ignore` | `company:{id}` | `payments:view` | `entity`, `from`, `to` (`pending \| confirmed \| ignored`), `paymentId \| null` |

#### H · Caja

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `cash_session.status_changed` | `POST /cash/sessions/open` y `/close` | `warehouse:{id}` | `cash:view`, `invoices:view` | `entity`, `from`, `to` (`open \| closed`), `openedBy`, `closedBy`, `totals` |
| `cash_session.totals_changed` | Pagos en efectivo o transferencia dentro de una caja abierta, gastos y pagos de compras desde caja | `warehouse:{id}` | `cash:view` | `sessionId`, `cashIn`, `transferIn`, `expenses`, `expectedCash`, `version` |
| `cash.expense_recorded` | Registro de gasto de caja (incluye `POST /purchases/:id/pay`, que deja el gasto `merchandise_arrival`) | `warehouse:{id}` | `cash:view`, `invoices:view` | `expense` (`expenseType`, `cashAmount`, `transferAmount`, concepto y referencia) |

#### I · Compras y proveedores

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `purchase.created` | `POST /purchases` | `warehouse:{id}` | `purchases:view` | `purchase` |
| `purchase.status_changed` | `POST /purchases/:id/receive` y `/void` | `warehouse:{id}` | `purchases:view` | `entity`, `from`, `to` (`draft \| ordered \| partial \| received \| voided`), `lines[]` con lo recibido, `reason` |
| `purchase.payment_recorded` | `POST /purchases/:id/pay` | `warehouse:{id}` | `purchases:view`, `cash:view` | `purchaseId`, `paid`, `balance`, `cashAmount`, `transferAmount` |
| `supplier_catalog.imported` | `POST /purchases/suppliers/:supplierId/catalog/upload` | `company:{id}` | `purchases:view`, `products:view` | `supplierId`, `linked`, `warnings` (aviso de invalidación) |

#### J · Devoluciones y garantías

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `return.created` | `POST /returns` | `warehouse:{id}` | `returns:view` | `return` |
| `return.status_changed` | `POST /returns/:id/approve` y `/reject` | `warehouse:{id}` | `returns:view`, `orders:view` | `entity`, `from`, `to` (`pending \| approved \| rejected`), `resolution` (`exchange \| credit_balance`), `rejectReason` |

(La aprobación también emite, en la misma transacción, `stock.changed` (`return_in`) y, si aplica, `credit.balance_changed`.)

#### K · Notificaciones, auditoría y respaldos

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `notification.created` | Todo `INSERT INTO notifications` (por ejemplo, restaurar un registro o un traslado entrante) | `actor:{tipo}:{id}` del destinatario | — | `notificationId`, `kind` (`info \| success \| warning \| error \| restore_record`), `title`, `message`, `link` |
| `notification.read` | `POST /notifications/:id/read` y `/read-all` | `actor:{tipo}:{id}` | — | `ids[] \| 'all'` (sincroniza el contador en todas las pestañas) |
| `audit.logged` | Todo `auditRepo.Insert` (resumen, no el detalle) | `company:{id}` + `platform` | `audit:view` | `auditId`, `entity`, `entityId`, `action`, `actor`, `reason` (alimenta la tabla de Auditoría abierta) |
| `backup.created` | `POST /backups` | `platform` | superadmin | `backup` (nombre, tipo, tamaño y fecha) |
| `backup.restored` | `POST /backups/:id/restore` | `platform` | superadmin | `backupId`. Va acompañado de `system.resync_required (backup_restored)` a **todas** las conexiones (reemplaza el `window.location.reload()` de `useBackups.ts:73`). |

#### Lo que **no** emite eventos (y por qué)

| Ruta | Motivo |
|---|---|
| `POST /auth/login`, `/auth/refresh` | No cambian datos que otros vean (el intento fallido no se publica) |
| `POST /audit-logs/context-change` | Lo hace el propio superadmin y solo deja la auditoría. No cambia sus canales: el superadmin sigue solo en `platform` (decisión 2), así que no pide ticket nuevo ni reconecta |
| `GET` de reportes (`/reports/*`) | Los reportes se marcan como desactualizados en el front cuando llegan eventos de venta, pago o stock, y se recalculan **una vez** si están en pantalla |
| `POST /employees/:id/unlock` | Se retira en la Fase 2.4 del plan principal |
| Vencimiento de documentos de cliente (por tiempo, sin ruta) | No hay mutación. Fase posterior: tarea programada en la API que emita `customer_document.expiring` |

### 6.4 Quién consume cada evento en el front (matriz de manejadores)

| Feature (`features/<x>/realtime/<x>.realtime.ts`) | Eventos que maneja |
|---|---|
| `auth` | `session.revoked`, `permissions.changed`, `account.updated` |
| `navigation` | `permissions.changed` (re-evalúa la pantalla activa), `entity.status_changed (warehouse)` (bodega activa desactivada) |
| `warehouses` | `entity.* (warehouse, location, warehouse_billing, doc_series)`, `stock.changed` (ocupación de ubicaciones) |
| `locations` | `entity.* (location)`, `stock.changed` |
| `shop` (catálogo y carrito) | `stock.changed`, `product.prices_changed`, `entity.* (product, product_code, brand, category, model, customer)`, `cart.updated` |
| `quotations` | `quotation.*`, `product.prices_changed`, `entity.status_changed (product, customer)` |
| `checkout` | `entity.status_changed (customer)`, `credit.balance_changed`, `stock.changed` |
| `orders` | `sale.*`, `invoice.*`, `return.status_changed` |
| `cashRegister` / `billing` / `cashExpenses` | `cash_session.*`, `cash.expense_recorded`, `sale.*`, `invoice.*`, `entity.* (warehouse_billing, doc_series)` |
| `inventory` | `stock.changed`, `entity.* (product, product_code, product_supplier)`, `count.*` |
| `catalogs` | `entity.* (brand, category, model)` |
| `transfers` | `transfer.*`, `stock.changed`, `entity.status_changed (warehouse, location, product)` |
| `purchases` | `purchase.*`, `supplier_catalog.imported`, `entity.* (supplier, product_supplier, location)` |
| `returns` | `return.*` |
| `customers` | `entity.* (customer, customer_document)`, `credit.balance_changed` |
| `wallet` | `payment.*`, `credit.balance_changed`, `entity.status_changed (customer)` |
| `bank` | `bank_statement.imported`, `bank_record.status_changed`, `payment.status_changed` |
| `employees` | `entity.* (employee, role)`, `permissions.changed` |
| `externals` | `entity.* (supplier, carrier)` |
| `companies` / `superadmin` | `entity.* (company, admin)` |
| `recyclebin` | `entity.archived`, `entity.restored` |
| `audit` | `audit.logged` |
| `notifications` | `notification.*` |
| `backups` | `backup.*`, `system.resync_required` |
| `reports` | `sale.*`, `payment.*`, `stock.changed`, `credit.balance_changed` (solo marcan «desactualizado») |
| `settings` / `screensaver` | `entity.* (company, screensaver_settings)`, `account.updated` |

### 6.5 Contrato TypeScript completo

Archivo: `importadoraInterfaz/src/features/realtime/types/realtime.contract.ts`

```ts
// realtime.contract · contrato v1 del websocket: sobre, canales y TODOS los tipos de evento con su cuerpo
// conecta con: realtime.adapter (valida contra esto), realtime.router y los manejadores *.realtime.ts de cada feature
// ojo: debe coincidir campo por campo con importadoraRealtime/contract (JSON Schema) y con importadoraApi/src/core/events/types.go

// ── piezas comunes ───────────────────────────────────────────────────────────────
export type ActorKind = 'employee' | 'admin' | 'superadmin' | 'system'
export interface RealtimeActor { type: ActorKind; id: number | null; name: string }

export type EntityKind =
  | 'company' | 'admin' | 'warehouse' | 'warehouse_billing' | 'doc_series' | 'location'
  | 'customer' | 'customer_document' | 'supplier' | 'carrier'
  | 'brand' | 'category' | 'model' | 'product' | 'product_code' | 'product_supplier'
  | 'role' | 'employee' | 'screensaver_settings'

export type DocumentKind =
  | 'sale' | 'invoice' | 'quotation' | 'transfer' | 'count' | 'purchase' | 'return'
  | 'payment' | 'bank_record' | 'cash_session'

export interface EntityRef<K extends string = EntityKind | DocumentKind> {
  kind: K
  id: number
  version: string                    // updated_at ISO con microsegundos
}

export type EntityStatus = 'active' | 'inactive' | 'blocked' | 'pending' | 'overdue' | 'draft' | 'discontinued'   // blocked: employees (bloqueo por intentos fallidos)
export interface Money { amount: number; currency: 'COP' }

// ── sobre ─────────────────────────────────────────────────────────────────────────
export interface RealtimeEnvelope<T extends RealtimeEventType = RealtimeEventType> {
  v: 1
  seq: number                        // 0 en los mensajes system.* que no salen de la BD
  id: string
  type: T
  occurredAt: string
  companyId: number | null
  warehouseIds: number[]
  actor: RealtimeActor
  correlationId?: string
  data: RealtimeEventMap[T]
}

// ── A · sistema ───────────────────────────────────────────────────────────────────
export interface WelcomeData { currentSeq: number; channels: string[]; heartbeatSec: number; contractVersion: 1 }
export interface ReplayCompletedData { fromSeq: number; toSeq: number; count: number }
export interface ResyncRequiredData { reason: 'gap_too_large' | 'contract_changed' | 'backup_restored'; currentSeq: number }
export interface WatermarkData { seq: number }

// ── B · sesión y acceso ───────────────────────────────────────────────────────────
export interface SessionRevokedData {
  sessionId: number | null
  exceptSessionId: number | null   // sesión que sigue viva (la que cambió su clave con PUT /auth/me/password)
  reason: 'logout' | 'password_changed' | 'account_inactive' | 'company_inactive' | 'warehouse_inactive'
}
export interface PermissionsChangedData { actorType: ActorKind; actorId: number; permissions: string[] }
export interface AccountUpdatedData { user: Record<string, unknown>; version: string }   // forma de GET /auth/me

// ── C · datos maestros ────────────────────────────────────────────────────────────
export interface EntityCreatedData<R = Record<string, unknown>> { entity: EntityRef<EntityKind>; row: R }
export interface EntityUpdatedData<R = Record<string, unknown>> { entity: EntityRef<EntityKind>; row: R; changedFields: string[] }
export interface EntityStatusChangedData<R = Record<string, unknown>> {
  entity: EntityRef<EntityKind>; from: EntityStatus; to: EntityStatus; reason: string | null; row: R
}
export interface EntityArchivedData { entity: EntityRef<EntityKind>; reason: string; recycleBinId: number }
export interface EntityRestoredData<R = Record<string, unknown>> {
  entity: EntityRef<EntityKind>; row: R; recycleBinId: number; restoredBy: RealtimeActor
}
export interface EntityDeletedData { entity: EntityRef<EntityKind>; parent: { kind: EntityKind; id: number } | null }

// ── D · precios ───────────────────────────────────────────────────────────────────
export interface ProductPricesChangedData {
  productId: number
  prices: { p1: number; p2: number; p3: number; p4: number }
  ivaRate: number
  version: string
}

// ── E · inventario ────────────────────────────────────────────────────────────────
export type StockCause =
  | 'sale_out' | 'transfer_out' | 'transfer_in' | 'purchase_in' | 'return_in'
  | 'adjustment' | 'count_adjust' | 'reservation' | 'release' | 'relocation'

export interface StockRowChange {
  stockId: number; productId: number; productCode: string; warehouseId: number; location: string
  available: number; reserved: number; sellable: number; minStock: number; lowStock: boolean; version: string
}
export interface StockTotal { productId: number; warehouseId: number; available: number; reserved: number; sellable: number }
export interface StockChangedData {
  cause: {
    type: StockCause
    referenceType: 'sale' | 'transfer' | 'purchase' | 'return' | 'count' | 'manual' | null
    referenceId: number | null
    referenceSerial: string | null
  }
  rows: StockRowChange[]             // máximo 200 por mensaje
  totals: StockTotal[]
}

export type TransferStatus = 'pending' | 'in_transit' | 'received' | 'with_novelty' | 'cancelled'
export interface TransferLine {
  lineId: number; productId: number; productCode: string; productName: string
  quantity: number; receivedQty: number; destinationLocation: string
}
export interface TransferRow {
  id: number; serial: string; status: TransferStatus; isPriority: boolean; notes: string | null
  originWarehouseId: number; destinationWarehouseId: number; lines: TransferLine[]
  createdBy: RealtimeActor; createdAt: string; version: string
}
export interface TransferCreatedData { transfer: TransferRow }
export interface TransferStatusChangedData {
  entity: EntityRef<'transfer'>; serial: string; from: TransferStatus; to: TransferStatus
  originWarehouseId: number; destinationWarehouseId: number; lines: TransferLine[]
  noveltyNotes: string | null; cancelReason: string | null
  dispatchedBy: RealtimeActor | null; receivedBy: RealtimeActor | null
}

export type CountStatus = 'pending' | 'verified' | 'corroborated'
export interface CountItem { productId: number; productCode: string; location: string; expectedQty: number; countedQty: number | null }
export interface CountCreatedData { count: { id: number; serial: string | null; warehouseId: number; status: CountStatus; items: CountItem[]; version: string } }
export interface CountProgressChangedData { entity: EntityRef<'count'>; items: CountItem[]; countedLines: number; totalLines: number }
export interface CountStatusChangedData {
  entity: EntityRef<'count'>; from: CountStatus; to: CountStatus
  differences: Array<{ productId: number; location: string; expectedQty: number; countedQty: number }>
}

// ── F · ventas ────────────────────────────────────────────────────────────────────
export interface CartUpdatedData { cart: Record<string, unknown> }          // forma de la respuesta del carrito

export type QuotationStatus = 'draft' | 'sent' | 'accepted' | 'rejected' | 'expired' | 'converted'
export interface QuotationCreatedData { quotation: Record<string, unknown> }
export interface QuotationUpdatedData { entity: EntityRef<'quotation'>; row: Record<string, unknown>; changedFields: string[] }
export interface QuotationStatusChangedData { entity: EntityRef<'quotation'>; from: QuotationStatus; to: QuotationStatus; saleId: number | null }

export type SaleStage =
  | 'verification' | 'verification_ok' | 'picking' | 'picking_ok' | 'packing' | 'packing_ok'
  | 'invoicing' | 'invoicing_ok' | 'dispatch' | 'sent' | 'cancelled'
export type SaleStatus = 'open' | 'completed' | 'cancelled'
export interface SaleCreatedData { sale: Record<string, unknown> }          // forma de la fila de la tabla de pedidos
export interface SaleStageChangedData {
  entity: EntityRef<'sale'>; serial: string; from: SaleStage; to: SaleStage; status: SaleStatus
  reason: string | null; takenBy: RealtimeActor | null
}
export interface SalePaymentChangedData {
  saleId: number; paidTotal: number; creditAmount: number; balance: number; paymentVerified: boolean; version: string
}
export interface InvoiceIssuedData {
  invoice: { id: number; saleId: number; serial: string; dianNumber: string | null; total: number; status: 'issued'; version: string }
}
export interface InvoiceAnnulledData { entity: EntityRef<'invoice'>; saleId: number; reason: string }

// ── G · pagos, cartera y conciliación ─────────────────────────────────────────────
export type PaymentStatus = 'review' | 'verified' | 'issue' | 'voided'
export interface PaymentCreatedData { payment: Record<string, unknown> }    // forma de la fila de Abonos
export interface PaymentStatusChangedData { entity: EntityRef<'payment'>; from: PaymentStatus; to: PaymentStatus; reason: string | null }
export interface CreditBalanceChangedData {
  customerId: number; creditLimit: number; used: number; available: number; favorBalance: number
  status: 'ok' | 'warning' | 'blocked'
  entry: { type: 'charge' | 'payment' | 'return' | 'adjust'; amount: number; referenceType: string | null; referenceId: number | null } | null
  version: string
}
export interface BankStatementImportedData { statementId: number; bank: string; recordsCreated: number; period: { from: string; to: string } | null }
export type BankRecordStatus = 'pending' | 'confirmed' | 'ignored'
export interface BankRecordStatusChangedData { entity: EntityRef<'bank_record'>; from: BankRecordStatus; to: BankRecordStatus; paymentId: number | null }

// ── H · caja ──────────────────────────────────────────────────────────────────────
export interface CashTotals { cashIn: number; transferIn: number; expenses: number; expectedCash: number }
export interface CashSessionStatusChangedData {
  entity: EntityRef<'cash_session'>; from: 'open' | 'closed' | null; to: 'open' | 'closed'
  openedBy: RealtimeActor | null; closedBy: RealtimeActor | null; totals: CashTotals
}
export interface CashSessionTotalsChangedData extends CashTotals { sessionId: number; version: string }
export type CashExpenseType = 'merchandise_arrival' | 'payroll' | 'loans' | 'overtime' | 'cleaning_supplies' | 'services' | 'other'
export interface CashExpenseRecordedData {
  expense: { id: number; sessionId: number; expenseType: CashExpenseType; cashAmount: number; transferAmount: number; concept: string; referenceType: string | null; referenceId: number | null }
}

// ── I · compras ───────────────────────────────────────────────────────────────────
export type PurchaseStatus = 'draft' | 'ordered' | 'partial' | 'received' | 'voided'
export interface PurchaseCreatedData { purchase: Record<string, unknown> }
export interface PurchaseStatusChangedData {
  entity: EntityRef<'purchase'>; from: PurchaseStatus; to: PurchaseStatus
  lines: Array<{ productId: number; ordered: number; received: number }>; reason: string | null
}
export interface PurchasePaymentRecordedData { purchaseId: number; paid: number; balance: number; cashAmount: number; transferAmount: number }
export interface SupplierCatalogImportedData { supplierId: number; linked: number; warnings: string[] }

// ── J · devoluciones ──────────────────────────────────────────────────────────────
export type ReturnStatus = 'pending' | 'approved' | 'rejected'
export interface ReturnCreatedData { return: Record<string, unknown> }
export interface ReturnStatusChangedData {
  entity: EntityRef<'return'>; from: ReturnStatus; to: ReturnStatus
  resolution: 'exchange' | 'credit_balance'; rejectReason: string | null
}

// ── K · notificaciones, auditoría y respaldos ────────────────────────────────────
export interface NotificationCreatedData {
  notificationId: number
  kind: 'info' | 'success' | 'warning' | 'error' | 'restore_record'
  title: string; message: string
  link: { module: string; subitem: string; entity?: { kind: string; id: number } } | null
}
export interface NotificationReadData { ids: number[] | 'all' }
export interface AuditLoggedData { auditId: number; entity: string; entityId: number | null; action: string; actor: RealtimeActor; reason: string | null }
export interface BackupCreatedData { backup: { id: string; kind: string; sizeBytes: number; createdAt: string } }
export interface BackupRestoredData { backupId: string }

// ── mapa tipo → cuerpo (unión discriminada) ───────────────────────────────────────
export interface RealtimeEventMap {
  'system.welcome': WelcomeData
  'system.replay_completed': ReplayCompletedData
  'system.resync_required': ResyncRequiredData
  'system.watermark': WatermarkData
  'session.revoked': SessionRevokedData
  'permissions.changed': PermissionsChangedData
  'account.updated': AccountUpdatedData
  'entity.created': EntityCreatedData
  'entity.updated': EntityUpdatedData
  'entity.status_changed': EntityStatusChangedData
  'entity.archived': EntityArchivedData
  'entity.restored': EntityRestoredData
  'entity.deleted': EntityDeletedData
  'product.prices_changed': ProductPricesChangedData
  'stock.changed': StockChangedData
  'transfer.created': TransferCreatedData
  'transfer.status_changed': TransferStatusChangedData
  'count.created': CountCreatedData
  'count.progress_changed': CountProgressChangedData
  'count.status_changed': CountStatusChangedData
  'cart.updated': CartUpdatedData
  'quotation.created': QuotationCreatedData
  'quotation.updated': QuotationUpdatedData
  'quotation.status_changed': QuotationStatusChangedData
  'sale.created': SaleCreatedData
  'sale.stage_changed': SaleStageChangedData
  'sale.payment_changed': SalePaymentChangedData
  'invoice.issued': InvoiceIssuedData
  'invoice.annulled': InvoiceAnnulledData
  'payment.created': PaymentCreatedData
  'payment.status_changed': PaymentStatusChangedData
  'credit.balance_changed': CreditBalanceChangedData
  'bank_statement.imported': BankStatementImportedData
  'bank_record.status_changed': BankRecordStatusChangedData
  'cash_session.status_changed': CashSessionStatusChangedData
  'cash_session.totals_changed': CashSessionTotalsChangedData
  'cash.expense_recorded': CashExpenseRecordedData
  'purchase.created': PurchaseCreatedData
  'purchase.status_changed': PurchaseStatusChangedData
  'purchase.payment_recorded': PurchasePaymentRecordedData
  'supplier_catalog.imported': SupplierCatalogImportedData
  'return.created': ReturnCreatedData
  'return.status_changed': ReturnStatusChangedData
  'notification.created': NotificationCreatedData
  'notification.read': NotificationReadData
  'audit.logged': AuditLoggedData
  'backup.created': BackupCreatedData
  'backup.restored': BackupRestoredData
}

export type RealtimeEventType = keyof RealtimeEventMap        // 48 tipos
export type RealtimeEvent = { [K in RealtimeEventType]: RealtimeEnvelope<K> }[RealtimeEventType]

// ── mensajes del cliente al servidor (los únicos permitidos) ──────────────────────
export type ClientMessage =
  | { type: 'hello'; v: 1; lastSeq: number | null }
  | { type: 'pong'; ts: number }

// ── códigos de cierre del socket (los usa el front para decidir qué hacer) ────────
export const REALTIME_CLOSE_CODES = {
  normal: 1000,
  serverShutdown: 1001,          // reconectar con espera
  ticketInvalid: 4001,           // pedir ticket nuevo y reconectar
  originRejected: 4003,          // no reconectar; error de configuración
  sessionRevoked: 4401,          // cerrar sesión en el front
  queueOverflow: 4408,           // reconectar y recuperar por seq
  protocolError: 4400,           // mensaje inválido del cliente
  pingTimeout: 1011,             // reconectar
  accountUnavailable: 4403,      // cerrar sesión con el aviso de cuenta no disponible
  tooManyConnections: 4429,      // no reconectar en esta pestaña
} as const
```

> Los cuerpos con `Record<string, unknown>` (`row`, `sale`, `payment`…) llevan **exactamente** la forma del DTO que la API devuelve en su `GET /:id` o en su lista. Así el front reusa el adaptador que ya tiene cada feature (`*.adapter.ts`) en lugar de duplicar tipos. En cada manejador se tipa con el DTO de esa feature (por ejemplo `EntityUpdatedData<CustomerDto>`).

### 6.6 Ejemplos reales de mensajes


```json
{ "v": 1, "seq": 9001, "id": "6d0c…", "type": "entity.status_changed",
  "occurredAt": "2026-11-12T15:04:05.123456Z", "companyId": 3, "warehouseIds": [],
  "actor": { "type": "admin", "id": 4, "name": "Ana Pérez" }, "correlationId": "req-81f2",
  "data": { "entity": { "kind": "customer", "id": 120, "version": "2026-11-12T15:04:05.120001Z" },
            "from": "active", "to": "inactive", "reason": "Cliente cerró su negocio en octubre",
            "row": { "id": 120, "name": "Ferretería El Tornillo", "personType": "legal", "status": "inactive", "updatedAt": "2026-11-12T15:04:05.120001Z" } } }
```

```json
{ "v": 1, "seq": 9002, "id": "a91e…", "type": "stock.changed",
  "occurredAt": "2026-11-12T15:06:10.004211Z", "companyId": 3, "warehouseIds": [7],
  "actor": { "type": "employee", "id": 18, "name": "Luis Gómez" }, "correlationId": "req-81f9",
  "data": { "cause": { "type": "transfer_out", "referenceType": "transfer", "referenceId": 42, "referenceSerial": "TR-000042" },
            "rows": [ { "stockId": 501, "productId": 88, "productCode": "FIL-220", "warehouseId": 7, "location": "A-03-2",
                        "available": 14, "reserved": 2, "sellable": 12, "minStock": 5, "lowStock": false,
                        "version": "2026-11-12T15:06:10.001100Z" } ],
            "totals": [ { "productId": 88, "warehouseId": 7, "available": 30, "reserved": 2, "sellable": 28 } ] } }
```

```json
{ "v": 1, "seq": 9003, "id": "c3b7…", "type": "transfer.status_changed",
  "occurredAt": "2026-11-12T15:06:10.004211Z", "companyId": 3, "warehouseIds": [7, 9],
  "actor": { "type": "employee", "id": 18, "name": "Luis Gómez" }, "correlationId": "req-81f9",
  "data": { "entity": { "kind": "transfer", "id": 42, "version": "2026-11-12T15:06:10.002000Z" },
            "serial": "TR-000042", "from": "pending", "to": "in_transit",
            "originWarehouseId": 7, "destinationWarehouseId": 9,
            "lines": [ { "lineId": 77, "productId": 88, "productCode": "FIL-220", "productName": "Filtro aceite 220",
                         "quantity": 6, "receivedQty": 0, "destinationLocation": "B-01-1" } ],
            "noveltyNotes": null, "cancelReason": null,
            "dispatchedBy": { "type": "employee", "id": 18, "name": "Luis Gómez" }, "receivedBy": null } }
```

---

## 7. Contrato Go: productor (API) y sobre (servicio realtime)

### 7.1 Productor · `importadoraApi/src/core/events/types.go`

Lo usan los servicios de la API para armar los eventos. El servicio `importadoraRealtime` **no** importa este paquete.

```go
// types.go · contrato v1 de los eventos de tiempo real que la API escribe en realtime_events
// conecta con: catalog.go (canal y permisos por tipo), recorder.go (Record/Stock) y los servicios de cada módulo (los crean)
// ojo: debe coincidir con importadoraRealtime/contract (JSON Schema) y con importadoraInterfaz/src/features/realtime/types/realtime.contract.ts
package events

import "time"

type Type string

// A · sistema: los genera importadoraRealtime, no la API (se listan para que el catálogo esté completo)
const (
	SystemWelcome         Type = "system.welcome"
	SystemReplayCompleted Type = "system.replay_completed"
	SystemResyncRequired  Type = "system.resync_required"
	SystemWatermark       Type = "system.watermark"
)

// B · sesión y acceso
const (
	SessionRevoked     Type = "session.revoked"
	PermissionsChanged Type = "permissions.changed"
	AccountUpdated     Type = "account.updated"
)

// C · datos maestros genéricos
const (
	EntityCreated       Type = "entity.created"
	EntityUpdated       Type = "entity.updated"
	EntityStatusChanged Type = "entity.status_changed"
	EntityArchived      Type = "entity.archived"
	EntityRestored      Type = "entity.restored"
	EntityDeleted       Type = "entity.deleted"
)

// D a K · dominios con flujo propio
const (
	ProductPricesChanged     Type = "product.prices_changed"
	StockChanged             Type = "stock.changed"
	TransferCreated          Type = "transfer.created"
	TransferStatusChanged    Type = "transfer.status_changed"
	CountCreated             Type = "count.created"
	CountProgressChanged     Type = "count.progress_changed"
	CountStatusChanged       Type = "count.status_changed"
	CartUpdated              Type = "cart.updated"
	QuotationCreated         Type = "quotation.created"
	QuotationUpdated         Type = "quotation.updated"
	QuotationStatusChanged   Type = "quotation.status_changed"
	SaleCreated              Type = "sale.created"
	SaleStageChanged         Type = "sale.stage_changed"
	SalePaymentChanged       Type = "sale.payment_changed"
	InvoiceIssued            Type = "invoice.issued"
	InvoiceAnnulled          Type = "invoice.annulled"
	PaymentCreated           Type = "payment.created"
	PaymentStatusChanged     Type = "payment.status_changed"
	CreditBalanceChanged     Type = "credit.balance_changed"
	BankStatementImported    Type = "bank_statement.imported"
	BankRecordStatusChanged  Type = "bank_record.status_changed"
	CashSessionStatusChanged Type = "cash_session.status_changed"
	CashSessionTotalsChanged Type = "cash_session.totals_changed"
	CashExpenseRecorded      Type = "cash.expense_recorded"
	PurchaseCreated          Type = "purchase.created"
	PurchaseStatusChanged    Type = "purchase.status_changed"
	PurchasePaymentRecorded  Type = "purchase.payment_recorded"
	SupplierCatalogImported  Type = "supplier_catalog.imported"
	ReturnCreated            Type = "return.created"
	ReturnStatusChanged      Type = "return.status_changed"
	NotificationCreated      Type = "notification.created"
	NotificationRead         Type = "notification.read"
	AuditLogged              Type = "audit.logged"
	BackupCreated            Type = "backup.created"
	BackupRestored           Type = "backup.restored"
)

// Kind: entidades de datos maestros (para entity.*) y documentos (para EntityRef)
type Kind string

const (
	KindCompany Kind = "company"; KindAdmin Kind = "admin"; KindWarehouse Kind = "warehouse"
	KindWarehouseBilling Kind = "warehouse_billing"; KindDocSeries Kind = "doc_series"; KindLocation Kind = "location"
	KindCustomer Kind = "customer"; KindCustomerDocument Kind = "customer_document"; KindSupplier Kind = "supplier"; KindCarrier Kind = "carrier"
	KindBrand Kind = "brand"; KindCategory Kind = "category"; KindModel Kind = "model"; KindProduct Kind = "product"
	KindProductCode Kind = "product_code"; KindProductSupplier Kind = "product_supplier"; KindRole Kind = "role"
	KindEmployee Kind = "employee"; KindScreensaverSettings Kind = "screensaver_settings"
	// documentos
	KindSale Kind = "sale"; KindInvoice Kind = "invoice"; KindQuotation Kind = "quotation"; KindTransfer Kind = "transfer"
	KindCount Kind = "count"; KindPurchase Kind = "purchase"; KindReturn Kind = "return"; KindPayment Kind = "payment"
	KindBankRecord Kind = "bank_record"; KindCashSession Kind = "cash_session"
)

type Actor struct {
	Type string `json:"type"` // employee | admin | superadmin | system
	ID   *int64 `json:"id"`
	Name string `json:"name"`
}

type EntityRef struct {
	Kind    Kind      `json:"kind"`
	ID      int64     `json:"id"`
	Version time.Time `json:"version"`
}

// Event lo crea un servicio dentro de su transacción; Seq, ID y OccurredAt los pone la BD al guardarlo
type Event struct {
	Type          Type
	CompanyID     *int64
	WarehouseIDs  []int64
	ActorChannel  string   // "employee:12"; vacío si no es personal
	Platform      bool     // true: también al canal platform (superadmins)
	Permissions   []string // basta con UNO; vacío = cualquiera del canal
	CorrelationID string   // id de la petición HTTP (middleware RequestID de Echo)
	Data          any      // uno de los *Data de abajo; se guarda como JSONB en payload
}
```

Cuerpos (uno por tipo, mismos nombres y campos que la sección 6.5): `StatusChangedData`, `EntityCreatedData`, `EntityUpdatedData`, `EntityArchivedData`, `EntityRestoredData`, `EntityDeletedData`, `ProductPricesChangedData`, `StockChangedData` (`StockCause`, `StockRowChange`, `StockTotal`), `TransferRow`, `TransferLine`, `TransferStatusChangedData`, `CountItem`, `CountProgressChangedData`, `CountStatusChangedData`, `CartUpdatedData`, `QuotationStatusChangedData`, `SaleStageChangedData`, `SalePaymentChangedData`, `InvoiceIssuedData`, `InvoiceAnnulledData`, `PaymentStatusChangedData`, `CreditBalanceChangedData`, `BankStatementImportedData`, `BankRecordStatusChangedData`, `CashTotals`, `CashSessionStatusChangedData`, `CashSessionTotalsChangedData`, `CashExpenseRecordedData`, `PurchaseStatusChangedData`, `PurchasePaymentRecordedData`, `SupplierCatalogImportedData`, `ReturnStatusChangedData`, `NotificationCreatedData`, `NotificationReadData`, `AuditLoggedData`, `BackupCreatedData`, `BackupRestoredData`, `SessionRevokedData`, `PermissionsChangedData` y `AccountUpdatedData`.

Ejemplo de cuerpos clave:

```go
type StatusChangedData struct {
	Entity EntityRef `json:"entity"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Reason *string   `json:"reason"`
	Row    any       `json:"row"` // el DTO de GET /:id de esa entidad
}

type StockRowChange struct {
	StockID     int64     `json:"stockId"`
	ProductID   int64     `json:"productId"`
	ProductCode string    `json:"productCode"`
	WarehouseID int64     `json:"warehouseId"`
	Location    string    `json:"location"`
	Available   int       `json:"available"`
	Reserved    int       `json:"reserved"`
	Sellable    int       `json:"sellable"`
	MinStock    int       `json:"minStock"`
	LowStock    bool      `json:"lowStock"`
	Version     time.Time `json:"version"`
}

type StockCause struct {
	Type            string  `json:"type"`
	ReferenceType   *string `json:"referenceType"`
	ReferenceID     *int64  `json:"referenceId"`
	ReferenceSerial *string `json:"referenceSerial"`
}

type StockTotal struct {
	ProductID   int64 `json:"productId"`
	WarehouseID int64 `json:"warehouseId"`
	Available   int   `json:"available"`
	Reserved    int   `json:"reserved"`
	Sellable    int   `json:"sellable"`
}

type StockChangedData struct {
	Cause  StockCause       `json:"cause"`
	Rows   []StockRowChange `json:"rows"`
	Totals []StockTotal     `json:"totals"`
}

type TransferStatusChangedData struct {
	Entity                 EntityRef      `json:"entity"`
	Serial                 string         `json:"serial"`
	From                   string         `json:"from"`
	To                     string         `json:"to"`
	OriginWarehouseID      int64          `json:"originWarehouseId"`
	DestinationWarehouseID int64          `json:"destinationWarehouseId"`
	Lines                  []TransferLine `json:"lines"`
	NoveltyNotes           *string        `json:"noveltyNotes"`
	CancelReason           *string        `json:"cancelReason"`
	DispatchedBy           *Actor         `json:"dispatchedBy"`
	ReceivedBy             *Actor         `json:"receivedBy"`
}
```

### 7.2 Enrutamiento por tipo · `importadoraApi/src/core/events/catalog.go`

Una tabla que dice, para cada tipo, **a qué canales va por defecto y qué permisos exige**. Los servicios no repiten esa decisión: llaman a constructores (`events.TransferStatus(...)`, `events.EntityStatus(kind, ...)`) que la aplican.

```go
// catalog.go · enrutamiento por defecto de cada tipo: canal (compañía, bodegas, actor, plataforma) y permisos
// conecta con: types.go y recorder.go; la prueba catalog_test.go exige que TODO Type tenga su entrada y que los permisos existan en el catálogo sembrado
package events

type route struct {
	Company, Warehouses, Actor, Platform bool
	Permissions                          []string
}

var entityPermissions = map[Kind][]string{
	KindWarehouse: {"warehouses:view", "transfers:view", "stock:view"},
	KindLocation:  {"stock:view", "transfers:view", "purchases:view"},
	KindCustomer:  {"customers:view", "orders:create", "quotations:view", "credits:view"},
	KindProduct:   {"products:view", "catalog:view", "stock:view", "transfers:view", "purchases:view"},
	// … una entrada por cada Kind de la tabla 6.3-C
}

var routes = map[Type]route{
	StockChanged:          {Company: true, Permissions: []string{"catalog:view", "stock:view", "transfers:view", "products:view"}},
	TransferCreated:       {Warehouses: true, Permissions: []string{"transfers:view"}},
	TransferStatusChanged: {Warehouses: true, Permissions: []string{"transfers:view"}},
	SaleStageChanged:      {Warehouses: true, Permissions: []string{"orders:view", "cash:view"}},
	SessionRevoked:        {Actor: true},
	NotificationCreated:   {Actor: true},
	// … una entrada por cada Type de la sección 6.3 (las entity.* toman sus permisos de entityPermissions)
}
```

### 7.3 Uso dentro de un servicio (ejemplo: despachar un traslado)

```go
err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
	for _, item := range items {
		row, err := s.inventoryRepo.DeductStock(ctx, tx, item.OriginStockID, item.Quantity, true) // ahora devuelve la fila
		if err != nil { return err }
		s.events.Stock(ctx, row, events.CauseTransferOut(transfer.ID, transfer.SerialCode))      // E2: al buffer de la tx
		// … RecordMovement del kardex (ya existe)
	}
	updated, err := s.inventoryRepo.DispatchTransfer(ctx, tx, transferID, actorEmployeeID)        // ahora devuelve el traslado
	if err != nil { return err }
	s.events.Record(ctx, events.TransferStatus(updated, "pending", "in_transit", actorRef))       // E1
	result = updated
	return nil
}) // WithTx ejecuta el gancho de events antes del COMMIT: INSERT realtime_events + pg_notify
```

### 7.4 Sobre leído por el servicio realtime · `importadoraRealtime/src/core/protocol/envelope.go`

El servicio realtime **no conoce los cuerpos**: lee las columnas de `realtime_events` y reenvía `payload` sin tocarlo.

```go
// envelope.go · sobre v1 tal como se lee de realtime_events y se envía por el socket; el cuerpo (payload) viaja sin interpretarse
// conecta con: features/events (lo lee de la BD), core/hub (decide a quién va) y features/connection (lo escribe en el socket)
package protocol

import (
	"encoding/json"
	"time"
)

// StoredEvent es una fila de realtime_events (lo que el hub necesita para enrutar)
type StoredEvent struct {
	Seq          int64
	ID           string
	Type         string
	CompanyID    *int64
	WarehouseIDs []int64
	ActorChannel *string
	Platform     bool
	Permissions  []string
	Actor        Actor
	CorrelationID *string
	Payload      json.RawMessage
	OccurredAt   time.Time
}

// Envelope es lo que se escribe en el socket (mismo formato que RealtimeEnvelope del front)
type Envelope struct {
	V             int             `json:"v"`
	Seq           int64           `json:"seq"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	OccurredAt    time.Time       `json:"occurredAt"`
	CompanyID     *int64          `json:"companyId"`
	WarehouseIDs  []int64         `json:"warehouseIds"`
	Actor         Actor           `json:"actor"`
	CorrelationID string          `json:"correlationId,omitempty"`
	Data          json.RawMessage `json:"data"`
}

type Actor struct {
	Type string `json:"type"`
	ID   *int64 `json:"id"`
	Name string `json:"name"`
}
```

Como el servicio realtime **sí** debe actuar sobre tres tipos (`session.revoked`, `permissions.changed` y `system.resync_required` por respaldo), solo esos tres cuerpos se decodifican allí, en `features/events/services/control_events.go`. Todo lo demás pasa sin decodificar.

Para que ningún servicio tenga que acordarse de `Flush`, se usa el `db.WithTx` que **ya existe** (`core/db/postgres.go:47`). Para no hacer que `core/db` dependa de `core/events`, se agrega un gancho genérico:
- `core/db/txhooks.go`: `db.OnBeforeCommit(ctx, func(tx pgx.Tx) error)`, que guarda funciones en el `context`.
- `WithTx` ejecuta esos ganchos después de `fn(tx)` y antes de `tx.Commit`. Si un gancho falla, se hace rollback.
- `events.Buffer` (API, `core/events/buffer.go`) se registra como gancho la primera vez que un servicio llama a `events.Record(ctx, …)` dentro de la transacción.

**Requisito previo:** antes del plan principal había **18** usos de `db.WithTx` y **21** transacciones abiertas a mano con `s.pool.Begin(ctx)` (por ejemplo `customer_write_service.go:334`, `payment_flow_service.go:57` y `payment_service.go:58, 189, 275`). La etapa P da la cifra real al terminar ese plan. Esas transacciones se pasan a `db.WithTx` en la etapa E6, antes de emitir eventos. Si no, sus eventos nunca se guardarían. Para encontrarlas: `grep -rn "\.Begin(\|BeginTx(" src/features`.

---

## 8. Estructura completa: cada archivo y su responsabilidad

> Toda carpeta y archivo de abajo se crea **en la fase indicada** (sección 9). Cada archivo empieza con la cabecera del proyecto: `// nombre · qué hace` + `// conecta con:` + `// ojo:` cuando aplique. Las pruebas `*_test.go` van al lado del archivo que prueban.

### 8.1 `importadoraRealtime/` (servicio nuevo)

```
importadoraRealtime/
├── PLAN-WEBSOCKET.md
├── README.md
├── go.mod / go.sum
├── main.go
├── .env.example
├── .gitignore
├── contract/
│   ├── REALTIME-CONTRATO.md
│   ├── CHANGELOG.md
│   ├── schemas/
│   │   ├── envelope.schema.json
│   │   ├── client-message.schema.json
│   │   └── events/<tipo>.schema.json          (48, uno por tipo de la sección 6.3)
│   ├── examples/<tipo>.json                   (48, uno por tipo)
│   └── scripts/validate.go                    (valida los 48 ejemplos contra sus schemas)
├── src/
│   ├── core/
│   │   ├── config/      config.go · loader.go · validators.go
│   │   ├── db/          postgres.go · listen_conn.go
│   │   ├── logging/     logger.go
│   │   ├── protocol/    envelope.go · client_messages.go · system_messages.go · close_codes.go
│   │   ├── hub/         hub.go · client.go · channels.go · filter.go · queue.go · metrics.go
│   │   ├── server/      server.go · origin.go · limits.go
│   │   └── bootstrap/   container.go · router.go
│   └── features/
│       ├── connection/  routes/ · controllers/ · services/ · dtos/
│       ├── tickets/     repositories/ · services/
│       ├── access/      repositories/ · services/ · dtos/
│       ├── events/      repositories/ · services/
│       └── health/      routes/ · controllers/ · services/
└── scripts/
    └── e2e/             README.md · main.go · flows/*.go
```

| Archivo | Responsabilidad |
|---|---|
| `PLAN-WEBSOCKET.md` | Este plan. |
| `README.md` | Qué es el servicio, cómo se relaciona con la API y el front (diagrama 0), cómo correrlo (`go run .`), variables, puertos y el usuario de BD. |
| `go.mod` | Módulo `importadoraRealtime`, Go 1.27; dependencias: `jackc/pgx/v5`, `coder/websocket`, `joho/godotenv`. **No** depende de `importadoraApi`. |
| `main.go` | Raíz de composición: carga la configuración → conecta la BD → arma el contenedor → arranca listener, purga y servidor HTTP → apagado ordenado con `SIGINT`/`SIGTERM` (cierra los sockets con 1001). |
| `.env.example` | Variables de la sección 5 (`DB_*` con el usuario `importadora_realtime`, `APP_PORT=3002`, `REALTIME_*`). |
| `contract/REALTIME-CONTRATO.md` | Fuente única: sobre, canales, catálogo de la sección 6.3 y reglas de versión. |
| `contract/CHANGELOG.md` | Historial del contrato (v1, campos agregados). Un cambio incompatible sube `v`. |
| `contract/schemas/envelope.schema.json` | JSON Schema del sobre. |
| `contract/schemas/client-message.schema.json` | JSON Schema de `hello` y `pong` (lo único que el cliente puede enviar). |
| `contract/schemas/events/*.schema.json` | Un schema por tipo (`stock.changed.schema.json`, `transfer.status_changed.schema.json`, …). |
| `contract/examples/*.json` | Un ejemplo válido por tipo. Lo usan las pruebas de la API (productor), del front (consumidor) y de este servicio. |
| **core/config** | |
| `config.go` | `struct Config` tipado: BD, puerto, orígenes permitidos, retención, colas, ping y máximo de recuperación. |
| `loader.go` | Lee `.env` y variables de entorno, con valores por defecto. |
| `validators.go` | Falla al arrancar si falta algo crítico (orígenes vacíos en producción, puerto o BD). |
| **core/db** | |
| `postgres.go` | Pool `pgxpool` con el usuario de solo lectura; `Ping` al arrancar. |
| `listen_conn.go` | Conexión **dedicada** (fuera del pool) para `LISTEN erp_events`, con reconexión y espera creciente. Avisa al listener cuando reconecta, para que relea desde el último `seq`. |
| **core/logging** | |
| `logger.go` | `log/slog` en JSON con campos fijos (`connId`, `actor`, `seq` y `type`). **Nunca** registra tickets ni payloads completos. |
| **core/protocol** | |
| `envelope.go` | `StoredEvent` (fila de la BD) y `Envelope` (lo que se envía); conversión entre ambos (sección 7.4). |
| `client_messages.go` | Lectura y validación de `hello`/`pong` (tamaño máximo 4 KB). Cualquier otro mensaje → cierre 4400. |
| `system_messages.go` | Constructores de `system.welcome`, `system.replay_completed` y `system.resync_required`. |
| `close_codes.go` | Códigos de cierre 1000/1001/4001/4003/4400/4401/4408 (iguales a `REALTIME_CLOSE_CODES` del front). |
| **core/hub** | |
| `hub.go` | Registro central: clientes por canal, por sesión y por actor. `Register`, `Unregister`, `Dispatch(StoredEvent)`, `CloseSession(id, code)`, `ReloadPermissions(actor)` y `BroadcastResync(scope)`. Un solo dueño del estado (goroutine con canales o `sync.RWMutex`). |
| `client.go` | Una conexión: identidad (`AccessContext`), canales, permisos, cola de salida y dos goroutines (lectura: `hello`/`pong`; escritura: cola + ping cada `REALTIME_PING_SEC`). |
| `channels.go` | Nombres de canal (`platform`, `company:{id}`, `warehouse:{id}`, `actor:{tipo}:{id}`) y cálculo de los canales de un actor (empleado: su compañía + su bodega; admin: su compañía + todas sus bodegas; superadmin: solo `platform`, sin compañía, decisión 2). |
| `filter.go` | ¿Este cliente recibe este evento? Canal coincide **y** (sin permisos requeridos **o** tiene uno de ellos **o** es admin de la compañía **o** es superadmin con permiso de lectura). |
| `queue.go` | Cola acotada por cliente (`REALTIME_MAX_CLIENT_QUEUE`). Si se llena, cierra con 4408 y el cliente recupera por `seq`. |
| `metrics.go` | Contadores: conexiones abiertas, eventos repartidos, colas llenas y retraso del listener (los expone `/health`). |
| **core/server** | |
| `server.go` | `http.Server` con `ServeMux`: `GET /ws` y `GET /health`; tiempos de espera y apagado ordenado. |
| `origin.go` | Valida `Origin` contra `REALTIME_ALLOWED_ORIGINS` (`AcceptOptions.OriginPatterns`) → 4003 si no coincide. |
| `limits.go` | Límites: tamaño de mensaje entrante, conexiones por actor (5 pestañas, decisión 6) y tasa de conexiones por IP. |
| **core/bootstrap** | |
| `container.go` | Crea repositorios, servicios, hub y listener, y los conecta (inyección de dependencias, igual que la API). |
| `router.go` | Registra las rutas de `connection` y `health` en el servidor. |
| **features/connection** | |
| `routes/connection_routes.go` | `GET /ws`. |
| `controllers/connection_controller.go` | Lee `?ticket=` y acepta el upgrade con `coder/websocket`; delega todo lo demás al servicio. |
| `services/connection_service.go` | Saludo completo: consumir ticket → validar acceso → calcular canales → registrar en el hub → `system.welcome` → esperar `hello` (5 s) → recuperación. |
| `services/replay_service.go` | Con `lastSeq`: lee de `realtime_events` los eventos del cliente con `seq > lastSeq` (filtrados como el hub). Si son más de `REALTIME_MAX_REPLAY` o el `seq` ya se purgó → `system.resync_required`. Al terminar → `system.replay_completed`. Mientras recupera, los eventos en vivo se encolan para no desordenar. |
| `dtos/hello_dto.go` | Forma del `hello` (`v`, `lastSeq`). |
| **features/tickets** | |
| `repositories/ticket_repo.go` | `Consume(hash)`: `UPDATE realtime_tickets SET used_at = now() WHERE ticket_hash = $1 AND used_at IS NULL AND expires_at > now() RETURNING …`. `PurgeExpired()`. |
| `services/ticket_service.go` | Calcula el `sha256` del ticket recibido, lo consume y devuelve la identidad, o un error que termina en 4001. |
| **features/access** | |
| `repositories/session_repo.go` | ¿La sesión (`refresh_tokens`) sigue activa? Misma regla que `IsSessionActive` de la API. |
| `repositories/actor_repo.go` | ¿El actor, su compañía y su bodega están activos? Misma regla que `IsActorAvailable`. |
| `repositories/permission_repo.go` | Permisos del actor (`employee_permissions` + `permissions`). Misma consulta que `LoadPermissionsForActor`. |
| `services/access_service.go` | Arma el `AccessContext` (actor, compañía, bodegas, sesión y permisos) y lo recalcula ante `permissions.changed`. |
| `dtos/access_context.go` | `AccessContext`: lo que el hub necesita saber de cada conexión. |
| **features/events** | |
| `repositories/event_repo.go` | `ReadSince(seq, limit)`, `ReadForClient(seq, scope, limit)`, `CurrentSeq()` y `Purge(olderThan)`. |
| `services/listener_service.go` | Bucle: espera `NOTIFY erp_events` → lee en lote desde el último `seq` procesado → `hub.Dispatch`. Al reconectar relee lo pendiente, así no se pierde nada aunque se hayan perdido notificaciones. |
| `services/control_events.go` | Actúa sobre los únicos eventos que este servicio interpreta: `session.revoked` → `hub.CloseSession(4401)`; `permissions.changed` → `access.Reload` + `hub.ReloadPermissions`; `backup.restored` → `hub.BroadcastResync`. Luego se reenvían normal. |
| `services/purge_service.go` | Cada hora borra eventos con más de `REALTIME_RETENTION_HOURS` y tickets vencidos. |
| **features/health** | |
| `routes/health_routes.go` | `GET /health`. |
| `controllers/health_controller.go` | Responde 200/503 en JSON. |
| `services/health_service.go` | Estado: BD, conexión `LISTEN`, último `seq` procesado contra `CurrentSeq` (retraso), conexiones abiertas y colas llenas. |
| **scripts/e2e** | |
| `README.md` | Cómo correr las pruebas e2e (API en `:3001`, realtime en `:3002`, base de prueba). |
| `main.go` | Ejecuta los flujos y resume OK/FALLA (mismo estilo que `importadoraApi/scripts/e2e`). |
| `flows/tickets.go` | Ticket válido, reutilizado (4001), vencido y con `Origin` inválido (4003). |
| `flows/channels.go` | Un empleado de la bodega 7 no recibe eventos de la bodega 9; el destino de un traslado sí. |
| `flows/permissions.go` | Sin `transfers:view` no llegan eventos de traslados; al dárselo en vivo empiezan a llegar. |
| `flows/replay.go` | Desconexión, cambios y reconexión con `lastSeq` → recibe exactamente lo que faltó; hueco grande → `resync_required`. |
| `flows/revocation.go` | Desactivar al empleado o cerrar su sesión → cierre 4401 inmediato. |
| `flows/load.go` | 500 conexiones y una ráfaga de 300 filas de stock: sin colas llenas y con un retraso bajo el umbral. |

### 8.2 `importadoraApi/` (solo lo que se agrega o cambia)

| Archivo | Responsabilidad |
|---|---|
| `src/core/db/txhooks.go` (nuevo) | `OnBeforeCommit(ctx, fn)` y `runBeforeCommit(ctx, tx)`: ganchos genéricos ligados al `context` de la transacción. |
| `src/core/db/postgres.go` (cambia) | `WithTx` ejecuta los ganchos antes de `Commit`; si fallan, rollback. `fn` recibe `ctx` (cambio de firma en todos sus usos). |
| `src/core/server/server.go` (cambia) | Solo se agrega `middleware.RequestID()`, para el `correlationId` de los eventos. |
| `src/core/middlewares/auth_middleware.go` (cambia) | `Authenticate` deja el actor también en el `context.Context` de la petición, para que `events.ActorFromContext` lo lea. |
| `src/core/events/actor.go` (nuevo) | Actor del evento desde el `context`, con caché de nombres (10 min). |
| `src/core/events/types.go` (nuevo) | Tipos, `Kind`, `Event` y todos los cuerpos (sección 7.1). |
| `src/core/events/catalog.go` (nuevo) | Canal y permisos por defecto de cada tipo y de cada `Kind` (sección 7.2). |
| `src/core/events/constructors.go` (nuevo) | Funciones que arman eventos correctos: `EntityStatus(kind, row, from, to, reason)`, `TransferStatus(...)`, `CauseTransferOut(...)`, etc. Los servicios usan estas y no arman `Event` a mano. |
| `src/core/events/recorder.go` (nuevo) | `Recorder` inyectado en los servicios: `Record(ctx, Event)` y `Stock(ctx, row, cause)`. Registra el buffer como gancho en la primera llamada. |
| `src/core/events/buffer.go` (nuevo) | Acumula eventos de una transacción, agrupa stock (última versión por `stockId`, lotes de 200), completa el actor y el `correlationId` desde el contexto, y hace `Flush`. |
| `src/core/events/repository.go` (nuevo) | `INSERT` en lote en `realtime_events` + `SELECT pg_notify('erp_events', <último seq>)`. |
| `src/core/events/*_test.go` (nuevo) | Rollback sin evento, agrupación de stock, `catalog_test.go` (todo tipo tiene ruta y sus permisos existen) y `contract_test.go` (cada ejemplo de `../importadoraRealtime/contract/examples` se decodifica en su struct sin campos perdidos). |
| `src/core/bootstrap/container.go` (cambia) | Crea el `Recorder` y lo pasa a los servicios. |
| `src/core/bootstrap/router.go` (cambia) | Registra `realtimeRoutes`. |
| `src/core/bootstrap/events_guard_test.go` (nuevo) | Falla si un servicio que modifica datos dentro de `db.WithTx` no llama `Record`, o si un escritor de stock no llama `Stock`. |
| `src/core/config/*` (cambia) | `REALTIME_TICKET_TTL_SEC` y `REALTIME_WS_URL`. |
| `src/features/realtime/routes/realtime_routes.go` (nuevo) | `POST /realtime/ticket` (requiere sesión). |
| `src/features/realtime/controllers/realtime_controller.go` (nuevo) | Responde `{ ticket, wsUrl, expiresAt }`. |
| `src/features/realtime/services/ticket_service.go` (nuevo) | Genera 32 bytes aleatorios (`crypto/rand`), guarda su `sha256` con actor, compañía, bodega y sesión, y devuelve el ticket en claro **una sola vez**. |
| `src/features/realtime/repositories/ticket_repo.go` (nuevo) | `INSERT INTO realtime_tickets`. |
| `src/features/realtime/dtos/realtime_dto.go` (nuevo) | DTO de respuesta. |
| `src/features/*/services/*.go` (cambian) | Llaman a `Record`/`Stock` dentro de su transacción según la sección 6.3, y responden la entidad actualizada (sección 12). |
| `src/features/inventory/repositories/inventory_repo.go`, `returns/repositories/return_repo.go`, `warehouses/repositories/warehouse_location_repo.go` (cambian) | Los escritores de stock devuelven la fila (`RETURNING`) para poder emitir `stock.changed`. |
| `src/core/db/migrations/2026112X_realtime.sql` (nuevo) + `startdb.pgsql` (cambia) | Tablas, rol y `updated_at` de la sección 5. |
| `docs/swagger.json` (cambia) | `POST /realtime/ticket`. |

### 8.3 `importadoraInterfaz/` (solo lo que se agrega o cambia)

| Archivo | Responsabilidad |
|---|---|
| `src/features/realtime/types/realtime.contract.ts` | Contrato completo (sección 6.5). |
| `src/features/realtime/types/realtime.types.ts` (cambia) | Estado de la conexión: `idle \| connecting \| open \| replaying \| reconnecting \| offline`. |
| `src/features/realtime/constants/realtime.constants.ts` (cambia) | Tiempos (espera de `hello`, ping y espera máxima) y códigos de cierre. Se retira el `REALTIME_EVENT_TYPES` provisional. |
| `src/features/realtime/services/realtime.ticket.ts` (nuevo) | `POST /realtime/ticket` con `api/client`. |
| `src/features/realtime/services/realtime.service.ts` (cambia) | Ciclo de vida del `WebSocket`: abrir con ticket, enviar `hello`, responder `pong`, interpretar los códigos de cierre (4001 → ticket nuevo; 4401 → cerrar sesión; 4003 → no reintentar) y reconectar con `realtime.backoff`. |
| `src/features/realtime/helpers/realtime.adapter.ts` (cambia) | Convierte el texto en `RealtimeEvent`: valida `v`, `type` conocido y la forma mínima. Se borran `realtime.url.ts` (`?token=`) y los nombres heredados. |
| `src/features/realtime/helpers/realtime.guards.ts` (nuevo) | Guardas de tipo por evento (`isStockChanged`, …) que usan los manejadores. |
| `src/features/realtime/helpers/realtime.sequencer.ts` (nuevo) | `lastSeq`, descarte de repetidos y detección de huecos (pide `hello` de nuevo). |
| `src/features/realtime/helpers/realtime.router.ts` (nuevo, reemplaza `realtime.handlers.ts`) | Registro `tipo → manejadores[]` y ejecución en orden; un manejador que falla no detiene a los demás. |
| `src/features/realtime/helpers/realtime.backoff.ts` | Se conserva. |
| `src/features/realtime/slices/realtime.slice.ts` (cambia) | Estado de la conexión, `lastSeq` y última conexión. **No** se persiste. |
| `src/features/realtime/thunks/realtime.thunks.ts` (cambia) | `startRealtime` / `stopRealtime` / `resyncVisibleScreen`. |
| `src/features/realtime/hooks/useRealtime.ts` | Se conserva (lo monta `ErpLayout`). |
| `src/features/realtime/hooks/useRealtimeStatus.ts` (nuevo) | Lee el estado para el indicador. |
| `src/features/realtime/components/RealtimeStatus.tsx` + `.css` (nuevo) | Punto discreto en el `AppHeader` existente (conectado / reconectando / sin conexión). |
| `src/features/realtime/index.ts` (cambia) | API pública: contrato, `registerRealtimeHandlers`, `useRealtime` y `RealtimeStatus`. |
| `src/app/store/realtimeRegistry.ts` (nuevo) | Único archivo que importa los manejadores de todas las features (vía su `index.ts`) y los registra. |
| `src/global/helpers/realtime/applyVersioned.ts` (nuevo) | Función pura: aplicar solo si `version` es más nueva. La usan todos los reducers. |
| `src/global/hooks/ui/useRecentlyChanged.ts` (nuevo) | Resaltado breve (1,5 s) de filas o campos cambiados por evento. |
| `src/features/<feature>/realtime/<feature>.realtime.ts` (nuevo en cada feature de la matriz 6.4) | Manejadores de esa feature: convierten el evento en acciones de su slice. Se exportan por su `index.ts`. |
| `src/features/<feature>/slices/*.ts` (cambian) | Acciones `upsertFromEvent`, `statusFromEvent`, `removeFromEvent` y `stockRowsChanged`, con `createEntityAdapter` donde haga falta. |
| `.env.example` (cambia) | `VITE_WS_URL=ws://localhost:3002/ws`. |

---

## 9. Ruta de desarrollo: etapas E0–E11

> Reemplaza las fases R0–R8 de versiones anteriores de este plan.
>
> **Cada etapa trae:** objetivo · requisitos previos · rama · directorios que toca · tareas numeradas por archivo · pruebas con nombre · criterio de salida · riesgos · cómo deshacerla.
>
> **Regla común:**
> - Antes de empezar: validar el estado actual del código y anotar en el PR la ruta elegida.
> - Al terminar: verificaciones del directorio tocado (sección 17.4) en verde.
> - No se abre la etapa siguiente sin el criterio de salida cumplido.

### 9.0 Punto de partida: el estado final del plan principal

Este plan **empieza cuando `PLAN-AJUSTES-UI-PERMISOS.md` termina** (su Fase 9 cerrada y su regresión total en verde). No corre en paralelo con él, no se coordina con el agente que lo ejecuta y no modifica nada de ese plan. Todo lo de abajo se construye **sobre** el resultado de ese plan.

Lo que se da por hecho al iniciar (y se verifica en la etapa P):

| Viene del plan principal | Por qué lo necesita el websocket |
|---|---|
| Fase 1: un solo drawer (`DrawerRight` + `formRegistry`), con armazón `drawer-form` | Los avisos en vivo («Despachado por…») se muestran en un único lugar por acción |
| Fases 2.3 y 2.5: `StatusSelect` común y estado de empleados con dropdown | `entity.status_changed` actualiza un solo componente en todas las tablas |
| Fase 4: edición completa y precarga desde `GET /:id` | `row` en los eventos usa el mismo DTO del detalle |
| Fase 5: cada mutación aplica su respuesta en el slice; listener de invalidación; revalidación al volver a la pestaña | Los manejadores reutilizan esas acciones; la revalidación queda como respaldo con el socket caído |
| Punto 6.4: carrito del admin a nombre de su empleado ligado; precio calculado en el servidor | `cart.updated` por canal personal y `stock.changed` coherente |
| Fase 7: `PUT /auth/me` y `PUT /auth/me/password` | Emiten `account.updated` y `session.revoked (password_changed)` |
| Fase 8: catálogo final `module:action`, permisos que fallan cerrados y rutas de lectura protegidas | Cada evento declara permisos que **existen** y el filtro del hub usa la misma regla que la API |
| Fase 9: migraciones aplicadas y `startdb.pgsql` al día | La migración del websocket se suma encima |
| Fase 2.4: desbloquear empleado retirado | No existe ningún evento para ese caso |

### 9.1 Etapa P · Verificación del punto de partida (antes de E0)

- **Objetivo:** confirmar que el código real coincide con la tabla 9.0. Si algo no coincide, se **ajusta este plan** antes de construir; no se corrige el plan principal desde aquí.
- **Rama:** ninguna (solo lectura).

**Tareas**
1. `go build ./... && go test ./...` en `importadoraApi` y `pnpm exec tsc -b && pnpm lint && pnpm test` en `importadoraInterfaz`, todo en verde.
2. Releer las rutas que modifican datos (`grep -rnE '\.(POST|PUT|PATCH|DELETE)\(' importadoraApi/src/features/*/routes`) y compararlas con la sección 6.3. Agregar al catálogo cualquier ruta nueva y quitar las retiradas (por ejemplo `/employees/:id/unlock`, `/staff`).
3. Leer el catálogo de permisos final (`startdb.pgsql`, bloque «CATÁLOGO DE PERMISOS») y ajustar la columna de permisos de la sección 6.3 y `entityPermissions` (7.2) a los nombres reales.
4. Contar de nuevo las transacciones manuales (`grep -rn "\.Begin(\|BeginTx(" importadoraApi/src/features`) y las auditorías después del commit. Actualizar las cifras de E6.
5. Revisar la firma actual de `db.WithTx` y de los escritores de stock (puede que el plan principal ya los haya tocado).
6. Revisar qué thunks de mutación del front todavía hacen GET de seguimiento (`grep -rn "dispatch(load\w*Thunk" importadoraInterfaz/src/features/*/thunks`) y actualizar la tabla 12.4.
7. Confirmar el nombre final de los slices y acciones que dejó la Fase 5, para usarlos en los manejadores.

**Criterio de salida:** las secciones 6.3, 7.2, 12.4 y las cifras de E6 actualizadas con lo encontrado, y esta tabla 9.0 marcada como «verificada» con la fecha.

#### 9.1.1 Resultado de la etapa P (11-nov-2026) · **verificada, con ajustes**

| # | Tarea | Resultado | Ajuste aplicado a este plan |
|---|---|---|---|
| 1 | Verificaciones en verde | API: `go build`, `go vet` y `go test` en verde. Front: `tsc -b`, `lint`, `test` (116 archivos, 758 pruebas), `check:api` (134/134) y `check:type` en verde. | — |
| 2 | Rutas que modifican datos vs 6.3 | Todas cubiertas salvo dos. `/employees/:id/unlock` y `/staff` ya no existen. | `PUT /auth/me/password` agregada a `session.revoked`. Ruta de cambio de contexto corregida a `/audit-logs/context-change`. |
| 3 | Catálogo de permisos final | `core/types/permission_types.go` es la fuente única: 22 módulos × (`view`, `view_detail`, `create`, `edit`, `delete`) + 16 acciones especiales; `change_status` y `update_prices` retirados; `screensaver:edit` solo admin/superadmin. Los eventos filtran por llaves de **lectura**, que no cambiaron. | `location` suma `transfers:create` y `products:view` (los mismos lectores que su ruta GET). `entityPermissions` (7.2) se escribe desde la tabla 6.3-C ya ajustada. |
| 4 | Transacciones y auditorías | **19** usos de `db.WithTx` y **21** transacciones manuales (`Begin`) en 13 servicios (`sale_stage_service.go` tiene 6). **77** `auditRepo.Insert`. | Cifras de E6: 21 transacciones a migrar. |
| 5 | `db.WithTx` y escritores de stock | La firma sigue `fn func(tx pgx.Tx) error` (`core/db/postgres.go`). Escriben `stock`: `inventory_repo.go`, `return_repo.go` y `warehouse_location_repo.go`. Sin cambios respecto al plan. | — |
| 6 | GET de seguimiento en thunks | **34** `dispatch(load…Thunk)` tras mutaciones en 11 archivos (bank, customers, employees, orders, returns, shop, superadmin, transfers, wallet). Además la Fase 5 dejó `app/store/crossSliceInvalidationMiddleware.ts`, que tras `checkout/*`, `orders/advanceStage*` y `transfers/receive*` vuelve a pedir pedidos, conteos y la tabla de productos (3 GET por mutación). | E8–E9 retiran esos GET y el listener de invalidación queda solo como respaldo con el socket caído. |
| 7 | Slices y acciones de la Fase 5 | No existe la «revalidación al volver a la pestaña» que 9.0 daba por hecha (`visibilitychange` solo se usa en `useSessionActivity`). `useNotifications.ts` sigue con `setInterval` y `useBackups.ts` con `window.location.reload()`. | **E7 agrega** `features/realtime/hooks/useRevalidateOnFocus.ts` (solo actúa con el socket caído). |
| — | Hallazgos extra | `features/realtime` ya existe pero es un prototipo: manda el token en `?token=` (queda en logs y proxies), usa `tipo/cuerpo/idCliente` y un solo manejador (`applyStockUpdate`). `VITE_WS_URL` está vacío en `.env` y `.env.example`. La Fase 8 dejó `useCurrentUserRefresh` y `can()` en `usePermissions`. | F1–F4 reemplazan el prototipo (ticket de un solo uso, sin token en la URL). El manejador de `permissions.changed` reutiliza `setCurrentUser` / `fetchCurrentUserThunk`; los botones ya leen `can()`, así que el menú y las acciones se actualizan solos. |

**Pendiente heredado del plan principal (no bloquea E0–E7, sí E8):** las migraciones `20261127` y `20261128` todavía no se aplicaron a una base real (no había `psql` en el equipo); la regresión total con base de prueba y el e2e `flow_permissions_matrix.py` quedaron sin correr. E1 se escribe encima de esas migraciones.

### 9.2 Mapa de dependencias entre etapas

```
P verificación ─► E0 contrato ─► E1 BD ─┬─► E2 productor API ──► E6 WithTx + RETURNING ─┐
                                        └─► E3 esqueleto ─► E4 conexión ─► E5 listener ─┤
                                        └─► E7 front base ──────────────────────────────┴─► E8 piloto ─► E9 dominios ─► E10 conflictos ─► E11 cierre
```

| Etapa | Depende de | Puede ir en paralelo con |
|---|---|---|
| P | Plan principal terminado | — |
| E0 | P | — |
| E1 | E0 | — |
| E2 | E1 | E3, E4, E5, E7 |
| E3 → E4 → E5 | E1 | E2, E6, E7 |
| E6 | E2 | E3–E5, E7 |
| E7 | E0 (contrato) | E2–E6 |
| E8 | E5, E6 y E7 | — |
| E9 → E10 → E11 | E8 | — |

Las etapas en paralelo tocan directorios distintos (`importadoraApi`, `importadoraRealtime` e `importadoraInterfaz`), así que no se pisan.

---

### E0 · Contrato v1 congelado

- **Objetivo:** que el contrato sea la fuente única antes de escribir código.
- **Requisitos:** etapa P cerrada y decisiones 1–4 de la sección 11 cerradas.
- **Rama:** `feat/realtime-e0-contrato`.
- **Toca:** solo `importadoraRealtime/contract/`.

**Tareas**
1. `contract/REALTIME-CONTRATO.md`, con:
   - el sobre;
   - los canales (6.1) y la convención de nombres (6.2);
   - el catálogo completo (6.3) en tablas;
   - la matriz de consumidores (6.4);
   - los códigos de cierre (14.6);
   - las reglas de versión: un campo nuevo opcional no sube `v`; quitar o renombrar un campo, o cambiar su tipo, **sí** sube `v`.
2. `contract/CHANGELOG.md` con la entrada `v1 — 2026-11-XX — versión inicial (48 tipos)`.
3. `contract/schemas/envelope.schema.json` (draft 2020-12):
   - `required`: `v, seq, id, type, occurredAt, companyId, warehouseIds, actor, data`;
   - `v`: `const: 1`; `type`: `enum` con los 48 tipos;
   - `additionalProperties: false` en el sobre (no en `data`).
4. `contract/schemas/client-message.schema.json`: `oneOf` entre `hello` (`v`, `lastSeq`: integer o null) y `pong` (`ts`: integer), con `maxLength` implícito por el límite de 4 KB.
5. `contract/schemas/events/<tipo>.schema.json`, uno por tipo (48). Reglas:
   - Todos los campos de la sección 6.5 son `required`, aunque su valor pueda ser `null`.
   - Los enums de estado son `enum` cerrados.
   - `row`, `sale`, `payment`, `purchase`, `return`, `quotation`, `cart` y `user` son `type: object` sin forma fija: su forma es el DTO de la API, validado por las pruebas de la API.
6. `contract/examples/<tipo>.json`, un ejemplo **completo** por tipo, con valores realistas de la semilla de `startdb.pgsql` (compañía 1, bodegas reales, productos reales).
7. `contract/scripts/validate.go` (programa Go pequeño dentro de `importadoraRealtime`, con `github.com/santhosh-tekuri/jsonschema/v6`): valida cada ejemplo contra el sobre y contra su schema, y comprueba que existan exactamente 48 schemas y 48 ejemplos con los mismos nombres.

**Pruebas**
- `go run ./contract/scripts/validate.go` → «48/48 válidos».

**Criterio de salida:** revisión tuya del catálogo y el contrato marcado como **congelado v1** en el `CHANGELOG`.

**Riesgos:** faltan eventos. *Mitigación:* repasar la tabla de rutas que modifican datos de la sección 6.3 contra `importadoraApi/src/features/*/routes` una última vez.

**Deshacer:** solo son documentos.

---

### E1 · Base de datos

- **Objetivo:** tablas, rol y versión por fila listos en la base de datos.
- **Rama:** `feat/realtime-e1-bd`.
- **Toca:** `importadoraApi/src/core/db/migrations/`, `importadoraApi/src/core/db/startdb.pgsql` y `Idea/startdb.pgsql` (según la regla 5 del plan principal).

**Tareas**
1. Tablas **sin** `updated_at` (revisado el 11-nov-2026 en `startdb.pgsql`): `brands`, `models`, `categories`, `roles`, `product_suppliers`, `counts`, `doc_series` y `employee_permissions`, más 7 tablas de documentos cuyos eventos llevan `version` en el contrato: `invoices`, `returns`, `payments`, `bank_statement_records`, `cash_sessions`, `customer_documents` y `product_codes` (15 en total). Además `product_prices` tenía la columna pero ningún trigger. La base **ya tenía** `fn_set_updated_at()` con 19 triggers, pero con `CURRENT_TIMESTAMP`: se reemplaza por `clock_timestamp()` y no se crea una función nueva. Todas reciben la columna y el trigger, porque la decisión 4 pide versión en **todas** las ediciones. Ya la tienen: `companies`, `admins`, `superadmins`, `employees`, `customers`, `externals`, `products`, `product_prices`, `quotations`, `warehouses`, `warehouse_facturation` y `warehouse_locations`.
2. Crear `migrations/2026112X_realtime.sql` (fecha siguiente libre), idempotente, en este orden:
   1. función `set_updated_at()`: `BEFORE UPDATE … NEW.updated_at = clock_timestamp()`. Se usa `clock_timestamp()` y no `now()`, para que dos cambios en la misma transacción tengan versiones distintas;
   2. `ALTER TABLE … ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP` donde falte;
   3. `CREATE TRIGGER trg_<tabla>_updated_at` en cada tabla de la lista, con `DROP TRIGGER IF EXISTS` antes;
   4. `realtime_events`, completa (sección 5): `seq`, `id`, **`tx_id xid8 NOT NULL DEFAULT pg_current_xact_id()`** (14.7), `company_id`, `warehouse_ids`, `actor_channel`, `platform`, `type`, `permissions`, `actor_type`, `actor_id`, `actor_name`, `correlation_id`, `payload` y `created_at`, más sus índices;
   5. `realtime_tickets` y su índice;
   6. rol `importadora_realtime` y sus `GRANT` (sección 5). La contraseña se define al desplegar, nunca en el script;
   7. cabecera con el porqué y la advertencia de no ejecutar `startdb.pgsql` sobre bases existentes.
3. Replicar todo en `startdb.pgsql`, dentro de los grupos correspondientes, con su comentario numerado igual que las demás tablas.
4. Si hay servicios que hoy hacen `SET updated_at = CURRENT_TIMESTAMP` a mano, se dejan: el trigger lo sobrescribe con `clock_timestamp()`.

**Pruebas (manuales, con evidencia en el PR)**
1. Base A: crear con el `startdb.pgsql` **anterior** y aplicar la migración.
2. Base B: crear con el `startdb.pgsql` **nuevo**.
3. `pg_dump --schema-only` de A y de B → `diff` vacío (salvo el orden de los objetos).
4. Conectado como `importadora_realtime`: `INSERT INTO customers …` → `permission denied`; `SELECT * FROM realtime_events` → OK; `UPDATE realtime_tickets SET used_at = now()` → OK; `UPDATE realtime_tickets SET actor_id = 1` → `permission denied`.
5. `UPDATE brands SET name = name WHERE id = 1` dos veces en la misma transacción → `updated_at` distinto cada vez.

**Criterio de salida:** las pruebas 1–5 en verde y la evidencia guardada en `migrations/README.md`.

**Riesgos:**
- El trigger cambia `updated_at` en tablas que algún flujo usaba para otra cosa. *Mitigación:* `grep -rn "updated_at" importadoraApi/src` y revisar cada uso.

**Deshacer:** un script `migrations/2026112X_realtime_down.sql` con `DROP TRIGGER`, `DROP TABLE realtime_tickets` y `DROP TABLE realtime_events`, y `DROP ROLE` tras revocar permisos. Las columnas `updated_at` se dejan: no hacen daño.

---

### E2 · Productor base en la API (todavía sin emitir eventos de negocio)

- **Objetivo:** toda la maquinaria para guardar eventos, más el ticket.
- **Rama:** `feat/realtime-e2-productor`.
- **Toca:** `importadoraApi/src/core/{db,events,bootstrap,config,server}`, `importadoraApi/src/features/realtime`, `importadoraApi/.env.example` y `docs/swagger.json`.

**Tareas**
1. `core/db/txhooks.go`:
   - `type txHooksKey struct{}`; `type BeforeCommit func(ctx context.Context, tx pgx.Tx) error`;
   - `func withHookRegistry(ctx) context.Context`;
   - `func OnBeforeCommit(ctx, fn)` (agrega a la lista; si no hay registro, devuelve error `ErrNoTxContext`);
   - `func runBeforeCommit(ctx, tx) error` (ejecuta en orden y se detiene en el primer error).
2. `core/db/postgres.go` · `WithTx`:
   - crea `ctx2 := withHookRegistry(ctx)`, ejecuta `fn` con `ctx2` y, si no hay error, `runBeforeCommit(ctx2, tx)` antes de `Commit`.
   - **Cambio de firma:** `fn func(ctx context.Context, tx pgx.Tx) error`. Hoy es `fn func(tx pgx.Tx) error`, y los 18 usos se ajustan aquí mismo: es un cambio mecánico y lo verifica el compilador. Así el `ctx` con el registro llega a los servicios.
3. `core/server/server.go`: agregar `e.Use(middleware.RequestID())`. Es el **único** cambio en el servidor, y sirve para el `correlationId`.
4. `core/events/types.go`: todo lo de la sección 7.1.
5. `core/events/catalog.go`:
   - `routes map[Type]route` con **las 48 entradas** y `entityPermissions map[Kind][]string` con **los 19 kinds** de datos maestros (tabla 6.3-C);
   - `func RouteFor(t Type, k Kind) route`.
6. `core/events/constructors.go`: un constructor por tipo emitido por la API (44, sin los 4 `system.*`). Firmas de ejemplo:
   - `EntityCreated(kind Kind, companyID int64, warehouseID *int64, row any, version time.Time, id int64) Event`
   - `EntityStatus(kind, companyID, warehouseID, id, version, from, to string, reason *string, row any) Event`
   - `Stock` no tiene constructor: va por `Recorder.Stock`.
   - `TransferCreated(row TransferRow) Event`
   - `TransferStatus(row TransferRow, from, to string, by Actor) Event` (canales: origen y destino)
   - `SaleStage(...)`, `PaymentStatus(...)`, `CreditBalance(...)`, … uno por tipo.
   - Cada constructor fija el `Type`, los canales (de `RouteFor`) y los permisos. **Los servicios nunca arman `Event{}` a mano.**
7. `core/events/actor.go`:
   - `func ActorFromContext(ctx) Actor`: toma `middlewares.ActorContext` del `context`, que el controlador pasa con `c.Request().Context()`. Para eso el middleware `Authenticate` deja el actor también en el `context.Context` de la petición, con `context.WithValue`.
   - Nombre: caché en memoria `actorNames` (mapa con un TTL de 10 min) que, si no lo tiene, consulta `employees`/`admins`/`superadmins` **una vez**.
   - `system` para eventos sin actor (tareas programadas).
8. `core/events/recorder.go`:
   - `type Recorder struct { repo *Repository; names *nameCache }`;
   - `func (r *Recorder) Record(ctx, e Event)`: obtiene o crea el `buffer` del `ctx` (registrándolo con `db.OnBeforeCommit` en la primera llamada) y agrega `e`;
   - `func (r *Recorder) Stock(ctx, row StockRow, cause StockCause)`: agrega al mapa de stock del buffer.
   - **Fuera de una transacción** (`ErrNoTxContext`), `Record` **falla en desarrollo** (`panic` con `APP_ENV=development`) y registra un error en producción. Así ningún evento se emite fuera de una transacción.
9. `core/events/buffer.go`:
   - `type buffer struct { events []Event; stock map[int64]stockEntry; causes []StockCause; actor Actor; correlationID string }`;
   - `flush(ctx, tx)`:
     1. arma los `stock.changed`: agrupa por causa, conserva la última versión de cada `stockId`, calcula `totals` por producto y bodega (`SUM` sobre las filas que estén en el buffer **más** un `SELECT` de las demás ubicaciones de ese producto en esa bodega, dentro de la misma transacción) y parte en lotes de 200;
     2. completa `actor` y `correlationId` en cada evento;
     3. `repo.InsertBatch(ctx, tx, events)` → `RETURNING seq`;
     4. `repo.Notify(ctx, tx, maxSeq)`.
10. `core/events/repository.go`:
    - `InsertBatch`: un solo `INSERT … VALUES (…),(…) RETURNING seq`, o `CopyFrom` si son más de 50 filas;
    - `Notify`: `SELECT pg_notify('erp_events', $1::text)`.
11. `core/events/doc.go`: comentario del paquete con la regla de oro: «se llama dentro de `db.WithTx`; nunca después del commit».
12. `features/realtime/` (ticket):
    - `routes/realtime_routes.go`: `POST /realtime/ticket` con `Authenticate()` y sin `RequirePermission`, porque todo actor autenticado puede conectarse; el filtro es por evento.
    - `services/ticket_service.go`: `crypto/rand` 32 bytes → `base64url` (ticket) → `sha256` hex (hash) → `INSERT` con actor, compañía, bodega, sesión y `expires_at = now() + TTL`. Devuelve `{ ticket, wsUrl, expiresAt }`. Límite: **5 tickets por minuto por sesión** (contador en memoria), para evitar abuso.
    - `repositories/ticket_repo.go`, `dtos/realtime_dto.go` y `controllers/realtime_controller.go`.
13. `core/bootstrap/container.go`: `events.NewRecorder(pool)` inyectado en **todos** los servicios que modifican datos (se agrega el campo `events *events.Recorder` en cada `struct` de servicio, sin usarlo todavía).
14. `core/config`: `REALTIME_TICKET_TTL_SEC` (por defecto 30, rango 10–120) y `REALTIME_WS_URL` (obligatoria si `APP_ENV=production`).
15. `../importadoraApi/docs/swagger.json` y `../importadoraApi/src/features/realtime`: documentación y registro de `POST /realtime/ticket`.

**Pruebas (Go, contra la base local de prueba)**
- `core/db/txhooks_test.go`: el gancho corre antes del commit; si el gancho falla, se hace rollback y la fila de negocio no queda; sin `WithTx`, `OnBeforeCommit` devuelve `ErrNoTxContext`.
- `core/events/buffer_test.go`:
  - `TestRollbackLeavesNoEvent`;
  - `TestStockCoalescesByStockID` (3 cambios de la misma fila → 1 fila con el último valor);
  - `TestStockBatchesOf200` (450 filas → 3 mensajes con el mismo `correlationId`);
  - `TestTotalsIncludeOtherLocations`.
- `core/events/catalog_test.go`:
  - `TestEveryTypeHasRoute` (48);
  - `TestEveryKindHasPermissions` (19);
  - `TestPermissionsExistInSeed` (lee `startdb.pgsql`, como hace `routes_guard_test.go`, y comprueba cada `module:action`).
- `core/events/contract_test.go`: para cada `../../../../importadoraRealtime/contract/examples/*.json`, decodifica `data` en el struct del tipo con `DisallowUnknownFields` → sin error, y lo vuelve a codificar sin perder campos.
- `features/realtime/services/ticket_service_test.go`: el hash se guarda y el ticket en claro no; vence a los TTL segundos; se respeta el límite de 5 por minuto.

**Criterio de salida:** todo `go test ./...` de la API en verde, y `go build` sin usos de `Record` todavía, salvo las pruebas.

**Riesgos:**
- El cambio de firma de `WithTx` toca muchos servicios. *Mitigación:* es mecánico y el compilador muestra todos los puntos; va en un commit propio dentro del PR para revisarlo aparte.

**Deshacer:** revertir el PR. Nada emite todavía.

#### Resultado de E2 (11-nov-2026) · **hecha**

- **Hecho:** `core/db/txhooks.go` (+ `TxLocal` para datos por transacción) y `WithTx(ctx, pool, func(ctx, tx))` con los 18 usos ajustados; `middleware.RequestID` (también en el log) y el actor y el id de petición en el `context.Context` (`middlewares.WithActor/ActorFrom`, `WithRequestID/RequestIDFrom`); `core/events/` (`types.go` con los 48 cuerpos, `catalog.go` con 48 rutas y 19 kinds, `constructors.go`, `causes.go`, `actor.go`, `buffer.go`, `repository.go`, `recorder.go`, `doc.go`); `features/realtime/` con `POST /realtime/ticket`; configuración `REALTIME_EVENTS_ENABLED`, `REALTIME_TICKET_TTL_SEC` (10–120) y `REALTIME_WS_URL` (obligatoria en producción); swagger y matriz.
- **Pruebas:** `catalog_test` (48 tipos, 19 kinds, permisos sembrados), `contract_test` (los 48 ejemplos del contrato entran y salen de los structs sin perder ni sobrar campos), `txhooks_test` y `buffer_test` contra PostgreSQL (rollback sin eventos, stock agrupado por fila con su valor final, 250 filas → 2 mensajes, totales con todas las ubicaciones, `pg_notify` al confirmar) y `ticket_service_test`. Además, contra una API local: ticket 201 ×5 y 429 al sexto, 401 sin sesión, solo el sha256 en la base; y los 10 flujos de `scripts/e2e` en verde.
- **Ajustes respecto a lo escrito arriba:**
  1. `Recorder.Stock(ctx, stockID, causa)` recibe solo el **id** y relee las filas **finales** dentro de la transacción al guardar. El evento siempre lleva el valor final aunque la fila cambie varias veces, y los escritores de stock no necesitan devolver la fila (E6 se simplifica: no hace falta `RETURNING` en los repositorios de stock).
  2. Constructores con nombre para los tipos con reglas propias (`entity.*`, traslados, auditoría, personales) y `events.New(tipo, Scope, cuerpo)` para el resto: aplican la misma tabla del catálogo, así que se cumple la regla «nunca `Event{}` a mano» sin 44 funciones casi iguales.
  3. El `Recorder` vive en el contenedor (`AppContainer.Events`) y se inyecta en cada servicio **cuando empieza a emitir** (E8–E9), en vez de agregar ahora un campo sin uso en todos.
  4. `admins:view` se suma a las llaves de gobernanza (no se siembra a empleados): la usan los eventos de administradores.
- **Hallazgos al correr los e2e:** los flujos daban permisos con las llaves anteriores a la Fase 8 (`payments:edit` para verificar, `view` sin `view_detail`); se actualizaron a `payments:verify` y `view_detail`.

---

### E3 · Esqueleto de `importadoraRealtime`

- **Rama:** `feat/realtime-e3-esqueleto`.
- **Toca:** solo `importadoraRealtime/` (excepto `contract/`).

**Tareas**
1. `go mod init importadoraRealtime`; `go get github.com/jackc/pgx/v5@v5.11.0 github.com/joho/godotenv@v1.5.1 github.com/coder/websocket@<versión estable con al menos dos semanas>`. Las versiones de pgx y godotenv son las mismas de la API.
2. `.gitignore` (`.env`, binarios, `coverage.out`) y `.env.example` (sección 14.8).
3. `src/core/config/{config.go,loader.go,validators.go}`, con las variables y validaciones de la tabla 14.8.
4. `src/core/logging/logger.go`: `slog.NewJSONHandler` (texto legible si `APP_ENV=development`); funciones `WithConn(connId)` y `WithEvent(seq, type)`.
5. `src/core/db/postgres.go`: `pgxpool` con `MaxConns = REALTIME_DB_MAX_CONNS` (por defecto 10) y `Ping` con tiempo límite de 10 s.
6. `src/core/server/server.go`:
   - `http.Server{ ReadHeaderTimeout: 5s, IdleTimeout: 60s }` (sin `WriteTimeout` global: rompería los sockets);
   - `ServeMux` con `GET /health` y `GET /ws` (este último devuelve 503 hasta E4);
   - apagado ordenado: deja de aceptar conexiones, el hub cierra todas con 1001 y espera hasta 10 s.
7. `src/core/server/origin.go`: lista de orígenes permitidos desde la configuración, con comparación exacta (esquema + host + puerto). `*` está prohibido salvo en `development`.
8. `src/features/health/*`: `GET /health` → `{ "status": "ok", "db": "ok", "listener": "n/a", "connections": 0, "lastSeq": null, "lagSeq": null, "version": "v1" }`; responde 503 si la BD falla.
9. `src/core/bootstrap/{container.go,router.go}` y `main.go` (orden de la sección 8.1).
10. `README.md`: qué es, diagrama de la sección 0, cómo correrlo, variables y usuario de BD.

**Pruebas**
- `config/validators_test.go`: falta la BD → error; orígenes vacíos en producción → error; `*` en producción → error.
- `server/origin_test.go`: coincidencia exacta; `http://localhost:5173` frente a `http://localhost:5174` → rechazado.
- `health/services/health_service_test.go`: la BD caída responde 503.
- Manual: `go run .` → `curl localhost:3002/health` → 200; `Ctrl+C` → apagado en menos de 10 s.

**Criterio de salida:** `go build ./... && go vet ./... && go test ./...` en verde dentro de `importadoraRealtime`.

**Deshacer:** borrar el directorio.

#### Resultado de E3 (11-nov-2026) · **hecha**

- **Hecho:** `go.mod` con `pgx/v5 v5.11.0` y `godotenv v1.5.1` (las mismas de la API); `.gitignore`, `.env.example`, `README.md`; `src/core/{config,logging,db,server,bootstrap}`; `features/health`; `main.go` con el orden de arranque y apagado de 14.1.
- **Puerto:** escucha en `PORT` (Railway) y usa `APP_PORT` solo si `PORT` no existe (decisión 10).
- **`/health`:** usa los campos de 14.9 (`lastProcessedSeq`, `visibleSeq`, `lagSeq`, `queueOverflows`, `uptimeSec`), con `listener: "n/a"` hasta E5; el listener y el hub se conectan por interfaces (`ListenerStatus`, `HubStats`).
- **`coder/websocket`:** se agrega en E4, cuando se acepta la primera conexión (E3 no la usa).
- **Pruebas:** validaciones de configuración (12 casos inválidos, `*` solo en desarrollo), orígenes con coincidencia exacta (10 casos), salud (BD caída, listener caído y retraso → 503) y apagado ordenado (`OnShutdown` corre y termina en < 10 s). Manual contra `importadora_test`: `/health` 200 con `db: ok`, `/ws` 503, ruta inexistente 404, `POST /health` 405 y configuración inválida → código de salida 1 con el motivo.

---

### E4 · Conexión, ticket, acceso y hub

- **Rama:** `feat/realtime-e4-conexion`.
- **Toca:** `importadoraRealtime/src/{core/protocol,core/hub,features/tickets,features/access,features/connection}`.

**Tareas** (la especificación exacta está en la sección 14):
1. `core/protocol/close_codes.go`: constantes de la tabla 14.6.
2. `core/protocol/envelope.go`: `StoredEvent`, `Envelope` y `func (s StoredEvent) Envelope() Envelope`.
3. `core/protocol/client_messages.go`: `ParseClientMessage([]byte) (ClientMessage, error)` (JSON estricto, 4 KB máximo; un tipo desconocido es error).
4. `core/protocol/system_messages.go`: `Welcome(...)`, `ReplayCompleted(...)` y `ResyncRequired(...)`, con `seq: 0` y `actor: system`.
5. `features/tickets/repositories/ticket_repo.go` y `services/ticket_service.go` (14.2, paso 2).
6. `features/access/repositories/{session_repo.go,actor_repo.go,permission_repo.go}`: copiar **literalmente** las consultas de `importadoraApi/src/features/auth/repositories/auth_session_repo.go` (`IsSessionActive`, `IsActorAvailable` y `LoadPermissionsForActor`), con un comentario `// ojo: copia de … — si cambia allá, cambiar aquí`.
7. `features/access/services/access_service.go` y `dtos/access_context.go`:
   - `AccessContext{ ActorType, ActorID, CompanyID, WarehouseIDs []int64, SessionID, Permissions map[string]bool, IsAdmin, IsSuperadmin }`;
   - las bodegas del admin salen de `SELECT id FROM warehouses WHERE company_id = $1 AND is_active`.
8. `core/hub/channels.go`: `ChannelsFor(AccessContext) []string` (tabla 14.3).
9. `core/hub/filter.go`: `Allowed(client, StoredEvent) bool` (algoritmo 14.4).
10. `core/hub/queue.go`: cola con canal de Go con buffer `REALTIME_MAX_CLIENT_QUEUE`; si `Push` encuentra la cola llena, devuelve `false` y el hub cierra con 4408.
11. `core/hub/client.go`: `readLoop` (solo `hello`/`pong`; `ReadLimit` de 4 KB) y `writeLoop` (cola + ping cada `REALTIME_PING_SEC`; sin respuesta en `REALTIME_PONG_TIMEOUT_SEC` → cierre 1011).
12. `core/hub/hub.go`: modelo de concurrencia de la sección 14.5.
13. `core/hub/metrics.go`: contadores con `atomic`.
14. `features/connection/*`: saludo de la sección 14.2 (pasos 1–6; la recuperación del paso 7 llega en E5). Sin `hello` a los 5 s → cierre 4400.
15. `core/server/limits.go`: máximo de `REALTIME_MAX_CONN_PER_ACTOR` (por defecto **5**, decisión 6) conexiones por actor; la 6.ª cierra la **más vieja** con 4429. Máximo de 20 intentos de conexión por minuto por IP.

**Pruebas**
- `hub/channels_test.go`: empleado → `company:1`, `warehouse:7`, `actor:employee:18`; admin → `company:1` + todas sus bodegas + `actor:admin:4`; superadmin → solo `platform` + `actor:superadmin:1`, aunque haya elegido compañía de contexto (decisión 2).
- `hub/filter_test.go`, tabla de casos:
  - otra compañía → no;
  - otra bodega en un evento de bodega → no;
  - mismo canal y sin el permiso → no;
  - mismo canal y uno de los permisos → sí;
  - admin → sí;
  - superadmin en un evento de lectura → sí;
  - evento de actor para otra persona → no.
- `hub/queue_test.go`: la cola llena devuelve `false`.
- `hub/hub_test.go`: registrar y quitar con `-race`; `CloseSession` cierra todas las pestañas de esa sesión y no otras.
- `tickets/services/ticket_service_test.go` (con BD): válido → identidad; reutilizado → `ErrTicketUsed`; vencido → `ErrTicketExpired`.
- `connection/services/connection_service_test.go` (con `httptest` + `coder/websocket` como cliente): ticket válido → `system.welcome` con los canales correctos; ticket inválido → cierre 4001; `Origin` inválido → 4003; sin `hello` → 4400.

**Criterio de salida:** pruebas en verde con `go test -race ./...`, y un cliente manual conectado con un ticket real de la API de E2.

#### Resultado de E4 (11-nov-2026) · **hecha**

- **Hecho:** `core/protocol/` (`close_codes.go`, `envelope.go`, `client_messages.go`, `system_messages.go`); `features/tickets/` (`Consume` atómico por `sha256` con motivo exacto `used` / `expired` / `not_found`); `features/access/` (consultas literales de sesión, disponibilidad y permisos de la API + bodegas activas para `admin`, y `superadmin` sin compañía ni bodegas por decisión 2); `core/hub/` (`channels.go`, `filter.go`, `queue.go`, `client.go`, `hub.go`, `metrics.go` con una sola goroutine dueña de los índices y serialización única por evento); `core/server/limits.go` (20 intentos/min por IP y máximo 5 pestañas por actor cerrando la más vieja con `4429`); `features/connection/` (saludo completo de 14.2 hasta `live`, encolando eventos desde el registro y vaciándolos después del `hello`).
- **Pruebas:** `protocol_test` (mensajes del cliente, mensajes de sistema validados contra los JSON Schema del contrato y conversión `StoredEvent → Envelope`), `hub_test` (`ChannelsFor` incluyendo superadmin con contexto, `Allowed` en tabla de 12 casos, cola llena, registro/baja concurrente de 200 clientes, `CloseSession`, 6.ª pestaña → `4429`, `Dispatch` con filtro y `4408`, `CloseAll` → `1001`, y `WriteLoop` saltando `seq` ya enviados), `ticket_service_test` y `access_service_test` contra `importadora_test` (consumo único, reusado, vencido, empleado/admin/superadmin, sesión revocada y cuenta inactiva), y `connection_test` con `httptest` + `coder/websocket` (`system.welcome` con canales, evento encolado antes del `hello` entregado tras el `hello`, `4001`, `4003`, `4400` por timeout o mensaje desconocido).
- **Punta a punta:** con `importadoraApi` (`:3091`) e `importadoraRealtime` (`:3002`) sobre `importadora_test`: login → `POST /realtime/ticket` (201) → upgrade `101 Switching Protocols` → `system.welcome` → `hello` → `/health` reporta `connections: 1`; reusar el mismo ticket cierra con `4001` (`ticket_invalid`).

---

### E5 · Listener, recuperación, eventos de control y purga

- **Rama:** `feat/realtime-e5-listener`.
- **Toca:** `importadoraRealtime/src/{core/db/listen_conn.go,features/events,features/connection/services/replay_service.go}`.

**Tareas**
1. `core/db/listen_conn.go`: conexión dedicada (`pgx.Connect`, **no** del pool) + `LISTEN erp_events`; reconexión con espera creciente (0,5 s → 30 s).
2. `features/events/repositories/event_repo.go`:
   - `ReadVisibleAfter(seq, limit)` (`WHERE seq > $1 AND tx_id < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY seq LIMIT $2`, ver 14.7);
   - `ReadAfterForScope(seq, companyIDs, limit)`;
   - `CurrentVisibleSeq()` (`MAX(seq)` con la misma condición de visibilidad);
   - `OldestSeq()`;
   - `PurgeOlderThan(d)`.
3. `features/events/services/listener_service.go` (algoritmo 14.7).
4. `features/events/services/control_events.go`:
   - `session.revoked` → `hub.CloseSession(sessionId, 4401)`, o `hub.CloseActor(actor, 4401)` si `sessionId` es nulo;
   - `permissions.changed` → `access.Reload(actor)` → `hub.ReloadPermissions(actor, perms)`;
   - `backup.restored` → `hub.BroadcastResync(scope, "backup_restored")`.
   - El evento se procesa **antes** de repartirse, y también se reparte (el front lo necesita).
5. `features/connection/services/replay_service.go` (algoritmo 14.2, paso 7).
6. `features/events/services/purge_service.go`: un `ticker` de 1 h; al arrancar, una vez; registra cuántas filas borró.
7. `features/health`: completar `listener`, `lastProcessedSeq`, `visibleSeq` y `lagSeq` (`CurrentVisibleSeq − lastProcessed`).
8. `core/hub`: envío periódico de `system.watermark` (cada `REALTIME_WATERMARK_SEC`) a los clientes `live`.

**Pruebas** (con BD; los eventos se insertan con un ayudante de prueba que imita a la API: `INSERT` + `pg_notify`)
- `listener_service_test.go`:
  - `TestDeliversInOrder` (100 eventos → llegan en orden de `seq`);
  - `TestOutOfOrderCommit` (prueba obligatoria 17.3);
  - `TestWatermarkSent` (sin eventos para el cliente, recibe `system.watermark` al avanzar `lastProcessed`);
  - `TestBackupRestoreResets` (la tabla vuelve atrás → `resync_required` a todos);
  - `TestRecoversAfterListenDrop` (cerrar la conexión `LISTEN` → reconecta y no pierde eventos insertados mientras tanto);
  - `TestBatchRead` (1.000 eventos → lecturas de 500).
- `replay_service_test.go`:
  - `TestReplayExact` (`lastSeq` = 10 y existen 11–20 → recibe 11–20 y `replay_completed`);
  - `TestReplayFilteredByScope`;
  - `TestResyncWhenPurged`;
  - `TestResyncWhenTooMany`;
  - `TestLiveEventsQueuedDuringReplay` (orden correcto).
- `control_events_test.go`: `session.revoked` cierra solo esa sesión; `permissions.changed` hace que el siguiente evento sin permiso ya no llegue.
- `purge_service_test.go`.

**Criterio de salida:** el servicio está **completo sin depender de que la API emita eventos reales**, y `/health` muestra `lagSeq` en 0 en reposo.

#### Resultado de E5 (11-nov-2026) · **hecha**

- **Hecho:** `core/db/listen_conn.go` (conexión dedicada `pgx.Connect` fuera del pool con `LISTEN erp_events` y espera creciente `0,5 s → 30 s`); `features/events/repositories/event_repo.go` (`ReadVisibleAfter` y `ReadAfterForScope` con `tx_id < pg_snapshot_xmin(pg_current_snapshot())`, `CurrentVisibleSeq`, `OldestSeq`, `PurgeOlderThan`); `features/events/services/listener_service.go` (algoritmo 14.7, lectura por lotes de 500, detección de retroceso de secuencia por respaldo restaurado, `system.watermark` periódico y métricas `ListenerStatus` para `/health`); `features/events/services/control_events.go` (`session.revoked` → `CloseSession`/`CloseActor`/`CloseActorExcept` con `4401`, `permissions.changed` → recarga en vivo en el hub antes del siguiente evento del lote, `backup.restored` → `BroadcastResync`, y recálculo de canales de bodegas para admins cuando se crea o cambia de estado una bodega); `features/connection/services/replay_service.go` (pasos 7b/7c con páginas de 500, filtro `Allowed`, `system.replay_completed` y `system.resync_required` con `lastSent = currentSeq` para no duplicar eventos encolados en vivo); `features/events/services/purge_service.go` (retención de 48 h al arrancar y cada hora); e integración completa en `bootstrap/container.go` y `main.go`.
- **Pruebas:** `listener_service_test` (`TestDeliversInOrder` con 100 eventos, `TestOutOfOrderCommit` obligatorio de 17.3 con dos transacciones abiertas en desorden, `TestWatermarkSent`, `TestBackupRestoreResets`, `TestRecoversAfterListenDrop` y `TestBatchRead` con 1.000 eventos), `replay_service_test` (`TestReplayExact`, `TestReplayFilteredByScope`, `TestResyncWhenPurged`, `TestResyncWhenTooMany` y `TestLiveEventsQueuedDuringReplay`), `control_events_test` (`session.revoked` por sesión y por actor con `exceptSessionId`, `permissions.changed` efectivo dentro del mismo lote y canales de bodega de admins) y `purge_service_test` (borra eventos de 72 h y conserva los de 1 h). En vivo sobre `importadora_test`, `/health` responde `listener: "ok"` y `lagSeq: 0`.

---

### E6 · Transacciones y escritores de stock (solo la API, sin cambiar el comportamiento)

- **Rama:** `feat/realtime-e6-withtx`.
- **Toca:** los servicios con `s.pool.Begin(ctx)` y los repositorios que escriben `stock`.

**Tareas**
1. Listar con `grep -rn "\.Begin(\|BeginTx(" src/features` (eran 21 antes del plan principal; la etapa P da la cifra real) y pasar cada una a `db.WithTx(ctx, s.pool, func(ctx context.Context, tx pgx.Tx) error {…})`, conservando exactamente el mismo orden de operaciones y el mismo manejo de errores.
2. En las auditorías que van **después** del commit con `s.pool` (eran 66; la etapa P da la cifra real), **no** se cambia nada en esta etapa. Se anota en el PR como deuda para E9. Su orden no afecta los eventos, porque estos van dentro de la transacción.
3. Los escritores de stock pasan a devolver la fila completa:
   - `inventory_repo.go`: `ReserveStock`, `ReleaseReservation` y `DeductStock` pasan a devolver `(*StockEntity, error)` con `RETURNING id, company_id, product_id, warehouse_id, location, available, reserved, min_stock, max_stock, updated_at`;
   - `UpsertStockDelta` y `SetStockAvailable` ya devuelven la entidad;
   - lo mismo en `return_repo.go` y `warehouse_location_repo.go`;
   - se ajustan las llamadas en los 11 servicios de la tabla 12.1, ignorando por ahora la fila devuelta (`_`).
4. Los servicios de despachar, recibir y cancelar traslados pasan a devolver la entidad actualizada (`(*dtos.TransferResponse, error)`), y sus controladores la responden (sección 12, «Cambios en la API»). **Este sí es un cambio de contrato HTTP:** el front lo ignora hasta E8, porque hoy solo mira si hubo éxito.

**Pruebas**
- Todo `go test ./...` igual.
- Todos los flujos de `importadoraApi/scripts/e2e/*.py` en verde **sin cambios**.
- `flow_transfers_scope.py` debe seguir pasando con la respuesta nueva, porque solo agrega campos.

**Criterio de salida:** comportamiento idéntico, comprobado con e2e.

**Riesgos:** una transacción manual con lógica especial (por ejemplo, `Commit` en medio). *Mitigación:* revisarlas una por una en el PR y, si alguna no encaja, documentarla y dejarla como excepción en la prueba guardia.

#### Resultado de E6 (11-nov-2026) · **hecha**

- **Hecho:** las 21 transacciones manuales con `s.pool.Begin(ctx)` en 13 archivos de `src/features` (`customer_write_service.go`, `product_write_service.go`, `recycle_bin_service.go`, `purchase_receive_service.go`, `purchase_service.go`, `return_service.go`, `payment_flow_service.go`, `payment_status_service.go`, `payment_service.go`, `sale_annul_service.go`, `sale_invoice_service.go`, `sale_stage_service.go` y `sale_service.go`) se migraron a `db.WithTx(ctx, s.pool, func(ctx context.Context, tx pgx.Tx) error { … })` conservando el mismo orden de operaciones, `SET LOCAL lock_timeout = '8s'` en `SaleService.Create` y el manejo de errores; se agregó la prueba guardia `TestNoManualBeginInFeatures` en `src/core/db/txhooks_test.go` (0 ocurrencias de `.Begin(` o `BeginTx(` en `src/features`); `DispatchTransfer`, `ReceiveTransfer` y `CancelTransfer` en `inventory_transfer_flow_service.go` y `transfer_controller.go` ahora devuelven `(*dtos.TransferResponse, error)` (el DTO completo de `GetTransferByID`). Los escritores de stock no requirieron cambio de firma porque `Recorder.Stock(ctx, stockID, causa)` de E2 relee la fila final dentro de la transacción en `Flush`.
- **Pruebas:** `go build ./...`, `go vet ./...`, `go test -count=1 ./...` sobre `importadora_test` en verde y los 8 flujos e2e (`flow_transfers_scope.py`, `flow_payments_transfers.py`, `flow_purchases_cash.py`, `flow_sale_annul.py`, `flow_reasons.py`, `flow_deactivation_reason.py`, `flow_company_admin.py` y `flow_notifications.py`) en verde sin fallas.

---

### E7 · Front base

- **Rama:** `feat/realtime-e7-front`.
- **Toca:** `importadoraInterfaz/src/features/realtime/`, `src/app/store/`, `src/global/helpers/realtime/`, `src/global/hooks/ui/useRecentlyChanged.ts`, el montaje en `AppHeader`, `.env.example`, y la dependencia de desarrollo `ajv` para las pruebas del contrato.

**Tareas** (la especificación exacta está en la sección 16):
1. `types/realtime.contract.ts` (6.5) y `types/realtime.types.ts` (estados de la máquina 16.1).
2. `constants/realtime.constants.ts`: `HELLO_TIMEOUT_MS`, `RECONNECT_BASE_MS = 1000`, `RECONNECT_MAX_MS = 30000`, `TICKET_RETRY_LIMIT = 3` y `REALTIME_CLOSE_CODES`. Se borra `REALTIME_EVENT_TYPES`.
3. `services/realtime.ticket.ts`: `requestTicket(): Promise<{ ticket; wsUrl; expiresAt }>` con `apiClient.post('/realtime/ticket')` y su constante en `src/api/endpoints`.
4. `services/realtime.service.ts`: máquina de estados 16.1 (una sola instancia por pestaña).
5. `helpers/realtime.adapter.ts`: `parseEnvelope(raw: unknown): RealtimeEvent | null` (16.2).
6. `helpers/realtime.guards.ts`: `isEvent<T>(e, type): e is RealtimeEnvelope<T>`, más guardas de forma mínima por tipo.
7. `helpers/realtime.sequencer.ts` (16.3).
8. `helpers/realtime.router.ts` (16.4) y `registerRealtimeHandlers(map)`.
9. `slices/realtime.slice.ts`: `status`, `lastSeq`, `connectedAt`, `lastError` y `replaying`. Se confirma que **no** está en la whitelist de `redux-persist`.
10. `thunks/realtime.thunks.ts`: `startRealtime`, `stopRealtime` y `resyncVisibleScreen` (despacha la carga de la pantalla activa según `ui.activeModule`/`activeSubitem`, mediante un mapa en `realtimeRegistry`).
11. `hooks/useRealtimeStatus.ts` y `components/RealtimeStatus.tsx` + `.css` (16.7), montado dentro del `AppHeader` existente.
12. `app/store/realtimeRegistry.ts`: por ahora vacío (solo `system.*` y `session.revoked` hacia `auth`); en E8–E9 se llena.
13. `global/helpers/realtime/applyVersioned.ts` (16.5) y `global/hooks/ui/useRecentlyChanged.ts` (16.6).
14. Se borran `helpers/realtime.url.ts` y `helpers/realtime.handlers.ts`, más los nombres heredados en el adaptador.
15. `.env.example`: `VITE_WS_URL` (solo como respaldo: la URL buena llega con el ticket) y `VITE_REALTIME_ENABLED=true` (interruptor de la sección 18.4).

**Pruebas (Vitest)**
- `realtime.adapter.test.ts`: los 48 ejemplos de `importadoraRealtime/contract/examples` se aceptan; un JSON roto, un `v: 2` o un tipo desconocido → `null`.
- `realtime.contract.test.ts`: con `ajv`, cada ejemplo valida contra su schema (la misma fuente que la API).
- `realtime.sequencer.test.ts`: duplicado o menor → descartado; mayor → aceptado y `lastSeq` avanza; `system.watermark` avanza `lastSeq` sin entregar nada; `system.*` con `seq` 0 → pasa; un salto de `seq` **no** es error (16.3).
- `realtime.router.test.ts`: varios manejadores por tipo; uno que lanza error no detiene a los demás.
- `applyVersioned.test.ts`: más nuevo → aplica; igual o más viejo → no; sin versión previa → aplica.
- `realtime.service.test.ts` (con un `WebSocket` simulado):
  - 4001 → pide ticket nuevo (máximo 3 veces seguidas y luego espera);
  - 4401 → despacha el cierre de sesión;
  - 4003 → `offline` sin reintentar;
  - 1006 → reconecta con espera creciente.

**Criterio de salida:** `pnpm exec tsc -b && pnpm lint && pnpm test` en verde, y el indicador muestra «Conectado» contra el servicio de E5.

#### Resultado de E7 (11-nov-2026) · **hecha**

- **Hecho:** `types/realtime.contract.ts` (contrato completo v1 con los 48 tipos, sobre, mensajes de cliente y códigos de cierre) y `types/realtime.types.ts` (estados de la máquina 16.1); `constants/realtime.constants.ts` (`HELLO_TIMEOUT_MS`, `RECONNECT_BASE_MS`, `RECONNECT_MAX_MS`, `TICKET_RETRY_LIMIT`, `REALTIME_CLOSE_CODES`, eliminado `REALTIME_EVENT_TYPES`); `services/realtime.ticket.ts` (`POST /realtime/ticket` registrado en `auth.endpoints.ts`); `services/realtime.service.ts` (máquina de estados 16.1 con consumo de ticket, envío de `hello`, manejo de `system.welcome`/`replay_completed`/`resync_required` y códigos de cierre 4001/4401/4403/4003/4429/4400/1006); `helpers/realtime.adapter.ts` (`parseEnvelope`), `helpers/realtime.guards.ts`, `helpers/realtime.sequencer.ts` y `helpers/realtime.router.ts`; `slices/realtime.slice.ts` y `thunks/realtime.thunks.ts` (`startRealtime`, `stopRealtime`, `resyncVisibleScreen`); `hooks/useRealtimeStatus.ts`, `hooks/useRevalidateOnFocus.ts` y `components/RealtimeStatus.tsx` + `.css` montado en `AppHeader`; `app/store/realtimeRegistry.ts`; `global/helpers/realtime/applyVersioned.ts` y `global/hooks/ui/useRecentlyChanged.ts` + `.is-recently-changed` en `data-table.css` y `--color-highlight-bg` en `tokens.css`; eliminados `realtime.url.ts` (`?token=`) y `realtime.handlers.ts`.
- **Pruebas:** `realtime.adapter.test.ts` (48 ejemplos válidos y rechazos), `realtime.contract.test.ts` (48 ejemplos contra `envelope.schema.json` y `events/*.schema.json` + `client-message.schema.json`), `realtime.sequencer.test.ts`, `realtime.router.test.ts`, `applyVersioned.test.ts` y `realtime.service.test.ts` (4001, 4401, 4003 y 1006). `tsc -b`, `eslint`, `check:api` (135/135) y `check:type` (164 CSS) en verde.

---

### E8 · Piloto: stock, traslados y activo ⇄ inactivo (API + front)

- **Rama:** `feat/realtime-e8-piloto`.
- **Toca:** los servicios de la API de esos dominios y las features del front (ya refactorizadas por el plan principal) `shop`, `inventory`, `transfers`, `warehouses`, `locations`, `catalogs`, `customers`, `employees`, `externals`, `companies`/`superadmin`, `navigation` y `auth` (solo estado).

**Tareas · API**
1. **Stock:** en los 11 servicios de la tabla 12.1, usar la fila devuelta → `s.events.Stock(ctx, row, events.Cause…(…))`.
2. **Traslados:** `CreateTransfer` → `TransferCreated`; `Dispatch`/`Receive`/`Cancel` → `TransferStatus` (con el traslado devuelto en E6).
3. **Activo ⇄ inactivo:** en cada servicio de los 19 kinds de datos maestros, en el punto donde cambia `status`/`is_active` → `events.EntityStatus(...)`. En los `DELETE` que desactivan, lo mismo.
4. **Sesiones afectadas:** desactivar un empleado, admin, bodega o compañía → además `SessionRevoked` por cada actor afectado (el empleado; o todos los empleados de la bodega o compañía, con una consulta dentro de la misma transacción).
5. **Carrito:** `POST /carts/:id/items` acepta `productId + warehouseId` y responde el carrito; todas las rutas de carrito → `CartUpdated` (canal del actor).

**Tareas · front**
6. Manejadores en `features/{shop,inventory,transfers,warehouses,locations,catalogs,customers,employees,externals,companies,superadmin,navigation}/realtime/*.realtime.ts` (patrón 16.4), registrados en `realtimeRegistry.ts`.
7. Slices con `upsertFromEvent`, `statusFromEvent`, `removeFromEvent` y `stockRowsChanged`, todos con `applyVersioned`.
8. Quitar `dispatch(loadProductsThunk())` de `shop.cart.thunks.ts` y `shop.cart.update.thunks.ts`; `findStockId` deja de usarse (el servidor lo resuelve).
9. `transfers.slice.ts`: `dispatchTransferThunk`/`acceptTransferThunk` aplican la respuesta completa; se quita `loadTransfersThunk` de `createTransferRequestThunk`.
10. Interfaz: resaltado (`useRecentlyChanged`), avisos en drawers abiertos («Despachado por…», «Desactivado por…: motivo»), líneas «no disponible» en el carrito y el borrador de traslado, y toasts según la política 16.8.

**Pruebas**
- API, `flow_realtime_pilot.py` en `importadoraApi/scripts/e2e`: solo HTTP. Despacha y recibe, y después consulta `realtime_events` vía un endpoint de depuración **solo de desarrollo**, o directo en la BD con `psql`, para comprobar los tipos esperados.
- Realtime, `scripts/e2e/flows/pilot.go`: con 4 clientes (A, B, C y D de la sección 12) y la API real, el ciclo de traslado y la desactivación de producto llegan a quien corresponde.
- Front: pruebas de cada manejador nuevo (evento → estado esperado).
- **Manual:** lista 12.5, puntos 1–7, con 4 navegadores.

**Criterio de salida:** los escenarios 12.1–12.3 cumplidos tal cual, y en las herramientas del navegador cero GET de seguimiento en esos flujos.

---

### E9 · Resto de dominios (un PR por bloque)

| PR | Bloque | API: eventos que se empiezan a emitir | Front: manejadores y GET de seguimiento que se quitan |
|---|---|---|---|
| E9.1 | Ventas | `sale.created`, `sale.stage_changed`, `sale.payment_changed`, `invoice.issued`, `invoice.annulled` | `orders`, `cashRegister`, `billing`; quitar `loadOrdersThunk` + `loadOrderCountsThunk` de `orders.thunks.ts:63-97` |
| E9.2 | Pagos, cartera y conciliación | `payment.*`, `credit.balance_changed`, `bank_statement.imported`, `bank_record.status_changed` | `wallet`, `bank`, `customers`, `checkout`; quitar las recargas de `wallet.payments.thunks.ts:61-110` y `bank.thunks.ts` |
| E9.3 | Caja | `cash_session.*`, `cash.expense_recorded` | `cashRegister`, `cashExpenses` |
| E9.4 | Compras | `purchase.*`, `supplier_catalog.imported` | `purchases` |
| E9.5 | Devoluciones | `return.*` | `returns`; quitar `loadReturnsThunk` de `returns.thunks.ts` |
| E9.6 | Cotizaciones y conteos | `quotation.*`, `count.*` | `quotations`, `inventory` (conteos) |
| E9.7 | Precios y datos maestros restantes | `product.prices_changed`, `entity.created/updated/archived/restored/deleted` de los 19 kinds | todas las tablas de catálogo; `recyclebin`; quitar `loadAdminsThunk`, `loadEmployeesThunk` y `loadRolesThunk` de sus thunks |
| E9.8 | Sesión y cuenta | `permissions.changed` (`PUT /employees/:id/permissions`), `account.updated` (`PUT /auth/me` de la Fase 7), `session.revoked` por logout y por `PUT /auth/me/password` | `auth`, `navigation`, `employees`, `settings` |
| E9.9 | Notificaciones | `notification.created` (gancho en `notification_repo.go`, que es el único lugar con `INSERT INTO notifications`), `notification.read` | `notifications`; **quitar el `setInterval`** de `useNotifications.ts:54` |
| E9.10 | Auditoría y respaldos | `audit.logged` (dentro de `auditRepo.Insert` cuando corre en una transacción; las auditorías que van después del commit se mueven dentro aquí), `backup.created`, `backup.restored` | `audit`, `backups`; **quitar** `window.location.reload()` de `useBackups.ts:73` |
| E9.11 | Reportes | (nada nuevo) | `reports`: marca «desactualizado» con eventos de venta, pago, stock y cartera, y recalcula una vez si el reporte está en pantalla |

Cada PR repite el patrón de E8 (API → manejadores → quitar GET → escenario de varios usuarios) y termina con `grep -rn "dispatch(load\w*Thunk" src/features/<feature>/thunks` sin llamadas dentro de thunks de mutación.

---

### E10 · Conflictos de edición

- **Rama:** `feat/realtime-e10-conflictos`.

**Tareas**
1. API: **todas** las ediciones (decisión 4) aceptan `expectedVersion` (ISO de `updated_at`). Si viene y es distinto → `409 STALE_VERSION` con la fila actual en `details`. Rutas: `PUT /customers/:id`, `/products/:id` (incluye precios), `/brands/:id`, `/categories/:id`, `/models/:id`, `/externals/:id`, `/warehouses/:id`, `/warehouses/:id/facturation`, `/warehouses/:id/locations/:locId`, `/doc-series/:id`, `/quotations/:id`, `/roles/:id`, `/employees/:id`, `/employees/:id/permissions`, `/admins/:id`, `/companies/:id`, `/counts/:id/items`, `/purchases/suppliers/:supplierId/products` y `/auth/me`. La comparación va dentro de la misma transacción con `SELECT … FOR UPDATE` para que no haya carrera.
   - Una guardia en `core/bootstrap` (como `routes_guard_test.go`) falla si un `PUT` de edición no lee `expectedVersion`.
2. Front (sobre los formularios completos que dejó la Fase 4 del plan principal):
   - el formulario guarda la `version` con la que abrió;
   - si llega `entity.updated`/`entity.status_changed` del mismo registro, muestra el aviso «Ana cambió este registro (campos: teléfono, ciudad). Ver cambios / Seguir con mis datos»;
   - al guardar con un 409, muestra los dos valores lado a lado.

**Pruebas:** un e2e con dos usuarios sobre el mismo cliente; pruebas del formulario.

---

### E11 · Robustez, seguridad y cierre

**Tareas**
1. Flujos e2e de `importadoraRealtime/scripts/e2e/flows/`:
   - `tickets.go`, `channels.go`, `permissions.go`, `replay.go`, `revocation.go`, `load.go`;
   - `multi_instance.go`: dos procesos en 3002 y 3003, y el cliente de cada uno recibe todo;
   - `restart.go`: matar el servicio, hacer mutaciones y levantarlo → los clientes recuperan.
2. `importadoraApi/src/core/bootstrap/events_guard_test.go` (sección 17.2).
3. Prueba de carga: 500 clientes, ráfaga de 300 filas de stock y 50 eventos por segundo durante 5 min. Criterio: el percentil 95 de la latencia de commit a cliente es menor de 300 ms en local, sin cierres 4408 y con memoria estable.
4. Documentación:
   - `importadoraRealtime/README.md` completo;
   - documentación vigente de API, interfaz y servicio en los README de cada repositorio y en `importadoraInterfaz/docs/CONVENTIONS.md`; mantener esos documentos alineados con el contrato versionado de este directorio.
5. Repaso de comentarios y cabeceras en los tres directorios.
6. Lista de aceptación 12.5 completa.

**Criterio de salida:** todo en verde y, en una sesión normal de trabajo, la pestaña de red del navegador solo muestra carga inicial y comandos.

---

## 10. Qué peticiones HTTP desaparecen

| Hoy | Con websocket |
|---|---|
| Sondeo de notificaciones (`useNotifications.ts:54`) | `notification.created` |
| Volver a pedir listas tras cambios de otros usuarios | `entity.*`, `<documento>.status_changed` |
| Revalidar stock en catálogo, carrito y traslados | `stock.changed` |
| Revalidar al volver a la pestaña (Fase 5.2-b) | Solo si el socket está caído |
| Descubrir una sesión revocada en la siguiente petición (401) | `session.revoked` al instante |

Se quedan en HTTP: la carga inicial de cada pantalla, los comandos, los archivos, las exportaciones y los reportes.

---

## 11. Decisiones

**Cerradas por el dueño (11-nov-2026):**

1. ✅ Retención de eventos para recuperar: **48 h**.
2. ✅ El superadmin **no va atado a ninguna compañía**: solo crea compañías y su admin. Su conexión solo recibe `platform` y su canal personal; nunca `company:*` ni `warehouse:*`, ni siquiera con compañía de contexto. Aplicado en 6.1, 6.3, 8.1, 14.3 y pruebas de E4.
3. ✅ Avisos: **generales, solo los que le competen al usuario** (su bodega, su etapa, sus notificaciones, su carrito, su cuenta y sus permisos). El resto se actualiza en silencio con resaltado. Es la tabla 16.8 tal cual.
4. ✅ Concurrencia optimista en **todas** las ediciones. Aplicado en E1 (8 tablas sin `updated_at`) y E10 (19 rutas `PUT`).

5. ✅ Un socket por pestaña. Compartir un solo socket entre pestañas (`BroadcastChannel`) queda como mejora posterior.
6. ✅ Máximo **5** pestañas conectadas por usuario (`REALTIME_MAX_CONN_PER_ACTOR=5`); la 6.ª cierra la más vieja con 4429.
7. ✅ `system.watermark` cada **30 s** (`REALTIME_WATERMARK_SEC=30`).
8. ✅ `audit.logged` **en vivo**, solo para quien tiene `audit:view`. Por la decisión 2, el superadmin recibe en vivo solo la auditoría de `platform`.
9. ✅ Si falla el `Flush` del evento, **se cancela la operación de negocio** (rollback y error 500 con el `correlationId`): nunca queda un cambio guardado sin su evento.
10. ✅ Despliegue en **Railway**: `importadoraRealtime` es otro servicio del mismo proyecto, con carpeta raíz propia, su dominio `wss://` y sin nginx (sección 18.2).

Todas las decisiones quedan cerradas: E0 puede empezar.

---

## 12. Escenarios detallados: productos en bodegas, traslados y activo ⇄ inactivo

> Cada escenario muestra: **cómo funciona hoy** (verificado en el código), **cómo queda con websocket**, qué recibe cada usuario, qué cambia en cada pantalla y **cuántas peticiones HTTP** cuesta. Sirve como lista de aceptación: si un escenario no se cumple tal cual, el websocket no está completo.
>
> **Usuarios de ejemplo:** A = vendedor en bodega 7 (Principal). B = bodeguero en bodega 7. C = bodeguero en bodega 9 (Norte). D = admin de la compañía, con «Productos» abierto.

### Cambios en la API que todos los escenarios necesitan

1. **Toda mutación responde la entidad actualizada.** Hoy `POST /transfers/:id/dispatch`, `/receive` y `/cancel` solo devuelven éxito: el servicio retorna `error` (`inventory_transfer_flow_service.go:23, 112, 222`). Por eso el front adivina el nuevo estado (`transfers.slice.ts:142-150`). Deben responder el traslado completo con sus líneas y su `updatedAt`. Lo mismo para los cambios de estado de cada entidad.
2. **Las transacciones pasan por `db.WithTx`** (las 21 manuales, sección 7).
3. **Los eventos se registran dentro de la transacción**, nunca junto a la auditoría que va después del commit.
4. **El permiso del evento puede ser «cualquiera de»:** `Event.Permissions []string`. Por ejemplo, el stock lo ve quien tenga `catalog:view` **o** `stock:view` **o** `transfers:view`. Se cambia el campo `Permission string` de la sección 7 por `Permissions []string`.

---

### 12.1 Productos en bodegas (cantidades físicas)

#### Quién cambia el stock (verificado)

Solo estos servicios llaman a los escritores de stock de `inventory_repo.go` (`UpsertStockDelta`, `SetStockAvailable`, `ReserveStock`, `ReleaseReservation`, `DeductStock`):

| Servicio | Operación | `cause.type` |
|---|---|---|
| `sales/services/sale_service.go` | Crear pedido → **reserva** | `reservation` |
| `sales/services/sale_stage_service.go` (2 llamadas) | Despachar o anular pedido → descuenta o libera | `sale_out` / `release` |
| `inventory/services/inventory_transfer_flow_service.go` (3) | Despachar (origen) / recibir (destino) / cancelar | `transfer_out` / `transfer_in` / `release` |
| `inventory/services/inventory_transfer_service.go` | Crear traslado → reserva en el origen | `reservation` |
| `inventory/services/inventory_service.go` (3) | Ajuste manual y reubicación | `adjustment` / `relocation` |
| `inventory/services/inventory_assign_service.go` (2) | Asignar stock a ubicación | `adjustment` |
| `inventory/services/inventory_count_service.go` | Corroborar conteo | `count_adjust` |
| `products/services/product_initial_stock_service.go` | Stock inicial de un producto | `adjustment` |
| `purchases/services/purchase_receive_service.go` | Recibir compra | `purchase_in` |
| `returns/services/return_service.go` (+ `return_repo.go`) | Devolución aprobada | `return_in` |
| `warehouses/repositories/warehouse_location_repo.go` | Mover entre ubicaciones | `relocation` |

**Regla de implementación:** los escritores de stock **ya devuelven la fila** (`RETURNING … available, reserved, …, updated_at`). Cada servicio, al recibirla, llama `events.Stock(ctx, row, cause)`. El buffer guarda **la última versión por `stockId`** y, antes del commit, arma **un solo** `stock.changed` con todas las filas de la transacción y los totales por producto y bodega. Un pedido de 12 productos produce 1 mensaje, no 12.

#### Hallazgo: el carrito hoy desperdicia peticiones

En `shop/thunks/shop.cart.thunks.ts:80-86`, **agregar un producto al carrito** hace hasta 4 peticiones:
1. `findStockId` (GET)
2. `addCartItem` (POST)
3. `fetchActiveCart` (GET)
4. `loadProductsThunk()` (GET de la página completa del catálogo)

El comentario dice «el backend descontó stock», pero **el carrito no toca la tabla `stock`**: ningún servicio de `carts` llama a los escritores de stock (la reserva ocurre al crear el pedido, en `sale_service.go`). La recarga del catálogo es pura pérdida. Pasa lo mismo al actualizar o quitar líneas (`shop.cart.update.thunks.ts:23, 41, 60`).

**Con el plan:**
- `POST /carts/:id/items` acepta `productId + warehouseId` (el servidor resuelve el `stockId`) y **responde el carrito completo**: 1 petición.
- Se borra `dispatch(loadProductsThunk())` de los 4 thunks.
- El stock del catálogo se actualiza solo con `stock.changed`, cuando de verdad cambia (al crear el pedido).

#### Escenario: A crea un pedido de 3 unidades del producto 88 en la bodega 7

| Paso | Qué pasa |
|---|---|
| 1 | A confirma el checkout → `POST /sales` (1 HTTP). |
| 2 | En la transacción: `ReserveStock(stock 501, 3)` → fila `available 20, reserved 5, sellable 15`, y `events.Stock(row, reservation, sale 881)`. Además, `sale.created`. |
| 3 | Commit → `realtime_events` (seq 9101 y 9102) → `pg_notify`. |
| 4 | Hub: `stock.changed` al canal `company:3` con permisos `catalog:view \| stock:view \| transfers:view`; el pedido al canal `warehouse:7` con `orders:view`. |
| 5 | **A**: la respuesta HTTP ya actualizó su pedido; aplica `stock.changed` (el catálogo baja a 15). |
| 6 | **B** (bodeguero, bodega 7): su tabla de Pedidos agrega el PED-000881 en «verificación» y se resalta. Su pantalla de Stock muestra `reservado 5` en la ubicación A-03-2. |
| 7 | **C** (bodega 9): su catálogo muestra 15 vendibles en Principal (la columna de otras bodegas), y su borrador de traslado desde Principal valida contra 15. |
| 8 | **D** (admin en Productos): el detalle del producto 88 abierto en el drawer actualiza la tabla de ubicaciones. |

**HTTP:** hoy, para que B, C y D lo vean, cada uno recarga su pantalla (1 a 3 GET cada uno, y solo si se dan cuenta). Con websocket: **1 POST en total y 0 GET**.

#### Qué hace cada slice con `stock.changed`

| Feature (manejador `*.realtime.ts`) | Estado que actualiza | Efecto visible |
|---|---|---|
| `shop` → `shop.products.slice` | `stockByWarehouse[warehouseId] = totals.sellable` (ya existe `applyStockUpdate`; se adapta a ids numéricos y al lote) | Tarjeta con nuevo vendible; «Agotado» y botón deshabilitado en 0 |
| `shop` → `shop.cart.slice` | Por línea con ese `stockId`: `maxAvailable = sellable` | Aviso «quedan 2» y bloqueo de confirmar si la cantidad supera lo disponible |
| `inventory` → `inventory.slice` (`stockTable`, `productStock`) | Fila por `stockId` (`available`, `reserved`, `lowStock`) | Fila resaltada; KPI de bajo stock |
| `transfers` → `transfers.slice` (`warehouseStock`, borrador) | Disponible del origen por `stockId` | El borrador marca la línea si pide más de lo que hay |
| `warehouses` → ubicaciones | Ocupación de la ubicación | Barra de ocupación de la ubicación |
| `inventory` → kardex abierto | Agrega la línea de movimiento (de `cause`) | Movimiento nuevo arriba |

Si un manejador no tiene la fila en memoria (por ejemplo, otra página de la tabla), **la ignora**: no se piden datos por cada evento. Cuando el usuario cambie de página, el GET normal trae el valor actual.

---

### 12.2 Traslados entre bodegas

#### Cómo funciona hoy (verificado)

| Acción | Front | API | Problema |
|---|---|---|---|
| Crear | `createTransferRequestThunk`: 1 POST **por bodega de origen** + `loadTransfersThunk` (GET) | `inventory_transfer_service.go` reserva en el origen | C (destino) no se entera. El stock reservado no se ve en el catálogo de nadie. |
| Despachar | `dispatchTransferThunk`: POST; el slice solo cambia el estado de la fila (`transfers.slice.ts:142`) | `DeductStock` en el origen + kardex `transfer_out`; responde vacío | El catálogo y el stock de A, B y D quedan viejos. C no ve que va en camino. |
| Recibir | `acceptTransferThunk`: POST; el slice cambia la fila (`:146`) | `UpsertStockDelta` en el destino + kardex `transfer_in` + `received_qty` por línea; responde vacío | El origen no ve que llegó. El stock del destino no se actualiza en ninguna pantalla. |
| Cancelar | POST | Libera la reserva | El destino no se entera. |

#### Cómo queda con websocket

Canales: **los dos** (`warehouse:{origen}` y `warehouse:{destino}`) con `transfers:view`. El stock va a `company:{id}` como en 12.1.

| Paso del traslado TR-000042 (bodega 7 → 9, 6 unidades) | Eventos de la misma transacción | B (origen) ve | C (destino) ve | A y D ven |
|---|---|---|---|---|
| **Crear** (B o C lo solicita) | `transfer.status_changed (null→pending)` + `stock.changed (reservation, origen)` | Fila nueva «Pendiente» en Traslados + botón «Despachar» | Fila nueva «Pendiente» + toast «Solicitud de traslado desde Principal» (si no la creó él) | Catálogo: vendible en Principal −6 |
| **Despachar** (B) | `transfer.status_changed (pending→in_transit)` + `stock.changed (transfer_out, origen)` | La fila pasa a «En tránsito»; en su drawer abierto, «Despachar» se deshabilita | Fila «En tránsito» + toast «TR-000042 va en camino a tu bodega» + botón «Recibir» | `available` de Principal −6 (`reserved` −6) |
| **Recibir completo** (C) | `transfer.status_changed (in_transit→received)` con `receivedQty` por línea + `stock.changed (transfer_in, destino)` | Fila «Recibido» + toast «TR-000042 recibido por Carlos» | Fila «Recibido»; Stock: B-01-1 +6 | Catálogo: Norte +6 |
| **Recibir con faltantes** (C recibe 4 de 6) | `→with_novelty` con `noveltyNotes` y la línea `quantity 6 / receivedQty 4` + `stock.changed (transfer_in, +4)` | «Con novedad» resaltado + toast con la novedad | «Con novedad»; stock +4 | Norte +4 |
| **Cancelar** (pendiente) | `→cancelled` con `cancelReason` + `stock.changed (release, origen)` | «Cancelado» | «Cancelado» + toast con el motivo | Principal: vendible +6 |

#### Choques entre usuarios (y cómo se resuelven)

- **B y otro bodeguero tienen abierto el mismo traslado y ambos van a despachar.** El primero despacha; el segundo recibe el evento y su botón se deshabilita con «Despachado por Luis hace un momento». Si alcanzó a hacer clic antes de que llegara el evento, la API ya responde `400 INVALID_STATUS` (`inventory_transfer_flow_service.go`, validación de `pending`). El front muestra el mensaje y aplica el estado real.
- **C tiene abierto el formulario de recepción y el traslado se cancela.** Llega `→cancelled`, el formulario se bloquea con el motivo y el pie solo deja «Cerrar».
- **El borrador de traslado (memoria del navegador, `transfers.draft`) pide más de lo que quedó en el origen.** `stock.changed` marca la línea y bloquea «Solicitar».

**HTTP por traslado completo (crear → despachar → recibir), sumando todos los usuarios:**
- **Hoy:** 3 POST + 1 GET automático + las recargas manuales de B, C, A y D para ver estados y stock. Son unas 6 a 12 GET más, y nadie sabe cuándo recargar.
- **Con websocket:** **3 POST y 0 GET.**

---

### 12.3 Activo ⇄ inactivo

#### Cómo funciona hoy (ejemplo: producto, verificado)

`ProductsTable` → `useProductStatus.changeStatus` → `saveProductDetailsThunk` (PUT) → `updateProductStatusLocally` (`useProductStatus.ts:15-16`). Solo cambia **la tabla de productos de D**:
- El catálogo de la tienda (`shop.products.slice`) sigue mostrando el producto inactivo hasta que se recargue.
- Los carritos y borradores de traslado que lo tienen no se enteran.
- Los demás usuarios no ven nada.

Pasa lo mismo con clientes, empleados, bodegas, ubicaciones, marcas, categorías, modelos y externos.

#### Cómo queda con websocket: evento `entity.status_changed`

Canal `company:{id}` (o `warehouse:{id}` para ubicaciones), con el permiso `view` de la entidad. Además, un canal de actor para quien se queda sin acceso.

| Entidad que cambia | Pantallas y estado que se actualizan al instante |
|---|---|
| **Producto** | Tabla Productos (fila + `StatusSelect`); catálogo de la tienda (lo quita si es inactivo y lo vuelve a mostrar si se activa, porque el evento trae la fila); carritos abiertos (línea «Producto no disponible», no deja confirmar); borradores de traslado; buscador de productos de compras y traslados; drawer de detalle abierto (aviso «Desactivado por D: *motivo*») |
| **Bodega** | Selector de bodega del encabezado; destinos de traslado; filtros «Bodega» de todas las tablas; **empleados de esa bodega**: `session.revoked (reason: account_inactive)` → su sesión se cierra con el aviso actual de `ACCOUNT_UNAVAILABLE` (el middleware ya los bloquea con `IsActorAvailable`) |
| **Ubicación** | Lista de ubicaciones de la bodega; selector de ubicación al recibir traslados, recibir compras y reubicar |
| **Cliente** | Tabla Clientes; selector de cliente del checkout y de cotizaciones; cartera; si estaba elegido en un carrito → aviso y bloqueo de confirmar |
| **Empleado** | Tabla Empleados (`StatusSelect`, Fase 2.5); selector de vendedor; **al empleado desactivado**: `session.revoked` y cierre inmediato (hoy se entera en su siguiente petición) |
| **Marca / categoría / modelo** | Tablas de catálogos; filtros del catálogo de la tienda; selects de los formularios de producto |
| **Proveedor / transportadora** | Tabla de externos; selects de compras y despachos |
| **Rol** | Tabla Roles; select de rol en el formulario de empleado |
| **Compañía** (superadmin) | Todas las sesiones de esa compañía: `session.revoked (company_inactive)` |

#### Escenario: D desactiva el producto 88 (que A tiene en su carrito)

| Paso | Qué pasa |
|---|---|
| 1 | D elige «Inactivo» → `ReasonPrompt` → `PUT /products/88 { status: 'inactive', reason }` (1 HTTP). Responde el producto con `updatedAt`. |
| 2 | En la transacción: `UPDATE products … RETURNING …` → `events.Record(entity.status_changed {kind: product, id: 88, from: active, to: inactive, reason})`. |
| 3 | **D**: la respuesta actualiza su fila; el evento llega con la misma versión y se ignora (sin parpadeo). |
| 4 | **A**: el catálogo quita el producto 88 y su línea del carrito se marca «no disponible», con «Confirmar» deshabilitado hasta quitarla. |
| 5 | **B y C**: si tienen el producto en un borrador de traslado o en el buscador de compras, se marca o desaparece. |
| 6 | Si D lo vuelve a activar, el mismo evento `inactive→active` lo restaura en todos lados. Para mostrarlo en el catálogo con todos sus datos, el manejador de `shop` usa el `row` que trae el mismo `entity.status_changed`. |

**HTTP:**
- **Hoy:** 1 PUT, y los demás ven datos falsos hasta recargar. A puede intentar confirmar con un producto inactivo y la API se lo rechaza.
- **Con websocket:** 1 PUT, 0 GET, y el error se previene antes de que A intente confirmar.

---

### 12.4 Balance de peticiones HTTP (lo que se elimina)

| Origen del consumo | Hoy | Con websocket |
|---|---|---|
| Agregar, cambiar o quitar del carrito | 3–4 peticiones por clic (incluye recargar el catálogo) | 1 |
| Crear traslado | N POST + 1 GET | N POST |
| Despachar, recibir o cancelar traslado | 1 POST + recargas manuales en cada usuario | 1 POST |
| Cambiar estado de cualquier entidad | 1 PUT + recargas manuales en cada usuario | 1 PUT |
| Pedidos (`orders.thunks.ts:63-97`): cada acción recarga lista + contadores | 1 POST + 2 GET | 1 POST |
| Cartera y abonos (`wallet.payments.thunks.ts:61-110`): cada acción recarga 2 listas | 1 POST + 2 GET | 1 POST |
| Conciliación bancaria (`bank.thunks.ts`) | 1 POST + 1–2 GET | 1 POST |
| Devoluciones (`returns.thunks.ts`) | 1 POST + 1 GET | 1 POST |
| Respaldos: `window.location.reload()` tras restaurar (`useBackups.ts:73`) | Recarga completa de la app | Evento `system.resync_required` + recarga de los slices |
| Notificaciones (`useNotifications.ts:54`) | 1 GET por intervalo y por pestaña, siempre | 0 (evento) |

**Regla final:** después de una mutación **no se hace ningún GET de seguimiento**. Lo propio se actualiza con la respuesta del endpoint y lo ajeno con eventos. Para comprobarlo, `grep -rn "dispatch(load\w*Thunk" src/features/*/thunks` no debe devolver llamadas dentro de thunks de mutación (las de carga inicial sí quedan).

---

### 12.5 Lista de aceptación (probar con 4 navegadores: A, B, C y D)

- [ ] Agregar al carrito hace **1** petición y no recarga el catálogo.
- [ ] Crear un pedido baja el vendible en el catálogo de B, C y D en menos de 1 s, sin recargar.
- [ ] Ciclo completo de traslado (crear → despachar → recibir con novedad): B y C ven cada estado y su stock al instante; los toasts solo le salen a la otra bodega.
- [ ] Dos usuarios intentan despachar el mismo traslado: uno lo logra; el otro ve el botón deshabilitado o el `INVALID_STATUS`, y el estado final es correcto en ambos.
- [ ] Desactivar un producto que está en el carrito de A: A lo ve marcado al instante y no puede confirmar.
- [ ] Desactivar una bodega: sus empleados quedan fuera al instante, y la bodega desaparece de los selectores de todos.
- [ ] Desactivar un empleado conectado: su sesión se cierra al instante.
- [ ] Conteo de 300 ubicaciones: llega en 2 mensajes (lotes de 200) y la tabla no se congela.
- [ ] Cortar la red 2 minutos durante cambios y volver: la recuperación por `seq` deja todo igual que en la base de datos (comparar con un GET manual de prueba).
- [ ] Un empleado sin `transfers:view` no recibe ningún evento de traslados (se verifica en las herramientas del navegador, pestaña de websocket).
- [ ] Con las herramientas del navegador abiertas, durante una sesión de trabajo normal solo aparecen peticiones de **carga inicial** y **comandos**.

---

## 13. Cambios por directorio: qué se integra, qué cambia y qué se retira

> Todo se mide contra el **estado final del plan principal** (sección 9.0).

### 13.1 `importadoraApi`

**Se integra**

| Qué | Dónde | Etapa |
|---|---|---|
| Ganchos antes del commit | `src/core/db/txhooks.go` | E2 |
| Paquete de eventos (tipos, catálogo, constructores, actor, recorder, buffer y repositorio) | `src/core/events/` | E2 |
| Ticket de conexión | `src/features/realtime/` → `POST /realtime/ticket` | E2 |
| Tablas `realtime_events` (con `tx_id`) y `realtime_tickets`, rol `importadora_realtime`, `updated_at` + trigger | Migración `2026112X_realtime.sql` + `startdb.pgsql` | E1 |
| Prueba guardia de eventos | `src/core/bootstrap/events_guard_test.go` | E11 |
| Variables `REALTIME_TICKET_TTL_SEC`, `REALTIME_WS_URL` | `src/core/config`, `.env.example` | E2 |
| Exclusión de las tablas de tiempo real en los respaldos | `features/backups` (`pg_dump --exclude-table-data`) | E9.10 |

**Se cambia**

| Qué | Cambio | Etapa |
|---|---|---|
| `db.WithTx` | Ejecuta los ganchos antes del commit; `fn` recibe `ctx` | E2 |
| `core/server/server.go` | Solo se agrega `middleware.RequestID()` | E2 |
| `middlewares.Authenticate` | Deja también el actor en el `context.Context` de la petición | E2 |
| Transacciones manuales | Pasan a `db.WithTx` | E6 |
| Escritores de stock | Devuelven la fila (`RETURNING`) | E6 |
| Despachar, recibir y cancelar traslados, y toda mutación que hoy responde vacío | Responden la entidad completa | E6 / E8–E9 |
| `POST /carts/:id/items` | Acepta `productId + warehouseId` y responde el carrito | E8 |
| Servicios que modifican datos | `events.Record` / `events.Stock` dentro de la transacción | E8–E9 |
| Auditorías después del commit | Pasan dentro de la transacción (para `audit.logged`) | E9.10 |
| `PUT` de edición elegidos | `expectedVersion` → 409 `STALE_VERSION` | E10 |

**Se retira:** nada de la lógica actual. Sin sockets, hub ni listener en la API.

### 13.2 `importadoraInterfaz`

**Se integra:** contrato, ticket, guardas, secuenciador, enrutador, indicador de conexión, `realtimeRegistry`, `applyVersioned`, `useRecentlyChanged`, un manejador `<feature>.realtime.ts` por feature (matriz 6.4), acciones de slice por evento y avisos en vivo en drawers y formularios.

**Se cambia:**
- `realtime.service.ts` (ticket, `hello`, códigos de cierre) y `realtime.adapter.ts` (sobre v1);
- el slice, los thunks, las constantes y los tipos de realtime;
- los thunks de mutación (dejan de volver a pedir datos);
- las cargas de listas (pasan por `applyVersioned` por fila, ver 16.9);
- los formularios de edición (`expectedVersion`);
- `.env.example` (`VITE_WS_URL`, `VITE_REALTIME_ENABLED`).

**Se retira:**
- `realtime.url.ts` (`?token=`), `realtime.handlers.ts` y `REALTIME_EVENT_TYPES` provisional;
- los nombres heredados `tipo`/`cuerpo`/`idCliente`;
- los GET de seguimiento en los thunks de mutación;
- el `setInterval` de notificaciones;
- el `window.location.reload()` de respaldos.

La revalidación al volver a la pestaña (Fase 5 del plan principal) **se conserva**, pero solo actúa con el socket caído.

### 13.3 `importadoraRealtime` (todo es nuevo)

Ver la estructura completa en la sección 8.1.

---

## 14. Especificación del servicio `importadoraRealtime`

### 14.1 Ciclo de vida del proceso

**Arranque (en este orden):**
1. Configuración (`config.Load` + `Validate`). Si falla, el proceso termina con código 1 y un mensaje claro.
2. Pool de BD + `Ping` (10 s).
3. `lastProcessed = events.CurrentVisibleSeq()` (ver 14.7). **No** se reparte historia al arrancar: cada cliente pide lo suyo con `hello`.
4. Conexión `LISTEN`.
5. Hub (su goroutine).
6. Listener (su goroutine).
7. Purga: corre una vez y luego cada hora.
8. Servidor HTTP.
9. Log `realtime listo`, con puerto, `lastProcessed` y versión del contrato.

**Apagado (`SIGINT`/`SIGTERM`):**
1. El servidor deja de aceptar conexiones.
2. El hub envía cierre 1001 a todos los clientes (ellos reconectan a otra instancia o esperan).
3. Se detienen el listener y la purga.
4. Se cierra el pool.
5. Tiempo máximo: 10 s.

### 14.2 Saludo de una conexión, paso a paso

| Paso | Qué ocurre | Límite | Si falla |
|---|---|---|---|
| 1 | `GET /ws?ticket=…`: se valida el `Origin` y se acepta el upgrade (`coder/websocket.Accept`, `OriginPatterns`, sin compresión por defecto) | — | 403 antes del upgrade, o cierre 4003 |
| 2 | Se consume el ticket: `sha256` → `UPDATE realtime_tickets … RETURNING` | 2 s | 4001 (`ticket_invalid`) |
| 3 | Acceso: sesión activa, actor disponible, permisos y bodegas → `AccessContext` | 2 s | 4401 (sesión inactiva) o 4403 (actor no disponible) |
| 4 | Canales: `ChannelsFor(AccessContext)` | — | — |
| 5 | Registro en el hub en estado `awaiting_hello`. Desde aquí, los eventos en vivo del cliente se **encolan**, pero aún no se envían. | — | Cola llena → 4408 |
| 6 | Envío de `system.welcome { currentSeq: lastProcessed, channels, heartbeatSec, contractVersion: 1 }` | — | — |
| 7 | Espera de `hello { lastSeq }` | 5 s | 4400 |
| 7a | `lastSeq = null` (primera conexión o pestaña nueva): sin recuperación. El front ya hace sus cargas HTTP. | — | — |
| 7b | `lastSeq < OldestSeq() − 1` **o** `lastProcessed − lastSeq > REALTIME_MAX_REPLAY`: se envía `system.resync_required { reason: 'gap_too_large', currentSeq }` | — | — |
| 7c | Si no: `ReadAfterForScope(lastSeq, alcance del cliente, 500)` en páginas hasta llegar a `lastProcessed`, aplicando `filter.Allowed`, y enviando en orden. Al final, `system.replay_completed { fromSeq, toSeq, count }`. | 10 s | 4408 si la cola se llena; el cliente lo intenta de nuevo |
| 8 | Se vacía lo encolado en los pasos 5–7, **solo** lo que tenga `seq > último enviado`, y el cliente pasa a `live` | — | — |
| 9 | En `live`: un ping cada `REALTIME_PING_SEC`; sin pong en `REALTIME_PONG_TIMEOUT_SEC` → cierre 1011. `system.watermark` cada `REALTIME_WATERMARK_SEC` si `lastProcessed` avanzó (14.7). | — | — |

### 14.3 Canales por tipo de actor

| Actor | Canales |
|---|---|
| Empleado | `company:{su compañía}`, `warehouse:{su bodega}`, `actor:employee:{id}` |
| Admin | `company:{su compañía}`, `warehouse:{cada bodega activa de su compañía}`, `actor:admin:{id}` |
| Superadmin | `platform` y `actor:superadmin:{id}`. **Nunca** `company:*` ni `warehouse:*` (decisión 2): no va atado a ninguna compañía, crea compañías y su admin. Si abre una pantalla de una compañía en modo lectura, esa pantalla se carga por HTTP y se revalida al volver a la pestaña; no recibe sus eventos en vivo |

Si cambia el conjunto de bodegas (bodega creada o desactivada), el servicio recalcula los canales de los admins de esa compañía al recibir `entity.created` / `entity.status_changed` de `kind: warehouse`. Esto se hace en `control_events.go`.

### 14.4 Filtro: ¿este cliente recibe este evento?

```
Allowed(c, e):
  // 1. canal
  si e.ActorChannel != nil:          devolver e.ActorChannel == c.ActorChannel
  enCanal = falso
  si e.Platform y c.EsSuperadmin:    enCanal = verdadero
  si e.CompanyID != nil y c tiene "company:{e.CompanyID}":
      si len(e.WarehouseIDs) == 0:   enCanal = verdadero
      si no, si c tiene algún "warehouse:{id}" de e.WarehouseIDs: enCanal = verdadero
      // un evento de compañía con bodegas afectadas (por ejemplo stock.changed) también llega a toda la compañía:
      si e.Type ∈ TIPOS_DE_COMPANIA (stock.changed, product.prices_changed, entity.*, credit.balance_changed, payment.*): enCanal = verdadero
  si no enCanal:                     devolver falso
  // 2. permisos
  si len(e.Permissions) == 0:        devolver verdadero
  si c.EsAdmin:                      devolver verdadero
  si c.EsSuperadmin:                 devolver verdadero   // solo lee; nunca recibe eventos para mutar
  devolver c.TieneAlguno(e.Permissions)
```

`TIPOS_DE_COMPANIA` sale del campo `Company` de `catalog.go` (7.2). La API lo guarda resuelto en la fila, así que el servicio no necesita conocer tipos. En concreto: la API guarda en `warehouse_ids` solo las bodegas que **restringen** el evento. Un evento de compañía lleva `warehouse_ids = '{}'`, aunque su contenido mencione bodegas. Por eso, en el servicio la regla real se reduce a «sin `warehouse_ids` → toda la compañía; con `warehouse_ids` → solo esas bodegas». La línea de `TIPOS_DE_COMPANIA` del pseudocódigo **no existe** en el código real: queda en la API.

### 14.5 Modelo de concurrencia del hub

- **Una goroutine dueña del estado** (`hub.run`) recibe por canales de Go: `register`, `unregister`, `dispatch` (lote de `StoredEvent`), `closeSession`, `closeActor`, `reloadPermissions`, `recomputeChannels` y `broadcastResync`. Nadie más toca los mapas, así que no hacen falta mutex en el estado.
- **Índices:** `byChannel map[string]map[*client]struct{}`, `bySession map[int64]map[*client]struct{}` y `byActor map[string]map[*client]struct{}`.
- **`dispatch`:**
  1. Por cada evento, une los clientes de los canales posibles en un conjunto (sin duplicados).
  2. Aplica `Allowed`.
  3. Serializa **una sola vez** el sobre (`[]byte`) y hace `client.queue.TryPush(bytes)`.
  4. Si la cola está llena → `unregister` + cierre 4408.
- **Cada cliente** tiene su `writeLoop` (lee de su cola y escribe con un límite de escritura de 10 s) y su `readLoop`. El hub nunca escribe directo en un socket.
- **Métricas** con `atomic`: `connections`, `eventsDispatched`, `eventsDelivered`, `queueOverflows` y `lastProcessedSeq`.

### 14.6 Códigos de cierre

| Código | Nombre | Lo provoca | Qué hace el front |
|---|---|---|---|
| 1000 | `normal` | El cliente cerró (logout) | Nada |
| 1001 | `going_away` | Apagado del servicio | Reconecta con espera creciente |
| 1011 | `ping_timeout` | No respondió el ping | Reconecta |
| 4001 | `ticket_invalid` | Ticket usado, vencido o inexistente | Pide ticket nuevo y reconecta (máximo 3 seguidos y luego espera creciente) |
| 4003 | `origin_rejected` | `Origin` no permitido | **No** reconecta; estado `offline` con error de configuración |
| 4400 | `protocol_error` | Mensaje inválido, demasiado grande o sin `hello` a tiempo | Reconecta una vez; si se repite, `offline` |
| 4401 | `session_revoked` | Sesión cerrada o revocada, cambio de contraseña | Despacha el cierre de sesión con el motivo |
| 4403 | `account_unavailable` | Actor, compañía o bodega inactivos | Cierra sesión con el aviso `ACCOUNT_UNAVAILABLE` actual |
| 4408 | `queue_overflow` | Cliente lento | Reconecta enviando `lastSeq` |
| 4429 | `too_many_connections` | Más de `REALTIME_MAX_CONN_PER_ACTOR` pestañas | Muestra «Demasiadas pestañas abiertas» y no reconecta en esa pestaña |

### 14.7 Listener: lectura segura con commits fuera de orden

**El problema:** `seq` se asigna al hacer el `INSERT`, no al confirmar. Si la transacción A toma `seq = 10` y la B `seq = 11`, y B confirma primero, el listener lee 11 y avanza `lastProcessed = 11`. Cuando A confirma, el 10 queda **por debajo** y se perdería.

**La solución** (patrón estándar de outbox en PostgreSQL):
- Columna `tx_id xid8 NOT NULL DEFAULT pg_current_xact_id()` en `realtime_events`.
- El listener solo lee filas de transacciones que ya terminaron **todas** las anteriores:

```sql
SELECT … FROM realtime_events
WHERE seq > $1
  AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY seq
LIMIT 500;
```

  Así, mientras A siga abierta, el 11 no se entrega. Cuando A confirma, se entregan 10 y 11 en orden.

- `CurrentVisibleSeq()`: la misma condición con `MAX(seq)`.

**Bucle:**
```
loop:
  esperar NOTIFY 'erp_events'  o  tick de seguridad cada 2 s  o  ctx cancelado
  repetir:
     filas = ReadVisibleAfter(lastProcessed, 500)
     si vacío: salir del repetir
     por cada fila: control_events.Handle(fila)   // revocar, permisos, resync, canales
     hub.Dispatch(filas)
     lastProcessed = filas[último].Seq
  si la conexión LISTEN se cayó: reconectar (espera creciente) y continuar; el tick de seguridad cubre las notificaciones perdidas
```

- **Huecos de `seq`:** son normales (rollbacks y eventos de otras compañías). Nunca se interpretan como pérdida.
- **Respaldo restaurado:** si `CurrentVisibleSeq() < lastProcessed` (la tabla volvió atrás), el servicio fija `lastProcessed = CurrentVisibleSeq()` y hace `hub.BroadcastResync('backup_restored')` a **todas** las conexiones.
- **`system.watermark { seq }`:** se envía a cada cliente `live` cada `REALTIME_WATERMARK_SEC` (por defecto 30 s) si `lastProcessed` avanzó. Así, un cliente que no recibió nada (por filtro) avanza su `lastSeq` y su próxima recuperación no recorre rangos enormes. **Es el tipo 48 del catálogo** (grupo A).

### 14.8 Configuración

| Variable | Por defecto | Validación |
|---|---|---|
| `APP_ENV` | `development` | `development` \| `staging` \| `production` |
| `APP_PORT` | `3002` | 1–65535 |
| `DB_HOST`, `DB_PORT`, `DB_DATABASE` | — | Obligatorias |
| `DB_USER` | `importadora_realtime` | Obligatoria; avisa si es `postgres` en producción |
| `DB_PASSWORD` | — | Obligatoria fuera de desarrollo |
| `REALTIME_DB_MAX_CONNS` | `10` | 2–50 |
| `REALTIME_ALLOWED_ORIGINS` | `http://localhost:5173` | Lista separada por comas; sin `*` fuera de desarrollo |
| `REALTIME_RETENTION_HOURS` | `48` | 1–720 |
| `REALTIME_MAX_REPLAY` | `5000` | 100–100000 |
| `REALTIME_MAX_CLIENT_QUEUE` | `256` | 16–4096 |
| `REALTIME_MAX_CONN_PER_ACTOR` | `5` | 1–50 |
| `REALTIME_PING_SEC` | `25` | 5–60 |
| `REALTIME_PONG_TIMEOUT_SEC` | `10` | 2–30 |
| `REALTIME_WATERMARK_SEC` | `30` | 5–300 |
| `REALTIME_HELLO_TIMEOUT_SEC` | `5` | 1–30 |
| `REALTIME_MAX_MESSAGE_BYTES` | `4096` | 512–65536 |

### 14.9 Logs y `/health`

**Logs (JSON):**
- conexión aceptada o cerrada (`connId`, `actor`, `channels`, `code` y duración);
- recuperación (`fromSeq`, `toSeq` y `count`);
- reconexión del `LISTEN`;
- purga (`deleted`);
- errores.

**Nunca** se registran el ticket, el `payload` ni los correos.

**`/health`:**
```json
{
  "status": "ok",
  "db": "ok",
  "listener": "ok",
  "connections": 42,
  "lastProcessedSeq": 91822,
  "visibleSeq": 91822,
  "lagSeq": 0,
  "queueOverflows": 0,
  "contractVersion": 1,
  "uptimeSec": 3600
}
```
Responde `503` si `db != ok`, si el `listener` lleva más de 30 s caído o si `lagSeq > 1000` durante más de 60 s.

### 14.10 Lista de seguridad del servicio

- [ ] Solo `GET /ws` y `GET /health` expuestos; `/health` no revela datos de negocio.
- [ ] `Origin` exacto y ticket de un solo uso que vence en 30 s.
- [ ] Usuario de BD sin permisos de escritura en tablas de negocio.
- [ ] El cliente solo puede enviar `hello`/`pong` (4 KB máximo); nada de lo que envía cambia sus canales ni sus permisos.
- [ ] Límites por actor y por IP.
- [ ] TLS (`wss://`) en el proxy fuera de desarrollo.
- [ ] Sesión revocada, cuenta inactiva o permisos retirados se aplican **en vivo**.
- [ ] Logs sin datos sensibles.

---

## 15. Especificación del productor (API)

### 15.1 Regla de oro

> Un evento se registra **dentro** de `db.WithTx`, después del cambio que describe y con los datos que devolvió la BD (`RETURNING`), nunca con los datos que mandó el cliente. Nunca después del commit.

### 15.2 Qué ocurre en `Flush` (antes del COMMIT)

1. Si el buffer está vacío, no hace nada.
2. **Stock:** agrupa las filas por `cause`, conserva la última versión por `stockId` y calcula los `totals` por (producto, bodega): filas del buffer + `SELECT SUM(available), SUM(reserved) FROM stock WHERE product_id = ANY($1) AND warehouse_id = ANY($2) GROUP BY …` dentro de la misma transacción. Parte en lotes de 200 filas, con el mismo `correlationId`.
3. Completa en cada evento: `actor` (de `ActorFromContext` + caché de nombres), `correlationId` (cabecera `X-Request-ID` que pone el middleware `RequestID`) y `company_id` si falta (del actor).
4. `InsertBatch` en `realtime_events` (`tx_id` lo pone el `DEFAULT`), `RETURNING seq`.
5. `SELECT pg_notify('erp_events', <máximo seq>)`.
6. Cualquier error → la transacción **completa** hace rollback. El cambio de negocio tampoco queda: preferimos fallar a dejar un cambio sin su evento.

### 15.3 Ruta → evento (lista de emisión que se implementa en E8–E9)

| Ruta | Servicio | Eventos de la misma transacción |
|---|---|---|
| `POST /companies` · `PUT` · `DELETE` | companies | `entity.created/updated/status_changed (company)` (+ `session.revoked` a todos sus actores si se desactiva) |
| `POST /admins` · `PUT` · `DELETE` | staff (admin) | `entity.* (admin)` (+ `session.revoked` si se desactiva; `account.updated` si es él mismo) |
| `POST /warehouses` · `PUT` · `DELETE` | warehouses | `entity.* (warehouse)` (+ `session.revoked` a sus empleados si se desactiva) |
| `PUT /warehouses/:id/facturation` | warehouses | `entity.updated (warehouse_billing)` |
| `PUT /doc-series/:id` | warehouses | `entity.updated (doc_series)` |
| `POST/PUT/DELETE /warehouses/:id/locations…` | warehouses (ubicaciones) | `entity.created/updated/deleted o status_changed (location)` |
| `POST/PUT/DELETE /customers` | customers | `entity.* (customer)` |
| `POST /customers/:id/documents` · `…/renew` · `DELETE …` | customers (documentos) | `entity.created/updated/deleted (customer_document)` |
| `POST/PUT/DELETE /externals` | externals | `entity.* (supplier \| carrier)` |
| `POST/PUT/DELETE /brands`, `/categories`, `/models` | catalog | `entity.* (brand \| category \| model)` |
| `POST/PUT/DELETE /products` | products | `entity.* (product)` (+ `product.prices_changed` si cambian precios; + `stock.changed` si se crea con stock inicial) |
| `POST/DELETE /products/:id/codes` | products | `entity.created/deleted (product_code)` |
| `POST/DELETE /products/:id/suppliers` | products | `entity.created/deleted (product_supplier)` |
| `POST/PUT/DELETE /roles` | staff (roles) | `entity.* (role)` |
| `POST/PUT/DELETE /employees` | staff | `entity.* (employee)` (+ `session.revoked` si se desactiva) |
| `PUT /employees/:id/credentials` | staff | `session.revoked (password_changed)` |
| `PUT /employees/:id/permissions` | staff | `permissions.changed` |
| `PUT /auth/me` · `PUT /auth/me/password` | auth (Fase 7) | `account.updated` · `session.revoked (password_changed)` a sus **otras** sesiones |
| `POST /auth/logout` | auth | `session.revoked (logout, sessionId)` |
| `POST /settings/screensaver` · `PUT` | screensaver | `entity.updated (screensaver_settings)` |
| `POST /stock/adjust` · `/assign` · `/relocate` | inventory | `stock.changed (adjustment \| relocation)` |
| `POST /transfers` | inventory | `transfer.created` + `stock.changed (reservation)` |
| `POST /transfers/:id/dispatch` · `/receive` · `/cancel` | inventory | `transfer.status_changed` + `stock.changed (transfer_out \| transfer_in \| release)` |
| `POST /counts` · `PUT /counts/:id/items` · `/verify` · `/corroborate` | inventory | `count.created` · `count.progress_changed` · `count.status_changed` (+ `stock.changed (count_adjust)` al corroborar) |
| `POST/DELETE /carts…` · `…/items` | carts | `cart.updated` |
| `POST /quotations` · `PUT /quotations/:id` | quotations | `quotation.created` · `quotation.updated` / `quotation.status_changed` |
| `POST /sales` | sales | `sale.created` + `stock.changed (reservation)` (+ `credit.balance_changed` si es a crédito; + `quotation.status_changed (converted)` si viene de una cotización) |
| `POST /sales/:id/verify` · `/verify-payment` · `/pick` · `/pack` · `/dispatch` · `/send` | sales | `sale.stage_changed` (+ `stock.changed (sale_out)` al despachar) |
| `POST /sales/:id/cancel` | sales | `sale.stage_changed (→cancelled)` + `stock.changed (release)` (+ `credit.balance_changed`) |
| `POST /sales/:id/invoice` | sales | `invoice.issued` + `sale.stage_changed` |
| `POST /invoices/:id/annul` | sales | `invoice.annulled` (+ lo que la anulación revierta) |
| `POST /payments` · `/batch` | payments | `payment.created` + `sale.payment_changed` + `credit.balance_changed` + `cash_session.totals_changed` |
| `POST /payments/:id/verify` · `/reject` · `/void` · `/reopen` | payments | `payment.status_changed` + `sale.payment_changed` + `credit.balance_changed` |
| `POST /wallet/adjust` | wallet | `credit.balance_changed` |
| `POST /bank-statements/upload-pdf` | bank_statements | `bank_statement.imported` |
| `PATCH /bank-statements/records/:id/confirm` · `/ignore` | bank_statements | `bank_record.status_changed` (+ `payment.status_changed` si concilia un pago) |
| `POST /cash/sessions/open` · `/close` | cash_sessions | `cash_session.status_changed` |
| `POST /purchases` | purchases | `purchase.created` |
| `POST /purchases/:id/receive` · `/void` | purchases | `purchase.status_changed` (+ `stock.changed (purchase_in)` al recibir) |
| `POST /purchases/:id/pay` | purchases | `purchase.payment_recorded` + `cash.expense_recorded` + `cash_session.totals_changed` |
| `POST /purchases/suppliers/:id/catalog/upload` · `PUT/DELETE …/products` | purchases | `supplier_catalog.imported` · `entity.updated/deleted (product_supplier)` |
| `POST /returns` · `/:id/approve` · `/:id/reject` | returns | `return.created` · `return.status_changed` (+ `stock.changed (return_in)` y `credit.balance_changed` al aprobar) |
| `POST /recycle-bin/:id/restore` | recyclebin | `entity.restored` + `notification.created` (al admin) |
| `POST /notifications/:id/read` · `/read-all` | notifications | `notification.read` |
| (todo `INSERT INTO notifications`) | `notification_repo.go` | `notification.created` |
| (todo `auditRepo.Insert` dentro de una transacción) | auth/audit_repo | `audit.logged` |
| `POST /backups` · `/:id/restore` | backups | `backup.created` · `backup.restored` (este último se inserta **después** de `pg_restore`, en una transacción propia) |

### 15.4 Endpoints que deben responder la entidad completa

Todo endpoint de la tabla 15.3 que hoy responde solo éxito o un id pasa a responder **el mismo DTO que su `GET /:id`**, con `updatedAt`. La etapa P confirma la lista exacta. Ya se sabe que incluye `POST /transfers/:id/dispatch`, `/receive` y `/cancel`, y probablemente las etapas de pedido y los cambios de estado de pagos.

### 15.5 Sesiones afectadas al desactivar

Dentro de la misma transacción:

```sql
-- empleados de una bodega que se desactiva
SELECT rt.id AS session_id, e.id AS employee_id
FROM refresh_tokens rt JOIN employees e ON e.id = rt.employee_id
WHERE e.primary_warehouse_id = $1 AND rt.revoked_at IS NULL AND rt.expires_at > now();
```

(Igual para la compañía, sumando admins y empleados.) Se emite un `session.revoked` por **actor**, con `sessionId: null`, para cerrar todas sus pestañas. Revocar las sesiones en `refresh_tokens` sigue siendo responsabilidad de la regla actual de la API: el evento solo cierra los sockets.

### 15.6 `POST /realtime/ticket`

- **Pide:** cuerpo vacío. Cabecera `Authorization: Bearer <access_token>`.
- **Responde 200:**
  ```json
  { "success": true, "data": { "ticket": "q2Zr…(43 caracteres base64url)", "wsUrl": "ws://localhost:3002/ws", "expiresAt": "2026-11-12T15:04:35Z" } }
  ```
- **Errores:** 401 (`MISSING_TOKEN`, `SESSION_REVOKED` o `ACCOUNT_UNAVAILABLE`, los mismos del middleware); 429 (`TICKET_RATE_LIMIT`, más de 5 por minuto por sesión).
- El ticket **no** se registra en logs; solo su hash va a la BD.

---

## 16. Especificación del front

### 16.1 Máquina de estados de la conexión (`realtime.service.ts`)

| Estado | Entra cuando | Sale a |
|---|---|---|
| `idle` | Sin sesión, o `VITE_REALTIME_ENABLED=false` | `connecting` al iniciar sesión |
| `connecting` | Pide ticket → abre `WebSocket(wsUrl?ticket=…)` | `open` al recibir `system.welcome`; `reconnecting` si falla |
| `open` | Recibió `welcome` → envía `hello { lastSeq }` | `replaying` si `lastSeq != null`; `live` si es null |
| `replaying` | Recibiendo recuperación | `live` con `system.replay_completed`; `resyncing` con `system.resync_required` |
| `resyncing` | Debe volver a cargar | `live` tras `resyncVisibleScreen()` + `lastSeq = currentSeq` |
| `live` | Normal | `reconnecting` (cierres 1001/1006/1011/4001/4408/4400), `offline` (4003/4429), `idle` (4401/4403/logout) |
| `reconnecting` | Espera creciente (1 s → 30 s, con ±20 % de variación aleatoria) | `connecting` |
| `offline` | Error de configuración o demasiadas pestañas | `connecting` solo si el usuario recarga la app |

Además:
- Al quedar `navigator.onLine` en `false`, pasa a `reconnecting` y espera el evento `online`.
- Al volver la pestaña a visible con más de 60 s oculta y el estado no es `live`, intenta conectar de inmediato.

### 16.2 Adaptador (`realtime.adapter.ts`)

1. `JSON.parse` dentro de `try`; si falla → `null` (y `console.warn` solo en desarrollo).
2. Verifica `v === 1`, `typeof type === 'string'` y que `type` esté en la lista de los 48 tipos; si no → `null`.
3. Verifica `seq` (number), `occurredAt` (string) y `data` (object).
4. Devuelve el objeto **tal cual** (sin renombrar), con tipo `RealtimeEvent`. La forma fina de `data` la revisan las guardas del manejador que la usa.

### 16.3 Secuenciador (`realtime.sequencer.ts`)

- `lastSeq` vive en el slice `realtime` (no se persiste).
- `accept(e)`:
  - `system.*` con `seq 0` → acepta;
  - si `e.seq <= lastSeq` → descarta (duplicado de una recuperación);
  - si no → acepta y fija `lastSeq = e.seq`.
- `system.watermark` → `lastSeq = max(lastSeq, data.seq)`.
- `system.welcome` en una pestaña nueva → `lastSeq = currentSeq`.
- **No** se detectan huecos por saltos de `seq`: son normales (14.7). Una pérdida solo puede ocurrir al caerse la conexión, y eso lo cubre el `hello` con `lastSeq`.

### 16.4 Enrutador y patrón de manejador

```ts
// realtime.router.ts · registro tipo → manejadores y ejecución en orden; un manejador que falla no detiene a los demás
type Handler<T extends RealtimeEventType> = (event: RealtimeEnvelope<T>, api: { dispatch: AppDispatch; getState: () => RootState }) => void
const handlers = new Map<RealtimeEventType, Handler<RealtimeEventType>[]>()
export const registerRealtimeHandlers = (map: Partial<{ [K in RealtimeEventType]: Handler<K>[] }>) => { /* agrega a la lista de cada tipo */ }
export const routeRealtimeEvent = (event: RealtimeEvent, api) => {
  for (const h of handlers.get(event.type) ?? []) {
    try { h(event, api) } catch (error) { if (import.meta.env.DEV) console.error('[realtime]', event.type, error) }
  }
}
```

```ts
// features/transfers/realtime/transfers.realtime.ts · manejadores de traslados: aplica creación, cambios de estado y stock del origen
// conecta con: realtime (contrato y registro), transfers.slice (acciones *FromEvent) y transfers.adapter (DTO → modelo)
import type { RealtimeHandlers } from '@/features/realtime'
import { transferUpsertedFromEvent, transferStatusFromEvent, warehouseStockFromEvent } from '../slices/transfers.slice'
import { toTransferFromEvent } from '../helpers/transfers.adapter'

export const transfersRealtimeHandlers: RealtimeHandlers = {
  'transfer.created': [(e, { dispatch }) => dispatch(transferUpsertedFromEvent(toTransferFromEvent(e.data.transfer)))],
  'transfer.status_changed': [(e, { dispatch, getState }) => {
    dispatch(transferStatusFromEvent(e.data))
    notifyIfOtherWarehouse(e, getState())          // toast solo a la otra bodega (16.8)
  }],
  'stock.changed': [(e, { dispatch }) => dispatch(warehouseStockFromEvent(e.data.rows))],
}
```

Reglas de todo manejador:
- **No** hace peticiones HTTP, salvo con `invalidate` explícito (`bank_statement.imported` y `supplier_catalog.imported`).
- Si la entidad no está en memoria, **ignora** el evento (no la va a buscar).
- Usa el adaptador de su feature para convertir el DTO.
- Toda escritura en un slice pasa por `applyVersioned`.

### 16.5 `applyVersioned`

```ts
// applyVersioned · aplica un cambio solo si su versión es más nueva que la guardada; base de todos los reducers por evento y de las cargas de listas
export const isNewer = (incoming?: string | null, current?: string | null): boolean =>
  !current || (!!incoming && Date.parse(incoming) > Date.parse(current))
```

Los ISO con microsegundos se comparan con `Date.parse`, que trunca a milisegundos. Si son iguales al milisegundo, se comparan como texto: el formato ISO UTC se ordena bien alfabéticamente.

### 16.6 Resaltado (`useRecentlyChanged`)

- `useRecentlyChanged(key: string, version?: string): boolean`: `true` durante 1,5 s cuando `version` cambia **por un evento**, no por la carga inicial.
- La clase `is-recently-changed` en la fila o el campo usa el token `--color-highlight-bg` (se agrega a `styles/tokens.css` con su versión oscura) y una transición `--duration-enter`. Se respeta `prefers-reduced-motion`.

### 16.7 Indicador `RealtimeStatus` (en el `AppHeader`)

| Estado | Se ve | Texto (tooltip / `aria-label`) |
|---|---|---|
| `live` | Punto verde | «Actualización en tiempo real activa» |
| `connecting`, `replaying`, `resyncing`, `reconnecting` | Punto ámbar con pulso | «Reconectando… los datos pueden tardar en actualizarse» |
| `offline` | Punto gris | «Sin tiempo real: los cambios de otros usuarios aparecerán al volver a esta pestaña» |

Sin botón de recargar. Solo tokens de diseño.

### 16.8 Política de avisos

| Situación | Aviso |
|---|---|
| Cambio hecho por el propio usuario | Nada extra (el toast lo da su acción, como hoy) |
| Cambio ajeno en una lista visible | Solo resaltado de fila |
| Cambio ajeno en el registro abierto en el drawer | Franja en el drawer: «*Ana Pérez* cambió este registro» (+ motivo si lo hay) y acciones que ya no aplican deshabilitadas con tooltip |
| Cambio ajeno en un formulario de edición abierto | Franja de conflicto (E10) |
| Traslado hacia mi bodega (creado o despachado) / desde mi bodega (recibido, novedad o cancelado) | Toast informativo |
| Pedido que llega a una etapa que yo atiendo (según mis permisos) | Toast informativo |
| `notification.created` | El contador y el panel de notificaciones actuales |
| Producto o cliente de mi carrito desactivado | Aviso en la línea del carrito, sin toast |
| `session.revoked` / cuenta no disponible | El aviso actual de sesión cerrada |
| `permissions.changed` | Toast «Tus permisos cambiaron»; si la pantalla actual ya no está permitida, se lleva al usuario a la primera permitida |

### 16.9 Carrera entre la carga HTTP y los eventos

- La conexión se abre al iniciar sesión, **antes** de que las pantallas carguen.
- Un evento puede llegar antes, durante o después de la respuesta HTTP de una lista. Para que nunca gane el dato viejo, **los reducers de carga de listas** (`load*Thunk.fulfilled`) también usan `isNewer` por fila: si la fila en memoria tiene una versión más nueva (llegada por evento), se conserva.
- Un evento de una entidad que todavía no está en memoria se ignora: la carga HTTP la trae ya actualizada.

### 16.10 Varias pestañas

- Cada pestaña tiene su socket (hasta `REALTIME_MAX_CONN_PER_ACTOR`).
- El carrito y las notificaciones se sincronizan entre pestañas por `cart.updated` y `notification.*` en el canal del actor.
- Compartir un solo socket entre pestañas (`BroadcastChannel`) queda como mejora futura (decisión 5).

### 16.11 Sin conexión e interruptor

- Mientras el estado no sea `live`, sigue activa la revalidación al volver a la pestaña que dejó la Fase 5 del plan principal.
- Con `VITE_REALTIME_ENABLED=false`, el front no se conecta y la app funciona con esa revalidación, como al terminar el plan principal. Sirve para desplegar de forma gradual (18.4).

---

## 17. Estrategia de pruebas

### 17.1 Niveles

| Nivel | Dónde | Qué cubre |
|---|---|---|
| Contrato | `importadoraRealtime/contract/scripts/validate.go`, `importadoraApi/src/core/events/contract_test.go`, `importadoraInterfaz/src/features/realtime/realtime.contract.test.ts` | Los 48 ejemplos válidos en los tres lados |
| Unitarias | `*_test.go` y `*.test.ts` al lado de cada archivo | Hub, filtro, canales, cola, secuenciador, adaptador, `applyVersioned` y manejadores |
| Integración con BD | API (buffer, rollback y `tx_id`) y realtime (listener, recuperación, tickets y control) | Comportamiento real con PostgreSQL |
| E2E HTTP | `importadoraApi/scripts/e2e/flow_realtime_*.py` | Que cada mutación deje sus filas en `realtime_events` |
| E2E websocket | `importadoraRealtime/scripts/e2e/flows/*.go` | Canales, permisos, recuperación, revocación, varias instancias, reinicio y carga |
| Manual | Lista 12.5 con 4 navegadores | Experiencia completa |

### 17.2 Prueba guardia de eventos (API)

`src/core/bootstrap/events_guard_test.go` lee como texto los archivos de `src/features/*/services/*.go` (igual que `routes_guard_test.go`) y falla si:
1. un archivo contiene `db.WithTx(` y `Exec`/`QueryRow` de `INSERT|UPDATE|DELETE` dentro, pero ninguna llamada a `s.events.Record(` ni a `s.events.Stock(`;
2. un archivo llama a `ReserveStock|ReleaseReservation|DeductStock|UpsertStockDelta|SetStockAvailable` sin `s.events.Stock(`.

Lleva una lista `exceptions` con su justificación (por ejemplo `login`/`refresh`, que no emiten). Así, quien agregue una mutación sin evento ve la prueba fallar con el archivo y la línea.

### 17.3 Prueba de commits fuera de orden (obligatoria)

`importadoraRealtime/src/features/events/services/listener_out_of_order_test.go`:
1. Abre la transacción A e inserta un evento (`seq` 10), **sin** confirmar.
2. Abre la transacción B, inserta un evento (`seq` 11) y confirma.
3. Afirma que el listener **no** entregó el 11.
4. Confirma A.
5. Afirma que entregó el 10 y el 11, en ese orden.

### 17.4 Comandos de verificación por directorio

| Directorio | Comando |
|---|---|
| `importadoraRealtime` | `go build ./... && go vet ./... && go test -race ./... && go run ./contract/scripts/validate.go` |
| `importadoraApi` | `go build ./... && go vet ./... && go test ./...` + los e2e de `scripts/e2e` contra una base de prueba |
| `importadoraInterfaz` | `pnpm exec tsc -b && pnpm lint && pnpm test && pnpm check:api && pnpm check:type` |

### 17.5 Datos de prueba

- Todas las pruebas con BD usan la base local creada desde `startdb.pgsql`, con su semilla (compañía 1, bodegas y empleados de prueba).
- Cada prueba limpia lo que crea (o corre dentro de una transacción con rollback, cuando no prueba el commit).
- Los e2e de websocket reutilizan los usuarios de la semilla (superadmin, admin y empleados por rol).

---

## 18. Operación

### 18.1 Desarrollo local

```bash
cd importadoraApi && go run .
```
```bash
cd importadoraRealtime && go run .
```
```bash
cd importadoraInterfaz && pnpm dev
```

Puertos: API `:3001`, realtime `:3002`, front `:5173`. Para crear el usuario `importadora_realtime` en local, se le da una contraseña con `ALTER ROLE importadora_realtime PASSWORD '…'` y se pone en `importadoraRealtime/.env`, que no se versiona.

### 18.2 Despliegue (Railway, decisión 10)

- **Tres servicios independientes.** La API ya corre en Railway (`importadora-back-production.up.railway.app`). `importadoraRealtime` se agrega como **otro servicio del mismo proyecto de Railway**, creado desde este repositorio:
  - **Root Directory:** `importadoraRealtime` (así Railway solo construye ese módulo Go; la API y el front no se tocan).
  - **Build:** detección automática de Go (`go build -o realtime .`); **Start command:** `./realtime`.
  - **Puerto:** Railway asigna `PORT`. El servicio escucha en `PORT` y usa `APP_PORT` (3002) solo si `PORT` no existe (`core/config` lo resuelve así).
  - **Healthcheck:** `/health`.
  - **Dominio:** el que genera Railway (`<servicio>.up.railway.app`), con TLS ya incluido: el front se conecta a `wss://<servicio>.up.railway.app/ws`.
  - **Base de datos:** `DB_HOST`, `DB_PORT` y `DB_DATABASE` apuntan al mismo Postgres de la API por la red privada de Railway, con `DB_USER=importadora_realtime` (mínimo privilegio, sección 5). Su contraseña vive solo en las variables de Railway.
  - **Variables:** las de la tabla 14.8, con `REALTIME_ALLOWED_ORIGINS=https://<dominio del front>` y, en la API, `REALTIME_WS_URL=wss://<servicio>.up.railway.app/ws` (la devuelve `POST /realtime/ticket`).
  - **Réplicas: 1.** El hub vive en memoria; con más réplicas cada una tendría su propio `LISTEN` y el tope de 5 pestañas contaría por réplica. Escalar a varias réplicas es una mejora posterior.
- **Orden de despliegue:**
  1. migración de BD;
  2. API, que empieza a escribir eventos (inofensivo si nadie los lee: la purga los limpia);
  3. servicio realtime;
  4. front con `VITE_REALTIME_ENABLED=true`.
- **Para bajar:** en orden inverso. Apagar el servicio realtime no afecta la API.

### 18.3 Proxy inverso

En Railway **no hace falta nginx**: su borde ya termina TLS y deja pasar el `Upgrade` de websocket. El ping de `REALTIME_PING_SEC` mantiene viva la conexión aunque no haya eventos. El ejemplo de abajo queda solo como referencia si algún día se despliega en un servidor propio.


```nginx
location /ws {
    proxy_pass http://127.0.0.1:3002/ws;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_read_timeout 75s;      # mayor que REALTIME_PING_SEC + REALTIME_PONG_TIMEOUT_SEC
    proxy_send_timeout 75s;
}
```

Si se usa `/ws` en el mismo dominio del front, `REALTIME_WS_URL=wss://<dominio>/ws` y `REALTIME_ALLOWED_ORIGINS=https://<dominio>`.

### 18.4 Despliegue gradual y cómo deshacerlo

| Interruptor | Dónde | Efecto |
|---|---|---|
| `VITE_REALTIME_ENABLED` | Front | `false` → no se conecta; la app funciona como al terminar el plan principal (revalidación al volver a la pestaña) |
| Apagar `importadoraRealtime` | Servidor | Los clientes quedan en `reconnecting` y siguen funcionando con la revalidación; al volver, recuperan por `seq` |
| `REALTIME_EVENTS_ENABLED` (API, por defecto `true`) | API | `false` → `Recorder` no escribe eventos. Solo para emergencias: con el websocket encendido, los clientes dejarían de ver cambios ajenos |

**Hasta que E9 esté completa**, los GET de seguimiento de cada dominio se quitan **en el mismo PR** que agrega sus eventos, nunca antes. Así, en todo momento cada dominio tiene una de las dos cosas: sus eventos o su GET de seguimiento.

### 18.5 Monitoreo

- `GET /health` del servicio realtime, consultado por el monitor del servidor.
- Alertas sugeridas:
  - `lagSeq > 1000` durante 1 min;
  - `queueOverflows` crece;
  - `listener != ok` durante 30 s;
  - crecimiento de `realtime_events` (`SELECT count(*)`) por encima de lo esperado.

### 18.6 Respaldos y tiempo real

- Los respaldos (`features/backups`) excluyen los **datos** de `realtime_events` y `realtime_tickets` (`pg_dump --exclude-table-data=realtime_events --exclude-table-data=realtime_tickets`). No tiene sentido restaurar eventos viejos ni tickets.
- Tras `pg_restore`, la API inserta `backup.restored`. El servicio detecta que la tabla volvió atrás (14.7) y manda `system.resync_required (backup_restored)` a todos. Esto reemplaza el `window.location.reload()` actual.

---

## 19. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Commits fuera de orden pierden eventos | Cambios que nunca llegan | `tx_id xid8` + `pg_snapshot_xmin` (14.7) y la prueba obligatoria 17.3 |
| Una mutación nueva sin evento | Pantallas desactualizadas sin que nadie lo note | Prueba guardia 17.2 + catálogo revisado en la etapa P |
| `Flush` falla y hace rollback del cambio de negocio | Una operación falla por el tiempo real | Es intencional (consistencia). `Flush` solo hace un `INSERT` en lote y un `pg_notify`; se vigila en logs y alertas |
| Consultas de acceso duplicadas entre la API y el servicio realtime se desalinean | Alguien recibe lo que no debe, o no recibe lo que sí | Comentario «copia de…» + prueba de integración que compara el resultado de ambos para los mismos actores de la semilla |
| Tabla `realtime_events` crece sin control | Disco y lentitud | Purga cada hora, índice por `created_at` y alerta de tamaño |
| Ráfagas (conteos o recepciones grandes) | Clientes lentos y colas llenas | Agrupación por `stockId`, lotes de 200, serialización una vez por evento y cierre 4408 con recuperación |
| El cliente aplica un dato viejo de una carga HTTP | Parpadeo o regreso de un valor viejo | `isNewer` también en cargas de listas (16.9) |
| Fuga de datos por canal mal calculado | Seguridad | Pruebas de tabla en `filter_test.go`/`channels_test.go` + e2e `channels.go`/`permissions.go` |
| Ticket robado | Suplantación por 30 s | Un solo uso, 30 s, ligado a la sesión, `Origin` exacto y TLS |
| Cambio de contrato rompe un lado | Eventos ignorados | Schemas únicos + pruebas de contrato en los tres lados + `CHANGELOG` y `v` |
| El plan principal terminó distinto a lo esperado | Este plan no encaja | Etapa P obligatoria antes de E0 |

---

## 20. Definición de terminado (todo el websocket)

- [ ] Etapa P verificada y secciones 6.3, 7.2 y 12.4 ajustadas a lo encontrado.
- [ ] Contrato v1 congelado con 48 schemas y 48 ejemplos válidos en los tres directorios.
- [ ] Migración aplicada con evidencia; `startdb.pgsql` crea la base equivalente.
- [ ] `importadoraRealtime` con todas sus pruebas (incluida 17.3) y e2e en verde; `/health` estable.
- [ ] La API emite los eventos de la tabla 15.3 y la prueba guardia 17.2 está en verde.
- [ ] El front aplica cada evento de la matriz 6.4, sin GET de seguimiento en ningún thunk de mutación, sin `setInterval` de notificaciones y sin `window.location.reload()`.
- [ ] La lista 12.5 se cumple completa con 4 navegadores.
- [ ] Interruptores de 18.4 probados (encendido y apagado).
- [ ] Documentación y cabeceras al día en los tres directorios.
