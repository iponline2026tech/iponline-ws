# Contrato del websocket · v1

> **Estado: congelado v1** (11-nov-2026). Fuente única del formato de los mensajes entre `importadoraRealtime` y el front.
> Los archivos que mandan son los JSON Schema de `schemas/`; este documento los explica. `go -C importadoraRealtime run ./contract/scripts/validate.go` (desde la raíz del repo; dentro de `importadoraRealtime/` basta `go run ./contract/scripts/validate.go`) valida los 48 ejemplos de `examples/` contra ellos.
> Lo implementan `importadoraApi/src/core/events/types.go` (productor), `importadoraRealtime/src/core/protocol` (sobre) e `importadoraInterfaz/src/features/realtime/types/realtime.contract.ts` (consumidor). Cada lado tiene una prueba que valida estos ejemplos.

## 1. Sobre

Todo mensaje del servidor al cliente es un sobre (`schemas/envelope.schema.json`):

| Campo | Tipo | Qué es |
|---|---|---|
| `v` | `1` | Versión del contrato |
| `seq` | entero ≥ 0 | Posición en `realtime_events`. `0` en los `system.*` que no salen de la BD |
| `id` | texto | Id único del evento (UUID) |
| `type` | uno de los 48 tipos | Qué pasó |
| `occurredAt` | fecha ISO | Cuándo se confirmó el cambio |
| `companyId` | entero o `null` | Compañía (`null`: plataforma) |
| `warehouseIds` | enteros | Bodegas involucradas (traslado: origen y destino) |
| `actor` | `{ type, id, name }` | Quién lo hizo (`employee`, `admin`, `superadmin` o `system`) |
| `correlationId` | texto, opcional | El `X-Request-ID` de la petición HTTP que lo causó |
| `data` | objeto | El cuerpo; su forma está en `schemas/events/<type>.schema.json` |

- El sobre no admite campos extra; `data` sí (un campo nuevo opcional no rompe a nadie).
- Todos los campos de cada cuerpo son obligatorios, aunque su valor pueda ser `null`.
- `row`, `sale`, `payment`, `purchase`, `return`, `quotation`, `cart` y `user` son objetos con la forma del DTO de la API (`GET /:id` o su lista); los valida la API, no este contrato.
- Toda versión (`version`) es el `updated_at` de la fila en ISO con microsegundos; sirve para descartar eventos viejos o repetidos.

## 2. Mensajes del cliente

Los únicos que acepta el servidor (`schemas/client-message.schema.json`). Cualquier otro, o uno de más de 4 KB, cierra con `4400`.

| Mensaje | Cuándo |
|---|---|
| `{ "type": "hello", "v": 1, "lastSeq": <entero o null> }` | Al abrir; `lastSeq` es lo último aplicado (o `null` la primera vez) |
| `{ "type": "pong", "ts": <entero> }` | Respuesta a cada ping del servidor |

## 3. Canales

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

Por la decisión 2, el superadmin solo recibe `platform` y su canal personal: nunca `company:*` ni `warehouse:*`.

## 4. Convención de nombres

- `<dominio>.<hecho_en_pasado>`, en inglés y en minúscula: `transfer.status_changed`, `stock.changed`.
- **Datos maestros** (entidades sin flujo de estados propio) usan los eventos genéricos `entity.*` con `entity.kind`. Así una tabla nueva de catálogo no necesita tipos nuevos.
- **Documentos con flujo** (pedido, traslado, compra, pago…) tienen eventos propios, porque llevan datos del flujo (líneas, montos, quién despachó o recibió).
- Todo evento con entidad lleva `entity.version` (`updated_at`) para descartar duplicados y eventos viejos.

## 5. Catálogo (48 tipos)

### A · Sistema y protocolo (no salen de la BD; los genera `importadoraRealtime`)

| Tipo | Cuándo | Canal | Cuerpo |
|---|---|---|---|
| `system.welcome` | Al aceptar la conexión | (solo esa conexión) | `currentSeq`, `channels`, `heartbeatSec`, `contractVersion` |
| `system.replay_completed` | Al terminar de reenviar los eventos pendientes tras un `hello` | (solo esa conexión) | `fromSeq`, `toSeq`, `count` |
| `system.resync_required` | Hueco mayor que `REALTIME_MAX_REPLAY`, contrato nuevo o **respaldo restaurado** | conexión o `company:{id}` / `platform` | `reason: 'gap_too_large' \| 'contract_changed' \| 'backup_restored'`, `currentSeq` |
| `system.watermark` | Cada `REALTIME_WATERMARK_SEC` si avanzó lo procesado (14.7) | (cada conexión `live`) | `seq` (el cliente avanza su `lastSeq` aunque no haya recibido eventos) |

### B · Sesión y acceso (canal `actor:{tipo}:{id}`)

| Tipo | Lo emite (ruta de la API → servicio) | Cuerpo | Efecto en el front |
|---|---|---|---|
| `session.revoked` | `POST /auth/logout` (otras pestañas de esa sesión); `DELETE /employees/:id`, `/admins/:id`, `/companies/:id` y `/warehouses/:id` (desactivar); `PUT /employees/:id/credentials` y `PUT /auth/me/password` (cambio de contraseña) | `sessionId \| null`, `exceptSessionId \| null` (con `PUT /auth/me/password` la API deja viva la sesión que cambió la clave: esas conexiones **no** se cierran), `reason: 'logout' \| 'password_changed' \| 'account_inactive' \| 'company_inactive' \| 'warehouse_inactive'` | El servicio realtime **cierra** el socket (código 4401) y el front cierra la sesión con el aviso actual |
| `permissions.changed` | `PUT /employees/:id/permissions` | `actorType`, `actorId`, `permissions: string[]` | El servicio realtime recalcula los permisos de la conexión; el front actualiza `auth.permissions`, menú, acciones y guardias sin cerrar sesión |
| `account.updated` | `PUT /employees/:id`, `PUT /admins/:id` y `PUT /auth/me` (Fase 7) sobre la propia cuenta | `user` (misma forma que `GET /auth/me`), `version` | Encabezado, perfil y configuración muestran los datos nuevos |

### C · Datos maestros (genéricos `entity.*`)

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

### D · Precios

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `product.prices_changed` | `PUT /products/:id` cuando cambian `product_prices` (y la edición de precios del catálogo) | `company:{id}` | `catalog:view`, `products:view`, `quotations:view` | `productId`, `prices: {p1..p4}`, `ivaRate`, `version` |

Efecto: catálogo, tarjetas y **carritos abiertos** (las líneas ya agregadas conservan su precio; se muestra un aviso si el precio de lista cambió antes de crear el pedido).

### E · Inventario

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `stock.changed` | Todo escritor de stock (sección 12.1): ventas, traslados, ajustes (`POST /stock/adjust`), asignación (`/stock/assign`), reubicación (`/stock/relocate`), conteos, compras, devoluciones y stock inicial | `company:{id}` | `catalog:view`, `stock:view`, `transfers:view`, `products:view` | `cause`, `rows[]` (valores finales), `totals[]` |
| `transfer.created` | `POST /transfers` (uno por bodega de origen) | `warehouse:{origen}` + `warehouse:{destino}` | `transfers:view` | `transfer` (fila completa con líneas) |
| `transfer.status_changed` | `POST /transfers/:id/dispatch`, `/receive` y `/cancel` | `warehouse:{origen}` + `warehouse:{destino}` | `transfers:view` | `entity`, `serial`, `from`, `to` (`pending \| in_transit \| received \| with_novelty \| cancelled`), `lines[]` con `receivedQty`, `noveltyNotes`, `cancelReason`, `dispatchedBy`, `receivedBy` |
| `count.created` | `POST /counts` | `warehouse:{id}` | `stock:view` | `count` (fila completa) |
| `count.progress_changed` | `PUT /counts/:id/items` | `warehouse:{id}` | `stock:view` | `entity`, `items[]` (`productId`, `expectedQty`, `countedQty`), `countedLines`, `totalLines` |
| `count.status_changed` | `POST /counts/:id/verify` y `/corroborate` | `warehouse:{id}` | `stock:view` | `entity`, `from`, `to` (`pending \| verified \| corroborated`), `differences[]` |

### F · Ventas: carrito, cotizaciones, pedidos y facturas

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `cart.updated` | `POST /carts`, `DELETE /carts/:id`, `POST/PUT/DELETE /carts/:id/items` | `actor:{tipo}:{id}` (solo su dueño) | — | `cart` (misma forma que la respuesta del carrito). Sincroniza las otras pestañas del mismo usuario. |
| `quotation.created` | `POST /quotations` | `company:{id}` | `quotations:view` | `quotation` |
| `quotation.updated` | `PUT /quotations/:id` (líneas, cliente o vigencia) | `company:{id}` | `quotations:view` | `entity`, `row`, `changedFields` |
| `quotation.status_changed` | Conversión de la cotización a pedido (`POST /sales` con `quotationId`) y vencimiento a los 30 días (al listar o abrir cotizaciones) | `company:{id}` | `quotations:view` | `entity`, `from`, `to` (`pending \| converted \| expired`), `saleId` |
| `sale.created` | `POST /sales` | `warehouse:{id}` | `orders:view`, `invoices:view`, `cash:view` | `sale` (fila de la tabla de pedidos) |
| `sale.stage_changed` | `POST /sales/:id/verify`, `/verify-payment`, `/pick`, `/pack`, `/dispatch`, `/send` y `/cancel` | `warehouse:{id}` | `orders:view`, `cash:view` | `entity`, `serial`, `from`, `to` (`verification …`pending | converted | expired`\| cancelled`), `status` (`open \| completed \| cancelled`), `reason`, `takenBy` |
| `sale.payment_changed` | Pagos que afectan al pedido (`POST /payments`, `/batch`, `/verify`, `/void`, `/reject`, `/reopen`) | `warehouse:{id}` | `orders:view`, `cash:view` | `saleId`, `paidTotal`, `creditAmount`, `balance`, `paymentVerified`, `version` |
| `invoice.issued` | `POST /sales/:id/invoice` | `warehouse:{id}` | `invoices:view`, `orders:view` | `invoice` (serie, número DIAN, totales y `saleId`) |
| `invoice.annulled` | `POST /invoices/:id/annul` | `warehouse:{id}` | `invoices:view`, `orders:view` | `entity`, `saleId`, `reason` |

### G · Pagos, cartera y conciliación

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `payment.created` | `POST /payments` y `POST /payments/batch` (uno por pago) | `company:{id}` | `payments:view`, `credits:view` | `payment` (fila de Abonos) |
| `payment.status_changed` | `POST /payments/:id/verify`, `/reject`, `/void` y `/reopen` | `company:{id}` | `payments:view`, `credits:view` | `entity`, `from`, `to` (`review \| verified \| issue \| voided`), `reason` |
| `credit.balance_changed` | Ventas a crédito, pagos, devoluciones con saldo a favor y `POST /wallet/adjust` | `company:{id}` | `credits:view`, `customers:view`, `orders:create` | `customerId`, `creditLimit`, `used`, `available`, `favorBalance`, `status` (`ok \| warning \| blocked`), `entry` (`charge \| payment \| return \| adjust`, monto y referencia), `version` |
| `bank_statement.imported` | `POST /bank-statements/upload-pdf` | `company:{id}` | `payments:view` | `statementId`, `bank`, `recordsCreated`, `period` (aviso: la tabla vuelve a pedir **una** vez su página visible) |
| `bank_record.status_changed` | `PATCH /bank-statements/records/:id/confirm` y `/ignore` | `company:{id}` | `payments:view` | `entity`, `from`, `to` (`pending \| confirmed \| ignored`), `paymentId \| null` |

### H · Caja

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `cash_session.status_changed` | `POST /cash/sessions/open` y `/close` | `warehouse:{id}` | `cash:view`, `invoices:view` | `entity`, `from`, `to` (`open \| closed`), `openedBy`, `closedBy`, `totals` |
| `cash_session.totals_changed` | Pagos en efectivo o transferencia dentro de una caja abierta, gastos y pagos de compras desde caja | `warehouse:{id}` | `cash:view` | `sessionId`, `cashIn`, `transferIn`, `abonosIn` y `paymentsCount` (opcionales), `expenses`, `expectedCash`, `version` |
| `cash.expense_recorded` | Registro de gasto de caja (incluye `POST /purchases/:id/pay`, que deja el gasto `merchandise_arrival`) | `warehouse:{id}` | `cash:view`, `invoices:view` | `expense` (`expenseType`, `cashAmount`, `transferAmount`, concepto y referencia) |

### I · Compras y proveedores

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `purchase.created` | `POST /purchases` | `warehouse:{id}` | `purchases:view` | `purchase` |
| `purchase.status_changed` | `POST /purchases/:id/receive` y `/void` | `warehouse:{id}` | `purchases:view` | `entity`, `from`, `to` (`pending | converted | expired`\| ordered \| partial \| received \| voided`), `lines[]` con lo recibido, `reason` |
| `purchase.payment_recorded` | `POST /purchases/:id/pay` | `warehouse:{id}` | `purchases:view`, `cash:view` | `purchaseId`, `paid`, `balance`, `cashAmount`, `transferAmount` |
| `supplier_catalog.imported` | `POST /purchases/suppliers/:supplierId/catalog/upload` | `company:{id}` | `purchases:view`, `products:view` | `supplierId`, `linked`, `warnings` (aviso de invalidación) |

### J · Devoluciones y garantías

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `return.created` | `POST /returns` | `warehouse:{id}` | `returns:view` | `return` |
| `return.status_changed` | `POST /returns/:id/approve` y `/reject` | `warehouse:{id}` | `returns:view`, `orders:view` | `entity`, `from`, `to` (`pending \| approved \| rejected`), `resolution` (`exchange \| credit_balance`), `rejectReason` |

(La aprobación también emite, en la misma transacción, `stock.changed` (`return_in`) y, si aplica, `credit.balance_changed`.)

### K · Notificaciones, auditoría y respaldos

| Tipo | Lo emite | Canal | Permisos | Cuerpo |
|---|---|---|---|---|
| `notification.created` | Todo `INSERT INTO notifications` (por ejemplo, restaurar un registro o un traslado entrante) | `actor:{tipo}:{id}` del destinatario | — | `notificationId`, `kind` (`info \| success \| warning \| error \| restore_record`), `title`, `message`, `link` |
| `notification.read` | `POST /notifications/:id/read` y `/read-all` | `actor:{tipo}:{id}` | — | `ids[] \| 'all'` (sincroniza el contador en todas las pestañas) |
| `audit.logged` | Todo `auditRepo.Insert` (resumen, no el detalle) | `company:{id}` + `platform` | `audit:view` | `auditId`, `entity`, `entityId`, `action`, `actor`, `reason` (alimenta la tabla de Auditoría abierta) |
| `backup.created` | `POST /backups` | `platform` | superadmin | `backup` (nombre, tipo, tamaño y fecha) |
| `backup.restored` | `POST /backups/:id/restore` | `platform` | superadmin | `backupId`. Va acompañado de `system.resync_required (backup_restored)` a **todas** las conexiones (reemplaza el `window.location.reload()` de `useBackups.ts:73`). |

### Lo que **no** emite eventos (y por qué)

| Ruta | Motivo |
|---|---|
| `POST /auth/login`, `/auth/refresh` | No cambian datos que otros vean (el intento fallido no se publica) |
| `POST /audit-logs/context-change` | Lo hace el propio superadmin y solo deja la auditoría. No cambia sus canales: el superadmin sigue solo en `platform` (decisión 2), así que no pide ticket nuevo ni reconecta |
| `GET` de reportes (`/reports/*`) | Los reportes se marcan como desactualizados en el front cuando llegan eventos de venta, pago o stock, y se recalculan **una vez** si están en pantalla |
| `POST /employees/:id/unlock` | Se retira en la Fase 2.4 del plan principal |
| Vencimiento de documentos de cliente (por tiempo, sin ruta) | No hay mutación. Fase posterior: tarea programada en la API que emita `customer_document.expiring` |

## 6. Quién consume cada evento en el front

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

## 7. Códigos de cierre

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

## 8. Reglas de versión

- **No sube `v`:** agregar un campo **opcional** a un cuerpo, agregar un valor nuevo a un enum que el front ya ignora si no lo conoce, o agregar un tipo nuevo (el front ignora los tipos que no conoce).
- **Sí sube `v`:** quitar o renombrar un campo, cambiar su tipo, volver obligatorio uno opcional o cambiar el significado de un valor.
- Al subir `v`, el servidor manda `system.resync_required` con `reason: 'contract_changed'` a los clientes con la versión vieja, y se agrega la entrada en `CHANGELOG.md`.
- Todo cambio toca, en el mismo PR: el schema, el ejemplo, este documento, `types.go`, `realtime.contract.ts` y el `CHANGELOG`. `validate.go` debe seguir en `48/48` (o el número nuevo).
