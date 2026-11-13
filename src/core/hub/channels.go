package hub

// channels.go · nombres de canal y a qué canales queda suscrita una conexión (tabla 14.3) o va un evento (columnas de realtime_events)
// conecta con: hub.go (índice byChannel), filter.go y features/access (AccessContext)
// ojo: el superadmin solo queda en platform y en su canal personal, nunca en company:* ni warehouse:* (decisión 2)

import (
	"strconv"

	"importadoraRealtime/src/core/events"
	"importadoraRealtime/src/features/access/dtos"
)

const Platform = "platform"

func companyChannel(id int64) string   { return "company:" + strconv.FormatInt(id, 10) }
func warehouseChannel(id int64) string { return "warehouse:" + strconv.FormatInt(id, 10) }
func actorChannel(key string) string   { return "actor:" + key }

// ChannelsFor son los canales de una conexión.
func ChannelsFor(a dtos.AccessContext) []string {
	channels := []string{}
	if a.IsSuperadmin {
		return append(channels, Platform, actorChannel(a.ActorKey()))
	}
	if a.CompanyID != nil {
		channels = append(channels, companyChannel(*a.CompanyID))
	}
	for _, w := range a.WarehouseIDs {
		channels = append(channels, warehouseChannel(w))
	}
	return append(channels, actorChannel(a.ActorKey()))
}

// eventChannels son los canales donde puede estar un destinatario del evento (después se aplica Allowed).
// Personal → solo su canal; con warehouse_ids → esas bodegas; sin warehouse_ids → toda la compañía; Platform → también platform.
func eventChannels(e events.StoredEvent) []string {
	if e.ActorChannel != nil {
		return []string{actorChannel(*e.ActorChannel)}
	}
	var out []string
	if e.Platform {
		out = append(out, Platform)
	}
	if e.CompanyID != nil {
		if len(e.WarehouseIDs) == 0 {
			out = append(out, companyChannel(*e.CompanyID))
		} else {
			for _, w := range e.WarehouseIDs {
				out = append(out, warehouseChannel(w))
			}
		}
	}
	return out
}
