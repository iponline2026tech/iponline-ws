// main.go · raíz de composición de importadoraRealtime: configuración → base → contenedor → listener/purga → servidor; apagado ordenado con SIGINT/SIGTERM
// conecta con: src/core/{config,logging,db,server,bootstrap}; orden de arranque y apagado en PLAN-WEBSOCKET.md, sección 14.1
// ojo: este servicio NO importa importadoraApi: comparte con ella solo la base de datos y el contrato (contract/)
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"importadoraRealtime/src/core/bootstrap"
	"importadoraRealtime/src/core/config"
	"importadoraRealtime/src/core/db"
	"importadoraRealtime/src/core/logging"
	"importadoraRealtime/src/core/protocol"
	"importadoraRealtime/src/core/server"
)

func main() {
	if err := run(); err != nil {
		slog.Error("el servicio realtime se detuvo con error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// 1. configuración (si falla, el proceso termina con código 1 y el motivo)
	cfg, err := config.Load()
	if err != nil {
		logging.New(true)
		return err
	}
	logger := logging.New(cfg.IsDevelopment())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. base de datos (Ping de 10 s)
	pool, err := db.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	// 3–7. contenedor, secuencia visible inicial, hub, listener y purga
	container := bootstrap.NewContainer(cfg, pool)
	if err := container.Listener.Init(ctx); err != nil {
		return err
	}

	hubCtx, stopHub := context.WithCancel(context.Background())
	defer stopHub()
	go container.Hub.Run(hubCtx)

	workersCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	go container.Listener.Run(workersCtx)
	go container.Purge.Run(workersCtx)

	// 8. servidor http
	srv := server.New(cfg.AppPort)
	bootstrap.RegisterRoutes(srv.Mux, container)
	// apagado: el hub cierra todas las conexiones con 1001 y se detienen listener y purga
	srv.OnShutdown(func(context.Context) {
		container.Hub.CloseAll(protocol.CloseGoingAway)
		stopWorkers()
	})
	errs := make(chan error, 1)
	go func() { errs <- srv.Start() }()

	// 9. listo
	logger.Info("realtime listo", "port", cfg.AppPort, "env", cfg.AppEnv, "lastProcessed", container.Listener.LastProcessedSeq(), "contractVersion", 1)

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	logger.Info("apagando realtime")
	if err := srv.Shutdown(context.Background()); err != nil {
		logger.Warn("apagado con pendientes", "error", err)
	}
	logger.Info("realtime detenido")
	return nil
}
