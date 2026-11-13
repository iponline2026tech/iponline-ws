package server

// server_test · el servidor arranca, responde y se apaga en orden: deja de aceptar, corre OnShutdown (el hub cierra con 1001) y termina en < 10 s

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

func TestGracefulShutdown(t *testing.T) {
	port := freePort(t)
	srv := New(port)
	srv.Mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	closedHub := false
	srv.OnShutdown(func(context.Context) { closedHub = true })

	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	var res *http.Response
	var err error
	for i := 0; i < 50; i++ {
		if res, err = http.Get("http://127.0.0.1:" + port + "/ping"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("el servidor no respondió: %v", err)
	}
	res.Body.Close()

	start := time.Now()
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Start debe terminar sin error al apagar: %v", err)
	}
	if !closedHub || time.Since(start) > ShutdownTimeout {
		t.Fatalf("el apagado debe correr OnShutdown y durar menos de %s (corrió: %v, duró %s)", ShutdownTimeout, closedHub, time.Since(start))
	}
	if _, err := http.Get("http://127.0.0.1:" + port + "/ping"); err == nil {
		t.Fatal("después de apagar no debe aceptar conexiones")
	}
}
