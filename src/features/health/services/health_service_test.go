package services

// health_service_test · la base caída, el listener caído o el retraso alto responden 503 (sano = false)

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeDB struct{ err error }

func (f fakeDB) Ping(context.Context) error { return f.err }

type fakeListener struct {
	healthy, lagBad bool
	last, visible   int64
}

func (f fakeListener) Healthy(time.Time) bool      { return f.healthy }
func (f fakeListener) LastProcessedSeq() int64     { return f.last }
func (f fakeListener) VisibleSeq() int64           { return f.visible }
func (f fakeListener) LagUnhealthy(time.Time) bool { return f.lagBad }

type fakeHub struct{}

func (fakeHub) Connections() int      { return 42 }
func (fakeHub) QueueOverflows() int64 { return 1 }

func TestHealthyWithoutListenerYet(t *testing.T) {
	r, ok := NewHealthService(fakeDB{}, nil, nil).Check(context.Background())
	if !ok || r.Status != "ok" || r.DB != "ok" || r.Listener != "n/a" || r.LastProcessedSeq != nil || r.ContractVersion != 1 {
		t.Fatalf("E3: sano sin listener ni hub: %+v", r)
	}
}

func TestDatabaseDownIs503(t *testing.T) {
	r, ok := NewHealthService(fakeDB{err: errors.New("sin conexión")}, nil, nil).Check(context.Background())
	if ok || r.DB != "down" || r.Status != "degraded" {
		t.Fatalf("la base caída debe dar 503: %+v", r)
	}
}

func TestListenerAndLag(t *testing.T) {
	r, ok := NewHealthService(fakeDB{}, fakeListener{healthy: true, last: 90, visible: 100}, fakeHub{}).Check(context.Background())
	if !ok || *r.LagSeq != 10 || r.Connections != 42 || r.QueueOverflows != 1 {
		t.Fatalf("listener sano con retraso 10: %+v", r)
	}
	if _, ok := NewHealthService(fakeDB{}, fakeListener{healthy: false}, nil).Check(context.Background()); ok {
		t.Fatal("el listener caído debe dar 503")
	}
	if _, ok := NewHealthService(fakeDB{}, fakeListener{healthy: true, lagBad: true}, nil).Check(context.Background()); ok {
		t.Fatal("el retraso alto sostenido debe dar 503")
	}
}
