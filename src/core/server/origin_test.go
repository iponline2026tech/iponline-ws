package server

// origin_test · solo los orígenes configurados, con coincidencia exacta de esquema, host y puerto

import "testing"

func TestOriginsMatchExactly(t *testing.T) {
	o := NewOrigins([]string{"http://localhost:5173", "https://app.importadora.com"})
	cases := map[string]bool{
		"http://localhost:5173":            true,
		"HTTP://LOCALHOST:5173":            true,
		"http://localhost:5174":            false, // otro puerto
		"https://localhost:5173":           false, // otro esquema
		"https://app.importadora.com":      true,
		"https://app.importadora.com:443":  true, // puerto por defecto explícito
		"https://evil.importadora.com":     false,
		"https://app.importadora.com.evil": false,
		"":                                 false, // sin Origin
		"null":                             false,
	}
	for origin, want := range cases {
		if got := o.Allow(origin); got != want {
			t.Errorf("Allow(%q) = %v, esperaba %v", origin, got, want)
		}
	}
}

func TestStarAllowsAnyBrowserOrigin(t *testing.T) {
	o := NewOrigins([]string{"*"})
	if !o.Allow("http://localhost:9999") || o.Allow("") {
		t.Fatal("«*» deja pasar cualquier navegador, pero no una petición sin Origin")
	}
}
