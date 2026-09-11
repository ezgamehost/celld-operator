//go:build e2e

// This deterministic runtime fixture tests the operator's Kubernetes lifecycle.
// It intentionally does not emulate celld's storage or replication protocol.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	path := os.Getenv("CELLD_WATCH") + "/boots"
	previous, _ := os.ReadFile(path)
	boots, _ := strconv.Atoi(strings.TrimSpace(string(previous)))
	if err := os.WriteFile(path, fmt.Appendf(nil, "%d", boots+1), 0600); err != nil {
		log.Fatal(err)
	}
	log.Printf("Boot %d", boots+1)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	state := http.NewServeMux()
	state.HandleFunc("/state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"occupied":1,"restoring":0,"shedding":null,"deployment":{"version":"deploy-a","generation":1,"draining":[],"swapping":0}}`))
	})
	public := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: time.Second}
	internal := &http.Server{Addr: ":8081", Handler: state, ReadHeaderTimeout: time.Second}
	go func() { _ = public.ListenAndServe() }()
	go func() { _ = internal.ListenAndServe() }()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	<-ctx.Done()
	stop, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	_ = public.Shutdown(stop)
	_ = internal.Shutdown(stop)
}
