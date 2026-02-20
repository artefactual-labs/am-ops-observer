package main

import (
	"log"

	"go-am-realtime-report-ui/internal/config"
	httpapi "go-am-realtime-report-ui/internal/http"
)

var version = "dev"

func main() {
	cfg := config.FromEnv()
	srv, err := httpapi.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}

	log.Printf("starting API server version=%s on %s", version, cfg.ListenAddr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
