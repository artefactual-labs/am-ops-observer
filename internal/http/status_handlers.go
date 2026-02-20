package http

import (
	"context"
	nethttp "net/http"
	"time"

	esstore "go-am-realtime-report-ui/internal/connectors/es"
	mysqlstore "go-am-realtime-report-ui/internal/connectors/mysql"
	promstore "go-am-realtime-report-ui/internal/connectors/prometheus"
	ssstore "go-am-realtime-report-ui/internal/connectors/ssdb"
)

func servicesStatusHandler(store *mysqlstore.Store, ssStore *ssstore.Store, esClient *esstore.Client, scraper *promstore.Scraper) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()

		payload := map[string]any{
			"generated_at": time.Now().UTC(),
			"services":     map[string]any{},
		}
		services := payload["services"].(map[string]any)

		services["mysql"] = mysqlStatus(ctx, store)
		services["storage_service_db"] = ssDBStatus(ctx, ssStore)
		services["elasticsearch"] = esStatus(ctx, esClient)
		services["prometheus"] = promStatus(ctx, scraper)

		writeJSON(w, nethttp.StatusOK, payload)
	}
}

func ssDBStatus(ctx context.Context, store *ssstore.Store) map[string]any {
	if store == nil {
		return map[string]any{"enabled": false, "ok": false, "error": "storage service db integration disabled"}
	}

	start := time.Now()
	stats, err := store.ServiceStats(ctx)
	recordDBQuery("ssdb", "ServiceStats", time.Since(start).Seconds(), err)
	if err != nil {
		return map[string]any{"enabled": true, "ok": false, "error": err.Error()}
	}
	return map[string]any{"enabled": true, "ok": true, "stats": stats}
}

func customerMappingStatusHandler(store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"enabled": false,
				"error":   "database integration disabled",
			})
			return
		}

		mode := store.CustomerMappingMode()
		payload := map[string]any{
			"enabled":               store.HasCustomerSourceMapping(),
			"mode":                  mode,
			"sqlite_path":           "",
			"supports_mapping_crud": store.HasCustomerSourceMapping(),
		}
		if mode == "sqlite_customer_mappings" {
			payload["sqlite_path"] = store.CustomerMappingPath()
		}

		writeJSON(w, nethttp.StatusOK, payload)
	}
}

func mysqlStatus(ctx context.Context, store *mysqlstore.Store) map[string]any {
	if store == nil {
		return map[string]any{"enabled": false, "ok": false, "error": "database integration disabled"}
	}

	start := time.Now()
	stats, err := store.ServiceStats(ctx)
	recordDBQuery("mcp", "ServiceStats", time.Since(start).Seconds(), err)
	if err != nil {
		return map[string]any{"enabled": true, "ok": false, "error": err.Error()}
	}

	return map[string]any{"enabled": true, "ok": true, "stats": stats}
}

func esStatus(ctx context.Context, esClient *esstore.Client) map[string]any {
	if esClient == nil || !esClient.Enabled() {
		return map[string]any{"enabled": false, "ok": false, "error": "elasticsearch integration disabled"}
	}

	start := time.Now()
	stats, err := esClient.ServiceStats(ctx)
	recordExternalProbe("elasticsearch", "ServiceStats", time.Since(start).Seconds(), err)
	if err != nil {
		return map[string]any{"enabled": true, "ok": false, "error": err.Error()}
	}

	return map[string]any{"enabled": true, "ok": true, "stats": stats}
}

func promStatus(ctx context.Context, scraper *promstore.Scraper) map[string]any {
	if scraper == nil || !scraper.Enabled() {
		return map[string]any{"enabled": false, "ok": false, "error": "prometheus integration disabled"}
	}

	start := time.Now()
	probes := scraper.ProbeTargets(ctx, []string{
		"process_start_time_seconds",
		"go_goroutines",
		"process_resident_memory_bytes",
		"process_cpu_seconds_total",
	})
	recordExternalProbe("prometheus_target", "ProbeTargets", time.Since(start).Seconds(), nil)

	up := 0
	for _, p := range probes {
		if p.OK {
			up++
		}
	}

	return map[string]any{
		"enabled":       true,
		"ok":            up == len(probes) && len(probes) > 0,
		"targets_total": len(probes),
		"targets_up":    up,
		"targets":       probes,
	}
}
