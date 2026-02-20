package http

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"strconv"
	"time"

	"go-am-realtime-report-ui/internal/config"
	esstore "go-am-realtime-report-ui/internal/connectors/es"
	mysqlstore "go-am-realtime-report-ui/internal/connectors/mysql"
	promstore "go-am-realtime-report-ui/internal/connectors/prometheus"
	ssstore "go-am-realtime-report-ui/internal/connectors/ssdb"
)

// Server wraps an HTTP server and route handlers.
type Server struct {
	httpServer *nethttp.Server
	mysqlStore *mysqlstore.Store
	ssStore    *ssstore.Store
	esStore    *esstore.Client
	promStore  *promstore.Scraper
	promConfig struct {
		matchPrefix string
		interval    time.Duration
	}
	promCancel context.CancelFunc
}

// NewServer creates a configured HTTP server with v1 endpoints.
func NewServer(cfg config.Config) (*Server, error) {
	var store *mysqlstore.Store
	if cfg.DBEnabled {
		createdStore, err := mysqlstore.NewStore(cfg)
		if err != nil {
			return nil, err
		}
		store = createdStore
	}
	var storageStore *ssstore.Store
	if cfg.SSDBEnabled {
		createdStore, err := ssstore.NewStore(cfg)
		if err != nil {
			return nil, err
		}
		storageStore = createdStore
	}
	var promScraper *promstore.Scraper
	if cfg.PromEnabled {
		promScraper = promstore.NewScraper(cfg.PromTargets, cfg.PromScrapeTimeout, cfg.PromHistoryMaxPoints)
	}
	var esClient *esstore.Client
	if cfg.ESEnabled {
		esClient = esstore.NewClient(cfg.ESEndpoint, cfg.ESTimeout)
	}

	mux := nethttp.NewServeMux()

	mux.HandleFunc("/", dashboardHandler)
	mux.HandleFunc("/favicon.ico", faviconHandler)
	mux.Handle("/metrics", metricsHandler())
	mux.HandleFunc("/api/v1/metrics/app", appMetricsSummaryHandler())
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", readyHandler)
	mux.HandleFunc("/api/v1/transfers/running", runningTransfersHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/sips/running", runningSIPsHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/transfers/completed", completedTransfersHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/transfers/", transferDetailRouter(cfg.DefaultRunningLimit, store, storageStore, esClient, cfg.ESLookupLimit))
	mux.HandleFunc("/api/v1/troubleshooting/stalled", stalledTransfersHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/troubleshooting/hotspots", errorHotspotsHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/troubleshooting/failure-counts", failureCountsHandler(store))
	mux.HandleFunc("/api/v1/transfers/failed", failedTransfersHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/troubleshooting/failure-signatures", failureSignaturesHandler(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/monthly", monthlyReportHandler(cfg.DefaultCustomerReport, store, storageStore))
	mux.HandleFunc("/api/v1/reports/customers", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/query", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/query/options", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/templates", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/templates/", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/customer-mappings", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/reports/customer-mappings/", reportRoutesRouter(cfg.DefaultRunningLimit, store))
	mux.HandleFunc("/api/v1/charts/transfer-durations", transferDurationChartHandler(cfg.DefaultCustomerReport, store))
	mux.HandleFunc("/api/v1/metrics/prometheus/live", promLiveMetricsHandler(promScraper, cfg.PromMatchPrefix))
	mux.HandleFunc("/api/v1/charts/prometheus", promChartHandler(promScraper, cfg.PromMatchPrefix))
	mux.HandleFunc("/api/v1/status/services", servicesStatusHandler(store, storageStore, esClient, promScraper))
	mux.HandleFunc("/api/v1/status/customer-mapping", customerMappingStatusHandler(store))
	mux.HandleFunc("/api/v1/settings/risk-thresholds", riskThresholdsHandler(cfg))
	mux.HandleFunc("/api/v1/aips", aipListHandler(cfg.DefaultRunningLimit, cfg.ESAIPIndex, esClient))
	mux.HandleFunc("/api/v1/aips/", aipDetailRouter(cfg.ESAIPPageSize, cfg.ESAIPIndex, esClient, storageStore))

	httpServer := &nethttp.Server{
		Addr:         cfg.ListenAddr,
		Handler:      loggingMiddleware(observabilityMiddleware(mux)),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	s := &Server{httpServer: httpServer, mysqlStore: store, ssStore: storageStore, esStore: esClient, promStore: promScraper}
	s.promConfig.matchPrefix = cfg.PromMatchPrefix
	s.promConfig.interval = cfg.PromScrapeInterval
	return s, nil
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	if s.promStore != nil && s.promStore.Enabled() {
		ctx, cancel := context.WithCancel(context.Background())
		s.promCancel = cancel
		go s.startPrometheusPoller(ctx)
	}
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.promCancel != nil {
		s.promCancel()
	}
	if s.mysqlStore != nil {
		_ = s.mysqlStore.Close()
	}
	if s.ssStore != nil {
		_ = s.ssStore.Close()
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) startPrometheusPoller(ctx context.Context) {
	interval := s.promConfig.interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	_, _ = s.promStore.Scrape(ctx, s.promConfig.matchPrefix)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.promStore.Scrape(ctx, s.promConfig.matchPrefix)
		}
	}
}

func healthHandler(w nethttp.ResponseWriter, _ *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().UTC(),
	})
}

func readyHandler(w nethttp.ResponseWriter, _ *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, map[string]any{
		"status": "ready",
	})
}

func loggingMiddleware(next nethttp.Handler) nethttp.Handler {
	return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: nethttp.StatusOK}
		next.ServeHTTP(rec, r)
		fmt.Printf("%s %s %s %s\n", r.Method, r.URL.Path, strconv.Itoa(rec.status), time.Since(start))
	})
}

func writeJSON(w nethttp.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
