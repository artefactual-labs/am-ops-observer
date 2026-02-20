package http

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	appStartedAtUnix = time.Now().Unix()
	inFlightRequests int64
	metricsMu        sync.Mutex
	httpSeries       = map[httpMetricKey]*httpMetricSeries{}
	dbQuerySeries    = map[dbMetricKey]*dbMetricSeries{}
	externalSeries   = map[externalMetricKey]*externalMetricSeries{}
	reportRunSeries  = map[reportRunMetricKey]*reportRunMetricSeries{}
)

func metricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		metricsMu.Lock()
		keys := make([]httpMetricKey, 0, len(httpSeries))
		for k := range httpSeries {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].Method != keys[j].Method {
				return keys[i].Method < keys[j].Method
			}
			if keys[i].Path != keys[j].Path {
				return keys[i].Path < keys[j].Path
			}
			return keys[i].Status < keys[j].Status
		})
		snapshot := make([]struct {
			Key    httpMetricKey
			Series httpMetricSeries
		}, 0, len(keys))
		for _, k := range keys {
			s := httpSeries[k]
			snapshot = append(snapshot, struct {
				Key    httpMetricKey
				Series httpMetricSeries
			}{Key: k, Series: *s})
		}
		metricsMu.Unlock()

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_http_requests_total Total HTTP requests handled by this app.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_http_requests_total counter")
		for _, it := range snapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_http_requests_total{method=%q,path=%q,status=%q} %d\n",
				escapeLabel(it.Key.Method), escapeLabel(it.Key.Path), escapeLabel(it.Key.Status), it.Series.Count)
		}

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_http_request_duration_seconds_sum Total duration in seconds for observed requests.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_http_request_duration_seconds_sum counter")
		for _, it := range snapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_http_request_duration_seconds_sum{method=%q,path=%q,status=%q} %.9f\n",
				escapeLabel(it.Key.Method), escapeLabel(it.Key.Path), escapeLabel(it.Key.Status), it.Series.DurationSecondsSum)
		}

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_http_request_duration_seconds_count Number of observed requests in duration series.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_http_request_duration_seconds_count counter")
		for _, it := range snapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_http_request_duration_seconds_count{method=%q,path=%q,status=%q} %d\n",
				escapeLabel(it.Key.Method), escapeLabel(it.Key.Path), escapeLabel(it.Key.Status), it.Series.Count)
		}

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_http_in_flight_requests In-flight HTTP requests currently served by this app.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_http_in_flight_requests gauge")
		_, _ = fmt.Fprintf(w, "am_report_ui_http_in_flight_requests %d\n", atomic.LoadInt64(&inFlightRequests))

		metricsMu.Lock()
		dbKeys := make([]dbMetricKey, 0, len(dbQuerySeries))
		for k := range dbQuerySeries {
			dbKeys = append(dbKeys, k)
		}
		sort.Slice(dbKeys, func(i, j int) bool {
			if dbKeys[i].Connector != dbKeys[j].Connector {
				return dbKeys[i].Connector < dbKeys[j].Connector
			}
			return dbKeys[i].Operation < dbKeys[j].Operation
		})
		dbSnapshot := make([]struct {
			Key    dbMetricKey
			Series dbMetricSeries
		}, 0, len(dbKeys))
		for _, k := range dbKeys {
			dbSnapshot = append(dbSnapshot, struct {
				Key    dbMetricKey
				Series dbMetricSeries
			}{k, *dbQuerySeries[k]})
		}

		exKeys := make([]externalMetricKey, 0, len(externalSeries))
		for k := range externalSeries {
			exKeys = append(exKeys, k)
		}
		sort.Slice(exKeys, func(i, j int) bool {
			if exKeys[i].Target != exKeys[j].Target {
				return exKeys[i].Target < exKeys[j].Target
			}
			return exKeys[i].Operation < exKeys[j].Operation
		})
		exSnapshot := make([]struct {
			Key    externalMetricKey
			Series externalMetricSeries
		}, 0, len(exKeys))
		for _, k := range exKeys {
			exSnapshot = append(exSnapshot, struct {
				Key    externalMetricKey
				Series externalMetricSeries
			}{k, *externalSeries[k]})
		}

		reportKeys := make([]reportRunMetricKey, 0, len(reportRunSeries))
		for k := range reportRunSeries {
			reportKeys = append(reportKeys, k)
		}
		sort.Slice(reportKeys, func(i, j int) bool {
			return reportKeys[i].Status < reportKeys[j].Status
		})
		reportSnapshot := make([]struct {
			Key    reportRunMetricKey
			Series reportRunMetricSeries
		}, 0, len(reportKeys))
		for _, k := range reportKeys {
			reportSnapshot = append(reportSnapshot, struct {
				Key    reportRunMetricKey
				Series reportRunMetricSeries
			}{k, *reportRunSeries[k]})
		}
		metricsMu.Unlock()

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_db_query_duration_seconds_sum Database query duration sum in seconds by connector/operation.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_db_query_duration_seconds_sum counter")
		for _, it := range dbSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_db_query_duration_seconds_sum{connector=%q,operation=%q} %.9f\n",
				escapeLabel(it.Key.Connector), escapeLabel(it.Key.Operation), it.Series.DurationSecondsSum)
		}
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_db_query_duration_seconds_count Database query observation count by connector/operation.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_db_query_duration_seconds_count counter")
		for _, it := range dbSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_db_query_duration_seconds_count{connector=%q,operation=%q} %d\n",
				escapeLabel(it.Key.Connector), escapeLabel(it.Key.Operation), it.Series.Count)
		}
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_db_query_errors_total Database query errors by connector/operation.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_db_query_errors_total counter")
		for _, it := range dbSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_db_query_errors_total{connector=%q,operation=%q} %d\n",
				escapeLabel(it.Key.Connector), escapeLabel(it.Key.Operation), it.Series.Errors)
		}

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_external_probe_duration_seconds_sum External probe duration sum in seconds by target/operation.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_external_probe_duration_seconds_sum counter")
		for _, it := range exSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_external_probe_duration_seconds_sum{target=%q,operation=%q} %.9f\n",
				escapeLabel(it.Key.Target), escapeLabel(it.Key.Operation), it.Series.DurationSecondsSum)
		}
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_external_probe_duration_seconds_count External probe observation count by target/operation.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_external_probe_duration_seconds_count counter")
		for _, it := range exSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_external_probe_duration_seconds_count{target=%q,operation=%q} %d\n",
				escapeLabel(it.Key.Target), escapeLabel(it.Key.Operation), it.Series.Count)
		}
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_external_probe_errors_total External probe errors by target/operation.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_external_probe_errors_total counter")
		for _, it := range exSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_external_probe_errors_total{target=%q,operation=%q} %d\n",
				escapeLabel(it.Key.Target), escapeLabel(it.Key.Operation), it.Series.Errors)
		}

		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_report_runs_total Report run count by status.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_report_runs_total counter")
		for _, it := range reportSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_report_runs_total{status=%q} %d\n", escapeLabel(it.Key.Status), it.Series.Count)
		}
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_report_run_duration_seconds_sum Report run duration sum in seconds by status.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_report_run_duration_seconds_sum counter")
		for _, it := range reportSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_report_run_duration_seconds_sum{status=%q} %.9f\n", escapeLabel(it.Key.Status), it.Series.DurationSecondsSum)
		}
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_report_run_duration_seconds_count Report run duration observation count by status.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_report_run_duration_seconds_count counter")
		for _, it := range reportSnapshot {
			_, _ = fmt.Fprintf(w, "am_report_ui_report_run_duration_seconds_count{status=%q} %d\n", escapeLabel(it.Key.Status), it.Series.Count)
		}

		uptime := time.Now().Unix() - appStartedAtUnix
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_uptime_seconds Process uptime in seconds.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_uptime_seconds gauge")
		_, _ = fmt.Fprintf(w, "am_report_ui_uptime_seconds %d\n", uptime)

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_goroutines Number of goroutines.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_goroutines gauge")
		_, _ = fmt.Fprintf(w, "am_report_ui_runtime_goroutines %d\n", runtime.NumGoroutine())
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_memory_alloc_bytes Heap allocation bytes.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_memory_alloc_bytes gauge")
		_, _ = fmt.Fprintf(w, "am_report_ui_runtime_memory_alloc_bytes %d\n", ms.Alloc)
		_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_gc_total Total GC runs since process start.")
		_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_gc_total counter")
		_, _ = fmt.Fprintf(w, "am_report_ui_runtime_gc_total %d\n", ms.NumGC)

		if cpuSec, ok := processCPUSeconds(); ok {
			_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_cpu_seconds_total Total CPU time consumed by this process in seconds.")
			_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_cpu_seconds_total counter")
			_, _ = fmt.Fprintf(w, "am_report_ui_runtime_cpu_seconds_total %.6f\n", cpuSec)
			if uptime > 0 {
				cpuPct := (cpuSec / float64(uptime)) * 100.0
				_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_cpu_percent Average CPU percent of one core since process start.")
				_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_cpu_percent gauge")
				_, _ = fmt.Fprintf(w, "am_report_ui_runtime_cpu_percent %.6f\n", cpuPct)
			}
		}
		if io := processIOStats(); io != nil {
			_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_io_read_bytes_total Bytes read by this process from storage.")
			_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_io_read_bytes_total counter")
			_, _ = fmt.Fprintf(w, "am_report_ui_runtime_io_read_bytes_total %d\n", io.ReadBytes)
			_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_io_write_bytes_total Bytes written by this process to storage.")
			_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_io_write_bytes_total counter")
			_, _ = fmt.Fprintf(w, "am_report_ui_runtime_io_write_bytes_total %d\n", io.WriteBytes)
			_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_io_read_syscalls_total Read syscalls issued by this process.")
			_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_io_read_syscalls_total counter")
			_, _ = fmt.Fprintf(w, "am_report_ui_runtime_io_read_syscalls_total %d\n", io.SysReadCalls)
			_, _ = fmt.Fprintln(w, "# HELP am_report_ui_runtime_io_write_syscalls_total Write syscalls issued by this process.")
			_, _ = fmt.Fprintln(w, "# TYPE am_report_ui_runtime_io_write_syscalls_total counter")
			_, _ = fmt.Fprintf(w, "am_report_ui_runtime_io_write_syscalls_total %d\n", io.SysWriteCalls)
		}
	})
}

func appMetricsSummaryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		type endpointRow struct {
			Method   string  `json:"method"`
			Path     string  `json:"path"`
			Status   string  `json:"status"`
			Count    uint64  `json:"count"`
			AvgMS    float64 `json:"avg_ms"`
			TotalMS  float64 `json:"total_ms"`
		}
		type dbRow struct {
			Connector string  `json:"connector"`
			Operation string  `json:"operation"`
			Count     uint64  `json:"count"`
			Errors    uint64  `json:"errors"`
			AvgMS     float64 `json:"avg_ms"`
		}

		metricsMu.Lock()
		httpRows := make([]endpointRow, 0, len(httpSeries))
		for k, s := range httpSeries {
			avg := 0.0
			if s.Count > 0 {
				avg = (s.DurationSecondsSum / float64(s.Count)) * 1000.0
			}
			httpRows = append(httpRows, endpointRow{
				Method:  k.Method,
				Path:    k.Path,
				Status:  k.Status,
				Count:   s.Count,
				AvgMS:   avg,
				TotalMS: s.DurationSecondsSum * 1000.0,
			})
		}

		dbRows := make([]dbRow, 0, len(dbQuerySeries))
		totalDBErrors := uint64(0)
		for k, s := range dbQuerySeries {
			avg := 0.0
			if s.Count > 0 {
				avg = (s.DurationSecondsSum / float64(s.Count)) * 1000.0
			}
			dbRows = append(dbRows, dbRow{
				Connector: k.Connector,
				Operation: k.Operation,
				Count:     s.Count,
				Errors:    s.Errors,
				AvgMS:     avg,
			})
			totalDBErrors += s.Errors
		}

		externalErrors := uint64(0)
		for _, s := range externalSeries {
			externalErrors += s.Errors
		}
		metricsMu.Unlock()

		sort.Slice(httpRows, func(i, j int) bool { return httpRows[i].AvgMS > httpRows[j].AvgMS })
		sort.Slice(dbRows, func(i, j int) bool { return dbRows[i].AvgMS > dbRows[j].AvgMS })

		topHTTP := httpRows
		if len(topHTTP) > 5 {
			topHTTP = topHTTP[:5]
		}
		topDB := dbRows
		if len(topDB) > 5 {
			topDB = topDB[:5]
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"meta": map[string]any{
				"generated_at": time.Now().UTC(),
			},
			"data": map[string]any{
				"top_http_slowest_avg_ms": topHTTP,
				"top_db_slowest_avg_ms":   topDB,
				"errors": map[string]any{
					"db_query_total":      totalDBErrors,
					"external_probe_total": externalErrors,
				},
			},
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func observabilityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		atomic.AddInt64(&inFlightRequests, 1)
		defer atomic.AddInt64(&inFlightRequests, -1)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := normalizeMetricPath(r.URL.Path)
		sec := time.Since(start).Seconds()
		recordHTTPMetric(r.Method, route, rec.status, sec)
	})
}

func normalizeMetricPath(path string) string {
	switch {
	case path == "/":
		return "/"
	case path == "/metrics":
		return "/metrics"
	case strings.HasPrefix(path, "/api/v1/transfers/") && strings.HasSuffix(path, "/summary"):
		return "/api/v1/transfers/{uuid}/summary"
	case strings.HasPrefix(path, "/api/v1/transfers/") && strings.HasSuffix(path, "/details"):
		return "/api/v1/transfers/{uuid}/details"
	case strings.HasPrefix(path, "/api/v1/transfers/") && strings.HasSuffix(path, "/timeline"):
		return "/api/v1/transfers/{uuid}/timeline"
	case strings.HasPrefix(path, "/api/v1/transfers/") && strings.HasSuffix(path, "/errors"):
		return "/api/v1/transfers/{uuid}/errors"
	case strings.HasPrefix(path, "/api/v1/aips/") && strings.HasSuffix(path, "/stats"):
		return "/api/v1/aips/{aip_uuid}/stats"
	case strings.HasPrefix(path, "/api/v1/aips/") && strings.HasSuffix(path, "/storage-service"):
		return "/api/v1/aips/{aip_uuid}/storage-service"
	case strings.HasPrefix(path, "/api/v1/reports/templates/"):
		return "/api/v1/reports/templates/{id}"
	default:
		return path
	}
}

type httpMetricKey struct {
	Method string
	Path   string
	Status string
}

type httpMetricSeries struct {
	Count              uint64
	DurationSecondsSum float64
}

type dbMetricKey struct {
	Connector string
	Operation string
}

type dbMetricSeries struct {
	Count              uint64
	Errors             uint64
	DurationSecondsSum float64
}

type externalMetricKey struct {
	Target    string
	Operation string
}

type externalMetricSeries struct {
	Count              uint64
	Errors             uint64
	DurationSecondsSum float64
}

type reportRunMetricKey struct {
	Status string
}

type reportRunMetricSeries struct {
	Count              uint64
	DurationSecondsSum float64
}

func recordHTTPMetric(method, path string, status int, durationSeconds float64) {
	key := httpMetricKey{
		Method: method,
		Path:   path,
		Status: fmt.Sprintf("%d", status),
	}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	row, ok := httpSeries[key]
	if !ok {
		row = &httpMetricSeries{}
		httpSeries[key] = row
	}
	row.Count++
	row.DurationSecondsSum += durationSeconds
}

func recordDBQuery(connector, operation string, durationSeconds float64, err error) {
	if connector == "" || operation == "" {
		return
	}
	key := dbMetricKey{Connector: connector, Operation: operation}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	row, ok := dbQuerySeries[key]
	if !ok {
		row = &dbMetricSeries{}
		dbQuerySeries[key] = row
	}
	row.Count++
	row.DurationSecondsSum += durationSeconds
	if err != nil {
		row.Errors++
	}
}

func recordExternalProbe(target, operation string, durationSeconds float64, err error) {
	if target == "" || operation == "" {
		return
	}
	key := externalMetricKey{Target: target, Operation: operation}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	row, ok := externalSeries[key]
	if !ok {
		row = &externalMetricSeries{}
		externalSeries[key] = row
	}
	row.Count++
	row.DurationSecondsSum += durationSeconds
	if err != nil {
		row.Errors++
	}
}

func recordReportRun(status string, durationSeconds float64) {
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "unknown"
	}
	key := reportRunMetricKey{Status: status}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	row, ok := reportRunSeries[key]
	if !ok {
		row = &reportRunMetricSeries{}
		reportRunSeries[key] = row
	}
	row.Count++
	row.DurationSecondsSum += durationSeconds
}

func escapeLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return v
}

func processCPUSeconds() (float64, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	user := float64(ru.Utime.Sec) + (float64(ru.Utime.Usec) / 1_000_000.0)
	sys := float64(ru.Stime.Sec) + (float64(ru.Stime.Usec) / 1_000_000.0)
	return user + sys, true
}

type ioStats struct {
	ReadBytes     uint64
	WriteBytes    uint64
	SysReadCalls  uint64
	SysWriteCalls uint64
}

func processIOStats() *ioStats {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return nil
	}
	out := &ioStats{}
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		valRaw := strings.TrimSpace(parts[1])
		v, err := strconv.ParseUint(valRaw, 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "read_bytes":
			out.ReadBytes = v
		case "write_bytes":
			out.WriteBytes = v
		case "syscr":
			out.SysReadCalls = v
		case "syscw":
			out.SysWriteCalls = v
		}
	}
	return out
}
