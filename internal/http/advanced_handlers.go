package http

import (
	"errors"
	nethttp "net/http"
	"strconv"
	"strings"
	"time"

	mysqlstore "go-am-realtime-report-ui/internal/connectors/mysql"
	promstore "go-am-realtime-report-ui/internal/connectors/prometheus"
)

func parseFailureWindow(raw string, defaultHours, maxHours int) (since time.Time, hours int) {
	hours = defaultHours
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "all" || raw == "forever" {
		return time.Time{}, 0
	}
	if raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 && v <= maxHours {
			hours = v
		}
	}
	return time.Now().UTC().Add(-time.Duration(hours) * time.Hour), hours
}

func stalledTransfersHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		limit := parseLimit(r, defaultLimit)
		start := time.Now()
		items, err := store.ListStalledTransfers(r.Context(), limit)
		recordDBQuery("mcp", "ListStalledTransfers", time.Since(start).Seconds(), err)
		if err != nil {
			status := nethttp.StatusInternalServerError
			if errors.Is(err, nethttp.ErrHandlerTimeout) {
				status = nethttp.StatusGatewayTimeout
			}
			writeJSON(w, status, map[string]any{"error": "failed to fetch stalled transfers"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{"limit": limit, "count": len(items)},
			"data": items,
		})
	}
}

func errorHotspotsHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		limit := parseLimit(r, defaultLimit)
		since, hours := parseFailureWindow(r.URL.Query().Get("hours"), 24, 24*365*20)
		unit := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("unit")))
		if unit == "" {
			unit = "transfer"
			}
			if unit != "transfer" && unit != "sip" && unit != "all" {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "invalid unit, use transfer|sip|all"})
				return
			}

			start := time.Now()
			items, err := store.ListErrorHotspots(r.Context(), since, limit, unit)
			recordDBQuery("mcp", "ListErrorHotspots", time.Since(start).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch error hotspots"})
				return
			}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{"hours": hours, "limit": limit, "unit": unit, "count": len(items)},
			"data": items,
		})
	}
}

func failureCountsHandler(store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		since, hours := parseFailureWindow(r.URL.Query().Get("hours"), 24, 24*365*20)
		startTransfer := time.Now()
		transferCounts, err := store.CountFailureCounts(r.Context(), since, "transfer")
		recordDBQuery("mcp", "CountFailureCounts.transfer", time.Since(startTransfer).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer failure counts"})
			return
		}
		startSIP := time.Now()
		sipCounts, err := store.CountFailureCounts(r.Context(), since, "sip")
		recordDBQuery("mcp", "CountFailureCounts.sip", time.Since(startSIP).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch sip failure counts"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"hours": hours,
			},
			"data": map[string]any{
				"transfer": transferCounts,
				"sip":      sipCounts,
				"total": map[string]any{
					"failed_tasks": transferCounts.FailedTasks + sipCounts.FailedTasks,
					"failed_units": transferCounts.FailedUnits + sipCounts.FailedUnits,
				},
			},
		})
	}
}

func failedTransfersHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		limit := parseLimit(r, defaultLimit)
		offset := parseOffset(r)
		since, hours := parseFailureWindow(r.URL.Query().Get("hours"), 24*30, 24*365*20)
		start := time.Now()
		items, err := store.ListFailedTransfers(r.Context(), since, limit, offset)
		recordDBQuery("mcp", "ListFailedTransfers", time.Since(start).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch failed transfers"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{"hours": hours, "limit": limit, "offset": offset, "count": len(items)},
			"data": items,
		})
	}
}

func failureSignaturesHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		limit := parseLimit(r, defaultLimit)
		since, hours := parseFailureWindow(r.URL.Query().Get("hours"), 24*30, 24*365*20)
		start := time.Now()
		items, err := store.ListFailureSignatures(r.Context(), since, limit)
		recordDBQuery("mcp", "ListFailureSignatures", time.Since(start).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch failure signatures"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{"hours": hours, "limit": limit, "count": len(items)},
			"data": items,
		})
	}
}

func transferDurationChartHandler(defaultCustomerID string, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		customerID := strings.TrimSpace(r.URL.Query().Get("customer_id"))
		if customerID == "" {
			customerID = defaultCustomerID
		}

		monthStr := strings.TrimSpace(r.URL.Query().Get("month"))
		if monthStr == "" {
			monthStr = time.Now().UTC().Format("2006-01")
		}
		month, err := time.Parse("2006-01", monthStr)
		if err != nil {
			writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "invalid month format, expected YYYY-MM"})
			return
		}

		start := time.Now()
		series, err := store.GetTransferDurationChart(r.Context(), customerID, month)
		recordDBQuery("mcp", "GetTransferDurationChart", time.Since(start).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to build transfer duration chart"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"customer_id": customerID,
				"month":       month.Format("2006-01"),
				"count":       len(series),
			},
			"data": series,
		})
	}
}

func promLiveMetricsHandler(scraper *promstore.Scraper, defaultPrefix string) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if scraper == nil || !scraper.Enabled() {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "prometheus integration disabled (set APP_PROM_ENABLED=true)"})
			return
		}

		prefix := strings.TrimSpace(r.URL.Query().Get("match"))
		if prefix == "" {
			prefix = defaultPrefix
		}

		start := time.Now()
		snaps, err := scraper.Scrape(r.Context(), prefix)
		recordExternalProbe("prometheus_target", "Scrape", time.Since(start).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusBadGateway, map[string]any{"error": "failed to scrape prometheus target(s)"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{"match": prefix, "targets": len(snaps)},
			"data": snaps,
		})
	}
}

func promChartHandler(scraper *promstore.Scraper, defaultPrefix string) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if scraper == nil || !scraper.Enabled() {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "prometheus integration disabled (set APP_PROM_ENABLED=true)"})
			return
		}

		targets := scraper.Targets()
		if len(targets) == 0 {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "no prometheus targets configured"})
			return
		}

		target := strings.TrimSpace(r.URL.Query().Get("target"))
		if target == "" {
			target = targets[0]
		}

		metric := strings.TrimSpace(r.URL.Query().Get("metric"))
		minutes := 60
		if raw := strings.TrimSpace(r.URL.Query().Get("minutes")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 0 && v <= 24*7 {
				minutes = v
			}
		}

		start := time.Now()
		_, scrapeErr := scraper.Scrape(r.Context(), defaultPrefix)
		recordExternalProbe("prometheus_target", "Scrape", time.Since(start).Seconds(), scrapeErr)

		if metric == "" {
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{"target": target, "minutes": minutes},
				"data": map[string]any{
					"targets":       targets,
					"known_metrics": scraper.KnownMetrics(target),
				},
			})
			return
		}

		since := time.Now().UTC().Add(-time.Duration(minutes) * time.Minute)
		points := scraper.Series(target, metric, since)
		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"target":  target,
				"metric":  metric,
				"minutes": minutes,
				"count":   len(points),
			},
			"data": points,
		})
	}
}
