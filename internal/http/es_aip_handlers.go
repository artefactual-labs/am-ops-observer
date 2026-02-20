package http

import (
	"fmt"
	nethttp "net/http"
	"strings"
	"time"

	esstore "go-am-realtime-report-ui/internal/connectors/es"
	ssstore "go-am-realtime-report-ui/internal/connectors/ssdb"
)

func aipListHandler(defaultLimit int, index string, esClient *esstore.Client) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if esClient == nil || !esClient.Enabled() {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "elasticsearch integration disabled (set APP_ES_ENABLED=true)"})
			return
		}

		limit := parseLimit(r, defaultLimit)
		cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))

		start := time.Now()
		res, err := esClient.ListAIPs(r.Context(), index, limit, cursor)
		recordExternalProbe("elasticsearch", "ListAIPs", time.Since(start).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusBadGateway, map[string]any{
				"error":  "failed to list AIPs from Elasticsearch",
				"detail": err.Error(),
			})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"index":       index,
				"limit":       limit,
				"count":       len(res.Items),
				"next_cursor": res.NextCursor,
			},
			"data": res.Items,
		})
	}
}

func aipDetailRouter(defaultPageSize int, index string, esClient *esstore.Client, ssStore *ssstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		trimmed := strings.TrimPrefix(r.URL.Path, "/api/v1/aips/")
		parts := strings.Split(strings.Trim(trimmed, "/"), "/")
		if len(parts) != 2 || parts[0] == "" {
			writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": "not found"})
			return
		}

		aipUUID := strings.TrimSpace(parts[0])
		switch parts[1] {
		case "stats":
			if esClient == nil || !esClient.Enabled() {
				writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "elasticsearch integration disabled (set APP_ES_ENABLED=true)"})
				return
			}
			pageSize := parseLimit(r, defaultPageSize)
			start := time.Now()
			stats, err := esClient.AIPStats(r.Context(), index, aipUUID, pageSize)
			recordExternalProbe("elasticsearch", "AIPStats", time.Since(start).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusBadGateway, map[string]any{
					"error":  fmt.Sprintf("failed to fetch AIP stats for %s", aipUUID),
					"detail": err.Error(),
				})
				return
			}

			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{
					"index":    index,
					"aip_uuid": aipUUID,
				},
				"data": stats,
			})
		case "storage-service":
			if ssStore == nil {
				writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "storage service database integration disabled (set APP_SS_DB_ENABLED=true)"})
				return
			}
			start := time.Now()
			packages, err := ssStore.LookupPackagesByUUIDs(r.Context(), []string{aipUUID})
			recordDBQuery("ssdb", "LookupPackagesByUUIDs", time.Since(start).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusBadGateway, map[string]any{
					"error":  fmt.Sprintf("failed to fetch storage service packages for %s", aipUUID),
					"detail": err.Error(),
				})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{
					"aip_uuid": aipUUID,
					"count":    len(packages),
				},
				"data": map[string]any{
					"lookup_uuids": []string{aipUUID},
					"packages":     packages,
				},
			})
		default:
			writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": "not found"})
		}
	}
}
