package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	nethttp "net/http"
	"strconv"
	"strings"
	"time"

	esstore "go-am-realtime-report-ui/internal/connectors/es"
	mysqlstore "go-am-realtime-report-ui/internal/connectors/mysql"
	ssstore "go-am-realtime-report-ui/internal/connectors/ssdb"
)

type createMappingRequest struct {
	CustomerID          string `json:"customer_id"`
	SourceOfAcquisition string `json:"source_of_acquisition"`
}

type replaceMappingsRequest struct {
	Sources []string `json:"sources"`
}

type runReportRequest struct {
	DateFrom   string   `json:"date_from"`
	DateTo     string   `json:"date_to"`
	Status     string   `json:"status"`
	CustomerID string   `json:"customer_id"`
	Limit      int      `json:"limit"`
	Offset     int      `json:"offset"`
	Columns    []string `json:"columns"`
}

type saveTemplateRequest struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Scope       string         `json:"scope"`
	Config      map[string]any `json:"config"`
}

func runningTransfersHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"error": "database integration disabled (set APP_DB_ENABLED=true)",
			})
			return
		}

		limit := parseLimit(r, defaultLimit)
		start := time.Now()
		items, err := store.ListRunningTransfers(r.Context(), limit)
		recordDBQuery("mcp", "ListRunningTransfers", time.Since(start).Seconds(), err)
		if err != nil {
			status := nethttp.StatusInternalServerError
			if errors.Is(err, nethttp.ErrHandlerTimeout) {
				status = nethttp.StatusGatewayTimeout
			}
			writeJSON(w, status, map[string]any{
				"error": "failed to fetch running transfers",
			})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"limit": limit,
				"count": len(items),
			},
			"data": items,
		})
	}
}

func runningSIPsHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"error": "database integration disabled (set APP_DB_ENABLED=true)",
			})
			return
		}

		limit := parseLimit(r, defaultLimit)
		start := time.Now()
		items, err := store.ListRunningSIPs(r.Context(), limit)
		recordDBQuery("mcp", "ListRunningSIPs", time.Since(start).Seconds(), err)
		if err != nil {
			status := nethttp.StatusInternalServerError
			if errors.Is(err, nethttp.ErrHandlerTimeout) {
				status = nethttp.StatusGatewayTimeout
			}
			writeJSON(w, status, map[string]any{
				"error": "failed to fetch running SIPs",
			})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"limit": limit,
				"count": len(items),
			},
			"data": items,
		})
	}
}

func completedTransfersHandler(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"error": "database integration disabled (set APP_DB_ENABLED=true)",
			})
			return
		}

		limit := parseLimit(r, defaultLimit)
		offset := parseOffset(r)

		var month *time.Time
		if monthStr := strings.TrimSpace(r.URL.Query().Get("month")); monthStr != "" {
			parsed, err := time.Parse("2006-01", monthStr)
			if err != nil {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{
					"error": "invalid month format, expected YYYY-MM",
				})
				return
			}
			month = &parsed
		}

		start := time.Now()
		items, err := store.ListCompletedTransfers(r.Context(), limit, offset, month)
		recordDBQuery("mcp", "ListCompletedTransfers", time.Since(start).Seconds(), err)
		if err != nil {
			status := nethttp.StatusInternalServerError
			if errors.Is(err, nethttp.ErrHandlerTimeout) {
				status = nethttp.StatusGatewayTimeout
			}
			writeJSON(w, status, map[string]any{"error": "failed to fetch completed transfers"})
			return
		}

		meta := map[string]any{
			"limit":  limit,
			"offset": offset,
			"count":  len(items),
		}
		if month != nil {
			meta["month"] = month.Format("2006-01")
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": meta,
			"data": items,
		})
	}
}

func transferDetailRouter(defaultLimit int, store *mysqlstore.Store, ssStore *ssstore.Store, esClient *esstore.Client, esLookupLimit int) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"error": "database integration disabled (set APP_DB_ENABLED=true)",
			})
			return
		}

		trimmed := strings.TrimPrefix(r.URL.Path, "/api/v1/transfers/")
		parts := strings.Split(strings.Trim(trimmed, "/"), "/")
		if len(parts) != 2 || parts[0] == "" {
			writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": "not found"})
			return
		}

		transferUUID := parts[0]
		action := parts[1]
		limit := parseLimit(r, defaultLimit)

		switch action {
		case "summary":
			start := time.Now()
			item, err := store.GetTransferSummary(r.Context(), transferUUID)
			recordDBQuery("mcp", "GetTransferSummary", time.Since(start).Seconds(), err)
			if err != nil {
				if strings.Contains(err.Error(), "no rows in result set") {
					writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": fmt.Sprintf("transfer not found: %s", transferUUID)})
					return
				}
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer summary"})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{"data": item})
		case "details":
			startSummary := time.Now()
			summary, err := store.GetTransferSummary(r.Context(), transferUUID)
			recordDBQuery("mcp", "GetTransferSummary", time.Since(startSummary).Seconds(), err)
			if err != nil {
				if strings.Contains(err.Error(), "no rows in result set") {
					writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": fmt.Sprintf("transfer not found: %s", transferUUID)})
					return
				}
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer summary"})
				return
			}

			startTimeline := time.Now()
			timeline, err := store.GetTransferTimeline(r.Context(), transferUUID, limit)
			recordDBQuery("mcp", "GetTransferTimeline", time.Since(startTimeline).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer timeline"})
				return
			}
			startErrors := time.Now()
			errs, err := store.GetTransferErrors(r.Context(), transferUUID, limit)
			recordDBQuery("mcp", "GetTransferErrors", time.Since(startErrors).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer errors"})
				return
			}

			payload := map[string]any{
				"summary":  summary,
				"timeline": timeline,
				"errors":   errs,
			}
			meta := map[string]any{
				"transfer_uuid":  transferUUID,
				"timeline_count": len(timeline),
				"error_count":    len(errs),
			}

			startPerf := time.Now()
			perf, perfErr := store.GetTransferPerformance(r.Context(), transferUUID)
			recordDBQuery("mcp", "GetTransferPerformance", time.Since(startPerf).Seconds(), perfErr)
			if perfErr != nil {
				meta["performance_error"] = perfErr.Error()
			} else if perf != nil {
				payload["performance"] = perf
				meta["microservices_count"] = perf.Summary["microservices_count"]
			}

			if esClient != nil && esClient.Enabled() {
				startES := time.Now()
				esResult, esErr := esClient.SearchTransfer(r.Context(), transferUUID, esLookupLimit)
				recordExternalProbe("elasticsearch", "SearchTransfer", time.Since(startES).Seconds(), esErr)
				if esErr != nil {
					meta["es_error"] = esErr.Error()
				} else if esResult != nil {
					// If transfer UUID returns no hits, try related SIP/AIP UUID from performance view.
					if esResult.TotalHits == 0 && perf != nil && strings.TrimSpace(perf.RelatedSIPUUID) != "" {
						startESAlt := time.Now()
						alt, altErr := esClient.SearchTransfer(r.Context(), strings.TrimSpace(perf.RelatedSIPUUID), esLookupLimit)
						recordExternalProbe("elasticsearch", "SearchTransfer", time.Since(startESAlt).Seconds(), altErr)
						if altErr == nil && alt != nil {
							esResult = alt
							meta["es_lookup_uuid"] = strings.TrimSpace(perf.RelatedSIPUUID)
						}
					}
					payload["elasticsearch"] = esResult
					meta["es_hits"] = esResult.TotalHits
				}
			}

			if ssStore != nil {
				lookupUUIDs := []string{transferUUID}
				if perf != nil && strings.TrimSpace(perf.RelatedSIPUUID) != "" {
					lookupUUIDs = append(lookupUUIDs, perf.RelatedSIPUUID)
				}
				startSS := time.Now()
				packages, ssErr := ssStore.LookupPackagesByUUIDs(r.Context(), lookupUUIDs)
				recordDBQuery("ssdb", "LookupPackagesByUUIDs", time.Since(startSS).Seconds(), ssErr)
				if ssErr != nil {
					meta["ss_error"] = ssErr.Error()
				} else {
					payload["storage_service"] = map[string]any{
						"lookup_uuids": lookupUUIDs,
						"packages":     packages,
					}
					meta["ss_packages"] = len(packages)
				}
			}

			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": meta,
				"data": payload,
			})
		case "timeline":
			start := time.Now()
			items, err := store.GetTransferTimeline(r.Context(), transferUUID, limit)
			recordDBQuery("mcp", "GetTransferTimeline", time.Since(start).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer timeline"})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{"transfer_uuid": transferUUID, "limit": limit, "count": len(items)},
				"data": items,
			})
		case "errors":
			start := time.Now()
			items, err := store.GetTransferErrors(r.Context(), transferUUID, limit)
			recordDBQuery("mcp", "GetTransferErrors", time.Since(start).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch transfer errors"})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{"transfer_uuid": transferUUID, "limit": limit, "count": len(items)},
				"data": items,
			})
		default:
			writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": "not found"})
		}
	}
}

func monthlyReportHandler(defaultCustomerID string, store *mysqlstore.Store, ssStore *ssstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"error": "database integration disabled (set APP_DB_ENABLED=true)",
			})
			return
		}

		customerID := r.URL.Query().Get("customer_id")
		if customerID == "" {
			customerID = defaultCustomerID
		}

		monthStr := r.URL.Query().Get("month")
		if monthStr == "" {
			monthStr = time.Now().UTC().Format("2006-01")
		}

		month, err := time.Parse("2006-01", monthStr)
		if err != nil {
			writeJSON(w, nethttp.StatusBadRequest, map[string]any{
				"error": "invalid month format, expected YYYY-MM",
			})
			return
		}

		start := time.Now()
		report, err := store.GetMonthlyReport(r.Context(), customerID, month)
		recordDBQuery("mcp", "GetMonthlyReport", time.Since(start).Seconds(), err)
		if err != nil {
			writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to build monthly report"})
			return
		}

		writeJSON(w, nethttp.StatusOK, map[string]any{
			"meta": map[string]any{
				"customer_id":          report.CustomerID,
				"month":                report.Month,
				"customer_filter_mode": store.CustomerMappingMode(),
			},
			"kpis":         report.KPIs,
			"timeseries":   report.Timeseries,
			"integrations": buildReportIntegrations(r.Context(), ssStore, month),
		})
	}
}

func buildReportIntegrations(ctx context.Context, ssStore *ssstore.Store, month time.Time) map[string]any {
	out := map[string]any{}
	if ssStore == nil {
		out["storage_service_db"] = map[string]any{
			"enabled": false,
			"error":   "storage service db integration disabled",
		}
		return out
	}

	start := time.Now()
	stats, err := ssStore.ReportStats(ctx, month)
	recordDBQuery("ssdb", "ReportStats", time.Since(start).Seconds(), err)
	if err != nil {
		out["storage_service_db"] = map[string]any{
			"enabled": true,
			"ok":      false,
			"error":   err.Error(),
		}
		return out
	}

	out["storage_service_db"] = map[string]any{
		"enabled": true,
		"ok":      true,
		"stats":   stats,
	}
	return out
}

func reportRoutesRouter(defaultLimit int, store *mysqlstore.Store) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if store == nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{"error": "database integration disabled (set APP_DB_ENABLED=true)"})
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/")
		if path == "query/options" {
			if r.Method != nethttp.MethodGet {
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"data": map[string]any{
					"columns": mysqlstore.AvailableTransferReportColumns(),
					"statuses": []string{
						"all",
						"success",
						"failed",
						"completed_with_non_blocking_errors",
					},
				},
			})
			return
		}
		if path == "query" {
			if r.Method != nethttp.MethodPost {
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
			var req runReportRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
				return
			}
			dateFrom, dateTo, err := parseReportDateRange(req.DateFrom, req.DateTo)
			if err != nil {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}

			limit := req.Limit
			if limit <= 0 || limit > 1000 {
				limit = defaultLimit
			}
			offset := req.Offset
			if offset < 0 {
				offset = 0
			}
			columns := mysqlstore.NormalizeTransferReportColumns(req.Columns)
			start := time.Now()
			result, err := store.RunTransferReport(r.Context(), mysqlstore.TransferReportOptions{
				DateFrom:   dateFrom,
				DateTo:     dateTo,
				Status:     req.Status,
				CustomerID: req.CustomerID,
				Limit:      limit,
				Offset:     offset,
				Columns:    columns,
			})
			recordDBQuery("mcp", "RunTransferReport", time.Since(start).Seconds(), err)
			recordReportRun(map[bool]string{true: "error", false: "success"}[err != nil], time.Since(start).Seconds())
			if err != nil {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{
					"date_from": dateFrom.Format("2006-01-02"),
					"date_to":   dateTo.Add(-24 * time.Hour).Format("2006-01-02"),
					"status":    strings.TrimSpace(req.Status),
					"customer":  strings.TrimSpace(req.CustomerID),
					"limit":     limit,
					"offset":    offset,
					"columns":   columns,
					"total":     result.Total,
					"count":     len(result.Rows),
				},
				"data": result.Rows,
			})
			return
		}
		if path == "templates" {
			if !store.HasTemplateStore() {
				writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
					"error": "template sqlite store not available",
					"hint":  "set APP_CUSTOMER_MAP_SQLITE_PATH to enable app-owned template persistence",
				})
				return
			}
			switch r.Method {
			case nethttp.MethodGet:
				limit := parseLimit(r, defaultLimit)
				start := time.Now()
				items, err := store.ListReportTemplates(r.Context(), limit)
				recordDBQuery("appsqlite", "ListReportTemplates", time.Since(start).Seconds(), err)
				if err != nil {
					writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to list report templates"})
					return
				}
				writeJSON(w, nethttp.StatusOK, map[string]any{
					"meta": map[string]any{"count": len(items), "limit": limit},
					"data": items,
				})
				return
			case nethttp.MethodPost:
				var req saveTemplateRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
					return
				}
				req.Name = strings.TrimSpace(req.Name)
				req.Description = strings.TrimSpace(req.Description)
				req.Scope = strings.TrimSpace(req.Scope)
				if req.Name == "" {
					writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "template name is required"})
					return
				}
				if req.Scope == "" {
					req.Scope = "transfer"
				}
				if req.Config == nil {
					req.Config = map[string]any{}
				}
				configJSON, err := json.Marshal(req.Config)
				if err != nil {
					writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "invalid template config"})
					return
				}
				startUpsert := time.Now()
				id, err := store.UpsertReportTemplate(r.Context(), req.Name, req.Description, req.Scope, string(configJSON))
				recordDBQuery("appsqlite", "UpsertReportTemplate", time.Since(startUpsert).Seconds(), err)
				if err != nil {
					writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": err.Error()})
					return
				}
				startGet := time.Now()
				item, err := store.GetReportTemplate(r.Context(), id)
				recordDBQuery("appsqlite", "GetReportTemplate", time.Since(startGet).Seconds(), err)
				if err != nil {
					writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "template saved but failed to read it back"})
					return
				}
				writeJSON(w, nethttp.StatusOK, map[string]any{
					"meta": map[string]any{"saved": true},
					"data": item,
				})
				return
			default:
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
		}

		if strings.HasPrefix(path, "templates/") {
			if !store.HasTemplateStore() {
				writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
					"error": "template sqlite store not available",
					"hint":  "set APP_CUSTOMER_MAP_SQLITE_PATH to enable app-owned template persistence",
				})
				return
			}
			idRaw := strings.Trim(strings.TrimPrefix(path, "templates/"), "/")
			id, err := strconv.ParseInt(idRaw, 10, 64)
			if err != nil || id <= 0 {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "invalid template id"})
				return
			}
			switch r.Method {
			case nethttp.MethodGet:
				startGet := time.Now()
				item, err := store.GetReportTemplate(r.Context(), id)
				recordDBQuery("appsqlite", "GetReportTemplate", time.Since(startGet).Seconds(), err)
				if err != nil {
					writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": "template not found"})
					return
				}
				writeJSON(w, nethttp.StatusOK, map[string]any{"data": item})
				return
			case nethttp.MethodDelete:
				startDelete := time.Now()
				deleted, err := store.DeleteReportTemplate(r.Context(), id)
				recordDBQuery("appsqlite", "DeleteReportTemplate", time.Since(startDelete).Seconds(), err)
				if err != nil {
					writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to delete template"})
					return
				}
				writeJSON(w, nethttp.StatusOK, map[string]any{
					"meta": map[string]any{"deleted": deleted, "id": id},
				})
				return
			default:
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
		}

		if !store.HasCustomerSourceMapping() {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]any{
				"error": "customer mapping backend not available",
				"hint":  "set APP_CUSTOMER_MAP_SQLITE_PATH or create CustomerTransferSources in MCP",
			})
			return
		}

		if path == "customers" {
			if r.Method != nethttp.MethodGet {
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
			limit := parseLimit(r, defaultLimit)
			start := time.Now()
			items, err := store.ListCustomers(r.Context(), limit)
			recordDBQuery("appsqlite", "ListCustomers", time.Since(start).Seconds(), err)
			if err != nil {
				writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to list customers"})
				return
			}
			writeJSON(w, nethttp.StatusOK, map[string]any{
				"meta": map[string]any{"limit": limit, "count": len(items)},
				"data": items,
			})
			return
		}

		if path == "customer-mappings" {
			if r.Method != nethttp.MethodGet {
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
			writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{
				"error": "read-only mode: customer mapping mutations are disabled",
			})
			return
		}

		if strings.HasPrefix(path, "customer-mappings/") {
			customerID := strings.TrimPrefix(path, "customer-mappings/")
			customerID = strings.Trim(customerID, "/")
			if customerID == "" {
				writeJSON(w, nethttp.StatusBadRequest, map[string]any{"error": "customer_id path parameter is required"})
				return
			}

			switch r.Method {
			case nethttp.MethodGet:
				start := time.Now()
				mappings, err := store.GetCustomerMappings(r.Context(), customerID)
				recordDBQuery("appsqlite", "GetCustomerMappings", time.Since(start).Seconds(), err)
				if err != nil {
					writeJSON(w, nethttp.StatusInternalServerError, map[string]any{"error": "failed to fetch customer mappings"})
					return
				}
				writeJSON(w, nethttp.StatusOK, map[string]any{
					"meta": map[string]any{"customer_id": customerID, "count": len(mappings.Sources)},
					"data": mappings,
				})
				return
			case nethttp.MethodPut, nethttp.MethodPost, nethttp.MethodDelete, nethttp.MethodPatch:
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{
					"error": "read-only mode: customer mapping mutations are disabled",
				})
				return
			default:
				writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				return
			}
		}

		writeJSON(w, nethttp.StatusNotFound, map[string]any{"error": "not found"})
	}
}

func parseReportDateRange(fromRaw, toRaw string) (time.Time, time.Time, error) {
	now := time.Now().UTC()
	from := now.AddDate(0, 0, -30)
	to := now

	if strings.TrimSpace(fromRaw) != "" {
		parsed, err := time.Parse("2006-01-02", strings.TrimSpace(fromRaw))
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid date_from, expected YYYY-MM-DD")
		}
		from = parsed.UTC()
	}
	if strings.TrimSpace(toRaw) != "" {
		parsed, err := time.Parse("2006-01-02", strings.TrimSpace(toRaw))
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid date_to, expected YYYY-MM-DD")
		}
		to = parsed.UTC()
	}

	if to.Before(from) {
		return time.Time{}, time.Time{}, fmt.Errorf("date_to must be the same or after date_from")
	}

	// Inclusive date range on API input, converted to [from, to+1d) for SQL.
	return time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC),
		time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour),
		nil
}

func parseLimit(r *nethttp.Request, defaultLimit int) int {
	limit := defaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}
	return limit
}

func parseOffset(r *nethttp.Request) int {
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	return offset
}
