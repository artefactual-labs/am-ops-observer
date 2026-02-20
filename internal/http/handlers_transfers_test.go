package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	mysqlstore "go-am-realtime-report-ui/internal/connectors/mysql"
)

func TestCompletedTransfersHandler_DBDisabled(t *testing.T) {
	h := completedTransfersHandler(50, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/transfers/completed?limit=20", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rr.Code)
	}

	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload["error"] == nil {
		t.Fatalf("expected error field in response")
	}
}

func TestTransferDetailRouter_DBDisabled(t *testing.T) {
	h := transferDetailRouter(50, nil, nil, nil, 5)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/transfers/abc-123/details", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rr.Code)
	}
}

func TestTransferDetailRouter_UnknownActionReturnsNotFound(t *testing.T) {
	h := transferDetailRouter(50, &mysqlstore.Store{}, nil, nil, 5)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/transfers/abc-123/unknown", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rr.Code)
	}
}

func TestTransferDetailRouter_InvalidPathReturnsNotFound(t *testing.T) {
	h := transferDetailRouter(50, &mysqlstore.Store{}, nil, nil, 5)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/transfers/abc-123", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rr.Code)
	}
}
