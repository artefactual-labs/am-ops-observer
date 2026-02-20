package mysql

import (
	"context"
	"errors"
	"strings"

	customermap "go-am-realtime-report-ui/internal/connectors/customermap"
)

// ReportTemplate is an app-owned saved report definition in SQLite.
type ReportTemplate struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Scope       string `json:"scope"`
	ConfigJSON  string `json:"config_json"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

var errTemplateStoreUnavailable = errors.New("template sqlite store not configured")

func (s *Store) templateStore() (*customermap.Store, error) {
	if s == nil || s.customerMap == nil {
		return nil, errTemplateStoreUnavailable
	}
	return s.customerMap, nil
}

func (s *Store) HasTemplateStore() bool {
	return s != nil && s.customerMap != nil
}

func (s *Store) ListReportTemplates(ctx context.Context, limit int) ([]ReportTemplate, error) {
	store, err := s.templateStore()
	if err != nil {
		return nil, err
	}
	items, err := store.ListReportTemplates(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]ReportTemplate, 0, len(items))
	for _, it := range items {
		row := ReportTemplate{
			ID:          it.ID,
			Name:        it.Name,
			Description: it.Description,
			Scope:       it.Scope,
			ConfigJSON:  it.ConfigJSON,
		}
		if it.CreatedAt != nil {
			row.CreatedAt = it.CreatedAt.UTC().Format("2006-01-02T15:04:05Z")
		}
		if it.UpdatedAt != nil {
			row.UpdatedAt = it.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z")
		}
		out = append(out, row)
	}
	return out, nil
}

func (s *Store) GetReportTemplate(ctx context.Context, id int64) (*ReportTemplate, error) {
	store, err := s.templateStore()
	if err != nil {
		return nil, err
	}
	it, err := store.GetReportTemplate(ctx, id)
	if err != nil {
		return nil, err
	}
	out := &ReportTemplate{
		ID:          it.ID,
		Name:        it.Name,
		Description: it.Description,
		Scope:       it.Scope,
		ConfigJSON:  it.ConfigJSON,
	}
	if it.CreatedAt != nil {
		out.CreatedAt = it.CreatedAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	if it.UpdatedAt != nil {
		out.UpdatedAt = it.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	return out, nil
}

func (s *Store) UpsertReportTemplate(ctx context.Context, name, description, scope, configJSON string) (int64, error) {
	store, err := s.templateStore()
	if err != nil {
		return 0, err
	}
	return store.UpsertReportTemplate(ctx, strings.TrimSpace(name), strings.TrimSpace(description), strings.TrimSpace(scope), strings.TrimSpace(configJSON))
}

func (s *Store) DeleteReportTemplate(ctx context.Context, id int64) (int64, error) {
	store, err := s.templateStore()
	if err != nil {
		return 0, err
	}
	return store.DeleteReportTemplate(ctx, id)
}
