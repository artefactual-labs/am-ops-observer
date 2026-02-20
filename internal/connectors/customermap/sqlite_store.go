package customermap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Summary represents one customer and number of mapped sources.
type Summary struct {
	CustomerID  string `json:"customer_id"`
	SourceCount int64  `json:"source_count"`
}

// Store manages customer/source mappings in SQLite.
type Store struct {
	db *sql.DB
}

// ReportTemplate is an app-owned persisted report template.
type ReportTemplate struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Scope       string     `json:"scope"`
	ConfigJSON  string     `json:"config_json"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
}

func NewSQLiteStore(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("sqlite path required")
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS customer_transfer_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  source_of_acquisition TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(customer_id, source_of_acquisition)
);
`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_cts_customer_id ON customer_transfer_sources(customer_id);`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_cts_source ON customer_transfer_sources(source_of_acquisition);`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS report_templates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  description TEXT NOT NULL DEFAULT '',
  scope TEXT NOT NULL DEFAULT 'transfer',
  config_json TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_rt_scope ON report_templates(scope);`); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) ListCustomers(ctx context.Context, limit int) ([]Summary, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT customer_id, COUNT(*)
FROM customer_transfer_sources
GROUP BY customer_id
ORDER BY customer_id
LIMIT ?;
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Summary, 0, limit)
	for rows.Next() {
		var item Summary
		if err := rows.Scan(&item.CustomerID, &item.SourceCount); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) SourcesForCustomer(ctx context.Context, customerID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT source_of_acquisition
FROM customer_transfer_sources
WHERE customer_id = ?
ORDER BY source_of_acquisition;
`, strings.TrimSpace(customerID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var src string
		if err := rows.Scan(&src); err != nil {
			return nil, err
		}
		src = strings.TrimSpace(src)
		if src != "" {
			out = append(out, src)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) CreateMapping(ctx context.Context, customerID, source string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO customer_transfer_sources (customer_id, source_of_acquisition)
VALUES (?, ?)
ON CONFLICT(customer_id, source_of_acquisition) DO NOTHING;
`, strings.TrimSpace(customerID), strings.TrimSpace(source))
	return err
}

func (s *Store) ReplaceMappings(ctx context.Context, customerID string, sources []string) (int, error) {
	customerID = strings.TrimSpace(customerID)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM customer_transfer_sources WHERE customer_id = ?`, customerID); err != nil {
		return 0, err
	}

	norm := normalizeSources(sources)
	for _, src := range norm {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO customer_transfer_sources (customer_id, source_of_acquisition)
VALUES (?, ?)
ON CONFLICT(customer_id, source_of_acquisition) DO NOTHING;
`, customerID, src); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(norm), nil
}

func (s *Store) DeleteMapping(ctx context.Context, customerID, source string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM customer_transfer_sources WHERE customer_id = ? AND source_of_acquisition = ?`, strings.TrimSpace(customerID), strings.TrimSpace(source))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) DeleteAllMappings(ctx context.Context, customerID string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM customer_transfer_sources WHERE customer_id = ?`, strings.TrimSpace(customerID))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func normalizeSources(sources []string) []string {
	seen := make(map[string]struct{}, len(sources))
	out := make([]string, 0, len(sources))
	for _, src := range sources {
		src = strings.TrimSpace(src)
		if src == "" {
			continue
		}
		if _, ok := seen[src]; ok {
			continue
		}
		seen[src] = struct{}{}
		out = append(out, src)
	}
	return out
}

func (s *Store) ListReportTemplates(ctx context.Context, limit int) ([]ReportTemplate, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, description, scope, config_json, created_at, updated_at
FROM report_templates
ORDER BY name ASC
LIMIT ?;
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]ReportTemplate, 0, limit)
	for rows.Next() {
		var (
			item      ReportTemplate
			createdAt sql.NullTime
			updatedAt sql.NullTime
		)
		if err := rows.Scan(&item.ID, &item.Name, &item.Description, &item.Scope, &item.ConfigJSON, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		if createdAt.Valid {
			t := createdAt.Time.UTC()
			item.CreatedAt = &t
		}
		if updatedAt.Valid {
			t := updatedAt.Time.UTC()
			item.UpdatedAt = &t
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) GetReportTemplate(ctx context.Context, id int64) (*ReportTemplate, error) {
	var (
		item      ReportTemplate
		createdAt sql.NullTime
		updatedAt sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, `
SELECT id, name, description, scope, config_json, created_at, updated_at
FROM report_templates
WHERE id = ?;
`, id).Scan(&item.ID, &item.Name, &item.Description, &item.Scope, &item.ConfigJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	if createdAt.Valid {
		t := createdAt.Time.UTC()
		item.CreatedAt = &t
	}
	if updatedAt.Valid {
		t := updatedAt.Time.UTC()
		item.UpdatedAt = &t
	}
	return &item, nil
}

func (s *Store) UpsertReportTemplate(ctx context.Context, name, description, scope, configJSON string) (int64, error) {
	name = strings.TrimSpace(name)
	description = strings.TrimSpace(description)
	scope = strings.ToLower(strings.TrimSpace(scope))
	configJSON = strings.TrimSpace(configJSON)
	if name == "" {
		return 0, fmt.Errorf("template name is required")
	}
	if scope == "" {
		scope = "transfer"
	}
	if scope != "transfer" {
		return 0, fmt.Errorf("unsupported scope: %s", scope)
	}
	if configJSON == "" {
		return 0, fmt.Errorf("config_json is required")
	}

	res, err := s.db.ExecContext(ctx, `
INSERT INTO report_templates (name, description, scope, config_json, created_at, updated_at)
VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT(name) DO UPDATE SET
  description = excluded.description,
  scope = excluded.scope,
  config_json = excluded.config_json,
  updated_at = CURRENT_TIMESTAMP;
`, name, description, scope, configJSON)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err == nil && id > 0 {
		return id, nil
	}

	var existingID int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM report_templates WHERE name = ?`, name).Scan(&existingID); err != nil {
		return 0, err
	}
	return existingID, nil
}

func (s *Store) DeleteReportTemplate(ctx context.Context, id int64) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM report_templates WHERE id = ?`, id)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
