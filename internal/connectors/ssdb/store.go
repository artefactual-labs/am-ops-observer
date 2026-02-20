package ssdb

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"go-am-realtime-report-ui/internal/config"
)

// Store wraps Storage Service MySQL access.
type Store struct {
	db           *sql.DB
	queryTimeout time.Duration
	dbName       string
}

// ServiceStats reports basic SS DB health/counters.
type ServiceStats struct {
	PingMS        int64            `json:"ping_ms"`
	UptimeSeconds int64            `json:"uptime_seconds"`
	PackagesTotal int64            `json:"packages_total"`
	PackageTypes  map[string]int64 `json:"package_types"`
	Locations     int64            `json:"locations"`
	Pipelines     int64            `json:"pipelines"`
	FilesTotal    int64            `json:"files_total"`
}

// PackageInfo provides package+location details for transfer/AIP/SIP UUID lookups.
type PackageInfo struct {
	UUID             string     `json:"uuid"`
	PackageType      string     `json:"package_type"`
	Status           string     `json:"status"`
	SizeBytes        int64      `json:"size_bytes"`
	CurrentPath      string     `json:"current_path"`
	StoredDate       *time.Time `json:"stored_date"`
	CurrentLocation  string     `json:"current_location"`
	LocationPurpose  string     `json:"location_purpose"`
	LocationPath     string     `json:"location_path"`
	SpacePath        string     `json:"space_path"`
	PipelineUUID     string     `json:"pipeline_uuid"`
	PipelineName     string     `json:"pipeline_name"`
	FilesCount       int64      `json:"files_count"`
	FilesStoredCount int64      `json:"files_stored_count"`
}

// ReportStats returns SS package counters useful for monthly reporting context.
type ReportStats struct {
	Month               string           `json:"month"`
	PackagesStoredMonth int64            `json:"packages_stored_month"`
	BytesStoredMonth    int64            `json:"bytes_stored_month"`
	PackagesTotal       int64            `json:"packages_total"`
	PackageTypes        map[string]int64 `json:"package_types"`
}

func NewStore(cfg config.Config) (*Store, error) {
	db, err := sql.Open("mysql", cfg.SSMySQLDSN())
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.SSDBConnTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db, queryTimeout: cfg.SSDBQueryTimeout, dbName: cfg.SSDBName}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) ServiceStats(ctx context.Context) (*ServiceStats, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start := time.Now()
	if err := s.db.PingContext(ctx); err != nil {
		return nil, err
	}

	out := &ServiceStats{
		PingMS:       time.Since(start).Milliseconds(),
		PackageTypes: map[string]int64{},
	}

	var statusName string
	var statusValue sql.NullString
	if err := s.db.QueryRowContext(ctx, `SHOW GLOBAL STATUS LIKE 'Uptime';`).Scan(&statusName, &statusValue); err == nil && statusValue.Valid {
		if v, err := time.ParseDuration(statusValue.String + "s"); err == nil {
			out.UptimeSeconds = int64(v.Seconds())
		}
	}

	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM locations_package;`).Scan(&out.PackagesTotal); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM locations_location;`).Scan(&out.Locations); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM locations_pipeline;`).Scan(&out.Pipelines); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM locations_file;`).Scan(&out.FilesTotal); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `SELECT COALESCE(package_type, ''), COUNT(*) FROM locations_package GROUP BY package_type;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pkgType string
		var n int64
		if err := rows.Scan(&pkgType, &n); err != nil {
			return nil, err
		}
		out.PackageTypes[pkgType] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (s *Store) LookupPackagesByUUIDs(ctx context.Context, uuids []string) ([]PackageInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	norm := normalizeUUIDs(uuids)
	if len(norm) == 0 {
		return []PackageInfo{}, nil
	}

	placeholders := strings.Repeat("?,", len(norm))
	placeholders = strings.TrimSuffix(placeholders, ",")
	args := make([]any, 0, len(norm))
	for _, id := range norm {
		args = append(args, id)
	}

	q := `
SELECT
  p.id,
  p.uuid,
  COALESCE(p.package_type, ''),
  COALESCE(p.status, ''),
  COALESCE(p.size, 0),
  COALESCE(p.current_path, ''),
  p.stored_date,
  COALESCE(l.uuid, ''),
  COALESCE(l.purpose, ''),
  COALESCE(l.relative_path, ''),
  COALESCE(sp.path, ''),
  COALESCE(pl.uuid, ''),
  COALESCE(pl.remote_name, pl.description, '')
FROM locations_package p
LEFT JOIN locations_location l
  ON l.uuid = p.current_location_id
LEFT JOIN locations_space sp
  ON sp.uuid = l.space_id
LEFT JOIN locations_pipeline pl
  ON pl.uuid = p.origin_pipeline_id
WHERE p.uuid IN (` + placeholders + `)
ORDER BY p.stored_date DESC;
`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byPackageID := map[int]PackageInfo{}
	order := make([]int, 0)
	for rows.Next() {
		var (
			pkgID  int
			item   PackageInfo
			stored sql.NullTime
		)
		if err := rows.Scan(
			&pkgID,
			&item.UUID,
			&item.PackageType,
			&item.Status,
			&item.SizeBytes,
			&item.CurrentPath,
			&stored,
			&item.CurrentLocation,
			&item.LocationPurpose,
			&item.LocationPath,
			&item.SpacePath,
			&item.PipelineUUID,
			&item.PipelineName,
		); err != nil {
			return nil, err
		}
		item.StoredDate = nullTimePtr(stored)
		byPackageID[pkgID] = item
		order = append(order, pkgID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(byPackageID) == 0 {
		return []PackageInfo{}, nil
	}

	idPlaceholders := strings.Repeat("?,", len(byPackageID))
	idPlaceholders = strings.TrimSuffix(idPlaceholders, ",")
	idArgs := make([]any, 0, len(byPackageID))
	for id := range byPackageID {
		idArgs = append(idArgs, id)
	}

	fq := `
SELECT
  package_id,
  COUNT(*) AS files_count,
  SUM(CASE WHEN ` + "`stored`" + ` = 1 THEN 1 ELSE 0 END) AS files_stored_count
FROM locations_file
WHERE package_id IN (` + idPlaceholders + `)
GROUP BY package_id;
`

	frows, err := s.db.QueryContext(ctx, fq, idArgs...)
	if err != nil {
		return nil, err
	}
	defer frows.Close()

	for frows.Next() {
		var (
			pkgID       int
			count       sql.NullInt64
			storedCount sql.NullInt64
		)
		if err := frows.Scan(&pkgID, &count, &storedCount); err != nil {
			return nil, err
		}
		item := byPackageID[pkgID]
		item.FilesCount = nullInt64Value(count)
		item.FilesStoredCount = nullInt64Value(storedCount)
		byPackageID[pkgID] = item
	}
	if err := frows.Err(); err != nil {
		return nil, err
	}

	out := make([]PackageInfo, 0, len(byPackageID))
	seen := map[int]struct{}{}
	for _, id := range order {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, byPackageID[id])
	}
	return out, nil
}

func (s *Store) ReportStats(ctx context.Context, month time.Time) (*ReportStats, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)

	out := &ReportStats{
		Month:        start.Format("2006-01"),
		PackageTypes: map[string]int64{},
	}

	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM locations_package;`).Scan(&out.PackagesTotal); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(size), 0)
FROM locations_package
WHERE stored_date >= ?
  AND stored_date < ?;
`, start, end).Scan(&out.PackagesStoredMonth, &out.BytesStoredMonth); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `SELECT COALESCE(package_type, ''), COUNT(*) FROM locations_package GROUP BY package_type;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pkgType string
		var n int64
		if err := rows.Scan(&pkgType, &n); err != nil {
			return nil, err
		}
		out.PackageTypes[pkgType] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func normalizeUUIDs(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, id := range in {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func nullTimePtr(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	t := nt.Time.UTC()
	return &t
}

func nullInt64Value(v sql.NullInt64) int64 {
	if !v.Valid {
		return 0
	}
	return v.Int64
}
