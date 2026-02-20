package mysql

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"go-am-realtime-report-ui/internal/config"
	customermap "go-am-realtime-report-ui/internal/connectors/customermap"
)

// RunningTransfer is the live troubleshooting view for a transfer in progress.
type RunningTransfer struct {
	TransferUUID   string     `json:"transfer_uuid"`
	Name           string     `json:"name"`
	Stage          string     `json:"stage"`
	Status         string     `json:"status"`
	StartedAt      *time.Time `json:"started_at"`
	ElapsedSeconds int64      `json:"elapsed_seconds"`
	LastProgressAt *time.Time `json:"last_progress_at"`
	Stuck          bool       `json:"stuck"`
}

// RunningSIP is the live troubleshooting view for a SIP in ingest processing.
type RunningSIP struct {
	SIPUUID        string     `json:"sip_uuid"`
	Stage          string     `json:"stage"`
	Status         string     `json:"status"`
	StartedAt      *time.Time `json:"started_at"`
	ElapsedSeconds int64      `json:"elapsed_seconds"`
	LastProgressAt *time.Time `json:"last_progress_at"`
	Stuck          bool       `json:"stuck"`
	FailedJobs     int64      `json:"failed_jobs"`
	ExecutingJobs  int64      `json:"executing_jobs"`
	AwaitingJobs   int64      `json:"awaiting_jobs"`
}

// Store wraps MySQL access for troubleshooting/reporting queries.
type Store struct {
	db                       *sql.DB
	queryTimeout             time.Duration
	stuckAfter               time.Duration
	dbName                   string
	hasCustomerSourceMapping bool
	customerMap              *customermap.Store
	customerMappingMode      string
	customerMappingPath      string
}

// NewStore creates a MySQL-backed store.
func NewStore(cfg config.Config) (*Store, error) {
	db, err := sql.Open("mysql", cfg.MySQLDSN())
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DBConnTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	hasMapping, err := detectCustomerSourceMappingTable(ctx, db, cfg.DBName)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	var customerMap *customermap.Store
	mappingMode := "source_of_acquisition_fallback"
	mappingPath := ""
	if path := strings.TrimSpace(cfg.CustomerMapSQLitePath); path != "" {
		m, err := customermap.NewSQLiteStore(path)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		customerMap = m
		mappingMode = "sqlite_customer_mappings"
		hasMapping = true
		mappingPath = path
	} else if hasMapping {
		mappingMode = "mysql_customer_mapping_table"
	}

	return &Store{
		db:                       db,
		queryTimeout:             cfg.DBQueryTimeout,
		stuckAfter:               cfg.RunningStuckAfter,
		dbName:                   cfg.DBName,
		hasCustomerSourceMapping: hasMapping,
		customerMap:              customerMap,
		customerMappingMode:      mappingMode,
		customerMappingPath:      mappingPath,
	}, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if s.customerMap != nil {
		_ = s.customerMap.Close()
	}
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// HasCustomerSourceMapping reports whether CustomerTransferSources table was detected.
func (s *Store) HasCustomerSourceMapping() bool {
	if s == nil {
		return false
	}
	return s.hasCustomerSourceMapping
}

// CustomerMappingMode reports which mapping backend is active.
func (s *Store) CustomerMappingMode() string {
	if s == nil {
		return "source_of_acquisition_fallback"
	}
	return s.customerMappingMode
}

// CustomerMappingPath returns the configured sqlite path when sqlite backend is active.
func (s *Store) CustomerMappingPath() string {
	if s == nil {
		return ""
	}
	return s.customerMappingPath
}

// ListRunningTransfers returns active transfers with their latest known progress markers.
func (s *Store) ListRunningTransfers(ctx context.Context, limit int) ([]RunningTransfer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  t.transferUUID,
  t.currentLocation,
  COALESCE(
    SUBSTRING_INDEX(
      GROUP_CONCAT(NULLIF(j.microserviceGroup, '') ORDER BY COALESCE(tsk.startTime, j.createdTime) DESC SEPARATOR '||'),
      '||',
      1
    ),
    'Processing'
  ) AS stage,
  MIN(COALESCE(tsk.startTime, j.createdTime)) AS started_at,
  MAX(COALESCE(tsk.endTime, tsk.startTime, j.createdTime)) AS last_progress_at,
  MAX(CASE WHEN j.currentStep = 3 THEN 1 ELSE 0 END) AS has_executing_jobs
FROM Transfers t
LEFT JOIN Jobs j
  ON j.SIPUUID = t.transferUUID
  AND j.unitType LIKE '%Transfer'
LEFT JOIN Tasks tsk
  ON tsk.jobuuid = j.jobUUID
WHERE t.status = 1
GROUP BY t.transferUUID, t.currentLocation
ORDER BY COALESCE(started_at, last_progress_at) ASC, t.transferUUID ASC
LIMIT ?;
`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	items := make([]RunningTransfer, 0, limit)
	for rows.Next() {
		var (
			transferUUID    string
			currentLocation string
			stage           string
			startedAt       sql.NullTime
			lastProgressAt  sql.NullTime
			hasExecuting    int
		)

		if err := rows.Scan(&transferUUID, &currentLocation, &stage, &startedAt, &lastProgressAt, &hasExecuting); err != nil {
			return nil, err
		}

		startedAtPtr := nullTimePtr(startedAt)
		lastProgressAtPtr := nullTimePtr(lastProgressAt)

		elapsed := int64(0)
		if startedAtPtr != nil {
			elapsed = int64(now.Sub(*startedAtPtr).Seconds())
		}

		stuck := false
		if lastProgressAtPtr != nil {
			stuck = now.Sub(*lastProgressAtPtr) > s.stuckAfter
		}

		status := "RUNNING"
		if hasExecuting == 0 {
			status = "WAITING"
		}

		items = append(items, RunningTransfer{
			TransferUUID:   transferUUID,
			Name:           transferNameFromLocation(currentLocation, transferUUID),
			Stage:          stage,
			Status:         status,
			StartedAt:      startedAtPtr,
			ElapsedSeconds: elapsed,
			LastProgressAt: lastProgressAtPtr,
			Stuck:          stuck,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

// ListRunningSIPs returns active SIP ingest rows based on SIP jobs state.
func (s *Store) ListRunningSIPs(ctx context.Context, limit int) ([]RunningSIP, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  j.SIPUUID AS sip_uuid,
  COALESCE(
    SUBSTRING_INDEX(
      GROUP_CONCAT(NULLIF(j.microserviceGroup, '') ORDER BY COALESCE(tsk.startTime, j.createdTime) DESC SEPARATOR '||'),
      '||',
      1
    ),
    'Ingest'
  ) AS stage,
  MIN(COALESCE(tsk.startTime, j.createdTime)) AS started_at,
  MAX(COALESCE(tsk.endTime, tsk.startTime, j.createdTime)) AS last_progress_at,
  SUM(CASE WHEN j.currentStep = 4 THEN 1 ELSE 0 END) AS failed_jobs,
  SUM(CASE WHEN j.currentStep = 3 THEN 1 ELSE 0 END) AS executing_jobs,
  SUM(CASE WHEN j.currentStep = 1 THEN 1 ELSE 0 END) AS awaiting_jobs
FROM Jobs j
LEFT JOIN Tasks tsk
  ON tsk.jobuuid = j.jobUUID
WHERE j.unitType LIKE '%SIP'
  AND j.SIPUUID IS NOT NULL
  AND j.SIPUUID <> ''
GROUP BY j.SIPUUID
HAVING SUM(CASE WHEN j.currentStep IN (1, 3) THEN 1 ELSE 0 END) > 0
ORDER BY COALESCE(started_at, last_progress_at) ASC, j.SIPUUID ASC
LIMIT ?;
`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	items := make([]RunningSIP, 0, limit)
	for rows.Next() {
		var (
			item           RunningSIP
			startedAt      sql.NullTime
			lastProgressAt sql.NullTime
		)
		if err := rows.Scan(
			&item.SIPUUID,
			&item.Stage,
			&startedAt,
			&lastProgressAt,
			&item.FailedJobs,
			&item.ExecutingJobs,
			&item.AwaitingJobs,
		); err != nil {
			return nil, err
		}

		item.StartedAt = nullTimePtr(startedAt)
		item.LastProgressAt = nullTimePtr(lastProgressAt)
		if item.StartedAt != nil {
			item.ElapsedSeconds = int64(now.Sub(*item.StartedAt).Seconds())
		}
		if item.LastProgressAt != nil {
			item.Stuck = now.Sub(*item.LastProgressAt) > s.stuckAfter
		}
		item.Status = "WAITING"
		if item.ExecutingJobs > 0 {
			item.Status = "RUNNING"
		}
		if item.FailedJobs > 0 {
			item.Status = "FAILED"
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func detectCustomerSourceMappingTable(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	const q = `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = ?
  AND table_name = 'CustomerTransferSources';
`
	var count sql.NullInt64
	if err := db.QueryRowContext(ctx, q, dbName).Scan(&count); err != nil {
		return false, err
	}
	return count.Valid && count.Int64 > 0, nil
}

func nullTimePtr(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	t := nt.Time.UTC()
	return &t
}

func transferNameFromLocation(currentLocation, transferUUID string) string {
	parts := strings.Split(strings.Trim(currentLocation, "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		candidate := sanitizeLocationSegment(parts[i], transferUUID)
		if candidate != "" {
			return candidate
		}
	}
	return transferUUID
}

func sanitizeLocationSegment(segment, transferUUID string) string {
	segment = strings.TrimSpace(strings.Trim(segment, "/"))
	if segment == "" {
		return ""
	}
	lower := strings.ToLower(segment)
	transferLower := strings.ToLower(strings.TrimSpace(transferUUID))

	// Ignore AM path placeholders and generic processing buckets.
	if strings.Contains(segment, "%") {
		return ""
	}
	if strings.Contains(lower, "currentlyprocessing") ||
		strings.Contains(lower, "watchdirectory") ||
		strings.Contains(lower, "sharedpath") ||
		strings.Contains(lower, "completed") ||
		strings.Contains(lower, "processing") {
		return ""
	}

	// Remove UUID suffix from transfer directory names.
	if transferLower != "" {
		suffix := "-" + transferLower
		if strings.HasSuffix(strings.ToLower(segment), suffix) {
			segment = segment[:len(segment)-len(suffix)]
		}
	}
	segment = strings.TrimSpace(segment)
	if segment == "" || strings.EqualFold(segment, transferUUID) {
		return ""
	}
	return segment
}
