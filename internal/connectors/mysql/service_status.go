package mysql

import (
	"context"
	"database/sql"
	"time"
)

// ServiceStats contains lightweight DB health and volume counters.
type ServiceStats struct {
	PingMS             int64 `json:"ping_ms"`
	UptimeSeconds      int64 `json:"uptime_seconds"`
	TransfersTotal     int64 `json:"transfers_total"`
	TransfersRunning   int64 `json:"transfers_running"`
	TransfersCompleted int64 `json:"transfers_completed"`
	JobsExecuting      int64 `json:"jobs_executing"`
	JobsFailed24h      int64 `json:"jobs_failed_24h"`
}

// ServiceStats returns MySQL health and high-level transfer/job counters.
func (s *Store) ServiceStats(ctx context.Context) (*ServiceStats, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start := time.Now()
	if err := s.db.PingContext(ctx); err != nil {
		return nil, err
	}

	out := &ServiceStats{
		PingMS: time.Since(start).Milliseconds(),
	}

	var statusName string
	var statusValue sql.NullString
	if err := s.db.QueryRowContext(ctx, `SHOW GLOBAL STATUS LIKE 'Uptime';`).Scan(&statusName, &statusValue); err == nil && statusValue.Valid {
		if v, err := time.ParseDuration(statusValue.String + "s"); err == nil {
			out.UptimeSeconds = int64(v.Seconds())
		}
	}

	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM Transfers;`).Scan(&out.TransfersTotal); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM Transfers WHERE status = 1;`).Scan(&out.TransfersRunning); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM Transfers WHERE status IN (2,3,4);`).Scan(&out.TransfersCompleted); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM Jobs WHERE currentStep = 3;`).Scan(&out.JobsExecuting); err != nil {
		return nil, err
	}

	var failed24h sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM Tasks t
JOIN Jobs j
  ON j.jobUUID = t.jobuuid
WHERE COALESCE(t.exitCode, 0) <> 0
  AND COALESCE(t.endTime, t.startTime, j.createdTime) >= UTC_TIMESTAMP() - INTERVAL 24 HOUR;
	`).Scan(&failed24h); err != nil {
		return nil, err
	}
	if failed24h.Valid {
		out.JobsFailed24h = failed24h.Int64
	}

	return out, nil
}
