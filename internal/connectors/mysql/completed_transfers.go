package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CompletedTransfer is a finished transfer row for dashboard/report browsing.
type CompletedTransfer struct {
	TransferUUID    string     `json:"transfer_uuid"`
	Name            string     `json:"name"`
	StatusCode      int        `json:"status_code"`
	Status          string     `json:"status"`
	FailureEvidence bool       `json:"failure_evidence"`
	Recoverable     bool       `json:"recoverable"`
	StartedAt       *time.Time `json:"started_at"`
	CompletedAt     *time.Time `json:"completed_at"`
	DurationSeconds int64      `json:"duration_seconds"`
	FilesTotal      int64      `json:"files_total"`
	FilesOriginal   int64      `json:"files_original"`
	FilesNormalized int64      `json:"files_normalized"`
}

// TransferSummary is a detailed transfer overview used by troubleshooting/report views.
type TransferSummary struct {
	TransferUUID    string     `json:"transfer_uuid"`
	Name            string     `json:"name"`
	StatusCode      int        `json:"status_code"`
	Status          string     `json:"status"`
	FailureEvidence bool       `json:"failure_evidence"`
	Recoverable     bool       `json:"recoverable"`
	StartedAt       *time.Time `json:"started_at"`
	CompletedAt     *time.Time `json:"completed_at"`
	DurationSeconds int64      `json:"duration_seconds"`
	FilesTotal      int64      `json:"files_total"`
	FilesOriginal   int64      `json:"files_original"`
	FilesNormalized int64      `json:"files_normalized"`
	FailedJobs      int64      `json:"failed_jobs"`
}

// ListCompletedTransfers returns newest completed transfers, optionally filtered by month.
func (s *Store) ListCompletedTransfers(ctx context.Context, limit, offset int, month *time.Time) ([]CompletedTransfer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	whereClause := "WHERE t.status IN (2, 3, 4) AND t.completed_at IS NOT NULL"
	args := make([]any, 0, 4)
	if month != nil {
		start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 1, 0)
		whereClause += " AND t.completed_at >= ? AND t.completed_at < ?"
		args = append(args, start, end)
	}

	q := fmt.Sprintf(`
SELECT
  t.transferUUID,
  t.currentLocation,
  t.status,
  COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at) AS started_at,
  t.completed_at,
  COALESCE(
    TIMESTAMPDIFF(
      SECOND,
      COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at),
      COALESCE(tt.transfer_finished_at, t.completed_at)
    ),
    0
  ) AS duration_sec,
  COALESCE(fc.total_files, 0) AS files_total,
  COALESCE(fc.original_files, 0) AS files_original,
  COALESCE(fc.normalized_files, 0) AS files_normalized,
  COALESCE(fm.failed_markers, 0) AS failed_markers,
  COALESCE(sf.has_sip_output, 0) AS has_sip_output
FROM Transfers t
LEFT JOIN (
  SELECT
    t2.transferUUID,
    MIN(COALESCE(tsk.startTime, j.createdTime)) AS transfer_started_at,
    COALESCE(
      t2.completed_at,
      MAX(COALESCE(tsk.endTime, tsk.startTime, j.createdTime))
    ) AS transfer_finished_at
  FROM Transfers t2
  LEFT JOIN Jobs j
    ON j.SIPUUID = t2.transferUUID
    AND j.unitType LIKE '%%Transfer'
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  GROUP BY t2.transferUUID, t2.completed_at
) tt
  ON tt.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    MIN(f.enteredSystem) AS transfer_first_seen_at
  FROM Files f
  WHERE f.transferUUID IS NOT NULL
  GROUP BY f.transferUUID
) tfs
  ON tfs.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    COUNT(*) AS total_files,
    SUM(CASE WHEN LOWER(f.fileGrpUse) = 'original' THEN 1 ELSE 0 END) AS original_files,
    SUM(CASE WHEN d.derivedFileUUID IS NOT NULL THEN 1 ELSE 0 END) AS normalized_files
  FROM Files f
  LEFT JOIN Derivations d
    ON d.sourceFileUUID = f.fileUUID
  GROUP BY f.transferUUID
) fc
  ON fc.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    j.SIPUUID AS transferUUID,
    SUM(CASE WHEN COALESCE(tsk.exitCode, 0) <> 0 AND j.currentStep = 4 THEN 1 ELSE 0 END) AS failed_markers
  FROM Jobs j
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  WHERE j.unitType LIKE '%%Transfer'
  GROUP BY j.SIPUUID
) fm
  ON fm.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    MAX(CASE WHEN f.sipUUID IS NOT NULL AND f.sipUUID <> '' THEN 1 ELSE 0 END) AS has_sip_output
  FROM Files f
  GROUP BY f.transferUUID
) sf
  ON sf.transferUUID = t.transferUUID
%s
ORDER BY t.completed_at DESC
LIMIT ? OFFSET ?;
`, whereClause)

	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]CompletedTransfer, 0, limit)
	for rows.Next() {
		var (
				item            CompletedTransfer
				currentLocation string
				startedAt       sql.NullTime
				completedAt     sql.NullTime
				failedMarkers   sql.NullInt64
				hasSIPOutput    sql.NullInt64
			)

		if err := rows.Scan(
			&item.TransferUUID,
			&currentLocation,
			&item.StatusCode,
			&startedAt,
			&completedAt,
			&item.DurationSeconds,
			&item.FilesTotal,
				&item.FilesOriginal,
				&item.FilesNormalized,
				&failedMarkers,
				&hasSIPOutput,
			); err != nil {
				return nil, err
			}

			item.Name = transferNameFromLocation(currentLocation, item.TransferUUID)
			item.FailureEvidence = nullInt64Value(failedMarkers) > 0
			item.Recoverable = item.FailureEvidence && nullInt64Value(hasSIPOutput) > 0
			item.Status = transferStatusNameWithEvidence(item.StatusCode, item.FailureEvidence, item.Recoverable)
			item.StartedAt = nullTimePtr(startedAt)
			item.CompletedAt = nullTimePtr(completedAt)
			items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

// GetTransferSummary returns one transfer summary with processing and file counters.
func (s *Store) GetTransferSummary(ctx context.Context, transferUUID string) (*TransferSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  t.transferUUID,
  t.currentLocation,
  t.status,
  COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at) AS started_at,
  t.completed_at,
  COALESCE(
    TIMESTAMPDIFF(
      SECOND,
      COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at),
      COALESCE(tt.transfer_finished_at, t.completed_at)
    ),
    0
  ) AS duration_sec,
  COALESCE(fc.total_files, 0) AS files_total,
  COALESCE(fc.original_files, 0) AS files_original,
  COALESCE(fc.normalized_files, 0) AS files_normalized,
  COALESCE(fj.failed_jobs, 0) AS failed_jobs,
  COALESCE(fm.failed_markers, 0) AS failed_markers,
  COALESCE(sf.has_sip_output, 0) AS has_sip_output
FROM Transfers t
LEFT JOIN (
  SELECT
    t2.transferUUID,
    MIN(COALESCE(tsk.startTime, j.createdTime)) AS transfer_started_at,
    COALESCE(
      t2.completed_at,
      MAX(COALESCE(tsk.endTime, tsk.startTime, j.createdTime))
    ) AS transfer_finished_at
  FROM Transfers t2
  LEFT JOIN Jobs j
    ON j.SIPUUID = t2.transferUUID
    AND j.unitType LIKE '%Transfer'
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  GROUP BY t2.transferUUID, t2.completed_at
) tt
  ON tt.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    MIN(f.enteredSystem) AS transfer_first_seen_at
  FROM Files f
  WHERE f.transferUUID IS NOT NULL
  GROUP BY f.transferUUID
) tfs
  ON tfs.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    COUNT(*) AS total_files,
    SUM(CASE WHEN LOWER(f.fileGrpUse) = 'original' THEN 1 ELSE 0 END) AS original_files,
    SUM(CASE WHEN d.derivedFileUUID IS NOT NULL THEN 1 ELSE 0 END) AS normalized_files
  FROM Files f
  LEFT JOIN Derivations d
    ON d.sourceFileUUID = f.fileUUID
  GROUP BY f.transferUUID
) fc
  ON fc.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    j.SIPUUID AS transferUUID,
    SUM(CASE WHEN COALESCE(tsk.exitCode, 0) <> 0 AND j.currentStep = 4 THEN 1 ELSE 0 END) AS failed_jobs
  FROM Jobs j
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  WHERE j.unitType LIKE '%Transfer'
  GROUP BY j.SIPUUID
) fj
  ON fj.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    j.SIPUUID AS transferUUID,
    SUM(CASE WHEN COALESCE(tsk.exitCode, 0) <> 0 AND j.currentStep = 4 THEN 1 ELSE 0 END) AS failed_markers
  FROM Jobs j
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  WHERE j.unitType LIKE '%Transfer'
  GROUP BY j.SIPUUID
) fm
  ON fm.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    MAX(CASE WHEN f.sipUUID IS NOT NULL AND f.sipUUID <> '' THEN 1 ELSE 0 END) AS has_sip_output
  FROM Files f
  GROUP BY f.transferUUID
) sf
  ON sf.transferUUID = t.transferUUID
WHERE t.transferUUID = ?
LIMIT 1;
`

	var (
		item            TransferSummary
		currentLocation string
		startedAt       sql.NullTime
		completedAt     sql.NullTime
		failedMarkers   sql.NullInt64
		hasSIPOutput    sql.NullInt64
	)

	err := s.db.QueryRowContext(ctx, q, transferUUID).Scan(
		&item.TransferUUID,
		&currentLocation,
		&item.StatusCode,
		&startedAt,
		&completedAt,
		&item.DurationSeconds,
		&item.FilesTotal,
		&item.FilesOriginal,
		&item.FilesNormalized,
		&item.FailedJobs,
		&failedMarkers,
		&hasSIPOutput,
	)
	if err != nil {
		return nil, err
	}

	item.Name = transferNameFromLocation(currentLocation, item.TransferUUID)
	item.FailureEvidence = nullInt64Value(failedMarkers) > 0
	item.Recoverable = item.FailureEvidence && nullInt64Value(hasSIPOutput) > 0
	item.Status = transferStatusNameWithEvidence(item.StatusCode, item.FailureEvidence, item.Recoverable)
	item.StartedAt = nullTimePtr(startedAt)
	item.CompletedAt = nullTimePtr(completedAt)
	return &item, nil
}

func transferStatusNameWithEvidence(code int, failureEvidence, recoverable bool) string {
	if failureEvidence && recoverable {
		return "COMPLETED_WITH_NON_BLOCKING_ERRORS"
	}
	if failureEvidence && (code == 2 || code == 3) {
		return "FAILED_OR_ABORTED"
	}
	return transferStatusName(code)
}

func transferStatusName(code int) string {
	switch code {
	case 1:
		return "RUNNING"
	case 2:
		return "SUCCESS"
	case 3:
		return "SUCCESS_WITH_WARNINGS"
	case 4:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}
