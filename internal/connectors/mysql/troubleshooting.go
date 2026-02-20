package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// TimelineEvent is a job/task event for transfer troubleshooting views.
type TimelineEvent struct {
	TransferUUID      string     `json:"transfer_uuid"`
	JobUUID           string     `json:"job_uuid"`
	TaskUUID          string     `json:"task_uuid,omitempty"`
	JobType           string     `json:"job_type"`
	MicroserviceGroup string     `json:"microservice_group"`
	StatusCode        int        `json:"status_code"`
	Status            string     `json:"status"`
	FileUUID          string     `json:"file_uuid,omitempty"`
	Execution         string     `json:"execution,omitempty"`
	Arguments         string     `json:"arguments,omitempty"`
	CreatedAt         *time.Time `json:"created_at"`
	StartedAt         *time.Time `json:"started_at"`
	EndedAt           *time.Time `json:"ended_at"`
	DurationSeconds   *int64     `json:"duration_seconds,omitempty"`
	ExitCode          *int64     `json:"exit_code,omitempty"`
}

// TransferError is a failed task/job entry with useful diagnostics.
type TransferError struct {
	TransferUUID      string     `json:"transfer_uuid"`
	JobUUID           string     `json:"job_uuid"`
	TaskUUID          string     `json:"task_uuid,omitempty"`
	JobType           string     `json:"job_type"`
	MicroserviceGroup string     `json:"microservice_group"`
	FileUUID          string     `json:"file_uuid,omitempty"`
	FilePath          string     `json:"file_path,omitempty"`
	Execution         string     `json:"execution,omitempty"`
	Arguments         string     `json:"arguments,omitempty"`
	CreatedAt         *time.Time `json:"created_at"`
	StartedAt         *time.Time `json:"started_at"`
	EndedAt           *time.Time `json:"ended_at"`
	ExitCode          *int64     `json:"exit_code,omitempty"`
	ErrorText         string     `json:"error_text"`
	StdOut            string     `json:"stdout,omitempty"`
	StdErr            string     `json:"stderr,omitempty"`
}

// StalledTransfer represents a running transfer with no recent progress.
type StalledTransfer struct {
	TransferUUID           string     `json:"transfer_uuid"`
	Name                   string     `json:"name"`
	Stage                  string     `json:"stage"`
	Status                 string     `json:"status"`
	StartedAt              *time.Time `json:"started_at"`
	LastProgressAt         *time.Time `json:"last_progress_at"`
	MinutesWithoutProgress int64      `json:"minutes_without_progress"`
}

// ErrorHotspot groups recurring failures by microservice/job type.
type ErrorHotspot struct {
	MicroserviceGroup string     `json:"microservice_group"`
	JobType           string     `json:"job_type"`
	Failures          int64      `json:"failures"`
	DistinctTransfers int64      `json:"distinct_transfers"`
	LastSeenAt        *time.Time `json:"last_seen_at"`
}

// FailedTransfer is a transfer-level failed processing summary for triage.
type FailedTransfer struct {
	TransferUUID      string     `json:"transfer_uuid"`
	Name              string     `json:"name"`
	StatusCode        int        `json:"status_code"`
	Status            string     `json:"status"`
	Recoverable       bool       `json:"recoverable"`
	FailedAt          *time.Time `json:"failed_at"`
	StartedAt         *time.Time `json:"started_at"`
	DurationSeconds   int64      `json:"duration_seconds"`
	FilesTotal        int64      `json:"files_total"`
	FailedJobs        int64      `json:"failed_jobs"`
	MicroserviceGroup string     `json:"microservice_group"`
	ErrorText         string     `json:"error_text"`
}

// FailureSignature groups recurring failures by normalized stderr signature.
type FailureSignature struct {
	Signature         string     `json:"signature"`
	MicroserviceGroup string     `json:"microservice_group"`
	Failures          int64      `json:"failures"`
	DistinctTransfers int64      `json:"distinct_transfers"`
	LastSeenAt        *time.Time `json:"last_seen_at"`
}

// FailureCounts summarizes failed task attempts and distinct failed workflows.
type FailureCounts struct {
	FailedTasks int64 `json:"failed_tasks"`
	FailedUnits int64 `json:"failed_units"`
}

func unitWhereClause(unit string) string {
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "sip":
		return "j.unitType LIKE '%SIP'"
	case "all":
		return "(j.unitType LIKE '%Transfer' OR j.unitType LIKE '%SIP')"
	default:
		return "j.unitType LIKE '%Transfer'"
	}
}

// CountFailureCounts returns failed task and distinct-workflow counts in a time window.
// unit can be "transfer", "sip", or "all".
func (s *Store) CountFailureCounts(ctx context.Context, since time.Time, unit string) (*FailureCounts, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	q := fmt.Sprintf(`
SELECT
  COUNT(*) AS failed_tasks,
  COUNT(DISTINCT j.SIPUUID) AS failed_units
FROM Jobs j
JOIN Tasks t
  ON t.jobuuid = j.jobUUID
WHERE %s
  AND j.currentStep = 4
  AND COALESCE(t.exitCode, 0) <> 0
  AND COALESCE(t.endTime, t.startTime, j.createdTime) >= ?;
`, unitWhereClause(unit))

	var out FailureCounts
	if err := s.db.QueryRowContext(ctx, q, since).Scan(&out.FailedTasks, &out.FailedUnits); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTransferTimeline returns ordered transfer events from Jobs/Tasks.
func (s *Store) GetTransferTimeline(ctx context.Context, transferUUID string, limit int) ([]TimelineEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  j.SIPUUID,
  j.jobUUID,
  COALESCE(tsk.taskUUID, ''),
  COALESCE(j.jobType, ''),
  COALESCE(j.microserviceGroup, ''),
  j.currentStep,
  COALESCE(tsk.fileUUID, ''),
  COALESCE(tsk.exec, ''),
  COALESCE(tsk.arguments, ''),
  j.createdTime,
  tsk.startTime,
  tsk.endTime,
  tsk.exitCode
FROM Jobs j
LEFT JOIN Tasks tsk
  ON tsk.jobuuid = j.jobUUID
WHERE j.unitType LIKE '%Transfer'
  AND j.SIPUUID = ?
ORDER BY COALESCE(tsk.startTime, j.createdTime) ASC, j.createdTime ASC
LIMIT ?;
`

	rows, err := s.db.QueryContext(ctx, q, transferUUID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]TimelineEvent, 0, limit)
	for rows.Next() {
		var (
			item      TimelineEvent
			createdAt sql.NullTime
			startedAt sql.NullTime
			endedAt   sql.NullTime
			exitCode  sql.NullInt64
		)

		if err := rows.Scan(
			&item.TransferUUID,
			&item.JobUUID,
			&item.TaskUUID,
			&item.JobType,
			&item.MicroserviceGroup,
			&item.StatusCode,
			&item.FileUUID,
			&item.Execution,
			&item.Arguments,
			&createdAt,
			&startedAt,
			&endedAt,
			&exitCode,
		); err != nil {
			return nil, err
		}

		item.Status = jobStatusName(item.StatusCode)
		item.CreatedAt = nullTimePtr(createdAt)
		item.StartedAt = nullTimePtr(startedAt)
		item.EndedAt = nullTimePtr(endedAt)
		item.ExitCode = nullInt64Ptr(exitCode)
		item.DurationSeconds = secondsBetween(item.StartedAt, item.EndedAt)
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

// GetTransferErrors returns failed transfer tasks and their stderr snippets.
func (s *Store) GetTransferErrors(ctx context.Context, transferUUID string, limit int) ([]TransferError, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  j.SIPUUID,
  j.jobUUID,
  COALESCE(tsk.taskUUID, ''),
  COALESCE(j.jobType, ''),
  COALESCE(j.microserviceGroup, ''),
  COALESCE(tsk.fileUUID, ''),
  COALESCE(f.currentLocation, ''),
  COALESCE(tsk.exec, ''),
  COALESCE(tsk.arguments, ''),
  j.createdTime,
  tsk.startTime,
  tsk.endTime,
  tsk.exitCode,
  COALESCE(tsk.stdOut, ''),
  COALESCE(tsk.stdError, '')
FROM Jobs j
LEFT JOIN Tasks tsk
  ON tsk.jobuuid = j.jobUUID
LEFT JOIN Files f
  ON f.fileUUID = tsk.fileUUID
		WHERE j.unitType LIKE '%Transfer'
		  AND j.SIPUUID = ?
		  AND COALESCE(tsk.exitCode, 0) <> 0
		  AND j.currentStep = 4
		ORDER BY COALESCE(tsk.endTime, tsk.startTime, j.createdTime) DESC
		LIMIT ?;
		`

	rows, err := s.db.QueryContext(ctx, q, transferUUID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]TransferError, 0, limit)
	for rows.Next() {
		var (
			item      TransferError
			createdAt sql.NullTime
			startedAt sql.NullTime
			endedAt   sql.NullTime
			exitCode  sql.NullInt64
			stdout    string
			stderr    string
		)

		if err := rows.Scan(
			&item.TransferUUID,
			&item.JobUUID,
			&item.TaskUUID,
			&item.JobType,
			&item.MicroserviceGroup,
			&item.FileUUID,
			&item.FilePath,
			&item.Execution,
			&item.Arguments,
			&createdAt,
			&startedAt,
			&endedAt,
			&exitCode,
			&stdout,
			&stderr,
		); err != nil {
			return nil, err
		}

		item.CreatedAt = nullTimePtr(createdAt)
		item.StartedAt = nullTimePtr(startedAt)
		item.EndedAt = nullTimePtr(endedAt)
		item.ExitCode = nullInt64Ptr(exitCode)
		item.StdOut = compactOptional(stdout)
		item.StdErr = compactOptional(stderr)
		item.ErrorText = compactError(stderr)
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

// ListStalledTransfers returns running transfers whose latest progress is older than stuckAfter.
func (s *Store) ListStalledTransfers(ctx context.Context, limit int) ([]StalledTransfer, error) {
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
HAVING last_progress_at IS NOT NULL
ORDER BY last_progress_at ASC
LIMIT ?;
`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	items := make([]StalledTransfer, 0, limit)
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

		lastProgressAtPtr := nullTimePtr(lastProgressAt)
		if lastProgressAtPtr == nil || now.Sub(*lastProgressAtPtr) <= s.stuckAfter {
			continue
		}

		status := "RUNNING"
		if hasExecuting == 0 {
			status = "WAITING"
		}

		items = append(items, StalledTransfer{
			TransferUUID:           transferUUID,
			Name:                   transferNameFromLocation(currentLocation, transferUUID),
			Stage:                  stage,
			Status:                 status,
			StartedAt:              nullTimePtr(startedAt),
			LastProgressAt:         lastProgressAtPtr,
			MinutesWithoutProgress: int64(now.Sub(*lastProgressAtPtr).Minutes()),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// ListErrorHotspots returns failing microservice/job groups for a recent time window.
// unit can be "transfer", "sip", or "all".
func (s *Store) ListErrorHotspots(ctx context.Context, since time.Time, limit int, unit string) ([]ErrorHotspot, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	q := fmt.Sprintf(`
SELECT
  COALESCE(NULLIF(j.microserviceGroup, ''), 'UNKNOWN') AS microservice_group,
  COALESCE(NULLIF(j.jobType, ''), 'UNKNOWN') AS job_type,
  COUNT(*) AS failures,
  COUNT(DISTINCT j.SIPUUID) AS distinct_transfers,
  MAX(COALESCE(tsk.endTime, tsk.startTime, j.createdTime)) AS last_seen_at
FROM Jobs j
LEFT JOIN Tasks tsk
  ON tsk.jobuuid = j.jobUUID
		WHERE %s
		  AND COALESCE(tsk.endTime, tsk.startTime, j.createdTime) >= ?
		  AND COALESCE(tsk.exitCode, 0) <> 0
		  AND j.currentStep = 4
		GROUP BY microservice_group, job_type
		ORDER BY failures DESC, last_seen_at DESC
		LIMIT ?;
	`, unitWhereClause(unit))

	rows, err := s.db.QueryContext(ctx, q, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]ErrorHotspot, 0, limit)
	for rows.Next() {
		var (
			item       ErrorHotspot
			lastSeenAt sql.NullTime
		)
		if err := rows.Scan(
			&item.MicroserviceGroup,
			&item.JobType,
			&item.Failures,
			&item.DistinctTransfers,
			&lastSeenAt,
		); err != nil {
			return nil, err
		}
		item.LastSeenAt = nullTimePtr(lastSeenAt)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// ListFailedTransfers returns failed transfers with latest failure context.
func (s *Store) ListFailedTransfers(ctx context.Context, since time.Time, limit, offset int) ([]FailedTransfer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  t.transferUUID,
  t.currentLocation,
  t.status,
  COALESCE(last_fail.fail_time, t.completed_at) AS failed_at,
  COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at) AS started_at,
  COALESCE(
    TIMESTAMPDIFF(
      SECOND,
      COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at),
      COALESCE(last_fail.fail_time, t.completed_at)
    ),
    0
  ) AS duration_sec,
  COALESCE(fc.total_files, 0) AS files_total,
  COALESCE(fj.failed_jobs, 0) AS failed_jobs,
  COALESCE(sf.has_sip_output, 0) AS has_sip_output,
  COALESCE(last_fail.microservice_group, 'UNKNOWN') AS microservice_group,
  COALESCE(last_fail.error_text, '') AS error_text
FROM Transfers t
LEFT JOIN (
  SELECT
    t2.transferUUID,
    MIN(COALESCE(tsk.startTime, j.createdTime)) AS transfer_started_at
  FROM Transfers t2
  LEFT JOIN Jobs j
    ON j.SIPUUID = t2.transferUUID
    AND j.unitType LIKE '%Transfer'
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  GROUP BY t2.transferUUID
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
    COUNT(*) AS total_files
  FROM Files f
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
    f.transferUUID,
    MAX(CASE WHEN f.sipUUID IS NOT NULL AND f.sipUUID <> '' THEN 1 ELSE 0 END) AS has_sip_output
  FROM Files f
  GROUP BY f.transferUUID
) sf
  ON sf.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    x.transfer_uuid,
    x.microservice_group,
    x.error_text,
    x.fail_time
  FROM (
	    SELECT
	      j.SIPUUID AS transfer_uuid,
	      COALESCE(NULLIF(j.microserviceGroup, ''), 'UNKNOWN') AS microservice_group,
	      COALESCE(NULLIF(tsk.stdError, ''), '') AS error_text,
	      COALESCE(tsk.endTime, tsk.startTime, j.createdTime) AS fail_time,
	      ROW_NUMBER() OVER (
	        PARTITION BY j.SIPUUID
	        ORDER BY
	          CASE WHEN COALESCE(tsk.exitCode, 0) <> 0 AND j.currentStep = 4 THEN 1 ELSE 0 END DESC,
	          COALESCE(tsk.endTime, tsk.startTime, j.createdTime) DESC
	      ) AS rn
	    FROM Jobs j
	    LEFT JOIN Tasks tsk
	      ON tsk.jobuuid = j.jobUUID
	    WHERE j.unitType LIKE '%Transfer'
	      AND COALESCE(tsk.exitCode, 0) <> 0
	      AND j.currentStep = 4
	  ) x
	  WHERE x.rn = 1
) last_fail
  ON last_fail.transfer_uuid = t.transferUUID
	WHERE last_fail.transfer_uuid IS NOT NULL
	  AND COALESCE(last_fail.fail_time, t.completed_at) >= ?
ORDER BY COALESCE(last_fail.fail_time, t.completed_at) DESC
LIMIT ? OFFSET ?;
`

	rows, err := s.db.QueryContext(ctx, q, since, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]FailedTransfer, 0, limit)
	for rows.Next() {
		var (
				item            FailedTransfer
				currentLocation string
				failedAt        sql.NullTime
				startedAt       sql.NullTime
				hasSIPOutput    sql.NullInt64
			)
		if err := rows.Scan(
			&item.TransferUUID,
			&currentLocation,
			&item.StatusCode,
			&failedAt,
			&startedAt,
				&item.DurationSeconds,
				&item.FilesTotal,
				&item.FailedJobs,
				&hasSIPOutput,
				&item.MicroserviceGroup,
				&item.ErrorText,
			); err != nil {
			return nil, err
		}
			item.Name = transferNameFromLocation(currentLocation, item.TransferUUID)
			item.Recoverable = nullInt64Value(hasSIPOutput) > 0
			item.Status = transferStatusNameWithEvidence(item.StatusCode, true, item.Recoverable)
			item.FailedAt = nullTimePtr(failedAt)
		item.StartedAt = nullTimePtr(startedAt)
		item.ErrorText = compactError(item.ErrorText)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// ListFailureSignatures returns grouped recurring error signatures.
func (s *Store) ListFailureSignatures(ctx context.Context, since time.Time, limit int) ([]FailureSignature, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT
  signature,
  microservice_group,
  COUNT(*) AS failures,
  COUNT(DISTINCT transfer_uuid) AS distinct_transfers,
  MAX(fail_time) AS last_seen_at
FROM (
  SELECT
    j.SIPUUID AS transfer_uuid,
    COALESCE(NULLIF(j.microserviceGroup, ''), 'UNKNOWN') AS microservice_group,
    COALESCE(tsk.endTime, tsk.startTime, j.createdTime) AS fail_time,
    LEFT(
      REPLACE(REPLACE(LOWER(TRIM(COALESCE(NULLIF(tsk.stdError, ''), CONCAT('job failed: ', COALESCE(j.jobType, 'unknown'))))), '\n', ' '), '\r', ' '),
      220
    ) AS signature
  FROM Jobs j
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
		  WHERE j.unitType LIKE '%Transfer'
		    AND COALESCE(tsk.endTime, tsk.startTime, j.createdTime) >= ?
		    AND COALESCE(tsk.exitCode, 0) <> 0
		    AND j.currentStep = 4
) fail_rows
GROUP BY signature, microservice_group
ORDER BY failures DESC, last_seen_at DESC
LIMIT ?;
`

	rows, err := s.db.QueryContext(ctx, q, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]FailureSignature, 0, limit)
	for rows.Next() {
		var (
			item       FailureSignature
			lastSeenAt sql.NullTime
		)
		if err := rows.Scan(&item.Signature, &item.MicroserviceGroup, &item.Failures, &item.DistinctTransfers, &lastSeenAt); err != nil {
			return nil, err
		}
		item.LastSeenAt = nullTimePtr(lastSeenAt)
		item.Signature = compactError(item.Signature)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func compactError(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "task/job marked failed without stderr text"
	}
	const maxLen = 500
	if len(trimmed) > maxLen {
		return trimmed[:maxLen] + "..."
	}
	return trimmed
}

func compactOptional(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	const maxLen = 500
	if len(trimmed) > maxLen {
		return trimmed[:maxLen] + "..."
	}
	return trimmed
}

func jobStatusName(code int) string {
	switch code {
	case 1:
		return "AWAITING_DECISION"
	case 2:
		return "COMPLETE"
	case 3:
		return "RUNNING"
	case 4:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func nullInt64Ptr(ni sql.NullInt64) *int64 {
	if !ni.Valid {
		return nil
	}
	v := ni.Int64
	return &v
}

func secondsBetween(start, end *time.Time) *int64 {
	if start == nil || end == nil {
		return nil
	}
	d := int64(end.Sub(*start).Seconds())
	if d < 0 {
		d = 0
	}
	return &d
}
