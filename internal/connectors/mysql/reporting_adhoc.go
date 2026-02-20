package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ReportColumn describes a selectable column in ad-hoc transfer reports.
type ReportColumn struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

// TransferReportOptions controls ad-hoc transfer report execution.
type TransferReportOptions struct {
	DateFrom   time.Time
	DateTo     time.Time
	Status     string
	CustomerID string
	Limit      int
	Offset     int
	Columns    []string
}

// TransferReportResult contains rows and total count for paginated report output.
type TransferReportResult struct {
	Rows  []map[string]any `json:"rows"`
	Total int64            `json:"total"`
}

var transferReportColumns = []ReportColumn{
	{Key: "transfer_uuid", Label: "Transfer UUID"},
	{Key: "name", Label: "Transfer Name"},
	{Key: "status", Label: "Status"},
	{Key: "status_code", Label: "Status Code"},
	{Key: "recoverable", Label: "Recoverable"},
	{Key: "started_at", Label: "Started At"},
	{Key: "completed_at", Label: "Completed At"},
	{Key: "duration_seconds", Label: "Duration (s)"},
	{Key: "files_total", Label: "Files Total"},
	{Key: "files_original", Label: "Files Original"},
	{Key: "files_normalized", Label: "Files Normalized"},
	{Key: "size_mb", Label: "Total Size (MB)"},
	{Key: "failed_jobs", Label: "Failed Jobs"},
	{Key: "failed_tasks", Label: "Failed Tasks"},
	{Key: "sip_uuid", Label: "SIP UUID"},
	{Key: "source_of_acquisition", Label: "Source Of Acquisition"},
}

var transferReportColumnSet = map[string]struct{}{
	"transfer_uuid":          {},
	"name":                   {},
	"status":                 {},
	"status_code":            {},
	"recoverable":            {},
	"started_at":             {},
	"completed_at":           {},
	"duration_seconds":       {},
	"files_total":            {},
	"files_original":         {},
	"files_normalized":       {},
	"size_mb":                {},
	"failed_jobs":            {},
	"failed_tasks":           {},
	"sip_uuid":               {},
	"source_of_acquisition":  {},
}

// AvailableTransferReportColumns returns columns accepted by RunTransferReport.
func AvailableTransferReportColumns() []ReportColumn {
	out := make([]ReportColumn, 0, len(transferReportColumns))
	out = append(out, transferReportColumns...)
	return out
}

// NormalizeTransferReportColumns validates and normalizes selected columns.
func NormalizeTransferReportColumns(columns []string) []string {
	out := make([]string, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		key := strings.ToLower(strings.TrimSpace(c))
		if key == "" {
			continue
		}
		if _, ok := transferReportColumnSet[key]; !ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	if len(out) == 0 {
		return []string{"transfer_uuid", "name", "status", "completed_at", "duration_seconds", "files_total", "failed_tasks"}
	}
	return out
}

// RunTransferReport executes a configurable transfer report.
func (s *Store) RunTransferReport(ctx context.Context, opts TransferReportOptions) (*TransferReportResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	status := strings.ToLower(strings.TrimSpace(opts.Status))
	if status == "" {
		status = "all"
	}

	filterClause, filterArgs, err := s.sourceFilterClause(ctx, opts.CustomerID)
	if err != nil {
		return nil, err
	}

	where := "WHERE t.status IN (2, 3, 4) AND t.completed_at >= ? AND t.completed_at < ?"
	args := []any{opts.DateFrom, opts.DateTo}
	if filterClause != "" {
		where += " " + filterClause
		args = append(args, filterArgs...)
	}

	switch status {
	case "all":
	case "success":
		where += " AND COALESCE(fm.failed_markers, 0) = 0"
	case "failed":
		where += " AND COALESCE(fm.failed_markers, 0) > 0 AND COALESCE(sf.has_sip_output, 0) = 0"
	case "completed_with_non_blocking_errors":
		where += " AND COALESCE(fm.failed_markers, 0) > 0 AND COALESCE(sf.has_sip_output, 0) = 1"
	default:
		return nil, fmt.Errorf("invalid status filter: %s", status)
	}

	countQuery := fmt.Sprintf(`
SELECT COUNT(*)
FROM Transfers t
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
%s;
`, where)

	var total sql.NullInt64
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, err
	}

	q := fmt.Sprintf(`
SELECT
  t.transferUUID,
  t.currentLocation,
  t.status,
  t.sourceOfAcquisition,
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
  COALESCE(fc.total_bytes, 0) AS size_bytes,
  COALESCE(fm.failed_markers, 0) AS failed_tasks,
  COALESCE(fj.failed_jobs, 0) AS failed_jobs,
  COALESCE(sf.has_sip_output, 0) AS has_sip_output,
  COALESCE(sf.sip_uuid, '') AS sip_uuid
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
    SUM(CASE WHEN d.derivedFileUUID IS NOT NULL THEN 1 ELSE 0 END) AS normalized_files,
    SUM(COALESCE(f.fileSize, 0)) AS total_bytes
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
    j.SIPUUID AS transferUUID,
    COUNT(DISTINCT CASE WHEN COALESCE(tsk.exitCode, 0) <> 0 AND j.currentStep = 4 THEN j.jobUUID END) AS failed_jobs
  FROM Jobs j
  LEFT JOIN Tasks tsk
    ON tsk.jobuuid = j.jobUUID
  WHERE j.unitType LIKE '%%Transfer'
  GROUP BY j.SIPUUID
) fj
  ON fj.transferUUID = t.transferUUID
LEFT JOIN (
  SELECT
    f.transferUUID,
    MAX(CASE WHEN f.sipUUID IS NOT NULL AND f.sipUUID <> '' THEN 1 ELSE 0 END) AS has_sip_output,
    MAX(COALESCE(NULLIF(f.sipUUID, ''), '')) AS sip_uuid
  FROM Files f
  GROUP BY f.transferUUID
) sf
  ON sf.transferUUID = t.transferUUID
%s
ORDER BY t.completed_at DESC
LIMIT ? OFFSET ?;
`, where)

	queryArgs := append([]any{}, args...)
	queryArgs = append(queryArgs, opts.Limit, opts.Offset)
	rows, err := s.db.QueryContext(ctx, q, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := NormalizeTransferReportColumns(opts.Columns)
	resultRows := make([]map[string]any, 0, opts.Limit)
	for rows.Next() {
		var (
			transferUUID      string
			currentLocation   string
			statusCode        int
			source            string
			startedAt         sql.NullTime
			completedAt       sql.NullTime
			durationSeconds   int64
			filesTotal        int64
			filesOriginal     int64
			filesNormalized   int64
			sizeBytes         int64
			failedTasks       int64
			failedJobs        int64
			hasSIPOutput      sql.NullInt64
			sipUUID           string
		)
		if err := rows.Scan(
			&transferUUID,
			&currentLocation,
			&statusCode,
			&source,
			&startedAt,
			&completedAt,
			&durationSeconds,
			&filesTotal,
			&filesOriginal,
			&filesNormalized,
			&sizeBytes,
			&failedTasks,
			&failedJobs,
			&hasSIPOutput,
			&sipUUID,
		); err != nil {
			return nil, err
		}

		recoverable := failedTasks > 0 && nullInt64Value(hasSIPOutput) > 0
		status := transferStatusNameWithEvidence(statusCode, failedTasks > 0, recoverable)
		rowAll := map[string]any{
			"transfer_uuid":         transferUUID,
			"name":                  transferNameFromLocation(currentLocation, transferUUID),
			"status":                status,
			"status_code":           statusCode,
			"recoverable":           recoverable,
			"started_at":            formatNullTime(startedAt),
			"completed_at":          formatNullTime(completedAt),
			"duration_seconds":      durationSeconds,
			"files_total":           filesTotal,
			"files_original":        filesOriginal,
			"files_normalized":      filesNormalized,
			"size_mb":               float64(sizeBytes) / 1024.0 / 1024.0,
			"failed_jobs":           failedJobs,
			"failed_tasks":          failedTasks,
			"sip_uuid":              sipUUID,
			"source_of_acquisition": source,
		}

		row := make(map[string]any, len(columns))
		for _, c := range columns {
			row[c] = rowAll[c]
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &TransferReportResult{
		Rows:  resultRows,
		Total: nullInt64Value(total),
	}, nil
}

func formatNullTime(v sql.NullTime) string {
	if !v.Valid {
		return ""
	}
	return v.Time.UTC().Format(time.RFC3339)
}
