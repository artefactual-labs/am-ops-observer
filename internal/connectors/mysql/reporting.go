package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"time"
)

// DailyReportPoint is an aggregated day bucket for monthly reports.
type DailyReportPoint struct {
	Date    string `json:"date"`
	Success int64  `json:"success"`
	Failed  int64  `json:"failed"`
}

// MonthlyReport contains KPI summary and chart-ready timeseries.
type MonthlyReport struct {
	Month      string             `json:"month"`
	CustomerID string             `json:"customer_id"`
	KPIs       map[string]any     `json:"kpis"`
	Timeseries []DailyReportPoint `json:"timeseries"`
}

// DurationChartPoint is a day bucket used directly by frontend graphs.
type DurationChartPoint struct {
	Date       string `json:"date"`
	Count      int64  `json:"count"`
	AvgSeconds int64  `json:"avg_seconds"`
	P50Seconds int64  `json:"p50_seconds"`
	P95Seconds int64  `json:"p95_seconds"`
}

// GetMonthlyReport builds a real report from Transfers/Jobs/Tasks/Files tables.
func (s *Store) GetMonthlyReport(ctx context.Context, customerID string, month time.Time) (*MonthlyReport, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)

	filterClause, args, err := s.sourceFilterClause(ctx, customerID)
	if err != nil {
		return nil, err
	}

	summaryQuery := fmt.Sprintf(`
SELECT
  COUNT(*) AS transfers_total,
  SUM(CASE WHEN t.status IN (2, 3) THEN 1 ELSE 0 END) AS transfers_success,
  SUM(CASE WHEN t.status = 4 THEN 1 ELSE 0 END) AS transfers_failed
FROM Transfers t
WHERE t.completed_at >= ?
  AND t.completed_at < ?
  %s;
`, filterClause)

	summaryArgs := append([]any{start, end}, args...)
	var total, success, failed sql.NullInt64
	if err := s.db.QueryRowContext(ctx, summaryQuery, summaryArgs...).Scan(&total, &success, &failed); err != nil {
		return nil, err
	}

	durations, err := s.transferDurationsForMonth(ctx, start, end, filterClause, args)
	if err != nil {
		return nil, err
	}
	avg, p50, p95 := durationStats(durations)

	filesQuery := fmt.Sprintf(`
SELECT
  COALESCE(SUM(fc.total_files), 0) AS files_total,
  COALESCE(SUM(fc.original_files), 0) AS files_original,
  COALESCE(SUM(fc.normalized_files), 0) AS files_normalized
FROM Transfers t
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
WHERE t.completed_at >= ?
  AND t.completed_at < ?
  %s;
`, filterClause)

	filesArgs := append([]any{start, end}, args...)
	var filesTotal, filesOriginal, filesNormalized sql.NullInt64
	if err := s.db.QueryRowContext(ctx, filesQuery, filesArgs...).Scan(&filesTotal, &filesOriginal, &filesNormalized); err != nil {
		return nil, err
	}

	timeseriesQuery := fmt.Sprintf(`
SELECT
  DATE(t.completed_at) AS d,
  SUM(CASE WHEN t.status IN (2, 3) THEN 1 ELSE 0 END) AS success_count,
  SUM(CASE WHEN t.status = 4 THEN 1 ELSE 0 END) AS failed_count
FROM Transfers t
WHERE t.completed_at >= ?
  AND t.completed_at < ?
  %s
GROUP BY DATE(t.completed_at)
ORDER BY DATE(t.completed_at);
`, filterClause)

	timeseriesArgs := append([]any{start, end}, args...)
	rows, err := s.db.QueryContext(ctx, timeseriesQuery, timeseriesArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	series := make([]DailyReportPoint, 0)
	for rows.Next() {
		var (
			day     time.Time
			success sql.NullInt64
			failed  sql.NullInt64
		)
		if err := rows.Scan(&day, &success, &failed); err != nil {
			return nil, err
		}
		series = append(series, DailyReportPoint{
			Date:    day.Format("2006-01-02"),
			Success: nullInt64Value(success),
			Failed:  nullInt64Value(failed),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	defaultSLASeconds := int64(24 * 60 * 60)
	onTime := int64(0)
	for _, d := range durations {
		if d <= defaultSLASeconds {
			onTime++
		}
	}
	slaPercent := 0.0
	if len(durations) > 0 {
		slaPercent = round2((float64(onTime) / float64(len(durations))) * 100)
	}

	backlogQuery := fmt.Sprintf(`
SELECT COUNT(*)
FROM Transfers t
WHERE t.status = 1
  %s;
`, filterClause)
	var backlog sql.NullInt64
	if err := s.db.QueryRowContext(ctx, backlogQuery, args...).Scan(&backlog); err != nil {
		return nil, err
	}

	report := &MonthlyReport{
		Month:      start.Format("2006-01"),
		CustomerID: customerID,
		KPIs: map[string]any{
			"transfers_total":      nullInt64Value(total),
			"transfers_success":    nullInt64Value(success),
			"transfers_failed":     nullInt64Value(failed),
			"avg_processing_sec":   avg,
			"p50_processing_sec":   p50,
			"p95_processing_sec":   p95,
			"files_total":          nullInt64Value(filesTotal),
			"files_original":       nullInt64Value(filesOriginal),
			"files_normalized":     nullInt64Value(filesNormalized),
			"sla_on_time_percent":  slaPercent,
			"backlog_end_of_month": nullInt64Value(backlog),
		},
		Timeseries: series,
	}

	return report, nil
}

func (s *Store) transferDurationsForMonth(ctx context.Context, start, end time.Time, filterClause string, filterArgs []any) ([]int64, error) {
	q := fmt.Sprintf(`
SELECT
  TIMESTAMPDIFF(
    SECOND,
    COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at),
    COALESCE(tt.transfer_finished_at, t.completed_at)
  ) AS duration_sec
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
    AND j.unitType = 'Transfer'
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
WHERE t.completed_at >= ?
  AND t.completed_at < ?
  AND t.status IN (2, 3, 4)
  AND COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at) IS NOT NULL
  AND COALESCE(tt.transfer_finished_at, t.completed_at) IS NOT NULL
  %s;
`, filterClause)

	args := append([]any{start, end}, filterArgs...)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	durations := make([]int64, 0)
	for rows.Next() {
		var dur sql.NullInt64
		if err := rows.Scan(&dur); err != nil {
			return nil, err
		}
		if dur.Valid && dur.Int64 >= 0 {
			durations = append(durations, dur.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return durations, nil
}

// GetTransferDurationChart returns per-day duration percentiles for monthly graphing.
func (s *Store) GetTransferDurationChart(ctx context.Context, customerID string, month time.Time) ([]DurationChartPoint, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	filterClause, args, err := s.sourceFilterClause(ctx, customerID)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf(`
SELECT
  DATE(t.completed_at) AS day_bucket,
  TIMESTAMPDIFF(
    SECOND,
    COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at),
    COALESCE(tt.transfer_finished_at, t.completed_at)
  ) AS duration_sec
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
    AND j.unitType = 'Transfer'
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
WHERE t.completed_at >= ?
  AND t.completed_at < ?
  AND t.status IN (2, 3, 4)
  AND COALESCE(tt.transfer_started_at, tfs.transfer_first_seen_at) IS NOT NULL
  AND COALESCE(tt.transfer_finished_at, t.completed_at) IS NOT NULL
  %s;
`, filterClause)

	queryArgs := append([]any{start, end}, args...)
	rows, err := s.db.QueryContext(ctx, q, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byDay := map[string][]int64{}
	for rows.Next() {
		var (
			day time.Time
			dur sql.NullInt64
		)
		if err := rows.Scan(&day, &dur); err != nil {
			return nil, err
		}
		if dur.Valid && dur.Int64 >= 0 {
			key := day.Format("2006-01-02")
			byDay[key] = append(byDay[key], dur.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(byDay))
	for k := range byDay {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	series := make([]DurationChartPoint, 0, len(keys))
	for _, day := range keys {
		d := byDay[day]
		avg, p50, p95 := durationStats(d)
		series = append(series, DurationChartPoint{
			Date:       day,
			Count:      int64(len(d)),
			AvgSeconds: avg,
			P50Seconds: p50,
			P95Seconds: p95,
		})
	}

	return series, nil
}

func durationStats(durations []int64) (int64, int64, int64) {
	if len(durations) == 0 {
		return 0, 0, 0
	}

	sorted := append([]int64(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	total := int64(0)
	for _, d := range sorted {
		total += d
	}
	avg := total / int64(len(sorted))
	p50 := percentile(sorted, 50)
	p95 := percentile(sorted, 95)

	return avg, p50, p95
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil((float64(p)/100.0)*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func nullInt64Value(v sql.NullInt64) int64 {
	if !v.Valid {
		return 0
	}
	return v.Int64
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
