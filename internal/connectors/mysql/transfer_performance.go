package mysql

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// TransferTiming contains package-level timing/task information.
type TransferTiming struct {
	StartTime       *time.Time `json:"start_time"`
	EndTime         *time.Time `json:"end_time"`
	DurationSeconds int64      `json:"duration_seconds"`
	Tasks           int64      `json:"tasks"`
}

// TransferSize contains file and volume information.
type TransferSize struct {
	Files   int64   `json:"files"`
	TotalMB float64 `json:"total_mb"`
}

// TransferFormatBreakdown groups files by format and use.
type TransferFormatBreakdown struct {
	PronomID    string `json:"pronom_id"`
	FileGrpUse  string `json:"file_grp_use"`
	Description string `json:"description"`
	Count       int64  `json:"count"`
}

// MicroserviceDuration represents one phase/microservice duration and CPU profile.
type MicroserviceDuration struct {
	Phase             string  `json:"phase"`
	MicroserviceGroup string  `json:"microservice_group"`
	CPUSeconds        int64   `json:"cpu_seconds"`
	Tasks             int64   `json:"tasks"`
	FailedTasks       int64   `json:"failed_tasks"`
	DurationSeconds   int64   `json:"duration_seconds"`
	CPUToWallRatio    float64 `json:"cpu_to_wall_ratio"`
	BottleneckHint    string  `json:"bottleneck_hint"`
}

// TransferPerformance is a detailed performance snapshot for one transfer.
type TransferPerformance struct {
	RelatedSIPUUID  string                    `json:"related_sip_uuid,omitempty"`
	TransferDetails TransferTiming            `json:"transfer_details"`
	TransferSize    TransferSize              `json:"transfer_size"`
	FormatBreakdown []TransferFormatBreakdown `json:"format_breakdown"`
	Microservices   []MicroserviceDuration    `json:"microservices"`
	Summary         map[string]any            `json:"summary"`
}

// GetTransferPerformance returns SQL-based per-transfer performance metrics.
func (s *Store) GetTransferPerformance(ctx context.Context, transferUUID string) (*TransferPerformance, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	out := &TransferPerformance{
		FormatBreakdown: make([]TransferFormatBreakdown, 0),
		Microservices:   make([]MicroserviceDuration, 0),
		Summary:         map[string]any{},
	}

	relatedSIP := ""
	_ = s.db.QueryRowContext(ctx, `
SELECT f.sipUUID
FROM Files f
WHERE f.transferUUID = ?
  AND f.sipUUID IS NOT NULL
GROUP BY f.sipUUID
ORDER BY COUNT(*) DESC
LIMIT 1;
`, transferUUID).Scan(&relatedSIP)
	out.RelatedSIPUUID = relatedSIP

	uuids := []any{transferUUID}
	where := "J.SIPUUID = ?"
	if relatedSIP != "" && relatedSIP != transferUUID {
		where = "J.SIPUUID = ? OR J.SIPUUID = ?"
		uuids = append(uuids, relatedSIP)
	}

	var (
		startTime sql.NullTime
		endTime   sql.NullTime
		tasks     sql.NullInt64
		durSec    sql.NullInt64
	)
	timingQuery := `
SELECT
  MIN(J.createdTime) AS start_time,
  MAX(T.endTime) AS end_time,
  COALESCE(TIMESTAMPDIFF(SECOND, MIN(T.createdTime), MAX(T.endTime)), 0) AS duration_seconds,
  COUNT(*) AS tasks
FROM Jobs J
JOIN Tasks T
  ON J.jobUUID = T.jobuuid
WHERE ` + where + `;`
	if err := s.db.QueryRowContext(ctx, timingQuery, uuids...).Scan(&startTime, &endTime, &durSec, &tasks); err != nil {
		return nil, err
	}
	out.TransferDetails = TransferTiming{
		StartTime:       nullTimePtr(startTime),
		EndTime:         nullTimePtr(endTime),
		DurationSeconds: nullInt64Value(durSec),
		Tasks:           nullInt64Value(tasks),
	}

	var (
		files   sql.NullInt64
		totalMB sql.NullFloat64
	)
	if err := s.db.QueryRowContext(ctx, `
SELECT
  COUNT(*) AS files,
  COALESCE(ROUND(SUM(fileSize)/1024/1024, 2), 0) AS mb
FROM Files
WHERE transferUUID = ?;
`, transferUUID).Scan(&files, &totalMB); err != nil {
		return nil, err
	}
	out.TransferSize = TransferSize{
		Files:   nullInt64Value(files),
		TotalMB: nullFloat64Value(totalMB),
	}

	formatsRows, err := s.db.QueryContext(ctx, `
SELECT
  COALESCE(V.pronom_id, '') AS pronom_id,
  COALESCE(F.fileGrpUse, '') AS file_grp_use,
  COALESCE(V.description, '') AS description,
  COUNT(*) AS c
FROM Files F
LEFT JOIN FilesIdentifiedIDs I
  ON I.fileUUID = F.fileUUID
LEFT JOIN fpr_formatversion V
  ON V.uuid = I.fileID
WHERE F.transferUUID = ?
GROUP BY I.fileID, F.fileGrpUse, V.pronom_id, V.description
ORDER BY c DESC
LIMIT 200;
`, transferUUID)
	if err != nil {
		return nil, err
	}
	defer formatsRows.Close()

	for formatsRows.Next() {
		var item TransferFormatBreakdown
		if err := formatsRows.Scan(&item.PronomID, &item.FileGrpUse, &item.Description, &item.Count); err != nil {
			return nil, err
		}
		out.FormatBreakdown = append(out.FormatBreakdown, item)
	}
	if err := formatsRows.Err(); err != nil {
		return nil, err
	}

	microRows, err := s.db.QueryContext(ctx, `
SELECT
  SUBSTRING(J.unitType, 5) AS phase,
  COALESCE(J.microserviceGroup, '') AS microservice_group,
  SUM(
    GREATEST(
      TIMESTAMPDIFF(
        SECOND,
        COALESCE(T.startTime, T.createdTime),
        COALESCE(T.endTime, T.startTime, T.createdTime)
      ),
      0
    )
  ) AS cpu_seconds,
  COUNT(T.taskUUID) AS tasks,
  SUM(
    CASE
      WHEN COALESCE(T.exitCode, 0) <> 0 AND J.currentStep = 4 THEN 1
      ELSE 0
    END
  ) AS failed_tasks,
  GREATEST(
    TIMESTAMPDIFF(
      SECOND,
      MIN(COALESCE(T.startTime, T.createdTime)),
      MAX(COALESCE(T.endTime, T.startTime, T.createdTime))
    ),
    0
  ) AS duration_seconds
FROM Jobs J
JOIN Tasks T
  ON J.jobUUID = T.jobuuid
WHERE `+where+`
GROUP BY J.unitType, J.microserviceGroup
ORDER BY MIN(T.createdTime);
`, uuids...)
	if err != nil {
		return nil, err
	}
	defer microRows.Close()

	totalCPU := int64(0)
	totalWall := int64(0)
	maxDuration := int64(0)
	topStage := ""

	for microRows.Next() {
		var item MicroserviceDuration
		if err := microRows.Scan(&item.Phase, &item.MicroserviceGroup, &item.CPUSeconds, &item.Tasks, &item.FailedTasks, &item.DurationSeconds); err != nil {
			return nil, err
		}

		if item.DurationSeconds > 0 {
			item.CPUToWallRatio = round2(float64(item.CPUSeconds) / float64(item.DurationSeconds))
		}
		item.BottleneckHint = bottleneckHint(item.CPUToWallRatio, item.MicroserviceGroup)

		totalCPU += item.CPUSeconds
		totalWall += item.DurationSeconds
		if item.DurationSeconds > maxDuration {
			maxDuration = item.DurationSeconds
			topStage = item.MicroserviceGroup
		}

		out.Microservices = append(out.Microservices, item)
	}
	if err := microRows.Err(); err != nil {
		return nil, err
	}

	out.Summary["microservices_count"] = len(out.Microservices)
	out.Summary["total_cpu_seconds"] = totalCPU
	out.Summary["total_wall_seconds"] = totalWall
	out.Summary["overall_cpu_to_wall_ratio"] = round2(safeRatio(totalCPU, totalWall))
	out.Summary["longest_microservice_group"] = topStage
	out.Summary["longest_microservice_seconds"] = maxDuration

	return out, nil
}

func safeRatio(cpu, wall int64) float64 {
	if wall <= 0 {
		return 0
	}
	return float64(cpu) / float64(wall)
}

func bottleneckHint(ratio float64, microservice string) string {
	name := strings.ToLower(strings.TrimSpace(microservice))
	if strings.Contains(name, "create sip from transfer") {
		return "may include human wait time"
	}
	if ratio >= 2.5 {
		return "cpu_bound"
	}
	if ratio <= 0.8 {
		return "io_or_wait_bound"
	}
	return "mixed"
}

func nullFloat64Value(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}
