package http

import (
	nethttp "net/http"

	"go-am-realtime-report-ui/internal/config"
)

func riskThresholdsHandler(cfg config.Config) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		writeJSON(w, nethttp.StatusOK, map[string]any{
			"data": map[string]any{
				"unknown_hot_rate":            cfg.RiskUnknownHotRate,
				"unknown_hot_abs":             cfg.RiskUnknownHotAbs,
				"missing_ids_hot_rate":        cfg.RiskMissingIDsHotRate,
				"missing_ids_hot_abs":         cfg.RiskMissingIDsHotAbs,
				"missing_created_hot_rate":    cfg.RiskMissingCreatedHotRate,
				"missing_created_hot_abs":     cfg.RiskMissingCreatedHotAbs,
				"ext_mismatch_hot_rate":       cfg.RiskExtMismatchHotRate,
				"ext_mismatch_hot_abs":        cfg.RiskExtMismatchHotAbs,
				"dup_files_hot_rate":          cfg.RiskDupFilesHotRate,
				"dup_files_hot_abs":           cfg.RiskDupFilesHotAbs,
				"dup_groups_hot_abs":          cfg.RiskDupGroupsHotAbs,
				"index_lag_p95_hot_sec":       cfg.RiskIndexLagP95HotSec,
				"min_diversity_ratio":         cfg.RiskMinDiversityRatio,
				"tiny_files_max":              cfg.RiskTinyFilesMax,
				"min_unique_formats":          cfg.RiskMinUniqueFormats,
				"min_unique_formats_for_tiny": cfg.RiskMinUniqueFormatsForTiny,
			},
		})
	}
}
