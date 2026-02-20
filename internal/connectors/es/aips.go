package es

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// AIPListItem is a selectable AIP row for UI/reporting.
type AIPListItem struct {
	AIPUUID string `json:"aip_uuid"`
	SIPName string `json:"sip_name"`
}

// AIPListResult is a paginated list of unique AIP UUIDs.
type AIPListResult struct {
	Items      []AIPListItem `json:"items"`
	NextCursor string        `json:"next_cursor"`
}

// KeyCount is a reusable count bucket.
type KeyCount struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// AIPFileSample is a representative file-level row for one AIP.
type AIPFileSample struct {
	FilePath          string `json:"file_path"`
	FileUUID          string `json:"file_uuid"`
	Bytes             int64  `json:"bytes"`
	Extension         string `json:"extension"`
	FormatRegistryKey string `json:"format_registry_key"`
	FormatName        string `json:"format_name"`
	Status            string `json:"status"`
	CreatedByAppDate  string `json:"created_by_app_date"`
	IndexedAt         string `json:"indexed_at"`
}

// AIPFormatVersionCount is a count bucket split by registry key/name/version.
type AIPFormatVersionCount struct {
	FormatRegistryKey string `json:"format_registry_key"`
	FormatName        string `json:"format_name"`
	FormatVersion     string `json:"format_version"`
	Count             int64  `json:"count"`
}

type premisEventMeta struct {
	EventType    string
	EventOutcome string
	ToolVersion  string
}

// AIPStats aggregates file/format/normalization details for one AIP.
type AIPStats struct {
	AIPUUID                     string                  `json:"aip_uuid"`
	SIPNames                    []string                `json:"sip_names"`
	FilesTotal                  int64                   `json:"files_total"`
	UniqueFileUUIDs             int64                   `json:"unique_file_uuids"`
	OriginalsWithNormalized     int64                   `json:"originals_with_normalized"`
	OriginalsWithoutNormalized  int64                   `json:"originals_without_normalized"`
	NormalizedRefsTotal         int64                   `json:"normalized_refs_total"`
	UniqueNormalizedObjectIDs   int64                   `json:"unique_normalized_object_ids"`
	NormalizedByOriginalAvg     float64                 `json:"normalized_by_original_avg"`
	TotalBytes                  int64                   `json:"total_bytes"`
	AverageBytes                float64                 `json:"average_bytes"`
	MinIndexedAt                string                  `json:"min_indexed_at"`
	MaxIndexedAt                string                  `json:"max_indexed_at"`
	MinCreatedByAppDate         string                  `json:"min_created_by_app_date"`
	MaxCreatedByAppDate         string                  `json:"max_created_by_app_date"`
	FirstEventDate              string                  `json:"first_event_date"`
	LastEventDate               string                  `json:"last_event_date"`
	StatusCounts                []KeyCount              `json:"status_counts"`
	OriginCounts                []KeyCount              `json:"origin_counts"`
	AccessionIDCounts           []KeyCount              `json:"accession_id_counts"`
	IsPartOfCounts              []KeyCount              `json:"is_part_of_counts"`
	ArchivematicaVersionCounts  []KeyCount              `json:"archivematica_version_counts"`
	FormatRegistryCounts        []KeyCount              `json:"format_registry_counts"`
	FormatNameCounts            []KeyCount              `json:"format_name_counts"`
	FormatVersionCounts         []AIPFormatVersionCount `json:"format_version_counts"`
	PREMISEventTypeCounts       []KeyCount              `json:"premis_event_type_counts"`
	PREMISEventOutcomeCounts    []KeyCount              `json:"premis_event_outcome_counts"`
	PREMISToolCounts            []KeyCount              `json:"premis_tool_counts"`
	MissingIdentifiers          int64                   `json:"missing_identifiers"`
	UnknownFormats              int64                   `json:"unknown_formats"`
	MissingCreatedByAppDate     int64                   `json:"missing_created_by_app_date"`
	ExtensionFormatMismatch     int64                   `json:"extension_format_mismatch"`
	DuplicateFilenameGroups     int64                   `json:"duplicate_filename_groups"`
	DuplicateFilenameCandidates int64                   `json:"duplicate_filename_candidates"`
	UniqueFormatSignatures      int64                   `json:"unique_format_signatures"`
	FormatDiversityRatio        float64                 `json:"format_diversity_ratio"`
	IndexedLagAvgSeconds        float64                 `json:"indexed_lag_avg_seconds"`
	IndexedLagP95Seconds        int64                   `json:"indexed_lag_p95_seconds"`
	LargestFiles                []AIPFileSample         `json:"largest_files"`
	ExtensionCounts             []KeyCount              `json:"extension_counts"`
	RawExtensionCounts          map[string]int64        `json:"raw_extension_counts"`
}

func (c *Client) ListAIPs(ctx context.Context, index string, limit int, cursor string) (*AIPListResult, error) {
	if !c.Enabled() {
		return nil, nil
	}
	if strings.TrimSpace(index) == "" {
		index = "aipfiles"
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	searchAfter, err := decodeCursor(cursor)
	if err != nil {
		return nil, err
	}

	items := make([]AIPListItem, 0, limit)
	seen := map[string]struct{}{}
	var nextSort []any

	for page := 0; page < 10 && len(items) < limit; page++ {
		body := map[string]any{
			"size":    limit * 3,
			"_source": []string{"AIPUUID", "sipName"},
			"sort": []any{
				map[string]any{"AIPUUID": "asc"},
				map[string]any{"FILEUUID": "asc"},
			},
		}
		if len(searchAfter) > 0 {
			body["search_after"] = searchAfter
		}

		res, err := c.search(ctx, index, body)
		if err != nil {
			return nil, err
		}
		if len(res.Hits) == 0 {
			nextSort = nil
			break
		}

		for _, h := range res.Hits {
			aip := strings.TrimSpace(asString(h.Source["AIPUUID"]))
			if aip == "" {
				continue
			}
			if _, ok := seen[aip]; ok {
				continue
			}
			seen[aip] = struct{}{}
			items = append(items, AIPListItem{
				AIPUUID: aip,
				SIPName: strings.TrimSpace(asString(h.Source["sipName"])),
			})
			if len(items) >= limit {
				break
			}
		}

		nextSort = res.Hits[len(res.Hits)-1].Sort
		if len(nextSort) == 0 {
			break
		}
		searchAfter = nextSort
	}

	nextCursor := ""
	if len(nextSort) > 0 {
		nextCursor, _ = encodeCursor(nextSort)
	}

	return &AIPListResult{Items: items, NextCursor: nextCursor}, nil
}

func (c *Client) AIPStats(ctx context.Context, index, aipUUID string, pageSize int) (*AIPStats, error) {
	if !c.Enabled() {
		return nil, nil
	}
	if strings.TrimSpace(index) == "" {
		index = "aipfiles"
	}
	aipUUID = strings.TrimSpace(aipUUID)
	if aipUUID == "" {
		return nil, fmt.Errorf("aip uuid required")
	}
	if pageSize <= 0 {
		pageSize = 500
	}
	if pageSize > 2000 {
		pageSize = 2000
	}

	stats := &AIPStats{
		AIPUUID:            aipUUID,
		SIPNames:           []string{},
		LargestFiles:       []AIPFileSample{},
		RawExtensionCounts: map[string]int64{},
	}

	sipNames := map[string]struct{}{}
	fileUUIDs := map[string]struct{}{}
	normalizedIDs := map[string]struct{}{}
	statusCounts := map[string]int64{}
	originCounts := map[string]int64{}
	accessionCounts := map[string]int64{}
	isPartOfCounts := map[string]int64{}
	amVersionCounts := map[string]int64{}
	formatRegistryCounts := map[string]int64{}
	formatNameCounts := map[string]int64{}
	formatVersionCounts := map[string]int64{}
	premisTypeCounts := map[string]int64{}
	premisOutcomeCounts := map[string]int64{}
	premisToolCounts := map[string]int64{}
	formatSignatureSet := map[string]struct{}{}
	basenameCounts := map[string]int64{}
	lagSeconds := make([]int64, 0)
	var minIndexedAt time.Time
	var maxIndexedAt time.Time
	var minCreatedByApp time.Time
	var maxCreatedByApp time.Time
	var firstEvent time.Time
	var lastEvent time.Time
	var searchAfter []any

	for page := 0; page < 1000; page++ {
		body := map[string]any{
			"size": pageSize,
			"_source": []string{
				"AIPUUID", "sipName", "FILEUUID", "filePath", "fileExtension",
				"status", "indexedAt", "size", "FileSize",
				"origin", "accessionid", "isPartOf", "identifiers", "archivematicaVersion",
				"METS",
			},
			"query": map[string]any{
				"bool": map[string]any{
					"should": []any{
						map[string]any{"term": map[string]any{"AIPUUID.keyword": aipUUID}},
						map[string]any{"term": map[string]any{"AIPUUID": aipUUID}},
					},
					"minimum_should_match": 1,
				},
			},
			"sort": []any{
				map[string]any{"FILEUUID": "asc"},
				map[string]any{"filePath.raw": "asc"},
			},
		}
		if len(searchAfter) > 0 {
			body["search_after"] = searchAfter
		}

		res, err := c.search(ctx, index, body)
		if err != nil {
			return nil, err
		}
		if len(res.Hits) == 0 {
			break
		}

		for _, h := range res.Hits {
			src := h.Source
			stats.FilesTotal++

			if sip := strings.TrimSpace(asString(src["sipName"])); sip != "" {
				sipNames[sip] = struct{}{}
			}
			if st := strings.TrimSpace(asString(src["status"])); st != "" {
				statusCounts[st]++
			}
			if v := strings.TrimSpace(asString(src["origin"])); v != "" {
				originCounts[v]++
			}
			if v := strings.TrimSpace(asString(src["accessionid"])); v != "" {
				accessionCounts[v]++
			}
			for _, v := range stringsFromAny(src["isPartOf"]) {
				isPartOfCounts[v]++
			}
			if v := strings.TrimSpace(asString(src["archivematicaVersion"])); v != "" {
				amVersionCounts[v]++
			}
			if countAnyItems(src["identifiers"]) == 0 {
				stats.MissingIdentifiers++
			}
			if f := strings.TrimSpace(asString(src["FILEUUID"])); f != "" {
				fileUUIDs[f] = struct{}{}
			}

			ext := extFromPath(asString(src["filePath"]))
			stats.RawExtensionCounts[ext]++
			if base := baseFromPath(asString(src["filePath"])); base != "" {
				basenameCounts[strings.ToLower(base)]++
			}

			indexedAt := parseIndexedAt(src["indexedAt"])
			if !indexedAt.IsZero() {
				if minIndexedAt.IsZero() || indexedAt.Before(minIndexedAt) {
					minIndexedAt = indexedAt
				}
				if maxIndexedAt.IsZero() || indexedAt.After(maxIndexedAt) {
					maxIndexedAt = indexedAt
				}
			}

			size, regKey, formatName, formatVersion, createdByApp, eventDates, premisEvents := extractObjectMeta(src["METS"])
			if size <= 0 {
				size = int64(asFloat(src["size"]))
			}
			if size <= 0 {
				size = int64(asFloat(src["FileSize"]))
			}
			if size > 0 {
				stats.TotalBytes += size
				pushLargestFile(&stats.LargestFiles, AIPFileSample{
					FilePath:          strings.TrimSpace(asString(src["filePath"])),
					FileUUID:          strings.TrimSpace(asString(src["FILEUUID"])),
					Bytes:             size,
					Extension:         ext,
					FormatRegistryKey: regKey,
					FormatName:        formatName,
					Status:            strings.TrimSpace(asString(src["status"])),
					CreatedByAppDate:  toISO(createdByApp),
					IndexedAt:         toISO(indexedAt),
				}, 10)
			}
			if regKey != "" {
				formatRegistryCounts[regKey]++
			}
			if formatName != "" {
				formatNameCounts[formatName]++
			}
			if regKey == "" && formatName == "" {
				stats.UnknownFormats++
			}
			if regKey != "" || formatName != "" || formatVersion != "" {
				key := strings.TrimSpace(regKey) + "\x1f" + strings.TrimSpace(formatName) + "\x1f" + strings.TrimSpace(formatVersion)
				formatVersionCounts[key]++
				formatSignatureSet[key] = struct{}{}
			}
			if !createdByApp.IsZero() {
				if minCreatedByApp.IsZero() || createdByApp.Before(minCreatedByApp) {
					minCreatedByApp = createdByApp
				}
				if maxCreatedByApp.IsZero() || createdByApp.After(maxCreatedByApp) {
					maxCreatedByApp = createdByApp
				}
				if !indexedAt.IsZero() && indexedAt.After(createdByApp) {
					lagSeconds = append(lagSeconds, int64(indexedAt.Sub(createdByApp).Seconds()))
				}
			} else {
				stats.MissingCreatedByAppDate++
			}

			rawExt := strings.ToLower(strings.TrimSpace(asString(src["fileExtension"])))
			if rawExt == "" {
				rawExt = ext
			}
			if mismatchByFormat(regKey, formatName, rawExt) {
				stats.ExtensionFormatMismatch++
			}
			for _, t := range eventDates {
				if firstEvent.IsZero() || t.Before(firstEvent) {
					firstEvent = t
				}
				if lastEvent.IsZero() || t.After(lastEvent) {
					lastEvent = t
				}
			}
			for _, ev := range premisEvents {
				if ev.EventType != "" {
					premisTypeCounts[ev.EventType]++
				}
				if ev.EventOutcome != "" {
					premisOutcomeCounts[ev.EventOutcome]++
				}
				if ev.ToolVersion != "" {
					premisToolCounts[ev.ToolVersion]++
				}
			}

			rels := extractNormalizedObjectIDs(src["METS"])
			if len(rels) > 0 {
				stats.OriginalsWithNormalized++
				stats.NormalizedRefsTotal += int64(len(rels))
				for _, id := range rels {
					normalizedIDs[id] = struct{}{}
				}
			} else {
				stats.OriginalsWithoutNormalized++
			}
		}

		searchAfter = res.Hits[len(res.Hits)-1].Sort
		if len(searchAfter) == 0 {
			break
		}
	}

	stats.UniqueFileUUIDs = int64(len(fileUUIDs))
	stats.UniqueNormalizedObjectIDs = int64(len(normalizedIDs))
	if stats.FilesTotal > 0 {
		stats.NormalizedByOriginalAvg = round2(float64(stats.NormalizedRefsTotal) / float64(stats.FilesTotal))
		stats.AverageBytes = round2(float64(stats.TotalBytes) / float64(stats.FilesTotal))
		stats.FormatDiversityRatio = round2(float64(len(formatSignatureSet)) / float64(stats.FilesTotal))
	}
	stats.UniqueFormatSignatures = int64(len(formatSignatureSet))
	for _, n := range basenameCounts {
		if n > 1 {
			stats.DuplicateFilenameGroups++
			stats.DuplicateFilenameCandidates += n
		}
	}
	if len(lagSeconds) > 0 {
		sort.Slice(lagSeconds, func(i, j int) bool { return lagSeconds[i] < lagSeconds[j] })
		var total int64
		for _, s := range lagSeconds {
			total += s
		}
		stats.IndexedLagAvgSeconds = round2(float64(total) / float64(len(lagSeconds)))
		stats.IndexedLagP95Seconds = percentileInt64(lagSeconds, 95)
	}
	stats.MinIndexedAt = toISO(minIndexedAt)
	stats.MaxIndexedAt = toISO(maxIndexedAt)
	stats.MinCreatedByAppDate = toISO(minCreatedByApp)
	stats.MaxCreatedByAppDate = toISO(maxCreatedByApp)
	stats.FirstEventDate = toISO(firstEvent)
	stats.LastEventDate = toISO(lastEvent)

	stats.SIPNames = keysSorted(sipNames)
	stats.StatusCounts = topCounts(statusCounts, 20)
	stats.OriginCounts = topCounts(originCounts, 20)
	stats.AccessionIDCounts = topCounts(accessionCounts, 20)
	stats.IsPartOfCounts = topCounts(isPartOfCounts, 20)
	stats.ArchivematicaVersionCounts = topCounts(amVersionCounts, 20)
	stats.FormatRegistryCounts = topCounts(formatRegistryCounts, 50)
	stats.FormatNameCounts = topCounts(formatNameCounts, 50)
	stats.FormatVersionCounts = topFormatVersions(formatVersionCounts, 50)
	stats.PREMISEventTypeCounts = topCounts(premisTypeCounts, 50)
	stats.PREMISEventOutcomeCounts = topCounts(premisOutcomeCounts, 50)
	stats.PREMISToolCounts = topCounts(premisToolCounts, 50)
	stats.ExtensionCounts = topCounts(stats.RawExtensionCounts, 30)
	return stats, nil
}

type searchHit struct {
	ID     string         `json:"_id"`
	Sort   []any          `json:"sort"`
	Source map[string]any `json:"_source"`
}

type searchResponse struct {
	Hits []searchHit
}

func (c *Client) search(ctx context.Context, index string, body map[string]any) (*searchResponse, error) {
	blob, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	u := c.endpoint + "/" + strings.Trim(index, "/") + "/_search"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(blob)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("elasticsearch status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var raw struct {
		Hits struct {
			Hits []searchHit `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return &searchResponse{Hits: raw.Hits.Hits}, nil
}

func decodeCursor(cursor string) ([]any, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return nil, nil
	}
	blob, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	var out []any
	if err := json.Unmarshal(blob, &out); err != nil {
		return nil, fmt.Errorf("invalid cursor payload")
	}
	return out, nil
}

func encodeCursor(sortVals []any) (string, error) {
	blob, err := json.Marshal(sortVals)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(blob), nil
}

func extFromPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "unknown"
	}
	name := path
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.LastIndex(name, "\\"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.LastIndex(name, "."); i >= 0 && i+1 < len(name) {
		return strings.ToLower(strings.TrimSpace(name[i+1:]))
	}
	return "unknown"
}

func extractNormalizedObjectIDs(mets any) []string {
	root, ok := mets.(map[string]any)
	if !ok {
		return nil
	}

	amd := asMap(root["amdSec"])
	amdDict := asMap(amd["mets:amdSec_dict"])
	tech := asMap(amdDict["mets:techMD_dict"])
	wrap := asMap(tech["mets:mdWrap_dict"])
	xmlData := asMap(wrap["mets:xmlData_dict"])
	obj := asMap(xmlData["premis:object_dict"])
	rel := obj["premis:relationship_dict"]

	ids := map[string]struct{}{}
	for _, item := range asSlice(rel) {
		rm := asMap(item)
		idDict := asMap(rm["premis:relatedObjectIdentifier_dict"])
		id := strings.TrimSpace(asString(idDict["premis:relatedObjectIdentifierValue"]))
		if id != "" {
			ids[id] = struct{}{}
		}
	}

	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func extractObjectMeta(mets any) (int64, string, string, string, time.Time, []time.Time, []premisEventMeta) {
	root, ok := mets.(map[string]any)
	if !ok {
		return 0, "", "", "", time.Time{}, nil, nil
	}

	amd := asMap(root["amdSec"])
	amdDict := asMap(amd["mets:amdSec_dict"])
	tech := asMap(amdDict["mets:techMD_dict"])
	wrap := asMap(tech["mets:mdWrap_dict"])
	xmlData := asMap(wrap["mets:xmlData_dict"])
	obj := asMap(xmlData["premis:object_dict"])
	objChar := asMap(obj["premis:objectCharacteristics_dict"])

	size := int64(asFloat(objChar["premis:size"]))
	format := asMap(objChar["premis:format_dict"])
	designation := asMap(format["premis:formatDesignation_dict"])
	registry := asMap(format["premis:formatRegistry_dict"])
	formatName := strings.TrimSpace(asString(designation["premis:formatName"]))
	formatVersion := strings.TrimSpace(asString(designation["premis:formatVersion"]))
	regKey := strings.TrimSpace(asString(registry["premis:formatRegistryKey"]))
	creatingApp := asMap(objChar["premis:creatingApplication_dict"])
	createdByApp := parseISO(asString(creatingApp["premis:dateCreatedByApplication"]))

	eventDates := make([]time.Time, 0)
	premisEvents := make([]premisEventMeta, 0)
	for _, eventWrap := range asSlice(amdDict["mets:digiprovMD_dict"]) {
		evMap := asMap(eventWrap)
		mdWrap := asMap(evMap["mets:mdWrap_dict"])
		xml := asMap(mdWrap["mets:xmlData_dict"])
		ev := asMap(xml["premis:event_dict"])
		t := parseISO(asString(ev["premis:eventDateTime"]))
		if !t.IsZero() {
			eventDates = append(eventDates, t)
		}
		eventType := strings.TrimSpace(asString(ev["premis:eventType"]))
		eventOutcome := strings.TrimSpace(asString(asMap(ev["premis:eventOutcomeInformation_dict"])["premis:eventOutcome"]))
		eventDetail := strings.TrimSpace(asString(asMap(ev["premis:eventDetailInformation_dict"])["premis:eventDetail"]))
		toolVersion := parseToolVersion(eventDetail)
		if eventType != "" || eventOutcome != "" || toolVersion != "" {
			premisEvents = append(premisEvents, premisEventMeta{
				EventType:    eventType,
				EventOutcome: eventOutcome,
				ToolVersion:  toolVersion,
			})
		}
	}

	return size, regKey, formatName, formatVersion, createdByApp, eventDates, premisEvents
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asSlice(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case map[string]any:
		return []any{t}
	default:
		return nil
	}
}

func keysSorted(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func topCounts(m map[string]int64, limit int) []KeyCount {
	out := make([]KeyCount, 0, len(m))
	for k, v := range m {
		out = append(out, KeyCount{Key: k, Count: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Key < out[j].Key
		}
		return out[i].Count > out[j].Count
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func topFormatVersions(m map[string]int64, limit int) []AIPFormatVersionCount {
	out := make([]AIPFormatVersionCount, 0, len(m))
	for k, v := range m {
		parts := strings.SplitN(k, "\x1f", 3)
		item := AIPFormatVersionCount{Count: v}
		if len(parts) > 0 {
			item.FormatRegistryKey = parts[0]
		}
		if len(parts) > 1 {
			item.FormatName = parts[1]
		}
		if len(parts) > 2 {
			item.FormatVersion = parts[2]
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].FormatRegistryKey != out[j].FormatRegistryKey {
			return out[i].FormatRegistryKey < out[j].FormatRegistryKey
		}
		if out[i].FormatName != out[j].FormatName {
			return out[i].FormatName < out[j].FormatName
		}
		return out[i].FormatVersion < out[j].FormatVersion
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

func parseIndexedAt(v any) time.Time {
	f := asFloat(v)
	if f <= 0 {
		return time.Time{}
	}
	sec := int64(f)
	nsec := int64((f - float64(sec)) * float64(time.Second))
	return time.Unix(sec, nsec).UTC()
}

func parseISO(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999-07:00",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func toISO(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func pushLargestFile(dst *[]AIPFileSample, item AIPFileSample, limit int) {
	if limit <= 0 {
		return
	}
	*dst = append(*dst, item)
	sort.Slice(*dst, func(i, j int) bool {
		if (*dst)[i].Bytes == (*dst)[j].Bytes {
			return (*dst)[i].FilePath < (*dst)[j].FilePath
		}
		return (*dst)[i].Bytes > (*dst)[j].Bytes
	})
	if len(*dst) > limit {
		*dst = (*dst)[:limit]
	}
}

func stringsFromAny(v any) []string {
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return nil
		}
		return []string{s}
	case []any:
		out := make([]string, 0, len(t))
		for _, it := range t {
			s := strings.TrimSpace(asString(it))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func countAnyItems(v any) int {
	switch t := v.(type) {
	case []any:
		return len(t)
	case map[string]any:
		return len(t)
	case string:
		if strings.TrimSpace(t) == "" {
			return 0
		}
		return 1
	default:
		return 0
	}
}

func baseFromPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	name := path
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.LastIndex(name, "\\"); i >= 0 {
		name = name[i+1:]
	}
	return strings.TrimSpace(name)
}

func mismatchByFormat(regKey, formatName, ext string) bool {
	ext = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(ext)), ".")
	if ext == "" {
		return false
	}

	key := strings.ToLower(strings.TrimSpace(regKey))
	name := strings.ToLower(strings.TrimSpace(formatName))
	expected := map[string]struct{}{}
	add := func(values ...string) {
		for _, v := range values {
			v = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(v)), ".")
			if v != "" {
				expected[v] = struct{}{}
			}
		}
	}

	switch key {
	case "fmt/353":
		add("tif", "tiff")
	case "fmt/11":
		add("png")
	case "fmt/4":
		add("gif")
	case "fmt/91":
		add("svg")
	case "fmt/43":
		add("jpg", "jpeg")
	case "fmt/402":
		add("tga")
	case "x-fmt/392":
		add("jp2", "j2k", "jpf")
	}

	if len(expected) == 0 {
		if strings.Contains(name, "portable network graphics") {
			add("png")
		} else if strings.Contains(name, "graphics interchange format") {
			add("gif")
		} else if strings.Contains(name, "scalable vector graphics") {
			add("svg")
		} else if strings.Contains(name, "jpeg") {
			add("jpg", "jpeg")
		} else if strings.Contains(name, "tiff") {
			add("tif", "tiff")
		}
	}

	if len(expected) == 0 {
		return false
	}
	_, ok := expected[ext]
	return !ok
}

func percentileInt64(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := int(float64(len(sorted)-1) * float64(p) / 100.0)
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func parseToolVersion(detail string) string {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		return ""
	}
	program := ""
	version := ""
	for _, part := range strings.Split(detail, ";") {
		p := strings.TrimSpace(part)
		if strings.HasPrefix(p, "program=") {
			program = strings.Trim(strings.TrimPrefix(p, "program="), "\"")
		}
		if strings.HasPrefix(p, "version=") {
			version = strings.Trim(strings.TrimPrefix(p, "version="), "\"")
		}
	}
	if program == "" && version == "" {
		return ""
	}
	if version == "" {
		return program
	}
	if program == "" {
		return version
	}
	return program + " " + version
}
