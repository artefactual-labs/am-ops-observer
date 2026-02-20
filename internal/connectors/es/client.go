package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Hit is a compact Elasticsearch hit summary.
type Hit struct {
	Index  string         `json:"index"`
	ID     string         `json:"id"`
	Score  float64        `json:"score"`
	Source map[string]any `json:"source"`
}

// Result is a transfer UUID lookup response from Elasticsearch.
type Result struct {
	TransferUUID string `json:"transfer_uuid"`
	TookMs       int64  `json:"took_ms"`
	TotalHits    int64  `json:"total_hits"`
	Hits         []Hit  `json:"hits"`
}

// ServiceStats holds lightweight cluster health and uptime data.
type ServiceStats struct {
	PingMS            int64    `json:"ping_ms"`
	Version           string   `json:"version"`
	ClusterName       string   `json:"cluster_name"`
	ClusterStatus     string   `json:"cluster_status"`
	NodeCount         int      `json:"node_count"`
	DataNodeCount     int      `json:"data_node_count"`
	ActiveShards      int      `json:"active_shards"`
	UnassignedShards  int      `json:"unassigned_shards"`
	PendingTasks      int      `json:"pending_tasks"`
	NodeUptimeSeconds int64    `json:"node_uptime_seconds"`
	NodeNames         []string `json:"node_names"`
}

// Client performs lightweight ES queries by transfer UUID.
type Client struct {
	endpoint string
	http     *http.Client
}

func NewClient(endpoint string, timeout time.Duration) *Client {
	return &Client{
		endpoint: strings.TrimRight(strings.TrimSpace(endpoint), "/"),
		http:     &http.Client{Timeout: timeout},
	}
}

func (c *Client) Enabled() bool {
	return c != nil && c.endpoint != ""
}

func (c *Client) SearchTransfer(ctx context.Context, transferUUID string, limit int) (*Result, error) {
	if !c.Enabled() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 5
	}

	query := map[string]any{
		"size": limit,
		"query": map[string]any{
			"bool": map[string]any{
				"should": []any{
					map[string]any{"term": map[string]any{"transferUUID.keyword": transferUUID}},
					map[string]any{"term": map[string]any{"transferUUID": transferUUID}},
					map[string]any{"term": map[string]any{"SIPUUID.keyword": transferUUID}},
					map[string]any{"term": map[string]any{"SIPUUID": transferUUID}},
					map[string]any{"term": map[string]any{"AIPUUID.keyword": transferUUID}},
					map[string]any{"term": map[string]any{"AIPUUID": transferUUID}},
					map[string]any{"query_string": map[string]any{"query": fmt.Sprintf("\"%s\"", transferUUID)}},
				},
				"minimum_should_match": 1,
			},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(c.endpoint + "/_search")
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
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
		blob, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("elasticsearch status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(blob)))
	}

	var raw struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Index  string         `json:"_index"`
				ID     string         `json:"_id"`
				Score  float64        `json:"_score"`
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	out := &Result{
		TransferUUID: transferUUID,
		TookMs:       raw.Took,
		TotalHits:    raw.Hits.Total.Value,
		Hits:         make([]Hit, 0, len(raw.Hits.Hits)),
	}

	for _, h := range raw.Hits.Hits {
		out.Hits = append(out.Hits, Hit{
			Index:  h.Index,
			ID:     h.ID,
			Score:  h.Score,
			Source: h.Source,
		})
	}

	return out, nil
}

// ServiceStats returns ES reachability, cluster health and node uptime summary.
func (c *Client) ServiceStats(ctx context.Context) (*ServiceStats, error) {
	if !c.Enabled() {
		return nil, nil
	}

	start := time.Now()
	root, err := c.getJSON(ctx, "/")
	if err != nil {
		return nil, err
	}
	pingMS := time.Since(start).Milliseconds()

	health, err := c.getJSON(ctx, "/_cluster/health")
	if err != nil {
		return nil, err
	}

	nodes, err := c.getJSON(ctx, "/_nodes/stats/jvm")
	if err != nil {
		return nil, err
	}

	out := &ServiceStats{
		PingMS:           pingMS,
		Version:          nestedString(root, "version", "number"),
		ClusterName:      asString(health["cluster_name"]),
		ClusterStatus:    asString(health["status"]),
		NodeCount:        int(asFloat(health["number_of_nodes"])),
		DataNodeCount:    int(asFloat(health["number_of_data_nodes"])),
		ActiveShards:     int(asFloat(health["active_shards"])),
		UnassignedShards: int(asFloat(health["unassigned_shards"])),
		PendingTasks:     int(asFloat(health["number_of_pending_tasks"])),
	}

	nodeNames := make([]string, 0)
	maxUptime := int64(0)
	if rawNodes, ok := nodes["nodes"].(map[string]any); ok {
		for _, rv := range rawNodes {
			node, ok := rv.(map[string]any)
			if !ok {
				continue
			}
			if name := asString(node["name"]); name != "" {
				nodeNames = append(nodeNames, name)
			}
			uptimeMS := int64(asFloat(nestedAny(node, "jvm", "uptime_in_millis")))
			if uptimeMS > maxUptime {
				maxUptime = uptimeMS
			}
		}
	}
	sort.Strings(nodeNames)
	out.NodeNames = nodeNames
	out.NodeUptimeSeconds = maxUptime / 1000

	return out, nil
}

func (c *Client) getJSON(ctx context.Context, path string) (map[string]any, error) {
	u, err := url.Parse(c.endpoint + path)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		blob, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("elasticsearch status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(blob)))
	}

	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func nestedAny(m map[string]any, keys ...string) any {
	cur := any(m)
	for _, k := range keys {
		nextMap, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = nextMap[k]
	}
	return cur
}

func nestedString(m map[string]any, keys ...string) string {
	return asString(nestedAny(m, keys...))
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}
