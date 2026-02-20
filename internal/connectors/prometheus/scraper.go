package prometheus

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Point is a chart-ready value at a specific timestamp.
type Point struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// LiveSnapshot is a one-shot scrape summary for a target.
type LiveSnapshot struct {
	Target      string             `json:"target"`
	ScrapedAt   time.Time          `json:"scraped_at"`
	SampleCount int                `json:"sample_count"`
	Metrics     map[string]float64 `json:"metrics"`
}

// TargetStatus is a single target probe used by service-status dashboards.
type TargetStatus struct {
	Target        string             `json:"target"`
	OK            bool               `json:"ok"`
	Error         string             `json:"error,omitempty"`
	PingMS        int64              `json:"ping_ms"`
	ScrapedAt     time.Time          `json:"scraped_at"`
	SampleCount   int                `json:"sample_count"`
	UptimeSeconds int64              `json:"uptime_seconds"`
	CPUSeconds    float64            `json:"cpu_seconds"`
	CPUPercent    float64            `json:"cpu_percent"`
	MemoryMB      float64            `json:"memory_mb"`
	Goroutines    int64              `json:"goroutines"`
	Metrics       map[string]float64 `json:"metrics,omitempty"`
}

type historyKey struct {
	target string
	metric string
}

// Scraper reads Prometheus text exposition from one or more targets.
type Scraper struct {
	client    *http.Client
	targets   []string
	maxPoints int

	mu      sync.RWMutex
	history map[historyKey][]Point
}

func NewScraper(targets []string, timeout time.Duration, maxPoints int) *Scraper {
	if maxPoints <= 0 {
		maxPoints = 720
	}
	clean := make([]string, 0, len(targets))
	for _, t := range targets {
		t = strings.TrimSpace(t)
		if t != "" {
			clean = append(clean, t)
		}
	}
	return &Scraper{
		client:    &http.Client{Timeout: timeout},
		targets:   clean,
		maxPoints: maxPoints,
		history:   make(map[historyKey][]Point),
	}
}

func (s *Scraper) Enabled() bool {
	return s != nil && len(s.targets) > 0
}

func (s *Scraper) Targets() []string {
	if s == nil {
		return nil
	}
	out := make([]string, len(s.targets))
	copy(out, s.targets)
	return out
}

// Scrape pulls each target and aggregates metrics by metric name.
func (s *Scraper) Scrape(ctx context.Context, matchPrefix string) ([]LiveSnapshot, error) {
	if !s.Enabled() {
		return nil, nil
	}

	now := time.Now().UTC()
	prefix := strings.TrimSpace(matchPrefix)
	items := make([]LiveSnapshot, 0, len(s.targets))

	for _, target := range s.targets {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}

		resp, err := s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("scrape %s: %w", target, err)
		}

		agg, count, err := parseAndAggregate(resp.Body, prefix)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", target, err)
		}

		s.record(target, now, agg)
		items = append(items, LiveSnapshot{
			Target:      target,
			ScrapedAt:   now,
			SampleCount: count,
			Metrics:     agg,
		})
	}

	return items, nil
}

// Series returns in-memory history for one metric/target since cutoff.
func (s *Scraper) Series(target, metric string, since time.Time) []Point {
	if s == nil {
		return nil
	}
	k := historyKey{target: target, metric: metric}

	s.mu.RLock()
	points := append([]Point(nil), s.history[k]...)
	s.mu.RUnlock()

	if since.IsZero() {
		return points
	}

	out := make([]Point, 0, len(points))
	for _, p := range points {
		if !p.Timestamp.Before(since) {
			out = append(out, p)
		}
	}
	return out
}

// KnownMetrics returns sorted metric names seen for a target.
func (s *Scraper) KnownMetrics(target string) []string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	uniq := map[string]struct{}{}
	for k := range s.history {
		if k.target == target {
			uniq[k.metric] = struct{}{}
		}
	}
	out := make([]string, 0, len(uniq))
	for m := range uniq {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func (s *Scraper) record(target string, ts time.Time, metrics map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for metric, v := range metrics {
		k := historyKey{target: target, metric: metric}
		pts := append(s.history[k], Point{Timestamp: ts, Value: v})
		if len(pts) > s.maxPoints {
			pts = pts[len(pts)-s.maxPoints:]
		}
		s.history[k] = pts
	}
}

func parseAndAggregate(r io.Reader, matchPrefix string) (map[string]float64, int, error) {
	s := bufio.NewScanner(r)
	agg := map[string]float64{}
	samples := 0

	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, value, ok := parseLine(line)
		if !ok {
			continue
		}
		if matchPrefix != "" && !strings.HasPrefix(name, matchPrefix) {
			continue
		}

		agg[name] += value
		samples++
	}
	if err := s.Err(); err != nil {
		return nil, 0, err
	}
	return agg, samples, nil
}

func parseLine(line string) (string, float64, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", 0, false
	}
	nameField := fields[0]
	name := nameField
	if i := strings.IndexByte(nameField, '{'); i >= 0 {
		name = nameField[:i]
	}
	if name == "" {
		return "", 0, false
	}

	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return "", 0, false
	}
	return name, v, true
}

// ProbeTargets checks all targets independently, returning per-target status.
func (s *Scraper) ProbeTargets(ctx context.Context, metrics []string) []TargetStatus {
	if !s.Enabled() {
		return nil
	}

	now := time.Now().UTC()
	out := make([]TargetStatus, 0, len(s.targets))
	for _, target := range s.targets {
		item := TargetStatus{
			Target:    target,
			ScrapedAt: now,
		}

		start := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			item.Error = err.Error()
			out = append(out, item)
			continue
		}

		resp, err := s.client.Do(req)
		item.PingMS = time.Since(start).Milliseconds()
		if err != nil {
			item.Error = err.Error()
			out = append(out, item)
			continue
		}

		samples, sampleCount, err := parseMetricSamples(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			item.Error = err.Error()
			out = append(out, item)
			continue
		}

		item.OK = true
		item.SampleCount = sampleCount
		item.Metrics = make(map[string]float64, len(metrics))
		for _, m := range metrics {
			if v, ok := samples[m]; ok {
				item.Metrics[m] = v
			}
		}

		if startSec, ok := samples["process_start_time_seconds"]; ok && startSec > 0 {
			item.UptimeSeconds = int64(now.Sub(time.Unix(int64(startSec), 0)).Seconds())
		}
		if cpuSec, ok := samples["process_cpu_seconds_total"]; ok {
			item.CPUSeconds = cpuSec
			if item.UptimeSeconds > 0 {
				// Average CPU utilization of one core over process lifetime.
				item.CPUPercent = (cpuSec / float64(item.UptimeSeconds)) * 100.0
			}
		}
		if rss, ok := samples["process_resident_memory_bytes"]; ok && rss > 0 {
			item.MemoryMB = rss / 1024.0 / 1024.0
		}
		if gs, ok := samples["go_goroutines"]; ok && gs >= 0 {
			item.Goroutines = int64(gs)
		}

		out = append(out, item)
	}

	return out
}

func parseMetricSamples(r io.Reader) (map[string]float64, int, error) {
	s := bufio.NewScanner(r)
	samples := map[string]float64{}
	count := 0

	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, value, ok := parseLine(line)
		if !ok {
			continue
		}
		if _, exists := samples[name]; !exists {
			samples[name] = value
		}
		count++
	}
	if err := s.Err(); err != nil {
		return nil, 0, err
	}
	return samples, count, nil
}
