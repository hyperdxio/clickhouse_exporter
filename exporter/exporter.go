package exporter

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	namespace = "clickhouse" // For Prometheus metrics.
)

// Exporter collects clickhouse stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	metricsURI      string
	asyncMetricsURI string
	eventsURI       string
	partsURI        string
	disksMetricURI  string
	client          *http.Client

	scrapeFailures prometheus.Counter

	user     string
	password string
}

// NewExporter returns an initialized Exporter.
func NewExporter(uri url.URL, insecure bool, user, password string) *Exporter {
	q := uri.Query()
	metricsURI := uri
	q.Set("query", "select metric, value from system.metrics")
	metricsURI.RawQuery = q.Encode()

	asyncMetricsURI := uri
	q.Set("query", "select replaceRegexpAll(toString(metric), '-', '_') AS metric, value from system.asynchronous_metrics")
	asyncMetricsURI.RawQuery = q.Encode()

	eventsURI := uri
	q.Set("query", "select event, value from system.events")
	eventsURI.RawQuery = q.Encode()

	partsURI := uri
	q.Set("query", "select database, table, sum(bytes) as bytes, count() as parts, sum(rows) as rows from system.parts where active = 1 group by database, table")
	partsURI.RawQuery = q.Encode()

	disksMetricURI := uri
	q.Set("query", `select name, sum(free_space) as free_space_in_bytes, sum(total_space) as total_space_in_bytes from system.disks group by name`)
	disksMetricURI.RawQuery = q.Encode()

	return &Exporter{
		metricsURI:      metricsURI.String(),
		asyncMetricsURI: asyncMetricsURI.String(),
		eventsURI:       eventsURI.String(),
		partsURI:        partsURI.String(),
		disksMetricURI:  disksMetricURI.String(),
		scrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "",
			Name:      "exporter_scrape_failures_total",
			Help:      "Number of errors while scraping clickhouse.",
		}),
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			},
			Timeout: 30 * time.Second,
		},
		user:     user,
		password: password,
	}
}

// Describe describes all the metrics ever exported by the clickhouse exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from clickhouse. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	// Clear the registry at the start of each collection
	metricRegistry = make(map[string]bool)

	metrics, err := e.parseKeyValueResponse(e.metricsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.metricsURI, err)
	}

	for _, m := range metrics {
		name := metricName(m.key, MetricSourceSystem)
		// Skip if we already processed a metric with this name
		if _, exists := metricRegistry[name]; exists {
			continue
		}

		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			// Namespace is already included in the transformed metric name
			Namespace: "",
			Name:      name,
			Help:      "ClickHouse metric: " + m.key,
		}, []string{}).WithLabelValues()
		newMetric.Set(m.value)
		newMetric.Collect(ch)

		// Register this metric name
		metricRegistry[name] = true
	}

	asyncMetrics, err := e.parseKeyValueResponse(e.asyncMetricsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.asyncMetricsURI, err)
	}

	for _, am := range asyncMetrics {
		name := metricName(am.key, MetricSourceAsync)
		// Skip if we already processed a metric with this name
		if _, exists := metricRegistry[name]; exists {
			continue
		}

		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      name,
			Help:      "ClickHouse async metric: " + am.key,
		}, []string{}).WithLabelValues()
		newMetric.Set(am.value)
		newMetric.Collect(ch)

		// Register this metric name
		metricRegistry[name] = true
	}

	events, err := e.parseKeyValueResponse(e.eventsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.eventsURI, err)
	}

	for _, ev := range events {
		name := metricName(ev.key, MetricSourceEvents) + "_total"
		// Skip if we already processed a metric with this name
		if _, exists := metricRegistry[name]; exists {
			continue
		}

		newMetric, _ := prometheus.NewConstMetric(
			prometheus.NewDesc(
				name,
				"ClickHouse event metric: "+ev.key, []string{}, nil),
			prometheus.CounterValue, float64(ev.value))
		ch <- newMetric

		// Register this metric name
		metricRegistry[name] = true
	}

	parts, err := e.parsePartsResponse(e.partsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.partsURI, err)
	}

	// Since parts metrics have labels, we need a different approach to avoid duplicates
	// First, register the metric names without collecting them
	bytesName := metricName("table_parts_bytes", MetricSourceParts)
	countName := metricName("table_parts_count", MetricSourceParts)
	rowsName := metricName("table_parts_rows", MetricSourceParts)

	// Register these names if they haven't been seen yet
	if _, exists := metricRegistry[bytesName]; !exists {
		metricRegistry[bytesName] = true

		// Now collect the metrics for all parts
		for _, part := range parts {
			newBytesMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "",
				Name:      bytesName,
				Help:      "ClickHouse table size in bytes",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newBytesMetric.Set(float64(part.bytes))
			newBytesMetric.Collect(ch)
		}
	}

	if _, exists := metricRegistry[countName]; !exists {
		metricRegistry[countName] = true

		// Now collect the metrics for all parts
		for _, part := range parts {
			newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "",
				Name:      countName,
				Help:      "ClickHouse number of parts of the table",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newCountMetric.Set(float64(part.parts))
			newCountMetric.Collect(ch)
		}
	}

	if _, exists := metricRegistry[rowsName]; !exists {
		metricRegistry[rowsName] = true

		// Now collect the metrics for all parts
		for _, part := range parts {
			newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "",
				Name:      rowsName,
				Help:      "ClickHouse number of rows in the table",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newRowsMetric.Set(float64(part.rows))
			newRowsMetric.Collect(ch)
		}
	}

	disksMetrics, err := e.parseDiskResponse(e.disksMetricURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.disksMetricURI, err)
	}

	// Similar approach for disk metrics
	freeSpaceName := metricName("free_space_in_bytes", MetricSourceDisks)
	totalSpaceName := metricName("total_space_in_bytes", MetricSourceDisks)

	if _, exists := metricRegistry[freeSpaceName]; !exists {
		metricRegistry[freeSpaceName] = true

		for _, dm := range disksMetrics {
			newFreeSpaceMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "",
				Name:      freeSpaceName,
				Help:      "ClickHouse disks free space in bytes",
			}, []string{"disk"}).WithLabelValues(dm.disk)
			newFreeSpaceMetric.Set(dm.freeSpace)
			newFreeSpaceMetric.Collect(ch)
		}
	}

	if _, exists := metricRegistry[totalSpaceName]; !exists {
		metricRegistry[totalSpaceName] = true

		for _, dm := range disksMetrics {
			newTotalSpaceMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "",
				Name:      totalSpaceName,
				Help:      "ClickHouse disks total space in bytes",
			}, []string{"disk"}).WithLabelValues(dm.disk)
			newTotalSpaceMetric.Set(dm.totalSpace)
			newTotalSpaceMetric.Collect(ch)
		}
	}

	return nil
}

func (e *Exporter) handleResponse(uri string) ([]byte, error) {
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	if e.user != "" && e.password != "" {
		req.Header.Set("X-ClickHouse-User", e.user)
		req.Header.Set("X-ClickHouse-Key", e.password)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error scraping clickhouse: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Error().Err(err).Msg("can't close resp.Body")
		}
	}()

	data, err := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		if err != nil {
			data = []byte(err.Error())
		}
		return nil, fmt.Errorf("status %s (%d): %s", resp.Status, resp.StatusCode, data)
	}

	return data, nil
}

type lineResult struct {
	key   string
	value float64
}

func parseNumber(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (e *Exporter) parseKeyValueResponse(uri string) ([]lineResult, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results = make([]lineResult, 0)

	for i, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 2 {
			return nil, fmt.Errorf("parseKeyValueResponse: unexpected %d line: %s", i, line)
		}
		k := strings.TrimSpace(parts[0])
		v, err := parseNumber(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, err
		}
		results = append(results, lineResult{k, v})

	}
	return results, nil
}

type diskResult struct {
	disk       string
	freeSpace  float64
	totalSpace float64
}

func (e *Exporter) parseDiskResponse(uri string) ([]diskResult, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results = make([]diskResult, 0)

	for i, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 3 {
			return nil, fmt.Errorf("parseDiskResponse: unexpected %d line: %s", i, line)
		}
		disk := strings.TrimSpace(parts[0])

		freeSpace, err := parseNumber(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, err
		}

		totalSpace, err := parseNumber(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, err
		}

		results = append(results, diskResult{disk, freeSpace, totalSpace})

	}
	return results, nil
}

type partsResult struct {
	database string
	table    string
	bytes    int
	parts    int
	rows     int
}

func (e *Exporter) parsePartsResponse(uri string) ([]partsResult, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results = make([]partsResult, 0)

	for i, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 5 {
			return nil, fmt.Errorf("parsePartsResponse: unexpected %d line: %s", i, line)
		}
		database := strings.TrimSpace(parts[0])
		table := strings.TrimSpace(parts[1])

		bytes, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, err
		}

		count, err := strconv.Atoi(strings.TrimSpace(parts[3]))
		if err != nil {
			return nil, err
		}

		rows, err := strconv.Atoi(strings.TrimSpace(parts[4]))
		if err != nil {
			return nil, err
		}

		results = append(results, partsResult{database, table, bytes, count, rows})
	}

	return results, nil
}

// Collect fetches the stats from configured clickhouse location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	upValue := 1

	if err := e.collect(ch); err != nil {
		log.Info().Msgf("Error scraping clickhouse: %s", err)
		e.scrapeFailures.Inc()
		e.scrapeFailures.Collect(ch)

		upValue = 0
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "up"),
			"Was the last query of ClickHouse successful.",
			nil, nil,
		),
		prometheus.GaugeValue, float64(upValue),
	)

}

func metricName(in string, source MetricSource) string {
	// First convert to snake case and replace dots with underscores
	out := toSnake(in)
	out = strings.Replace(out, ".", "_", -1)

	// Then transform to appropriate format based on source
	transformed := toClickHouseMetricsFormat(out, source)

	// Remove redundant clickhouse_ prefix from final name
	if strings.HasPrefix(transformed, "clickhouse_") {
		transformed = transformed[len("clickhouse_"):]
	}

	return transformed
}

// MetricSource represents the source table of a ClickHouse metric
type MetricSource string

const (
	MetricSourceSystem       MetricSource = "system.metrics"
	MetricSourceAsync        MetricSource = "system.asynchronous_metrics"
	MetricSourceEvents       MetricSource = "system.events"
	MetricSourceParts        MetricSource = "system.parts"
	MetricSourceDisks        MetricSource = "system.disks"
	MetricSourceDefault      MetricSource = ""
)

// MetricRegistry ensures we don't create duplicate metrics with different help texts
var metricRegistry = make(map[string]bool)

// toClickHouseMetricsFormat converts snake_case metric names to CamelCase format with appropriate prefix
// Examples:
// - clickhouse_disk_s3_no_such_key_errors (from system.metrics) -> ClickHouseMetrics_DiskS3NoSuchKeyErrors
// - clickhouse_tcp_threads (from system.asynchronous_metrics) -> ClickHouseAsyncMetrics_TCPThreads
// - clickhouse_query_count (from system.events) -> ClickHouseEvents_QueryCount
func toClickHouseMetricsFormat(in string, source MetricSource) string {
	// Remove namespace prefix if present
	if strings.HasPrefix(in, namespace+"_") {
		in = in[len(namespace)+1:]
	}

	// Choose prefix based on source
	var prefix string
	switch source {
	case MetricSourceSystem:
		prefix = "ClickHouseMetrics_"
	case MetricSourceAsync:
		prefix = "ClickHouseAsyncMetrics_"
	case MetricSourceEvents:
		prefix = "ClickHouseEvents_"
	case MetricSourceParts:
		prefix = "ClickHouseParts_"
	case MetricSourceDisks:
		prefix = "ClickHouseDisks_"
	default:
		prefix = "ClickHouseMetrics_"
	}

	// Process special metric name patterns
	// Example: merge_tree_data_select_executor_threads_active -> MergeTreeBackgroundExecutorThreadsActive

	// Special case handling
	switch {
	case containsAllParts(in, "merge_tree_data_select_executor_threads_active"):
		return "ClickHouseMetrics_MergeTreeBackgroundExecutorThreadsActive"
	// Add more special cases here as needed
	}

	// Regular processing for other metric names
	parts := strings.Split(in, "_")
	for i, part := range parts {
		if len(part) > 0 {
			// Handle known compound terms
			switch {
			case i < len(parts)-1 && part == "merge" && parts[i+1] == "tree":
				parts[i] = "MergeTree"
				parts[i+1] = ""
				continue
			case i < len(parts)-1 && part == "back" && parts[i+1] == "ground":
				parts[i] = "Background"
				parts[i+1] = ""
				continue
			}

			// Handle common acronyms
			switch strings.ToLower(part) {
			case "tcp", "ip", "http", "https", "url", "uri", "ssl", "tls", "ssh", "dns", "s3", "io", "os":
				parts[i] = strings.ToUpper(part)
			default:
				parts[i] = strings.ToUpper(part[:1]) + part[1:]
			}
		}
	}

	// Join all parts, filtering out empty ones
	var result []string
	for _, part := range parts {
		if part != "" {
			result = append(result, part)
		}
	}

	return prefix + strings.Join(result, "")
}

// toSnake convert the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func toSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}

// Helper function to check if a string contains all parts of another string
func containsAllParts(s, parts string) bool {
	return strings.Contains(s, parts)
}

// check interface
var _ prometheus.Collector = (*Exporter)(nil)
