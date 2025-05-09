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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
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
			Name:      "ClickHouseExporter_ScrapeFailuresTotal",
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
	metrics, err := e.parseKeyValueResponse(e.metricsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.metricsURI, err)
	}

	for _, m := range metrics {
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseMetrics_" + metricName(m.key),
			Help:      "Number of " + m.key + " currently processed",
		}, []string{}).WithLabelValues()
		newMetric.Set(m.value)
		newMetric.Collect(ch)
	}

	asyncMetrics, err := e.parseKeyValueResponse(e.asyncMetricsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.asyncMetricsURI, err)
	}

	for _, am := range asyncMetrics {
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseAsyncMetrics_" + metricName(am.key),
			Help:      "Number of " + am.key + " async processed",
		}, []string{}).WithLabelValues()
		newMetric.Set(am.value)
		newMetric.Collect(ch)
	}

	events, err := e.parseKeyValueResponse(e.eventsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.eventsURI, err)
	}

	for _, ev := range events {
		newMetric, _ := prometheus.NewConstMetric(
			prometheus.NewDesc(
				"ClickHouseEvents_"+metricName(ev.key)+"_total",
				"Number of "+ev.key+" total processed", []string{}, nil),
			prometheus.CounterValue, float64(ev.value))
		ch <- newMetric
	}

	parts, err := e.parsePartsResponse(e.partsURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.partsURI, err)
	}

	for _, part := range parts {
		newBytesMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseParts_TablePartsBytes",
			Help:      "Table size in bytes",
		}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
		newBytesMetric.Set(float64(part.bytes))
		newBytesMetric.Collect(ch)

		newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseParts_TablePartsCount",
			Help:      "Number of parts of the table",
		}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
		newCountMetric.Set(float64(part.parts))
		newCountMetric.Collect(ch)

		newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseParts_TablePartsRows",
			Help:      "Number of rows in the table",
		}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
		newRowsMetric.Set(float64(part.rows))
		newRowsMetric.Collect(ch)
	}

	disksMetrics, err := e.parseDiskResponse(e.disksMetricURI)
	if err != nil {
		return fmt.Errorf("error scraping clickhouse url %v: %v", e.disksMetricURI, err)
	}

	for _, dm := range disksMetrics {
		newFreeSpaceMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseDisks_FreeSpaceInBytes",
			Help:      "Disks free_space_in_bytes capacity",
		}, []string{"disk"}).WithLabelValues(dm.disk)
		newFreeSpaceMetric.Set(dm.freeSpace)
		newFreeSpaceMetric.Collect(ch)

		newTotalSpaceMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "",
			Name:      "ClickHouseDisks_TotalSpaceInBytes",
			Help:      "Disks total_space_in_bytes capacity",
		}, []string{"disk"}).WithLabelValues(dm.disk)
		newTotalSpaceMetric.Set(dm.totalSpace)
		newTotalSpaceMetric.Collect(ch)
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
			prometheus.BuildFQName("", "", "up"),
			"Was the last query of ClickHouse successful.",
			nil, nil,
		),
		prometheus.GaugeValue, float64(upValue),
	)

}

func metricName(in string) string {
	// Split by dots and underscores
	parts := strings.FieldsFunc(in, func(r rune) bool {
		return r == '.' || r == '_' || r == '-'
	})

	// Capitalize each part
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}

	// Join with underscore
	return strings.Join(parts, "_")
}

// check interface
var _ prometheus.Collector = (*Exporter)(nil)
