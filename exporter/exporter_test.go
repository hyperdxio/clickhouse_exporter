package exporter

import (
	"net/url"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestScrape(t *testing.T) {
	clickhouseUrl, err := url.Parse("http://127.0.0.1:8123/")
	if err != nil {
		t.Fatal(err)
	}
	exporter := NewExporter(*clickhouseUrl, false, "", "")

	t.Run("Describe", func(t *testing.T) {
		ch := make(chan *prometheus.Desc)
		go func() {
			exporter.Describe(ch)
			close(ch)
		}()

		for range ch {
		}
	})

	t.Run("Collect", func(t *testing.T) {
		ch := make(chan prometheus.Metric)
		var err error
		go func() {
			err = exporter.collect(ch)
			if err != nil {
				panic("failed")
			}
			close(ch)
		}()

		for range ch {
		}
	})
}

func TestParseNumber(t *testing.T) {
	type testCase struct {
		in  string
		out float64
	}

	testCases := []testCase{
		{in: "1", out: 1},
		{in: "1.1", out: 1.1},
	}

	for _, tc := range testCases {
		out, err := parseNumber(tc.in)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if out != tc.out {
			t.Fatalf("wrong output: %f, expected %f", out, tc.out)
		}
	}
}

func TestToClickHouseMetricsFormat(t *testing.T) {
	type testCase struct {
		in     string
		source MetricSource
		out    string
	}

	const namespace = "clickhouse"
	testCases := []testCase{
		// System metrics
		{in: "clickhouse_disk_s3_no_such_key_errors", source: MetricSourceSystem, out: "ClickHouseMetrics_DiskS3NoSuchKeyErrors"},
		{in: "clickhouse_http_connections", source: MetricSourceSystem, out: "ClickHouseMetrics_HTTPConnections"},
		{in: "disk_s3_no_such_key_errors", source: MetricSourceSystem, out: "ClickHouseMetrics_DiskS3NoSuchKeyErrors"},
		{in: "memory_usage", source: MetricSourceSystem, out: "ClickHouseMetrics_MemoryUsage"},
		{in: "merge_tree_data_select_executor_threads_active", source: MetricSourceSystem, out: "ClickHouseMetrics_MergeTreeBackgroundExecutorThreadsActive"},

		// Async metrics
		{in: "clickhouse_tcp_threads", source: MetricSourceAsync, out: "ClickHouseAsyncMetrics_TCPThreads"},
		{in: "clickhouse_tcp_connection", source: MetricSourceSystem, out: "ClickHouseMetrics_TCPConnection"},
		{in: "os_idle_time", source: MetricSourceAsync, out: "ClickHouseAsyncMetrics_OSIdleTime"},
		{in: "asynchronous_metrics", source: MetricSourceAsync, out: "ClickHouseAsyncMetrics_AsynchronousMetrics"},

		// Events metrics
		{in: "clickhouse_query_count", source: MetricSourceEvents, out: "ClickHouseEvents_QueryCount"},
		{in: "read_compressed_bytes", source: MetricSourceEvents, out: "ClickHouseEvents_ReadCompressedBytes"},

		// Parts metrics
		{in: "table_parts_bytes", source: MetricSourceParts, out: "ClickHouseParts_TablePartsBytes"},

		// Disks metrics
		{in: "clickhouse_total_space_in_bytes", source: MetricSourceDisks, out: "ClickHouseDisks_TotalSpaceInBytes"},
		{in: "free_space_in_bytes", source: MetricSourceDisks, out: "ClickHouseDisks_FreeSpaceInBytes"},

		// Default metrics (no specific source)
		{in: "some_unknown_metric", source: MetricSourceDefault, out: "ClickHouseMetrics_SomeUnknownMetric"},
	}

	for _, tc := range testCases {
		out := toClickHouseMetricsFormat(tc.in, tc.source)
		if out != tc.out {
			t.Fatalf("wrong output for %s from %s: got %s, expected %s", tc.in, tc.source, out, tc.out)
		}
	}
}
