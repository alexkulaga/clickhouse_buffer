package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var _ Metrics = (*metrics)(nil)

type Metrics interface {
	ObserveDBLatency(tableName string, latency float64)
	ObserveBufferFlushLatency(tableName string, latency float64)
	IncBufferInsertCounter(tableName string)
	SetBufferLen(tableName string, bufferLen int64)
	Close()
}

type metrics struct {
	metricsList []prometheus.Collector
	//
	BufferInsertCounter    *prometheus.CounterVec
	BufferRowsFlushLatency *prometheus.HistogramVec
	BufferLen              *prometheus.GaugeVec
	DBInsertLatency        *prometheus.HistogramVec
}

func NewMetrics(subsystem string) Metrics {
	m := &metrics{}
	m.registerMetrics(subsystem)

	return m
}

func (m *metrics) registerMetrics(subsystem string) {
	m.BufferInsertCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "table_buffer_insert",
		},
		[]string{"table"},
	)

	m.BufferRowsFlushLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "table_buffer_rows_flush_latency",
			Buckets:   []float64{.1, .5, 1, 5, 10, 50, 100, 500, 1500, 5000, 30000},
		},
		[]string{"table"},
	)

	m.BufferLen = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "table_buffer_len",
		},
		[]string{"table"},
	)

	m.DBInsertLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "table_buffer_db_insert_latency",
			Buckets:   []float64{.1, .5, 1, 5, 10, 50, 100, 500, 1500, 5000, 30000},
		},
		[]string{"table"},
	)

	prometheus.MustRegister(m.BufferInsertCounter, m.BufferRowsFlushLatency, m.BufferLen, m.DBInsertLatency)
	m.metricsList = append(m.metricsList, m.BufferInsertCounter, m.BufferRowsFlushLatency, m.BufferLen, m.DBInsertLatency)
}

func (m *metrics) Close() {
	for _, metric := range m.metricsList {
		prometheus.Unregister(metric)
	}
}

func (m *metrics) IncBufferInsertCounter(tableName string) {
	m.BufferInsertCounter.WithLabelValues(tableName).Inc()
}

func (m *metrics) SetBufferLen(tableName string, bufferLen int64) {
	m.BufferLen.WithLabelValues(tableName).Set(float64(bufferLen))
}

func (m *metrics) ObserveDBLatency(tableName string, latency float64) {
	m.DBInsertLatency.WithLabelValues(tableName).Observe(latency)
}

func (m *metrics) ObserveBufferFlushLatency(tableName string, latency float64) {
	m.BufferRowsFlushLatency.WithLabelValues(tableName).Observe(latency)
}
