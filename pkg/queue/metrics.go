package queue

import "time"

// MetricsReporter defines the interface for reporting metrics.
type MetricsReporter interface {
	IncPublished()
	IncConsumed()
	IncError(op string)
	IncDLQ()
	ObserveLatency(op string, d time.Duration)
}

// NoopMetricsReporter is a no-op implementation of MetricsReporter.
type NoopMetricsReporter struct{}

func (m *NoopMetricsReporter) IncPublished()                             {}
func (m *NoopMetricsReporter) IncConsumed()                              {}
func (m *NoopMetricsReporter) IncError(op string)                        {}
func (m *NoopMetricsReporter) IncDLQ()                                   {}
func (m *NoopMetricsReporter) ObserveLatency(op string, d time.Duration) {}
