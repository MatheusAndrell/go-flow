package telemetry

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
)

// Metrics implements the recorder ports of the engine, worker, executions
// service and HTTP layer. Labels are bounded: workflow names, handler names
// and statuses, never ids or tenants.
type Metrics struct {
	Registry *prometheus.Registry

	executionsStarted  *prometheus.CounterVec
	executionsFinished *prometheus.CounterVec
	executionDuration  *prometheus.HistogramVec
	executionsActive   *prometheus.GaugeVec
	tasksFinished      *prometheus.CounterVec
	taskDuration       *prometheus.HistogramVec
	taskQueueWait      *prometheus.HistogramVec
	taskRetries        *prometheus.CounterVec
	tasksRetrying      prometheus.Gauge
	deadLetters        *prometheus.CounterVec
	leasesExpired      prometheus.Counter
	workerInFlight     *prometheus.GaugeVec
	workerCapacity     *prometheus.GaugeVec
	httpRequests       *prometheus.CounterVec
	httpDuration       *prometheus.HistogramVec
	rateLimited        prometheus.Counter
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	durations := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}
	m := &Metrics{
		Registry: reg,
		executionsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goflow_executions_started_total", Help: "Executions created by clients.",
		}, []string{"workflow"}),
		executionsFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goflow_executions_finished_total", Help: "Executions that reached a terminal status.",
		}, []string{"workflow", "status"}),
		executionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goflow_execution_duration_seconds", Help: "Time from start to terminal status.", Buckets: durations,
		}, []string{"workflow", "status"}),
		executionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goflow_executions_active", Help: "Non-terminal executions by status (sampled from the database).",
		}, []string{"status"}),
		tasksFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goflow_tasks_finished_total", Help: "Task attempts by outcome (succeeded, retried, failed).",
		}, []string{"handler", "kind", "outcome"}),
		taskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goflow_task_duration_seconds", Help: "Handler execution time per attempt.", Buckets: durations,
		}, []string{"handler"}),
		taskQueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goflow_task_queue_wait_seconds", Help: "Delay between a task becoming runnable and being claimed.", Buckets: durations,
		}, []string{"handler"}),
		taskRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goflow_task_retries_total", Help: "Retries scheduled after failed attempts.",
		}, []string{"handler"}),
		tasksRetrying: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "goflow_tasks_retry_scheduled", Help: "Tasks waiting for a retry (sampled from the database).",
		}),
		deadLetters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goflow_dead_letters_total", Help: "Tasks moved to the dead letter queue.",
		}, []string{"handler"}),
		leasesExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goflow_task_leases_expired_total", Help: "Tasks recovered after their worker stopped heartbeating.",
		}),
		workerInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goflow_worker_in_flight_tasks", Help: "Tasks currently executing on this worker.",
		}, []string{"worker"}),
		workerCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goflow_worker_capacity", Help: "Configured concurrency of this worker.",
		}, []string{"worker"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goflow_http_requests_total", Help: "HTTP requests by route and status code.",
		}, []string{"method", "route", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goflow_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		rateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goflow_http_rate_limited_total", Help: "Requests rejected by the rate limiter.",
		}),
	}
	reg.MustRegister(m.executionsStarted, m.executionsFinished, m.executionDuration, m.executionsActive, m.tasksFinished,
		m.taskDuration, m.taskQueueWait, m.taskRetries, m.tasksRetrying, m.deadLetters, m.leasesExpired,
		m.workerInFlight, m.workerCapacity, m.httpRequests, m.httpDuration, m.rateLimited)
	return m
}

func (m *Metrics) ExecutionStarted(workflow string) {
	m.executionsStarted.WithLabelValues(workflow).Inc()
}

func (m *Metrics) ExecutionFinished(workflow string, status execution.Status, d time.Duration) {
	m.executionsFinished.WithLabelValues(workflow, string(status)).Inc()
	m.executionDuration.WithLabelValues(workflow, string(status)).Observe(d.Seconds())
}

func (m *Metrics) TaskFinished(handler string, kind execution.TaskKind, outcome string) {
	m.tasksFinished.WithLabelValues(handler, string(kind), outcome).Inc()
}

func (m *Metrics) TaskRetried(handler string)      { m.taskRetries.WithLabelValues(handler).Inc() }
func (m *Metrics) TaskDeadLettered(handler string) { m.deadLetters.WithLabelValues(handler).Inc() }
func (m *Metrics) LeaseExpired()                   { m.leasesExpired.Inc() }

func (m *Metrics) TaskClaimed(handler string, wait time.Duration) {
	m.taskQueueWait.WithLabelValues(handler).Observe(max(wait, 0).Seconds())
}

func (m *Metrics) TaskExecuted(handler string, d time.Duration) {
	m.taskDuration.WithLabelValues(handler).Observe(d.Seconds())
}

func (m *Metrics) WorkerInFlight(worker string, n int) {
	m.workerInFlight.WithLabelValues(worker).Set(float64(n))
}
func (m *Metrics) WorkerCapacity(worker string, n int) {
	m.workerCapacity.WithLabelValues(worker).Set(float64(n))
}

func (m *Metrics) HTTPRequest(method, route string, code int, d time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(code)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

func (m *Metrics) RateLimited() { m.rateLimited.Inc() }

// CountSource provides database-derived gauges.
type CountSource interface {
	Counts(ctx context.Context) (map[execution.Status]int64, int64, error)
}

// StatsCollector samples gauges that are only knowable from the database.
type StatsCollector struct {
	metrics *Metrics
	source  CountSource
}

func NewStatsCollector(m *Metrics, source CountSource) *StatsCollector {
	return &StatsCollector{metrics: m, source: source}
}

func (c *StatsCollector) Collect(ctx context.Context) error {
	counts, retrying, err := c.source.Counts(ctx)
	if err != nil {
		return err
	}
	for _, s := range execution.AllStatuses {
		if !s.Terminal() {
			c.metrics.executionsActive.WithLabelValues(string(s)).Set(float64(counts[s]))
		}
	}
	c.metrics.tasksRetrying.Set(float64(retrying))
	return nil
}
