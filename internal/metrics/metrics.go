package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)
	HTTPDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// Kafka metrics
	KafkaMessagesSent = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_messages_sent_total",
			Help: "Total number of Kafka messages sent",
		},
	)
	KafkaMessagesProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_messages_processed_total",
			Help: "Total number of Kafka messages processed",
		},
	)
	KafkaErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_errors_total",
			Help: "Total number of Kafka errors",
		},
		[]string{"operation"},
	)
	KafkaConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Lag of Kafka consumer",
		},
		[]string{"topic", "partition"},
	)

	// Business metrics
	FlightsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "flights_processed_total",
			Help: "Total number of flights processed",
		},
	)
	Passengers = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "flight_passengers",
			Help:    "Distribution of passengers per flight",
			Buckets: []float64{50, 100, 150, 200, 300, 500},
		},
	)
	AircraftTypeCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flights_by_aircraft_type_total",
			Help: "Count of different aircraft types",
		},
		[]string{"aircraft_type"},
	)
	FlightMetaStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flight_meta_status",
			Help: "Status of flight metadata",
		},
		[]string{"status"},
	)

	// Outbox metrics
	OutboxMessagesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "outbox_messages",
			Help: "Total number of messages in the outbox",
		},
		[]string{"status"},
	)
	OutboxMessagesSentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_messages_sent_total",
			Help: "Total number of messages sent from the outbox",
		},
	)
	OutboxMessagesFailedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_messages_failed_total",
			Help: "Total number of messages that failed to be sent from the outbox",
		},
	)
	OutboxProcessingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "outbox_processing_duration_seconds",
			Help:    "Duration of outbox message processing in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
	OutboxRetryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_retries_total",
			Help: "Number of times messages in the outbox have been retried",
		},
	)
	OutboxLagSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "outbox_lag_seconds",
			Help:    "Lag of messages in the outbox in seconds",
			Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
	)
	OutboxPendingCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_pending_count",
			Help: "Number of pending messages in the outbox",
		},
	)

	// Redis metrics
	RedisRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_requests_total",
			Help: "Total number of Redis requests",
		},
		[]string{"operation"}, // get, set, delete
	)
	RedisHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)
	RedisMissesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)
	RedisErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_errors_total",
			Help: "Total number of Redis errors",
		},
		[]string{"operation"},
	)
	RedisCacheSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cache_size_bytes",
			Help: "Approximate size of Redis cache (if available)",
		},
	)
	RedisDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_request_duration_seconds",
			Help:    "Duration of Redis requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
	RedisKeyRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_key_requests_total",
			Help: "Total number of Redis key lookups",
		},
		[]string{"key"},
	)
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func Handler() http.Handler {
	return promhttp.Handler()
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func ObserveHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		end := time.Since(start).Seconds()

		HTTPRequests.WithLabelValues(r.Method, r.Pattern, strconv.Itoa(sw.status)).Inc()
		HTTPDuration.WithLabelValues(r.Method, r.Pattern).Observe(end)
	})
}

func init() {
	prometheus.MustRegister(HTTPRequests, HTTPDuration, KafkaMessagesSent, KafkaMessagesProcessed, KafkaErrors,
		KafkaConsumerLag, FlightsProcessed, Passengers, AircraftTypeCount, FlightMetaStatus, OutboxMessagesTotal,
		OutboxMessagesSentTotal, OutboxMessagesFailedTotal, OutboxProcessingDuration, OutboxRetryCount, OutboxLagSeconds,
		OutboxPendingCount, RedisRequestsTotal, RedisHitsTotal, RedisMissesTotal, RedisErrorsTotal, RedisCacheSize,
		RedisDuration, RedisKeyRequests)
}
