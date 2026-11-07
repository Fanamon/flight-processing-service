package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/metrics"
	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Fanamon/flight-processing-service/internal/service"
	"github.com/redis/go-redis/v9"
)

type FlightHandler struct {
	flightService     *service.FlightService
	redisClient       *redis.Client
	redisTimeDuration time.Duration
}

func NewFlightHandler(service *service.FlightService, redisClient *redis.Client, redisTimeDuration time.Duration) *FlightHandler {
	return &FlightHandler{
		flightService:     service,
		redisClient:       redisClient,
		redisTimeDuration: redisTimeDuration,
	}
}

func (h *FlightHandler) CreateFlight(w http.ResponseWriter, r *http.Request) {
	request, err := decodeFlightRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metaID, err := h.flightService.CreateFlight(request)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"id":             metaID,
		"flight_number":  request.FlightNumber,
		"departure_date": request.DepartureDate,
		"status":         "pending",
	})
}

func (h *FlightHandler) GetFlight(w http.ResponseWriter, r *http.Request) {
	flightNumber := r.URL.Query().Get("flight_number")
	if flightNumber == "" {
		http.Error(w, "flight_number is required", http.StatusBadRequest)
		return
	}

	departureDateStr := r.URL.Query().Get("departure_date")
	departureDate, err := time.Parse(time.RFC3339, departureDateStr)
	if err != nil {
		http.Error(w, "invalid departure_date format", http.StatusBadRequest)
		return
	}

	key := fmt.Sprintf("flight:data:%s:%s", flightNumber, departureDate.UTC().Format(time.RFC3339))
	metrics.RedisRequestsTotal.WithLabelValues("get").Inc()
	metrics.RedisKeyRequests.WithLabelValues(key).Inc()
	start := time.Now()
	redisValue, err := h.redisClient.Get(r.Context(), key).Result()
	metrics.RedisDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(redisValue))
		metrics.RedisHitsTotal.Inc()
		return
	}
	if errors.Is(err, redis.Nil) {
		metrics.RedisMissesTotal.Inc()
	} else {
		log.Printf("redis: %v", err)
		metrics.RedisErrorsTotal.WithLabelValues("get").Inc()
	}

	flightData, err := h.flightService.GetFlight(flightNumber, departureDate)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "flight not found", http.StatusNotFound)
		} else {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}

		return
	}

	payload, err := json.Marshal(map[string]any{
		"flight_number":    flightData.FlightNumber,
		"departure_date":   flightData.DepartureDate,
		"passengers_count": flightData.PassengersCount,
	})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	metrics.RedisRequestsTotal.WithLabelValues("set").Inc()
	start = time.Now()
	err = h.redisClient.Set(r.Context(), key, payload, h.redisTimeDuration).Err()
	metrics.RedisDuration.WithLabelValues("set").Observe(time.Since(start).Seconds())
	if err != nil {
		log.Printf("redis: %v", err)
		metrics.RedisErrorsTotal.WithLabelValues("set").Inc()
	}
	h.flightService.UpdateCacheSize(r.Context())

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}

func (h *FlightHandler) GetFlightMeta(w http.ResponseWriter, r *http.Request) {
	flightNumber := r.PathValue("flight_number")
	if flightNumber == "" {
		http.Error(w, "flight_number is required", http.StatusBadRequest)
		return
	}

	status, limit, err := parseMetaQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	key := fmt.Sprintf("flight:meta:%s:status=%s:limit=%d", flightNumber, status, limit)
	metrics.RedisRequestsTotal.WithLabelValues("get").Inc()
	metrics.RedisKeyRequests.WithLabelValues(key).Inc()
	start := time.Now()
	redisValue, err := h.redisClient.Get(r.Context(), key).Result()
	metrics.RedisDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(redisValue))
		metrics.RedisHitsTotal.Inc()
		return
	}
	if errors.Is(err, redis.Nil) {
		metrics.RedisMissesTotal.Inc()
	} else {
		log.Printf("redis: %v", err)
		metrics.RedisErrorsTotal.WithLabelValues("get").Inc()
	}

	metaResponse, err := h.flightService.GetFlightMeta(flightNumber, status, limit)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(metaResponse)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	metrics.RedisRequestsTotal.WithLabelValues("set").Inc()
	start = time.Now()
	err = h.redisClient.Set(r.Context(), key, payload, h.redisTimeDuration).Err()
	metrics.RedisDuration.WithLabelValues("set").Observe(time.Since(start).Seconds())
	if err != nil {
		log.Printf("redis: %v", err)
		metrics.RedisErrorsTotal.WithLabelValues("set").Inc()
	}
	h.flightService.UpdateCacheSize(r.Context())

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}

func (h *FlightHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /api/flights", metrics.ObserveHTTP(http.HandlerFunc(h.CreateFlight)))
	mux.Handle("GET /api/flights", metrics.ObserveHTTP(http.HandlerFunc(h.GetFlight)))
	mux.Handle("GET /api/flights/{flight_number}/meta", metrics.ObserveHTTP(http.HandlerFunc(h.GetFlightMeta)))
	mux.Handle("GET /metrics", metrics.Handler())

	return mux
}

func decodeFlightRequest(r *http.Request) (*model.FlightRequest, error) {
	var request model.FlightRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}

	if request.FlightNumber == "" || request.AircraftType == "" || request.PassengersCount < 0 || request.DepartureDate.IsZero() {
		return nil, fmt.Errorf("invalid flight request")
	}

	return &request, nil
}

func parseMetaQuery(r *http.Request) (string, int, error) {
	status := r.URL.Query().Get("status")
	if status != "" && (status != "pending" && status != "processed" && status != "error") {
		return "", 0, fmt.Errorf("invalid status value")
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed < 1 {
			return "", 0, fmt.Errorf("invalid limit value")
		}
		if parsed > 100 {
			parsed = 100
		}
		limit = parsed
	}

	return status, limit, nil
}
