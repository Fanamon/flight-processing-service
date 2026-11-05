package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/kafka"
	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Fanamon/flight-processing-service/internal/repository"
	"github.com/google/uuid"
)

type FlightHandler struct {
	db               *sql.DB
	repositoryMeta   *repository.MetaRepository
	repositoryOutbox *repository.OutboxRepository
	repositoryFlight *repository.FlightRepository
	topic            string
}

func (h *FlightHandler) CreateFlight(w http.ResponseWriter, r *http.Request) {
	request, err := decodeFlightRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	meta, err := h.insertFlight(request)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"id":             meta.ID,
		"flight_number":  meta.FlightNumber,
		"departure_date": meta.DepartureDate,
		"status":         meta.Status,
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

	flightData, err := h.repositoryFlight.Get(flightNumber, departureDate)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "flight not found", http.StatusNotFound)
		} else {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"flight_number":    flightData.FlightNumber,
		"departure_date":   flightData.DepartureDate,
		"passengers_count": flightData.PassengersCount,
	})
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

	meta, total, err := h.repositoryMeta.GetByFlightNumber(flightNumber, status, limit, 0)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	metaResponse := createMetaResponse(meta)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"flight_number": flightNumber,
		"meta":          metaResponse,
		"pagination": map[string]any{
			"total": total,
			"limit": limit,
		},
	})
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

func (h *FlightHandler) insertFlight(request *model.FlightRequest) (*model.FlightMeta, error) {
	tx, err := h.db.Begin()
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	meta := &model.FlightMeta{
		FlightNumber:  request.FlightNumber,
		DepartureDate: request.DepartureDate,
	}
	if err := h.repositoryMeta.CreateTx(tx, meta); err != nil {
		return nil, err
	}

	payload, err := kafka.MarshalFlightMessage(meta.ID, request)
	if err != nil {
		return nil, err
	}

	outboxMessage := &model.OutboxMessage{
		MessageID: uuid.NewString(),
		Topic:     h.topic,
		Payload:   string(payload),
	}
	if err := h.repositoryOutbox.CreateMessage(tx, outboxMessage); err != nil {
		{
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return meta, nil
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

func createMetaResponse(items []*model.FlightMeta) []map[string]any {
	metaResponse := make([]map[string]any, len(items))
	for index, item := range items {
		metaResponse[index] = map[string]any{
			"id":             item.ID,
			"flight_number":  item.FlightNumber,
			"departure_date": item.DepartureDate,
			"status":         item.Status,
			"created_at":     item.CreatedAt,
			"processed_at":   item.ProcessedAt,
		}
	}

	return metaResponse
}
