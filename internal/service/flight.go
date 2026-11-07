package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/kafka"
	"github.com/Fanamon/flight-processing-service/internal/metrics"
	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Fanamon/flight-processing-service/internal/repository"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type FlightService struct {
	db               *sql.DB
	repositoryMeta   *repository.MetaRepository
	repositoryOutbox *repository.OutboxRepository
	repositoryFlight *repository.FlightRepository
	redisClient      *redis.Client
	topic            string
}

func NewFlightService(db *sql.DB, meta *repository.MetaRepository, outbox *repository.OutboxRepository,
	flight *repository.FlightRepository, redisClient *redis.Client, topic string,
) *FlightService {
	return &FlightService{
		db:               db,
		repositoryMeta:   meta,
		repositoryOutbox: outbox,
		repositoryFlight: flight,
		redisClient:      redisClient,
		topic:            topic,
	}
}

func (s *FlightService) CreateFlight(request *model.FlightRequest) (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()

	meta := &model.FlightMeta{
		FlightNumber:  request.FlightNumber,
		DepartureDate: request.DepartureDate,
	}
	if err := s.repositoryMeta.CreateTx(tx, meta); err != nil {
		return 0, err
	}

	payload, err := kafka.MarshalFlightMessage(meta.ID, request)
	if err != nil {
		return 0, err
	}

	outboxMessage := &model.OutboxMessage{
		MessageID: uuid.NewString(),
		Topic:     s.topic,
		Payload:   string(payload),
	}
	if err := s.repositoryOutbox.CreateMessage(tx, outboxMessage); err != nil {
		{
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	pending, processed, errorCount, err := s.repositoryMeta.CollectMessagesCount()
	if err != nil {
		log.Printf("collect messages count: %v", err)
	} else {
		metrics.FlightMetaStatus.WithLabelValues("pending").Set(float64(pending))
		metrics.FlightMetaStatus.WithLabelValues("processed").Set(float64(processed))
		metrics.FlightMetaStatus.WithLabelValues("error").Set(float64(errorCount))
	}

	return meta.ID, nil
}

func (s *FlightService) ProcessFlightMessage(message []byte) error {
	metaID, flightRequest, err := kafka.UnmarshalFlightMessage(message)
	if err != nil {
		metrics.KafkaErrors.WithLabelValues("process").Inc()
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		metrics.KafkaErrors.WithLabelValues("process").Inc()
		return err
	}

	defer tx.Rollback()

	flight := &model.FlightData{
		AircraftType:    flightRequest.AircraftType,
		FlightNumber:    flightRequest.FlightNumber,
		DepartureDate:   flightRequest.DepartureDate,
		ArrivalDate:     flightRequest.ArrivalDate,
		PassengersCount: flightRequest.PassengersCount,
	}
	if err := s.repositoryFlight.UpsertTx(tx, flight); err != nil {
		metrics.KafkaErrors.WithLabelValues("process").Inc()
		return err
	}

	if err := s.repositoryMeta.UpdateStatusTx(tx, metaID, "processed"); err != nil {
		metrics.KafkaErrors.WithLabelValues("process").Inc()
		return err
	}

	if err := tx.Commit(); err != nil {
		metrics.KafkaErrors.WithLabelValues("process").Inc()
		return err
	}

	dataKey := fmt.Sprintf("flight:data:%s:%s", flightRequest.FlightNumber, flightRequest.DepartureDate.UTC().Format(time.RFC3339))
	metrics.RedisRequestsTotal.WithLabelValues("delete").Inc()
	start := time.Now()
	err = s.redisClient.Del(context.Background(), dataKey).Err()
	metrics.RedisDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	if err != nil {
		log.Printf("redis: %v", err)
		metrics.RedisErrorsTotal.WithLabelValues("delete").Inc()
	}
	pattern := fmt.Sprintf("flight:meta:%s:*", flightRequest.FlightNumber)
	start = time.Now()
	iter := s.redisClient.Scan(context.Background(), 0, pattern, 0).Iterator()
	metrics.RedisDuration.WithLabelValues("scan").Observe(time.Since(start).Seconds())
	for iter.Next(context.Background()) {
		metrics.RedisRequestsTotal.WithLabelValues("delete").Inc()
		start = time.Now()
		err := s.redisClient.Del(context.Background(), iter.Val()).Err()
		metrics.RedisDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
		if err != nil {
			log.Printf("redis: %v", err)
			metrics.RedisErrorsTotal.WithLabelValues("delete").Inc()
		}
	}
	if err := iter.Err(); err != nil {
		log.Printf("redis: %v", err)
	}
	s.UpdateCacheSize(context.Background())

	pending, processed, errorCount, err := s.repositoryMeta.CollectMessagesCount()
	if err != nil {
		log.Printf("collect messages count: %v", err)
	} else {
		metrics.FlightMetaStatus.WithLabelValues("pending").Set(float64(pending))
		metrics.FlightMetaStatus.WithLabelValues("processed").Set(float64(processed))
		metrics.FlightMetaStatus.WithLabelValues("error").Set(float64(errorCount))
	}

	metrics.KafkaMessagesProcessed.Inc()
	metrics.FlightsProcessed.Inc()
	metrics.Passengers.Observe(float64(flightRequest.PassengersCount))
	metrics.AircraftTypeCount.WithLabelValues(flightRequest.AircraftType).Inc()

	return nil
}

func (s *FlightService) GetFlight(flightNumber string, departureDate time.Time) (*model.FlightData, error) {
	return s.repositoryFlight.Get(flightNumber, departureDate)
}

func (s *FlightService) GetFlightMeta(flightNumber string, status string, limit int) (*model.FlightMetaResponse, error) {
	meta, total, err := s.repositoryMeta.GetByFlightNumber(flightNumber, status, limit, 0)
	if err != nil {
		return nil, err
	}

	return &model.FlightMetaResponse{
		FlightNumber: flightNumber,
		Meta:         meta,
		Pagination: model.Pagination{
			Total: total,
			Limit: limit,
		},
	}, nil
}

func (s *FlightService) UpdateCacheSize(ctx context.Context) {
	start := time.Now()
	info, err := s.redisClient.Info(ctx, "memory").Result()
	metrics.RedisDuration.WithLabelValues("info").Observe(time.Since(start).Seconds())
	if err != nil {
		log.Printf("redis: %v", err)
		return
	}

	for _, line := range strings.Split(info, "\n") {
		if !strings.HasPrefix(line, "used_memory:") {
			continue
		}

		raw := strings.TrimSpace(strings.TrimPrefix(line, "used_memory:"))
		size, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			log.Printf("redis: %v", err)
			return
		}

		metrics.RedisCacheSize.Set(size)
		return
	}
}
