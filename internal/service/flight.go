package service

import (
	"database/sql"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/kafka"
	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Fanamon/flight-processing-service/internal/repository"
	"github.com/google/uuid"
)

type FlightService struct {
	db               *sql.DB
	repositoryMeta   *repository.MetaRepository
	repositoryOutbox *repository.OutboxRepository
	repositoryFlight *repository.FlightRepository
	topic            string
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

	return meta.ID, nil
}

func (s *FlightService) ProcessFlightMessage(message []byte) error {
	metaID, flightRequest, err := kafka.UnmarshalFlightMessage(message)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
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
		return err
	}

	if err := s.repositoryMeta.UpdateStatusTx(tx, metaID, "processed"); err != nil {
		return err
	}

	return tx.Commit()
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
