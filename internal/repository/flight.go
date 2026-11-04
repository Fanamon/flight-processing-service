package repository

import (
	"database/sql"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Masterminds/squirrel"
)

type FlightRepository struct {
	db *sql.DB
	sb squirrel.StatementBuilderType
}

func NewFlightRepository(db *sql.DB) *FlightRepository {
	return &FlightRepository{
		db: db,
		sb: squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
	}
}

func (repo *FlightRepository) Get(flightNumber string, departureDate time.Time) (*model.FlightData, error) {
	query, args, err := repo.sb.
		Select("aircraft_type", "flight_number", "departure_date", "arrival_date", "passengers_count", "updated_at").
		From("flights").
		Where(squirrel.Eq{"flight_number": flightNumber, "departure_date": departureDate}).
		ToSql()
	if err != nil {
		return nil, err
	}

	flight := &model.FlightData{}
	err = repo.db.QueryRow(query, args...).Scan(
		&flight.AircraftType,
		&flight.FlightNumber,
		&flight.DepartureDate,
		&flight.ArrivalDate,
		&flight.PassengersCount,
		&flight.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return flight, nil
}

func (repo *FlightRepository) Upsert(flight *model.FlightData) error {
	now := time.Now().UTC()

	builder := repo.sb.
		Insert("flights").
		Columns("flight_number", "departure_date", "aircraft_type", "arrival_date", "passengers_count", "updated_at").
		Values(flight.FlightNumber, flight.DepartureDate, flight.AircraftType, flight.ArrivalDate, flight.PassengersCount, now).
		Suffix(`ON CONFLICT (flight_number, departure_date) DO UPDATE SET
        aircraft_type = EXCLUDED.aircraft_type,
        arrival_date = EXCLUDED.arrival_date,
        passengers_count = EXCLUDED.passengers_count,
        updated_at = EXCLUDED.updated_at`)

	_, err := builder.RunWith(repo.db).Exec()
	if err != nil {
		return err
	}

	flight.UpdatedAt = now

	return nil
}
