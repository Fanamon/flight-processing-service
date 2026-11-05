package model

import "time"

type FlightRequest struct {
	AircraftType    string    `json:"aircraft_type"`
	FlightNumber    string    `json:"flight_number"`
	DepartureDate   time.Time `json:"departure_date"`
	ArrivalDate     time.Time `json:"arrival_date"`
	PassengersCount int       `json:"passengers_count"`
}

type FlightMeta struct {
	ID            int        `db:"id" json:"id"`
	FlightNumber  string     `db:"flight_number" json:"flight_number"`
	DepartureDate time.Time  `db:"departure_date" json:"departure_date"`
	Status        string     `db:"status" json:"status"` // pending, processed, error
	CreatedAt     time.Time  `db:"created_at" json:"created_at"`
	ProcessedAt   *time.Time `db:"processed_at" json:"processed_at"`
}

type FlightData struct {
	AircraftType    string    `db:"aircraft_type"`
	FlightNumber    string    `db:"flight_number"`
	DepartureDate   time.Time `db:"departure_date"`
	ArrivalDate     time.Time `db:"arrival_date"`
	PassengersCount int       `db:"passengers_count"`
	UpdatedAt       time.Time `db:"updated_at"`
}

type OutboxMessage struct {
	ID         int        `db:"id"`
	MessageID  string     `db:"message_id"` // UUID
	Topic      string     `db:"topic"`
	Payload    string     `db:"payload"` // JSON
	Status     string     `db:"status"`  // pending, sent, failed
	RetryCount int        `db:"retry_count"`
	CreatedAt  time.Time  `db:"created_at"`
	SentAt     *time.Time `db:"sent_at"`
	LastError  *string    `db:"last_error"`
}

type Pagination struct {
	Total int `json:"total"`
	Limit int `json:"limit"`
}

type FlightMetaResponse struct {
	FlightNumber string        `json:"flight_number"`
	Meta         []*FlightMeta `json:"meta"`
	Pagination   Pagination    `json:"pagination"`
}
