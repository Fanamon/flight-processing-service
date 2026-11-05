package kafka

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Fanamon/flight-processing-service/internal/repository"
	"github.com/IBM/sarama"
)

type ConsumerGroup struct {
	db               *sql.DB
	repositoryFlight *repository.FlightRepository
	repositoryMeta   *repository.MetaRepository
	group            sarama.ConsumerGroup
	topic            string
}

func NewConsumerGroup(brokerAddrs []string, groupID string, topic string, db *sql.DB, repositoryFlight *repository.FlightRepository, repositoryMeta *repository.MetaRepository) (*ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	group, err := sarama.NewConsumerGroup(brokerAddrs, groupID, config)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroup{
		db:               db,
		repositoryFlight: repositoryFlight,
		repositoryMeta:   repositoryMeta,
		group:            group,
		topic:            topic,
	}, nil
}

func (cg *ConsumerGroup) Run(ctx context.Context) error {
	for {
		err := cg.group.Consume(ctx, []string{cg.topic}, cg)
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (cg *ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (cg *ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (cg *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := cg.handle(msg.Value); err != nil {
			return err
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

func (cg *ConsumerGroup) Close() error {
	return cg.group.Close()
}

func (cg *ConsumerGroup) handle(message []byte) error {
	jsonMessage := &flightMessage{}
	if err := json.Unmarshal(message, jsonMessage); err != nil {
		return err
	}

	tx, err := cg.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	flight := &model.FlightData{
		AircraftType:    jsonMessage.AircraftType,
		FlightNumber:    jsonMessage.FlightNumber,
		DepartureDate:   jsonMessage.DepartureDate,
		ArrivalDate:     jsonMessage.ArrivalDate,
		PassengersCount: jsonMessage.PassengersCount,
	}
	if err := cg.repositoryFlight.UpsertTx(tx, flight); err != nil {
		return err
	}

	if err := cg.repositoryMeta.UpdateStatusTx(tx, jsonMessage.MetaID, "processed"); err != nil {
		return err
	}

	return tx.Commit()
}
