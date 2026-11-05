package kafka

import (
	"context"
	"strconv"

	"github.com/Fanamon/flight-processing-service/internal/metrics"
	"github.com/IBM/sarama"
)

type ConsumerGroup struct {
	flightProcessor FlightProcessor
	group           sarama.ConsumerGroup
	topic           string
}

type FlightProcessor interface {
	ProcessFlightMessage(message []byte) error
}

func NewConsumerGroup(brokerAddrs []string, groupID string, topic string, flightProcessor FlightProcessor) (*ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	group, err := sarama.NewConsumerGroup(brokerAddrs, groupID, config)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroup{
		flightProcessor: flightProcessor,
		group:           group,
		topic:           topic,
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
		lag := claim.HighWaterMarkOffset() - msg.Offset - 1
		metrics.KafkaConsumerLag.WithLabelValues(msg.Topic, strconv.Itoa(int(msg.Partition))).Set(float64(lag))

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
	return cg.flightProcessor.ProcessFlightMessage(message)
}
