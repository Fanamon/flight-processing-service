package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/IBM/sarama"
)

type SyncProducer struct {
	producer sarama.SyncProducer
	topic    string
}

type flightMessage struct {
	MetaID int `json:"meta_id"`
	model.FlightRequest
}

func NewSyncProducer(brokerAddrs []string, topic string) (*SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	sp, err := sarama.NewSyncProducer(brokerAddrs, config)
	if err != nil {
		return nil, err
	}

	return &SyncProducer{producer: sp, topic: topic}, nil
}

func (p *SyncProducer) Send(topic string, payload []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(payload),
	}

	_, _, err := p.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

func (p *SyncProducer) Close() error {
	return p.producer.Close()
}

func (p *SyncProducer) SendFlightMessage(metaID int, flightRequest *model.FlightRequest) error {
	payload, err := MarshalFlightMessage(metaID, flightRequest)
	if err != nil {
		return err
	}

	if err := p.Send(p.topic, payload); err != nil {
		return fmt.Errorf("send flight message: %w", err)
	}

	return nil
}

func MarshalFlightMessage(metaID int, request *model.FlightRequest) ([]byte, error) {
	if request == nil {
		return nil, fmt.Errorf("flight request is nil")
	}

	flightMsg := flightMessage{
		MetaID:        metaID,
		FlightRequest: *request,
	}

	payload, err := json.Marshal(flightMsg)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func UnmarshalFlightMessage(payload []byte) (int, *model.FlightRequest, error) {
	jsonMessage := &flightMessage{}
	if err := json.Unmarshal(payload, jsonMessage); err != nil {
		return 0, nil, err
	}

	return jsonMessage.MetaID, &jsonMessage.FlightRequest, nil
}
