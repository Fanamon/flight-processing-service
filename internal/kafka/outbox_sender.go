package kafka

import (
	"context"
	"log"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/repository"
)

type OutboxSender struct {
	repository          *repository.OutboxRepository
	producer            *SyncProducer
	OutboxPollInterval  time.Duration
	OutboxBatchSize     int64
	OutboxMaxRetries    int64
	OutboxRetentionDays int64
}

func (os *OutboxSender) Run(ctx context.Context) error {
	ticker := time.NewTicker(os.OutboxPollInterval)

	defer ticker.Stop()

	nextCleanup := time.Now().Add(time.Hour)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			outboxMessages, err := os.repository.GetPendingMessages(os.OutboxBatchSize)
			if err != nil {
				return err
			}

			for _, message := range outboxMessages {
				if err := os.producer.Send(message.Topic, []byte(message.Payload)); err != nil {
					if markErr := os.repository.MarkAsFailed(message.MessageID, err.Error(), os.OutboxMaxRetries); markErr != nil {
						log.Printf("mark outbox failed: %v", markErr)
					}

					continue
				}

				if markErr := os.repository.MarkAsSent(message.MessageID); markErr != nil {
					log.Printf("mark outbox sent: %v", markErr)
				}
			}

			if time.Now().After(nextCleanup) {
				_ = os.repository.CleanupOldMessages(os.OutboxRetentionDays)
				nextCleanup = time.Now().Add(time.Hour)
			}
		}
	}
}
