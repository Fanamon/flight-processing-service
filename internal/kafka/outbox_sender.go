package kafka

import (
	"context"
	"log"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/metrics"
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

func NewOutboxSender(repo *repository.OutboxRepository, producer *SyncProducer, poll time.Duration,
	batchSize int64, maxRetries int64, retentionDays int64,
) *OutboxSender {
	return &OutboxSender{
		repository:          repo,
		producer:            producer,
		OutboxPollInterval:  poll,
		OutboxBatchSize:     batchSize,
		OutboxMaxRetries:    maxRetries,
		OutboxRetentionDays: retentionDays,
	}
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
				start := time.Now()

				if err := os.producer.Send(message.Topic, []byte(message.Payload)); err != nil {
					if markErr := os.repository.MarkAsFailed(message.MessageID, err.Error(), os.OutboxMaxRetries); markErr != nil {
						log.Printf("mark outbox failed: %v", markErr)
					}

					metrics.KafkaErrors.WithLabelValues("send").Inc()
					metrics.OutboxMessagesFailedTotal.Inc()
					metrics.OutboxRetryCount.Inc()

					continue
				}

				metrics.KafkaMessagesSent.Inc()
				metrics.OutboxMessagesSentTotal.Inc()

				if markErr := os.repository.MarkAsSent(message.MessageID); markErr != nil {
					log.Printf("mark outbox sent: %v", markErr)
				}

				metrics.OutboxProcessingDuration.Observe(time.Since(start).Seconds())
				metrics.OutboxLagSeconds.Observe(time.Since(message.CreatedAt).Seconds())
			}

			pending, sent, failed, err := os.repository.CollectMessagesCount()
			if err != nil {
				log.Printf("collect messages count: %v", err)
			} else {
				metrics.OutboxMessagesTotal.WithLabelValues("pending").Set(float64(pending))
				metrics.OutboxMessagesTotal.WithLabelValues("sent").Set(float64(sent))
				metrics.OutboxMessagesTotal.WithLabelValues("failed").Set(float64(failed))
				metrics.OutboxPendingCount.Set(float64(pending))
			}

			if time.Now().After(nextCleanup) {
				_ = os.repository.CleanupOldMessages(os.OutboxRetentionDays)
				nextCleanup = time.Now().Add(time.Hour)
			}
		}
	}
}
