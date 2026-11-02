package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBDSN               string
	KafkaBrokers        string
	KafkaTopic          string
	KafkaGroupID        string
	HTTPPort            string
	OutboxPollInterval  time.Duration
	OutboxBatchSize     int64
	OutboxMaxRetries    int64
	OutboxRetentionDays int64
}

func Load() (Config, error) {
	cfg := Config{}

	fieldsStrings := []struct {
		key string
		dst *string
	}{
		{"DB_DSN", &cfg.DBDSN},
		{"KAFKA_BROKERS", &cfg.KafkaBrokers},
		{"KAFKA_TOPIC", &cfg.KafkaTopic},
		{"KAFKA_GROUP_ID", &cfg.KafkaGroupID},
		{"HTTP_PORT", &cfg.HTTPPort},
	}

	fieldsTimeDuration := []struct {
		key string
		dst *time.Duration
	}{
		{"OUTBOX_POLL_INTERVAL", &cfg.OutboxPollInterval},
	}

	fieldsInt64 := []struct {
		key string
		dst *int64
	}{
		{"OUTBOX_BATCH_SIZE", &cfg.OutboxBatchSize},
		{"OUTBOX_MAX_RETRIES", &cfg.OutboxMaxRetries},
		{"OUTBOX_RETENTION_DAYS", &cfg.OutboxRetentionDays},
	}

	for _, field := range fieldsStrings {
		value, err := ParseEnvString(field.key)
		if err != nil {
			return Config{}, err
		}

		*field.dst = value
	}

	for _, field := range fieldsTimeDuration {
		value, err := ParseEnvTimeDuration(field.key)
		if err != nil {
			return Config{}, err
		}

		*field.dst = value
	}

	for _, field := range fieldsInt64 {
		value, err := ParseEnvInt64(field.key)
		if err != nil {
			return Config{}, err
		}

		*field.dst = value
	}

	return cfg, nil
}

func ParseEnvString(key string) (string, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return "", fmt.Errorf("%s is required", key)
	}

	return raw, nil
}

func ParseEnvTimeDuration(key string) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, fmt.Errorf("%s is required", key)
	}

	interval, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}

	return interval, nil
}

func ParseEnvInt64(key string) (int64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, fmt.Errorf("%s is required", key)
	}

	number, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}

	return number, nil
}
