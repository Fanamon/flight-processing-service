package repository

import (
	"database/sql"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Masterminds/squirrel"
)

type OutboxRepository struct {
	db *sql.DB
	sb squirrel.StatementBuilderType
}

func NewOutboxRepository(db *sql.DB) *OutboxRepository {
	return &OutboxRepository{
		db: db,
		sb: squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
	}
}

func (repo *OutboxRepository) CreateMessage(tx *sql.Tx, message *model.OutboxMessage) error {
	now := time.Now().UTC()

	builder := repo.sb.
		Insert("outbox_messages").
		Columns("message_id", "topic", "payload", "status", "retry_count", "created_at").
		Values(message.MessageID, message.Topic, message.Payload, "pending", 0, now).
		Suffix("RETURNING id")

	query, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	err = tx.QueryRow(query, args...).Scan(&message.ID)
	if err != nil {
		return err
	}

	message.Status = "pending"
	message.RetryCount = 0
	message.CreatedAt = now

	return nil
}

func (repo *OutboxRepository) GetPendingMessages(limit int64) ([]*model.OutboxMessage, error) {
	builder := repo.sb.
		Select("id", "message_id", "topic", "payload", "status", "retry_count", "created_at", "sent_at", "last_error").
		From("outbox_messages").
		Where(squirrel.Eq{"status": "pending"}).
		OrderBy("created_at ASC").
		Limit(uint64(limit))

	rows, err := builder.RunWith(repo.db).Query()
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	messages, err := scanOutboxMessage(rows)
	if err != nil {
		return nil, err
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (repo *OutboxRepository) MarkAsSent(messageID string) error {
	builder := repo.sb.
		Update("outbox_messages").
		Set("status", "sent").
		Set("sent_at", time.Now().UTC()).
		Set("retry_count", 0).
		Where(squirrel.Eq{"message_id": messageID})

	result, err := builder.RunWith(repo.db).Exec()
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (repo *OutboxRepository) MarkAsFailed(messageID string, errorMsg string, maxRetries int64) error {
	builder := repo.sb.
		Update("outbox_messages").
		Set("retry_count", squirrel.Expr("retry_count + 1")).
		Set("last_error", errorMsg).
		Set("status", squirrel.Expr(
			"CASE WHEN retry_count + 1 >= ? THEN 'failed' ELSE 'pending' END",
			maxRetries,
		)).
		Where(squirrel.Eq{"message_id": messageID})

	result, err := builder.RunWith(repo.db).Exec()
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (repo *OutboxRepository) CleanupOldMessages(retentionDays int64) error {
	cutoffDate := time.Now().UTC().AddDate(0, 0, -int(retentionDays))

	builder := repo.sb.
		Delete("outbox_messages").
		Where(squirrel.Eq{"status": "sent"}).
		Where(squirrel.Lt{"created_at": cutoffDate})

	_, err := builder.RunWith(repo.db).Exec()
	if err != nil {
		return err
	}

	return nil
}

func scanOutboxMessage(rows *sql.Rows) ([]*model.OutboxMessage, error) {
	var messages []*model.OutboxMessage

	for rows.Next() {
		message := &model.OutboxMessage{}
		var sentAt sql.NullTime
		var lastError sql.NullString

		err := rows.Scan(
			&message.ID,
			&message.MessageID,
			&message.Topic,
			&message.Payload,
			&message.Status,
			&message.RetryCount,
			&message.CreatedAt,
			&sentAt,
			&lastError,
		)
		if err != nil {
			return nil, err
		}

		if sentAt.Valid {
			message.SentAt = &sentAt.Time
		}

		if lastError.Valid {
			message.LastError = &lastError.String
		}

		messages = append(messages, message)
	}

	return messages, nil
}
