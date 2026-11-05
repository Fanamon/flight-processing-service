package repository

import (
	"database/sql"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/model"
	"github.com/Masterminds/squirrel"
)

type MetaRepository struct {
	db *sql.DB
	sb squirrel.StatementBuilderType
}

func NewMetaRepository(db *sql.DB) *MetaRepository {
	return &MetaRepository{
		db: db,
		sb: squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
	}
}

func (repo *MetaRepository) Create(meta *model.FlightMeta) error {
	return repo.create(repo.db, meta)
}

func (repo *MetaRepository) CreateTx(tx *sql.Tx, meta *model.FlightMeta) error {
	return repo.create(tx, meta)
}

func (repo *MetaRepository) UpdateStatus(id int, status string) error {
	return repo.updateStatus(repo.db, id, status)
}

func (repo *MetaRepository) UpdateStatusTx(tx *sql.Tx, id int, status string) error {
	return repo.updateStatus(tx, id, status)
}

func (repo *MetaRepository) GetByFlightNumber(flightNumber string, status string, limit int, offset int) ([]*model.FlightMeta, int, error) {
	builder := repo.sb.
		Select("id", "flight_number", "departure_date", "status", "created_at", "processed_at").
		From("flight_meta").
		OrderBy("created_at DESC").
		Limit(uint64(limit)).
		Offset(uint64(offset))

	builder = filterByFlight(builder, flightNumber, status)

	rows, err := builder.RunWith(repo.db).Query()
	if err != nil {
		return nil, 0, err
	}

	defer rows.Close()

	items, err := scanFlightMeta(rows)
	if err != nil {
		return nil, 0, err
	}

	countQuery := repo.sb.
		Select("COUNT(*)").
		From("flight_meta")

	countQuery = filterByFlight(countQuery, flightNumber, status)

	total := 0
	err = countQuery.RunWith(repo.db).QueryRow().Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	return items, total, nil
}

func (repo *MetaRepository) create(runner squirrel.BaseRunner, meta *model.FlightMeta) error {
	now := time.Now().UTC()
	err := repo.sb.
		Insert("flight_meta").
		Columns("flight_number", "departure_date", "status", "created_at").
		Values(meta.FlightNumber, meta.DepartureDate, "pending", now).
		Suffix("RETURNING id").
		RunWith(runner).
		QueryRow().
		Scan(&meta.ID)
	if err != nil {
		return err
	}

	meta.Status = "pending"
	meta.CreatedAt = now

	return nil
}

func (repo *MetaRepository) updateStatus(runner squirrel.BaseRunner, id int, status string) error {
	now := time.Now().UTC()

	query, args, err := repo.sb.
		Update("flight_meta").
		Set("status", status).
		Set("processed_at", now).
		Where(squirrel.Eq{"id": id}).
		ToSql()
	if err != nil {
		return err
	}

	result, err := runner.Exec(query, args...)
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

func filterByFlight(builder squirrel.SelectBuilder, flightNumber, status string) squirrel.SelectBuilder {
	builder = builder.Where(squirrel.Eq{"flight_number": flightNumber})
	if status != "" {
		builder = builder.Where(squirrel.Eq{"status": status})
	}

	return builder
}

func scanFlightMeta(rows *sql.Rows) ([]*model.FlightMeta, error) {
	items := make([]*model.FlightMeta, 0)

	for rows.Next() {
		meta := &model.FlightMeta{}
		var processedAt sql.NullTime

		err := rows.Scan(
			&meta.ID,
			&meta.FlightNumber,
			&meta.DepartureDate,
			&meta.Status,
			&meta.CreatedAt,
			&processedAt,
		)
		if err != nil {
			return nil, err
		}

		if processedAt.Valid {
			meta.ProcessedAt = &processedAt.Time
		}

		items = append(items, meta)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}
