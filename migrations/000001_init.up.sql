CREATE TABLE flight_meta (
    id SERIAL PRIMARY KEY,
    flight_number TEXT NOT NULL,
    departure_date TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ
);

CREATE TABLE flights (
    flight_number TEXT NOT NULL,
    departure_date TIMESTAMPTZ NOT NULL,
    aircraft_type TEXT NOT NULL,
    arrival_date TIMESTAMPTZ NOT NULL,
    passengers_count INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (flight_number, departure_date)
);

CREATE TABLE outbox_messages (
    id SERIAL PRIMARY KEY,
    message_id UUID NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    payload JSON NOT NULL,
    status TEXT NOT NULL,
    retry_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX outbox_messages_status_idx ON outbox_messages (status);
CREATE INDEX outbox_messages_created_at_idx ON outbox_messages (created_at);