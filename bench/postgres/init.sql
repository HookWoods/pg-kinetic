CREATE TABLE IF NOT EXISTS accounts (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    balance_cents BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO accounts (email, balance_cents)
VALUES
    ('alice@example.com', 1000),
    ('bob@example.com', 2000)
ON CONFLICT (email) DO NOTHING;
