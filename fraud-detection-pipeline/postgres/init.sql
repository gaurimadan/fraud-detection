CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    location TEXT NOT NULL,
    merchant TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    is_high_amount BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time
    ON transactions (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_customer_time
    ON transactions (customer_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_high_amount
    ON transactions (event_time DESC) WHERE is_high_amount;

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id TEXT PRIMARY KEY,
    alert_type TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    metric_value INTEGER NOT NULL,
    details JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_window
    ON fraud_alerts (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_customer
    ON fraud_alerts (customer_id, window_start DESC);
