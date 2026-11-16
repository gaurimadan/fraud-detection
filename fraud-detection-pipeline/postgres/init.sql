CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    location TEXT NOT NULL,
    merchant TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    step INTEGER NOT NULL DEFAULT 0,
    transaction_type TEXT NOT NULL DEFAULT 'PAYMENT',
    name_orig TEXT NOT NULL DEFAULT '',
    name_dest TEXT NOT NULL DEFAULT '',
    oldbalance_org DOUBLE PRECISION NOT NULL DEFAULT 0,
    newbalance_orig DOUBLE PRECISION NOT NULL DEFAULT 0,
    oldbalance_dest DOUBLE PRECISION NOT NULL DEFAULT 0,
    newbalance_dest DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_flagged_fraud BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_probability DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_fraud_predicted BOOLEAN NOT NULL DEFAULT FALSE,
    prediction_threshold DOUBLE PRECISION NOT NULL DEFAULT 0,
    model_version TEXT NOT NULL DEFAULT 'unknown',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time
    ON transactions (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_customer_time
    ON transactions (customer_id, event_time DESC);
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
