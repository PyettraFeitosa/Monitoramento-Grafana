-- init/01_monitor_schema.sql

-- Conectar ao banco correto (caso o script rode solto, mas o docker já faz isso no DB 'checkout')
\c checkout;

CREATE TABLE IF NOT EXISTS transactions_metrics (
    time_bucket TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (time_bucket, status)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    metric VARCHAR(50),
    value_detected INT,
    threshold_limit DECIMAL(10,2),
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);