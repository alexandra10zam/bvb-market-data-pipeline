-- Layered warehouse: raw (as extracted) -> core (validated) -> marts (analytics-ready)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS raw.prices_daily (
    ticker       TEXT        NOT NULL,
    trade_date   DATE        NOT NULL,
    open         NUMERIC(18, 6),
    high         NUMERIC(18, 6),
    low          NUMERIC(18, 6),
    close        NUMERIC(18, 6),
    adj_close    NUMERIC(18, 6),
    volume       BIGINT,
    source       TEXT        NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS core.prices_daily (
    ticker      TEXT           NOT NULL,
    trade_date  DATE           NOT NULL,
    open        NUMERIC(18, 6) NOT NULL CHECK (open > 0),
    high        NUMERIC(18, 6) NOT NULL CHECK (high > 0),
    low         NUMERIC(18, 6) NOT NULL CHECK (low > 0 AND low <= high),
    close       NUMERIC(18, 6) NOT NULL CHECK (close > 0),
    adj_close   NUMERIC(18, 6) NOT NULL,
    volume      BIGINT         NOT NULL CHECK (volume >= 0),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS core.rejected_rows (
    run_id      BIGINT      NOT NULL,
    ticker      TEXT,
    trade_date  DATE,
    reason      TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS marts.daily_indicators (
    ticker          TEXT    NOT NULL,
    trade_date      DATE    NOT NULL,
    adj_close       NUMERIC(18, 6) NOT NULL,
    daily_return    DOUBLE PRECISION,
    sma_20          DOUBLE PRECISION,
    sma_50          DOUBLE PRECISION,
    sma_200         DOUBLE PRECISION,
    high_52w        DOUBLE PRECISION,
    drawdown_52w    DOUBLE PRECISION,
    volatility_20d  DOUBLE PRECISION,
    below_sma_200   BOOLEAN NOT NULL,
    drawdown_alert  BOOLEAN NOT NULL,
    golden_cross    BOOLEAN NOT NULL,
    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_indicators_alerts
    ON marts.daily_indicators (trade_date) WHERE drawdown_alert;

-- Latest snapshot per ticker: what a dashboard or alerting job reads
CREATE OR REPLACE VIEW marts.latest_snapshot AS
SELECT DISTINCT ON (ticker)
       ticker, trade_date, adj_close,
       round((daily_return * 100)::numeric, 2)   AS daily_return_pct,
       round((drawdown_52w * 100)::numeric, 2)   AS drawdown_52w_pct,
       round((volatility_20d * 100)::numeric, 2) AS volatility_20d_pct,
       below_sma_200, drawdown_alert
FROM marts.daily_indicators
ORDER BY ticker, trade_date DESC;

-- Monthly performance per ticker
CREATE OR REPLACE VIEW marts.monthly_returns AS
SELECT ticker,
       date_trunc('month', trade_date)::date AS month,
       round(((exp(sum(ln(1 + daily_return))) - 1) * 100)::numeric, 2) AS return_pct,
       count(*) AS trading_days
FROM marts.daily_indicators
WHERE daily_return IS NOT NULL AND daily_return > -1
GROUP BY ticker, date_trunc('month', trade_date);

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,
    window_start    DATE        NOT NULL,
    window_end      DATE        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running',
    rows_extracted  INT,
    rows_rejected   INT,
    rows_loaded     INT,
    error           TEXT
);
