-- Refreshed by the stock_report DAG every time stocks.daily_prices is updated (data-aware scheduling).
-- Per symbol over the last 365 days of data: return, annualized volatility, maximum drawdown, last close vs
-- its 50-day moving average. All window functions.
CREATE TABLE IF NOT EXISTS stocks.symbol_report (
    symbol             text PRIMARY KEY,
    first_date         date,
    last_date          date,
    last_close         numeric(14, 4),
    return_pct         numeric(8, 2),
    volatility_pct     numeric(8, 2),
    max_drawdown_pct   numeric(8, 2),
    above_ma50         boolean,
    computed_at        timestamptz NOT NULL DEFAULT now()
);

WITH prices AS (
    SELECT symbol, trade_date, adj_close
    FROM stocks.daily_prices
    -- The 365 days up to the latest loaded date (not today): reproducible, and still meaningful on a
    -- weekend or after a failed run.
    WHERE trade_date > (SELECT max(trade_date) FROM stocks.daily_prices) - 365
),
enriched AS (
    SELECT *,
           adj_close / lag(adj_close) OVER w - 1                                    AS daily_return,
           max(adj_close) OVER (PARTITION BY symbol ORDER BY trade_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   AS running_peak,
           avg(adj_close) OVER (PARTITION BY symbol ORDER BY trade_date
                                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)          AS ma50,
           row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC)         AS recency
    FROM prices
    WINDOW w AS (PARTITION BY symbol ORDER BY trade_date)
),
per_symbol AS (
    SELECT symbol,
           min(trade_date)                                                         AS first_date,
           max(trade_date)                                                         AS last_date,
           max(adj_close) FILTER (WHERE recency = 1)                               AS last_close,
           100 * ((array_agg(adj_close ORDER BY trade_date DESC))[1]
                  / (array_agg(adj_close ORDER BY trade_date))[1] - 1)             AS return_pct,
           100 * stddev_samp(daily_return) * sqrt(252)                             AS volatility_pct,
           100 * min(adj_close / running_peak - 1)                                 AS max_drawdown_pct,
           bool_or(adj_close > ma50) FILTER (WHERE recency = 1)                    AS above_ma50
    FROM enriched
    GROUP BY symbol
)
INSERT INTO stocks.symbol_report (symbol, first_date, last_date, last_close, return_pct, volatility_pct,
                                  max_drawdown_pct, above_ma50)
SELECT * FROM per_symbol
ON CONFLICT (symbol) DO UPDATE SET
    first_date = EXCLUDED.first_date, last_date = EXCLUDED.last_date, last_close = EXCLUDED.last_close,
    return_pct = EXCLUDED.return_pct, volatility_pct = EXCLUDED.volatility_pct,
    max_drawdown_pct = EXCLUDED.max_drawdown_pct, above_ma50 = EXCLUDED.above_ma50, computed_at = now();
