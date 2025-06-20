CREATE TABLE IF NOT EXISTS {table_name}
(
    ticker String,
    timestamp DateTime('UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ticker, timestamp)
SETTINGS index_granularity = 8192;
