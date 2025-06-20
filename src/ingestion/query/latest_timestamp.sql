SELECT max(timestamp) AS latest_timestamp
FROM stock_ohlcv
WHERE ticker = '{ticker}'
