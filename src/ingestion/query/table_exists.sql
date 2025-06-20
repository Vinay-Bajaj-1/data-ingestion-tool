SELECT count(*) AS table_count
FROM system.tables
WHERE database = '{database}'
  AND name = '{table_name}'
