CREATE TABLE IF NOT EXISTS checkout_sales_hourly (
  source_file TEXT,
  hour TEXT,
  today INT,
  yesterday INT,
  same_day_last_week INT,
  avg_last_week FLOAT,
  avg_last_month FLOAT
);

-- Importa checkout_1.csv
COPY checkout_sales_hourly (hour, today, yesterday, same_day_last_week, avg_last_week, avg_last_month)
FROM '/data/checkout_1.csv'
DELIMITER ','
CSV HEADER;

UPDATE checkout_sales_hourly
SET source_file = 'checkout_1'
WHERE source_file IS NULL;

-- Importa checkout_2.csv
COPY checkout_sales_hourly (hour, today, yesterday, same_day_last_week, avg_last_week, avg_last_month)
FROM '/data/checkout_2.csv'
DELIMITER ','
CSV HEADER;

UPDATE checkout_sales_hourly
SET source_file = 'checkout_2'
WHERE source_file IS NULL;
