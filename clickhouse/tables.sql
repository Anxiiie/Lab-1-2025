CREATE TABLE IF NOT EXISTS weather_hourly (
    id UInt64,
    city String,
    datetime DateTime,
    temperature_2m Float32,
    precipitation Float32,
    windspeed_10m Float32,
    winddirection_10m UInt16,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (city, datetime);

CREATE TABLE IF NOT EXISTS weather_daily (
    id UInt64,
    city String,
    date Date,
    min_temp Float32,
    max_temp Float32,
    avg_temp Float32,
    total_precipitation Float32,
    max_windspeed Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (city, date);

