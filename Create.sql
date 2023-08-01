CREATE TABLE IF NOT EXISTS weather_data (
    observation_time TIMESTAMP,
    temp_C INT,
    temp_F INT,
    weatherCode INT,
    weatherIconUrl VARCHAR(1000),
    weatherDesc VARCHAR(1000),
    precipMM REAL,
    humidity INT,
    visibility INT,
    pressure INT,
    cloudcover INT,
    FeelsLikeC INT,
    FeelsLikeF INT,
    uvIndex INT,
    city VARCHAR(100),
    UNIQUE (observation_time, city)
)