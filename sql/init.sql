CREATE SCHEMA data;

CREATE TABLE data.data (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  value TEXT NOT NULL
);

INSERT INTO data.data (name, value) VALUES ('foo', 'bar');
INSERT INTO data.data (name, value) VALUES ('baz', 'qux');


CREATE TABLE IF NOT EXISTS data.raw (
  id SERIAL PRIMARY KEY,
  ts_request TIMESTAMP NOT NULL,
  ts_location TIMESTAMP NOT NULL,
  raw_data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS data.temp_change (
  "date" TIMESTAMP NOT NULL UNIQUE,
  state VARCHAR(4) NOT NULL,
  mean_temp FLOAT NOT NULL,
  temp_fluct FLOAT NOT NULL
);
