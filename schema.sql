-- Resolved chassis records
CREATE TABLE IF NOT EXISTS chassis (
    chassis_id     VARCHAR PRIMARY KEY,
    chassis_number VARCHAR NOT NULL
);

-- Tracks every chassis event seen (used as a lookup by english_statement)
CREATE TABLE IF NOT EXISTS chassis_processed (
    chassis_id     VARCHAR PRIMARY KEY,
    chassis_number VARCHAR NOT NULL
);

-- Final enriched english_statement records
CREATE TABLE IF NOT EXISTS english_statement (
    english_statement_id VARCHAR PRIMARY KEY,
    chassis_id           VARCHAR NOT NULL,
    description          TEXT,
    chassis_number       VARCHAR
);

-- Holding area for english_statement events whose chassis has not arrived yet
CREATE TABLE IF NOT EXISTS english_statement_stage (
    english_statement_id VARCHAR PRIMARY KEY,
    chassis_id           VARCHAR NOT NULL,
    description          TEXT
);

-- Speeds up the chassis-arrival lookup in english_statement_stage
CREATE INDEX IF NOT EXISTS idx_es_stage_chassis_id
    ON english_statement_stage (chassis_id);
