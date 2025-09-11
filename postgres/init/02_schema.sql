CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";


CREATE SCHEMA IF NOT EXISTS dubai;
CREATE SCHEMA IF NOT EXISTS analytics;


-- Table: Administrative Sectors (explicit schema)
CREATE TABLE IF NOT EXISTS dubai.sectors (
    id SERIAL PRIMARY KEY,
    name_ar VARCHAR(200),
    name_en VARCHAR(200) NOT NULL,
    total_population INTEGER,
    total_area DECIMAL,
    total_density DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dubai.sectors IS 'Administrative sectors of Dubai emirate';


-- Table: Communities (Districts)
CREATE TABLE IF NOT EXISTS dubai.communities (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ar VARCHAR(200) NOT NULL,
    name_en VARCHAR(200) NOT NULL,
    sector_id INTEGER REFERENCES dubai.sectors(id),
    area_km2 DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dubai.communities IS 'Communities/districts within Dubai sectors';


-- Table: Population Time Series Data
CREATE TABLE IF NOT EXISTS dubai.population_data (
    id SERIAL PRIMARY KEY,
    community_id INTEGER REFERENCES dubai.communities(id),
    year INTEGER NOT NULL,
    population INTEGER NOT NULL,
    density DECIMAL NOT NULL,
    is_estimated BOOLEAN DEFAULT FALSE,
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(community_id, year)
);

COMMENT ON TABLE dubai.population_data IS 'Historical and projected population data for communities';


-- Indexes (explicit schema)
CREATE INDEX IF NOT EXISTS idx_communities_code ON dubai.communities(code);
CREATE INDEX IF NOT EXISTS idx_communities_sector ON dubai.communities(sector_id);
CREATE INDEX IF NOT EXISTS idx_population_data_community ON dubai.population_data(community_id);
CREATE INDEX IF NOT EXISTS idx_population_data_year ON dubai.population_data(year);
