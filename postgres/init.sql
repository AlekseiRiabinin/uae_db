CREATE TABLE IF NOT EXISTS sectors (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE,
    name_ar VARCHAR(200),
    name_en VARCHAR(200),
    total_population INTEGER,
    total_area DECIMAL,
    total_density DECIMAL
);

CREATE TABLE IF NOT EXISTS communities (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ar VARCHAR(200) NOT NULL,
    name_en VARCHAR(200) NOT NULL,
    sector_id INTEGER REFERENCES sectors(id),
    area_km2 DECIMAL,
    UNIQUE(code)
);

CREATE TABLE IF NOT EXISTS population_data (
    id SERIAL PRIMARY KEY,
    community_id INTEGER REFERENCES communities(id),
    year INTEGER NOT NULL,
    population INTEGER NOT NULL,
    density DECIMAL NOT NULL,
    is_estimated BOOLEAN DEFAULT FALSE,
    data_source VARCHAR(200),
    UNIQUE(community_id, year)
);

-- Indexes for performance
CREATE INDEX idx_communities_code ON communities(code);
CREATE INDEX idx_population_data_year ON population_data(year);
CREATE INDEX idx_population_data_community ON population_data(community_id);
