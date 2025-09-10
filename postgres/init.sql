-- Dubai Population Database Schema


-- Create database if it doesn't exist
SELECT 'CREATE DATABASE dubai_population'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dubai_population')\gexec


-- Connect to the target database
\c uae_population


-- Create dedicated roles with appropriate permissions
DO $$
BEGIN
    -- Create API user with read-only access
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_user') THEN
        CREATE ROLE api_user WITH LOGIN PASSWORD 'api_password';
    END IF;
    
    -- Create ETL user with write access
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_user') THEN
        CREATE ROLE etl_user WITH LOGIN PASSWORD 'etl_password';
    END IF;
END$$;


-- Enable essential extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";


-- Create schemas for better organization
CREATE SCHEMA IF NOT EXISTS dubai;
CREATE SCHEMA IF NOT EXISTS analytics;


-- Set default schema
SET search_path TO dubai, public;

-- Table: Administrative Sectors
CREATE TABLE IF NOT EXISTS sectors (
    id SERIAL PRIMARY KEY,
    name_ar VARCHAR(200),
    name_en VARCHAR(200) NOT NULL,
    total_population INTEGER,
    total_area DECIMAL,
    total_density DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE sectors IS 'Administrative sectors of Dubai emirate';


-- Table: Communities (Districts)
CREATE TABLE IF NOT EXISTS communities (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ar VARCHAR(200) NOT NULL,
    name_en VARCHAR(200) NOT NULL,
    sector_id INTEGER REFERENCES sectors(id),
    area_km2 DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE communities IS 'Communities/districts within Dubai sectors';


-- Table: Population Time Series Data
CREATE TABLE IF NOT EXISTS population_data (
    id SERIAL PRIMARY KEY,
    community_id INTEGER REFERENCES communities(id),
    year INTEGER NOT NULL,
    population INTEGER NOT NULL,
    density DECIMAL NOT NULL,
    is_estimated BOOLEAN DEFAULT FALSE,
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(community_id, year)
);

COMMENT ON TABLE population_data IS 'Historical and projected population data for communities';


-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_communities_code ON communities(code);
CREATE INDEX IF NOT EXISTS idx_communities_sector ON communities(sector_id);

CREATE INDEX IF NOT EXISTS idx_population_data_community ON population_data(community_id);
CREATE INDEX IF NOT EXISTS idx_population_data_year ON population_data(year);


-- Analytics views for reporting and visualization
CREATE OR REPLACE VIEW analytics.emirate_population AS
SELECT 
    pd.year,
    SUM(pd.population) as total_population,
    ROUND(SUM(pd.population) / NULLIF(SUM(c.area_km2), 0), 2) as density,
    COUNT(*) as communities_count,
    SUM(CASE WHEN pd.is_estimated THEN 1 ELSE 0 END) as estimated_years_count
FROM population_data pd
JOIN communities c ON pd.community_id = c.id
GROUP BY pd.year
ORDER BY pd.year;


CREATE OR REPLACE VIEW analytics.sector_population AS
SELECT 
    s.name_en as sector_name,
    pd.year,
    SUM(pd.population) as total_population,
    ROUND(SUM(pd.population) / NULLIF(SUM(c.area_km2), 0), 2) as density,
    COUNT(DISTINCT c.id) as communities_count
FROM population_data pd
JOIN communities c ON pd.community_id = c.id
JOIN sectors s ON c.sector_id = s.id
GROUP BY s.name_en, pd.year
ORDER BY s.name_en, pd.year;


CREATE OR REPLACE VIEW analytics.community_population AS
SELECT 
    c.code as community_code,
    c.name_en as community_name,
    s.name_en as sector_name,
    pd.year,
    pd.population,
    pd.density,
    pd.is_estimated,
    c.area_km2
FROM population_data pd
JOIN communities c ON pd.community_id = c.id
JOIN sectors s ON c.sector_id = s.id
ORDER BY c.name_en, pd.year;


-- Grant permissions
GRANT USAGE ON SCHEMA dubai, analytics TO api_user, etl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA dubai, analytics TO api_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dubai TO etl_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA dubai TO etl_user;


-- Create update trigger for timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER update_sectors_updated_at 
    BEFORE UPDATE ON sectors 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


CREATE TRIGGER update_communities_updated_at 
    BEFORE UPDATE ON communities 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


CREATE TRIGGER update_population_data_updated_at 
    BEFORE UPDATE ON population_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
