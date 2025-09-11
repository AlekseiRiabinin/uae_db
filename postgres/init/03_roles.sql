-- Create roles with permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_user') THEN
        CREATE ROLE api_user WITH LOGIN PASSWORD 'api_password';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_user') THEN
        CREATE ROLE etl_user WITH LOGIN PASSWORD 'etl_password';
    END IF;
END
$$;


-- Grant permissions
GRANT USAGE ON SCHEMA dubai, analytics TO api_user, etl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA dubai, analytics TO api_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dubai TO etl_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA dubai TO etl_user;
