DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dubai_population') THEN
        CREATE DATABASE dubai_population;
    END IF;
END
$$;


CREATE ROLE api_user WITH LOGIN PASSWORD 'api_password';
CREATE ROLE etl_user WITH LOGIN PASSWORD 'etl_password';
