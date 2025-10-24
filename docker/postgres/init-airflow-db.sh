#!/bin/bash
set -e

# Create Airflow database and user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Airflow user if it doesn't exist
    DO
    \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD 'airflow123';
        END IF;
    END
    \$\$;

    -- Create Airflow database if it doesn't exist
    SELECT 'CREATE DATABASE airflow OWNER airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

echo "Airflow database and user created successfully"