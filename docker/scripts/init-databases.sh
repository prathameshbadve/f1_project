#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE f1_dagster;
    GRANT ALL PRIVILEGES ON DATABASE f1_dagster TO $POSTGRES_USER;
EOSQL

echo "✅ Database f1_dagster created successfully!"