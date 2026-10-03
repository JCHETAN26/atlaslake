-- Runs once, on first container start. Creates the databases and service roles the platform uses.
-- Application schemas are created by `atlas db init` (sql/*.sql) so they can be re-applied in tests.

CREATE DATABASE analytics;          -- Phase 4 baseline warehouse + Gold serving copy
CREATE DATABASE iceberg_catalog;    -- Iceberg JDBC catalog (Spark + PyIceberg share it)

CREATE ROLE marquez LOGIN PASSWORD 'marquez';
CREATE DATABASE marquez OWNER marquez;

CREATE ROLE airflow LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Least-privilege roles for the operational source (grants applied in sql/source_schema.sql).
CREATE ROLE commerce_app LOGIN PASSWORD 'commerce_app';       -- workload generator: DML on commerce tables
CREATE ROLE etl_reader   LOGIN PASSWORD 'etl_reader';         -- extract jobs: read-only + outbox read
CREATE ROLE analyst      LOGIN PASSWORD 'analyst';            -- analytics db: read-only, no PII
