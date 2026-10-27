-- =====================================================================
-- CONTROL DE ACCESO BASADO EN ROLES Y PRINCIPIO DE MÍNIMO PRIVILEGIO
-- =====================================================================

-- 1. Rol de Ingesta (Airflow / Ingestor)
-- Solo puede insertar y leer eventos crudos en el esquema raw
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sismo_ingest_user') THEN
        CREATE ROLE sismo_ingest_user WITH LOGIN PASSWORD 'ingest_secret_2026';
    END IF;
END $$;

GRANT CONNECT ON DATABASE sismos_db TO sismo_ingest_user;
GRANT USAGE ON SCHEMA raw TO sismo_ingest_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA raw TO sismo_ingest_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT, INSERT, UPDATE ON TABLES TO sismo_ingest_user;

-- 2. Rol de Transformación (Airflow / Spark Batch)
-- Requiere lectura en raw, y control total en staging, curated y quality
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sismo_transform_user') THEN
        CREATE ROLE sismo_transform_user WITH LOGIN PASSWORD 'transform_secret_2026';
    END IF;
END $$;

GRANT CONNECT ON DATABASE sismos_db TO sismo_transform_user;
GRANT USAGE ON SCHEMA raw, staging, curated, quality TO sismo_transform_user;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO sismo_transform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO sismo_transform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA curated TO sismo_transform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA quality TO sismo_transform_user;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA curated TO sismo_transform_user;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA quality TO sismo_transform_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL PRIVILEGES ON TABLES TO sismo_transform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT ALL PRIVILEGES ON TABLES TO sismo_transform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality GRANT ALL PRIVILEGES ON TABLES TO sismo_transform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT ALL PRIVILEGES ON SEQUENCES TO sismo_transform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality GRANT ALL PRIVILEGES ON SEQUENCES TO sismo_transform_user;

-- 3. Rol de Consumo (FastAPI Plataforma Web)
-- MÍNIMO PRIVILEGIO: Solo lectura SELECT en curated y quality_metrics
-- NO tiene acceso a raw ni a staging
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sismo_read_user') THEN
        CREATE ROLE sismo_read_user WITH LOGIN PASSWORD 'read_secret_2026';
    END IF;
END $$;

GRANT CONNECT ON DATABASE sismos_db TO sismo_read_user;
GRANT USAGE ON SCHEMA curated, quality TO sismo_read_user;
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO sismo_read_user;
GRANT SELECT ON quality.quality_metrics TO sismo_read_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT SELECT ON TABLES TO sismo_read_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality GRANT SELECT ON TABLES TO sismo_read_user;
