-- =====================================================================
-- SISTEMA DE MONITOREO SÍSMICO GLOBAL (SISMOWATCH)
-- PostgreSQL DDL: Arquitectura Medallón (Curated Serving Layer) y Calidad
-- =====================================================================

-- Crear esquemas Medallón y Gobernanza
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS quality;

-- ---------------------------------------------------------------------
-- 1. ESQUEMA RAW (BRONZE - Copia inmutable / Metadatos de ingesta)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.earthquake_events_raw (
    id VARCHAR(50) PRIMARY KEY,
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT NOT NULL,
    payload JSONB NOT NULL
);

-- ---------------------------------------------------------------------
-- 2. ESQUEMA STAGING (SILVER - Datos Limpios y Tipados)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.earthquake_events_staging (
    id VARCHAR(50) PRIMARY KEY,
    mag NUMERIC(4,2),
    mag_type VARCHAR(15),
    place VARCHAR(255),
    time_epoch BIGINT,
    time_utc TIMESTAMP NOT NULL,
    updated_epoch BIGINT,
    updated_utc TIMESTAMP,
    longitude NUMERIC(7,4) NOT NULL,
    latitude NUMERIC(7,4) NOT NULL,
    depth_km NUMERIC(6,2) NOT NULL,
    tsunami INT DEFAULT 0,
    sig INT,
    status VARCHAR(20),
    type VARCHAR(30),
    url TEXT,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- 3. ESQUEMA CURATED (GOLD - Capa de Servicio para Consumo y Analítica)
-- ---------------------------------------------------------------------

-- A. Eventos sísmicos consolidados y enriquecidos
CREATE TABLE IF NOT EXISTS curated.earthquake_events (
    id VARCHAR(50) PRIMARY KEY,
    mag NUMERIC(4,2) NOT NULL,
    mag_type VARCHAR(15),
    place VARCHAR(255),
    country_region VARCHAR(100) NOT NULL,
    time_utc TIMESTAMP NOT NULL,
    updated_utc TIMESTAMP,
    longitude NUMERIC(7,4) NOT NULL,
    latitude NUMERIC(7,4) NOT NULL,
    depth_km NUMERIC(6,2) NOT NULL,
    tsunami INT NOT NULL DEFAULT 0,
    sig INT,
    status VARCHAR(20),
    alert_level VARCHAR(20) NOT NULL DEFAULT 'LEVE',
    depth_type VARCHAR(20) NOT NULL DEFAULT 'SUPERFICIAL',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_curated_time ON curated.earthquake_events (time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_curated_mag ON curated.earthquake_events (mag DESC);
CREATE INDEX IF NOT EXISTS idx_curated_region ON curated.earthquake_events (country_region);

-- B. Modelo Analítico 1: Resumen Diario por País
CREATE TABLE IF NOT EXISTS curated.resumen_diario_pais (
    fecha_evento DATE NOT NULL,
    pais_estado VARCHAR(100) NOT NULL,
    total_sismos INT NOT NULL DEFAULT 0,
    magnitud_maxima NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    magnitud_promedio NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    total_alertas_tsunami INT NOT NULL DEFAULT 0,
    sismos_criticos_m6 INT NOT NULL DEFAULT 0,
    sismos_peligrosos_m5 INT NOT NULL DEFAULT 0,
    ultima_actualizacion_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_evento, pais_estado)
);

CREATE INDEX IF NOT EXISTS idx_resumen_fecha ON curated.resumen_diario_pais (fecha_evento DESC);

-- C. Modelo Analítico 2: Zonas Críticas y Hotspots (Cuadrículas de 1°x1°)
CREATE TABLE IF NOT EXISTS curated.zonas_criticas_frecuentes (
    cuadricula_lat NUMERIC(5, 1) NOT NULL,
    cuadricula_lon NUMERIC(5, 1) NOT NULL,
    zona_referencia VARCHAR(100),
    total_sismos_registrados INT NOT NULL DEFAULT 0,
    sismos_graves_m5 INT NOT NULL DEFAULT 0,
    magnitud_maxima NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    nivel_alerta VARCHAR(20) NOT NULL DEFAULT 'NORMAL',
    fecha_primer_sismo TIMESTAMP,
    fecha_ultimo_sismo TIMESTAMP,
    ultima_actualizacion_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cuadricula_lat, cuadricula_lon)
);

-- D. Reportes Ejecutivos Diarios generados por el Pool de IA
CREATE TABLE IF NOT EXISTS curated.ai_daily_reports (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    summary_narrative TEXT NOT NULL,
    critical_events JSONB DEFAULT '[]'::jsonb,
    active_zones JSONB DEFAULT '[]'::jsonb,
    anomaly_analysis JSONB DEFAULT '{}'::jsonb,
    verified_metrics JSONB NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    api_key_index INT NOT NULL,
    tokens_used INT NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    estimated_cost_usd NUMERIC(8,6) NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_reports_date ON curated.ai_daily_reports (report_date DESC);

-- ---------------------------------------------------------------------
-- 4. ESQUEMA QUALITY (Calidad de Datos y Trazabilidad de Métricas)
-- ---------------------------------------------------------------------

-- A. Tabla de Métricas de Calidad de Datos
CREATE TABLE IF NOT EXISTS quality.quality_metrics (
    id SERIAL PRIMARY KEY,
    execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dag_run_id VARCHAR(100),
    records_evaluated INT NOT NULL DEFAULT 0,
    records_passed INT NOT NULL DEFAULT 0,
    records_failed INT NOT NULL DEFAULT 0,
    quality_score NUMERIC(5,2) NOT NULL DEFAULT 100.00,
    freshness_lag_seconds INT NOT NULL DEFAULT 0,
    rules_summary JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_quality_time ON quality.quality_metrics (execution_time DESC);

-- B. Auditoría y Logs de Ejecuciones de IA
CREATE TABLE IF NOT EXISTS quality.ai_execution_logs (
    id SERIAL PRIMARY KEY,
    execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    task_name VARCHAR(100) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    api_key_index INT NOT NULL,
    prompt_tokens INT NOT NULL DEFAULT 0,
    completion_tokens INT NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    estimated_cost_usd NUMERIC(8,6) NOT NULL DEFAULT 0.0,
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    error_message TEXT
);
