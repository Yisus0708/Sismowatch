"""
DAG 3: dag_03_daily_ai_report.py
Proyecto: SismoWatch
Frecuencia: Diario a las 07:00 UTC (0 7 * * *)

Propósito:
  1. Extraer métricas consolidadas oficiales del día desde PostgreSQL Curated.
  2. Ejecutar detección analítica de anomalías (Z-Score sobre frecuencia de sismos).
  3. Redactar el resumen ejecutivo con el Pool de 5 API Keys de Google AI Studio (Gemini).
  4. Validar estrictamente que las cifras del texto coincidan con los cálculos matemáticos oficiales.
  5. Registrar el reporte en curated.ai_daily_reports y auditar la llamada en quality.ai_execution_logs.
"""

import os
import sys
import logging
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import Json

from airflow import DAG
from airflow.operators.python import PythonOperator

# Incluir ruta de módulos personalizados
sys.path.insert(0, "/opt/airflow/include")
try:
    from ai_service import generar_reporte_ia_diario
except ImportError:
    from airflow.include.ai_service import generar_reporte_ia_diario

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sismo_transform_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "transform_secret_2026")


default_args = {
    "owner": "sismowatch_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5)
}


def extraer_metricas_y_generar_reporte_ia(**context):
    today_str = (context.get("logical_date") or datetime.utcnow()).strftime("%Y-%m-%d")
    logging.info(f"Iniciando generación de Reporte con IA para la fecha: {today_str}")

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB
    )
    cur = conn.cursor()

    # 1. Métricas Consolidadas Oficiales (SQL Puro)
    cur.execute("""
        SELECT 
            COUNT(*) AS total_sismos,
            COALESCE(MAX(mag), 0.0) AS max_mag,
            COALESCE(ROUND(AVG(mag), 2), 0.0) AS avg_mag,
            COALESCE(SUM(tsunami), 0) AS total_tsunami,
            COALESCE(SUM(CASE WHEN mag >= 6.0 THEN 1 ELSE 0 END), 0) AS criticos_m6,
            COALESCE(SUM(CASE WHEN mag >= 5.0 AND mag < 6.0 THEN 1 ELSE 0 END), 0) AS graves_m5
        FROM curated.earthquake_events
        WHERE time_utc >= CURRENT_DATE - INTERVAL '1 day';
    """)
    kpis = cur.fetchone()

    total_sismos = int(kpis[0]) if kpis else 0
    max_mag = float(kpis[1]) if kpis else 0.0
    avg_mag = float(kpis[2]) if kpis else 0.0
    total_tsunami = int(kpis[3]) if kpis else 0
    criticos_m6 = int(kpis[4]) if kpis else 0
    graves_m5 = int(kpis[5]) if kpis else 0

    # 2. Top Países
    cur.execute("""
        SELECT country_region, COUNT(*) AS total
        FROM curated.earthquake_events
        WHERE time_utc >= CURRENT_DATE - INTERVAL '1 day'
        GROUP BY country_region
        ORDER BY total DESC
        LIMIT 5;
    """)
    top_paises = [{"pais": r[0], "total": int(r[1])} for r in cur.fetchall()]

    # 3. Serie temporal por hora para Z-Score
    cur.execute("""
        SELECT EXTRACT(HOUR FROM time_utc)::int AS hora, COUNT(*) AS total
        FROM curated.earthquake_events
        WHERE time_utc >= CURRENT_DATE - INTERVAL '1 day'
        GROUP BY hora
        ORDER BY hora;
    """)
    hourly_dict = {int(r[0]): int(r[1]) for r in cur.fetchall()}
    hourly_counts = [hourly_dict.get(h, 0) for h in range(24)]

    # 4. Eventos críticos destacados
    cur.execute("""
        SELECT id, mag, place, country_region, time_utc, alert_level
        FROM curated.earthquake_events
        WHERE time_utc >= CURRENT_DATE - INTERVAL '1 day' AND mag >= 5.0
        ORDER BY mag DESC
        LIMIT 5;
    """)
    critical_events = [
        {
            "id": r[0], "mag": float(r[1]), "place": r[2],
            "country_region": r[3], "time_utc": r[4].isoformat() if r[4] else None,
            "alert_level": r[5]
        }
        for r in cur.fetchall()
    ]

    metrics = {
        "total_sismos": total_sismos,
        "magnitud_maxima": max_mag,
        "magnitud_promedio": avg_mag,
        "total_alertas_tsunami": total_tsunami,
        "sismos_criticos_m6": criticos_m6,
        "sismos_peligrosos_m5": graves_m5,
        "top_paises": top_paises,
        "critical_events": critical_events
    }

    # 5. Invocación al Pool de 5 Claves de Google AI Studio con Validación Estricta
    report_obj = generar_reporte_ia_diario(today_str, metrics, hourly_counts)

    # 6. Guardar Reporte en curated.ai_daily_reports
    q_rep = """
    INSERT INTO curated.ai_daily_reports (
        report_date, summary_narrative, critical_events, active_zones,
        anomaly_analysis, verified_metrics, model_name, api_key_index,
        tokens_used, latency_ms, estimated_cost_usd
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (report_date) DO UPDATE SET
        summary_narrative = EXCLUDED.summary_narrative,
        critical_events = EXCLUDED.critical_events,
        active_zones = EXCLUDED.active_zones,
        anomaly_analysis = EXCLUDED.anomaly_analysis,
        verified_metrics = EXCLUDED.verified_metrics,
        model_name = EXCLUDED.model_name,
        api_key_index = EXCLUDED.api_key_index,
        tokens_used = EXCLUDED.tokens_used,
        latency_ms = EXCLUDED.latency_ms,
        estimated_cost_usd = EXCLUDED.estimated_cost_usd;
    """
    cur.execute(q_rep, (
        report_obj.report_date,
        report_obj.summary_narrative,
        Json(report_obj.critical_events),
        Json(report_obj.active_zones),
        Json(report_obj.anomaly_analysis),
        Json(report_obj.verified_metrics),
        report_obj.model_name,
        report_obj.api_key_index,
        report_obj.tokens_used,
        report_obj.latency_ms,
        report_obj.estimated_cost_usd
    ))

    # 7. Registrar auditoría en quality.ai_execution_logs
    q_log = """
    INSERT INTO quality.ai_execution_logs (
        task_name, model_name, api_key_index, prompt_tokens,
        completion_tokens, latency_ms, estimated_cost_usd, status
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.execute(q_log, (
        "dag_03_daily_ai_report",
        report_obj.model_name,
        report_obj.api_key_index,
        int(report_obj.tokens_used * 0.6),
        int(report_obj.tokens_used * 0.4),
        report_obj.latency_ms,
        report_obj.estimated_cost_usd,
        "SUCCESS"
    ))

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"¡Reporte Diario con IA guardado exitosamente para {today_str}!")


with DAG(
    dag_id="dag_03_daily_ai_report",
    default_args=default_args,
    description="Generación diaria de Reporte Narrativo con IA y Detección de Anomalías Z-Score",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ia", "gemini", "reportes", "anomalias", "sismowatch"],
    doc_md="""
    # DAG de Reporte Diario con IA
    Genera resúmenes ejecutivos automáticos conectándose al Pool de 5 API Keys de Gemini.
    Aplica controles para evitar alucinaciones numéricas y cuenta con fallback de contingencia.
    """
) as dag:

    tarea_reporte_ia = PythonOperator(
        task_id="tarea_generar_reporte_ia",
        python_callable=extraer_metricas_y_generar_reporte_ia
    )
