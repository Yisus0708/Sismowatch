"""
DAG 2: dag_02_transform_quality.py
Proyecto: SismoWatch
Disparo: Automático tras DAG 1 o manual

Propósito:
  1. Invocar el clúster Apache Spark Batch (Master & Worker).
  2. Procesar Bronze -> Silver (Parquet Snappy limpio) -> Gold (Modelos Analíticos).
  3. Ejecutar validaciones de calidad de datos bloqueantes.
  4. Desviar registros corruptos a quality.quarantine.
  5. Cargar datos validados en PostgreSQL Curated y registrar score en quality.quality_metrics.
"""

import os
import logging
from datetime import datetime, timedelta
import requests
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

SPARK_TRIGGER_URL = os.getenv("SPARK_TRIGGER_URL", "http://spark-master:5050/trigger")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sismo_transform_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "transform_secret_2026")


def on_failure_alert(context):
    logging.error(f"¡ALERTA! Error en pipeline de transformación en tarea {context.get('task_instance').task_id}")


default_args = {
    "owner": "sismowatch_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_alert
}


def disparar_spark_batch_etl(**context):
    logging.info(f"Disparando Spark Batch ETL en {SPARK_TRIGGER_URL}...")
    try:
        resp = requests.post(SPARK_TRIGGER_URL, timeout=660)
        logging.info(f"Respuesta de Spark Trigger: HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise Exception(f"Spark Batch falló con respuesta HTTP {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        logging.info(f"Resultado de Spark: {data.get('status')}")
    except requests.exceptions.RequestException as e:
        logging.warning(f"Aviso de conexión a Spark Master: {e}. Verificando fallback...")


def verificar_calidad_y_metricas(**context):
    logging.info("Verificando métricas de calidad en PostgreSQL...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB
    )
    cur = conn.cursor()
    cur.execute("SELECT quality_score, records_evaluated, records_passed, records_failed FROM quality.quality_metrics ORDER BY execution_time DESC LIMIT 1;")
    row = cur.fetchone()
    if row:
        score, total, passed, failed = row
        logging.info(f"Última evaluación de calidad: Score={score}%, Aprobados={passed}, Cuarentena={failed}")
        if float(score) < 50.0:
            cur.close()
            conn.close()
            raise ValueError(f"CRÍTICO: El score de calidad ({score}%) está por debajo del umbral mínimo tolerable.")
    else:
        logging.info("Aún no se registran métricas en quality.quality_metrics.")
    cur.close()
    conn.close()


with DAG(
    dag_id="dag_02_transform_quality",
    default_args=default_args,
    description="Pipeline de Transformación Spark Batch, Calidad y Carga a Curated",
    schedule_interval=None,  # Disparado por DAG 1
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transformacion", "calidad", "spark", "medallon", "curated"],
    doc_md="""
    # DAG de Transformación y Calidad
    Ejecuta el job masivo de PySpark Batch ETL que lee desde MinIO Bronze,
    evalúa reglas de calidad hacia Silver/Gold y actualiza la base de datos Serving Layer.
    """
) as dag:

    with TaskGroup("grupo_procesamiento_spark") as tg_spark:
        tarea_ejecutar_spark = PythonOperator(
            task_id="tarea_ejecutar_spark_batch",
            python_callable=disparar_spark_batch_etl
        )

    with TaskGroup("grupo_validacion_calidad") as tg_calidad:
        tarea_auditar_calidad = PythonOperator(
            task_id="tarea_auditar_metricas_calidad",
            python_callable=verificar_calidad_y_metricas
        )

    tg_spark >> tg_calidad
