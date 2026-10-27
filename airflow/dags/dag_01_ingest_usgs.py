"""
DAG 1: dag_01_ingest_usgs.py
Proyecto: SismoWatch
Frecuencia: Cada 5 minutos (*/5 * * * *)

Propósito:
  1. Conectar a la API pública de USGS (feed all_hour.geojson).
  2. Guardar inmutablemente en MinIO S3 (Capa Bronze: s3a://bronze/batch/).
  3. Registrar metadatos e idempotencia en PostgreSQL (raw.earthquake_events_raw).
  4. Disparar automáticamente el DAG de Transformación y Calidad (DAG 2).
"""

import os
import json
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import Json
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "Uvp62vvXRo9GiUjZPiC6")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "SVnjG3CxoqgmAZ7r3nbVAYDK2V6wvxLUGMq35YRn")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sismo_ingest_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ingest_secret_2026")


def on_failure_alert(context):
    logging.error(f"¡ALERTA CRÍTICA! Falló la tarea {context.get('task_instance').task_id} en DAG {context.get('dag').dag_id}")


default_args = {
    "owner": "sismowatch_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=3),
    "on_failure_callback": on_failure_alert
}


def descargar_e_ingestar_usgs(**context):
    logging.info(f"Conectando a USGS API: {USGS_FEED_URL}")
    response = requests.get(USGS_FEED_URL, timeout=20)
    response.raise_for_status()
    geojson_data = response.json()

    features = geojson_data.get("features", [])
    logging.info(f"Descargados {len(features)} eventos sísmicos de USGS.")

    now_utc = datetime.utcnow()
    fecha_str = now_utc.strftime("%Y-%m-%d")
    timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")

    # 1. Guardar en MinIO Bronze (S3)
    try:
        minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        if not minio_client.bucket_exists("bronze"):
            minio_client.make_bucket("bronze")

        file_content = json.dumps(geojson_data).encode("utf-8")
        object_name = f"batch/ano={now_utc.year}/mes={now_utc.month:02d}/dia={now_utc.day:02d}/sismos_feed_{timestamp_str}.json"
        
        from io import BytesIO
        minio_client.put_object("bronze", object_name, BytesIO(file_content), len(file_content), content_type="application/json")
        logging.info(f"Guardado en MinIO S3: s3a://bronze/{object_name}")
    except Exception as e_minio:
        logging.warning(f"Aviso MinIO: {e_minio}")

    # 2. Inserción Idempotente en PostgreSQL (raw.earthquake_events_raw)
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB
        )
        cur = conn.cursor()
        q_insert = """
        INSERT INTO raw.earthquake_events_raw (id, source_url, payload, received_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE SET
            payload = EXCLUDED.payload,
            received_at = CURRENT_TIMESTAMP;
        """
        data_to_insert = [
            (f.get("id"), USGS_FEED_URL, Json(f))
            for f in features if f.get("id")
        ]
        from psycopg2.extras import execute_batch
        execute_batch(cur, q_insert, data_to_insert)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"PostgreSQL raw.earthquake_events_raw actualizado con {len(data_to_insert)} registros.")
    except Exception as e_pg:
        logging.error(f"Error insertando en PostgreSQL raw: {e_pg}")
        raise e_pg


with DAG(
    dag_id="dag_01_ingest_usgs",
    default_args=default_args,
    description="Ingesta periódica cada 5 min desde USGS hacia MinIO Bronze y PostgreSQL Raw",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingesta", "usgs", "bronze", "sismowatch"],
    doc_md="""
    # DAG de Ingesta USGS
    Descarga periódica cada 5 minutos de eventos sísmicos recientes.
    Garantiza idempotencia y persistencia inmutable en la Capa Bronze.
    """
) as dag:

    tarea_ingesta = PythonOperator(
        task_id="tarea_descargar_e_ingestar_usgs",
        python_callable=descargar_e_ingestar_usgs
    )

    tarea_disparar_transformacion = TriggerDagRunOperator(
        task_id="tarea_disparar_transformacion",
        trigger_dag_id="dag_02_transform_quality",
        wait_for_completion=False
    )

    tarea_ingesta >> tarea_disparar_transformacion
