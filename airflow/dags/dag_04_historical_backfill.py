"""
DAG 4: dag_04_historical_backfill.py
Proyecto: SismoWatch
Ejecución: Manual o por API REST con Parámetros

Propósito:
  1. Ejecutar backfill histórico parametrizado (por defecto últimos 30 días).
  2. Consultar el endpoint de búsqueda de USGS con filtro de magnitud mínima.
  3. Almacenar la respuesta en MinIO Bronze y PostgreSQL Raw de forma idempotente.
  4. Disparar el procesamiento Spark Batch sin generar duplicados.
"""

import os
import json
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import Json, execute_batch
from minio import Minio

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "Uvp62vvXRo9GiUjZPiC6")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "SVnjG3CxoqgmAZ7r3nbVAYDK2V6wvxLUGMq35YRn")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sismo_ingest_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ingest_secret_2026")


default_args = {
    "owner": "sismowatch_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15)
}


def ejecutar_backfill_historico(**context):
    params = context.get("params", {})
    days_back = int(params.get("days_back", 30))
    min_mag = float(params.get("min_magnitude", 2.5))

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_str}&endtime={end_str}&minmagnitude={min_mag}"
    logging.info(f"Iniciando descarga histórica de USGS: {url}")

    response = requests.get(url, timeout=120)
    response.raise_for_status()
    geojson_data = response.json()
    features = geojson_data.get("features", [])
    logging.info(f"Descargados {len(features)} sismos históricos (M >= {min_mag}, {start_str} a {end_str}).")

    # 1. Guardar en MinIO Bronze S3
    try:
        minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        if not minio_client.bucket_exists("bronze"):
            minio_client.make_bucket("bronze")

        file_bytes = json.dumps(geojson_data).encode("utf-8")
        obj_name = f"batch/backfill/sismos_historico_{start_str}_al_{end_str}.json"
        from io import BytesIO
        minio_client.put_object("bronze", obj_name, BytesIO(file_bytes), len(file_bytes), content_type="application/json")
        logging.info(f"Guardado en MinIO S3: s3a://bronze/{obj_name}")
    except Exception as e_s3:
        logging.warning(f"Aviso guardando en MinIO: {e_s3}")

    # 2. Carga Idempotente en PostgreSQL Raw
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
        data = [
            (f.get("id"), url, Json(f))
            for f in features if f.get("id")
        ]
        execute_batch(cur, q_insert, data, page_size=500)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"PostgreSQL raw actualizado con {len(data)} registros de backfill.")
    except Exception as e_pg:
        logging.error(f"Error insertando backfill en PostgreSQL: {e_pg}")
        raise e_pg


with DAG(
    dag_id="dag_04_historical_backfill",
    default_args=default_args,
    description="Backfill histórico parametrizado (30 días) desde USGS",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "days_back": Param(30, type="integer", minimum=1, maximum=90, description="Días hacia atrás a consultar"),
        "min_magnitude": Param(2.5, type="number", minimum=1.0, maximum=8.0, description="Magnitud mínima a filtrar")
    },
    tags=["backfill", "historico", "usgs", "sismowatch"],
    doc_md="""
    # DAG de Backfill Histórico
    Permite rellenar vacíos históricos mediante consultas parametrizadas a la API USGS.
    Garantiza no duplicación mediante inserciones 'ON CONFLICT DO UPDATE'.
    """
) as dag:

    tarea_backfill = PythonOperator(
        task_id="tarea_ejecutar_backfill_historico",
        python_callable=ejecutar_backfill_historico
    )

    tarea_disparar_spark = TriggerDagRunOperator(
        task_id="tarea_disparar_spark_backfill",
        trigger_dag_id="dag_02_transform_quality",
        wait_for_completion=False
    )

    tarea_backfill >> tarea_disparar_spark
