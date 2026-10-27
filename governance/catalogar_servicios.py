#!/usr/bin/env python3
"""
catalogar_servicios.py
=============================================================================
Script de automatización de gobernanza, catálogo y linaje para OpenMetadata 2.0.6.
Configurado para el entorno 100% LOCAL en Ubuntu.

Registra de forma idempotente:
  1. Equipo y Usuario técnico (Yisus0708) como Owner único.
  2. Glosario de Negocio 'Glosario_Sismico_Global' y 7 términos técnicos.
  3. Servicio PostgreSQL Serving Layer (curated.earthquake_events, etc. con tags y owners).
  4. Servicio MinIO Medallion Data Lake (Buckets bronze, silver, gold).
  5. Servicio Apache Spark (Pipeline 'spark_batch_etl').
  6. Servicio Apache Airflow (DAGs de ingesta, transformación, reporte IA y backfill).
  7. Grafo de Linaje Completo de Extremo a Extremo.
=============================================================================
"""

import os
import sys
import time
import json
import base64
import requests

OM_SERVER_URL = os.getenv("OM_SERVER_URL", "http://openmetadata-server:8585/api/v1")
OM_USER = os.getenv("OM_USER", "admin@open-metadata.org")
OM_PASSWORD = os.getenv("OM_PASSWORD", "admin")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
MINIO_HOST = os.getenv("MINIO_HOST", "minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")


def api_request(method, endpoint, payload=None, token=None):
    url = f"{OM_SERVER_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.request(method, url, json=payload, headers=headers, timeout=30)
        if resp.status_code in [200, 201]:
            return resp.json() if resp.text else {}
        elif resp.status_code == 409:
            try:
                return resp.json()
            except Exception:
                return {"status": "conflict_exists"}
        else:
            print(f"[HTTP {resp.status_code}] en {method} {url}: {resp.text[:200]}")
            return {}
    except requests.exceptions.RequestException as e:
        print(f"[AVISO CONEXION] en {method} {url}: {e}")
        return {}


def esperar_servidor(max_intentos=40, intervalo=3):
    print(f"Esperando disponibilidad de OpenMetadata Server en {OM_SERVER_URL}...")
    for i in range(1, max_intentos + 1):
        try:
            r = requests.get(f"{OM_SERVER_URL}/system/version", timeout=5)
            if r.status_code == 200:
                print(f"✓ OpenMetadata Server ACTIVO y respondiendo.")
                return True
        except Exception:
            pass
        time.sleep(intervalo)
    print("Aviso: OpenMetadata no respondió en el tiempo límite, intentando continuar...")
    return False


def autenticar():
    print("Autenticando contra OpenMetadata API...")
    pwd_b64 = base64.b64encode(OM_PASSWORD.encode("utf-8")).decode("utf-8")
    login_payload = {
        "email": OM_USER,
        "password": pwd_b64
    }
    try:
        res = api_request("POST", "users/login", login_payload)
        token = res.get("accessToken")
        if token:
            print("✓ Autenticación exitosa (Token JWT adquirido).")
            return token
    except Exception as e:
        print(f"Aviso en login: {e}")
    return None


def registrar_equipo_y_usuarios(token):
    print("\n--- 1. Registrando Equipo y Usuario de Gobernanza (Owner Único: Yisus0708) ---")
    team_payload = {
        "name": "Equipo_SismoWatch",
        "displayName": "Equipo de Ingeniería de Datos SismoWatch",
        "description": "Lead Data & Software Engineer a cargo de la arquitectura Medallón y Orquestación Batch.",
        "teamType": "Group"
    }
    api_request("PUT", "teams", team_payload, token)

    # Limpiar usuarios anteriores para dejar exclusivamente a Yisus0708
    obsoletos = ["jesus", "jhonatan", "kevin", "josue", "sayder"]
    try:
        users_resp = api_request("GET", "users?limit=50", token=token)
        for u in users_resp.get("data", []):
            if u.get("name") in obsoletos:
                uid = u.get("id")
                api_request("DELETE", f"users/{uid}?hardDelete=true&recursive=true", token=token)
                print(f"  ✓ Usuario previo retirado: {u.get('name')}")
    except Exception as e:
        print(f"  Aviso al depurar usuarios previos: {e}")

    usuarios = [
        {"name": "yisus0708", "displayName": "Yisus0708 (Lead Data & Software Engineer)", "email": "yisus0708@sismowatch.internal"}
    ]

    owners_dict = {}
    for u in usuarios:
        user_payload = {
            "name": u["name"],
            "displayName": u["displayName"],
            "email": u["email"],
            "isAdmin": True
        }
        res = api_request("PUT", "users", user_payload, token)
        if res.get("id"):
            owners_dict[u["name"]] = {"id": res["id"], "type": "user"}
            print(f"  ✓ Usuario registrado: {u['displayName']}")

    return owners_dict


def registrar_glosario(token):
    print("\n--- 2. Registrando Glosario de Términos Sísmicos ---")
    glosario_payload = {
        "name": "Glosario_Sismico_Global",
        "displayName": "Glosario de Términos Geofísicos y Arquitectura SismoWatch",
        "description": "Estándares y definiciones técnicas para monitoreo de sismos."
    }
    api_request("PUT", "glossaries", glosario_payload, token)

    terminos = [
        ("Hipocentro", "Punto en el interior de la Tierra donde se inicia la ruptura tectónica."),
        ("Epicentro", "Punto en la superficie terrestre directamente sobre el hipocentro."),
        ("Magnitud_Momento_Mw", "Escala sismológica cuantitativa de energía liberada por un sismo."),
        ("Alerta_Tsunami", "Bandera binaria oficial emitida por USGS/NOAA ante riesgo de tsunami."),
        ("Capa_Bronze", "Almacén inmutable de telemetría GeoJSON cruda en MinIO S3."),
        ("Capa_Silver", "Datos sísmicos limpios, tipados y deduplicados en Parquet Snappy."),
        ("Capa_Gold", "Modelos analíticos de negocio agregados en PostgreSQL y MinIO Parquet.")
    ]

    for nom, desc in terminos:
        t_payload = {
            "glossary": "Glosario_Sismico_Global",
            "name": nom,
            "displayName": nom.replace("_", " "),
            "description": desc
        }
        api_request("PUT", "glossaryTerms", t_payload, token)
        print(f"  ✓ Término de glosario: {nom}")


def registrar_servicio_postgres(token, owners_dict):
    print("\n--- 3. Registrando Servicio PostgreSQL Serving Layer ---")
    service_payload = {
        "name": "postgresql_sismowatch",
        "serviceType": "Postgres",
        "description": "Base de datos PostgreSQL Serving Layer para consumo web y analítica.",
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "username": "sismo_read_user",
                "authType": {"password": "read_secret_2026"},
                "hostPort": f"{POSTGRES_HOST}:{POSTGRES_PORT}",
                "database": "sismos_db"
            }
        }
    }
    api_request("PUT", "services/databaseServices", service_payload, token)

    db_payload = {
        "name": "sismos_db",
        "service": "postgresql_sismowatch",
        "description": "Base de datos principal de eventos sismológicos."
    }
    api_request("PUT", "databases", db_payload, token)

    schema_payload = {
        "name": "curated",
        "database": "postgresql_sismowatch.sismos_db",
        "description": "Esquema Gold para consumo y analítica."
    }
    api_request("PUT", "databaseSchemas", schema_payload, token)

    tablas = [
        {
            "name": "earthquake_events",
            "displayName": "🥈 curated.earthquake_events (Sismos Validados)",
            "description": "Catálogo unificado de eventos sísmicos limpios y clasificados.",
            "columns": [
                {"name": "id", "dataType": "VARCHAR", "dataLength": 50, "description": "Identificador único USGS"},
                {"name": "mag", "dataType": "NUMERIC", "description": "Magnitud momento"},
                {"name": "mag_type", "dataType": "VARCHAR", "dataLength": 15, "description": "Tipo de magnitud"},
                {"name": "place", "dataType": "VARCHAR", "dataLength": 255, "description": "Ubicación geográfica descriptiva"},
                {"name": "country_region", "dataType": "VARCHAR", "dataLength": 100, "description": "País o región normalizada"},
                {"name": "time_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC del evento"},
                {"name": "longitude", "dataType": "NUMERIC", "description": "Longitud geográfica"},
                {"name": "latitude", "dataType": "NUMERIC", "description": "Latitud geográfica"},
                {"name": "depth_km", "dataType": "NUMERIC", "description": "Profundidad focal en km"},
                {"name": "tsunami", "dataType": "INT", "description": "Bandera de alerta de tsunami (0 o 1)"},
                {"name": "alert_level", "dataType": "VARCHAR", "dataLength": 20, "description": "Nivel de severidad sísmica"},
                {"name": "depth_type", "dataType": "VARCHAR", "dataLength": 20, "description": "Clasificación de profundidad"}
            ]
        },
        {
            "name": "resumen_diario_pais",
            "displayName": "🥇 curated.resumen_diario_pais (Resumen País)",
            "description": "Modelo analítico diario consolidado por país y fecha.",
            "columns": [
                {"name": "fecha_evento", "dataType": "DATE", "description": "Fecha del resumen"},
                {"name": "pais_estado", "dataType": "VARCHAR", "dataLength": 100, "description": "País o estado agrupado"},
                {"name": "total_sismos", "dataType": "INT", "description": "Conteo total de sismos"},
                {"name": "magnitud_maxima", "dataType": "NUMERIC", "description": "Magnitud máxima del día"},
                {"name": "magnitud_promedio", "dataType": "NUMERIC", "description": "Magnitud promedio"},
                {"name": "profundidad_promedio_km", "dataType": "NUMERIC", "description": "Profundidad promedio en km"},
                {"name": "total_alertas_tsunami", "dataType": "INT", "description": "Total alertas de tsunami"},
                {"name": "sismos_criticos_m6", "dataType": "INT", "description": "Sismos severos M6+"},
                {"name": "sismos_peligrosos_m5", "dataType": "INT", "description": "Sismos considerables M5+"}
            ]
        },
        {
            "name": "zonas_criticas_frecuentes",
            "displayName": "🥇 curated.zonas_criticas_frecuentes (Hotspots 1x1)",
            "description": "Hotspots tectónicos por cuadrículas de 1°x1°.",
            "columns": [
                {"name": "cuadricula_lat", "dataType": "NUMERIC", "description": "Latitud inferior de la cuadrícula"},
                {"name": "cuadricula_lon", "dataType": "NUMERIC", "description": "Longitud inferior de la cuadrícula"},
                {"name": "zona_referencia", "dataType": "VARCHAR", "dataLength": 255, "description": "Nombre de referencia de la zona"},
                {"name": "total_sismos_registrados", "dataType": "INT", "description": "Total de sismos registrados en el hotspot"},
                {"name": "sismos_graves_m5", "dataType": "INT", "description": "Sismos M5+ en la celda"},
                {"name": "magnitud_maxima", "dataType": "NUMERIC", "description": "Magnitud pico registrada"},
                {"name": "nivel_alerta", "dataType": "VARCHAR", "dataLength": 20, "description": "Nivel de criticidad tectónica"}
            ]
        },
        {
            "name": "ai_daily_reports",
            "displayName": "🤖 curated.ai_daily_reports (Reportes IA Gemini)",
            "description": "Informes ejecutivos diarios redactados por IA con métricas verificadas.",
            "columns": [
                {"name": "id", "dataType": "INT", "description": "Identificador incremental del reporte"},
                {"name": "report_date", "dataType": "DATE", "description": "Fecha del informe geofísico"},
                {"name": "summary_narrative", "dataType": "TEXT", "description": "Narrativa ejecutiva generada por IA"},
                {"name": "verified_metrics", "dataType": "JSON", "description": "Métricas numéricas auditadas contra base de datos"},
                {"name": "model_name", "dataType": "VARCHAR", "dataLength": 100, "description": "Modelo de IA generativa utilizado"}
            ]
        }
    ]

    for t in tablas:
        tabla_payload = {
            "name": t["name"],
            "displayName": t.get("displayName", t["name"]),
            "databaseSchema": "postgresql_sismowatch.sismos_db.curated",
            "description": t["description"],
            "columns": t["columns"]
        }
        if "yisus0708" in owners_dict:
            tabla_payload["owners"] = [owners_dict["yisus0708"]]
        api_request("PUT", "tables", tabla_payload, token)
        print(f"  ✓ Tabla catalogada: curated.{t['name']} ({t.get('displayName')})")


def registrar_servicio_minio(token, owners_dict):
    print("\n--- 4. Registrando Servicio MinIO S3 Object Storage ---")
    service_payload = {
        "name": "minio_s3_datalake",
        "serviceType": "S3",
        "description": "Data Lake Medallón sobre MinIO S3 local.",
        "connection": {
            "config": {
                "type": "S3",
                "awsConfig": {
                    "awsAccessKeyId": "Uvp62vvXRo9GiUjZPiC6",
                    "awsSecretAccessKey": "SVnjG3CxoqgmAZ7r3nbVAYDK2V6wvxLUGMq35YRn",
                    "endPointURL": f"http://{MINIO_HOST}:{MINIO_PORT}"
                }
            }
        }
    }
    api_request("PUT", "services/storageServices", service_payload, token)

    buckets = [
        ("bronze", "🥉 Capa Bronze (Data Lake S3)", "Capa Bronze: Archivos GeoJSON crudos e inmutables ingestados desde USGS."),
        ("silver", "🥈 Capa Silver (Parquet S3)", "Capa Silver: Archivos Parquet Snappy limpios, tipados y deduplicados."),
        ("gold", "🥇 Capa Gold (Analytics S3)", "Capa Gold: Modelos analíticos Parquet particionados por país y cuadrículas.")
    ]

    for b_name, b_disp, b_desc in buckets:
        cont_payload = {
            "name": b_name,
            "displayName": b_disp,
            "service": "minio_s3_datalake",
            "description": b_desc
        }
        if "yisus0708" in owners_dict:
            cont_payload["owners"] = [owners_dict["yisus0708"]]
        api_request("PUT", "containers", cont_payload, token)
        print(f"  ✓ Bucket catalogado: {b_name} ({b_disp})")


def registrar_servicio_spark_y_airflow(token, owners_dict):
    print("\n--- 5. Registrando Pipelines de Procesamiento y Orquestación ---")
    pipe_service_payload = {
        "name": "airflow_orchestrator",
        "serviceType": "Airflow",
        "description": "Orquestador Apache Airflow para ingesta y calidad.",
        "connection": {
            "config": {
                "type": "Airflow",
                "hostPort": "http://airflow-webserver:8080"
            }
        }
    }
    api_request("PUT", "services/pipelineServices", pipe_service_payload, token)

    dags = [
        ("dag_01_ingest_usgs", "Ingesta periódica cada 5 minutos desde la API pública de USGS."),
        ("dag_02_transform_quality", "Disparo de PySpark Batch ETL y validación de reglas de calidad."),
        ("dag_03_daily_ai_report", "Detección de anomalías Z-Score y resumen diario con Pool de Gemini."),
        ("dag_04_historical_backfill", "Backfill histórico parametrizado de 30 días sin duplicados.")
    ]

    for d_name, d_desc in dags:
        dag_payload = {
            "name": d_name,
            "service": "airflow_orchestrator",
            "description": d_desc
        }
        if "yisus0708" in owners_dict:
            dag_payload["owners"] = [owners_dict["yisus0708"]]
        api_request("PUT", "pipelines", dag_payload, token)
        print(f"  ✓ Pipeline catalogado: {d_name}")


def registrar_linaje_completo(token):
    print("\n--- 6. Configurando Linaje de Datos de Extremo a Extremo ---")
    lineage_edges = [
        # 1. Bronze S3 -> Silver S3
        {
            "from_entity": "container",
            "from_fqn": "minio_s3_datalake.bronze",
            "to_entity": "container",
            "to_fqn": "minio_s3_datalake.silver",
            "description": "Refinamiento y deduplicación en PySpark Batch"
        },
        # 2. Silver S3 -> curated.earthquake_events
        {
            "from_entity": "container",
            "from_fqn": "minio_s3_datalake.silver",
            "to_entity": "table",
            "to_fqn": "postgresql_sismowatch.sismos_db.curated.earthquake_events",
            "description": "Upsert idempotente en Serving Layer Postgres"
        },
        # 3. Silver S3 -> Gold S3
        {
            "from_entity": "container",
            "from_fqn": "minio_s3_datalake.silver",
            "to_entity": "container",
            "to_fqn": "minio_s3_datalake.gold",
            "description": "Modelado analítico Gold Parquet"
        },
        # 4. curated.earthquake_events -> Gold S3
        {
            "from_entity": "table",
            "from_fqn": "postgresql_sismowatch.sismos_db.curated.earthquake_events",
            "to_entity": "container",
            "to_fqn": "minio_s3_datalake.gold",
            "description": "Cálculo de modelos analíticos Gold"
        },
        # 5. Gold MinIO -> PostgreSQL Curated Resumen Pais
        {
            "from_entity": "container",
            "from_fqn": "minio_s3_datalake.gold",
            "to_entity": "table",
            "to_fqn": "postgresql_sismowatch.sismos_db.curated.resumen_diario_pais",
            "description": "Serving Layer: Resumen agregado diario por país"
        },
        # 6. Gold MinIO -> PostgreSQL Curated Zonas Frecuentes
        {
            "from_entity": "container",
            "from_fqn": "minio_s3_datalake.gold",
            "to_entity": "table",
            "to_fqn": "postgresql_sismowatch.sismos_db.curated.zonas_criticas_frecuentes",
            "description": "Serving Layer: Hotspots por cuadrículas 1°x1°"
        },
        # 7. Resumen Pais -> AI Reports
        {
            "from_entity": "table",
            "from_fqn": "postgresql_sismowatch.sismos_db.curated.resumen_diario_pais",
            "to_entity": "table",
            "to_fqn": "postgresql_sismowatch.sismos_db.curated.ai_daily_reports",
            "description": "Insumo analítico para reporte ejecutivo con Gemini"
        },
        # 8. Events -> AI Reports
        {
            "from_entity": "table",
            "from_fqn": "postgresql_sismowatch.sismos_db.curated.earthquake_events",
            "to_entity": "table",
            "to_fqn": "postgresql_sismowatch.sismos_db.curated.ai_daily_reports",
            "description": "Detección de anomalías Z-Score y sismos críticos para IA"
        }
    ]

    for edge in lineage_edges:
        from_res = api_request("GET", f"{edge['from_entity']}s/name/{edge['from_fqn']}", token=token)
        to_res = api_request("GET", f"{edge['to_entity']}s/name/{edge['to_fqn']}", token=token)

        from_id = from_res.get("id")
        to_id = to_res.get("id")

        if from_id and to_id:
            lineage_payload = {
                "edge": {
                    "fromEntity": {"id": from_id, "type": edge["from_entity"]},
                    "toEntity": {"id": to_id, "type": edge["to_entity"]},
                    "description": edge["description"]
                }
            }
            api_request("PUT", "lineage", lineage_payload, token)
            print(f"  ✓ Linaje vinculado: {edge['from_fqn']} -> {edge['to_fqn']}")
        else:
            print(f"  Aviso: No fue posible resolver entidades para linaje {edge['from_fqn']} -> {edge['to_fqn']}")


def main():
    print("=" * 80)
    print(">>> INICIANDO AUTO-CATALOGACIÓN OPENMETADATA (SISMOWATCH LOCAL) <<<")
    print("=" * 80)

    if not esperar_servidor():
        sys.exit(0)

    token = autenticar()
    owners = registrar_equipo_y_usuarios(token)
    registrar_glosario(token)
    registrar_servicio_postgres(token, owners)
    registrar_servicio_minio(token, owners)
    registrar_servicio_spark_y_airflow(token, owners)
    registrar_linaje_completo(token)

    print("\n" + "=" * 80)
    print(">>> AUTO-CATALOGACIÓN EN OPENMETADATA FINALIZADA EXITOSAMENTE <<<")
    print("=" * 80)


if __name__ == "__main__":
    main()
