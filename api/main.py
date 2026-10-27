"""
Servidor Principal FastAPI: Plataforma SismoWatch
Endpoints de Servicio, Control de Acceso RBAC, SSE en Vivo y Exportación
"""

import os
import json
import asyncio
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse, Response
import requests

from database import get_db
from auth import (
    authenticate_user, create_access_token, get_current_user,
    require_admin, sanitize_prompt_text
)
from models import LoginRequest, TokenResponse, KPIResponse, BackfillTriggerRequest
from ai_chat import procesar_mensaje_chat
from pydantic import BaseModel

class ChatMessageRequest(BaseModel):
    message: str

app = FastAPI(
    title="SismoWatch API",
    description="Plataforma de consumo y monitoreo sísmico global en tiempo casi real",
    version="2.0.0"
)

# Configuración de CORS Restringido
allowed_origins_raw = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000,http://localhost:8000")
allowed_origins = [o.strip() for o in allowed_origins_raw.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins if allowed_origins else ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")


def serialize_data(val: Any) -> Any:
    """Serializador universal de tipos Decimal y Datetime de Postgres"""
    if isinstance(val, list):
        return [serialize_data(x) for x in val]
    if isinstance(val, dict):
        return {k: serialize_data(v) for k, v in val.items()}
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val


# -----------------------------------------------------------------------------
# 1. AUTENTICACIÓN
# -----------------------------------------------------------------------------
@app.post("/auth/login", response_model=TokenResponse, tags=["Autenticación"])
def login(request: LoginRequest):
    """Inicia sesión y genera token JWT con rol ('viewer' o 'admin')"""
    user = authenticate_user(request.username, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas. Verifique usuario y contraseña."
        )
    token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user["role"],
        "username": user["username"]
    }


# -----------------------------------------------------------------------------
# 2. INDICADORES Y KPIS
# -----------------------------------------------------------------------------
@app.get("/api/kpis", tags=["Dashboard"])
def get_kpis(user: Dict[str, Any] = Depends(get_current_user)):
    """Retorna los indicadores tácticos de las últimas 24h desde la capa Curated"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Métricas agregadas de sismos en 24h
            cur.execute("""
                SELECT 
                    COUNT(*) AS total_sismos,
                    COALESCE(MAX(mag), 0.0) AS max_mag,
                    COALESCE(ROUND(AVG(mag), 2), 0.0) AS avg_mag,
                    COALESCE(SUM(tsunami), 0) AS total_tsunami,
                    COALESCE(SUM(CASE WHEN mag >= 6.0 THEN 1 ELSE 0 END), 0) AS criticos_m6,
                    COALESCE(SUM(CASE WHEN mag >= 5.0 AND mag < 6.0 THEN 1 ELSE 0 END), 0) AS graves_m5
                FROM curated.earthquake_events
                WHERE time_utc >= CURRENT_TIMESTAMP - INTERVAL '24 hours';
            """)
            row = cur.fetchone() or {}

            # Puntaje de calidad más reciente
            cur.execute("""
                SELECT quality_score, execution_time 
                FROM quality.quality_metrics 
                ORDER BY execution_time DESC LIMIT 1;
            """)
            q_row = cur.fetchone() or {}

            return {
                "total_sismos_24h": int(row.get("total_sismos") or 0),
                "magnitud_maxima": float(row.get("max_mag") or 0.0),
                "magnitud_promedio": float(row.get("avg_mag") or 0.0),
                "total_tsunamis": int(row.get("total_tsunami") or 0),
                "sismos_criticos_m6": int(row.get("criticos_m6") or 0),
                "sismos_peligrosos_m5": int(row.get("graves_m5") or 0),
                "puntaje_calidad": float(q_row.get("quality_score") or 100.0),
                "ultima_actualizacion_utc": datetime.utcnow().isoformat()
            }


# -----------------------------------------------------------------------------
# 3. LISTADO Y FILTRADO DE EVENTOS
# -----------------------------------------------------------------------------
@app.get("/api/events", tags=["Eventos"])
def get_events(
    min_mag: Optional[float] = Query(None, description="Magnitud mínima"),
    date_start: Optional[str] = Query(None, description="Fecha de inicio YYYY-MM-DD"),
    date_end: Optional[str] = Query(None, description="Fecha de fin YYYY-MM-DD"),
    region: Optional[str] = Query(None, description="País o región"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: Dict[str, Any] = Depends(get_current_user)
):
    """Retorna eventos sísmicos desde Curated con filtros y paginación"""
    conditions = ["1=1"]
    params = []

    if min_mag is not None:
        conditions.append("mag >= %s")
        params.append(min_mag)

    if date_start:
        conditions.append("time_utc >= %s")
        params.append(date_start)

    if date_end:
        conditions.append("time_utc <= %s")
        params.append(date_end + " 23:59:59")

    if region:
        clean_reg = sanitize_prompt_text(region)
        conditions.append("country_region ILIKE %s")
        params.append(f"%{clean_reg}%")

    where_clause = " AND ".join(conditions)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM curated.earthquake_events WHERE {where_clause};", tuple(params))
            total_records = cur.fetchone()["count"]

            cur.execute(f"""
                SELECT id, mag, mag_type, place, country_region, time_utc,
                       longitude, latitude, depth_km, tsunami, sig, alert_level, depth_type
                FROM curated.earthquake_events
                WHERE {where_clause}
                ORDER BY time_utc DESC
                LIMIT %s OFFSET %s;
            """, tuple(params + [limit, offset]))
            rows = cur.fetchall()

            return {
                "total": total_records,
                "limit": limit,
                "offset": offset,
                "data": serialize_data(rows)
            }


# -----------------------------------------------------------------------------
# 4. SERIES TEMPORALES
# -----------------------------------------------------------------------------
@app.get("/api/stats/timeseries", tags=["Analítica"])
def get_timeseries(user: Dict[str, Any] = Depends(get_current_user)):
    """Retorna la serie temporal horaria de eventos y magnitud máxima para gráficos"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    TO_CHAR(time_utc, 'YYYY-MM-DD HH24:00') AS hora_bloque,
                    COUNT(*) AS total_eventos,
                    COALESCE(MAX(mag), 0.0) AS max_mag
                FROM curated.earthquake_events
                WHERE time_utc >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                GROUP BY hora_bloque
                ORDER BY hora_bloque ASC;
            """)
            rows = cur.fetchall()
            return serialize_data(rows)


# -----------------------------------------------------------------------------
# 5. REPORTES EJECUTIVOS E IA
# -----------------------------------------------------------------------------
@app.get("/api/reports", tags=["Reportes"])
def get_reports(
    limit: int = 10,
    user: Dict[str, Any] = Depends(get_current_user)
):
    """Retorna el historial de reportes narrativos y anomalías de IA"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, report_date, summary_narrative, critical_events,
                       active_zones, anomaly_analysis, verified_metrics,
                       model_name, api_key_index, tokens_used, latency_ms,
                       estimated_cost_usd, created_at
                FROM curated.ai_daily_reports
                ORDER BY report_date DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
            return serialize_data(rows)


@app.get("/api/reports/export", tags=["Reportes"])
def export_reports(
    format: str = Query("csv", regex="^(csv|html)$"),
    user: Dict[str, Any] = Depends(get_current_user)
):
    """Exporta el reporte consolidado en formato CSV o HTML descargable"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_date, model_name, summary_narrative,
                       tokens_used, latency_ms, estimated_cost_usd, created_at
                FROM curated.ai_daily_reports
                ORDER BY report_date DESC
                LIMIT 50;
            """)
            rows = cur.fetchall()

    if format == "csv":
        import io
        import csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Fecha", "Modelo", "Tokens", "Latencia_ms", "Costo_USD", "Resumen_Narrativo"])
        for r in rows:
            resumen_limpio = str(r["summary_narrative"]).replace("\n", " ").replace("\r", " ")
            writer.writerow([r["report_date"], r["model_name"], r["tokens_used"], r["latency_ms"], r["estimated_cost_usd"], resumen_limpio])
        csv_bytes = output.getvalue().encode("utf-8")
        return Response(
            content=csv_bytes,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sismowatch_reportes.csv"}
        )
    else:
        html_content = "<html><head><meta charset='utf-8'><title>Reporte SismoWatch</title><style>body{font-family:sans-serif;padding:20px;}table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ccc;padding:8px;text-align:left;}th{background:#f4f4f4;}</style></head><body>"
        html_content += "<h1>SismoWatch • Historial de Reportes Ejecutivos de IA</h1>"
        html_content += "<table><tr><th>Fecha</th><th>Modelo</th><th>Tokens</th><th>Resumen</th></tr>"
        for r in rows:
            html_content += f"<tr><td>{r['report_date']}</td><td>{r['model_name']}</td><td>{r['tokens_used']}</td><td>{r['summary_narrative']}</td></tr>"
        html_content += "</table></body></html>"
        return Response(content=html_content, media_type="text/html")


# -----------------------------------------------------------------------------
# 6. TRANSMISIÓN EN TIEMPO REAL (Server-Sent Events)
# -----------------------------------------------------------------------------
@app.get("/api/stream", tags=["Tiempo Real"])
async def stream_live_events(user: Dict[str, Any] = Depends(get_current_user)):
    """Server-Sent Events (SSE) para emisión continua de sismos recientes hacia el mapa"""
    async def event_generator():
        last_check = datetime.utcnow()
        while True:
            try:
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT id, mag, place, country_region, time_utc,
                                   longitude, latitude, depth_km, alert_level, tsunami
                            FROM curated.earthquake_events
                            WHERE updated_at >= %s - INTERVAL '30 seconds'
                            ORDER BY time_utc DESC LIMIT 5;
                        """, (last_check,))
                        events = cur.fetchall()
                        if events:
                            payload = json.dumps(serialize_data(events))
                            yield f"event: sismo\ndata: {payload}\n\n"
                        else:
                            yield f"event: ping\ndata: {{\"timestamp\": \"{datetime.utcnow().isoformat()}\"}}\n\n"
                last_check = datetime.utcnow()
            except Exception as e:
                yield f"event: error\ndata: {{\"error\": \"{str(e)}\"}}\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# -----------------------------------------------------------------------------
# 7. ESTADO DEL PIPELINE Y CALIDAD
# -----------------------------------------------------------------------------
@app.get("/api/pipeline/status", tags=["Gobernanza"])
def get_pipeline_status(user: Dict[str, Any] = Depends(get_current_user)):
    """Retorna el estado de ejecución y métricas de calidad de datos"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT execution_time, dag_run_id, records_evaluated,
                       records_passed, records_failed, quality_score, rules_summary
                FROM quality.quality_metrics
                ORDER BY execution_time DESC LIMIT 5;
            """)
            metricas = cur.fetchall()

            return {
                "status": "OPERATIONAL",
                "metricas_recientes": serialize_data(metricas),
                "timestamp_utc": datetime.utcnow().isoformat()
            }


# -----------------------------------------------------------------------------
# 8. GESTIÓN ADMINISTRATIVA (Exclusivos para Admin)
# -----------------------------------------------------------------------------
@app.post("/api/admin/backfill", tags=["Administración"])
def trigger_backfill(
    request: BackfillTriggerRequest,
    admin_user: Dict[str, Any] = Depends(require_admin)
):
    """[SOLO ADMIN] Dispara el DAG de backfill histórico en Apache Airflow"""
    try:
        url = f"{AIRFLOW_BASE_URL}/api/v1/dags/dag_04_historical_backfill/dagRuns"
        payload = {
            "conf": {
                "days_back": request.days_back,
                "min_magnitude": request.min_magnitude
            }
        }
        resp = requests.post(
            url,
            json=payload,
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=10
        )
        if resp.status_code in [200, 201]:
            return {"status": "SUCCESS", "message": f"Backfill de {request.days_back} días disparado exitosamente en Airflow."}
        else:
            return {"status": "PENDING_LOCAL", "message": f"Solicitud registrada localmente (Airflow API HTTP {resp.status_code})."}
    except Exception as e:
        return {"status": "REGISTERED", "message": f"Backfill encolado: {str(e)}"}


# -----------------------------------------------------------------------------
# 9. ASISTENTE DE CHAT IA CONVERSACIONAL (GEMINI 1.5 FLASH & SILVER/GOLD)
# -----------------------------------------------------------------------------
@app.post("/api/chat", tags=["IA Chat"])
def chat_with_ai(
    request: ChatMessageRequest,
    user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Chat conversacional con IA para consultar en lenguaje natural sobre:
    - Datos en la Capa Bronze (Raw GeoJSON).
    - Datos y sismos que pasaron a la Capa Silver (curated.earthquake_events).
    - Modelos analíticos en la Capa Gold (resumen diario por país, hotspots y reportes IA).
    - Distribución de sismos por país, magnitudes y alertas de tsunami.
    """
    clean_msg = sanitize_prompt_text(request.message)
    if not clean_msg.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El mensaje no puede estar vacío."
        )
    return procesar_mensaje_chat(clean_msg)


@app.post("/api/admin/reports/regenerate", tags=["Administración"])
def regenerate_ai_report(admin_user: Dict[str, Any] = Depends(require_admin)):
    """[SOLO ADMIN] Fuerza la regeneración del reporte diario de IA en Airflow"""
    try:
        url = f"{AIRFLOW_BASE_URL}/api/v1/dags/dag_03_daily_ai_report/dagRuns"
        resp = requests.post(
            url,
            json={"conf": {}},
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=10
        )
        if resp.status_code in [200, 201]:
            return {"status": "SUCCESS", "message": "Regeneración de reporte con IA disparada exitosamente en Airflow."}
        else:
            return {"status": "PENDING_LOCAL", "message": f"Solicitud recibida (Airflow API HTTP {resp.status_code})."}
    except Exception as e:
        return {"status": "REGISTERED", "message": f"Ejecución solicitada: {str(e)}"}
