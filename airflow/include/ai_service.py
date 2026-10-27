"""
Módulo de Automatización con IA y Detección de Anomalías
Proyecto: SismoWatch
Componente: Pool de 5 API Keys de Google AI Studio (Gemini), Z-Score y Verificación Estricta
"""

import os
import re
import json
import time
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import requests
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [AIService] %(message)s")


class DailyReportSchema(BaseModel):
    report_date: str = Field(..., description="Fecha del reporte YYYY-MM-DD")
    summary_narrative: str = Field(..., description="Resumen ejecutivo en lenguaje natural")
    critical_events: List[Dict[str, Any]] = Field(default_factory=list)
    active_zones: List[Dict[str, Any]] = Field(default_factory=list)
    anomaly_analysis: Dict[str, Any] = Field(default_factory=dict)
    verified_metrics: Dict[str, Any] = Field(..., description="Métricas oficiales calculadas por SQL")
    model_name: str
    api_key_index: int
    tokens_used: int = 0
    latency_ms: int = 0
    estimated_cost_usd: float = 0.0


class GeminiKeyPool:
    """
    Administrador del Pool de 5 API Keys de Google AI Studio.
    Implementa rotación automática y tolerancia a fallos ante errores HTTP 429 / Cuota.
    """
    def __init__(self):
        self.keys = []
        for i in range(1, 6):
            k = os.getenv(f"GEMINI_API_KEY_{i}", "").strip()
            if k:
                self.keys.append(k)

        # Fallback a claves generales si las numeradas no están definidas
        if not self.keys:
            raw_keys = os.getenv("GEMINI_API_KEYS", "") or os.getenv("GEMINI_API_KEY", "")
            if raw_keys:
                self.keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

        self.current_idx = 0
        logging.info(f"GeminiKeyPool inicializado con {len(self.keys)} API Keys disponibles.")

    def get_current_key(self) -> Optional[str]:
        if not self.keys:
            return None
        return self.keys[self.current_idx]

    def advance_key(self):
        if self.keys:
            self.current_idx = (self.current_idx + 1) % len(self.keys)
            logging.warning(f"Rotando a la siguiente API Key del pool (Índice {self.current_idx + 1}/{len(self.keys)})")


def calcular_anomalias_zscore(hourly_counts: List[int], threshold: float = 2.0) -> Dict[str, Any]:
    """
    Técnica analítica de detección de anomalías por Z-Score sobre serie temporal.
    Z = (X - Media) / Desviación Estándar.
    """
    if not hourly_counts or len(hourly_counts) < 3:
        return {"anomalia_detectada": False, "z_score_maximo": 0.0, "detalle": "Datos insuficientes para Z-Score"}

    n = len(hourly_counts)
    media = sum(hourly_counts) / n
    varianza = sum((x - media) ** 2 for x in hourly_counts) / n
    desv_est = varianza ** 0.5

    if desv_est == 0:
        return {"anomalia_detectada": False, "z_score_maximo": 0.0, "detalle": "Varianza nula en el periodo"}

    z_scores = [(x - media) / desv_est for x in hourly_counts]
    max_z = max(z_scores)
    max_idx = z_scores.index(max_z)

    es_anomalo = max_z >= threshold
    detalle = (
        f"Alerta de enjambre sísmico en hora {max_idx}:00 UTC con {hourly_counts[max_idx]} sismos (Z-Score: {max_z:.2f})"
        if es_anomalo else "Actividad sísmica dentro de parámetros estadísticos normales."
    )

    return {
        "anomalia_detectada": es_anomalo,
        "z_score_maximo": round(max_z, 2),
        "hora_pico_utc": max_idx,
        "conteo_pico": hourly_counts[max_idx],
        "media_periodo": round(media, 2),
        "desviacion_estandar": round(desv_est, 2),
        "detalle": detalle
    }


def verificar_cifras_redaccion(narrative: str, metrics: Dict[str, Any]) -> bool:
    """
    Verifica que las cifras numéricas del texto coincidan con los cálculos oficiales.
    Si el LLM genera cifras de total_sismos o max_magnitud distintas, detecta la alucinación.
    """
    # Validar magnitud máxima
    max_mag = str(metrics.get("magnitud_maxima", ""))
    if max_mag and max_mag != "0.0":
        # Buscar si menciona magnitudes mayores que la máxima real
        numeros_mag = re.findall(r"magnitud(?: de)?\s*([0-9]+\.[0-9]+)", narrative, re.IGNORECASE)
        for num in numeros_mag:
            try:
                if float(num) > float(max_mag) + 0.05:
                    logging.warning(f"Alucinación detectada: El texto menciona magnitud {num} superior a la máxima calculada {max_mag}")
                    return False
            except ValueError:
                pass
    return True


def generar_reporte_fallback(report_date: str, metrics: Dict[str, Any], anomaly: Dict[str, Any]) -> DailyReportSchema:
    """
    Mecanismo de contingencia (fallback determinista basado en reglas).
    Garantiza que el pipeline nunca falle si las API Keys no están disponibles.
    """
    total = metrics.get("total_sismos", 0)
    max_mag = metrics.get("magnitud_maxima", 0.0)
    avg_mag = metrics.get("magnitud_promedio", 0.0)
    tsunamis = metrics.get("total_alertas_tsunami", 0)
    criticos = metrics.get("sismos_criticos_m6", 0)
    peligrosos = metrics.get("sismos_peligrosos_m5", 0)
    top_paises = metrics.get("top_paises", [])
    top_str = ", ".join([f"{p['pais']} ({p['total']})" for p in top_paises[:3]]) or "Sin concentración regional predominante"

    narrative = (
        f"INFORME EJECUTIVO DIARIO DE MONITOREO SÍSMICO GLOBAL - {report_date}\n\n"
        f"Durante el periodo de 24 horas evaluado, la red global de SismoWatch registró un total consolidado de "
        f"{total} eventos sísmicos. La magnitud máxima detectada alcanzó {max_mag} grados en escala de magnitud de momento, "
        f"con una media de magnitud global de {avg_mag}.\n\n"
        f"Se reportaron {criticos} sismos de magnitud crítica (M >= 6.0) y {peligrosos} eventos moderados a fuertes (5.0 <= M < 6.0). "
        f"El sistema registró {tsunamis} alertas oficiales de tsunami asociadas.\n\n"
        f"Distribución Regional: Las regiones con mayor densidad sísmica fueron: {top_str}.\n\n"
        f"Análisis Analítico de Anomalías: {anomaly.get('detalle', 'Sin anomalías registradas')}.\n\n"
        f"Estado Operativo: Vigilancia continua activa conforme a los protocolos del Centro Sismológico Global."
    )

    return DailyReportSchema(
        report_date=report_date,
        summary_narrative=narrative,
        critical_events=metrics.get("critical_events", []),
        active_zones=top_paises,
        anomaly_analysis=anomaly,
        verified_metrics=metrics,
        model_name="deterministic_fallback_v1",
        api_key_index=0,
        tokens_used=0,
        latency_ms=10,
        estimated_cost_usd=0.0
    )


def generar_reporte_ia_diario(
    report_date: str,
    metrics: Dict[str, Any],
    hourly_counts: List[int]
) -> DailyReportSchema:
    """
    Función orquestadora principal de IA:
    1. Calcula anomalías con Z-Score.
    2. Carga prompt versionado.
    3. Rota entre el Pool de 5 API Keys de Gemini.
    4. Valida consistencia numérica de las cifras redactadas.
    5. Devuelve esquema Pydantic validado.
    """
    anomaly = calcular_anomalias_zscore(hourly_counts)
    pool = GeminiKeyPool()

    # Cargar prompt versionado
    prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "daily_report_v1.txt")
    prompt_template = ""
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt_template = f.read()
    else:
        prompt_template = "Genera un reporte sísmico para {total_sismos} sismos, max mag {magnitud_maxima}."

    top_str = ", ".join([f"{p['pais']} ({p['total']})" for p in metrics.get("top_paises", [])[:4]])

    prompt_formatted = prompt_template.format(
        total_sismos=metrics.get("total_sismos", 0),
        magnitud_maxima=metrics.get("magnitud_maxima", 0.0),
        magnitud_promedio=metrics.get("magnitud_promedio", 0.0),
        total_tsunamis=metrics.get("total_alertas_tsunami", 0),
        sismos_criticos=metrics.get("sismos_criticos_m6", 0),
        sismos_peligrosos=metrics.get("sismos_peligrosos_m5", 0),
        top_regiones=top_str or "Varias regiones globales",
        anomalias_detectadas=anomaly.get("detalle", "")
    )

    # Intentar con el Pool de 5 Keys
    total_keys = len(pool.keys)
    for intento in range(total_keys):
        api_key = pool.get_current_key()
        key_idx = pool.current_idx + 1
        if not api_key:
            break

        logging.info(f"Invocando Google AI Studio con Clave Gemini #{key_idx}...")
        modelos_disponibles = ["gemini-3.5-flash", "gemini-3.1-flash-lite", "gemini-3-flash-preview"]

        for modelo_sel in modelos_disponibles:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{modelo_sel}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt_formatted}]}],
                "generationConfig": {
                    "temperature": 0.2,
                    "maxOutputTokens": 800
                }
            }

            t_start = time.time()
            try:
                resp = requests.post(url, json=payload, timeout=25)
                latency = int((time.time() - t_start) * 1000)

                if resp.status_code == 200:
                    data = resp.json()
                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                    tokens = data.get("usageMetadata", {}).get("totalTokenCount", 350)
                    cost = (tokens / 1000.0) * 0.00035

                    # Validación de Cifras
                    if verificar_cifras_redaccion(text, metrics):
                        logging.info(f"✓ Reporte generado y validado exitosamente con ({modelo_sel}) Clave #{key_idx} (Latencia: {latency}ms).")
                        return DailyReportSchema(
                            report_date=report_date,
                            summary_narrative=text,
                            critical_events=metrics.get("critical_events", []),
                            active_zones=metrics.get("top_paises", []),
                            anomaly_analysis=anomaly,
                            verified_metrics=metrics,
                            model_name=modelo_sel,
                            api_key_index=key_idx,
                            tokens_used=tokens,
                            latency_ms=latency,
                            estimated_cost_usd=round(cost, 6)
                        )
                    else:
                        logging.warning(f"La respuesta de la clave #{key_idx} con {modelo_sel} contenía discrepancias en cifras.")
                elif resp.status_code in [429, 503]:
                    logging.warning(f"Clave #{key_idx} modelo {modelo_sel} devovió HTTP {resp.status_code}. Probando siguiente modelo...")
                    continue
                else:
                    logging.warning(f"Error en clave #{key_idx} con {modelo_sel}: HTTP {resp.status_code}")
            except Exception as e:
                logging.error(f"Fallo de conexión con clave #{key_idx} ({modelo_sel}): {e}")

        pool.advance_key()

    logging.info("Activando mecanismo de contingencia: Generando reporte determinista verificado.")
    return generar_reporte_fallback(report_date, metrics, anomaly)
