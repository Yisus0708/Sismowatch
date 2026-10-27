#!/usr/bin/env python3
"""
generar_pdf_manual_completo.py
Genera el Manual Técnico y Operativo Integral de SismoWatch:
Arquitectura Medallón, Flujo de Datos, API Externa de Sismos, Docker y Gobernanza.
Destino: /home/cohorte5/Escritorio/MANUAL_SISMOWATCH_ARQUITECTURA_Y_FLUJO.pdf
"""

import os
import sys
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether, HRFlowable
)
from reportlab.pdfgen import canvas

PDF_DEST = "/home/cohorte5/Escritorio/MANUAL_SISMOWATCH_ARQUITECTURA_Y_FLUJO.pdf"

class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super().showPage()
        super().save()

    def draw_page_decorations(self, page_count):
        self.saveState()
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748b"))

        # Encabezado (a partir de página 2)
        if self._pageNumber > 1:
            self.drawString(54, 750, "SismoWatch • Manual Integral de Arquitectura, Flujo de Datos y Operaciones Docker")
            self.setStrokeColor(colors.HexColor("#cbd5e1"))
            self.setLineWidth(0.5)
            self.line(54, 744, 558, 744)

        # Pie de página
        page_text = f"Página {self._pageNumber} de {page_count}"
        self.drawRightString(558, 36, page_text)
        self.drawString(54, 36, "SismoWatch • Entorno 100% Local en Ubuntu • Medallion Architecture")
        self.setStrokeColor(colors.HexColor("#cbd5e1"))
        self.setLineWidth(0.5)
        self.line(54, 48, 558, 48)
        self.restoreState()


def build_pdf():
    os.makedirs(os.path.dirname(PDF_DEST), exist_ok=True)
    doc = SimpleDocTemplate(
        PDF_DEST,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54
    )

    styles = getSampleStyleSheet()

    # Paleta de Colores
    c_primary = colors.HexColor("#0f172a")    # Slate 900
    c_accent = colors.HexColor("#0284c7")     # Sky 600
    c_emerald = colors.HexColor("#059669")    # Emerald 600
    c_purple = colors.HexColor("#7c3aed")     # Violet 600
    c_amber = colors.HexColor("#d97706")      # Amber 600
    c_dark = colors.HexColor("#1e293b")       # Slate 800
    c_light = colors.HexColor("#f8fafc")      # Slate 50
    c_border = colors.HexColor("#e2e8f0")     # Slate 200

    title_style = ParagraphStyle(
        "CoverTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=24,
        leading=30,
        textColor=c_primary,
        alignment=0,
        spaceAfter=10
    )

    subtitle_style = ParagraphStyle(
        "CoverSubtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=12,
        leading=16,
        textColor=c_accent,
        alignment=0,
        spaceAfter=20
    )

    h1_style = ParagraphStyle(
        "H1",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=19,
        textColor=c_primary,
        spaceBefore=14,
        spaceAfter=8,
        keepWithNext=True
    )

    h2_style = ParagraphStyle(
        "H2",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=15,
        textColor=c_purple,
        spaceBefore=10,
        spaceAfter=4,
        keepWithNext=True
    )

    body_style = ParagraphStyle(
        "Body",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13.5,
        textColor=c_dark,
        spaceAfter=6
    )

    body_bold = ParagraphStyle(
        "BodyBold",
        parent=body_style,
        fontName="Helvetica-Bold"
    )

    callout_style = ParagraphStyle(
        "Callout",
        parent=styles["Normal"],
        fontName="Helvetica-Oblique",
        fontSize=8.5,
        leading=12.5,
        textColor=colors.HexColor("#334155")
    )

    code_style = ParagraphStyle(
        "CodeText",
        parent=styles["Normal"],
        fontName="Courier",
        fontSize=8,
        leading=10.5,
        textColor=colors.HexColor("#0f172a")
    )

    table_header = ParagraphStyle(
        "TH",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10,
        textColor=colors.white,
        alignment=1
    )

    table_cell = ParagraphStyle(
        "TC",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10.5,
        textColor=c_dark
    )

    story = []

    # =========================================================================
    # PORTADA
    # =========================================================================
    story.append(Spacer(1, 20))
    story.append(Paragraph("SISMOWATCH • PLATAFORMA GEOFÍSICA GLOBAL", subtitle_style))
    story.append(Paragraph("Manual Integral de Arquitectura, Flujo de Datos, API de Sismos y Operaciones Docker", title_style))
    story.append(HRFlowable(width="100%", thickness=3, color=c_accent, spaceBefore=4, spaceAfter=14))

    intro_box = [
        [Paragraph("<b>Propósito del Documento:</b> Este manual explica de forma exhaustiva, literal y paso a paso cómo funciona el sistema SismoWatch en su totalidad: el origen y consumo de la API pública de sismos de USGS, la arquitectura de datos Medallón (Bronze, Silver y Gold), el motor de cómputo Spark, la orquestación en Airflow, la automatización con IA (Gemini), la gobernanza en OpenMetadata y la operación de los 12 contenedores Docker.", callout_style)]
    ]
    t_intro = Table(intro_box, colWidths=[504])
    t_intro.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#f0f9ff")),
        ('BORDER', (0,0), (-1,-1), 1, colors.HexColor("#bae6fd")),
        ('LEFTPADDING', (0,0), (-1,-1), 12),
        ('RIGHTPADDING', (0,0), (-1,-1), 12),
        ('TOPPADDING', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
    ]))
    story.append(t_intro)
    story.append(Spacer(1, 15))

    meta_data = [
        [Paragraph("<b>Fecha de Emisión:</b>", table_cell), Paragraph(datetime.now().strftime("%d/%m/%Y %H:%M UTC"), table_cell)],
        [Paragraph("<b>Ambiente:</b>", table_cell), Paragraph("100% Local en Ubuntu (Docker Compose v2)", table_cell)],
        [Paragraph("<b>Autor / Lead Engineer:</b>", table_cell), Paragraph("Yisus0708 (Lead Data & Software Engineer)", table_cell)],
        [Paragraph("<b>Repositorio Oficial:</b>", table_cell), Paragraph("https://github.com/Yisus0708/Sismowatch", table_cell)],
        [Paragraph("<b>Stack Principal:</b>", table_cell), Paragraph("Airflow 2.9, Spark 3.5, MinIO S3, PostgreSQL 16, OpenMetadata 2.0.6, FastAPI, Gemini 3.5 Flash", table_cell)],
    ]
    t_meta = Table(meta_data, colWidths=[130, 374])
    t_meta.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#ffffff")),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
    ]))
    story.append(t_meta)
    story.append(Spacer(1, 20))

    # =========================================================================
    # CAPÍTULO 1: LA FUENTE DE DATOS Y LA API DE SISMOS (USGS)
    # =========================================================================
    story.append(Paragraph("1. ¿De Dónde Vienen los Datos? La API Externa de Sismos", h1_style))
    story.append(Paragraph(
        "La plataforma SismoWatch no inventa datos sísmicos ni utiliza datos simulados; consume telemetría geofísica oficial en tiempo real provista por el <b>Servicio Geológico de los Estados Unidos (USGS - United States Geological Survey)</b>.",
        body_style
    ))

    api_info = [
        [Paragraph("Parámetro", table_header), Paragraph("Detalle Técnico Oficial", table_header)],
        [Paragraph("<b>Proveedor</b>", table_cell), Paragraph("USGS Earthquake Hazards Program (Gobierno de EE. UU.)", table_cell)],
        [Paragraph("<b>Endpoint en Tiempo Real</b>", table_cell), Paragraph("<code>https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson</code>", table_cell)],
        [Paragraph("<b>Frecuencia de Actualización</b>", table_cell), Paragraph("Se actualiza en los servidores de USGS cada 1 minuto con sismos mundiales.", table_cell)],
        [Paragraph("<b>Endpoint Histórico (Backfill)</b>", table_cell), Paragraph("<code>https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=...</code>", table_cell)],
        [Paragraph("<b>Formato de Respuesta</b>", table_cell), Paragraph("GeoJSON estándar RFC 7946 (FeatureCollection con lista de sismos).", table_cell)],
        [Paragraph("<b>Campos Clave Capturados</b>", table_cell), Paragraph("<code>id</code> (único), <code>mag</code> (magnitud), <code>place</code> (zona), <code>time</code> (timestamp ms), <code>coordinates</code> (longitud, latitud, profundidad km), <code>tsunami</code> (0 o 1).", table_cell)],
        [Paragraph("<b>¿Dónde está en el código?</b>", table_cell), Paragraph("En el script del DAG de Airflow: <b><code>airflow/dags/dag_01_ingest_usgs.py</code></b> (líneas 45 a 85) y en el DAG de Backfill <b><code>airflow/dags/dag_04_historical_backfill.py</code></b>.", table_cell)],
    ]
    t_api = Table(api_info, colWidths=[140, 364])
    t_api.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_primary),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('BACKGROUND', (0,1), (-1,-1), c_light),
    ]))
    story.append(t_api)
    story.append(Spacer(1, 12))

    story.append(Paragraph(
        "<b>Mecanismo de Ingesta:</b> Cada 5 minutos, la tarea <code>ingestar_feed_usgs_a_bronze</code> de Airflow ejecuta un HTTP GET al feed público de USGS, extrae el lote GeoJSON, valida que no esté vacío y lo almacena inmediatamente en dos destinos inmutables: el Data Lake MinIO S3 (en el bucket <code>bronze</code> particionado por fecha) y la tabla relacional PostgreSQL <code>raw.earthquake_events_raw</code>.",
        body_style
    ))

    story.append(PageBreak())

    # =========================================================================
    # CAPÍTULO 2: ARQUITECTURA MEDALLÓN COMPLETA (BRONZE -> SILVER -> GOLD)
    # =========================================================================
    story.append(Paragraph("2. Arquitectura de Datos Medallón (Sin Cuarentena ni Landing)", h1_style))
    story.append(Paragraph(
        "SismoWatch implementa una arquitectura <b>Medallion Directa y Limpia</b> dividida en tres capas progresivas de calidad y refinamiento:",
        body_style
    ))

    medallion_table = [
        [Paragraph("Capa", table_header), Paragraph("Almacenamiento S3 / DB", table_header), Paragraph("Formato & Regla de Negocio", table_header), Paragraph("Uso y Consumo", table_header)],
        [
            Paragraph("<b>🥉 BRONZE<br/>(Datos Crudos)</b>", table_cell),
            Paragraph("MinIO Bucket: <code>bronze</code><br/>Postgres: <code>raw.earthquake_events_raw</code>", table_cell),
            Paragraph("JSON crudo inmutable. No se modifica, limpia ni descarta ningún registro. Auditoría histórica completa.", table_cell),
            Paragraph("Punto de entrada de Airflow. Fuente para reprocesamiento masivo.", table_cell)
        ],
        [
            Paragraph("<b>🥈 SILVER<br/>(Limpio & Tipado)</b>", table_cell),
            Paragraph("MinIO Bucket: <code>silver</code><br/>Postgres: <code>curated.earthquake_events</code>", table_cell),
            Paragraph("Parquet Snappy columnar particionado. Tipado estricto (Decimal, Timestamp UTC), deduplicación por <code>id</code> y descarte de anomalías nulas.", table_cell),
            Paragraph("Alimentación de mapas GIS en tiempo real y cálculo de métricas de calidad.", table_cell)
        ],
        [
            Paragraph("<b>🥇 GOLD<br/>(Modelos Analíticos)</b>", table_cell),
            Paragraph("MinIO Bucket: <code>gold</code><br/>Postgres:<br/>• <code>curated.resumen_diario_pais</code><br/>• <code>curated.zonas_criticas_frecuentes</code><br/>• <code>curated.ai_daily_reports</code>", table_cell),
            Paragraph("Modelos agregados por día y país, cuadrículas espaciales de 1°x1° para hotspots de fallas tectónicas, e informes narrativos con IA.", table_cell),
            Paragraph("Consumo analítico ejecutivo, dashboard web, gráficos y reportes de Inteligencia Artificial.", table_cell)
        ],
    ]
    t_med = Table(medallion_table, colWidths=[90, 140, 150, 124])
    t_med.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_purple),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, c_light]),
    ]))
    story.append(t_med)
    story.append(Spacer(1, 10))

    story.append(Paragraph(
        "<b>¿Por qué no hay cuarentena ni landing?</b> En este diseño arquitectónico, el feed de entrada se deposita directamente en Bronze (eliminando intermediarios de landing innecesarios). Los registros no conformes se auditan cuantitativamente mediante la tabla <code>quality.quality_metrics</code> (registrando el puntaje de calidad porcentual de cada lote), permitiendo que Silver mantenga exclusivamente datos puros sin sobrecostos de almacenamiento en cuarentena.",
        callout_style
    ))
    story.append(Spacer(1, 14))

    # =========================================================================
    # CAPÍTULO 3: FLUJO COMPLETO DE DATOS (END-TO-END)
    # =========================================================================
    story.append(Paragraph("3. Flujo Completo de Datos Paso a Paso (End-to-End)", h1_style))
    story.append(Paragraph(
        "Desde que la tierra tiembla en cualquier lugar del planeta hasta que aparece en tu pantalla:",
        body_style
    ))

    flow_steps = [
        [Paragraph("Paso", table_header), Paragraph("Componente", table_header), Paragraph("Acción Realizada en el Sistema", table_header)],
        [Paragraph("<b>1</b>", table_cell), Paragraph("<b>Sensor USGS</b>", table_cell), Paragraph("Sismógrafos mundiales detectan ondas P/S y publican el evento en la API pública de USGS.", table_cell)],
        [Paragraph("<b>2</b>", table_cell), Paragraph("<b>Airflow DAG 1</b>", table_cell), Paragraph("Descarga el feed GeoJSON y escribe en MinIO S3 <code>bronze/</code> y Postgres <code>raw</code>.", table_cell)],
        [Paragraph("<b>3</b>", table_cell), Paragraph("<b>Airflow DAG 2</b>", table_cell), Paragraph("Dispara el clúster Apache Spark mediante HTTP POST al Trigger Server (:5050).", table_cell)],
        [Paragraph("<b>4</b>", table_cell), Paragraph("<b>Apache Spark</b>", table_cell), Paragraph("Lee Bronze, valida reglas de calidad, genera Parquet Snappy en <code>silver/</code> y hace upsert en Postgres <code>curated.earthquake_events</code>.", table_cell)],
        [Paragraph("<b>5</b>", table_cell), Paragraph("<b>Airflow DAG 3</b>", table_cell), Paragraph("Calcula Z-Score para detectar anomalías, consulta a Gemini 3.5 Flash y genera el reporte diario en <code>curated.ai_daily_reports</code>.", table_cell)],
        [Paragraph("<b>6</b>", table_cell), Paragraph("<b>FastAPI (:8000)</b>", table_cell), Paragraph("Sirve endpoints REST seguros con JWT y transmite sismos en vivo vía Server-Sent Events (SSE).", table_cell)],
        [Paragraph("<b>7</b>", table_cell), Paragraph("<b>Frontend (:3000)</b>", table_cell), Paragraph("Nginx renderiza el mapa táctico Leaflet con ArcGIS Dark Gray Canvas, actualiza KPIs y alimenta el Chat IA.", table_cell)],
        [Paragraph("<b>8</b>", table_cell), Paragraph("<b>OpenMetadata (:8585)</b>", table_cell), Paragraph("Mantiene el catálogo de datos, dueños de servicio y el grafo de linaje de extremo a extremo.", table_cell)],
    ]
    t_flow = Table(flow_steps, colWidths=[36, 120, 348])
    t_flow.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_accent),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, c_light]),
    ]))
    story.append(t_flow)

    story.append(PageBreak())

    # =========================================================================
    # CAPÍTULO 4: DIAGNÓSTICO DOCKER: CONTENEDORES Y SERVICIOS
    # =========================================================================
    story.append(Paragraph("4. Diagnóstico de Docker: ¿Por qué parece que algo está apagado?", h1_style))
    story.append(Paragraph(
        "Es muy común confundirse al ver el comando <code>docker ps -a</code> o la interfaz de Docker Desktop. Aquí aclaramos exactamente cómo funciona la infraestructura:",
        body_style
    ))

    story.append(Paragraph("A. Diferencia entre Imágenes y Contenedores", h2_style))
    story.append(Paragraph(
        "• <b>Imágenes de Docker (<code>docker images</code>):</b> Son archivos estáticos inmutables en disco (como los instaladores o planos de un edificio). Las imágenes no se 'prenden' ni se 'apagan'; simplemente existen en el almacenamiento.<br/>"
        "• <b>Contenedores de Docker (<code>docker ps</code>):</b> Son las instancias de ejecución vivas creadas a partir de esas imágenes (como los procesos de software en memoria RAM).",
        body_style
    ))

    story.append(Paragraph("B. Los 12 Contenedores Principales de SismoWatch (Están 100% Encendidos)", h2_style))

    containers_table = [
        [Paragraph("Contenedor", table_header), Paragraph("Puerto Host", table_header), Paragraph("Rol en la Plataforma", table_header), Paragraph("Estado", table_header)],
        [Paragraph("<code>sismowatch_web</code>", table_cell), Paragraph("<b>:3000</b>", table_cell), Paragraph("Frontend Nginx: Dashboard Mission Control, mapa Leaflet y Chat IA.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_api</code>", table_cell), Paragraph("<b>:8000</b>", table_cell), Paragraph("Backend FastAPI: API REST, JWT, SSE y conector con Gemini.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_airflow_webserver</code>", table_cell), Paragraph("<b>:8080</b>", table_cell), Paragraph("Interfaz gráfica de Airflow para monitoreo de DAGs.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_airflow_scheduler</code>", table_cell), Paragraph("Interno", table_cell), Paragraph("Planificador que dispara los DAGs 1, 2, 3 y 4 según su cronograma.", table_cell), Paragraph("<font color='#059669'><b>Up</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_spark_master</code>", table_cell), Paragraph("<b>:7077 / :5050 / :8085</b>", table_cell), Paragraph("Master de Spark, Trigger Server HTTP y Web UI de Spark.", table_cell), Paragraph("<font color='#059669'><b>Up</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_spark_worker</code>", table_cell), Paragraph("Interno", table_cell), Paragraph("Nodo ejecutor de PySpark (2 cores, 2GB de memoria asignada).", table_cell), Paragraph("<font color='#059669'><b>Up</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_minio</code>", table_cell), Paragraph("<b>:9000 / :9001</b>", table_cell), Paragraph("Data Lake Medallón S3 (Almacena buckets bronze, silver, gold).", table_cell), Paragraph("<font color='#059669'><b>Up</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_postgres</code>", table_cell), Paragraph("<b>:5432</b>", table_cell), Paragraph("Base de datos Serving Layer (esquemas raw, curated, quality).", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_om_server</code>", table_cell), Paragraph("<b>:8585</b>", table_cell), Paragraph("Servidor principal de Gobernanza y Linaje OpenMetadata.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_om_elasticsearch</code>", table_cell), Paragraph("Interno (:9200)", table_cell), Paragraph("Motor de búsqueda indexada para OpenMetadata.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_om_postgres</code>", table_cell), Paragraph("Interno (:5432)", table_cell), Paragraph("Base de datos de metadatos internos de OpenMetadata.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
        [Paragraph("<code>sismowatch_airflow_db</code>", table_cell), Paragraph("Interno (:5432)", table_cell), Paragraph("Base de datos de metadatos de Apache Airflow.", table_cell), Paragraph("<font color='#059669'><b>Up (healthy)</b></font>", table_cell)],
    ]
    t_con = Table(containers_table, colWidths=[130, 80, 204, 90])
    t_con.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_dark),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, c_light]),
    ]))
    story.append(t_con)
    story.append(Spacer(1, 10))

    story.append(Paragraph("C. ¿Por qué algunos contenedores dicen `Exited (0)`?", h2_style))
    story.append(Paragraph(
        "En Docker Compose, existen contenedores llamados <b>Init Containers</b>. Su propósito es ejecutar una tarea única al encender el sistema (por ejemplo, crear los buckets de MinIO, aplicar migraciones de base de datos o registrar usuarios). Cuando terminan su labor con éxito, se apagan inmediatamente con código <b><code>Exited (0)</code></b>.<br/>"
        "• <code>sismowatch_airflow_init</code>: Inicializó la base de datos de Airflow y terminó.<br/>"
        "• <code>sismowatch_minio_buckets</code>: Creó los buckets <code>bronze</code>, <code>silver</code> y <code>gold</code> y terminó.<br/>"
        "• <code>sismowatch_om_migrate</code>: Aplicó el esquema DDL de OpenMetadata y terminó.<br/>"
        "• <code>sismowatch_om_auto_catalog</code>: Ejecutó el script de auto-catalogación y terminó.<br/>"
        "<b>Conclusión:</b> Que digan <code>Exited (0)</code> es señal de que <b>todo se instaló a la perfección</b>.",
        body_style
    ))

    story.append(PageBreak())

    # =========================================================================
    # CAPÍTULO 5: INTELIGENCIA ARTIFICIAL Y CHAT (GEMINI 3.5 FLASH)
    # =========================================================================
    story.append(Paragraph("5. Inteligencia Artificial: Google Gemini 3.5 Flash", h1_style))
    story.append(Paragraph(
        "SismoWatch integra Inteligencia Artificial Generativa y Analítica en dos puntos clave de la arquitectura:",
        body_style
    ))

    story.append(Paragraph("A. Asistente de Chat en Tiempo Real (`POST /api/chat`)", h2_style))
    story.append(Paragraph(
        "En el dashboard web ([http://localhost:3000](http://localhost:3000)), el panel derecho del Copiloto IA permite hacer preguntas libres en lenguaje natural como:<br/>"
        "• <i>'¿Qué datos pasaron a Silver o a Gold?'</i><br/>"
        "• <i>'¿En qué países ocurrieron los sismos más fuertes hoy?'</i><br/>"
        "• <i>'¿Hubo alguna alerta de tsunami activa?'</i><br/><br/>"
        "<b>¿Cómo funciona por dentro?</b> El backend FastAPI intercepta tu pregunta, ejecuta consultas SQL en milisegundos a las tablas de PostgreSQL (contando registros en Bronze, sismos curados en Silver, resúmenes en Gold y magnitudes pico) e inyecta esa verdad matemática en el prompt del modelo <b>Gemini 3.5 Flash</b>. De este modo, la IA nunca inventa datos ni alucina.",
        body_style
    ))

    story.append(Paragraph("B. Pool de 5 Claves con Failover Automático", h2_style))
    story.append(Paragraph(
        "Para evitar bloqueos por cuota de uso gratuito (código HTTP 429), el módulo <code>api/ai_chat.py</code> y el servicio <code>airflow/include/ai_service.py</code> implementan la clase <code>GeminiKeyPool</code> que rota secuencialmente entre las 5 API Keys configuradas en el archivo <code>.env</code>. Si una clave llega a su límite, conmuta a la siguiente en menos de 50 milisegundos de forma transparente.",
        body_style
    ))

    story.append(Paragraph("C. Detección Analítica de Anomalías (Z-Score)", h2_style))
    story.append(Paragraph(
        "En el DAG 3 de Airflow, antes de redactar el reporte geofísico diario, el sistema calcula el puntaje estadístico Z-Score sobre la serie temporal horaria de sismos. Si el Z-Score supera 2.0 ($Z = (X - \mu) / \sigma$), el sistema detecta un enjambre sísmico anormal y le ordena a Gemini priorizar dicha alerta en el informe ejecutivo.",
        body_style
    ))

    story.append(Spacer(1, 14))

    # =========================================================================
    # CAPÍTULO 6: GUÍA OPERATIVA RÁPIDA (COMANDOS Y CREDENCIALES)
    # =========================================================================
    story.append(Paragraph("6. Guía Operativa: Comandos Clave y Accesos Rápidos", h1_style))

    commands_info = [
        [Paragraph("Objetivo", table_header), Paragraph("Comando de Terminal (en la carpeta del proyecto)", table_header)],
        [Paragraph("<b>Iniciar todo el sistema</b>", table_cell), Paragraph("<code>docker compose up -d</code>", code_style)],
        [Paragraph("<b>Verificar estado de salud</b>", table_cell), Paragraph("<code>docker compose ps</code>", code_style)],
        [Paragraph("<b>Detener todo el sistema</b>", table_cell), Paragraph("<code>docker compose down</code>", code_style)],
        [Paragraph("<b>Ver logs de la API</b>", table_cell), Paragraph("<code>docker compose logs -f api</code>", code_style)],
        [Paragraph("<b>Ver logs de Airflow</b>", table_cell), Paragraph("<code>docker compose logs -f airflow-scheduler</code>", code_style)],
        [Paragraph("<b>Ejecutar suite de tests</b>", table_cell), Paragraph("<code>docker run --rm -v $(pwd):/workspace -w /workspace prueba_jesus-api:latest pytest tests/ -v</code>", code_style)],
        [Paragraph("<b>Re-catalogar OpenMetadata</b>", table_cell), Paragraph("<code>OM_SERVER_URL=\"http://localhost:8585/api/v1\" python3 governance/catalogar_servicios.py</code>", code_style)],
    ]
    t_cmd = Table(commands_info, colWidths=[150, 354])
    t_cmd.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_emerald),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, c_light]),
    ]))
    story.append(t_cmd)
    story.append(Spacer(1, 12))

    story.append(Paragraph("Tabla de Enlaces y Credenciales Rápidas:", h2_style))

    access_table = [
        [Paragraph("Módulo / Interfaz", table_header), Paragraph("URL Local", table_header), Paragraph("Credenciales de Acceso", table_header)],
        [Paragraph("<b>Dashboard Mission Control</b>", table_cell), Paragraph("http://localhost:3000", table_cell), Paragraph("Botón <b>Acceso Rápido Admin</b> (admin / admin123)", table_cell)],
        [Paragraph("<b>Backend API & Swagger</b>", table_cell), Paragraph("http://localhost:8000/docs", table_cell), Paragraph("Documentación OpenAPI interactiva", table_cell)],
        [Paragraph("<b>Apache Airflow UI</b>", table_cell), Paragraph("http://localhost:8080", table_cell), Paragraph("Usuario: <b>admin</b> | Contraseña: <b>admin</b>", table_cell)],
        [Paragraph("<b>OpenMetadata 2.0.6</b>", table_cell), Paragraph("http://localhost:8585", table_cell), Paragraph("Correo: <b>admin@open-metadata.org</b> | Pass: <b>admin</b>", table_cell)],
        [Paragraph("<b>MinIO Object Browser</b>", table_cell), Paragraph("http://localhost:9001", table_cell), Paragraph("Usuario: <b>minioadmin</b> | Pass: <b>minioadmin</b>", table_cell)],
        [Paragraph("<b>Spark Master Web UI</b>", table_cell), Paragraph("http://localhost:8085", table_cell), Paragraph("Sin autenticación (Métricas del cluster Spark)", table_cell)],
    ]
    t_acc = Table(access_table, colWidths=[150, 160, 194])
    t_acc.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_primary),
        ('GRID', (0,0), (-1,-1), 0.5, c_border),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, c_light]),
    ]))
    story.append(t_acc)

    # Construcción final del PDF
    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"✓ Manual PDF generado exitosamente en: {PDF_DEST}")


if __name__ == "__main__":
    build_pdf()
