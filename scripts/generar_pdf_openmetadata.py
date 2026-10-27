#!/usr/bin/env python3
"""
generar_pdf_openmetadata.py
Genera un manual técnico y ejecutivo en PDF sobre cómo funciona OpenMetadata 2.0.6
en la arquitectura SismoWatch y cómo verificar que todo esté 100% operativo.
Destino: /home/cohorte5/Escritorio/MANUAL_OPENMETADATA_SISMOWATCH.pdf
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

PDF_DEST = "/home/cohorte5/Escritorio/MANUAL_OPENMETADATA_SISMOWATCH.pdf"

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
            self.drawString(54, 750, "SismoWatch • Plataforma de Gobernanza y Linaje OpenMetadata 2.0.6")
            self.setStrokeColor(colors.HexColor("#cbd5e1"))
            self.setLineWidth(0.5)
            self.line(54, 744, 558, 744)

        # Pie de página
        page_text = f"Página {self._pageNumber} de {page_count}"
        self.drawRightString(558, 36, page_text)
        self.drawString(54, 36, "Confidencial • Entorno 100% Local • Arquitectura Medallón SismoWatch")
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

    # Estilos personalizados
    c_primary = colors.HexColor("#0f172a")    # Slate 900
    c_accent = colors.HexColor("#0284c7")     # Sky 600
    c_emerald = colors.HexColor("#059669")    # Emerald 600
    c_purple = colors.HexColor("#7c3aed")     # Violet 600
    c_dark = colors.HexColor("#1e293b")

    title_style = ParagraphStyle(
        "CoverTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=24,
        leading=28,
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
        "Header1",
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
        "Header2",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=16,
        textColor=c_accent,
        spaceBefore=10,
        spaceAfter=6,
        keepWithNext=True
    )

    body_style = ParagraphStyle(
        "BodyDark",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=14,
        textColor=c_dark,
        spaceAfter=6
    )

    bullet_style = ParagraphStyle(
        "BulletItem",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13.5,
        textColor=c_dark,
        leftIndent=14,
        spaceAfter=4
    )

    callout_style = ParagraphStyle(
        "CalloutText",
        parent=styles["Normal"],
        fontName="Helvetica-Oblique",
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#065f46")
    )

    table_header_style = ParagraphStyle(
        "TableHeader",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8.5,
        leading=11,
        textColor=colors.white
    )

    table_cell_style = ParagraphStyle(
        "TableCell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10.5,
        textColor=c_dark
    )

    table_cell_bold = ParagraphStyle(
        "TableCellBold",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10.5,
        textColor=c_dark
    )

    story = []

    # =========================================================================
    # PORTADA
    # =========================================================================
    story.append(Spacer(1, 20))
    story.append(Paragraph("MANUAL DE GOBERNANZA, CATÁLOGO Y LINAJE", title_style))
    story.append(Paragraph("OpenMetadata 2.0.6 en la Arquitectura SismoWatch • Guía de Validación Integral", subtitle_style))
    story.append(HRFlowable(width="100%", thickness=2, color=c_accent, spaceBefore=0, spaceAfter=15))

    meta_data = [
        [Paragraph("<b>Fecha de Emisión:</b>", table_cell_bold), Paragraph(datetime.now().strftime("%d de Octubre de %Y"), table_cell_style)],
        [Paragraph("<b>Entorno:</b>", table_cell_bold), Paragraph("100% Local (Ubuntu Linux • Docker Compose)", table_cell_style)],
        [Paragraph("<b>URL de Acceso:</b>", table_cell_bold), Paragraph("http://localhost:8585", table_cell_style)],
        [Paragraph("<b>Credenciales de Administrador:</b>", table_cell_bold), Paragraph("admin@open-metadata.org / admin", table_cell_style)],
        [Paragraph("<b>Autor / Lead Engineer:</b>", table_cell_bold), Paragraph("Yisus0708 (Lead Data & Software Engineer)", table_cell_style)],
        [Paragraph("<b>Versión del Catálogo:</b>", table_cell_bold), Paragraph("Release 2.0.6 (PostgreSQL 16, MinIO S3, PySpark 3.5, Airflow 2.9)", table_cell_style)]
    ]
    t_meta = Table(meta_data, colWidths=[150, 354])
    t_meta.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#f8fafc")),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor("#e2e8f0")),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
    ]))
    story.append(t_meta)
    story.append(Spacer(1, 15))

    # Resumen Ejecutivo
    story.append(Paragraph("1. Resumen Ejecutivo y Propósito de OpenMetadata", h1_style))
    story.append(Paragraph(
        "OpenMetadata es la plataforma central de <b>Gobernanza de Datos, Catálogo Unificado y Linaje de Extremo a Extremo</b> "
        "implementada en SismoWatch. Su propósito principal es eliminar los 'silos de datos', garantizar la calidad técnica, "
        "asignar responsabilidades directas a cada componente de software (Data Owners) y permitir la auditoría visual "
        "de cómo la información sísmica fluye desde las APIs públicas de USGS hasta los modelos analíticos y reportes de IA.",
        body_style
    ))
    story.append(Paragraph(
        "En una arquitectura de datos moderna, saber <i>dónde están los datos</i> no es suficiente; es imperativo saber "
        "<i>de dónde vinieron</i>, <i>quién los transformó</i>, <i>qué reglas de calidad pasaron</i> y <i>qué modelos de negocio "
        "alimentan</i>. OpenMetadata resuelve este desafío de forma automática y centralizada.",
        body_style
    ))

    story.append(Spacer(1, 10))

    # =========================================================================
    # SECCIÓN 2: ARQUITECTURA MEDALLÓN Y SERVICIOS REGISTRADOS
    # =========================================================================
    story.append(Paragraph("2. Arquitectura Medallón y Servicios Catalogados", h1_style))
    story.append(Paragraph(
        "SismoWatch adopta el patrón de arquitectura Medallón simplificado (directo, sin capas obsoletas de cuarentena ni landing), "
        "clasificando los activos de datos en tres niveles de refinamiento catalogados en OpenMetadata:",
        body_style
    ))

    servicios_data = [
        [Paragraph("Servicio OpenMetadata", table_header_style), Paragraph("Tecnología / Endpoint", table_header_style), Paragraph("Rol en la Arquitectura SismoWatch", table_header_style), Paragraph("Owner Técnico", table_header_style)],
        [
            Paragraph("<b>minio_s3_datalake</b>", table_cell_bold),
            Paragraph("MinIO S3 (minio:9000)<br/>Buckets: bronze, silver, gold", table_cell_style),
            Paragraph("Almacenamiento de objetos inmutables. Bronze (GeoJSON crudo), Silver (Parquet limpio), Gold (Modelos Parquet).", table_cell_style),
            Paragraph("Yisus0708<br/>(Lead Data Eng.)", table_cell_style)
        ],
        [
            Paragraph("<b>postgresql_sismowatch</b>", table_cell_bold),
            Paragraph("PostgreSQL 16 (postgres:5432)<br/>BD: sismos_db • Esquema: curated", table_cell_style),
            Paragraph("Serving Layer de consumo analítico para Web Dashboard y APIs. Alberga 192 sismos curados, 35 resúmenes y 143 hotspots.", table_cell_style),
            Paragraph("Yisus0708<br/>(Lead Data Eng.)", table_cell_style)
        ],
        [
            Paragraph("<b>airflow_orchestrator</b>", table_cell_bold),
            Paragraph("Apache Airflow 2.9 (webserver:8080)<br/>4 DAGs activos", table_cell_style),
            Paragraph("Orquestador continuo: Ingesta periódica, PySpark ETL, validación de reglas de calidad y generación de reportes IA.", table_cell_style),
            Paragraph("Yisus0708<br/>(Lead Data Eng.)", table_cell_style)
        ],
        [
            Paragraph("<b>spark_batch_etl</b>", table_cell_bold),
            Paragraph("Apache Spark 3.5 (master:7077)<br/>Clúster distribuido", table_cell_style),
            Paragraph("Motor de procesamiento masivo en memoria para deduplicación, cálculo de métricas de calidad y particionamiento.", table_cell_style),
            Paragraph("Yisus0708<br/>(Lead Data Eng.)", table_cell_style)
        ]
    ]

    t_serv = Table(servicios_data, colWidths=[110, 130, 194, 70])
    t_serv.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_primary),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#f8fafc")])
    ]))
    story.append(t_serv)

    story.append(PageBreak())

    # =========================================================================
    # SECCIÓN 3: GLOSARIO DE TÉRMINOS SÍSMICOS
    # =========================================================================
    story.append(Paragraph("3. Glosario de Negocio Sísmico Global", h1_style))
    story.append(Paragraph(
        "Para unificar el vocabulario entre ingenieros de software, científicos de datos y tomadores de decisiones, "
        "se registró el glosario oficial <b>Glosario_Sismico_Global</b> en OpenMetadata con los siguientes 7 términos estándar:",
        body_style
    ))

    glosario_data = [
        [Paragraph("Término", table_header_style), Paragraph("Categoría", table_header_style), Paragraph("Definición Técnica Oficial en OpenMetadata", table_header_style)],
        [
            Paragraph("<b>Hipocentro</b>", table_cell_bold),
            Paragraph("Geofísica", table_cell_style),
            Paragraph("Punto tridimensional en el interior de la corteza terrestre donde se inicia la fractura o ruptura tectónica (foco sísmico).", table_cell_style)
        ],
        [
            Paragraph("<b>Epicentro</b>", table_cell_bold),
            Paragraph("Geofísica", table_cell_style),
            Paragraph("Punto en la superficie terrestre ubicado en la vertical exacta sobre el hipocentro, expresado en latitud y longitud.", table_cell_style)
        ],
        [
            Paragraph("<b>Magnitud_Momento_Mw</b>", table_cell_bold),
            Paragraph("Metrología", table_cell_style),
            Paragraph("Escala sismológica cuantitativa y continua que mide la energía total liberada por un sismo sin saturarse en grandes magnitudes.", table_cell_style)
        ],
        [
            Paragraph("<b>Alerta_Tsunami</b>", table_cell_bold),
            Paragraph("Seguridad", table_cell_style),
            Paragraph("Bandera binaria (0 o 1) emitida oficialmente por NOAA/USGS cuando las características y profundidad del sismo presentan riesgo de maremoto.", table_cell_style)
        ],
        [
            Paragraph("<b>Capa_Bronze</b>", table_cell_bold),
            Paragraph("Arquitectura", table_cell_style),
            Paragraph("Almacén inmutable de telemetría GeoJSON cruda en MinIO S3 tal como se recibe de la fuente pública de USGS.", table_cell_style)
        ],
        [
            Paragraph("<b>Capa_Silver</b>", table_cell_bold),
            Paragraph("Arquitectura", table_cell_style),
            Paragraph("Datos sísmicos enriquecidos, validados, tipados y deduplicados por PySpark, almacenados en formato columnar Parquet Snappy.", table_cell_style)
        ],
        [
            Paragraph("<b>Capa_Gold</b>", table_cell_bold),
            Paragraph("Arquitectura", table_cell_style),
            Paragraph("Modelos analíticos agregados (resúmenes por país, cuadrículas de hotspots y reportes ejecutivos de IA) listos para consumo final.", table_cell_style)
        ]
    ]

    t_glo = Table(glosario_data, colWidths=[130, 80, 294])
    t_glo.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_accent),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#f8fafc")])
    ]))
    story.append(t_glo)
    story.append(Spacer(1, 10))

    # =========================================================================
    # SECCIÓN 4: LINAJE DE DATOS DE EXTREMO A EXTREMO
    # =========================================================================
    story.append(Paragraph("4. Grafo de Linaje Interactivo (Data Lineage)", h1_style))
    story.append(Paragraph(
        "El linaje de datos es la joya visual de OpenMetadata. Permite observar el flujo exacto de las transformaciones sin "
        "tener que leer el código fuente. En SismoWatch se han interconectado los siguientes grafos dirigidos:",
        body_style
    ))

    callout_box = [
        [Paragraph(
            "<b>Flujo Principal de Ingesta, Curación e Inteligencia Artificial:</b><br/>"
            "1. <b>minio_s3_datalake.bronze</b> (Telemetría cruda GeoJSON)<br/>"
            "   ➔ Refinamiento y Deduplicación en Clúster PySpark Batch<br/>"
            "2. <b>minio_s3_datalake.silver</b> (Datos limpios en Parquet)<br/>"
            "   ➔ Upsert Idempotente a PostgreSQL Serving Layer<br/>"
            "3. <b>postgresql_sismowatch.sismos_db.curated.earthquake_events</b> (Catálogo Unificado de Sismos)<br/>"
            "   ➔ Análisis Estadístico Z-Score y Resumen Diario con Pool de Gemini 1.5 Flash<br/>"
            "4. <b>postgresql_sismowatch.sismos_db.curated.ai_daily_reports</b> (Informes Ejecutivos Auditados)<br/><br/>"
            "<b>Flujo Analítico Dimensional Gold:</b><br/>"
            "5. <b>minio_s3_datalake.silver</b> ➔ <b>minio_s3_datalake.gold</b> (Agregaciones Parquet)<br/>"
            "6. <b>minio_s3_datalake.gold</b> ➔ <b>postgresql_sismowatch.sismos_db.curated.resumen_diario_pais</b>",
            callout_style
        )]
    ]
    t_callout = Table(callout_box, colWidths=[504])
    t_callout.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#ecfdf5")),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor("#10b981")),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
    ]))
    story.append(t_callout)

    story.append(PageBreak())

    # =========================================================================
    # SECCIÓN 5: GUÍA PASO A PASO PARA VERIFICAR EN LA INTERFAZ WEB
    # =========================================================================
    story.append(Paragraph("5. Guía de Verificación en la Interfaz Web (http://localhost:8585)", h1_style))
    story.append(Paragraph(
        "Sigue esta secuencia paso a paso para comprobar visualmente que cada entidad está registrada y operativa:",
        body_style
    ))

    pasos = [
        ("Paso 1: Inicio de Sesión", "Ingresa en el navegador a <b>http://localhost:8585</b>. En la pantalla de login introduce el correo <b>admin@open-metadata.org</b> y la contraseña <b>admin</b>. Serás redirigido al panel general."),
        ("Paso 2: Comprobar el Glosario", "En el menú lateral izquierdo, haz clic en el ícono de libro <b>Governance</b> ➔ <b>Glossaries</b>. Selecciona <b>Glosario_Sismico_Global</b>. Verifica que figuren los 7 términos con sus descripciones completas."),
        ("Paso 3: Explorar Tablas PostgreSQL", "Haz clic en la brújula <b>Explore</b> en la barra lateral ➔ pestaña <b>Databases</b> (o Tables). Selecciona <b>postgresql_sismowatch</b> ➔ <b>sismos_db</b> ➔ esquema <b>curated</b>. Verás las 4 tablas oficiales: <i>earthquake_events</i>, <i>resumen_diario_pais</i>, <i>zonas_criticas_frecuentes</i> y <i>ai_daily_reports</i>. Observa que el Owner asignado es <b>Yisus0708</b>."),
        ("Paso 4: Inspeccionar Esquema y Columnas", "Haz clic en la tabla <b>earthquake_events</b>. En la pestaña <b>Schema</b> podrás ver todas las columnas tipadas (id VARCHAR, mag NUMERIC, country_region VARCHAR, alert_level VARCHAR, etc.) con sus definiciones."),
        ("Paso 5: Visualizar el Linaje Interactivo", "Dentro de la vista de la tabla <b>earthquake_events</b>, haz clic en la pestaña superior <b>Lineage</b>. Observarás el grafo interactivo: a la izquierda el contenedor <b>minio_s3_datalake.silver</b> conectándose a la tabla, y a la derecha la tabla conectándose hacia <b>ai_daily_reports</b>. Puedes arrastrar los nodos y hacer zoom."),
        ("Paso 6: Comprobar el Data Lake MinIO", "Vuelve a <b>Explore</b> ➔ pestaña <b>Storage Services</b> (o Containers). Selecciona el servicio <b>minio_s3_datalake</b>. Confirma los tres buckets del Medallón: <b>bronze</b>, <b>silver</b> y <b>gold</b>, con el Owner <b>Yisus0708</b>."),
        ("Paso 7: Verificar Pipelines de Airflow", "En <b>Explore</b> ➔ pestaña <b>Pipelines</b>, abre el servicio <b>airflow_orchestrator</b>. Verás los 4 DAGs catalogados (<i>dag_01_ingest_usgs</i>, <i>dag_02_transform_quality</i>, <i>dag_03_daily_ai_report</i>, <i>dag_04_historical_backfill</i>)."),
        ("Paso 8: Revisar Equipo y Miembros", "En la esquina inferior izquierda, haz clic en el ícono de engranaje <b>Settings</b> ➔ <b>Members</b> ➔ <b>Teams</b>. Selecciona el grupo <b>Equipo_SismoWatch</b> y comprueba el usuario técnico registrado (<b>Yisus0708</b>) con su cargo de Lead Data & Software Engineer.")
    ]

    for p_title, p_desc in pasos:
        story.append(Paragraph(f"<b>{p_title}</b>", h2_style))
        story.append(Paragraph(p_desc, bullet_style))

    story.append(Spacer(1, 8))

    # =========================================================================
    # SECCIÓN 6: MATRIZ DE AUDITORÍA Y CHECKLIST
    # =========================================================================
    story.append(Paragraph("6. Matriz de Auditoría y Estado de Validación", h1_style))

    check_data = [
        [Paragraph("Componente Evaluado", table_header_style), Paragraph("Entidad en OpenMetadata", table_header_style), Paragraph("Estado Actual", table_header_style), Paragraph("Resultado de Validación", table_header_style)],
        [
            Paragraph("Glosario de Negocio", table_cell_bold),
            Paragraph("Glosario_Sismico_Global", table_cell_style),
            Paragraph("<font color='#059669'><b>ACTIVO</b></font>", table_cell_style),
            Paragraph("7 términos registrados con definiciones geofísicas.", table_cell_style)
        ],
        [
            Paragraph("Serving Layer (Postgres)", table_cell_bold),
            Paragraph("curated.earthquake_events", table_cell_style),
            Paragraph("<font color='#059669'><b>OPERATIVO</b></font>", table_cell_style),
            Paragraph("192 eventos catalogados con esquema de columnas completo.", table_cell_style)
        ],
        [
            Paragraph("Serving Layer (Postgres)", table_cell_bold),
            Paragraph("curated.resumen_diario_pais", table_cell_style),
            Paragraph("<font color='#059669'><b>OPERATIVO</b></font>", table_cell_style),
            Paragraph("35 resúmenes diarios catalogados con owner y tags.", table_cell_style)
        ],
        [
            Paragraph("Serving Layer (Postgres)", table_cell_bold),
            Paragraph("curated.ai_daily_reports", table_cell_style),
            Paragraph("<font color='#059669'><b>OPERATIVO</b></font>", table_cell_style),
            Paragraph("Informes de Gemini catalogados y auditados.", table_cell_style)
        ],
        [
            Paragraph("Data Lake (MinIO S3)", table_cell_bold),
            Paragraph("minio_s3_datalake (bronze/silver/gold)", table_cell_style),
            Paragraph("<font color='#059669'><b>OPERATIVO</b></font>", table_cell_style),
            Paragraph("3 buckets sincronizados bajo patrón Medallón.", table_cell_style)
        ],
        [
            Paragraph("Pipelines de Orquestación", table_cell_bold),
            Paragraph("airflow_orchestrator (DAGs 01 al 04)", table_cell_style),
            Paragraph("<font color='#059669'><b>SINCRONIZADO</b></font>", table_cell_style),
            Paragraph("4 DAGs mapeados a Airflow Webserver en :8080.", table_cell_style)
        ],
        [
            Paragraph("Linaje de Extremo a Extremo", table_cell_bold),
            Paragraph("Lineage Graph (Bronze -> Silver -> Gold -> Curated -> AI)", table_cell_style),
            Paragraph("<font color='#059669'><b>CONECTADO</b></font>", table_cell_style),
            Paragraph("Grafo dirigido 100% interactivo y navegable sin errores 404.", table_cell_style)
        ]
    ]

    t_chk = Table(check_data, colWidths=[110, 140, 70, 184])
    t_chk.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), c_primary),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#f8fafc")])
    ]))
    story.append(t_chk)

    # Construir documento con NumberedCanvas
    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"✓ Documento PDF generado exitosamente en: {PDF_DEST}")


if __name__ == "__main__":
    build_pdf()
