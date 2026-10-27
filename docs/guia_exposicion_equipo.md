# Guía Oficial de Estudio y Guión de Sustentación: Plataforma SismoWatch

Documento maestro de preparación técnica para la sustentación del proyecto de Big Data y Arquitectura de Datos SismoWatch.
**Autor y Expositor:** **Yisus0708** — *Lead Data & Software Engineer*.

---

## 1. Estructura General de la Exposición (Expositor Único: Yisus0708)

| Bloque | Módulo / Componente Técnico | Tema Central de Exposición | Tiempo Estimado |
| :--- | :--- | :--- | :--- |
| **1** | **Visión y Arquitectura Global** | Apertura, Arquitectura Lambda + Medallón (Bronze, Silver, Gold) y Principios de Diseño | 3 min |
| **2** | **Ingesta y Mensajería (Kafka)** | Apache Kafka en modo KRaft, Tópico `sismos_crudos` y Conectores Kafka Connect | 3 min |
| **3** | **Cómputo en Tiempo Real (Spark)**| Spark Structured Streaming: Micro-lotes continuos, tipado Silver y Upsert en PostgreSQL | 3 min |
| **4** | **Orquestación Batch (Airflow)** | Apache Airflow: Ingesta periódica, DAGs de calidad, backfill histórico y reportes con IA | 3 min |
| **5** | **Procesamiento Masivo (PySpark)**| PySpark Batch ETL: Computación distribuida, Parquet Snappy, modelos Gold analíticos | 3 min |
| **6** | **Gobernanza y Demo GIS (OpenMetadata & Web)** | Data Lake MinIO, PostgreSQL Serving Layer, Linaje en OpenMetadata y Demostración en vivo | 4 a 5 min |

---

## 2. Guión Detallado por Módulos Técnicos

---

### Módulo 1: Visión y Arquitectura Global de Datos

#### Guión de Exposición:
> "Buenos días. SismoWatch resuelve un reto crítico de ingeniería de datos: **¿cómo capturar, procesar y alertar eventos sísmicos mundiales en tiempo real, garantizando al mismo tiempo la reconciliación histórica, auditoría de calidad y gobernanza de extremo a extremo?**
>
> Para solucionar esto, implementé una **Arquitectura Lambda** combinada con un **Data Lake Medallón (Bronze, Silver, Gold)**:
> 1. Un **Speed Layer (Carril Streaming)** de baja latencia que procesa eventos segundo a segundo desde la API de USGS para visualización inmediata en el mapa GIS.
> 2. Un **Batch Layer (Carril por Lotes)** orquestado por Airflow y procesado por Spark que consolida, valida y repara históricos ante eventuales cortes de red.
>
> Ambos carriles convergen en una **Serving Layer en PostgreSQL**, gobernada por **OpenMetadata 2.0.6** con linaje automatizado, y consumida por la plataforma web interactiva **SismoWatch GIS** con Copilot de IA geofísica."

---

### Módulo 2: Ingesta y Mensajería con Apache Kafka & Kafka Connect

#### Guión de Exposición:
> "Para la capa de captura y mensajería en tiempo real, implementé **Apache Kafka** y **Kafka Connect**.
>
> Kafka opera en modo **KRaft (Kafka Raft)**, eliminando la dependencia histórica de ZooKeeper, lo que reduce la latencia de metadatos, optimiza el uso de memoria y acelera la recuperación ante fallos.
>
> Se configuraron conectores industriales en **Kafka Connect**:
> 1. **HTTP Source Connector:** Realiza polling continuo del feed GeoJSON de USGS y publica los eventos crudos en el tópico particionado `sismos_crudos`.
> 2. **S3 Sink Connector:** Escribe de forma inmutable el flujo crudo directamente en el Data Lake MinIO en la capa `bronze/streaming/`.
>
> Esto desacopla 100% la fuente externa del procesamiento: si Spark o la red se detienen, los mensajes quedan retenidos de forma segura en Kafka sin pérdida de información."

#### Preguntas de Defensa Técnica:
* **¿Por qué Kafka Connect en lugar de un bucle `while True` en Python?**
  * *Respuesta:* "Kafka Connect es tolerante a fallos de nivel industrial. Si el contenedor se reinicia, Connect registra el offset en `connect-offsets` y reanuda sin perder mensajes ni duplicar la ingesta. Gestiona reintentos automáticos y escalabilidad sin código frágil."
* **¿Por qué KRaft en lugar de ZooKeeper?**
  * *Respuesta:* "ZooKeeper introducía un punto único de falla y sobrecarga de sincronización externa. Con KRaft (KIP-500), el consenso se maneja internamente mediante el protocolo Raft en el puerto 29093, permitiendo recuperaciones en menos de 3 segundos."

---

### Módulo 3: Procesamiento en Tiempo Real con Spark Structured Streaming

#### Guión de Exposición:
> "El procesamiento continuo en memoria está implementado mediante **Spark Structured Streaming** (`spark_streaming_job.py`).
>
> El pipeline opera continuamente:
> 1. Lee del tópico `sismos_crudos` mediante `readStream` con `failOnDataLoss=false`.
> 2. Parsea el payload GeoJSON de USGS, aplanando coordenadas (`latitud`, `longitud`, `profundidad`), magnitud y lugar.
> 3. Clasifica la severidad (`CRÍTICO` si $Mw \ge 6.0$, `MODERADO` si $5.0 \le Mw < 6.0$) y el tipo de profundidad (`SUPERFICIAL`, `INTERMEDIA`, `PROFUNDA`).
> 4. Escribe en MinIO en la capa **Silver** en formato columnar Parquet Snappy particionado por `fecha_evento`.
> 5. Ejecuta `foreachBatch` para escribir en PostgreSQL mediante `upsert` (`ON CONFLICT (id_evento) DO UPDATE`), permitiendo que el cliente web visualice los nuevos sismos en milisegundos."

#### Preguntas de Defensa Técnica:
* **¿Por qué Spark Structured Streaming y no DStreams clásico?**
  * *Respuesta:* "Structured Streaming procesa los datos como una tabla relacional infinita bajo el motor optimizado Catalyst. Permite la misma sintaxis de DataFrames que Spark Batch y garantiza semántica *Exactly-Once* mediante checkpoints en disco."
* **¿Cómo evitas bloquear PostgreSQL con el streaming continuo?**
  * *Respuesta:* "Se utiliza `foreachBatch` con micro-lotes y connection pools, actualizando únicamente la tabla delta del día. Las consultas pesadas de históricos van contra las tablas batch, eliminando contención de bloqueos."

---

### Módulo 4: Orquestación del Flujo de Trabajo con Apache Airflow

#### Guión de Exposición:
> "La automatización, calendarización y monitoreo de tareas batch están gobernadas por **Apache Airflow 2.9**:
> 1. **`dag_01_ingest_usgs`:** Ingesta periódica cada 5 minutos desde la API oficial de USGS hacia MinIO Bronze.
> 2. **`dag_02_transform_quality`:** Dispara el procesamiento PySpark Batch ETL y calcula el puntaje de calidad `quality_score`.
> 3. **`dag_03_daily_ai_report`:** Calcula anomalías estadísticas Z-Score y genera la narrativa ejecutiva con el pool tolerante a fallos de Gemini.
> 4. **`dag_04_historical_backfill`:** Backfill histórico parametrizado de 30 días con deduplicación estricta.
>
> Todos los DAGs están desacoplados mediante variables de entorno y cuentan con políticas de reintentos automáticos."

#### Preguntas de Defensa Técnica:
* **¿Cómo maneja Airflow la resiliencia ante fallos transitorios?**
  * *Respuesta:* "Cada tarea cuenta con política de reintentos (`retries=2, retry_delay=timedelta(minutes=2)`). Si hay una indisponibilidad temporal de red o almacenamiento, Airflow reintenta de forma autónoma y deja trazabilidad en los logs de auditoría."

---

### Módulo 5: Cómputo Masivo por Lotes con PySpark Batch ETL

#### Guión de Exposición:
> "El procesamiento de gran volumen a través de las capas Medallón está implementado en **PySpark Batch** (`spark_batch_etl.py`):
> 1. **Lectura Bronze:** Ingesta de archivos GeoJSON inmutables desde MinIO S3 (`s3a://bronze/batch/`).
> 2. **Refinamiento a Silver:** Limpieza de tipos, casteo con precisión decimal estricta (`DecimalType(4,2)`), validación de rangos geográficos y guardado en Parquet Snappy particionado por `fecha_evento`.
> 3. **Modelado Analítico Gold:**
>    - **`resumen_diario_pais`:** Totales de sismos, sismos críticos ($Mw \ge 6.0$), magnitud máxima y profundidad promedio agrupados por país y día.
>    - **`zonas_criticas_frecuentes`:** Cuadrículas de $1^\circ \times 1^\circ$ para detección de hotspots y fallas activas.
> 4. **Carga en PostgreSQL Serving Layer:** Persistencia en el esquema `curated` y registro de métricas de calidad."

#### Preguntas de Defensa Técnica:
* **¿Por qué formato Parquet con Snappy en lugar de CSV o JSON?**
  * *Respuesta:* "Parquet es columnar y almacena estadísticas de min/max por bloque con compresión Snappy, reduciendo el tamaño en disco hasta en un 75%. Habilita *Column Pruning* y *Predicate Pushdown*, logrando que Spark lea solo las columnas necesarias y descarte bloques completos sin I/O innecesario."

---

### Módulo 6: Almacenamiento, Gobernanza con OpenMetadata y Demo Web

#### Guión de Exposición:
> "Para consolidar el valor del sistema:
>
> **1. Data Lake MinIO y PostgreSQL Serving Layer:**
> MinIO implementa particionamiento Hive (`ano/mes/dia`) para garantizar *Partition Pruning*. En PostgreSQL, la Serving Layer ofrece consultas sub-segundo con esquemas separados (`raw`, `staging`, `curated`, `quality`) y principio de mínimo privilegio.
>
> **2. Gobernanza Integral con OpenMetadata 2.0.6:**
> - Catálogo unificado de tablas PostgreSQL, buckets MinIO S3 y DAGs de Airflow.
> - **Grafo de linaje interactivo** que traza el flujo completo desde `bronze` hasta los reportes ejecutivos de IA.
> - **Glosario técnico:** Términos estandarizados (Hipocentro, Epicentro, Escala Mw, Alerta de Tsunami, Capas Medallón).
> - Asignación de Ownership técnico unificado bajo **Yisus0708**.
>
> **3. Demostración en Vivo de SismoWatch GIS (`:3000`):**
> - **Mapa Mission Control ArcGIS:** Muestra eventos globales clasificados por severidad, fallas tectónicas vectoriales y sismo más crítico destacado.
> - **Buscador Inteligente:** Búsqueda inmediata por país o región con normalización fonética y tildes.
> - **Copilot IA Geofísico:** Asistente conversacional con Gemini 3.5 Flash que responde preguntas en lenguaje natural respaldado por datos oficiales en PostgreSQL."

---

## 3. Hoja de Preparación para la Evaluación de Desempeño del Lead Engineer

### 1. ¿Cómo se garantiza la consistencia eventual entre el carril Batch y el carril Streaming?
* **Respuesta:** "Mediante la inmutabilidad de la capa Bronze y la reconciliación determinista en la Serving Layer. El carril streaming opera sobre una ventana móvil para alertas tempranas, mientras que el carril batch computa la función determinista $f(DatosCrudos) = VistaHistorica$. Al ser Bronze inmutable, el batch reescribe cualquier discrepancia ocurrida en streaming cada 24 horas, alcanzando consistencia eventual perfecta."

### 2. ¿Qué patrón de particionamiento físico se utilizó en MinIO y por qué?
* **Respuesta:** "Utilizamos particionamiento temporal estilo Hive: `ano=YYYY/mes=MM/dia=DD/` en Bronze y `fecha_evento=YYYY-MM-DD/` en Silver. Esto habilita **Partition Pruning** en Apache Spark: al consultar sismos de un rango temporal, Spark solo escanea la partición física correspondiente, ahorrando hasta un 90% de operaciones de lectura e I/O."

### 3. ¿Cómo está implementada la seguridad y el principio de mínimo privilegio?
* **Respuesta:** "En PostgreSQL existen 3 roles desacoplados: `sismo_ingest_user` solo escribe en `raw`; `sismo_transform_user` lee en `raw` y administra `staging`, `curated` y `quality`; y `sismo_read_user` solo tiene permiso `SELECT` sobre `curated` y `quality` (usado por la API web). Además, los endpoints administrativos requieren JWT con rol `admin`, y el escaneo con Bandit arrojó 0 vulnerabilidades de severidad Alta o Media."
