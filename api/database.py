"""
Gestor de Conexiones a Base de Datos PostgreSQL
Proyecto: SismoWatch API
Principio de Mínimo Privilegio: Conecta como sismo_read_user (Solo lectura en curated y quality)
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sismo_read_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "read_secret_2026")


@contextmanager
def get_db():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        cursor_factory=RealDictCursor,
        connect_timeout=5
    )
    try:
        yield conn
    finally:
        conn.close()
