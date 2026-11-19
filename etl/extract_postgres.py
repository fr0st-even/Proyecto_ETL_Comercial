"""Fuente 2: clientes, productos y sucursales desde la base operacional (Neon)."""
import os
import pandas as pd
from sqlalchemy import create_engine


def _engine():
    uri = os.environ["NEON_OPERACIONAL_URI"].replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(uri)


def extraer_postgres():
    engine = _engine()
    clientes = pd.read_sql("SELECT * FROM clientes", engine)
    productos = pd.read_sql("SELECT * FROM productos", engine)
    sucursales = pd.read_sql("SELECT * FROM sucursales", engine)
    print(f"[extract_postgres] clientes={len(clientes)}, productos={len(productos)}, sucursales={len(sucursales)}")
    return clientes, productos, sucursales