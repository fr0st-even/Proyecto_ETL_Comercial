"""Fuente 1: lee ventas.csv. Todo se lee como texto para no perder datos malos."""
import os
import pandas as pd

RUTA_CSV = os.getenv("VENTAS_CSV", "/opt/airflow/data/ventas.csv")


def extraer_csv(ruta=RUTA_CSV):
    df = pd.read_csv(ruta, dtype=str, keep_default_na=False)
    df.insert(0, "fila_origen", range(2, len(df) + 2))  # fila en el archivo (1 = encabezado)
    print(f"[extract_csv] {len(df)} filas leidas de {ruta}")
    return df