"""Prueba rapida de extraccion + limpieza. Guarda evidencia en data/aceptados.csv y data/rechazados.csv.
Uso (dentro del contenedor):  docker compose exec airflow python -m etl.prueba_local"""
import os

from etl.extract_api import extraer_tipo_cambio
from etl.extract_csv import extraer_csv
from etl.extract_postgres import extraer_postgres
from etl.transform import transformar

DATA = os.getenv("EVIDENCIA_DIR", "/opt/airflow/data")

if __name__ == "__main__":
    ventas = extraer_csv()
    clientes, productos, sucursales = extraer_postgres()
    tasa = extraer_tipo_cambio()["tasa_usd_gtq"]

    aceptados, rechazados = transformar(ventas, clientes, productos, sucursales, tasa)

    aceptados.to_csv(os.path.join(DATA, "aceptados.csv"), index=False)
    rechazados.to_csv(os.path.join(DATA, "rechazados.csv"), index=False)
    print(f"leidas={len(ventas)} aceptadas={len(aceptados)} rechazadas={len(rechazados)}")
    print(rechazados[["id_origen", "motivo"]].to_string(index=False))