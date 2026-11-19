"""Fuente 3: tipo de cambio USD/GTQ desde la API. Guarda la respuesta y la fecha de consulta."""
import json
import os
from datetime import datetime

import requests

URL = "https://open.er-api.com/v6/latest/USD"
CARPETA_EVIDENCIA = os.getenv("EVIDENCIA_DIR", "/opt/airflow/data")


def extraer_tipo_cambio():
    fecha_consulta = datetime.now()
    resp = requests.get(URL, timeout=15)
    resp.raise_for_status()  # si falla, Airflow reintenta la tarea
    datos = resp.json()

    if datos.get("result") != "success" or "GTQ" not in datos.get("rates", {}):
        raise ValueError(f"Respuesta inesperada de la API: {datos}")

    tasa = float(datos["rates"]["GTQ"])

    # Evidencia de la extraccion (entregable del proyecto)
    ruta = os.path.join(CARPETA_EVIDENCIA, f"tipo_cambio_{fecha_consulta:%Y%m%d_%H%M%S}.json")
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump({"fecha_consulta": fecha_consulta.isoformat(), "respuesta": datos}, f, ensure_ascii=False, indent=2)

    print(f"[extract_api] 1 USD = {tasa} GTQ (consultado {fecha_consulta:%Y-%m-%d %H:%M:%S})")
    return {"tasa_usd_gtq": tasa, "fecha_consulta": fecha_consulta, "respuesta": datos}