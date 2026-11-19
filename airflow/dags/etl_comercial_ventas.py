"""DAG ETL de Comercial GT: CSV + PostgreSQL + API -> staging -> limpieza -> Data Warehouse -> resumen."""
import json
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task

from etl import extract_api, extract_csv, extract_postgres, load, transform

TMP = "/opt/airflow/data/tmp"  # archivos intermedios entre tareas, una carpeta por ejecucion


def _dir(id_ej):
    ruta = os.path.join(TMP, str(id_ej))
    os.makedirs(ruta, exist_ok=True)
    return ruta


def _al_fallar(context):
    """Se ejecuta cuando una tarea falla despues de agotar sus reintentos."""
    id_ej = context["ti"].xcom_pull(task_ids="iniciar_ejecucion")
    load.marcar_error(id_ej, f"Fallo la tarea {context['task_instance'].task_id}: {context.get('exception')}")


@dag(dag_id="etl_comercial_ventas", start_date=datetime(2026, 11, 1), schedule=None, catchup=False,
     default_args={"retries": 3, "retry_delay": timedelta(minutes=1), "on_failure_callback": _al_fallar},
     tags=["bd2", "etl"])
def etl_comercial_ventas():

    @task
    def iniciar_ejecucion():
        return load.iniciar_ejecucion()

    @task
    def extraer_csv(id_ej):
        df = extract_csv.extraer_csv()
        df.to_csv(os.path.join(_dir(id_ej), "ventas.csv"), index=False)
        return len(df)

    @task
    def extraer_postgres(id_ej):
        clientes, productos, sucursales = extract_postgres.extraer_postgres()
        for nombre, df in [("clientes", clientes), ("productos", productos), ("sucursales", sucursales)]:
            df.to_csv(os.path.join(_dir(id_ej), nombre + ".csv"), index=False)
        return len(clientes) + len(productos) + len(sucursales)

    @task
    def extraer_api(id_ej):
        t = extract_api.extraer_tipo_cambio()
        t["fecha_consulta"] = t["fecha_consulta"].isoformat()
        with open(os.path.join(_dir(id_ej), "tasa.json"), "w") as f:
            json.dump(t, f)
        return t["tasa_usd_gtq"]

    @task
    def cargar_staging(id_ej, n_csv, n_pg, tasa):
        d = _dir(id_ej)
        ventas = pd.read_csv(os.path.join(d, "ventas.csv"), dtype=str, keep_default_na=False)
        ventas["fila_origen"] = ventas["fila_origen"].astype(int)
        cl, pr, su = (pd.read_csv(os.path.join(d, n + ".csv")) for n in ("clientes", "productos", "sucursales"))
        with open(os.path.join(d, "tasa.json")) as f:
            t = json.load(f)
        load.cargar_staging(id_ej, ventas, cl, pr, su, t)
        return n_csv

    @task
    def limpiar_transformar(id_ej, _stg, tasa):
        ventas, cl, pr, su = load.leer_staging(id_ej)
        ok, bad = transform.transformar(ventas, cl, pr, su, tasa)
        ok.to_csv(os.path.join(_dir(id_ej), "aceptados.csv"), index=False)
        load.registrar_rechazados(id_ej, bad)
        return {"leidas": len(ventas), "aceptadas": len(ok), "rechazadas": len(bad)}

    @task
    def cargar_data_warehouse(id_ej, lim):
        if lim["aceptadas"]:
            ok = pd.read_csv(os.path.join(_dir(id_ej), "aceptados.csv"), dtype=str)
        else:
            ok = pd.DataFrame()
        res = load.cargar_dw(id_ej, ok)
        res["fecha_min"] = ok["fecha_venta"].min() if len(ok) else None
        res["fecha_max"] = ok["fecha_venta"].max() if len(ok) else None
        return res

    @task
    def ejecutar_procedure(id_ej, dw):
        if dw["fecha_min"]:
            load.ejecutar_sp(dw["fecha_min"], dw["fecha_max"])
        return "ok"

    @task
    def cerrar_ejecucion(id_ej, lim, dw, _sp):
        load.cerrar_ejecucion(id_ej, lim["leidas"], dw["cargadas"], lim["rechazadas"],
                              f"Ventas validas que ya existian y no se recargaron: {dw['existentes']}")

    id_ej = iniciar_ejecucion()
    csv, pg, api = extraer_csv(id_ej), extraer_postgres(id_ej), extraer_api(id_ej)
    stg = cargar_staging(id_ej, csv, pg, api)
    lim = limpiar_transformar(id_ej, stg, api)
    dw = cargar_data_warehouse(id_ej, lim)
    sp = ejecutar_procedure(id_ej, dw)
    cerrar_ejecucion(id_ej, lim, dw, sp)


etl_comercial_ventas()