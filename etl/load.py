"""Carga a staging y Data Warehouse, registro en etl_log y llamada al Stored Procedure."""
import json
import os

import pandas as pd
from sqlalchemy import create_engine, text


def engine_dw():
    uri = os.environ["NEON_DW_URI"].replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(uri, pool_pre_ping=True)  # Neon suspende la base si no se usa


def iniciar_ejecucion():
    with engine_dw().begin() as c:
        id_ej = c.execute(text("INSERT INTO etl_log (estado) VALUES ('EN_PROCESO') RETURNING id_ejecucion")).scalar()
    print(f"[load] Ejecucion {id_ej} iniciada")
    return id_ej


def cargar_staging(id_ej, ventas, clientes, productos, sucursales, tasa):
    e = engine_dw()
    for nombre, df in [("stg_ventas", ventas), ("stg_clientes", clientes),
                       ("stg_productos", productos), ("stg_sucursales", sucursales)]:
        df = df.copy()
        df.insert(0, "id_ejecucion", id_ej)
        df.to_sql(nombre, e, if_exists="append", index=False)
        print(f"[load] {nombre}: {len(df)} filas")
    with e.begin() as c:
        c.execute(text("INSERT INTO stg_tipo_cambio (id_ejecucion, tasa_usd_gtq, fecha_consulta, respuesta_api) "
                       "VALUES (:e, :t, :f, CAST(:r AS jsonb))"),
                  {"e": id_ej, "t": tasa["tasa_usd_gtq"], "f": tasa["fecha_consulta"], "r": json.dumps(tasa["respuesta"])})


def leer_staging(id_ej):
    e = engine_dw()
    q = lambda t: pd.read_sql(text(f"SELECT * FROM {t} WHERE id_ejecucion = :e"), e, params={"e": id_ej})
    ventas = q("stg_ventas").drop(columns=["id_ejecucion", "fecha_carga"])
    return ventas, q("stg_clientes"), q("stg_productos"), q("stg_sucursales")


def registrar_rechazados(id_ej, rech):
    if rech.empty:
        return
    with engine_dw().begin() as c:
        c.execute(text("INSERT INTO etl_rechazados (id_ejecucion, id_origen, datos_origen, motivo) "
                       "VALUES (:e, :o, :d, :m)"),
                  [{"e": id_ej, "o": r.id_origen, "d": r.datos_origen, "m": r.motivo} for r in rech.itertuples()])
    print(f"[load] {len(rech)} rechazos guardados en etl_rechazados")


UPSERTS = [
    """INSERT INTO dim_cliente (id_cliente_origen, nombre, ciudad, departamento)
       SELECT id_cliente, nombre, ciudad, departamento FROM stg_clientes WHERE id_ejecucion = :ej
       ON CONFLICT (id_cliente_origen) DO UPDATE SET nombre = EXCLUDED.nombre,
            ciudad = EXCLUDED.ciudad, departamento = EXCLUDED.departamento""",
    """INSERT INTO dim_producto (id_producto_origen, producto, categoria, costo)
       SELECT id_producto, producto, categoria, costo FROM stg_productos WHERE id_ejecucion = :ej
       ON CONFLICT (id_producto_origen) DO UPDATE SET producto = EXCLUDED.producto,
            categoria = EXCLUDED.categoria, costo = EXCLUDED.costo""",
    """INSERT INTO dim_sucursal (id_sucursal_origen, sucursal, departamento)
       SELECT id_sucursal, sucursal, departamento FROM stg_sucursales WHERE id_ejecucion = :ej
       ON CONFLICT (id_sucursal_origen) DO UPDATE SET sucursal = EXCLUDED.sucursal,
            departamento = EXCLUDED.departamento""",
]

SQL_FACT = """
INSERT INTO fact_ventas (id_venta, id_fecha, id_cliente, id_producto, id_sucursal, cantidad,
    precio_unitario, moneda, tipo_cambio, total_original, descuento_original,
    total_gtq, descuento_gtq, total_neto_gtq, id_ejecucion)
SELECT CAST(:id_venta AS integer), CAST(to_char(CAST(:fecha_venta AS date), 'YYYYMMDD') AS integer),
    c.id_cliente_dw, p.id_producto_dw, s.id_sucursal_dw, CAST(:cantidad AS integer),
    CAST(:precio_unitario AS numeric), :moneda, CAST(:tipo_cambio AS numeric),
    CAST(:total_original AS numeric), CAST(:descuento_original AS numeric),
    CAST(:total_gtq AS numeric), CAST(:descuento_gtq AS numeric), CAST(:total_neto_gtq AS numeric), :ej
FROM dim_cliente c, dim_producto p, dim_sucursal s
WHERE c.id_cliente_origen = CAST(:id_cliente AS integer)
  AND p.id_producto_origen = CAST(:id_producto AS integer)
  AND s.id_sucursal_origen = CAST(:id_sucursal AS integer)
ON CONFLICT (id_venta) DO NOTHING"""


def cargar_dw(id_ej, aceptados):
    """Actualiza dimensiones y carga hechos. Las ventas que ya existen se omiten (sin duplicar)."""
    cargadas = 0
    with engine_dw().begin() as c:
        for sql in UPSERTS:
            c.execute(text(sql), {"ej": id_ej})
        for fila in aceptados.to_dict("records"):
            cargadas += c.execute(text(SQL_FACT), {**fila, "ej": id_ej}).rowcount
    existentes = len(aceptados) - cargadas
    print(f"[load] fact_ventas: {cargadas} nuevas, {existentes} ya existian")
    return {"cargadas": cargadas, "existentes": existentes}


def ejecutar_sp(fecha_inicio, fecha_fin):
    with engine_dw().begin() as c:
        c.execute(text("CALL sp_actualizar_resumen_ventas(:i, :f)"), {"i": fecha_inicio, "f": fecha_fin})
    print(f"[load] Resumen actualizado del {fecha_inicio} al {fecha_fin}")


def cerrar_ejecucion(id_ej, leidas, cargadas, rechazadas, detalle=""):
    with engine_dw().begin() as c:
        c.execute(text("UPDATE etl_log SET fin = now(), estado = 'EXITOSO', filas_leidas = :l, "
                       "filas_cargadas = :c, filas_rechazadas = :r, detalle = :d WHERE id_ejecucion = :e"),
                  {"l": leidas, "c": cargadas, "r": rechazadas, "d": detalle, "e": id_ej})
    print(f"[load] Ejecucion {id_ej} EXITOSA: leidas={leidas}, cargadas={cargadas}, rechazadas={rechazadas}")


def marcar_error(id_ej, mensaje):
    if id_ej is None:
        return
    with engine_dw().begin() as c:
        c.execute(text("UPDATE etl_log SET fin = now(), estado = 'ERROR', detalle = :d WHERE id_ejecucion = :e"),
                  {"d": str(mensaje)[:500], "e": id_ej})