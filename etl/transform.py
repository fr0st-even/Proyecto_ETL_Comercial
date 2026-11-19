"""Limpieza y validacion de ventas. Cada fila termina en 'aceptados' o en 'rechazados' (con motivo).
Ninguna fila se descarta en silencio."""
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

FORMATOS_FECHA = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"]
MONEDAS = {"GTQ", "USD"}
DOS = Decimal("0.01")


def _fecha(texto):
    for fmt in FORMATOS_FECHA:
        try:
            return datetime.strptime(texto, fmt).date()
        except ValueError:
            pass
    return None


def _entero(texto):
    try:
        return int(texto)
    except (ValueError, TypeError):
        return None


def _decimal(texto):
    try:
        valor = Decimal(texto)
        return valor if valor.is_finite() else None
    except (InvalidOperation, TypeError):
        return None


def transformar(ventas, clientes, productos, sucursales, tasa_usd_gtq):
    """Devuelve (aceptados, rechazados) como DataFrames."""
    ids_cliente = set(clientes["id_cliente"])
    ids_producto = set(productos["id_producto"])
    ids_sucursal = set(sucursales["id_sucursal"])
    tasa = Decimal(str(tasa_usd_gtq))

    aceptados, rechazados, vistos = [], [], set()

    for fila in ventas.to_dict("records"):
        original = {k: v for k, v in fila.items()}
        motivos = []

        # TRIM a todos los textos
        f = {k: (v.strip() if isinstance(v, str) else v) for k, v in fila.items()}

        # id_venta y duplicados
        id_venta = _entero(f["id_venta"])
        if id_venta is None:
            motivos.append("id_venta invalido")
        elif id_venta in vistos:
            motivos.append("Venta duplicada (ya se proceso este id_venta; se conserva la primera)")
        else:
            vistos.add(id_venta)

        # Fecha
        fecha = _fecha(f["fecha_venta"])
        if fecha is None:
            motivos.append(f"Fecha invalida: '{f['fecha_venta']}'")

        # Llaves que deben existir
        for campo, existentes, nombre in [("id_cliente", ids_cliente, "Cliente"),
                                          ("id_producto", ids_producto, "Producto"),
                                          ("id_sucursal", ids_sucursal, "Sucursal")]:
            valor = _entero(f[campo])
            if valor is None:
                motivos.append(f"{campo} invalido: '{f[campo]}'")
            elif valor not in existentes:
                motivos.append(f"{nombre} inexistente ({campo}={valor})")
            f[campo] = valor

        # Cantidad y precio
        cantidad = _entero(f["cantidad"])
        if cantidad is None or cantidad <= 0:
            motivos.append(f"Cantidad invalida: '{f['cantidad']}'")
        precio = _decimal(f["precio_unitario"])
        if precio is None or precio < 0:
            motivos.append(f"Precio invalido: '{f['precio_unitario']}'")

        # Descuento: nulo -> 0
        if f["descuento"] == "":
            descuento = Decimal("0")
        else:
            descuento = _decimal(f["descuento"])
            if descuento is None or descuento < 0:
                motivos.append(f"Descuento invalido: '{f['descuento']}'")

        # Moneda
        moneda = f["moneda"].upper()
        if moneda not in MONEDAS:
            motivos.append(f"Moneda desconocida: '{f['moneda']}'")

        # Importes (solo si todo lo anterior es valido)
        if not motivos:
            total_original = (cantidad * precio).quantize(DOS)
            if descuento > total_original:
                motivos.append("Descuento mayor al total de la venta")

        if motivos:
            rechazados.append({
                "id_origen": str(id_venta) if id_venta is not None else f"fila {fila.get('fila_origen')}",
                "datos_origen": json.dumps(original, ensure_ascii=False),
                "motivo": "; ".join(motivos),
            })
            continue

        tipo_cambio = tasa if moneda == "USD" else Decimal("1")
        aceptados.append({
            "id_venta": id_venta,
            "fecha_venta": fecha,
            "id_cliente": f["id_cliente"],
            "id_producto": f["id_producto"],
            "id_sucursal": f["id_sucursal"],
            "cantidad": cantidad,
            "precio_unitario": precio.quantize(DOS),
            "moneda": moneda,
            "tipo_cambio": tipo_cambio,
            "metodo_pago": f["metodo_pago"].upper(),
            "vendedor": f["vendedor"].upper() if f["vendedor"] else "NO ASIGNADO",
            "total_original": total_original,
            "descuento_original": descuento.quantize(DOS),
            "total_gtq": (total_original * tipo_cambio).quantize(DOS),
            "descuento_gtq": (descuento * tipo_cambio).quantize(DOS),
            "total_neto_gtq": ((total_original - descuento) * tipo_cambio).quantize(DOS),
        })

    print(f"[transform] aceptados={len(aceptados)}, rechazados={len(rechazados)}")
    return pd.DataFrame(aceptados), pd.DataFrame(rechazados, columns=["id_origen", "datos_origen", "motivo"])