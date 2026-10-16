"""
areas/inventario.py — RF4: Gestión de inventario de suministros médicos (Figura 5) + Query 3.

Tablas (script actualizado):
    informeSuministroIngreso  (ubicación, insumo) -> cantidadOrdenada = capacidad normal
    suministroUbicacion       (ubicación, insumo) -> cantidadDisponible = stock actual
    entradaSuministro         bitácora de entradas  [AJUSTE]
    informeSuministroSalida   bitácora de salidas (a un empleado) y traslados entre almacenes

Tres tipos de movimiento, todos en UNA transacción:
    E  entrada   suma stock en una ubicación                   -> entradaSuministro
    S  salida    resta stock y lo entrega a un empleado        -> informeSuministroSalida (destino NULL)
    T  traslado  resta en la fuente, suma en el destino        -> informeSuministroSalida (destino = almacén)
Cada insumo está atado obligatoriamente a un proveedor registrado (FK).
"""
import re

import pyodbc
from pyspark.sql import functions as F

from core import config, db, graficos, id_generators
from core.db import ErrorNegocio
from core.spark import leer_sql

TITULO = "Inventario de suministros médicos"
TABLAS = ["suministroUbicacion", "informeSuministroIngreso", "suministro", "proveedor",
          "entradaSuministro", "informeSuministroSalida"]

TIPOS = {"Q": "Quirúrgico", "F": "Farmacéutico", "M": "Material médico"}

# Una fila por (ubicación, insumo): existencia, % de la capacidad, valor (existencia × costo),
# proveedor, y la marca "critico" si está bajo el 15 % (mismo criterio que VW_Stock_Critico).
# Se renombran las columnas (existencia, capacidadNormal) para que la interfaz no dependa
# de los nombres de las tablas. idStock = "U01/Q001": clave única de la fila.
BASE_SQL = f"""
SELECT sb.codigoUbicacion + '/' + sb.codigoSuministro AS idStock,
       su.codigoSuministro, su.nombre AS suministro, su.descripcion, su.tipo,
       CASE su.tipo WHEN 'Q' THEN 'Quirúrgico' WHEN 'F' THEN 'Farmacéutico' ELSE 'Material médico' END AS tipoNombre,
       sb.codigoUbicacion, u.nombre AS ubicacion,
       sb.cantidadDisponible AS existencia, ig.cantidadOrdenada AS capacidadNormal,
       CAST(100.0 * sb.cantidadDisponible / ig.cantidadOrdenada AS dec(6,1)) AS porcentaje,
       su.costoUnitario, CAST(sb.cantidadDisponible * su.costoUnitario AS dec(12,2)) AS valorInventario,
       su.codigoProveedor, pr.nombre AS proveedor,
       CAST(CASE WHEN sb.cantidadDisponible < {config.UMBRAL_STOCK_CRITICO} * ig.cantidadOrdenada
                 THEN 1 ELSE 0 END AS bit) AS critico
FROM suministroUbicacion sb
JOIN informeSuministroIngreso ig ON ig.codigoUbicacion = sb.codigoUbicacion
                                AND ig.codigoSuministro = sb.codigoSuministro
JOIN suministro su ON su.codigoSuministro = sb.codigoSuministro
JOIN ubicacion u   ON u.codigoUbicacion = sb.codigoUbicacion
JOIN proveedor pr  ON pr.codigoProveedor = su.codigoProveedor
"""

COLUMNAS = [
    {"campo": "codigoSuministro", "etiqueta": "ID Sum.", "tipo": "id"},
    {"campo": "suministro", "etiqueta": "Suministro", "extra": "descripcion"},
    {"campo": "tipoNombre", "etiqueta": "Tipo"},
    {"campo": "ubicacion", "etiqueta": "Ubicación"},
    {"campo": "existencia", "etiqueta": "Stock", "tipo": "stock"},
    {"campo": "costoUnitario", "etiqueta": "Costo unitario", "tipo": "moneda"},
    {"campo": "valorInventario", "etiqueta": "Valor", "tipo": "moneda"},
    {"campo": "proveedor", "etiqueta": "Proveedor"},
]


def tabla() -> list[dict]:
    return db.consultar(BASE_SQL + " ORDER BY porcentaje, su.codigoSuministro")


def metricas() -> list[dict]:
    m = db.consultar_uno(f"""SELECT COUNT(DISTINCT codigoSuministro) AS insumos, SUM(valorInventario) AS valor,
                                    SUM(CAST(critico AS int)) AS criticos FROM ({BASE_SQL}) q""")
    proveedores = db.escalar("SELECT COUNT(*) FROM proveedor")
    semana = db.consultar_uno("""
        SELECT (SELECT COUNT(*) FROM entradaSuministro WHERE fecha >= DATEADD(day, -7, GETDATE())) AS entradas,
               (SELECT COUNT(*) FROM informeSuministroSalida WHERE codigoUbicacionDestino IS NULL
                                                             AND fecha >= DATEADD(day, -7, GETDATE())) AS salidas,
               (SELECT COUNT(*) FROM informeSuministroSalida WHERE codigoUbicacionDestino IS NOT NULL
                                                             AND fecha >= DATEADD(day, -7, GETDATE())) AS traslados""")
    return [
        {"etiqueta": "Insumos en catálogo", "valor": m["insumos"] or 0, "detalle": "quirúrgicos y farmacéuticos"},
        {"etiqueta": "Valor del inventario", "valor": m["valor"] or 0, "formato": "moneda", "detalle": "existencia × costo"},
        {"etiqueta": "Stock crítico", "valor": m["criticos"] or 0, "detalle": "< 15 % de la capacidad",
         "tono": "alerta" if m["criticos"] else ""},
        {"etiqueta": "Proveedores", "valor": proveedores or 0, "detalle": "registrados"},
        {"etiqueta": "Entradas (7 días)", "valor": semana["entradas"] or 0, "detalle": "reabastecimiento"},
        {"etiqueta": "Salidas (7 días)", "valor": semana["salidas"] or 0, "detalle": "entregadas a personal"},
        {"etiqueta": "Traslados (7 días)", "valor": semana["traslados"] or 0, "detalle": "entre almacenes"},
    ]


def stock_critico() -> list[dict]:
    """Query 3 — lee la vista VW_Stock_Critico (sql/02_indices_vistas.sql), con el contacto del proveedor."""
    return db.consultar("SELECT * FROM VW_Stock_Critico ORDER BY porcentaje, suministro")


def catalogos() -> dict:
    return {
        "proveedores": db.consultar("SELECT codigoProveedor AS id, nombre, contacto, telefono, correo,"
                                    " (SELECT COUNT(*) FROM suministro s WHERE s.codigoProveedor = p.codigoProveedor)"
                                    " AS insumos FROM proveedor p ORDER BY nombre"),
        "ubicaciones": db.consultar("""SELECT u.codigoUbicacion AS id, u.nombre,
                                              e.nombres + ' ' + e.apellidos AS encargado, u.empleadoEncargado
                                       FROM ubicacion u JOIN empleado e ON e.numeroEmpleado = u.empleadoEncargado
                                       ORDER BY u.codigoUbicacion"""),
        "suministros": db.consultar("""SELECT s.codigoSuministro AS id, s.nombre, s.descripcion, s.tipo,
                                              s.costoUnitario, s.codigoProveedor, p.nombre AS proveedor,
                                              ISNULL((SELECT SUM(cantidadDisponible) FROM suministroUbicacion sb
                                                      WHERE sb.codigoSuministro = s.codigoSuministro), 0) AS existencia
                                       FROM suministro s JOIN proveedor p ON p.codigoProveedor = s.codigoProveedor
                                       ORDER BY s.codigoSuministro"""),
        # Destinatario de una salida: cualquier empleado activo, sin importar su puesto.
        "empleados": db.consultar("""SELECT e.numeroEmpleado AS id, e.nombres + ' ' + e.apellidos AS nombre,
                                            p.nombre AS puesto
                                     FROM empleado e JOIN puesto p ON p.codigoPuesto = e.codigoPuesto
                                     WHERE e.activo = 1 ORDER BY e.nombres, e.apellidos"""),
        "tipos": [{"id": k, "nombre": v} for k, v in TIPOS.items()],
    }


def movimientos_recientes() -> list[dict]:
    """Últimas entradas, salidas y traslados juntos (UNION ALL de las dos bitácoras)."""
    return db.consultar("""
        SELECT TOP 15 * FROM (
            SELECT en.fecha, 'E' AS tipo, en.cantidad, s.nombre AS suministro, u.nombre AS ubicacion,
                   NULL AS destino, NULL AS destinatario
            FROM entradaSuministro en
            JOIN suministro s ON s.codigoSuministro = en.codigoSuministro
            JOIN ubicacion u  ON u.codigoUbicacion = en.codigoUbicacion
            UNION ALL
            SELECT sa.fecha, CASE WHEN sa.codigoUbicacionDestino IS NULL THEN 'S' ELSE 'T' END,
                   sa.cantidadDespachada, s.nombre, uf.nombre, ud.nombre, e.nombres + ' ' + e.apellidos
            FROM informeSuministroSalida sa
            JOIN suministro s  ON s.codigoSuministro = sa.codigoSuministro
            JOIN ubicacion uf  ON uf.codigoUbicacion = sa.codigoUbicacionFuente
            LEFT JOIN ubicacion ud ON ud.codigoUbicacion = sa.codigoUbicacionDestino
            JOIN empleado e    ON e.numeroEmpleado = sa.empleadoDestinatario
        ) m ORDER BY fecha DESC""")


def informe_por_ubicacion(codigo: str) -> dict:
    """Figura 5: insumos de una ubicación con su encargado y proveedor."""
    ubicacion = db.consultar_uno("""SELECT u.codigoUbicacion, u.nombre, e.numeroEmpleado,
                                           e.nombres + ' ' + e.apellidos AS encargado, p.nombre AS puesto
                                    FROM ubicacion u JOIN empleado e ON e.numeroEmpleado = u.empleadoEncargado
                                    JOIN puesto p ON p.codigoPuesto = e.codigoPuesto
                                    WHERE u.codigoUbicacion = ?""", (codigo,))
    if not ubicacion:
        raise ErrorNegocio("Ubicación inexistente.")
    filas = db.consultar(BASE_SQL + " WHERE sb.codigoUbicacion = ? ORDER BY su.codigoSuministro", (codigo,))
    return {"ubicacion": ubicacion, "filas": filas}


# ── Validaciones ─────────────────────────────────────────────────────
def _entero(valor, campo, errores, minimo=0):
    try:
        numero = int(valor)
        if numero < minimo:
            raise ValueError
        return numero
    except (TypeError, ValueError):
        errores[campo] = f"Número entero ≥ {minimo}."
        return None


def _validar_suministro(datos: dict, nuevo: bool) -> dict:
    """Al crear también se pide la ubicación y el stock inicial; al editar, solo los datos del insumo."""
    d = {
        "nombre": " ".join((datos.get("nombre") or "").split()),
        "descripcion": " ".join((datos.get("descripcion") or "").split()) or None,
        "tipo": (datos.get("tipo") or "").upper(),
        "codigoProveedor": datos.get("codigoProveedor") or "",
    }
    errores = {}
    if not d["nombre"]:
        errores["nombre"] = "Obligatorio."
    elif len(d["nombre"]) > 30:
        errores["nombre"] = "Máximo 30 caracteres."
    if d["descripcion"] and len(d["descripcion"]) > 50:
        errores["descripcion"] = "Máximo 50 caracteres."
    if nuevo and d["tipo"] not in TIPOS:
        errores["tipo"] = "Elegí el tipo."
    try:
        d["costoUnitario"] = round(float(datos.get("costoUnitario")), 2)
        if not 0 < d["costoUnitario"] < 100000:
            raise ValueError
    except (TypeError, ValueError):
        errores["costoUnitario"] = "Costo mayor a 0 (máx. 99,999.99)."
    # RF4: proveedor OBLIGATORIO y que exista en la tabla proveedor (además hay una FK en SQL).
    if not db.escalar("SELECT COUNT(*) FROM proveedor WHERE codigoProveedor = ?", (d["codigoProveedor"],)):
        errores["codigoProveedor"] = "Obligatorio: elegí un proveedor registrado."
    if nuevo:
        d["codigoUbicacion"] = datos.get("codigoUbicacion") or ""
        if not db.escalar("SELECT COUNT(*) FROM ubicacion WHERE codigoUbicacion = ?", (d["codigoUbicacion"],)):
            errores["codigoUbicacion"] = "Elegí la ubicación inicial."
        d["existencia"] = _entero(datos.get("existencia"), "existencia", errores, 0)
        d["capacidadNormal"] = _entero(datos.get("capacidadNormal"), "capacidadNormal", errores, 1)
    if errores:
        raise ErrorNegocio(errores)
    return d


def _habilitar_en_ubicacion(cur, ubicacion: str, codigo: str, capacidad: int) -> None:
    """
    Primera vez que un insumo llega a una ubicación: se crea su ficha en
    informeSuministroIngreso (con la capacidad normal) y su fila de stock en 0.
    El orden importa: suministroUbicacion tiene FK hacia informeSuministroIngreso.
    """
    cur.execute("INSERT INTO informeSuministroIngreso VALUES (?, ?, ?)", ubicacion, codigo, capacidad)
    cur.execute("INSERT INTO suministroUbicacion VALUES (?, ?, 0)", ubicacion, codigo)


def crear_suministro(datos: dict) -> dict:
    """Insumo + su ficha en la ubicación + stock inicial + la entrada en bitácora, en una transacción."""
    d = _validar_suministro(datos, nuevo=True)
    with id_generators.LOCK_IDS:
        codigo = id_generators.generar_id_suministro(d["tipo"])
        with db.transaccion() as cur:
            cur.execute("INSERT INTO suministro VALUES (?,?,?,?,?,?)", codigo, d["nombre"], d["descripcion"],
                        d["tipo"], d["costoUnitario"], d["codigoProveedor"])
            _habilitar_en_ubicacion(cur, d["codigoUbicacion"], codigo, d["capacidadNormal"])
            if d["existencia"]:
                cur.execute("UPDATE suministroUbicacion SET cantidadDisponible = ?"
                            " WHERE codigoUbicacion = ? AND codigoSuministro = ?",
                            d["existencia"], d["codigoUbicacion"], codigo)
                cur.execute("INSERT INTO entradaSuministro (codigoUbicacion, codigoSuministro, cantidad)"
                            " VALUES (?, ?, ?)", d["codigoUbicacion"], codigo, d["existencia"])
    return {"codigoSuministro": codigo, "mensaje": f"Insumo {codigo} registrado."}


def actualizar_suministro(codigo: str, datos: dict) -> dict:
    d = _validar_suministro(datos, nuevo=False)
    with db.transaccion() as cur:
        cur.execute("UPDATE suministro SET nombre=?, descripcion=?, costoUnitario=?, codigoProveedor=?"
                    " WHERE codigoSuministro = ?", d["nombre"], d["descripcion"], d["costoUnitario"],
                    d["codigoProveedor"], codigo)
        if cur.rowcount == 0:
            raise ErrorNegocio(f"No existe el insumo {codigo}.")
    return {"mensaje": f"Insumo {codigo} actualizado."}


def eliminar_suministro(codigo: str) -> dict:
    """Solo se puede borrar un insumo cargado por error: sin stock y sin salidas/traslados."""
    with db.transaccion() as cur:
        stock = cur.execute("SELECT ISNULL(SUM(cantidadDisponible), 0) FROM suministroUbicacion"
                            " WHERE codigoSuministro = ?", codigo).fetchone()[0]
        salidas = cur.execute("SELECT COUNT(*) FROM informeSuministroSalida WHERE codigoSuministro = ?",
                              codigo).fetchone()[0]
        if stock or salidas:
            raise ErrorNegocio(f"{codigo} tiene stock o salidas registradas: no se puede borrar "
                               "(conservá el historial; dejalo en 0).")
        # Se borra de "abajo hacia arriba" para respetar las FK.
        cur.execute("DELETE FROM entradaSuministro WHERE codigoSuministro = ?", codigo)
        cur.execute("DELETE FROM suministroUbicacion WHERE codigoSuministro = ?", codigo)
        cur.execute("DELETE FROM informeSuministroIngreso WHERE codigoSuministro = ?", codigo)
        cur.execute("DELETE FROM suministro WHERE codigoSuministro = ?", codigo)
        if cur.rowcount == 0:
            raise ErrorNegocio(f"No existe el insumo {codigo}.")
    return {"mensaje": f"Insumo {codigo} eliminado."}


def _stock_bloqueado(cur, ubicacion: str, codigo: str):
    """
    Lee el stock de (ubicación, insumo) BLOQUEANDO la fila hasta el COMMIT
    (UPDLOCK, HOLDLOCK): si dos personas despachan a la vez, la segunda
    espera y ve el stock ya descontado. Devuelve None si no existe la fila.
    """
    fila = cur.execute("SELECT cantidadDisponible FROM suministroUbicacion WITH (UPDLOCK, HOLDLOCK)"
                       " WHERE codigoUbicacion = ? AND codigoSuministro = ?", ubicacion, codigo).fetchone()
    return None if fila is None else fila[0]


def _sumar(cur, ubicacion, codigo, cantidad):
    cur.execute("UPDATE suministroUbicacion SET cantidadDisponible = cantidadDisponible + ?"
                " WHERE codigoUbicacion = ? AND codigoSuministro = ?", cantidad, ubicacion, codigo)


def registrar_movimiento(datos: dict) -> dict:
    """
    Entrada (E), salida a un empleado (S) o traslado entre almacenes (T).
    Todo ocurre en UNA transacción: si falta stock o algún dato es inválido,
    no se modifica nada.
    """
    errores = {}
    ubicacion = datos.get("codigoUbicacion") or ""          # para E: dónde entra; para S/T: de dónde sale
    codigo = datos.get("codigoSuministro") or ""
    tipo = (datos.get("tipo") or "").upper()
    destino = datos.get("codigoUbicacionDestino") or ""
    destinatario = datos.get("empleadoDestinatario") or ""
    cantidad = _entero(datos.get("cantidad"), "cantidad", errores, 1)
    if tipo not in ("E", "S", "T"):
        errores["tipo"] = "Entrada, salida o traslado."
    if not codigo:
        errores["codigoSuministro"] = "Elegí el insumo."
    if not ubicacion:
        errores["codigoUbicacion"] = "Elegí la ubicación."
    if tipo == "T":
        if not destino:
            errores["codigoUbicacionDestino"] = "Elegí el almacén destino."
        elif destino == ubicacion:
            errores["codigoUbicacionDestino"] = "El destino debe ser distinto del origen."
        elif not db.escalar("SELECT COUNT(*) FROM ubicacion WHERE codigoUbicacion = ?", (destino,)):
            errores["codigoUbicacionDestino"] = "Almacén inexistente."
        if not destinatario and destino:
            # Si no se indica quién recibe un traslado, lo recibe el encargado del almacén destino.
            destinatario = db.escalar("SELECT empleadoEncargado FROM ubicacion WHERE codigoUbicacion = ?",
                                      (destino,)) or ""
    if tipo in ("S", "T"):
        # Destinatario: cualquier empleado ACTIVO, sin importar su puesto.
        if not destinatario:
            errores["empleadoDestinatario"] = "Indicá qué empleado recibe."
        elif not db.escalar("SELECT COUNT(*) FROM empleado WHERE numeroEmpleado = ? AND activo = 1",
                            (destinatario,)):
            errores["empleadoDestinatario"] = "Ese empleado no existe o está dado de baja."
    if errores:
        raise ErrorNegocio(errores)

    with db.transaccion() as cur:
        if tipo == "E":
            if _stock_bloqueado(cur, ubicacion, codigo) is None:
                capacidad = _entero(datos.get("capacidadNormal"), "capacidadNormal", errores, 1)
                if errores:
                    raise ErrorNegocio({"capacidadNormal": "Primera entrada en esta ubicación: indicá la capacidad normal."})
                _habilitar_en_ubicacion(cur, ubicacion, codigo, capacidad)
            _sumar(cur, ubicacion, codigo, cantidad)
            cur.execute("INSERT INTO entradaSuministro (codigoUbicacion, codigoSuministro, cantidad) VALUES (?, ?, ?)",
                        ubicacion, codigo, cantidad)
        else:
            # S y T descuentan del origen. En un traslado se bloquean las dos filas
            # siempre en el mismo orden (por código) para que dos traslados cruzados
            # (A->B y B->A al mismo tiempo) no se queden esperándose entre sí (deadlock).
            if tipo == "T":
                for ubic in sorted([ubicacion, destino]):
                    _stock_bloqueado(cur, ubic, codigo)
            existencia = _stock_bloqueado(cur, ubicacion, codigo)
            if existencia is None:
                raise ErrorNegocio("Ese insumo no tiene stock en esa ubicación.")
            if cantidad > existencia:
                raise ErrorNegocio({"cantidad": f"Stock insuficiente: hay {existencia} unidades en esa ubicación."})
            _sumar(cur, ubicacion, codigo, -cantidad)
            if tipo == "T":
                if _stock_bloqueado(cur, destino, codigo) is None:
                    capacidad = _entero(datos.get("capacidadNormal"), "capacidadNormal", errores, 1)
                    if errores:
                        raise ErrorNegocio({"capacidadNormal": "El destino no maneja este insumo todavía: "
                                                               "indicá su capacidad normal allí."})
                    _habilitar_en_ubicacion(cur, destino, codigo, capacidad)
                _sumar(cur, destino, codigo, cantidad)
            cur.execute("INSERT INTO informeSuministroSalida (codigoUbicacionFuente, codigoSuministro,"
                        " cantidadDespachada, codigoUbicacionDestino, empleadoDestinatario) VALUES (?, ?, ?, ?, ?)",
                        ubicacion, codigo, cantidad, destino if tipo == "T" else None, destinatario)
        nueva = _stock_bloqueado(cur, ubicacion, codigo)
    textos = {"E": "Entrada de {n} u. registrada. Existencia actual: {q}.",
              "S": "Salida de {n} u. registrada. Quedan {q} en el origen.",
              "T": "Traslado de {n} u. registrado. Quedan {q} en el origen."}
    return {"mensaje": textos[tipo].format(n=cantidad, q=nueva)}


# ── Proveedores ──────────────────────────────────────────────────────
def _validar_proveedor(datos: dict, codigo_actual: str = "") -> dict:
    d = {c: " ".join((datos.get(c) or "").split()) for c in ("nombre", "contacto", "telefono", "correo")}
    errores = {}
    for campo, maximo in {"nombre": 30, "contacto": 40, "telefono": 20, "correo": 50}.items():
        if len(d[campo]) > maximo:
            errores[campo] = f"Máximo {maximo} caracteres."
    if not d["nombre"]:
        errores["nombre"] = "Obligatorio."
    elif db.escalar("SELECT COUNT(*) FROM proveedor WHERE nombre = ? AND codigoProveedor <> ?",
                    (d["nombre"], codigo_actual)):
        errores["nombre"] = "Ya existe un proveedor con ese nombre."
    if not re.match(r"^\+?[0-9][0-9 -]{6,18}$", d["telefono"]):
        errores["telefono"] = "Teléfono obligatorio (números, espacios, guiones)."
    if d["correo"] and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", d["correo"]):
        errores["correo"] = "Correo inválido."
    if errores:
        raise ErrorNegocio(errores)
    d["contacto"] = d["contacto"] or None
    d["correo"] = d["correo"] or None
    return d


def crear_proveedor(datos: dict) -> dict:
    d = _validar_proveedor(datos)
    with id_generators.LOCK_IDS:
        codigo = id_generators.generar_id_proveedor()
        try:
            with db.transaccion() as cur:
                cur.execute("INSERT INTO proveedor VALUES (?,?,?,?,?)", codigo, d["nombre"], d["contacto"],
                            d["telefono"], d["correo"])
        except pyodbc.IntegrityError as exc:
            raise ErrorNegocio({"nombre": "Ya existe un proveedor con ese nombre."}) from exc
    return {"codigoProveedor": codigo, "mensaje": f"Proveedor {codigo} registrado."}


def actualizar_proveedor(codigo: str, datos: dict) -> dict:
    d = _validar_proveedor(datos, codigo)
    with db.transaccion() as cur:
        cur.execute("UPDATE proveedor SET nombre=?, contacto=?, telefono=?, correo=? WHERE codigoProveedor = ?",
                    d["nombre"], d["contacto"], d["telefono"], d["correo"], codigo)
    return {"mensaje": f"Proveedor {codigo} actualizado."}


# ── PySpark: dataset, filtros y gráficas ─────────────────────────────
def dataset():
    return leer_sql(BASE_SQL)


def filtros() -> list[dict]:
    cat = catalogos()
    return [
        {"id": "texto", "tipo": "texto", "etiqueta": "Buscar",
         "columnas": ["codigoSuministro", "suministro", "descripcion"], "ayuda": "código, nombre o descripción"},
        {"id": "tipo", "tipo": "opcion", "etiqueta": "Tipo", "opciones": list(TIPOS.items())},
        {"id": "codigoUbicacion", "tipo": "opcion", "etiqueta": "Ubicación",
         "opciones": [(u["id"], u["nombre"]) for u in cat["ubicaciones"]]},
        {"id": "codigoProveedor", "tipo": "opcion", "etiqueta": "Proveedor",
         "opciones": [(p["id"], p["nombre"]) for p in cat["proveedores"]]},
        {"id": "critico", "tipo": "booleano", "etiqueta": "Stock crítico"},
        {"id": "porcentaje", "tipo": "rango", "etiqueta": "% de capacidad"},
        {"id": "costoUnitario", "tipo": "rango", "etiqueta": "Costo unitario (Q.)"},
    ]


def _nivel_stock(df):
    """% de capacidad por insumo (sumando todas sus ubicaciones). Los críticos salen en rojo."""
    pdf = (df.groupBy("suministro")
           .agg((F.sum("existencia") * 100 / F.sum("capacidadNormal")).alias("pct"))
           .toPandas())
    return graficos.barras_h(pdf, "suministro", "pct", "Nivel de stock (% de la capacidad normal)",
                             formato="{:.0f} %", resaltar=lambda f: f["pct"] < config.UMBRAL_STOCK_CRITICO * 100,
                             eje="% de capacidad — en rojo: bajo el 15 %")


def _salidas_por_semana(df):
    """Consumo = salidas entregadas a personal (los traslados no son consumo, solo cambian de lugar)."""
    mov = leer_sql("SELECT codigoUbicacionFuente + '/' + codigoSuministro AS idStock, cantidadDespachada AS cantidad,"
                   " fecha FROM informeSuministroSalida WHERE codigoUbicacionDestino IS NULL")
    pdf = (mov.join(df.select("idStock"), "idStock")
           .groupBy(F.date_trunc("week", "fecha").alias("semana")).agg(F.sum("cantidad").alias("unidades"))
           .orderBy("semana").toPandas())
    return graficos.linea(pdf, "semana", "unidades", "Consumo semanal (salidas)", "unidades")


GRAFICOS = {
    "stock": {"titulo": "Nivel de stock", "tipo": "Barras", "fn": _nivel_stock},
    "proveedor": {"titulo": "Valor por proveedor", "tipo": "Barras",
                  "fn": lambda df: graficos.barras_h(
                      df.groupBy("proveedor").agg(F.sum("valorInventario").alias("valor")).toPandas(),
                      "proveedor", "valor", "Valor del inventario por proveedor", formato="Q.{:,.0f}",
                      eje="quetzales")},
    "tipo": {"titulo": "Por tipo de insumo", "tipo": "Dona",
             "fn": lambda df: graficos.dona(
                 df.select("codigoSuministro", "tipoNombre").distinct().groupBy("tipoNombre").count().toPandas(),
                 "tipoNombre", "count", "Insumos por tipo")},
    "costos": {"titulo": "Costos unitarios", "tipo": "Histograma",
               "fn": lambda df: graficos.histograma(
                   df.select("codigoSuministro", "costoUnitario").distinct().toPandas()["costoUnitario"],
                   "Distribución de costos unitarios", "quetzales", bins=10)},
    "consumo": {"titulo": "Consumo semanal", "tipo": "Línea", "fn": _salidas_por_semana},
}
