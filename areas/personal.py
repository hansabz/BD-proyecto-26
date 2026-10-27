"""
areas/personal.py — RF1: Gestión del personal médico y administrativo.
Alta (con ID generado en PySpark según el rol), modificación, consulta y
baja lógica. NSS único y catálogos (puesto, especialidad, turno) validados.
"""
import datetime as dt
import re

import pyodbc
from pyspark.sql import functions as F

from core import db, graficos, id_generators
from core.db import ErrorNegocio
from core.spark import leer_sql

TITULO = "Personal médico y administrativo"
# Si alguna de estas tablas cambia (otro usuario da de alta a alguien), la vista se refresca sola.
TABLAS = ["empleado", "asignacionSala", "puesto"]

# SELECT base: empleado + nombres legibles de sus catálogos (JOIN) + sus salas.
# STRING_AGG junta varias filas en un texto: "Clínica 2, Cuidados Intensivos".

BASE_SQL = """
SELECT e.numeroEmpleado, e.nombres, e.apellidos, e.NSS,
       e.codigoPuesto, p.nombre AS puesto,
       e.codigoEspecialidad, es.nombre AS especialidad,
       e.codigoTurno, t.nombre AS turno,
       p.salario, e.fechaContratacion,
       -- antigüedad en años con un decimal (DATEDIFF en días / 365.25 para contar bisiestos)
       CAST(DATEDIFF(day, e.fechaContratacion, GETDATE()) / 365.25 AS dec(5,1)) AS antiguedad,
       e.activo, e.fechaBaja,
       ISNULL((SELECT STRING_AGG(s.nombre, ', ') FROM asignacionSala a
               JOIN sala s ON s.codigoSala = a.codigoSala
               WHERE a.numeroEmpleado = e.numeroEmpleado), '') AS salas,
       ISNULL((SELECT STRING_AGG(a.codigoSala, ',') FROM asignacionSala a
               WHERE a.numeroEmpleado = e.numeroEmpleado), '') AS codigosSala
FROM empleado e
JOIN puesto p        ON p.codigoPuesto = e.codigoPuesto
JOIN especialidad es ON es.codigoEspecialidad = e.codigoEspecialidad
JOIN turno t         ON t.codigoTurno = e.codigoTurno
"""
ORDEN = (" ORDER BY e.activo DESC, CASE LEFT(e.numeroEmpleado, 1) WHEN 'M' THEN 1 WHEN 'E' THEN 2 ELSE 3 END,"
         " e.numeroEmpleado")

COLUMNAS = [
    {"campo": "numeroEmpleado", "etiqueta": "No. Emp", "tipo": "id"},
    {"campo": "nombres", "etiqueta": "Nombres"},
    {"campo": "apellidos", "etiqueta": "Apellidos"},
    {"campo": "NSS", "etiqueta": "NSS", "tipo": "mono"},
    {"campo": "puesto", "etiqueta": "Puesto"},
    {"campo": "especialidad", "etiqueta": "Especialidad"},
    {"campo": "turno", "etiqueta": "Turno", "tipo": "turno"},
    {"campo": "salas", "etiqueta": "Salas"},
    {"campo": "fechaContratacion", "etiqueta": "Contratación", "tipo": "fecha"},
    {"campo": "antiguedad", "etiqueta": "Antigüedad (años)", "tipo": "numero"},
    {"campo": "salario", "etiqueta": "Salario base", "tipo": "moneda"},
    {"campo": "activo", "etiqueta": "Estado", "tipo": "activo"},
]

# Formato del NSS como en la Figura 1 (883920-K). El mismo control está en SQL (CK_empleado_NSS).
PATRON_NSS = r"^[0-9]{6}-[0-9A-Z]$"


# ── Catálogos ────────────────────────────────────────────────────────
def catalogos() -> dict:
    """Opciones de los <select> del formulario: solo valores que existen en los catálogos."""
    return {
        "puestos": db.consultar("SELECT codigoPuesto AS id, nombre, salario FROM puesto ORDER BY nombre"),
        "especialidades": db.consultar("SELECT codigoEspecialidad AS id, nombre FROM especialidad ORDER BY nombre"),
        "turnos": db.consultar("SELECT codigoTurno AS id, nombre FROM turno ORDER BY codigoTurno"),
        "salas": db.consultar("SELECT codigoSala AS id, nombre FROM sala ORDER BY codigoSala"),
    }


# ── Consulta ─────────────────────────────────────────────────────────
def tabla() -> list[dict]:
    return db.consultar(BASE_SQL + ORDEN)


def obtener(numero: str) -> dict:
    fila = db.consultar_uno(BASE_SQL + " WHERE e.numeroEmpleado = ?", (numero,))
    if not fila:
        raise ErrorNegocio(f"No existe el empleado {numero}.")
    return fila


def asignacion_por_sala(codigo_sala: str | None) -> list[dict]:
    """Query 1 — VW_Asignacion_Personal (Figura 1)."""
    if codigo_sala:
        return db.consultar("SELECT * FROM VW_Asignacion_Personal WHERE codigoSala = ? ORDER BY puesto, nombreEmpleado",
                            (codigo_sala,))
    return db.consultar("SELECT * FROM VW_Asignacion_Personal ORDER BY codigoSala, puesto, nombreEmpleado")


def metricas() -> list[dict]:
    """KPIs de la página. Un solo SELECT con SUM(CASE ...) cuenta todo de una vez."""
    m = db.consultar_uno("""
        SELECT SUM(CASE WHEN e.activo = 1 THEN 1 ELSE 0 END) AS activos,
               SUM(CASE WHEN e.activo = 1 AND e.numeroEmpleado LIKE 'M%' THEN 1 ELSE 0 END) AS medicos,
               SUM(CASE WHEN e.activo = 1 AND e.numeroEmpleado LIKE 'E%' THEN 1 ELSE 0 END) AS enfermeria,
               SUM(CASE WHEN e.activo = 1 AND e.numeroEmpleado LIKE 'A%' THEN 1 ELSE 0 END) AS auxiliares,
               SUM(CASE WHEN e.activo = 0 THEN 1 ELSE 0 END) AS bajas,
               SUM(CASE WHEN e.activo = 1 THEN p.salario ELSE 0 END) AS nomina,
               -- antigüedad promedio del personal activo, a partir de fechaContratacion
               AVG(CASE WHEN e.activo = 1 THEN DATEDIFF(day, e.fechaContratacion, GETDATE()) / 365.25 END)
                 AS antiguedad
        FROM empleado e JOIN puesto p ON p.codigoPuesto = e.codigoPuesto""")
    sin_sala = db.escalar("""SELECT COUNT(*) FROM empleado e WHERE e.activo = 1
                             AND NOT EXISTS (SELECT 1 FROM asignacionSala a WHERE a.numeroEmpleado = e.numeroEmpleado)""")
    return [
        {"etiqueta": "Personal activo", "valor": m["activos"] or 0, "detalle": f"{m['bajas'] or 0} de baja lógica"},
        {"etiqueta": "Médicos", "valor": m["medicos"] or 0, "detalle": "prefijo M"},
        {"etiqueta": "Enfermería", "valor": m["enfermeria"] or 0, "detalle": "prefijo E"},
        {"etiqueta": "Auxiliares", "valor": m["auxiliares"] or 0, "detalle": "prefijo A"},
        {"etiqueta": "Nómina mensual", "valor": m["nomina"] or 0, "formato": "moneda", "detalle": "salario base activo"},
        {"etiqueta": "Antigüedad promedio", "valor": round(float(m["antiguedad"] or 0), 1),
         "detalle": "años desde la contratación"},
        {"etiqueta": "Sin sala asignada", "valor": sin_sala or 0, "detalle": "personal activo",
         "tono": "alerta" if sin_sala else ""},
    ]


# ── Validación ───────────────────────────────────────────────────────
def _validar(datos: dict, numero_actual: str | None = None) -> dict:
    """
    Revisa TODO el formulario y junta los errores por campo en un dict, para
    mostrarlos todos juntos (no de a uno). `numero_actual` se usa al editar:
    el NSS puede repetirse consigo mismo, pero no con otro empleado.
    Devuelve los datos ya limpios (sin espacios de más, NSS en mayúsculas).
    """
    limpio = {
        "nombres": " ".join((datos.get("nombres") or "").split()),
        "apellidos": " ".join((datos.get("apellidos") or "").split()),
        "NSS": (datos.get("NSS") or "").strip().upper(),
        "codigoPuesto": datos.get("codigoPuesto") or "",
        "codigoEspecialidad": datos.get("codigoEspecialidad") or "",
        "codigoTurno": datos.get("codigoTurno") or "",
        "salas": [s for s in (datos.get("salas") or []) if s],
    }
    errores = {}
    # fechaContratacion: obligatoria (NOT NULL en el script) y no puede ser futura.
    try:
        limpio["fechaContratacion"] = dt.date.fromisoformat(datos.get("fechaContratacion") or "")
        if limpio["fechaContratacion"] > dt.date.today():
            errores["fechaContratacion"] = "No puede ser una fecha futura."
        elif limpio["fechaContratacion"].year < 1950:
            errores["fechaContratacion"] = "Fecha fuera de rango."
    except ValueError:
        errores["fechaContratacion"] = "Obligatoria."
    for campo in ("nombres", "apellidos"):
        if not limpio[campo]:
            errores[campo] = "Obligatorio."
        elif len(limpio[campo]) > 25:
            errores[campo] = "Máximo 25 caracteres."
    if not re.match(PATRON_NSS, limpio["NSS"]):
        errores["NSS"] = "Formato 000000-X (6 dígitos, guion, dígito o letra)."
    else:
        # RF1: NSS único. Se busca también entre los dados de baja (siguen existiendo).
        duplicado = db.consultar_uno(
            "SELECT numeroEmpleado, nombres, apellidos, activo FROM empleado WHERE NSS = ? AND numeroEmpleado <> ?",
            (limpio["NSS"], numero_actual or ""))
        if duplicado:
            estado = "" if duplicado["activo"] else " (dado de baja)"
            errores["NSS"] = (f"Ya pertenece a {duplicado['numeroEmpleado']} — "
                              f"{duplicado['nombres']} {duplicado['apellidos']}{estado}.")
    catalogos_validos = {
        "codigoPuesto": ("puesto", "codigoPuesto"),
        "codigoEspecialidad": ("especialidad", "codigoEspecialidad"),
        "codigoTurno": ("turno", "codigoTurno"),
    }
    # RF1: turno y especialidad "válidos según los catálogos": deben existir en su tabla.
    for campo, (tabla_cat, col) in catalogos_validos.items():
        if not limpio[campo] or not db.escalar(f"SELECT COUNT(*) FROM {tabla_cat} WHERE {col} = ?", (limpio[campo],)):
            errores[campo] = "Elegí una opción válida del catálogo."
    for sala in limpio["salas"]:
        if not db.escalar("SELECT COUNT(*) FROM sala WHERE codigoSala = ?", (sala,)):
            errores["salas"] = f"La sala {sala} no existe."
    if errores:
        raise ErrorNegocio(errores)
    return limpio


# ── Alta / modificación / baja ───────────────────────────────────────
def crear(datos: dict) -> dict:
    """Alta: validar -> generar ID con PySpark según el rol -> INSERT en una transacción."""
    d = _validar(datos)
    with id_generators.LOCK_IDS:            # nadie más genera IDs hasta que terminemos de insertar
        numero = id_generators.generar_id_empleado(d["codigoPuesto"])
        try:
            with db.transaccion() as cur:
                cur.execute(
                    "INSERT INTO empleado (numeroEmpleado, nombres, apellidos, NSS, codigoPuesto,"
                    " codigoEspecialidad, codigoTurno, fechaContratacion) VALUES (?,?,?,?,?,?,?,?)",
                    numero, d["nombres"], d["apellidos"], d["NSS"], d["codigoPuesto"],
                    d["codigoEspecialidad"], d["codigoTurno"], d["fechaContratacion"])
                for sala in d["salas"]:
                    # fechaAsignacion no se manda: la pone el DEFAULT de SQL Server (hoy).
                    cur.execute("INSERT INTO asignacionSala (codigoSala, numeroEmpleado) VALUES (?, ?)", sala, numero)
        except pyodbc.IntegrityError as exc:
            # Red de seguridad: si aun así se coló un NSS repetido, lo frena la
            # restricción UNIQUE de SQL Server y lo traducimos a un mensaje claro.
            if "UQ_empleado_NSS" in str(exc):
                raise ErrorNegocio({"NSS": "Ese NSS ya está registrado."}) from exc
            raise
    return {"numeroEmpleado": numero, "mensaje": f"Empleado {numero} registrado."}


def actualizar(numero: str, datos: dict) -> dict:
    """
    Modificación. El ID no cambia. Las salas se sincronizan en la misma transacción:
    se quitan las que se desmarcaron y se agregan las nuevas; las que siguen
    conservan su fechaAsignacion original.
    """
    actual = obtener(numero)
    d = _validar(datos, numero)
    if d["codigoPuesto"] != actual["codigoPuesto"]:
        nuevo_prefijo = id_generators.prefijo_por_rol(id_generators.extraer_rol(d["codigoPuesto"]))
        if nuevo_prefijo != numero[0]:
            raise ErrorNegocio({"codigoPuesto": (
                f"El ID {numero} es de otro rol. Para cambiar de rol, da de baja y registrá de nuevo.")})
    with db.transaccion() as cur:
        cur.execute(
            "UPDATE empleado SET nombres=?, apellidos=?, NSS=?, codigoPuesto=?, codigoEspecialidad=?, codigoTurno=?,"
            " fechaContratacion=? WHERE numeroEmpleado = ?",
            d["nombres"], d["apellidos"], d["NSS"], d["codigoPuesto"], d["codigoEspecialidad"], d["codigoTurno"],
            d["fechaContratacion"], numero)
        actuales = {fila[0] for fila in cur.execute(
            "SELECT codigoSala FROM asignacionSala WHERE numeroEmpleado = ?", numero).fetchall()}
        for sala in actuales - set(d["salas"]):          # salas que se desmarcaron
            cur.execute("DELETE FROM asignacionSala WHERE codigoSala = ? AND numeroEmpleado = ?", sala, numero)
        for sala in set(d["salas"]) - actuales:          # salas nuevas (fecha = DEFAULT hoy)
            cur.execute("INSERT INTO asignacionSala (codigoSala, numeroEmpleado) VALUES (?, ?)", sala, numero)
    return {"mensaje": f"Empleado {numero} actualizado."}


def dar_de_baja(numero: str) -> dict:
    """
    Baja LÓGICA (RF1): no se borra la fila, se marca activo = 0 con la fecha.
    Así se conserva el historial (citas atendidas, ubicaciones a cargo, etc.).
    """
    with db.transaccion() as cur:
        cur.execute("UPDATE empleado SET activo = 0, fechaBaja = GETDATE() WHERE numeroEmpleado = ? AND activo = 1",
                    numero)
        if cur.rowcount == 0:     # rowcount = cuántas filas modificó el UPDATE
            raise ErrorNegocio(f"{numero} no existe o ya estaba dado de baja.")
    return {"mensaje": f"{numero} dado de baja (baja lógica: el registro se conserva)."}


def reactivar(numero: str) -> dict:
    with db.transaccion() as cur:
        cur.execute("UPDATE empleado SET activo = 1, fechaBaja = NULL WHERE numeroEmpleado = ? AND activo = 0", numero)
        if cur.rowcount == 0:
            raise ErrorNegocio(f"{numero} no existe o ya estaba activo.")
    return {"mensaje": f"{numero} reactivado."}


# ── PySpark: dataset, filtros y gráficas ─────────────────────────────
def dataset():
    """El mismo SELECT de la tabla, pero como DataFrame de Spark (para filtrar, limpiar y graficar)."""
    return leer_sql(BASE_SQL)


def filtros() -> list[dict]:
    """Filtros del botón "Filtrar". Para agregar uno nuevo, sumá un dict acá (ver core/filtros.py)."""
    cat = catalogos()
    return [
        {"id": "texto", "tipo": "texto", "etiqueta": "Buscar",
         "columnas": ["numeroEmpleado", "nombres", "apellidos", "NSS"], "ayuda": "ID, nombre o NSS"},
        {"id": "codigoPuesto", "tipo": "opcion", "etiqueta": "Puesto",
         "opciones": [(p["id"], p["nombre"]) for p in cat["puestos"]]},
        {"id": "codigoEspecialidad", "tipo": "opcion", "etiqueta": "Especialidad",
         "opciones": [(e["id"], e["nombre"]) for e in cat["especialidades"]]},
        {"id": "codigoTurno", "tipo": "opcion", "etiqueta": "Turno",
         "opciones": [(t["id"], t["nombre"]) for t in cat["turnos"]]},
        {"id": "activo", "tipo": "booleano", "etiqueta": "Activo"},
        {"id": "salario", "tipo": "rango", "etiqueta": "Salario base (Q.)"},
        {"id": "antiguedad", "tipo": "rango", "etiqueta": "Antigüedad (años)"},
        {"id": "fechaContratacion", "tipo": "fechas", "etiqueta": "Fecha de contratación"},
    ]


def _conteo(df, col):
    """Cuántas filas hay por cada valor de `col` (Spark groupBy) -> pandas."""
    return df.groupBy(col).count().toPandas()


def _antiguedad(df):
    anios = df.select("antiguedad").toPandas()["antiguedad"]
    promedio = anios.mean() if len(anios) else None
    return graficos.histograma(anios, "Antigüedad del personal (desde la fecha de contratación)",
                               "años en el hospital", bins=8, linea=promedio,
                               etiqueta_linea=f"promedio {promedio:.1f} años" if promedio is not None else "")


# id -> título, tipo de gráfica (elegido según el tipo de dato) y la función que la dibuja.
# Cada función recibe el DataFrame ya filtrado/limpio según lo que esté viendo el usuario.
GRAFICOS = {
    "puesto": {"titulo": "Distribución por puesto", "tipo": "Dona",
               "fn": lambda df: graficos.dona(_conteo(df, "puesto"), "puesto", "count", "Personal por puesto")},
    "especialidad": {"titulo": "Por especialidad", "tipo": "Barras",
                     "fn": lambda df: graficos.barras_h(_conteo(df, "especialidad"), "especialidad", "count",
                                                        "Personal por especialidad", eje="empleados")},
    "turno": {"titulo": "Cobertura por turno", "tipo": "Dona",
              "fn": lambda df: graficos.dona(_conteo(df, "turno"), "turno", "count", "Cobertura por turno")},
    "nomina": {"titulo": "Nómina por puesto", "tipo": "Barras",
               "fn": lambda df: graficos.barras_h(
                   df.groupBy("puesto").agg(F.sum("salario").alias("total")).toPandas(),
                   "puesto", "total", "Nómina mensual por puesto", formato="Q.{:,.0f}", eje="quetzales")},
    # Antigüedad = años entre fechaContratacion y hoy (columna calculada en BASE_SQL).
    # Es una variable numérica continua -> histograma; la línea marca el promedio.
    "antiguedad": {"titulo": "Antigüedad", "tipo": "Histograma", "fn": lambda df: _antiguedad(df)},
}
