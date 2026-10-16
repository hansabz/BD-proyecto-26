"""
core/db.py — Acceso transaccional a SQL Server con pyodbc.

¿Por qué hay dos caminos hacia la base de datos?
  • ESCRIBIR (INSERT / UPDATE / DELETE del CRUD) → pasa por este módulo.
    PySpark está hecho para leer y analizar grandes volúmenes, pero no sabe
    hacer transacciones (BEGIN / COMMIT / ROLLBACK). pyodbc sí.
  • ANALIZAR (filtros, generación de IDs, gráficas) → pasa por
    core/spark.py, que usa ESTE módulo para leer las filas y después las
    convierte en un DataFrame de Spark.

pyodbc habla con SQL Server a través del "driver ODBC" que instala Microsoft.
Los "?" en las consultas son parámetros: pyodbc los reemplaza de forma segura
(evita inyección SQL), nunca armamos SQL pegando texto del usuario.
"""
from contextlib import contextmanager
import datetime
import decimal

import pyodbc

from core import config

# En orden de preferencia: si hay más de uno instalado, se usa el más nuevo.
_DRIVERS_PREFERIDOS = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
)


class ErrorNegocio(Exception):
    """
    Una regla del hospital que no se cumple (NSS repetido, cama ocupada,
    stock insuficiente...). No es un error del programa: app.py la atrapa y
    le muestra el mensaje al usuario con un código HTTP 400.

    Puede recibir:
      • un texto  -> mensaje general        ErrorNegocio("La cama está ocupada")
      • un dict   -> errores por campo      ErrorNegocio({"NSS": "Ya existe"})
        (el formulario pinta cada mensaje debajo del campo correspondiente)
    """


def _driver() -> str:
    instalados = pyodbc.drivers()
    for nombre in _DRIVERS_PREFERIDOS:
        if nombre in instalados:
            return nombre
    raise RuntimeError("No hay un driver ODBC de SQL Server instalado.")


def cadena_conexion(database: str | None = None) -> str:
    """Arma el texto de conexión: driver + servidor + base + forma de autenticarse."""
    partes = [
        f"DRIVER={{{_driver()}}}",
        f"SERVER={config.DB_SERVER}",
        f"DATABASE={database or config.DB_NAME}",
        "TrustServerCertificate=yes",   # SQL Express local usa un certificado propio
        "Encrypt=no",
    ]
    if config.DB_USER:
        partes += [f"UID={config.DB_USER}", f"PWD={config.DB_PASSWORD}"]
    else:
        partes.append("Trusted_Connection=yes")   # autenticación de Windows
    return ";".join(partes)


def conectar(database: str | None = None, autocommit: bool = False):
    # autocommit=False: los cambios NO se guardan hasta llamar conn.commit().
    return pyodbc.connect(cadena_conexion(database), autocommit=autocommit, timeout=5)


@contextmanager
def transaccion():
    """
    Uso:
        with db.transaccion() as cur:
            cur.execute("INSERT ...")
            cur.execute("UPDATE ...")

    Si TODO lo de adentro termina bien -> COMMIT (se guarda todo junto).
    Si algo lanza una excepción (incluida una ErrorNegocio) -> ROLLBACK
    (no se guarda NADA). Así nunca queda, por ejemplo, un paciente creado
    sin su registro de ingreso, o una cama marcada ocupada sin internación.
    """
    conn = conectar()
    try:
        cur = conn.cursor()
        yield cur              # acá se ejecuta el bloque "with"
        conn.commit()
    except Exception:
        conn.rollback()
        raise                  # la excepción sigue su camino hasta app.py
    finally:
        conn.close()


def consultar_crudo(sql: str, params: tuple = ()):
    """
    Ejecuta un SELECT y devuelve (descripcion, filas) sin transformar.
    `descripcion` trae el nombre y el tipo de Python de cada columna;
    core/spark.py la usa para armar el esquema del DataFrame.
    """
    conn = conectar()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.description, cur.fetchall()
    finally:
        conn.close()


def _a_json(valor):
    """Convierte tipos de SQL Server que JSON no entiende (Decimal, fechas) a texto/número."""
    if isinstance(valor, decimal.Decimal):
        return float(valor)
    if isinstance(valor, datetime.datetime):
        return valor.strftime("%Y-%m-%d %H:%M")
    if isinstance(valor, datetime.time):
        return valor.strftime("%H:%M")
    if isinstance(valor, datetime.date):
        return valor.isoformat()
    return valor


def consultar(sql: str, params: tuple = ()) -> list[dict]:
    """SELECT -> lista de diccionarios {columna: valor}, lista para mandar al navegador."""
    descripcion, filas = consultar_crudo(sql, params)
    columnas = [c[0] for c in descripcion]
    return [{col: _a_json(v) for col, v in zip(columnas, fila)} for fila in filas]


def consultar_uno(sql: str, params: tuple = ()) -> dict | None:
    """Igual que consultar(), pero devuelve solo la primera fila (o None)."""
    filas = consultar(sql, params)
    return filas[0] if filas else None


def escalar(sql: str, params: tuple = ()):
    """Devuelve un único valor: útil para SELECT COUNT(*) ... o SELECT SUM(...)."""
    descripcion, filas = consultar_crudo(sql, params)
    return filas[0][0] if filas else None
