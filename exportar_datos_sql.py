"""
exportar_datos_sql.py — Exporta los datos actuales de hospitalProyecto a un script SQL.

Genera sql/03_datos_prueba.sql con un INSERT por tabla (en el orden que exigen las
claves foráneas), para que cualquiera pueda reconstruir la base SOLO con SQL Server
Management Studio, sin Python:

    1) sql/01_esquema.sql        tablas
    2) sql/02_indices_vistas.sql índices, vistas y procedimiento
    3) sql/03_datos_prueba.sql   datos (este archivo generado)

Uso (con la base ya cargada, por ejemplo después de setup_db.py):
    python exportar_datos_sql.py
"""
import datetime as dt
import decimal
from pathlib import Path

from core import config, db

# Orden de inserción: primero las tablas de las que dependen las demás (por las FK).
TABLAS = [
    "puesto", "especialidad", "turno", "sala", "empleado", "asignacionSala",
    "paciente", "registroIngreso", "cama", "asignacionCama", "informeCita",
    "ubicacion", "proveedor", "suministro", "informeSuministroIngreso",
    "suministroUbicacion", "entradaSuministro", "informeSuministroSalida",
]
# Tablas con columna IDENTITY (autoincremental): para insertar el ID a mano hay que
# activar IDENTITY_INSERT mientras dura el INSERT.
CON_IDENTITY = {"entradaSuministro", "informeSuministroSalida"}
FILAS_POR_INSERT = 500          # SQL Server acepta hasta 1000 filas por INSERT ... VALUES


def literal(valor) -> str:
    """Convierte un valor de Python en un literal de T-SQL."""
    if valor is None:
        return "NULL"
    if isinstance(valor, bool):
        return "1" if valor else "0"
    if isinstance(valor, (int, float, decimal.Decimal)):
        return str(valor)
    if isinstance(valor, dt.datetime):
        return f"'{valor.strftime('%Y-%m-%dT%H:%M:%S')}'"     # ISO 8601: no depende del idioma del servidor
    if isinstance(valor, dt.date):
        return f"'{valor.strftime('%Y%m%d')}'"               # yyyymmdd: tampoco depende del idioma
    if isinstance(valor, dt.time):
        return f"'{valor.strftime('%H:%M:%S')}'"
    return "N'" + str(valor).replace("'", "''") + "'"        # N'' = texto Unicode (tildes, ñ)


def exportar(destino: Path) -> dict:
    lineas = [
        "/* ==========================================================================",
        "   HOSPITAL \"W\" 2.0 — Datos de prueba",
        f"   Generado con exportar_datos_sql.py el {dt.datetime.now():%d/%m/%Y %H:%M}.",
        "   Ejecutar DESPUÉS de 01_esquema.sql y 02_indices_vistas.sql, sobre una base vacía.",
        "   Los logs de telemetría NO están aquí: viven en bigdata/datos/ (no van a la base).",
        "   ========================================================================== */",
        f"USE [{config.DB_NAME}];",
        "SET NOCOUNT ON;",
        "GO",
        "",
    ]
    conteo = {}
    for tabla in TABLAS:
        descripcion, filas = db.consultar_crudo(f"SELECT * FROM {tabla}")
        columnas = ", ".join(c[0] for c in descripcion)
        conteo[tabla] = len(filas)
        lineas.append(f"-- {tabla}: {len(filas)} filas")
        if not filas:
            continue
        if tabla in CON_IDENTITY:
            lineas.append(f"SET IDENTITY_INSERT {tabla} ON;")
        for i in range(0, len(filas), FILAS_POR_INSERT):
            bloque = filas[i:i + FILAS_POR_INSERT]
            valores = ",\n".join("    (" + ", ".join(literal(v) for v in fila) + ")" for fila in bloque)
            lineas.append(f"INSERT INTO {tabla} ({columnas}) VALUES\n{valores};")
        if tabla in CON_IDENTITY:
            lineas.append(f"SET IDENTITY_INSERT {tabla} OFF;")
        lineas.append("GO")
        lineas.append("")
    destino.write_text("\n".join(lineas), encoding="utf-8-sig")   # con BOM: SSMS respeta las tildes
    return conteo


if __name__ == "__main__":
    salida = config.BASE_DIR / "sql" / "03_datos_prueba.sql"
    resultado = exportar(salida)
    print(f"{sum(resultado.values())} filas exportadas a {salida}")
