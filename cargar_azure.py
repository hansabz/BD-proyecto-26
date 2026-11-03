"""
cargar_azure.py — Copia la base local (hospitalProyecto de tu SQL Express) a Azure SQL.

    .\\.venv\\Scripts\\python cargar_azure.py

Pide el servidor, el usuario y la contraseña de Azure (la contraseña no se
muestra al escribirla ni se guarda en ningún archivo). Luego:
  1. crea las tablas, índices y vistas en Azure (sql/01 y sql/02);
  2. copia todas las filas de tu base local, tabla por tabla, en orden de FK.
La base de Azure tiene que existir y estar VACÍA (recién creada en el portal).
"""
import getpass
import re
import sys

import pyodbc

from core import config, db
from exportar_datos_sql import CON_IDENTITY, TABLAS


def conectar_azure(servidor: str, base: str, usuario: str, clave: str):
    cadena = ";".join([
        f"DRIVER={{{db._driver()}}}", f"SERVER=tcp:{servidor},1433", f"DATABASE={base}",
        f"UID={usuario}", f"PWD={clave}", "Encrypt=yes", "TrustServerCertificate=no",
    ])
    # timeout alto: la base gratis de Azure se pausa y tarda en despertar (~1 min).
    return pyodbc.connect(cadena, timeout=90)


def ejecutar_script(cur, nombre: str) -> None:
    texto = (config.BASE_DIR / "sql" / nombre).read_text(encoding="utf-8")
    for lote in re.split(r"^\s*GO\s*$", texto, flags=re.M | re.I):
        if lote.strip():
            cur.execute(lote)


def main():
    print("Copia de la base local a Azure SQL\n")
    servidor = input("Servidor de Azure (ej. hospitalw.database.windows.net): ").strip()
    base = input("Nombre de la base en Azure [hospitalProyecto]: ").strip() or "hospitalProyecto"
    usuario = input("Usuario administrador de Azure: ").strip()
    clave = getpass.getpass("Contraseña (no se ve al escribir): ")

    print("\nConectando a Azure (puede tardar ~1 minuto si la base estaba pausada)...")
    azure = conectar_azure(servidor, base, usuario, clave)
    cur = azure.cursor()
    if cur.execute("SELECT COUNT(*) FROM sys.tables").fetchone()[0]:
        sys.exit("La base de Azure ya tiene tablas: este script es para una base recién creada y vacía.")

    print("Creando tablas, índices y vistas...")
    ejecutar_script(cur, "01_esquema.sql")
    ejecutar_script(cur, "02_indices_vistas.sql")
    azure.commit()

    print("Copiando datos desde tu SQL Server local...")
    for tabla in TABLAS:
        descripcion, filas = db.consultar_crudo(f"SELECT * FROM {tabla}")
        if not filas:
            continue
        columnas = ", ".join(c[0] for c in descripcion)
        marcas = ", ".join("?" for _ in descripcion)
        if tabla in CON_IDENTITY:       # conservar noEntrada / noSalida originales
            cur.execute(f"SET IDENTITY_INSERT {tabla} ON")
        cur.fast_executemany = True
        cur.executemany(f"INSERT INTO {tabla} ({columnas}) VALUES ({marcas})", [tuple(f) for f in filas])
        if tabla in CON_IDENTITY:
            cur.execute(f"SET IDENTITY_INSERT {tabla} OFF")
        print(f"  {tabla}: {len(filas)} filas")
    azure.commit()
    azure.close()
    print("\nListo. La base de Azure tiene una copia de tu base local.")


if __name__ == "__main__":
    main()
