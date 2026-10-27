"""
pruebas_sistema.py — Pruebas automáticas de todo el sistema.

Crea una base DESECHABLE (hospitalProyecto_pruebas) con setup_db.py, prueba la
base de datos, las reglas de negocio de RF1–RF4, las 3 queries SQL, la Fase II
de Big Data con PySpark y las pantallas, y al final borra esa base.
La base real (hospitalProyecto) no se toca.

    python pruebas_sistema.py

Tarda unos 3 minutos (la mayor parte es el arranque de Spark).
"""
import datetime as dt
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

BASE_PRUEBAS = "hospitalProyecto_pruebas"
os.environ["HOSPITAL_DB_NAME"] = BASE_PRUEBAS          # debe definirse ANTES de importar core
RAIZ = Path(__file__).resolve().parent
sys.path.insert(0, str(RAIZ))

resultados = []                                       # (grupo, nombre, ok, detalle)


def prueba(grupo, nombre):
    """Decorador: ejecuta la función y registra si pasó o falló (con el motivo)."""
    def envolver(fn):
        try:
            detalle = fn()
            resultados.append((grupo, nombre, True, detalle or ""))
        except AssertionError as exc:
            resultados.append((grupo, nombre, False, str(exc)))
        except Exception as exc:  # noqa: BLE001
            resultados.append((grupo, nombre, False, f"{type(exc).__name__}: {exc}"))
            traceback.print_exc()
        return fn
    return envolver


def main():
    # ── Preparación: base desechable + respaldo de los archivos de Big Data ──
    datos = RAIZ / "bigdata" / "datos"
    respaldo = Path(tempfile.mkdtemp())
    for nombre in ("logs_monitoreo_pacientes.csv",):
        if (datos / nombre).exists():
            shutil.copy2(datos / nombre, respaldo / nombre)
    parquet_existia = (datos / "logs_refinados.parquet").exists()
    if parquet_existia:
        shutil.copytree(datos / "logs_refinados.parquet", respaldo / "parquet")

    print(f"Creando la base de pruebas {BASE_PRUEBAS} (setup_db.py)...")
    subprocess.run([sys.executable, str(RAIZ / "setup_db.py"), "--reset"], check=True,
                   env={**os.environ, "PYTHONIOENCODING": "utf-8"}, stdout=subprocess.DEVNULL)

    from app import app
    from core import db
    cliente = app.test_client()
    try:
        ejecutar(cliente, db)
    finally:
        # ── Limpieza: restaurar archivos y borrar la base de pruebas ──
        if (respaldo / "logs_monitoreo_pacientes.csv").exists():
            shutil.copy2(respaldo / "logs_monitoreo_pacientes.csv", datos / "logs_monitoreo_pacientes.csv")
        if (datos / "logs_refinados.parquet").exists():
            shutil.rmtree(datos / "logs_refinados.parquet")
        if parquet_existia:
            shutil.copytree(respaldo / "parquet", datos / "logs_refinados.parquet")
        conn = db.conectar("master", autocommit=True)
        conn.execute(f"ALTER DATABASE [{BASE_PRUEBAS}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        conn.execute(f"DROP DATABASE [{BASE_PRUEBAS}]")
        conn.close()
    informe()


def ejecutar(c, db):
    from pyodbc import IntegrityError

    def falla_en_sql(sql, *params):
        """True si SQL Server rechaza la sentencia (por CHECK, FK, UNIQUE...)."""
        try:
            with db.transaccion() as cur:
                cur.execute(sql, *params)
            return False
        except IntegrityError:
            return True

    def post(url, datos=None):
        r = c.post(url, json=datos or {})
        return r.status_code, r.get_json()

    # ═══ 1. BASE DE DATOS ═══════════════════════════════════════════════
    G = "1. Base de datos"

    @prueba(G, "Las 18 tablas existen")
    def _():
        n = db.escalar("SELECT COUNT(*) FROM sys.tables")
        assert n == 18, f"hay {n} tablas"

    @prueba(G, "Cada tabla tiene al menos un CHECK y un DEFAULT")
    def _():
        faltan = db.consultar("""SELECT t.name FROM sys.tables t
            WHERE NOT EXISTS (SELECT 1 FROM sys.check_constraints c WHERE c.parent_object_id = t.object_id)
               OR NOT EXISTS (SELECT 1 FROM sys.default_constraints d WHERE d.parent_object_id = t.object_id)""")
        assert not faltan, f"sin CHECK o DEFAULT: {[f['name'] for f in faltan]}"

    @prueba(G, "Un CHECK rechaza datos inválidos (código de puesto de 2 letras)")
    def _():
        assert falla_en_sql("INSERT INTO puesto (codigoPuesto, nombre, salario) VALUES ('ME', 'X', 100)")

    @prueba(G, "Un DEFAULT completa el valor que no se manda (sala sin camas -> 0)")
    def _():
        with db.transaccion() as cur:
            cur.execute("INSERT INTO sala (codigoSala, nombre) VALUES ('S99', 'Sala de prueba')")
        assert db.escalar("SELECT camasDisponibles FROM sala WHERE codigoSala = 'S99'") == 0
        with db.transaccion() as cur:
            cur.execute("DELETE FROM sala WHERE codigoSala = 'S99'")

    @prueba(G, "Las vistas y el procedimiento de las queries existen")
    def _():
        objetos = {f["name"] for f in db.consultar("SELECT name FROM sys.objects WHERE type IN ('V', 'P')")}
        for nombre in ("VW_Asignacion_Personal", "SP_HistorialCitas", "VW_Stock_Critico", "VW_Registro_Camas"):
            assert nombre in objetos, f"falta {nombre}"

    @prueba(G, "Índices agrupados de alta concurrencia creados")
    def _():
        nombres = {f["name"] for f in db.consultar("SELECT name FROM sys.indexes WHERE type_desc = 'CLUSTERED'")}
        for idx in ("CIX_informeCita_paciente_fecha", "CIX_entradaSuministro_fecha", "CIX_informeSuministroSalida_fecha"):
            assert idx in nombres, f"falta {idx}"

    # ═══ 2. QUERIES SQL ═════════════════════════════════════════════════
    G = "2. Queries SQL (Fase I)"

    @prueba(G, "Query 1: VW_Asignacion_Personal con las columnas de la Figura 1")
    def _():
        filas = db.consultar("SELECT * FROM VW_Asignacion_Personal WHERE codigoSala = 'S01'")
        assert filas, "sin filas"
        for col in ("sala", "camasDisponibles", "numeroEmpleado", "nombreEmpleado", "NSS", "puesto",
                    "especialidad", "turno", "salarioBase"):
            assert col in filas[0], f"falta la columna {col}"
        return f"{len(filas)} empleados en S01"

    @prueba(G, "Query 2: historial de citas en orden cronológico")
    def _():
        citas = db.consultar("EXEC SP_HistorialCitas @ID_Paciente = 'PAC-0001'")
        assert len(citas) >= 4, f"solo {len(citas)} citas"
        claves = [(x["fecha"], x["hora"]) for x in citas]
        assert claves == sorted(claves), "no están en orden"
        return f"{len(citas)} citas"

    @prueba(G, "Query 3: solo insumos bajo el 15 % de cantidadIdeal, con contacto del proveedor")
    def _():
        filas = db.consultar("SELECT * FROM VW_Stock_Critico")
        assert filas, "sin insumos críticos en los datos de prueba"
        for f in filas:
            assert f["existencia"] < 0.15 * f["cantidadIdeal"], f"{f['codigoSuministro']} no es crítico"
            assert f["telefono"], "falta el teléfono del proveedor"
        return f"{len(filas)} críticos"

    # ═══ 3. RF1 PERSONAL ════════════════════════════════════════════════
    G = "3. RF1 Personal"
    base = {"nombres": "Prueba", "apellidos": "Sistema", "codigoEspecialidad": "ESP01", "codigoTurno": "MAT",
            "fechaContratacion": "2020-01-15", "salas": ["S01"]}

    @prueba(G, "ID por rol con PySpark + match: médico M, enfermera jefe E, auxiliar A")
    def _():
        ids = []
        for puesto, nss, letra in (("MED", "100001-A", "M"), ("EJF", "100002-B", "E"), ("AUX", "100003-C", "A")):
            s, r = post("/api/personal", {**base, "codigoPuesto": puesto, "NSS": nss})
            assert s == 201, r
            assert r["numeroEmpleado"].startswith(letra), r["numeroEmpleado"]
            ids.append(r["numeroEmpleado"])
        return ", ".join(ids)

    @prueba(G, "NSS duplicado rechazado")
    def _():
        s, r = post("/api/personal", {**base, "codigoPuesto": "MED", "NSS": "883920-K"})
        assert s == 400 and "NSS" in r.get("campos", {}), r

    @prueba(G, "NSS con formato inválido rechazado")
    def _():
        s, r = post("/api/personal", {**base, "codigoPuesto": "MED", "NSS": "12345"})
        assert s == 400 and "NSS" in r.get("campos", {}), r

    @prueba(G, "Especialidad o turno fuera del catálogo rechazados")
    def _():
        s, r = post("/api/personal", {**base, "codigoPuesto": "MED", "NSS": "100004-D", "codigoTurno": "XXX"})
        assert s == 400 and "codigoTurno" in r.get("campos", {}), r

    @prueba(G, "Baja lógica: el empleado queda inactivo pero no se borra; se puede reactivar")
    def _():
        s, r = post("/api/personal/empleado/M1001/baja")
        assert s == 200, r
        fila = db.consultar_uno("SELECT activo, fechaBaja FROM empleado WHERE numeroEmpleado = 'M1001'")
        assert fila and fila["activo"] is False and fila["fechaBaja"], fila
        s, r = post("/api/personal/empleado/M1001/reactivar")
        assert s == 200 and db.escalar("SELECT activo FROM empleado WHERE numeroEmpleado = 'M1001'") is True

    @prueba(G, "Editar conserva la fecha de asignación de las salas que siguen")
    def _():
        antes = db.escalar("SELECT fechaAsignacion FROM asignacionSala WHERE numeroEmpleado = 'M1001' AND codigoSala = 'S01'")
        e = c.get("/api/personal/empleado/M1001").get_json()
        datos = {**{k: e[k] for k in ("nombres", "apellidos", "NSS", "codigoPuesto", "codigoEspecialidad",
                                       "codigoTurno", "fechaContratacion")}, "salas": ["S01", "S02"]}
        r = c.put("/api/personal/empleado/M1001", json=datos)
        assert r.status_code == 200, r.get_json()
        despues = db.escalar("SELECT fechaAsignacion FROM asignacionSala WHERE numeroEmpleado = 'M1001' AND codigoSala = 'S01'")
        assert antes == despues, f"{antes} -> {despues}"

    # ═══ 4. RF2 PACIENTES ═══════════════════════════════════════════════
    G = "4. RF2 Admisión"
    ficha = {"nombres": "Ana", "apellidos": "Prueba", "fechaNac": "1948-03-02", "sexo": "F", "telefono": "+502 5555-0000",
             "direccion": "Zona 1", "nombreFam": "Luis Prueba", "parentescoFam": "Hijo", "telefonoFam": "+502 4444-0000",
             "medicoRef": "Dr. X", "clinicaRef": "Clínica Y"}
    nuevo = {}

    @prueba(G, "Admisión crea PAC-#### e ING-### con fecha de registro automática")
    def _():
        s, r = post("/api/pacientes", ficha)
        assert s == 201, r
        nuevo.update(r)
        assert r["numeroPaciente"].startswith("PAC-") and r["numeroIngreso"].startswith("ING-")
        fecha = db.escalar("SELECT fechaReg FROM registroIngreso WHERE numeroIngreso = ?", (r["numeroIngreso"],))
        assert abs((dt.datetime.now() - fecha).total_seconds()) < 120, f"fechaReg = {fecha}"
        return f"{r['numeroPaciente']} / {r['numeroIngreso']}"

    @prueba(G, "Sin familiar responsable o sin médico que refiere: rechazado")
    def _():
        s, r = post("/api/pacientes", {**ficha, "nombreFam": "", "medicoRef": ""})
        assert s == 400 and {"nombreFam", "medicoRef"} <= set(r.get("campos", {})), r

    @prueba(G, "Cita fuera del horario (CHECK 07:00–19:00) rechazada")
    def _():
        s, r = post("/api/pacientes/expediente/PAC-0001/citas",
                    {"numeroEmpleado": "M1001", "codigoSala": "S01", "fecha": "2026-12-01", "hora": "22:00"})
        assert s == 400 and "hora" in r.get("campos", {}), r

    @prueba(G, "Cita válida aparece en el historial (Query 2)")
    def _():
        s, r = post("/api/pacientes/expediente/PAC-0001/citas",
                    {"numeroEmpleado": "M1001", "codigoSala": "S01", "fecha": "2026-12-01", "hora": "10:00"})
        assert s == 201, r
        exp = c.get("/api/pacientes/expediente/PAC-0001").get_json()
        assert any(x["fecha"] == "2026-12-01" for x in exp["citas"])

    # ═══ 5. RF3 HOSPITALIZACIÓN ═════════════════════════════════════════
    G = "5. RF3 Hospitalización"

    @prueba(G, "No permite asignar una cama ocupada")
    def _():
        camas = c.get("/api/hospitalizacion/camas").get_json()["camas"]
        ocupada = next(x for x in camas if x["ocupada"])
        s, r = post("/api/hospitalizacion/asignar", {"numeroIngreso": nuevo["numeroIngreso"],
                                                      "codigoSala": ocupada["codigoSala"], "numeroCama": ocupada["numeroCama"]})
        assert s == 400 and "ocupada" in r["error"], r

    @prueba(G, "El índice único de SQL Server también lo impide (aunque se salte la app)")
    def _():
        fila = db.consultar_uno("SELECT TOP 1 numeroCama, codigoSala FROM asignacionCama WHERE codigoEstado = 1")
        assert falla_en_sql("INSERT INTO asignacionCama (numeroIngreso, numeroPaciente, numeroCama, codigoSala)"
                            " VALUES (?, ?, ?, ?)", nuevo["numeroIngreso"], nuevo["numeroPaciente"],
                            fila["numeroCama"], fila["codigoSala"])

    @prueba(G, "Interna en cama libre con fecha de ingreso automática; el alta registra la salida")
    def _():
        camas = c.get("/api/hospitalizacion/camas").get_json()["camas"]
        libre = next(x for x in camas if not x["ocupada"])
        s, r = post("/api/hospitalizacion/asignar", {"numeroIngreso": nuevo["numeroIngreso"],
                                                      "codigoSala": libre["codigoSala"], "numeroCama": libre["numeroCama"]})
        assert s == 201, r
        f = db.consultar_uno("SELECT fechaIngreso, codigoEstado FROM asignacionCama WHERE numeroIngreso = ?",
                             (nuevo["numeroIngreso"],))
        assert f["codigoEstado"] is True and f["fechaIngreso"]
        s, r = post(f"/api/hospitalizacion/ingreso/{nuevo['numeroIngreso']}/alta")
        assert s == 200, r
        f = db.consultar_uno("SELECT fechaSalida, codigoEstado FROM asignacionCama WHERE numeroIngreso = ?",
                             (nuevo["numeroIngreso"],))
        assert f["codigoEstado"] is False and f["fechaSalida"], f
        estado = db.escalar("SELECT codigoEstado FROM cama WHERE numeroCama = ? AND codigoSala = ?",
                            (libre["numeroCama"], libre["codigoSala"]))
        assert estado is False, "la cama no quedó libre"

    # ═══ 6. RF4 INVENTARIO ══════════════════════════════════════════════
    G = "6. RF4 Inventario"
    insumo = {}

    @prueba(G, "Insumo sin proveedor rechazado")
    def _():
        s, r = post("/api/inventario/suministros", {"nombre": "X", "tipo": "F", "costoUnitario": "5", "codigoProveedor": "",
                                                   "codigoUbicacion": "U01", "existencia": "1", "cantidadIdeal": "10"})
        assert s == 400 and "codigoProveedor" in r.get("campos", {}), r

    @prueba(G, "Insumo nuevo (ID F### por tipo) en stock crítico aparece en la Query 3")
    def _():
        s, r = post("/api/inventario/suministros", {"nombre": "Prueba", "tipo": "F", "costoUnitario": "5", "codigoProveedor": "PR003",
                                                   "codigoUbicacion": "U01", "existencia": "10", "cantidadIdeal": "100"})
        assert s == 201 and r["codigoSuministro"].startswith("F"), r
        insumo["cod"] = r["codigoSuministro"]
        assert any(x["codigoSuministro"] == insumo["cod"] for x in c.get("/api/inventario/critico").get_json())
        return insumo["cod"]

    @prueba(G, "Salida sin empleado destinatario rechazada")
    def _():
        s, r = post("/api/inventario/movimientos", {"tipo": "S", "codigoSuministro": insumo["cod"], "codigoUbicacion": "U01", "cantidad": 1})
        assert s == 400 and "empleadoDestinatario" in r.get("campos", {}), r

    @prueba(G, "Salida mayor al stock rechazada")
    def _():
        s, r = post("/api/inventario/movimientos", {"tipo": "S", "codigoSuministro": insumo["cod"], "codigoUbicacion": "U01",
                                                    "cantidad": 999, "empleadoDestinatario": "A1001"})
        assert s == 400 and "cantidad" in r.get("campos", {}), r

    @prueba(G, "Entrada suma stock y cantidadOrdenada; sale de la alerta al pasar el 15 %")
    def _():
        s, r = post("/api/inventario/movimientos", {"tipo": "E", "codigoSuministro": insumo["cod"], "codigoUbicacion": "U01", "cantidad": 40})
        assert s == 201, r
        f = db.consultar_uno("""SELECT sb.cantidadDisponible, ig.cantidadOrdenada FROM suministroUbicacion sb
                                JOIN informeSuministroIngreso ig ON ig.codigoUbicacion = sb.codigoUbicacion
                                 AND ig.codigoSuministro = sb.codigoSuministro
                                WHERE sb.codigoUbicacion = 'U01' AND sb.codigoSuministro = ?""", (insumo["cod"],))
        assert f["cantidadDisponible"] == 50 and f["cantidadOrdenada"] == 50, f
        assert not any(x["codigoSuministro"] == insumo["cod"] for x in c.get("/api/inventario/critico").get_json())

    @prueba(G, "Salida a un empleado y traslado entre almacenes")
    def _():
        s, r = post("/api/inventario/movimientos", {"tipo": "S", "codigoSuministro": insumo["cod"], "codigoUbicacion": "U01",
                                                    "cantidad": 5, "empleadoDestinatario": "A1001"})
        assert s == 201, r
        s, r = post("/api/inventario/movimientos", {"tipo": "T", "codigoSuministro": insumo["cod"], "codigoUbicacion": "U01",
                                                    "codigoUbicacionDestino": "U02", "cantidad": 5, "cantidadIdeal": 20})
        assert s == 201, r
        stock = {f["codigoUbicacion"]: f["cantidadDisponible"] for f in db.consultar(
            "SELECT codigoUbicacion, cantidadDisponible FROM suministroUbicacion WHERE codigoSuministro = ?", (insumo["cod"],))}
        assert stock == {"U01": 40, "U02": 5}, stock

    # ═══ 7. PANTALLAS, FILTROS Y GRÁFICAS ═══════════════════════════════
    G = "7. Pantallas y gráficas"
    from areas import AREAS

    @prueba(G, "Las 6 páginas cargan")
    def _():
        for url in ("/", "/personal", "/pacientes", "/hospitalizacion", "/inventario", "/telemetria"):
            assert c.get(url).status_code == 200, url

    @prueba(G, "Filtrar en PySpark (personal por puesto Médico)")
    def _():
        s, r = post("/api/personal/filtrar", {"filtros": {"codigoPuesto": "MED"}})
        assert s == 200 and r["total"] > 0 and all(x["codigoPuesto"] == "MED" for x in r["registros"]), r.get("error")
        return f"{r['total']} médicos"

    @prueba(G, "Las 26 gráficas (pandas + matplotlib) se generan")
    def _():
        malas = [f"{a}/{g}" for a, m in AREAS.items() for g in m.GRAFICOS
                 if c.get(f"/api/{a}/grafico/{g}").mimetype != "image/png"]
        assert not malas, f"fallan: {malas}"

    @prueba(G, "Tiempo real: la firma cambia cuando cambia un dato")
    def _():
        antes = c.get("/api/pacientes/firma").get_json()["firma"]
        with db.transaccion() as cur:
            cur.execute("UPDATE paciente SET direccion = 'Cambio de prueba' WHERE numeroPaciente = 'PAC-0001'")
        assert c.get("/api/pacientes/firma").get_json()["firma"] != antes

    # ═══ 8. BIG DATA (FASE II) ══════════════════════════════════════════
    G = "8. Big Data / PySpark"

    @prueba(G, "Ingesta masiva: menos de 5,000 registros rechazado")
    def _():
        s, r = post("/api/telemetria/generar", {"registros": 4000})
        assert s == 400, r

    @prueba(G, "Ingesta masiva: CSV con el esquema de la Figura 6 y ≥ 5,000 filas")
    def _():
        s, r = post("/api/telemetria/generar", {"registros": 6000})
        assert s == 200, r
        ruta = RAIZ / "bigdata" / "datos" / "logs_monitoreo_pacientes.csv"
        with ruta.open(encoding="utf-8") as f:
            encabezado = f.readline().strip()
            filas = sum(1 for _ in f)
        assert encabezado == "ID_Registro,ID_Paciente,Timestamp,Frecuencia_Cardiaca,Saturacion_Oxigeno,Alerta_Generada"
        assert filas >= 5000, filas
        return f"{filas} lecturas"

    @prueba(G, "Query BD 1: la suma de los motivos = total de descartados")
    def _():
        r = c.get("/api/telemetria/bd1").get_json()
        assert sum(m["registros"] for m in r["por_motivo"]) == r["descartados"] > 0, r
        assert r["leidos"] == r["validos"] + r["descartados"]
        return f"{r['validos']} válidos, {r['descartados']} descartados"

    @prueba(G, "Query BD 2: top 5 con Spark SQL, ordenado de mayor a menor")
    def _():
        filas = c.get("/api/telemetria/bd2").get_json()["filas"]
        assert 0 < len(filas) <= 5
        valores = [f["alertas_criticas"] for f in filas]
        assert valores == sorted(valores, reverse=True), valores

    @prueba(G, "Query BD 3: UDF clasifica en Bajo / Moderado / Crítico")
    def _():
        dist = c.get("/api/telemetria/bd3").get_json()["distribucion"]
        assert set(dist) <= {"Bajo", "Moderado", "Crítico"} and sum(dist.values()) > 0, dist
        return str(dist)

    @prueba(G, "El nivel de riesgo llega a asignacionCama.nivelRiesgo")
    def _():
        s, r = post("/api/telemetria/riesgo")
        assert s == 200 and r["internaciones"], r
        sin = db.escalar("SELECT COUNT(*) FROM asignacionCama WHERE codigoEstado = 1 AND nivelRiesgo IS NULL")
        assert sin == 0, f"{sin} internados sin nivel de riesgo"

    @prueba(G, "Los logs NO se guardan en la base de datos")
    def _():
        assert not db.escalar("SELECT COUNT(*) FROM sys.columns WHERE name IN ('ID_Registro', 'Frecuencia_Cardiaca')")

    @prueba(G, "Exportación a Parquet y relectura con el mismo número de registros")
    def _():
        s, r = post("/api/telemetria/parquet")
        assert s == 200, r
        assert r["verificacion"]["registros"] == r["limpieza"]["validos"], r
        return r["metodo"]

    @prueba(G, "Limpiar datos solo existe en Telemetría")
    def _():
        assert c.post("/api/personal/limpiar").status_code == 400
        assert c.post("/api/telemetria/limpiar").status_code == 200


def informe():
    print()
    grupo_actual = None
    for grupo, nombre, ok, detalle in resultados:
        if grupo != grupo_actual:
            print(f"\n{grupo}")
            grupo_actual = grupo
        marca = "  OK   " if ok else "  FALLA"
        extra = f"  ({detalle})" if detalle else ""
        print(f"{marca} {nombre}{extra}")
    aprobadas = sum(1 for r in resultados if r[2])
    print(f"\n{aprobadas} de {len(resultados)} pruebas aprobadas.")
    sys.exit(0 if aprobadas == len(resultados) else 1)


if __name__ == "__main__":
    main()
