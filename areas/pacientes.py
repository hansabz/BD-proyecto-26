"""
areas/pacientes.py — RF2: Admisión y control de pacientes (Figura 2) + Query 2 (Figura 4).
La admisión crea el paciente y su registro de ingreso en una sola
transacción: datos demográficos, familiar responsable y médico externo.
"""
import datetime as dt
import re

from pyspark.sql import functions as F

from core import db, graficos, id_generators
from core.db import ErrorNegocio
from core.spark import leer_sql

TITULO = "Admisión y control de pacientes"
TABLAS = ["paciente", "registroIngreso", "asignacionCama", "informeCita"]

# SELECT base: cada paciente con
#   • su edad exacta (DATEDIFF de años, menos 1 si todavía no cumplió este año),
#   • los datos de su ÚLTIMO ingreso (OUTER APPLY ... TOP 1 ... ORDER BY fecha DESC),
#   • su estado calculado: Internado / En espera de cama / Alta / Sin ingreso.
BASE_SQL = """
SELECT p.numeroPaciente, p.nombres, p.apellidos, p.fechaNac,
       DATEDIFF(year, p.fechaNac, GETDATE())
         - CASE WHEN DATEADD(year, DATEDIFF(year, p.fechaNac, GETDATE()), p.fechaNac) > GETDATE() THEN 1 ELSE 0 END
         AS edad,
       p.sexo, p.telefono, p.direccion,
       r.numeroIngreso AS ultimoIngreso, r.fechaReg, r.nombreFam, r.parentescoFam, r.telefonoFam,
       r.medicoRef, r.clinicaRef,
       (SELECT COUNT(*) FROM registroIngreso x WHERE x.numeroPaciente = p.numeroPaciente) AS ingresos,
       CASE
         WHEN EXISTS (SELECT 1 FROM asignacionCama a WHERE a.numeroPaciente = p.numeroPaciente AND a.codigoEstado = 1)
              THEN 'Internado'
         WHEN r.numeroIngreso IS NULL THEN 'Sin ingreso'
         WHEN NOT EXISTS (SELECT 1 FROM asignacionCama a WHERE a.numeroIngreso = r.numeroIngreso)
              THEN 'En espera de cama'
         ELSE 'Alta'
       END AS estado
FROM paciente p
OUTER APPLY (SELECT TOP 1 * FROM registroIngreso ri
             WHERE ri.numeroPaciente = p.numeroPaciente ORDER BY ri.fechaReg DESC) r
"""

COLUMNAS = [
    {"campo": "numeroPaciente", "etiqueta": "No. Paciente", "tipo": "id"},
    {"campo": "nombres", "etiqueta": "Nombres"},
    {"campo": "apellidos", "etiqueta": "Apellidos"},
    {"campo": "edad", "etiqueta": "Edad", "tipo": "numero"},
    {"campo": "sexo", "etiqueta": "Sexo"},
    {"campo": "telefono", "etiqueta": "Teléfono", "tipo": "mono"},
    {"campo": "nombreFam", "etiqueta": "Familiar responsable", "extra": "parentescoFam"},
    {"campo": "medicoRef", "etiqueta": "Médico que refiere", "extra": "clinicaRef"},
    {"campo": "fechaReg", "etiqueta": "Último ingreso", "tipo": "fecha"},
    {"campo": "estado", "etiqueta": "Estado", "tipo": "estado"},
]

PATRON_TELEFONO = r"^\+?[0-9][0-9 -]{6,18}$"


def tabla() -> list[dict]:
    return db.consultar(BASE_SQL + " ORDER BY p.numeroPaciente DESC")


def metricas() -> list[dict]:
    m = db.consultar_uno(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN estado = 'Internado' THEN 1 ELSE 0 END) AS internados,
               SUM(CASE WHEN estado = 'En espera de cama' THEN 1 ELSE 0 END) AS espera,
               AVG(CAST(edad AS float)) AS edad
        FROM ({BASE_SQL}) q""")
    ingresos_30 = db.escalar("SELECT COUNT(*) FROM registroIngreso WHERE fechaReg >= DATEADD(day, -30, GETDATE())")
    citas_7 = db.escalar("""SELECT COUNT(*) FROM informeCita
                            WHERE fecha BETWEEN CAST(GETDATE() AS date) AND DATEADD(day, 7, CAST(GETDATE() AS date))""")
    return [
        {"etiqueta": "Pacientes registrados", "valor": m["total"] or 0, "detalle": "expedientes"},
        {"etiqueta": "Internados ahora", "valor": m["internados"] or 0, "detalle": "con cama asignada"},
        {"etiqueta": "En espera de cama", "valor": m["espera"] or 0, "detalle": "ingresados sin cama",
         "tono": "alerta" if m["espera"] else ""},
        {"etiqueta": "Edad promedio", "valor": round(m["edad"] or 0, 1), "detalle": "años"},
        {"etiqueta": "Ingresos (30 días)", "valor": ingresos_30 or 0, "detalle": "admisiones"},
        {"etiqueta": "Citas próximos 7 días", "valor": citas_7 or 0, "detalle": "consulta externa"},
    ]


def ficha(numero: str) -> dict:
    # Lo que muestra el modal "Expediente" al hacer clic en un paciente.
    """Ficha de ingreso (Figura 2) + historial de citas (Query 2) + ingresos previos."""
    paciente = db.consultar_uno(BASE_SQL + " WHERE p.numeroPaciente = ?", (numero,))
    if not paciente:
        raise ErrorNegocio(f"No existe el paciente {numero}.")
    return {
        "paciente": paciente,
        "citas": historial_citas(numero),
        "ingresos": db.consultar("""
            SELECT r.numeroIngreso, r.fechaReg, r.medicoRef, r.clinicaRef,
                   a.codigoSala, s.nombre AS sala, a.numeroCama, a.fechaIngreso, a.fechaSalida,
                   CASE WHEN a.numeroIngreso IS NULL THEN 'En espera de cama'
                        WHEN a.codigoEstado = 1 THEN 'Internado' ELSE 'Alta Médica' END AS estado
            FROM registroIngreso r
            LEFT JOIN asignacionCama a ON a.numeroIngreso = r.numeroIngreso
            LEFT JOIN sala s ON s.codigoSala = a.codigoSala
            WHERE r.numeroPaciente = ? ORDER BY r.fechaReg DESC""", (numero,)),
    }


def historial_citas(numero: str) -> list[dict]:
    """
    Query 2 — llama al procedimiento almacenado SP_HistorialCitas (sql/02_indices_vistas.sql)
    pasándole el ID del paciente como parámetro. Devuelve las citas en orden cronológico.
    """
    return db.consultar("EXEC SP_HistorialCitas @ID_Paciente = ?", (numero,))


def catalogos() -> dict:
    return {
        "medicos": db.consultar("""SELECT e.numeroEmpleado AS id, e.nombres + ' ' + e.apellidos AS nombre,
                                          es.nombre AS especialidad, e.codigoEspecialidad,
                                          ISNULL((SELECT TOP 1 codigoSala FROM asignacionSala a
                                                  WHERE a.numeroEmpleado = e.numeroEmpleado), 'S03') AS sala
                                   FROM empleado e JOIN especialidad es ON es.codigoEspecialidad = e.codigoEspecialidad
                                   WHERE e.activo = 1 AND e.numeroEmpleado LIKE 'M%' ORDER BY e.nombres"""),
        "salas": db.consultar("SELECT codigoSala AS id, nombre FROM sala ORDER BY codigoSala"),
        "clinicas": [r["clinicaRef"] for r in db.consultar(
            "SELECT DISTINCT clinicaRef FROM registroIngreso ORDER BY clinicaRef")],
        "parentescos": ["Hija", "Hijo", "Esposa", "Esposo", "Nieta", "Nieto", "Hermana", "Hermano",
                        "Sobrina", "Sobrino", "Cuidador/a", "Otro"],
    }


# ── Validación ───────────────────────────────────────────────────────
def _texto(datos, campo):
    return " ".join((datos.get(campo) or "").split())


def _validar(datos: dict) -> dict:
    """Valida la ficha de ingreso completa (Figura 2) y junta todos los errores por campo."""
    # _texto() quita espacios de más: "  Juan   Pérez " -> "Juan Pérez"
    d = {c: _texto(datos, c) for c in ("nombres", "apellidos", "telefono", "direccion", "nombreFam",
                                         "parentescoFam", "telefonoFam", "medicoRef", "clinicaRef", "sexo")}
    errores = {}
    # Largo máximo = el tamaño de la columna varchar en SQL Server (si no, el INSERT fallaría).
    limites = {"nombres": 25, "apellidos": 25, "telefono": 20, "direccion": 50, "nombreFam": 40,
               "parentescoFam": 15, "telefonoFam": 20, "medicoRef": 30, "clinicaRef": 35}
    for campo, maximo in limites.items():
        if len(d[campo]) > maximo:
            errores[campo] = f"Máximo {maximo} caracteres."
    # RF2: obligatorios los datos del paciente, del familiar responsable y del médico que refiere.
    for campo in ("nombres", "apellidos", "nombreFam", "parentescoFam", "telefonoFam", "medicoRef", "clinicaRef"):
        if not d[campo]:
            errores[campo] = "Obligatorio."
    for campo in ("telefono", "telefonoFam"):
        if d[campo] and not re.match(PATRON_TELEFONO, d[campo]):
            errores[campo] = "Solo números, espacios, guiones y + inicial."
    if d["sexo"] not in ("M", "F"):
        errores["sexo"] = "Elegí M o F."
    try:
        d["fechaNac"] = dt.date.fromisoformat(datos.get("fechaNac") or "")
        if d["fechaNac"] > dt.date.today():
            errores["fechaNac"] = "No puede ser una fecha futura."
        elif d["fechaNac"].year < 1900:
            errores["fechaNac"] = "Fecha fuera de rango."
    except ValueError:
        errores["fechaNac"] = "Fecha inválida."
    if errores:
        raise ErrorNegocio(errores)
    for campo in ("telefono", "direccion"):
        d[campo] = d[campo] or None
    return d


# ── Admisión / modificación ──────────────────────────────────────────
def admitir(datos: dict) -> dict:
    """
    Si viene `numeroPaciente` es un reingreso de un paciente existente;
    si no, se crea el expediente nuevo. En ambos casos se genera un
    registro de ingreso con fecha y hora automáticas (GETDATE()).
    """
    existente = (datos.get("numeroPaciente") or "").strip()
    d = _validar(datos)
    with id_generators.LOCK_IDS:
        if existente:
            estado = db.consultar_uno(BASE_SQL + " WHERE p.numeroPaciente = ?", (existente,))
            if not estado:
                raise ErrorNegocio(f"No existe el paciente {existente}.")
            # No se puede abrir un segundo ingreso mientras el anterior sigue activo.
            if estado["estado"] in ("Internado", "En espera de cama"):
                raise ErrorNegocio(f"{existente} ya tiene un ingreso activo ({estado['estado'].lower()}).")
            numero = existente
        else:
            numero = id_generators.generar_id_paciente()
        ingreso = id_generators.generar_id_ingreso()
        # Paciente + registro de ingreso en UNA transacción: o se guardan los dos o ninguno.
        with db.transaccion() as cur:
            if existente:
                cur.execute("UPDATE paciente SET nombres=?, apellidos=?, fechaNac=?, sexo=?, telefono=?, direccion=?"
                            " WHERE numeroPaciente = ?",
                            d["nombres"], d["apellidos"], d["fechaNac"], d["sexo"], d["telefono"], d["direccion"],
                            numero)
            else:
                cur.execute("INSERT INTO paciente VALUES (?,?,?,?,?,?,?)",
                            numero, d["nombres"], d["apellidos"], d["fechaNac"], d["sexo"], d["telefono"],
                            d["direccion"])
            # fechaReg no se manda: la pone SQL Server con DEFAULT GETDATE() (hora real del servidor).
            cur.execute("INSERT INTO registroIngreso (numeroIngreso, numeroPaciente, nombreFam, parentescoFam,"
                        " telefonoFam, medicoRef, clinicaRef) VALUES (?,?,?,?,?,?,?)",
                        ingreso, numero, d["nombreFam"], d["parentescoFam"], d["telefonoFam"], d["medicoRef"],
                        d["clinicaRef"])
    return {"numeroPaciente": numero, "numeroIngreso": ingreso,
            "mensaje": f"{numero} admitido con el ingreso {ingreso}. Queda en espera de cama."}


def actualizar(numero: str, datos: dict) -> dict:
    d = _validar(datos)
    with db.transaccion() as cur:
        cur.execute("UPDATE paciente SET nombres=?, apellidos=?, fechaNac=?, sexo=?, telefono=?, direccion=?"
                    " WHERE numeroPaciente = ?",
                    d["nombres"], d["apellidos"], d["fechaNac"], d["sexo"], d["telefono"], d["direccion"], numero)
        if cur.rowcount == 0:
            raise ErrorNegocio(f"No existe el paciente {numero}.")
        # Los datos del familiar y la referencia viven en el último ingreso.
        cur.execute("""UPDATE registroIngreso SET nombreFam=?, parentescoFam=?, telefonoFam=?, medicoRef=?, clinicaRef=?
                       WHERE numeroIngreso = (SELECT TOP 1 numeroIngreso FROM registroIngreso
                                              WHERE numeroPaciente = ? ORDER BY fechaReg DESC)""",
                    d["nombreFam"], d["parentescoFam"], d["telefonoFam"], d["medicoRef"], d["clinicaRef"], numero)
    return {"mensaje": f"Expediente {numero} actualizado."}


def agendar_cita(numero: str, datos: dict) -> dict:
    """Agenda una cita (alimenta el historial de Query 2). Impide dos citas del mismo médico a la misma hora."""
    errores = {}
    medico = db.consultar_uno("SELECT numeroEmpleado, codigoEspecialidad FROM empleado"
                              " WHERE numeroEmpleado = ? AND activo = 1", (datos.get("numeroEmpleado") or "",))
    if not medico:
        errores["numeroEmpleado"] = "Elegí un médico activo."
    sala = datos.get("codigoSala") or ""
    if not db.escalar("SELECT COUNT(*) FROM sala WHERE codigoSala = ?", (sala,)):
        errores["codigoSala"] = "Elegí una sala."
    try:
        fecha = dt.date.fromisoformat(datos.get("fecha") or "")
        hora = dt.time.fromisoformat(datos.get("hora") or "")
    except ValueError:
        errores["fecha"] = "Fecha y hora obligatorias."
    else:
        # Misma regla que CK_cita_hora en SQL Server: horario de consulta externa.
        if not dt.time(7, 0) <= hora <= dt.time(19, 0):
            errores["hora"] = "Las citas son de 07:00 a 19:00."
    if errores:
        raise ErrorNegocio(errores)
    choque = db.consultar_uno("SELECT numeroCita FROM informeCita WHERE numeroEmpleado = ? AND fecha = ? AND hora = ?",
                              (medico["numeroEmpleado"], fecha, hora))
    if choque:
        raise ErrorNegocio({"hora": f"El médico ya tiene la cita {choque['numeroCita']} a esa hora."})
    with id_generators.LOCK_IDS:
        cita = id_generators.generar_id_cita()
        with db.transaccion() as cur:
            cur.execute("INSERT INTO informeCita VALUES (?,?,?,?,?,?,?)", cita, numero, fecha, hora,
                        medico["numeroEmpleado"], medico["codigoEspecialidad"], sala)
    return {"mensaje": f"Cita {cita} agendada."}


# ── PySpark: dataset, filtros y gráficas ─────────────────────────────
def dataset():
    return leer_sql(BASE_SQL)


def filtros() -> list[dict]:
    clinicas = catalogos()["clinicas"]
    return [
        {"id": "texto", "tipo": "texto", "etiqueta": "Buscar",
         "columnas": ["numeroPaciente", "nombres", "apellidos", "nombreFam", "medicoRef"],
         "ayuda": "ID, paciente, familiar o médico"},
        {"id": "sexo", "tipo": "opcion", "etiqueta": "Sexo", "opciones": [("F", "Femenino"), ("M", "Masculino")]},
        {"id": "estado", "tipo": "opcion", "etiqueta": "Estado",
         "opciones": [(e, e) for e in ("Internado", "En espera de cama", "Alta", "Sin ingreso")]},
        {"id": "clinicaRef", "tipo": "opcion", "etiqueta": "Clínica que refiere", "opciones": [(c, c) for c in clinicas]},
        {"id": "edad", "tipo": "rango", "etiqueta": "Edad (años)"},
        {"id": "fechaReg", "tipo": "fechas", "etiqueta": "Fecha de ingreso"},
    ]


def _ingresos_por_mes(df):
    """Spark: une los ingresos con los pacientes visibles (respeta filtros) y cuenta por mes."""
    ingresos = leer_sql("SELECT numeroPaciente, fechaReg FROM registroIngreso")
    return (ingresos.join(df.select("numeroPaciente"), "numeroPaciente")
            .groupBy(F.date_trunc("month", "fechaReg").alias("mes")).count()
            .orderBy("mes").toPandas())


GRAFICOS = {
    "edades": {"titulo": "Edades", "tipo": "Histograma",
               "fn": lambda df: graficos.histograma(df.select("edad").toPandas()["edad"], "Distribución de edades",
                                                    "años", bins=range(55, 105, 5), linea=65,
                                                    etiqueta_linea="adulto mayor (65)")},
    "sexo": {"titulo": "Sexo", "tipo": "Dona",
             "fn": lambda df: graficos.dona(
                 df.withColumn("sexo", F.when(F.col("sexo") == "F", "Femenino").otherwise("Masculino"))
                 .groupBy("sexo").count().toPandas(), "sexo", "count", "Pacientes por sexo")},
    "estado": {"titulo": "Estado actual", "tipo": "Dona",
               "fn": lambda df: graficos.dona(df.groupBy("estado").count().toPandas(), "estado", "count",
                                              "Estado hospitalario")},
    "clinicas": {"titulo": "Clínicas que refieren", "tipo": "Barras",
                 "fn": lambda df: graficos.barras_h(
                     df.filter(F.col("clinicaRef").isNotNull()).groupBy("clinicaRef").count().toPandas(),
                     "clinicaRef", "count", "Pacientes por clínica que refiere", eje="pacientes")},
    "ingresos": {"titulo": "Ingresos por mes", "tipo": "Línea",
                 "fn": lambda df: graficos.linea(_ingresos_por_mes(df), "mes", "count",
                                                 "Admisiones por mes", "ingresos")},
}
