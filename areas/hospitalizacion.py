"""
areas/hospitalizacion.py — RF3: Control de hospitalización y asignación de camas (Figura 3).

Doble candado contra asignar una cama ocupada:
  1. La transacción bloquea la fila de la cama (UPDLOCK, HOLDLOCK) y
     verifica su estado antes de insertar.
  2. El índice único filtrado UX_asignacionCama_camaOcupada hace que el
     propio motor rechace una segunda internación activa en esa cama.
Las fechas de ingreso y salida las pone SQL Server (GETDATE()), no el
usuario, para que reflejen la hora real del movimiento.
"""
import pyodbc
from pyspark.sql import functions as F

from core import db, graficos
from core.db import ErrorNegocio
from core.spark import leer_sql

TITULO = "Hospitalización y camas"
TABLAS = ["asignacionCama", "cama", "registroIngreso"]

# Figura 3 + días de estancia (si sigue internado, se cuenta hasta ahora con GETDATE())
# + nivelRiesgo, que calcula PySpark a partir de los logs de telemetría (Fase II).
BASE_SQL = """
SELECT v.numeroIngreso, v.numeroPaciente, v.paciente, v.codigoSala, v.sala, v.numeroCama,
       v.fechaIngreso, v.fechaSalida, v.estado, v.nivelRiesgo,
       CAST(DATEDIFF(hour, v.fechaIngreso, ISNULL(v.fechaSalida, GETDATE())) / 24.0 AS dec(7,1)) AS dias
FROM VW_Registro_Camas v
"""

COLUMNAS = [
    {"campo": "numeroIngreso", "etiqueta": "No. Ingreso", "tipo": "id"},
    {"campo": "numeroPaciente", "etiqueta": "Paciente", "tipo": "mono", "extra": "paciente"},
    {"campo": "sala", "etiqueta": "Sala asignada"},
    {"campo": "numeroCama", "etiqueta": "No. Cama", "tipo": "mono"},
    {"campo": "fechaIngreso", "etiqueta": "Fecha ingreso", "tipo": "fechahora"},
    {"campo": "fechaSalida", "etiqueta": "Fecha salida (real)", "tipo": "fechahora", "vacio": "N/A"},
    {"campo": "dias", "etiqueta": "Días", "tipo": "numero"},
    {"campo": "estado", "etiqueta": "Estado", "tipo": "estado"},
    {"campo": "nivelRiesgo", "etiqueta": "Nivel de riesgo", "tipo": "riesgo"},
]


def tabla() -> list[dict]:
    return db.consultar(BASE_SQL + " ORDER BY CASE WHEN v.fechaSalida IS NULL THEN 0 ELSE 1 END, v.fechaIngreso DESC")


def metricas() -> list[dict]:
    c = db.consultar_uno("""SELECT COUNT(*) AS total,
                                   SUM(CASE WHEN codigoEstado = 1 THEN 1 ELSE 0 END) AS ocupadas,
                                   SUM(CASE WHEN codigoEstado = 0 THEN 1 ELSE 0 END) AS libres
                            FROM cama""")
    estancia = db.escalar("""SELECT AVG(DATEDIFF(hour, fechaIngreso, fechaSalida) / 24.0) FROM asignacionCama
                             WHERE codigoEstado = 0 AND fechaSalida >= DATEADD(day, -90, GETDATE())""")
    espera = len(pendientes())
    # Nivel de riesgo de los internados ahora (lo escribe el pipeline de telemetría).
    riesgo = db.consultar_uno("""SELECT SUM(CASE WHEN nivelRiesgo = 'Crítico' THEN 1 ELSE 0 END) AS criticos,
                                         SUM(CASE WHEN nivelRiesgo IS NULL THEN 1 ELSE 0 END) AS sin_dato
                                  FROM asignacionCama WHERE codigoEstado = 1""")
    # cama.codigoEstado es bit: 0 = disponible, 1 = ocupada.
    ocupacion = (c["ocupadas"] or 0) / max(1, c["total"] or 0)
    return [
        {"etiqueta": "Ocupación", "valor": round(ocupacion * 100), "formato": "porcentaje",
         "detalle": f"{c['ocupadas']} de {c['total']} camas",
         "tono": "alerta" if ocupacion > 0.85 else ""},
        {"etiqueta": "Camas disponibles", "valor": c["libres"] or 0, "detalle": "listas para asignar"},
        {"etiqueta": "Internados", "valor": c["ocupadas"] or 0, "detalle": "camas ocupadas"},
        {"etiqueta": "Riesgo crítico", "valor": riesgo["criticos"] or 0,
         "detalle": f"internados · {riesgo['sin_dato'] or 0} sin lecturas" if riesgo["sin_dato"] else "internados (telemetría)",
         "tono": "alerta" if riesgo["criticos"] else ""},
        {"etiqueta": "En espera de cama", "valor": espera, "detalle": "ingresos sin asignar",
         "tono": "alerta" if espera else ""},
        {"etiqueta": "Estancia promedio", "valor": round(float(estancia or 0), 1), "detalle": "días (altas 90 d)"},
    ]


def mapa_camas() -> list[dict]:
    """Todas las camas con su estado y, si está ocupada, el paciente (LEFT JOIN con la internación activa)."""
    return db.consultar("""
        SELECT c.codigoSala, s.nombre AS sala, c.numeroCama,
               c.codigoEstado AS ocupada,     -- bit -> true / false en el JSON
               CASE WHEN c.codigoEstado = 1 THEN 'Ocupada' ELSE 'Disponible' END AS estado,
               a.numeroIngreso, a.numeroPaciente, p.nombres + ' ' + p.apellidos AS paciente, a.fechaIngreso,
               a.nivelRiesgo
        FROM cama c
        JOIN sala s        ON s.codigoSala = c.codigoSala
        LEFT JOIN asignacionCama a ON a.numeroCama = c.numeroCama AND a.codigoSala = c.codigoSala
                                  AND a.codigoEstado = 1
        LEFT JOIN paciente p ON p.numeroPaciente = a.numeroPaciente
        ORDER BY c.codigoSala, c.numeroCama""")


def pendientes() -> list[dict]:
    """Ingresos registrados que todavía no tienen cama."""
    return db.consultar("""
        SELECT r.numeroIngreso, r.numeroPaciente, p.nombres + ' ' + p.apellidos AS paciente, r.fechaReg,
               DATEDIFF(year, p.fechaNac, GETDATE()) AS edad
        FROM registroIngreso r
        JOIN paciente p ON p.numeroPaciente = r.numeroPaciente
        WHERE NOT EXISTS (SELECT 1 FROM asignacionCama a WHERE a.numeroIngreso = r.numeroIngreso)
          AND NOT EXISTS (SELECT 1 FROM asignacionCama a WHERE a.numeroPaciente = r.numeroPaciente
                                                         AND a.codigoEstado = 1)
        ORDER BY r.fechaReg""")


def asignar(datos: dict) -> dict:
    """
    RF3: interna al paciente de un ingreso en una cama. Todo dentro de UNA transacción:
      1. el ingreso existe y todavía no tiene cama
      2. el paciente no está internado en otra cama
      3. se BLOQUEA la fila de la cama y se verifica que esté disponible (codigoEstado = 0)
      4. INSERT de la internación (fechaIngreso la pone SQL Server) + cama -> codigoEstado = 1
    Si cualquier paso lanza ErrorNegocio, el ROLLBACK deshace todo.
    """
    ingreso = (datos.get("numeroIngreso") or "").strip()
    sala = (datos.get("codigoSala") or "").strip()
    cama = (datos.get("numeroCama") or "").strip()
    if not (ingreso and sala and cama):
        raise ErrorNegocio("Elegí el ingreso, la sala y la cama.")
    try:
        with db.transaccion() as cur:
            reg = cur.execute("SELECT numeroPaciente FROM registroIngreso WHERE numeroIngreso = ?", ingreso).fetchone()
            if not reg:
                raise ErrorNegocio(f"No existe el ingreso {ingreso}.")
            if cur.execute("SELECT 1 FROM asignacionCama WHERE numeroIngreso = ?", ingreso).fetchone():
                raise ErrorNegocio(f"El ingreso {ingreso} ya tiene una cama asignada.")
            paciente = reg[0]
            otra = cur.execute("SELECT numeroCama, codigoSala FROM asignacionCama"
                               " WHERE numeroPaciente = ? AND codigoEstado = 1", paciente).fetchone()
            if otra:
                raise ErrorNegocio(f"{paciente} ya está internado en la cama {otra[0]} ({otra[1]}).")
            # Bloquea la fila de la cama hasta el COMMIT: nadie más puede tomarla en paralelo.
            # UPDLOCK = "la voy a modificar"; HOLDLOCK = mantener el bloqueo hasta el final.
            # Si dos recepcionistas eligen la misma cama a la vez, el segundo espera y
            # al continuar ya la ve ocupada (1), así que recibe el mensaje de cama ocupada.
            fila = cur.execute("SELECT codigoEstado FROM cama WITH (UPDLOCK, HOLDLOCK)"
                               " WHERE numeroCama = ? AND codigoSala = ?", cama, sala).fetchone()
            if not fila:
                raise ErrorNegocio(f"La cama {cama} no existe en la sala {sala}.")
            if fila[0]:                     # bit 1 = ocupada
                ocupante = cur.execute("SELECT numeroPaciente FROM asignacionCama WHERE numeroCama = ?"
                                       " AND codigoSala = ? AND codigoEstado = 1", cama, sala).fetchone()
                raise ErrorNegocio(f"La cama {cama} ya está ocupada"
                                   f"{' por ' + ocupante[0] if ocupante else ''}. Elegí otra.")
            cur.execute("INSERT INTO asignacionCama (numeroIngreso, numeroPaciente, numeroCama, codigoSala)"
                        " VALUES (?, ?, ?, ?)", ingreso, paciente, cama, sala)   # fechaIngreso = GETDATE()
            cur.execute("UPDATE cama SET codigoEstado = 1 WHERE numeroCama = ? AND codigoSala = ?", cama, sala)
    except pyodbc.IntegrityError as exc:
        # Segundo candado: el índice único filtrado de SQL Server (sql/01_esquema.sql).
        if "UX_asignacionCama" in str(exc):
            raise ErrorNegocio("La cama o el paciente ya tienen una internación activa.") from exc
        raise
    return {"mensaje": f"{paciente} internado en la cama {cama}. Ingreso registrado con la hora actual."}


def dar_alta(ingreso: str) -> dict:
    """RF3: registra la salida real (GETDATE()) y libera la cama, en una sola transacción."""
    with db.transaccion() as cur:
        fila = cur.execute("SELECT numeroCama, codigoSala, numeroPaciente FROM asignacionCama WITH (UPDLOCK)"
                           " WHERE numeroIngreso = ? AND codigoEstado = 1", ingreso).fetchone()
        if not fila:
            raise ErrorNegocio(f"El ingreso {ingreso} no está internado.")
        cur.execute("UPDATE asignacionCama SET fechaSalida = GETDATE(), codigoEstado = 0 WHERE numeroIngreso = ?",
                    ingreso)
        cur.execute("UPDATE cama SET codigoEstado = 0 WHERE numeroCama = ? AND codigoSala = ?", fila[0], fila[1])
    return {"mensaje": f"Alta médica de {fila[2]}. La cama {fila[0]} quedó disponible."}


def salas() -> list[dict]:
    return db.consultar("SELECT codigoSala AS id, nombre FROM sala WHERE camasDisponibles > 0 ORDER BY codigoSala")


# ── PySpark: dataset, filtros y gráficas ─────────────────────────────
def dataset():
    return leer_sql(BASE_SQL)


def filtros() -> list[dict]:
    return [
        {"id": "texto", "tipo": "texto", "etiqueta": "Buscar",
         "columnas": ["numeroIngreso", "numeroPaciente", "paciente", "numeroCama"], "ayuda": "ingreso, paciente o cama"},
        {"id": "codigoSala", "tipo": "opcion", "etiqueta": "Sala", "opciones": [(s["id"], s["nombre"]) for s in salas()]},
        {"id": "estado", "tipo": "opcion", "etiqueta": "Estado",
         "opciones": [("Internado", "Internado"), ("Alta Médica", "Alta Médica")]},
        {"id": "nivelRiesgo", "tipo": "opcion", "etiqueta": "Nivel de riesgo",
         "opciones": [("Crítico", "Crítico"), ("Moderado", "Moderado"), ("Bajo", "Bajo")]},
        {"id": "dias", "tipo": "rango", "etiqueta": "Días de estancia"},
        {"id": "fechaIngreso", "tipo": "fechas", "etiqueta": "Fecha de ingreso"},
    ]


def _ocupacion(_df):
    """Ocupadas / disponibles por sala. Lee la tabla cama (estado actual, no el historial)."""
    camas = leer_sql("SELECT s.nombre AS sala, c.codigoEstado FROM cama c JOIN sala s ON s.codigoSala = c.codigoSala")
    pdf = (camas.groupBy("sala")
           .agg(F.sum(F.when(F.col("codigoEstado"), 1).otherwise(0)).alias("ocupadas"),      # bit -> booleano en Spark
                F.sum(F.when(~F.col("codigoEstado"), 1).otherwise(0)).alias("disponibles"))
           .orderBy("sala").toPandas())
    return graficos.apiladas(pdf, "sala", [("ocupadas", "Ocupadas", graficos.ALERTA),
                                           ("disponibles", "Disponibles", graficos.PRINCIPAL)],
                             "Ocupación de camas por sala (en vivo)")


def _riesgo_internados(df):
    """Nivel de riesgo (asignacionCama.nivelRiesgo) de las internaciones visibles."""
    pdf = (df.withColumn("nivel", F.coalesce(F.col("nivelRiesgo"), F.lit("Sin lecturas")))
           .groupBy("nivel").count().toPandas())
    return graficos.dona(pdf, "nivel", "count", "Nivel de riesgo de las internaciones (telemetría)")


GRAFICOS = {
    "riesgo": {"titulo": "Nivel de riesgo", "tipo": "Dona", "fn": _riesgo_internados},
    "ocupacion": {"titulo": "Ocupación por sala", "tipo": "Barras apiladas", "fn": _ocupacion},
    "estancia": {"titulo": "Días de estancia", "tipo": "Histograma",
                 "fn": lambda df: graficos.histograma(
                     df.filter(F.col("estado") == "Alta Médica").select("dias").toPandas()["dias"],
                     "Duración de la estancia (altas)", "días", bins=12)},
    "estado": {"titulo": "Internados vs. altas", "tipo": "Dona",
               "fn": lambda df: graficos.dona(df.groupBy("estado").count().toPandas(), "estado", "count",
                                              "Hospitalizaciones por estado")},
    "sala": {"titulo": "Hospitalizaciones por sala", "tipo": "Barras",
             "fn": lambda df: graficos.barras_h(df.groupBy("sala").count().toPandas(), "sala", "count",
                                                "Hospitalizaciones por sala", eje="ingresos")},
    "semanas": {"titulo": "Ingresos por semana", "tipo": "Línea",
                "fn": lambda df: graficos.linea(
                    df.groupBy(F.date_trunc("week", "fechaIngreso").alias("semana")).count()
                    .orderBy("semana").toPandas(), "semana", "count", "Ingresos a sala por semana", "ingresos")},
}
