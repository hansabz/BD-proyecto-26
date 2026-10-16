"""
areas/telemetria.py — Fase II (Big Data) dentro del dashboard.

Los logs de los sensores (logs_monitoreo_pacientes.csv, Figura 6) NO se
guardan en SQL Server: se leen del archivo y se procesan con PySpark.
Lo único que vuelve a la base es el resultado: el nivel de riesgo de cada
internación (asignacionCama.nivelRiesgo).

    Paso 1  Ingesta masiva        bigdata/generar_logs.py
    Paso 2  Query BD 1            limpieza (también el botón "Limpiar datos" de la tabla)
    Paso 3  Query BD 2            top 5 de alertas críticas con Spark SQL
    Paso 4  Query BD 3            UDF lambda -> Nivel_Riesgo de cada lectura
    Paso 5  Riesgo -> SQL Server  nivel sostenido de cada internación -> asignacionCama.nivelRiesgo
    Paso 6  Parquet               exportación del DataFrame refinado

Cumple la misma "forma" que las demás áreas (ver areas/__init__.py), así que
reutiliza la tabla en vivo, los filtros y las gráficas.
"""
import time

import pandas as pd
from pyspark.sql import functions as F

from bigdata import generar_logs, pipeline
from core import db, graficos
from core.db import ErrorNegocio
from core.spark import a_registros, leer_sql, obtener_spark

TITULO = "Telemetría de pacientes (Big Data)"
TABLAS = []                 # no usa tablas de SQL Server: la "firma" es la fecha del archivo
LIMITE_FILAS = 500          # la tabla del navegador muestra como máximo esta cantidad de filas

COLUMNAS = [
    {"campo": "ID_Registro", "etiqueta": "ID Registro", "tipo": "mono"},
    {"campo": "ID_Paciente", "etiqueta": "ID Paciente", "tipo": "id"},
    {"campo": "Timestamp", "etiqueta": "Timestamp", "tipo": "fechahora"},
    {"campo": "Frecuencia_Cardiaca", "etiqueta": "Frec. cardíaca", "tipo": "numero"},
    {"campo": "Saturacion_Oxigeno", "etiqueta": "Sat. oxígeno", "tipo": "numero"},
    {"campo": "Alerta_Generada", "etiqueta": "Alerta", "tipo": "alerta"},
]


def _existe() -> bool:
    return pipeline.RUTA_CSV.exists()


def _requerir_csv():
    if not _existe():
        raise ErrorNegocio("Todavía no hay logs: usá «Generar logs» (paso 1 del pipeline).")


def firma() -> str:
    """Cambia cada vez que se regenera el archivo -> la tabla se refresca sola."""
    if not _existe():
        return "sin-archivo"
    info = pipeline.RUTA_CSV.stat()
    return f"{info.st_mtime_ns}:{info.st_size}"


# ── Tabla y métricas (pandas: leer 6,000 filas de un CSV es instantáneo) ──
def _leer_pandas() -> pd.DataFrame:
    return pd.read_csv(pipeline.RUTA_CSV, dtype={"Frecuencia_Cardiaca": "Int64", "Saturacion_Oxigeno": "Int64"})


def tabla() -> list[dict]:
    if not _existe():
        return []
    pdf = _leer_pandas().sort_values("Timestamp", ascending=False).head(LIMITE_FILAS)
    pdf["Timestamp"] = pdf["Timestamp"].str.slice(0, 16)
    return pdf.astype(object).where(pdf.notna(), None).to_dict(orient="records")


def total_registros() -> int:
    return len(_leer_pandas()) if _existe() else 0


def metricas() -> list[dict]:
    if not _existe():
        return [{"etiqueta": "Registros", "valor": 0, "detalle": "generá los logs (paso 1)"}]
    pdf = _leer_pandas()
    fc, spo2 = pdf["Frecuencia_Cardiaca"], pdf["Saturacion_Oxigeno"]
    invalidos = int((fc.isna() | spo2.isna() | (fc < pipeline.FC_MIN) | (fc > pipeline.FC_MAX)
                     | (spo2 < pipeline.SPO2_MIN) | (spo2 > pipeline.SPO2_MAX)).sum())
    criticos = db.escalar("SELECT COUNT(*) FROM asignacionCama WHERE codigoEstado = 1 AND nivelRiesgo = 'Crítico'")
    return [
        {"etiqueta": "Registros", "valor": len(pdf), "detalle": "lecturas de sensores"},
        {"etiqueta": "Pacientes monitoreados", "valor": int(pdf["ID_Paciente"].nunique()), "detalle": "IDs únicos"},
        {"etiqueta": "SpO2 < 90 %", "valor": int((spo2 < 90).sum()), "detalle": "alertas críticas (BD 2)",
         "tono": "alerta"},
        {"etiqueta": "Lecturas inválidas", "valor": invalidos, "detalle": "nulos o fallas (BD 1)",
         "tono": "alerta" if invalidos else ""},
        {"etiqueta": "Internados en riesgo crítico", "valor": criticos or 0, "detalle": "asignacionCama.nivelRiesgo",
         "tono": "alerta" if criticos else ""},
        {"etiqueta": "Parquet exportado", "valor": "Sí" if pipeline.RUTA_PARQUET.exists() else "No",
         "formato": "texto", "detalle": "logs_refinados.parquet"},
    ]


# ── Pasos del pipeline (botones de la página) ────────────────────────
def generar(registros: int) -> dict:
    if not 5000 <= registros <= 200000:
        raise ErrorNegocio({"registros": "Entre 5,000 (mínimo de la guía) y 200,000."})
    try:
        resultado = generar_logs.generar(registros, generar_logs.estancias_de_la_base())
    except ValueError as exc:
        raise ErrorNegocio(str(exc)) from exc
    resultado["mensaje"] = (f"Se generaron {registros:,} lecturas de {resultado['pacientes']} pacientes "
                            "hospitalizados en logs_monitoreo_pacientes.csv.")
    return resultado


def _limpio():
    """Ingesta + Query BD 1. Devuelve (spark, limpio, descartados, resumen)."""
    _requerir_csv()
    spark = obtener_spark()
    return spark, *pipeline.bd1_limpieza(pipeline.ingesta(spark))


def paso_bd1() -> dict:
    inicio = time.perf_counter()
    _, _, _, resumen = _limpio()
    resumen["por_motivo"] = [{"motivo": k, "registros": v} for k, v in resumen["por_motivo"].items()]
    resumen["segundos"] = round(time.perf_counter() - inicio, 2)
    return resumen


def paso_bd2() -> dict:
    inicio = time.perf_counter()
    spark, limpio, _, _ = _limpio()
    filas = a_registros(pipeline.bd2_top5(spark, limpio))
    return {"filas": filas, "sql": " ".join(pipeline.SQL_TOP5.split()), "segundos": round(time.perf_counter() - inicio, 2)}


def paso_bd3() -> dict:
    inicio = time.perf_counter()
    _, limpio, _, _ = _limpio()
    con_riesgo = pipeline.bd3_riesgo(limpio).cache()
    distribucion = {r["Nivel_Riesgo"]: r["count"] for r in con_riesgo.groupBy("Nivel_Riesgo").count().collect()}
    muestra = a_registros(con_riesgo.filter(F.col("Nivel_Riesgo") == "Crítico").orderBy(F.desc("Timestamp")).limit(8))
    return {"distribucion": distribucion, "muestra": muestra, "segundos": round(time.perf_counter() - inicio, 2)}


def paso_riesgo() -> dict:
    """
    Paso 5: lleva el riesgo de las lecturas a la base.
      1. ingesta + BD 1 + BD 3 (cada lectura con su Nivel_Riesgo)
      2. cruce con asignacionCama: cada lectura cae en la internación del
         paciente vigente en ese momento (fechaIngreso <= Timestamp <= fechaSalida)
      3. nivel sostenido por internación (ver pipeline.UMBRAL_SOSTENIDO)
      4. UPDATE asignacionCama SET nivelRiesgo = ...   (una sola transacción)
    Los logs en sí no se insertan en ninguna tabla.
    """
    inicio = time.perf_counter()
    _, limpio, _, _ = _limpio()
    internaciones = leer_sql("SELECT numeroIngreso, numeroPaciente, fechaIngreso, fechaSalida FROM asignacionCama")
    por_internacion = a_registros(
        pipeline.riesgo_por_internacion(pipeline.bd3_riesgo(limpio), internaciones).orderBy("numeroIngreso"))
    if por_internacion:
        with db.transaccion() as cur:
            cur.executemany("UPDATE asignacionCama SET nivelRiesgo = ? WHERE numeroIngreso = ?",
                            [(f["nivelRiesgo"], f["numeroIngreso"]) for f in por_internacion])
    return {
        "internaciones": por_internacion,
        "umbral": int(pipeline.UMBRAL_SOSTENIDO * 100),
        "segundos": round(time.perf_counter() - inicio, 2),
        "mensaje": f"Nivel de riesgo actualizado en {len(por_internacion)} internaciones (asignacionCama).",
    }


def paso_parquet() -> dict:
    """Pipeline completo: ingesta -> BD 1 -> BD 3 -> .write.parquet() -> relectura de verificación."""
    inicio = time.perf_counter()
    spark, limpio, _, resumen = _limpio()
    salida = pipeline.exportar_parquet(pipeline.bd3_riesgo(limpio))
    salida["verificacion"] = pipeline.verificar_parquet(spark)
    salida["limpieza"] = {"leidos": resumen["leidos"], "validos": resumen["validos"]}
    salida["segundos"] = round(time.perf_counter() - inicio, 2)
    salida["mensaje"] = f"Parquet escrito con {salida['metodo']}: {salida['verificacion']['registros']:,} registros."
    return salida


# ── Interfaz genérica del área: dataset, limpieza, filtros, gráficas ──
def dataset():
    _requerir_csv()
    return pipeline.ingesta(obtener_spark())


def datos_limpios(df):
    """Lo que usan "Filtrar" y las gráficas cuando el usuario ya apretó "Limpiar datos"."""
    return df.filter(pipeline.condicion_valida()).dropDuplicates(["ID_Registro"])


def limpiar() -> dict:
    """Botón "Limpiar datos" de la tabla = Query BD 1. Un motivo por registro descartado."""
    _, limpio, descartados, resumen = _limpio()
    pasos = [{"paso": "Lectura del CSV", "detalle": "registros leídos por Spark", "afectados": resumen["leidos"],
              "accion": "leidos"}]
    pasos += [{"paso": motivo, "detalle": "registro descartado", "afectados": n, "accion": "descartados"}
              for motivo, n in resumen["por_motivo"].items()]
    pasos.append({"paso": "Lecturas válidas", "detalle": "pasan a BD 2 y BD 3", "afectados": resumen["validos"],
                  "accion": "validos"})
    malos = a_registros(descartados.withColumnRenamed("motivo_descarte", "motivo").orderBy("ID_Registro"))
    return {
        "total_original": resumen["leidos"],
        "total_limpio": resumen["validos"],
        "total_descartados": resumen["descartados"],
        "pasos": pasos,
        "registros": a_registros(limpio.orderBy(F.desc("Timestamp")).limit(LIMITE_FILAS)),
        "descartados": malos,
    }


def filtros() -> list[dict]:
    return [
        {"id": "texto", "tipo": "texto", "etiqueta": "Buscar", "columnas": ["ID_Paciente", "ID_Registro"],
         "ayuda": "PAC-0001 o LOG-000123"},
        {"id": "Alerta_Generada", "tipo": "opcion", "etiqueta": "Alerta", "opciones": [("Sí", "Sí"), ("No", "No")]},
        {"id": "Frecuencia_Cardiaca", "tipo": "rango", "etiqueta": "Frecuencia cardíaca (lpm)"},
        {"id": "Saturacion_Oxigeno", "tipo": "rango", "etiqueta": "Saturación de oxígeno (%)"},
        {"id": "Timestamp", "tipo": "fechas", "etiqueta": "Fecha de la lectura"},
    ]


def _top5(df):
    pdf = pipeline.bd2_top5(obtener_spark(), datos_limpios(df)).toPandas()
    return graficos.barras_h(pdf, "ID_Paciente", "alertas_criticas",
                             "Query BD 2 · Top 5 pacientes con más alertas críticas (SpO2 < 90 %)",
                             eje="lecturas con SpO2 < 90 %")


def _riesgo(df):
    pdf = pipeline.bd3_riesgo(datos_limpios(df)).groupBy("Nivel_Riesgo").count().toPandas()
    return graficos.dona(pdf, "Nivel_Riesgo", "count", "Query BD 3 · Nivel de riesgo de las lecturas (UDF lambda)")


GRAFICOS = {
    "top5": {"titulo": "Top 5 alertas críticas", "tipo": "Barras · Spark SQL", "fn": _top5},
    "riesgo": {"titulo": "Nivel de riesgo", "tipo": "Dona · UDF", "fn": _riesgo},
    "fc": {"titulo": "Frecuencia cardíaca", "tipo": "Histograma",
           "fn": lambda df: graficos.histograma(
               df.select("Frecuencia_Cardiaca").toPandas()["Frecuencia_Cardiaca"].astype(float),
               "Distribución de la frecuencia cardíaca", "latidos por minuto", bins=30,
               linea=pipeline.FC_MAX, etiqueta_linea=f"límite del sensor ({pipeline.FC_MAX})")},
    "spo2": {"titulo": "Saturación de oxígeno", "tipo": "Histograma",
             "fn": lambda df: graficos.histograma(
                 df.select("Saturacion_Oxigeno").toPandas()["Saturacion_Oxigeno"].astype(float),
                 "Distribución de la saturación de oxígeno", "% SpO2", bins=25, linea=90,
                 etiqueta_linea="umbral crítico (90 %)")},
    "alertas": {"titulo": "Alertas por día", "tipo": "Línea",
                "fn": lambda df: graficos.linea(
                    df.filter(F.col("Alerta_Generada") == "Sí")
                    .groupBy(F.to_date("Timestamp").alias("dia")).count().orderBy("dia").toPandas(),
                    "dia", "count", "Alertas generadas por día", "alertas")},
}
