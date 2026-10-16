"""
bigdata/pipeline.py — Fase II: procesamiento distribuido de la telemetría con PySpark.

    ingesta()          lee logs_monitoreo_pacientes.csv con un esquema explícito (Figura 6)
    bd1_limpieza()     Query BD 1 — quita nulos y lecturas atípicas (fallas del sensor)
    bd2_top5()         Query BD 2 — Spark SQL sobre una vista temporal: top 5 de alertas críticas
    bd3_riesgo()       Query BD 3 — UDF lambda que inyecta la columna Nivel_Riesgo
    riesgo_por_internacion()  lleva el riesgo de las lecturas a cada internación
                              (asignacionCama.nivelRiesgo)
    exportar_parquet() Exportación corporativa con .write.parquet()

Los logs NO se guardan en SQL Server: se leen del CSV, se procesan en Spark
y lo único que vuelve a la base es el nivelRiesgo de cada internación.

Las funciones reciben y devuelven DataFrames: las usa el dashboard
(areas/telemetria.py) y su lógica es la misma que la del notebook de Colab
(bigdata/HospitalW_BigData.ipynb).
"""
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

CARPETA_DATOS = Path(__file__).resolve().parent / "datos"
RUTA_CSV = CARPETA_DATOS / "logs_monitoreo_pacientes.csv"
RUTA_PARQUET = CARPETA_DATOS / "logs_refinados.parquet"

# Rangos fisiológicamente posibles. Fuera de ellos, la lectura es una falla del sensor.
FC_MIN, FC_MAX = 30, 220           # latidos por minuto (ejemplo de la guía)
SPO2_MIN, SPO2_MAX = 50, 100       # % de saturación (más de 100 % es imposible)

# Esquema de la Figura 6. Declararlo evita que Spark lea todo como texto
# (o que tenga que recorrer el archivo dos veces para adivinar los tipos).
ESQUEMA = T.StructType([
    T.StructField("ID_Registro", T.StringType(), True),
    T.StructField("ID_Paciente", T.StringType(), True),
    T.StructField("Timestamp", T.TimestampType(), True),
    T.StructField("Frecuencia_Cardiaca", T.IntegerType(), True),
    T.StructField("Saturacion_Oxigeno", T.IntegerType(), True),
    T.StructField("Alerta_Generada", T.StringType(), True),
])


# ── Ingesta ──────────────────────────────────────────────────────────
def ingesta(spark: SparkSession, ruta: Path | str = RUTA_CSV) -> DataFrame:
    return (spark.read
            .option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(ESQUEMA)
            .csv(str(ruta)))


# ── Query BD 1 — limpieza y filtrado de anomalías ────────────────────
# Cada lectura descartada tiene UN solo motivo (el primero que cumple, en este
# orden). Por eso la suma de los motivos es exactamente el total de descartados.
MOTIVOS = [
    "Valores nulos (sensor desconectado)",
    f"FC < {FC_MIN} lpm (falla del sensor)",
    f"FC > {FC_MAX} lpm (falla del sensor)",
    f"SpO2 fuera de {SPO2_MIN}–{SPO2_MAX} % (falla del sensor)",
    "Registro duplicado",
]


def condicion_valida():
    """Una lectura es válida si no tiene nulos y sus signos vitales están en rango posible."""
    return (
        F.col("ID_Paciente").isNotNull() & F.col("Timestamp").isNotNull()
        & F.col("Frecuencia_Cardiaca").isNotNull() & F.col("Saturacion_Oxigeno").isNotNull()
        & F.col("Frecuencia_Cardiaca").between(FC_MIN, FC_MAX)
        & F.col("Saturacion_Oxigeno").between(SPO2_MIN, SPO2_MAX)
    )


def con_motivo(df: DataFrame) -> DataFrame:
    """Agrega la columna motivo_descarte (NULL = lectura válida)."""
    repetido = F.row_number().over(Window.partitionBy("ID_Registro").orderBy("Timestamp")) > 1
    motivo = (
        F.when(F.col("Frecuencia_Cardiaca").isNull() | F.col("Saturacion_Oxigeno").isNull()
               | F.col("ID_Paciente").isNull() | F.col("Timestamp").isNull(), MOTIVOS[0])
        .when(F.col("Frecuencia_Cardiaca") < FC_MIN, MOTIVOS[1])
        .when(F.col("Frecuencia_Cardiaca") > FC_MAX, MOTIVOS[2])
        .when(~F.col("Saturacion_Oxigeno").between(SPO2_MIN, SPO2_MAX), MOTIVOS[3])
        .when(repetido, MOTIVOS[4])
    )
    return df.withColumn("motivo_descarte", motivo)


def bd1_limpieza(df: DataFrame) -> tuple[DataFrame, DataFrame, dict]:
    """
    Devuelve (limpio, descartados, resumen).
      limpio       lecturas válidas (sin la columna de motivo)
      descartados  lecturas removidas, cada una con su motivo_descarte
      resumen      leídos, válidos, descartados y cuántos por cada motivo
    """
    marcado = con_motivo(df).cache()
    conteo = {r["motivo_descarte"]: r["count"]
              for r in marcado.groupBy("motivo_descarte").count().collect()}
    validos = conteo.pop(None, 0)
    descartados = sum(conteo.values())
    resumen = {"leidos": validos + descartados, "validos": validos, "descartados": descartados,
               "por_motivo": {m: conteo.get(m, 0) for m in MOTIVOS}}
    limpio = marcado.filter(F.col("motivo_descarte").isNull()).drop("motivo_descarte")
    return limpio, marcado.filter(F.col("motivo_descarte").isNotNull()), resumen


# ── Query BD 2 — top 5 de alertas críticas con Spark SQL ─────────────
SQL_TOP5 = """
    SELECT ID_Paciente,
           COUNT(*)                   AS alertas_criticas,
           ROUND(MIN(Saturacion_Oxigeno), 0) AS spo2_minima,
           ROUND(AVG(Saturacion_Oxigeno), 1) AS spo2_promedio
    FROM logs_limpios
    WHERE Saturacion_Oxigeno < 90
    GROUP BY ID_Paciente
    ORDER BY alertas_criticas DESC, ID_Paciente
    LIMIT 5
"""


def bd2_top5(spark: SparkSession, df_limpio: DataFrame) -> DataFrame:
    # La vista temporal hace que el DataFrame se pueda consultar con SQL normal.
    df_limpio.createOrReplaceTempView("logs_limpios")
    return spark.sql(SQL_TOP5)


# ── Query BD 3 — clasificación de riesgo con una UDF lambda ──────────
# Evalúa las DOS variables a la vez. Umbrales clínicos habituales:
#   Crítico  : SpO2 < 90 %  o  FC > 130  o  FC < 40
#   Moderado : SpO2 90–93 % o  FC 101–130 o FC 40–49
#   Bajo     : todo lo demás
clasificar_riesgo = F.udf(
    lambda fc, spo2: None if fc is None or spo2 is None else (
        "Crítico" if spo2 < 90 or fc > 130 or fc < 40 else
        "Moderado" if spo2 < 94 or fc > 100 or fc < 50 else
        "Bajo"
    ),
    T.StringType(),
)


def bd3_riesgo(df: DataFrame) -> DataFrame:
    """Spark reparte la UDF entre los workers: cada uno clasifica sus propias filas."""
    return df.withColumn("Nivel_Riesgo", clasificar_riesgo(F.col("Frecuencia_Cardiaca"),
                                                           F.col("Saturacion_Oxigeno")))


# ── De lecturas a pacientes: nivel de riesgo sostenido ───────────────
# La UDF clasifica CADA lectura. Para el nivel del paciente no alcanza con una
# sola lectura (un pico aislado es ruido del sensor), así que se mira la
# proporción de lecturas de cada nivel durante el período:
#   Crítico   si al menos el 25 % de sus lecturas fueron críticas
#   Moderado  si al menos el 25 % fueron moderadas o críticas
#   Bajo      en cualquier otro caso
UMBRAL_SOSTENIDO = 0.25


def _nivel_sostenido(total, criticas, moderadas):
    return (F.when(criticas / total >= UMBRAL_SOSTENIDO, "Crítico")
            .when((criticas + moderadas) / total >= UMBRAL_SOSTENIDO, "Moderado")
            .otherwise("Bajo"))


def _conteos():
    return [F.count(F.lit(1)).alias("lecturas"),
            F.sum(F.when(F.col("Nivel_Riesgo") == "Crítico", 1).otherwise(0)).alias("criticas"),
            F.sum(F.when(F.col("Nivel_Riesgo") == "Moderado", 1).otherwise(0)).alias("moderadas")]


def riesgo_por_paciente(df_riesgo: DataFrame) -> DataFrame:
    """Una fila por paciente con su nivel sostenido (lo usa el notebook, que no tiene base de datos)."""
    return (df_riesgo.groupBy("ID_Paciente").agg(*_conteos())
            .withColumn("nivelRiesgo", _nivel_sostenido(F.col("lecturas"), F.col("criticas"), F.col("moderadas"))))


def riesgo_por_internacion(df_riesgo: DataFrame, internaciones: DataFrame) -> DataFrame:
    """
    Cruza cada lectura con la internación en la que ocurrió: mismo paciente y
    Timestamp entre fechaIngreso y fechaSalida (o ahora, si sigue internado).
    Devuelve una fila por numeroIngreso con su nivelRiesgo -> asignacionCama.
    """
    lecturas, cama = df_riesgo.alias("l"), internaciones.alias("i")
    cruce = lecturas.join(
        cama,
        (F.col("l.ID_Paciente") == F.col("i.numeroPaciente"))
        & (F.col("l.Timestamp") >= F.col("i.fechaIngreso"))
        & (F.col("l.Timestamp") <= F.coalesce(F.col("i.fechaSalida"), F.current_timestamp())),
    )
    return (cruce.groupBy("i.numeroIngreso", "i.numeroPaciente").agg(*_conteos())
            .withColumn("nivelRiesgo", _nivel_sostenido(F.col("lecturas"), F.col("criticas"), F.col("moderadas"))))


# ── Exportación corporativa a Parquet ────────────────────────────────
def exportar_parquet(df: DataFrame, ruta: Path | str = RUTA_PARQUET) -> dict:
    """
    Escribe con .write.parquet(). En Linux (Colab / Databricks) funciona
    directo. En Windows, Spark necesita winutils.exe de Hadoop; si no está,
    se escribe el mismo DataFrame con pyarrow (mismo formato Parquet) y se
    informa el método usado.
    """
    ruta = Path(ruta)
    try:
        df.write.mode("overwrite").parquet(str(ruta))
        metodo = "spark .write.parquet()"
    except Exception as exc:  # noqa: BLE001
        if not _falta_hadoop_windows(exc):
            raise
        if ruta.exists():
            shutil.rmtree(ruta)
        ruta.mkdir(parents=True)
        df.toPandas().to_parquet(ruta / "part-00000.parquet", engine="pyarrow", index=False)
        metodo = "pyarrow (respaldo: Windows sin winutils.exe)"
    archivos = sorted(p.name for p in ruta.glob("*.parquet"))
    tamano = sum(p.stat().st_size for p in ruta.glob("*.parquet"))
    return {"ruta": str(ruta), "metodo": metodo, "archivos": archivos, "bytes": tamano}


def _falta_hadoop_windows(exc: Exception) -> bool:
    """True si el error es el clásico de Spark en Windows sin winutils.exe / hadoop.dll."""
    texto = str(exc)
    return any(clave in texto for clave in ("winutils", "HADOOP_HOME", "NativeIO$Windows", "UnsatisfiedLinkError"))


def verificar_parquet(spark: SparkSession, ruta: Path | str = RUTA_PARQUET) -> dict:
    """Relee el Parquet: comprueba que el archivo es válido y que conserva el esquema."""
    try:
        df = spark.read.parquet(str(ruta))
        return {"registros": df.count(), "lector": "spark.read.parquet()",
                "columnas": [f"{c.name}: {c.dataType.simpleString()}" for c in df.schema]}
    except Exception as exc:  # noqa: BLE001
        if not _falta_hadoop_windows(exc):
            raise
        import pyarrow.parquet as pq
        tabla = pq.read_table(str(ruta))
        return {"registros": tabla.num_rows, "lector": "pyarrow (respaldo Windows)",
                "columnas": [f"{c.name}: {c.type}" for c in tabla.schema]}
