"""
Arma bigdata/HospitalW_BigData.ipynb (el notebook de la Fase II para Google Colab / Databricks).

Se genera con este script para que el notebook use EXACTAMENTE el mismo
generador de logs que el dashboard (bigdata/generar_logs.py), sin copiarlo
a mano. Volver a correrlo si se cambia generar_logs.py:

    python bigdata/construir_notebook.py
"""
import json
import re
from pathlib import Path

AQUI = Path(__file__).resolve().parent


def md(texto):
    return {"cell_type": "markdown", "metadata": {}, "source": texto.strip("\n").splitlines(keepends=True)}


def code(texto):
    return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
            "source": texto.strip("\n").splitlines(keepends=True)}


# Generador de logs: el mismo código del proyecto, sin la parte de SQL Server ni el main.
fuente = (AQUI / "generar_logs.py").read_text(encoding="utf-8")
fuente = fuente[fuente.index("import argparse"):fuente.index("def estancias_de_la_base")]
fuente = fuente.replace("import argparse\n", "")
fuente = re.sub(r'RUTA_POR_DEFECTO = .*\n', 'RUTA_POR_DEFECTO = Path("logs_monitoreo_pacientes.csv")\n', fuente)

celdas = [
    md("""
# Hospital W 2.0 — Fase II: Procesamiento analítico distribuido con PySpark

Pipeline de Big Data sobre la telemetría de los sensores de pacientes (Figura 6 de la guía):

| Paso | Requisito de la guía |
|---|---|
| 1 | **Ecosistema nube** — inicializar un objeto `SparkSession` |
| 2 | **Ingesta masiva** — simular `logs_monitoreo_pacientes.csv` con ≥ 5,000 registros |
| 3 | **Query Big Data 1** — limpieza y filtrado de anomalías |
| 4 | **Query Big Data 2** — top 5 de alertas clínicas críticas con Spark SQL |
| 5 | **Query Big Data 3** — clasificación automática de riesgo con una UDF lambda |
| 6 | Nivel de riesgo sostenido por paciente (en el sistema se guarda en `asignacionCama.nivelRiesgo`) |
| 7 | **Exportación corporativa** — `.write.parquet()` |

Los logs **no se guardan en la base de datos**: viven solo en el CSV y en el Parquet refinado.

**Cómo correrlo en Google Colab:** *Archivo → Subir notebook* → elegir este archivo →
*Entorno de ejecución → Ejecutar todas*. No necesita archivos externos: los datos se generan en la celda 2.
"""),
    md("## 1. Ecosistema nube: SparkSession"),
    code("""
# Colab ya trae PySpark en las versiones recientes; esta línea lo instala si faltara.
!pip install -q pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (SparkSession.builder
         .appName("HospitalW-BigData")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

print("Spark", spark.version, "| master:", spark.sparkContext.master)
spark
"""),
    md("""
## 2. Ingesta masiva: simulación de la telemetría

Se generan **6,000 lecturas** (mínimo pedido: 5,000) con el esquema de la Figura 6, de los
pacientes hospitalizados durante la última semana (en el dashboard salen de `asignacionCama`;
aquí, sin base de datos, se simulan 12 pacientes internados).
Cada paciente tiene un perfil (estable, inestable o crítico) y se inyectan a propósito
~2 % de valores nulos (sensor desconectado) y ~3 % de lecturas imposibles (falla del sensor),
que son las que debe remover la Query BD 1.
"""),
    code(fuente.strip() + """

resumen = generar(registros=6000)
resumen
"""),
    code("""
# Lectura con esquema explícito (Figura 6): Spark no tiene que adivinar los tipos.
ESQUEMA = T.StructType([
    T.StructField("ID_Registro", T.StringType(), True),
    T.StructField("ID_Paciente", T.StringType(), True),
    T.StructField("Timestamp", T.TimestampType(), True),
    T.StructField("Frecuencia_Cardiaca", T.IntegerType(), True),
    T.StructField("Saturacion_Oxigeno", T.IntegerType(), True),
    T.StructField("Alerta_Generada", T.StringType(), True),
])

logs = (spark.read
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(ESQUEMA)
        .csv("logs_monitoreo_pacientes.csv"))

logs.printSchema()
print("Registros leídos:", logs.count())
logs.show(5, truncate=False)
"""),
    md("""
## 3. Query Big Data 1 — Limpieza y filtrado de anomalías

Se remueven los registros con **valores nulos** y las **lecturas atípicas** que representan
fallas del sensor: frecuencia cardíaca **< 30 o > 220 lpm** y saturación fuera de 50–100 %.
Cada registro descartado recibe **un solo motivo** (el primero que cumple), así que la suma
de los motivos es exactamente el total de descartados.
"""),
    code("""
FC_MIN, FC_MAX = 30, 220
SPO2_MIN, SPO2_MAX = 50, 100

from pyspark.sql import Window

# Motivo de descarte de cada fila (NULL = la lectura es válida). Un solo motivo por fila.
repetido = F.row_number().over(Window.partitionBy("ID_Registro").orderBy("Timestamp")) > 1
motivo = (F.when(F.col("Frecuencia_Cardiaca").isNull() | F.col("Saturacion_Oxigeno").isNull()
                 | F.col("ID_Paciente").isNull() | F.col("Timestamp").isNull(), "Valores nulos (sensor desconectado)")
           .when(F.col("Frecuencia_Cardiaca") < FC_MIN, "FC < 30 lpm (falla del sensor)")
           .when(F.col("Frecuencia_Cardiaca") > FC_MAX, "FC > 220 lpm (falla del sensor)")
           .when(~F.col("Saturacion_Oxigeno").between(SPO2_MIN, SPO2_MAX), "SpO2 fuera de 50–100 % (falla del sensor)")
           .when(repetido, "Registro duplicado"))

marcado = logs.withColumn("motivo_descarte", motivo).cache()
print("Registros descartados por motivo:")
marcado.filter(F.col("motivo_descarte").isNotNull()).groupBy("motivo_descarte").count().show(truncate=False)

logs_limpios = marcado.filter(F.col("motivo_descarte").isNull()).drop("motivo_descarte")
descartados = marcado.filter(F.col("motivo_descarte").isNotNull()).count()
print(f"Leídos: {logs.count():,}  |  Descartados: {descartados:,}  |  Válidos: {logs_limpios.count():,}")
"""),
    md("""
## 4. Query Big Data 2 — Top 5 de alertas clínicas críticas (Spark SQL)

Sobre una **vista temporal** de los logs limpios: los 5 pacientes únicos con más lecturas de
**saturación de oxígeno < 90 %** durante el período de monitoreo.
"""),
    code("""
logs_limpios.createOrReplaceTempView("logs_limpios")

top5 = spark.sql(\"\"\"
    SELECT ID_Paciente,
           COUNT(*)                         AS alertas_criticas,
           MIN(Saturacion_Oxigeno)          AS spo2_minima,
           ROUND(AVG(Saturacion_Oxigeno), 1) AS spo2_promedio
    FROM logs_limpios
    WHERE Saturacion_Oxigeno < 90
    GROUP BY ID_Paciente
    ORDER BY alertas_criticas DESC, ID_Paciente
    LIMIT 5
\"\"\")
top5.show()
"""),
    md("""
## 5. Query Big Data 3 — Clasificación automática de riesgo (UDF lambda)

Una función **UDF lambda** evalúa *simultáneamente* la frecuencia cardíaca y la saturación de
oxígeno, y Spark la ejecuta de forma distribuida para inyectar la columna `Nivel_Riesgo`:

| Nivel | Criterio |
|---|---|
| Crítico | SpO2 < 90 % **o** FC > 130 **o** FC < 40 |
| Moderado | SpO2 90–93 % **o** FC 101–130 **o** FC 40–49 |
| Bajo | todo lo demás |
"""),
    code("""
clasificar_riesgo = F.udf(
    lambda fc, spo2: None if fc is None or spo2 is None else (
        "Crítico" if spo2 < 90 or fc > 130 or fc < 40 else
        "Moderado" if spo2 < 94 or fc > 100 or fc < 50 else
        "Bajo"),
    T.StringType())

logs_refinados = logs_limpios.withColumn(
    "Nivel_Riesgo", clasificar_riesgo(F.col("Frecuencia_Cardiaca"), F.col("Saturacion_Oxigeno")))

logs_refinados.groupBy("Nivel_Riesgo").count().orderBy(F.desc("count")).show()
logs_refinados.filter(F.col("Nivel_Riesgo") == "Crítico").show(5, truncate=False)
"""),
    md("""
## 6. Nivel de riesgo sostenido por paciente

La UDF clasifica **cada lectura**. Para el nivel del paciente no alcanza con una lectura aislada
(un pico puede ser ruido del sensor), así que se mira la proporción durante el período:

| Nivel del paciente | Regla |
|---|---|
| Crítico | al menos el 25 % de sus lecturas fueron críticas |
| Moderado | al menos el 25 % fueron moderadas o críticas |
| Bajo | cualquier otro caso |

En el dashboard este resultado se cruza con la internación de cada paciente y se guarda en
`asignacionCama.nivelRiesgo` (lo único que vuelve a SQL Server; los logs no se guardan).
"""),
    code("""
UMBRAL = 0.25
riesgo_paciente = (logs_refinados.groupBy("ID_Paciente")
    .agg(F.count(F.lit(1)).alias("lecturas"),
         F.sum(F.when(F.col("Nivel_Riesgo") == "Crítico", 1).otherwise(0)).alias("criticas"),
         F.sum(F.when(F.col("Nivel_Riesgo") == "Moderado", 1).otherwise(0)).alias("moderadas"))
    .withColumn("nivelRiesgo",
                F.when(F.col("criticas") / F.col("lecturas") >= UMBRAL, "Crítico")
                 .when((F.col("criticas") + F.col("moderadas")) / F.col("lecturas") >= UMBRAL, "Moderado")
                 .otherwise("Bajo"))
    .orderBy(F.desc("criticas")))

riesgo_paciente.show(truncate=False)
riesgo_paciente.toPandas().to_csv("riesgo_por_paciente.csv", index=False)
"""),
    md("""
## 7. Exportación corporativa a Parquet

El DataFrame refinado (limpio + `Nivel_Riesgo`) se guarda en formato **Parquet** con
`.write.parquet()` y se vuelve a leer para comprobar que el archivo es válido y conserva el esquema.
"""),
    code("""
RUTA_PARQUET = "logs_refinados.parquet"
logs_refinados.write.mode("overwrite").parquet(RUTA_PARQUET)

verificacion = spark.read.parquet(RUTA_PARQUET)
verificacion.printSchema()
print("Registros en el Parquet:", verificacion.count())
!ls -la logs_refinados.parquet
"""),
    md("## Extra: visualización del resultado (pandas + matplotlib)"),
    code("""
import matplotlib.pyplot as plt

riesgo = logs_refinados.groupBy("Nivel_Riesgo").count().toPandas().set_index("Nivel_Riesgo")["count"]
top = top5.toPandas()

fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4))
a1.pie(riesgo, labels=riesgo.index, autopct="%1.0f%%", wedgeprops={"width": 0.4})
a1.set_title("Nivel de riesgo (Query BD 3)")
a2.barh(top["ID_Paciente"][::-1], top["alertas_criticas"][::-1], color="#0f6e66")
a2.set_title("Top 5 alertas críticas (Query BD 2)")
a2.set_xlabel("lecturas con SpO2 < 90 %")
plt.tight_layout()
plt.show()
"""),
    md("""
### Descargar los resultados (opcional, solo en Colab)
Comprime el Parquet y el CSV para entregarlos junto al proyecto.
"""),
    code("""
import shutil
shutil.make_archive("resultados_bigdata", "zip", ".", "logs_refinados.parquet")
try:
    from google.colab import files
    files.download("resultados_bigdata.zip")
    files.download("logs_monitoreo_pacientes.csv")
    files.download("riesgo_por_paciente.csv")
except ImportError:
    print("Fuera de Colab: los archivos quedaron en la carpeta actual.")
"""),
]

notebook = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3"},
                 "language_info": {"name": "python"}, "colab": {"provenance": []}},
    "cells": celdas,
}
destino = AQUI / "HospitalW_BigData.ipynb"
destino.write_text(json.dumps(notebook, ensure_ascii=False, indent=1), encoding="utf-8")
print("Notebook escrito en", destino)
