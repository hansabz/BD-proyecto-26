"""
core/spark.py — La SparkSession y el puente SQL Server -> DataFrame de PySpark.

Conceptos:
  • SparkSession: el "punto de entrada" a Spark. Crearla arranca la JVM y
    tarda ~20 s, por eso se crea UNA sola vez y se reutiliza (patrón singleton).
  • DataFrame de Spark: una tabla distribuida. Las operaciones (filter,
    groupBy, withColumn...) son PEREZOSAS: no se ejecutan hasta que se pide
    un resultado con una "acción" (collect, count, first, toPandas).
  • master("local[2]"): Spark corre en esta misma PC usando 2 núcleos, en vez
    de un clúster. El código es idéntico al que correría en un clúster real.

¿Cómo llegan los datos de SQL Server a Spark?
  1. pyodbc ejecuta el SELECT (core/db.py) -> filas de Python.
  2. Leemos el tipo de cada columna desde el cursor y armamos un esquema
     de Spark (StructType) explícito.
  3. spark.createDataFrame(filas, esquema) -> DataFrame listo para analizar.
  No usamos el conector JDBC de Spark porque exige un .jar extra y usuario/
  contraseña de SQL; con este puente basta la autenticación de Windows.
"""
import datetime
import decimal
import threading

from core import config, db

# Hay que preparar JAVA_HOME / SPARK_HOME ANTES de importar pyspark.
config.preparar_entorno_spark()

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

_spark: SparkSession | None = None
_lock = threading.Lock()   # Flask atiende varios pedidos a la vez: que solo uno cree la sesión


def obtener_spark() -> SparkSession:
    """Devuelve la SparkSession compartida, creándola la primera vez."""
    global _spark
    with _lock:
        if _spark is None:
            _spark = (
                SparkSession.builder
                .master("local[2]")                                   # Spark local, 2 núcleos
                .appName("HospitalW-Dashboard")
                .config("spark.ui.enabled", "false")                  # sin la web de monitoreo de Spark
                .config("spark.sql.shuffle.partitions", "4")          # pocos datos -> pocas particiones (más rápido)
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .config("spark.sql.session.timeZone", "America/Guatemala")
                .config("spark.driver.memory", "1g")
                .getOrCreate()
            )
            _spark.sparkContext.setLogLevel("ERROR")                 # menos ruido en la consola
        return _spark


def precalentar() -> None:
    """
    Arranca Spark en un hilo aparte apenas inicia la app, así el primer clic
    en "Filtrar" o "Limpiar" no espera los ~20 s de arranque de la JVM.
    """
    threading.Thread(target=lambda: obtener_spark().range(1).count(), daemon=True).start()


def spark_listo() -> bool:
    """Lo consulta el navegador para mostrar "PySpark listo" en la barra lateral."""
    return _spark is not None


# Tipo que devuelve pyodbc para cada columna -> (tipo de Spark, conversión del valor).
# Ej.: SQL dec(7,2) llega como decimal.Decimal -> lo pasamos a float (DoubleType).
_MAPEO_TIPOS = {
    str: (T.StringType(), None),
    int: (T.LongType(), None),
    bool: (T.BooleanType(), None),               # columnas bit (activo, critico)
    float: (T.DoubleType(), None),
    decimal.Decimal: (T.DoubleType(), float),    # salario, costoUnitario
    datetime.datetime: (T.TimestampType(), None),
    datetime.date: (T.DateType(), None),         # fechaNac
    datetime.time: (T.StringType(), lambda t: t.strftime("%H:%M")),  # hora de cita
}


def leer_sql(sql: str, params: tuple = ()) -> DataFrame:
    """Ejecuta un SELECT en SQL Server y devuelve el resultado como DataFrame de Spark."""
    descripcion, filas = db.consultar_crudo(sql, params)

    # 1) Esquema explícito: una StructField por columna, con su tipo de Spark.
    #    (Si dejáramos que Spark lo adivine, falla con columnas que vienen todas en NULL.)
    campos, conversiones = [], []
    for col in descripcion:                  # col[0] = nombre, col[1] = tipo de Python
        tipo_spark, conv = _MAPEO_TIPOS.get(col[1], (T.StringType(), str))
        campos.append(T.StructField(col[0], tipo_spark, True))   # True = admite NULL
        conversiones.append(conv)

    # 2) Convertimos los valores que lo necesitan (Decimal -> float, time -> "HH:MM").
    datos = [
        tuple(conv(v) if (conv and v is not None) else v for v, conv in zip(fila, conversiones))
        for fila in filas
    ]
    # 3) Filas + esquema -> DataFrame distribuido.
    return obtener_spark().createDataFrame(datos, T.StructType(campos))


def leer_tabla(tabla: str) -> DataFrame:
    return leer_sql(f"SELECT * FROM {tabla}")


def a_registros(df: DataFrame) -> list[dict]:
    """
    DataFrame de Spark -> lista de dicts para mandar como JSON al navegador.
    collect() es la "acción" que dispara todo el cálculo pendiente de Spark.
    """
    salida = []
    for fila in df.collect():
        registro = {}
        for clave, valor in fila.asDict().items():
            if isinstance(valor, datetime.datetime):
                valor = valor.strftime("%Y-%m-%d %H:%M")
            elif isinstance(valor, datetime.date):
                valor = valor.isoformat()
            registro[clave] = valor
        salida.append(registro)
    return salida
