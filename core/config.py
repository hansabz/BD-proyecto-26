r"""
core/config.py — Configuración central del proyecto.

Todo lo que puede cambiar entre una computadora y otra (nombre del servidor
SQL, usuario, ruta de Java) vive acá. Cada valor se puede sobreescribir con
una variable de entorno, así cada integrante del grupo puede usar su propia
instancia sin tocar el código. Ejemplo en PowerShell:

    $env:HOSPITAL_DB_SERVER = "MI-PC\SQLEXPRESS"
    python app.py
"""
import os
import sys
from pathlib import Path

# Carpeta raíz del proyecto ("version claude"). Se usa para encontrar sql/.
BASE_DIR = Path(__file__).resolve().parent.parent

# ── SQL Server ────────────────────────────────────────────────────────
# os.getenv("NOMBRE", valor_por_defecto): si la variable de entorno existe
# se usa esa; si no, el valor por defecto.
DB_SERVER = os.getenv("HOSPITAL_DB_SERVER", r"localhost\SQLEXPRESS")
DB_NAME = os.getenv("HOSPITAL_DB_NAME", "hospitalProyecto")
# Si no se definen usuario/contraseña se usa autenticación de Windows
# (la misma con la que entrás a SQL Server Management Studio).
DB_USER = os.getenv("HOSPITAL_DB_USER", "")
DB_PASSWORD = os.getenv("HOSPITAL_DB_PASSWORD", "")

# ── Spark ─────────────────────────────────────────────────────────────
# PySpark es una librería de Python que por dentro arranca un proceso de
# Java (la JVM). Spark 4 necesita Java 17 o 21: con Java 23+ falla la parte
# de Hadoop en Windows. Si JAVA_HOME no está definido, buscamos un JDK
# compatible en las rutas de instalación habituales.
_JDK_CANDIDATOS = [
    r"C:\Program Files\Java\jdk-21",
    r"C:\Program Files\Java\jdk-17",
    r"C:\Program Files\Eclipse Adoptium\jdk-21",
    r"C:\Program Files\Eclipse Adoptium\jdk-17",
]


def preparar_entorno_spark() -> None:
    """Se llama UNA vez, antes de importar pyspark (ver core/spark.py)."""
    if not os.environ.get("JAVA_HOME"):
        for ruta in _JDK_CANDIDATOS:
            if Path(ruta).exists():
                os.environ["JAVA_HOME"] = ruta
                break
    # Spark lanza "workers" de Python aparte; les decimos que usen el mismo
    # intérprete del entorno virtual (.venv) que está corriendo la app.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # Le indicamos a Spark dónde está instalado (dentro del paquete pyspark).
    # Sin esto, un script .cmd de Spark intenta averiguarlo solo y se confunde
    # con el espacio de "BD CRUD 2026" (de ahí el aviso "CRUD no se reconoce").
    if not os.environ.get("SPARK_HOME"):
        import importlib.util
        especificacion = importlib.util.find_spec("pyspark")
        if especificacion and especificacion.origin:
            os.environ["SPARK_HOME"] = str(Path(especificacion.origin).parent)


# Cada cuántos milisegundos el navegador pregunta "¿cambió algo?" para que la
# tabla sea "en tiempo real" (ver /api/<area>/firma en app.py).
INTERVALO_TIEMPO_REAL_MS = int(os.getenv("HOSPITAL_INTERVALO_MS", "4000"))

# Umbral de Query 3: un insumo es "crítico" si tiene menos del 15 % de su capacidad.
UMBRAL_STOCK_CRITICO = 0.15
