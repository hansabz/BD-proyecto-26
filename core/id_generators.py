"""
core/id_generators.py — Generadores de ID con PySpark.

Reglas:
    Médico      -> M1001, M1002, ...
    Enfermera   -> E1001, E1002, ...   (también "Enfermera Jefe")
    Auxiliar    -> A1001, A1002, ...
    Paciente    -> PAC-0001, PAC-0002, ...
    Ingreso     -> ING-001, ING-002, ...
    Suministro  -> Q001 / F001 / M001 según el tipo (quirúrgico/farmacéutico/material)
    Proveedor   -> PR001, PR002, ...
    Cita        -> 101, 102, ...

Cómo funciona para un empleado (ejemplo: alta de una "Enfermera Jefe"):
  1. extraer_rol("EJF")
       Spark lee la tabla puesto, filtra el código EJF y transforma el nombre
       con funciones NATIVAS de Spark:  "Enfermera Jefe"
         trim       -> "Enfermera Jefe"
         lower      -> "enfermera jefe"
         translate  -> quita tildes ("médico" -> "medico")
         split[0]   -> "enfermera"          (primera palabra = el rol)
  2. prefijo_por_rol("enfermera")  -> un `match` (el switch de Python) -> "E"
  3. _siguiente_numero(...)
       Spark lee los numeroEmpleado, se queda con los que empiezan con E
       seguidos de dígitos (rlike), extrae la parte numérica (regexp_extract),
       la pasa a entero (cast) y calcula el máximo (max): E1007 -> 1007.
       Siguiente = 1007 + 1 = 1008.
  4. Resultado: "E1008"
"""
import threading

from pyspark.sql import functions as F

from core.db import ErrorNegocio
from core.spark import leer_sql

# Si dos personas registran a la vez, las dos podrían calcular "E1008".
# Los servicios toman este candado mientras generan el ID E insertan,
# así el segundo espera a que el primero termine. (RLock = el mismo hilo
# puede tomarlo dos veces sin trabarse.)
LOCK_IDS = threading.RLock()

# translate() reemplaza carácter por carácter: á->a, é->e, ...
_TILDES, _SIN_TILDES = "áéíóúÁÉÍÓÚ", "aeiouAEIOU"


def _siguiente_numero(tabla: str, columna: str, patron: str, inicio: int) -> int:
    """
    Busca en `tabla.columna` los IDs que cumplen `patron` y devuelve max + 1.
    El patrón tiene UN grupo de captura (\\d+) con la parte numérica.
    Si todavía no hay ninguno, devuelve `inicio` (ej. 1001 para empleados).
    """
    df = leer_sql(f"SELECT {columna} AS id FROM {tabla}")
    maximo = (
        df.filter(F.col("id").rlike(patron))                  # solo IDs con este prefijo
        .select(
            F.max(                                             # el mayor...
                F.regexp_extract("id", patron, 1)              # ...de la parte numérica ("1007")
                .cast("int")                                   # ...convertida a número (1007)
            ).alias("maximo")
        )
        .first()["maximo"]                                     # first() = acción: ejecuta el cálculo
    )
    return inicio if maximo is None else maximo + 1


def extraer_rol(codigo_puesto: str) -> str:
    """
    Lee el nombre del puesto en Spark y lo normaliza a una sola palabra:
    "  Médico " -> "medico", "Enfermera Jefe" -> "enfermera".
    """
    puestos = leer_sql("SELECT codigoPuesto, nombre FROM puesto")
    fila = (
        puestos.filter(F.col("codigoPuesto") == codigo_puesto)
        .select(
            F.split(                                           # 4) partir por espacios...
                F.translate(                                   # 3) quitar tildes
                    F.lower(                                   # 2) minúsculas
                        F.trim(F.col("nombre"))                # 1) sacar espacios de los bordes
                    ),
                    _TILDES, _SIN_TILDES,
                ),
                r"\s+",
            ).getItem(0).alias("rol")                          # ...y quedarse con la primera palabra
        )
        .first()
    )
    if fila is None:
        raise ErrorNegocio(f"El puesto '{codigo_puesto}' no existe en el catálogo.")
    return fila["rol"]


def prefijo_por_rol(rol: str) -> str:
    """El "switch": según el rol extraído por Spark, decide la letra del ID."""
    match rol:
        case "medico" | "medica" | "doctor" | "doctora":
            return "M"
        case "enfermera" | "enfermero":
            return "E"
        case "auxiliar":
            return "A"
        case _:                       # cualquier otro puesto: no hay regla definida
            raise ErrorNegocio(
                f"El puesto '{rol}' no tiene una regla de ID (solo médico, enfermera o auxiliar)."
            )


def generar_id_empleado(codigo_puesto: str) -> str:
    prefijo = prefijo_por_rol(extraer_rol(codigo_puesto))
    # rf"^{prefijo}(\d+)$" con prefijo "M" queda ^M(\d+)$ : "M" + solo dígitos hasta el final.
    numero = _siguiente_numero("empleado", "numeroEmpleado", rf"^{prefijo}(\d+)$", 1001)
    return f"{prefijo}{numero}"


def generar_id_paciente() -> str:
    numero = _siguiente_numero("paciente", "numeroPaciente", r"^PAC-(\d+)$", 1)
    return f"PAC-{numero:04d}"          # :04d = rellenar con ceros a 4 dígitos -> PAC-0044


def generar_id_ingreso() -> str:
    numero = _siguiente_numero("registroIngreso", "numeroIngreso", r"^ING-(\d+)$", 1)
    return f"ING-{numero:03d}"


def prefijo_por_tipo_suministro(tipo: str) -> str:
    """Mismo patrón con match para los insumos: el tipo decide la letra del código."""
    match tipo.strip().upper():
        case "Q" | "QUIRURGICO":
            return "Q"
        case "F" | "FARMACEUTICO":
            return "F"
        case "M" | "MATERIAL":
            return "M"
        case _:
            raise ErrorNegocio("Tipo de suministro inválido (Q, F o M).")


def generar_id_suministro(tipo: str) -> str:
    prefijo = prefijo_por_tipo_suministro(tipo)
    numero = _siguiente_numero("suministro", "codigoSuministro", rf"^{prefijo}(\d+)$", 1)
    return f"{prefijo}{numero:03d}"


def generar_id_proveedor() -> str:
    numero = _siguiente_numero("proveedor", "codigoProveedor", r"^PR(\d+)$", 1)
    return f"PR{numero:03d}"


def generar_id_cita() -> str:
    numero = _siguiente_numero("informeCita", "numeroCita", r"^(\d+)$", 101)
    return str(numero)
