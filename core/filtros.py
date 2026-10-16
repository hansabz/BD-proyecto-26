"""
core/filtros.py — Motor de filtros en PySpark (botón "Filtrar" de cada vista).

Cada área declara sus filtros como una lista de diccionarios; el dashboard
dibuja el formulario a partir de esa misma lista y este módulo los aplica
sobre el DataFrame. Para agregar un filtro nuevo basta con sumar una
entrada en el FILTROS del área — no hay que tocar HTML ni JavaScript.

Tipos soportados:
    texto        busca (contiene, sin distinguir mayúsculas) en varias columnas
    opcion       igualdad exacta contra una lista de opciones
    rango        número mínimo / máximo   (params: <id>_min, <id>_max)
    fechas       fecha desde / hasta       (params: <id>_desde, <id>_hasta)
    booleano     sí / no

Ejemplo: el usuario elige Puesto = "Médico" y escribe "pérez" en Buscar.
El navegador manda {"codigoPuesto": "MED", "texto": "pérez"} y acá se arma:
    df.filter(lower(numeroEmpleado) contiene "pérez" OR lower(nombres) contiene ... )
      .filter(codigoPuesto == "MED")
Spark recién ejecuta todo eso cuando app.py hace collect().
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _num(valor):
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def aplicar(df: DataFrame, definiciones: list[dict], params: dict) -> tuple[DataFrame, list[str]]:
    """
    definiciones: la lista FILTROS del área (qué filtros existen).
    params:       lo que el usuario completó en el formulario.
    Devuelve el DataFrame filtrado y textos legibles de los filtros activos
    (se muestran como "chips" arriba de la tabla).
    """
    activos = []
    for f in definiciones:
        ident, col = f["id"], f.get("columna", f["id"])   # por defecto, el id del filtro ES la columna
        match f["tipo"]:                                   # un "switch" por tipo de filtro
            case "texto":
                valor = (params.get(ident) or "").strip().lower()
                if valor:
                    columnas = f.get("columnas", [col])
                    # OR entre todas las columnas: basta que UNA contenga el texto
                    cond = F.lit(False)
                    for c in columnas:
                        cond = cond | F.lower(F.col(c).cast("string")).contains(valor)
                    df = df.filter(cond)
                    activos.append(f"{f['etiqueta']}: «{valor}»")
            case "opcion":
                valor = params.get(ident)
                if valor not in (None, ""):
                    df = df.filter(F.col(col) == valor)
                    etiqueta = dict((str(o[0]), o[1]) for o in f.get("opciones", [])).get(str(valor), valor)
                    activos.append(f"{f['etiqueta']}: {etiqueta}")
            case "booleano":
                valor = params.get(ident)
                if valor in ("1", "0"):
                    df = df.filter(F.col(col) == (valor == "1"))
                    activos.append(f"{f['etiqueta']}: {'Sí' if valor == '1' else 'No'}")
            case "rango":
                minimo, maximo = _num(params.get(f"{ident}_min")), _num(params.get(f"{ident}_max"))
                if minimo is not None:
                    df = df.filter(F.col(col) >= minimo)
                if maximo is not None:
                    df = df.filter(F.col(col) <= maximo)
                if minimo is not None or maximo is not None:
                    activos.append(f"{f['etiqueta']}: {minimo if minimo is not None else '…'}"
                                   f" – {maximo if maximo is not None else '…'}")
            case "fechas":
                desde, hasta = params.get(f"{ident}_desde"), params.get(f"{ident}_hasta")
                if desde:                      # to_date: compara solo la fecha, ignora la hora
                    df = df.filter(F.to_date(F.col(col)) >= F.to_date(F.lit(desde)))
                if hasta:
                    df = df.filter(F.to_date(F.col(col)) <= F.to_date(F.lit(hasta)))
                if desde or hasta:
                    activos.append(f"{f['etiqueta']}: {desde or '…'} → {hasta or '…'}")
    return df, activos
