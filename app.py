"""
app.py — Servidor web (Flask). Es la "puerta de entrada": recibe los pedidos
del navegador, llama a la función del área que corresponde y devuelve
HTML (las páginas) o JSON (los datos).

Recorrido de un clic, por ejemplo "Filtrar" en Personal:
    navegador  --POST /api/personal/filtrar {filtros}-->  api_filtrar()
    api_filtrar -> personal.dataset()   (SQL Server -> DataFrame de Spark)
                -> filtros.aplicar()    (filter de PySpark)
                -> spark.a_registros()  (collect -> lista de dicts)
    navegador  <--JSON {registros, activos}--  y JavaScript redibuja la tabla

Hospital W 2.0 — Dashboard administrativo.

    python app.py      ->  http://127.0.0.1:5000

Rutas genéricas (las mismas para las 4 áreas):
    GET  /api/<area>/tabla            tabla en vivo + métricas (SQL Server)
    GET  /api/<area>/firma            huella de los datos: cambia si alguien modificó algo
    POST /api/<area>/filtrar          filtros aplicados en PySpark
    POST /api/<area>/limpiar          Query Big Data 1 — solo en Telemetría (logs de los sensores)
    GET  /api/<area>/grafico/<id>     PNG con pandas + matplotlib (respeta los filtros)
"""
import logging
import time

from flask import Flask, Response, abort, jsonify, render_template, request

from areas import AREAS, hospitalizacion, inventario, pacientes, personal, telemetria
from core import config, db, filtros, graficos, spark

app = Flask(__name__)
app.json.ensure_ascii = False
app.json.sort_keys = False
log = logging.getLogger("hospital")


# ── Errores ──────────────────────────────────────────────────────────
# Cualquier ErrorNegocio que se lance en cualquier parte termina acá y se
# convierte en una respuesta 400 con el mensaje; el JavaScript la muestra
# debajo del campo correspondiente o como aviso.
@app.errorhandler(db.ErrorNegocio)
def error_negocio(exc):
    detalle = exc.args[0] if exc.args else "Operación inválida."
    if isinstance(detalle, dict):
        return jsonify({"error": "Revisá los campos marcados.", "campos": detalle}), 400
    return jsonify({"error": str(detalle)}), 400


@app.errorhandler(Exception)
def error_general(exc):
    if hasattr(exc, "code") and isinstance(getattr(exc, "code"), int):  # errores HTTP de Flask (404, etc.)
        return exc
    log.exception("Error no controlado")
    return jsonify({"error": f"Error interno: {exc}"}), 500


def _area(nombre):
    """"personal" -> el módulo areas/personal.py (o 404 si no existe)."""
    modulo = AREAS.get(nombre)
    if not modulo:
        abort(404)
    return modulo


def _firma(tablas: list[str]) -> str:
    """
    "Huella digital" de los datos: para cada tabla, COUNT(*) + CHECKSUM_AGG
    (un número que cambia si cambia cualquier fila). El navegador la pide cada
    pocos segundos; si es distinta a la anterior, recarga la tabla. Así la
    tabla es en tiempo real sin traer todos los datos en cada consulta.
    """
    partes = ", ".join(
        f"(SELECT CAST(COUNT(*) AS varchar) + ':' + ISNULL(CAST(CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS varchar), '0')"
        f" FROM {t})" for t in tablas)
    fila = db.consultar_crudo(f"SELECT {partes}")[1][0]
    return "|".join(str(v) for v in fila)


def _parametros() -> dict:
    datos = request.get_json(silent=True) or {}
    return datos.get("filtros") or {k: v for k, v in request.args.items() if k != "limpio"}


def _usar_limpio() -> bool:
    datos = request.get_json(silent=True) or {}
    return bool(datos.get("limpio")) or request.args.get("limpio") == "1"


def _dataset_spark(modulo):
    # Usado por "Filtrar" y por las gráficas: si el usuario apretó "Limpiar datos"
    # (solo existe en Telemetría), se filtra/grafica sobre las lecturas válidas.
    """DataFrame del área: crudo o ya limpio, con los filtros aplicados."""
    df = modulo.dataset()
    if _usar_limpio():
        df = modulo.datos_limpios(df)
    return filtros.aplicar(df, modulo.filtros(), _parametros())


# ── Páginas HTML (templates/) ────────────────────────────────────────
@app.context_processor
def contexto_global():
    return {"intervalo_ms": config.INTERVALO_TIEMPO_REAL_MS}


@app.route("/")
def inicio():
    return render_template("inicio.html", activo="inicio")


@app.route("/<area>")
def pagina_area(area):
    # Le pasamos a la página la configuración del área (columnas, filtros, gráficas);
    # static/js/app.js arma la tabla, el formulario de filtros y los radios con eso.
    modulo = _area(area)
    return render_template(
        f"{area}.html", activo=area, area=area, titulo=modulo.TITULO,
        config_area={
            "area": area,
            "columnas": modulo.COLUMNAS,
            "filtros": modulo.filtros(),
            "graficos": [{"id": k, "titulo": v["titulo"], "tipo": v["tipo"]} for k, v in modulo.GRAFICOS.items()],
            # El botón "Limpiar datos" solo aparece en las áreas con limpieza propia (Telemetría: Query BD 1).
            "limpieza": hasattr(modulo, "limpiar"),
        },
    )


# ── API genérica: las mismas 5 rutas sirven a las 4 áreas ────────────
def _firma_area(modulo) -> str:
    # Telemetría no vive en SQL Server: su firma es la fecha/tamaño del archivo CSV.
    return modulo.firma() if hasattr(modulo, "firma") else _firma(modulo.TABLAS)


def _limite(modulo):
    """Áreas con muchísimas filas (telemetría) mandan al navegador solo las primeras N."""
    return getattr(modulo, "LIMITE_FILAS", None)


@app.get("/api/<area>/tabla")
def api_tabla(area):
    modulo = _area(area)
    registros = modulo.tabla()
    total = modulo.total_registros() if hasattr(modulo, "total_registros") else len(registros)
    return jsonify({"registros": registros, "total": total, "metricas": modulo.metricas(),
                    "firma": _firma_area(modulo), "spark": spark.spark_listo()})


@app.get("/api/<area>/firma")
def api_firma(area):
    return jsonify({"firma": _firma_area(_area(area)), "spark": spark.spark_listo()})


@app.post("/api/<area>/filtrar")
def api_filtrar(area):
    inicio_t = time.perf_counter()
    modulo = _area(area)
    df, activos = _dataset_spark(modulo)
    limite = _limite(modulo)
    if limite:
        total = df.count()                      # cuántos cumplen el filtro (todos)
        registros = spark.a_registros(df.limit(limite))   # pero solo se envían los primeros N
    else:
        registros = spark.a_registros(df)
        total = len(registros)
    return jsonify({"registros": registros, "activos": activos, "total": total,
                    "segundos": round(time.perf_counter() - inicio_t, 2)})


@app.post("/api/<area>/limpiar")
def api_limpiar(area):
    inicio_t = time.perf_counter()
    modulo = _area(area)
    if not hasattr(modulo, "limpiar"):
        raise db.ErrorNegocio("Esta vista no tiene limpieza de datos: la Query BD 1 es sobre los logs de telemetría.")
    resultado = modulo.limpiar()
    resultado["segundos"] = round(time.perf_counter() - inicio_t, 2)
    return jsonify(resultado)


@app.get("/api/<area>/grafico/<grafico>")
def api_grafico(area, grafico):
    modulo = _area(area)
    definicion = modulo.GRAFICOS.get(grafico) or abort(404)
    df, _ = _dataset_spark(modulo)
    figura = definicion["fn"](df)
    return Response(graficos.a_png(figura), mimetype="image/png", headers={"Cache-Control": "no-store"})


# ── Rutas propias de cada CRUD ───────────────────────────────────────
# Cada una solo traduce HTTP <-> función del área; la lógica está en areas/*.py.
# (Van con prefijos como /empleado/ para no chocar con /api/<area>/tabla.)

# ── Personal (RF1) ───────────────────────────────────────────────────
@app.get("/api/personal/catalogos")
def personal_catalogos():
    return jsonify(personal.catalogos())


@app.get("/api/personal/asignacion")
def personal_asignacion():
    return jsonify(personal.asignacion_por_sala(request.args.get("sala")))


@app.get("/api/personal/empleado/<numero>")
def personal_obtener(numero):
    return jsonify(personal.obtener(numero))


@app.post("/api/personal")
def personal_crear():
    return jsonify(personal.crear(request.get_json() or {})), 201


@app.put("/api/personal/empleado/<numero>")
def personal_actualizar(numero):
    return jsonify(personal.actualizar(numero, request.get_json() or {}))


@app.post("/api/personal/empleado/<numero>/baja")
def personal_baja(numero):
    return jsonify(personal.dar_de_baja(numero))


@app.post("/api/personal/empleado/<numero>/reactivar")
def personal_reactivar(numero):
    return jsonify(personal.reactivar(numero))


# ── Pacientes (RF2) ──────────────────────────────────────────────────
@app.get("/api/pacientes/catalogos")
def pacientes_catalogos():
    return jsonify(pacientes.catalogos())


@app.get("/api/pacientes/expediente/<numero>")
def pacientes_ficha(numero):
    return jsonify(pacientes.ficha(numero))


@app.post("/api/pacientes")
def pacientes_admitir():
    return jsonify(pacientes.admitir(request.get_json() or {})), 201


@app.put("/api/pacientes/expediente/<numero>")
def pacientes_actualizar(numero):
    return jsonify(pacientes.actualizar(numero, request.get_json() or {}))


@app.post("/api/pacientes/expediente/<numero>/citas")
def pacientes_cita(numero):
    return jsonify(pacientes.agendar_cita(numero, request.get_json() or {})), 201


# ── Hospitalización (RF3) ────────────────────────────────────────────
@app.get("/api/hospitalizacion/camas")
def hosp_camas():
    return jsonify({"camas": hospitalizacion.mapa_camas(), "pendientes": hospitalizacion.pendientes()})


@app.post("/api/hospitalizacion/asignar")
def hosp_asignar():
    return jsonify(hospitalizacion.asignar(request.get_json() or {})), 201


@app.post("/api/hospitalizacion/ingreso/<ingreso>/alta")
def hosp_alta(ingreso):
    return jsonify(hospitalizacion.dar_alta(ingreso))


# ── Inventario (RF4) ─────────────────────────────────────────────────
@app.get("/api/inventario/catalogos")
def inv_catalogos():
    return jsonify(inventario.catalogos())


@app.get("/api/inventario/critico")
def inv_critico():
    return jsonify(inventario.stock_critico())


@app.get("/api/inventario/movimientos")
def inv_movimientos():
    return jsonify(inventario.movimientos_recientes())


@app.get("/api/inventario/ubicacion/<codigo>")
def inv_ubicacion(codigo):
    return jsonify(inventario.informe_por_ubicacion(codigo))


@app.post("/api/inventario/suministros")
def inv_crear():
    return jsonify(inventario.crear_suministro(request.get_json() or {})), 201


@app.put("/api/inventario/suministros/<codigo>")
def inv_actualizar(codigo):
    return jsonify(inventario.actualizar_suministro(codigo, request.get_json() or {}))


@app.delete("/api/inventario/suministros/<codigo>")
def inv_eliminar(codigo):
    return jsonify(inventario.eliminar_suministro(codigo))


@app.post("/api/inventario/movimientos")
def inv_movimiento():
    return jsonify(inventario.registrar_movimiento(request.get_json() or {})), 201


@app.post("/api/inventario/proveedores")
def inv_proveedor_crear():
    return jsonify(inventario.crear_proveedor(request.get_json() or {})), 201


@app.put("/api/inventario/proveedores/<codigo>")
def inv_proveedor_actualizar(codigo):
    return jsonify(inventario.actualizar_proveedor(codigo, request.get_json() or {}))


# ── Telemetría · Big Data (Fase II) ──────────────────────────────────
@app.post("/api/telemetria/generar")
def tel_generar():
    datos = request.get_json(silent=True) or {}
    try:
        registros = int(datos.get("registros") or 6000)
    except ValueError:
        raise db.ErrorNegocio({"registros": "Número entero."})
    return jsonify(telemetria.generar(registros))


@app.get("/api/telemetria/bd1")
def tel_bd1():
    return jsonify(telemetria.paso_bd1())


@app.get("/api/telemetria/bd2")
def tel_bd2():
    return jsonify(telemetria.paso_bd2())


@app.get("/api/telemetria/bd3")
def tel_bd3():
    return jsonify(telemetria.paso_bd3())


@app.post("/api/telemetria/riesgo")
def tel_riesgo():
    return jsonify(telemetria.paso_riesgo())


@app.post("/api/telemetria/parquet")
def tel_parquet():
    return jsonify(telemetria.paso_parquet())


# ── Panel general ────────────────────────────────────────────────────
@app.get("/api/resumen")
def resumen():
    return jsonify({
        "areas": {nombre: modulo.metricas() for nombre, modulo in AREAS.items()},
        "critico": inventario.stock_critico()[:6],
        "camas": hospitalizacion.mapa_camas(),
        "pendientes": hospitalizacion.pendientes(),
        "citas": db.consultar("""
            SELECT TOP 6 ic.numeroCita, ic.fecha, CONVERT(varchar(5), ic.hora, 108) AS hora,
                   p.nombres + ' ' + p.apellidos AS paciente, e.nombres + ' ' + e.apellidos AS medico,
                   es.nombre AS especialidad
            FROM informeCita ic
            JOIN paciente p ON p.numeroPaciente = ic.numeroPaciente
            JOIN empleado e ON e.numeroEmpleado = ic.numeroEmpleado
            JOIN especialidad es ON es.codigoEspecialidad = ic.codigoEspecialidad
            WHERE ic.fecha >= CAST(GETDATE() AS date)
            ORDER BY ic.fecha, ic.hora"""),
        "spark": spark.spark_listo(),
        "firma": _firma(sorted({t for m in AREAS.values() for t in m.TABLAS})),
    })


@app.get("/api/resumen/firma")
def resumen_firma():
    return jsonify({"firma": _firma(sorted({t for m in AREAS.values() for t in m.TABLAS})),
                    "spark": spark.spark_listo()})


# Esto corre solo al ejecutar "python app.py" (no al importarlo).
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark.precalentar()
    print("Hospital W 2.0 → http://127.0.0.1:5000  (Spark se inicia en segundo plano, ~20 s)")
    app.run(debug=False, threaded=True, port=5000)
