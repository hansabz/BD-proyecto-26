"""
areas/ — Un módulo por área del hospital. Los 4 tienen la MISMA forma, así
app.py puede atenderlos con las mismas rutas genéricas (/api/<area>/...):

    TITULO        nombre que se muestra en la página
    TABLAS        tablas de SQL Server que, si cambian, refrescan la vista "en vivo"
    BASE_SQL      el SELECT (con JOINs) que alimenta la tabla del dashboard
    COLUMNAS      qué columnas muestra la tabla y con qué formato (id, moneda, fecha...)

    tabla()       filas actuales (SQL Server directo, rápido)       -> tabla en vivo
    metricas()    tarjetas de KPIs de arriba de la página
    dataset()     el mismo BASE_SQL pero como DataFrame de PySpark   -> filtros / gráficas
    filtros()     qué filtros ofrece el botón "Filtrar" (ver core/filtros.py)
    limpiar()     (solo Telemetría) Query BD 1 para el botón "Limpiar datos"
    GRAFICOS      gráficas disponibles en el radio "Gráfico" (ver core/graficos.py)

    + las funciones del CRUD propias de cada área (crear, actualizar, baja, admitir,
      asignar cama, registrar movimiento...), que validan y escriben con
      transacciones de core/db.py.
"""
from areas import hospitalizacion, inventario, pacientes, personal, telemetria

# El nombre de la clave es el que aparece en la URL: /personal, /api/personal/tabla, etc.
AREAS = {
    "personal": personal,
    "pacientes": pacientes,
    "hospitalizacion": hospitalizacion,
    "inventario": inventario,
    "telemetria": telemetria,     # Fase II: lee un CSV con PySpark, no SQL Server
}
