# Hospital W 2.0 — Dashboard administrativo

Proyecto final de **Base de Datos I** (Universidad Galileo, ciclo 2S26): sistema CRUD web
para el Hospital W con base de datos transaccional en **SQL Server** y procesamiento de
Big Data con **PySpark**. La interfaz es HTML/CSS y las gráficas salen de **pandas + matplotlib**.

> 📘 Manual de uso y explicación del funcionamiento: [`docs/Manual_HospitalW.docx`](docs/Manual_HospitalW.docx)

## Qué hace

| Área | Qué permite |
|---|---|
| **Personal** (RF1) | Alta, modificación, consulta y baja lógica. NSS único; ID generado por rol (M1001 / E1001 / A1001) con PySpark |
| **Admisión** (RF2) | Ficha de ingreso del paciente (Figura 2), expediente e historial de citas (Query 2) |
| **Hospitalización** (RF3) | Mapa de camas, internación sin choques de cama, fechas automáticas, nivel de riesgo |
| **Inventario** (RF4) | Stock por ubicación, entradas, salidas a un empleado, traslados entre almacenes, alerta de stock crítico (Query 3) |
| **Telemetría** (Fase II) | Logs masivos de sensores: limpieza (BD 1), top 5 de alertas (BD 2), riesgo con UDF (BD 3), Parquet |

Cada vista tiene una **tabla en tiempo real**, botón **Filtrar** (PySpark) y radio
**Tabla / Gráfico** para verla en forma gráfica.

## Cómo empezar

### Opción A — Solo la base de datos (SQL Server Management Studio)

Para revisar el modelo, las vistas y las queries sin instalar nada más:

1. En SSMS: `CREATE DATABASE hospitalProyecto;`
2. Con la base `hospitalProyecto` seleccionada, ejecutar en orden:
   1. `sql/01_esquema.sql` — tablas
   2. `sql/02_indices_vistas.sql` — índices, Query 1 (vista), Query 2 (procedimiento) y Query 3 (vista)
   3. `sql/03_datos_prueba.sql` — datos de prueba
3. Probar las queries:
   ```sql
   SELECT * FROM VW_Asignacion_Personal WHERE codigoSala = 'S01';   -- Query 1 (Figura 1)
   EXEC SP_HistorialCitas @ID_Paciente = 'PAC-0001';                 -- Query 2 (Figura 4)
   SELECT * FROM VW_Stock_Critico ORDER BY porcentaje;               -- Query 3
   ```
4. Opcional: `sql/04_prueba_indices.sql` mide el efecto del índice agrupado.

`sql/00_script_original_comentado.sql` es el script original del grupo, explicado línea por línea.

### Opción B — La aplicación completa

Requisitos (Windows):

| Programa | Versión | Notas |
|---|---|---|
| Python | 3.11 o superior | probado con 3.13 |
| Java (JDK) | **17 o 21** | Spark 4 **no** funciona con Java 23 o superior |
| SQL Server | Express o Developer | con autenticación de Windows |
| ODBC Driver for SQL Server | 17 o 18 | viene con SSMS; si no, se baja de Microsoft |

```powershell
git clone https://github.com/hansabz/BD-proyecto-26.git
cd BD-proyecto-26
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\python setup_db.py --reset   # crea hospitalProyecto + datos + logs + nivel de riesgo (¡BORRA la base si ya existía!)
.\.venv\Scripts\python app.py                # abrir http://127.0.0.1:5000
```

La primera acción con PySpark tarda ~20 s (arranca la JVM); la barra lateral muestra
"PySpark listo" cuando termina.

**Si tu SQL Server no se llama `localhost\SQLEXPRESS`** (o usas usuario y contraseña):

```powershell
$env:HOSPITAL_DB_SERVER = "MI-PC\MI-INSTANCIA"     # o "localhost" para la instancia por defecto
$env:HOSPITAL_DB_USER = "sa"; $env:HOSPITAL_DB_PASSWORD = "..."   # solo si no usas autenticación de Windows
```

Si Java no está en `C:\Program Files\Java\jdk-21` o `jdk-17`, definir `JAVA_HOME` apuntando al JDK.

### Fase II en la nube (Google Colab)

Subir `bigdata/HospitalW_BigData.ipynb` a Google Colab (*Archivo → Subir notebook*) y
*Entorno de ejecución → Ejecutar todas*. Es autosuficiente: genera sus propios datos.

### Problemas conocidos

- **Parquet en Windows:** Spark necesita `winutils.exe` de Hadoop para escribir archivos en
  Windows. Sin él, la app exporta el mismo Parquet con `pyarrow` y lo indica en pantalla.
  En Colab se usa `.write.parquet()` directamente.
- **`setup_db.py --reset` borra la base** y la vuelve a crear. No usarlo si cargaron datos a mano.
- El aviso `"CRUD" no se reconoce como un comando` en la consola es inofensivo.

## Mapa del código

```
BD-proyecto-26/
├── app.py                 Servidor Flask: TODAS las rutas (páginas y API). Empezá a leer por acá.
├── setup_db.py            Crea la base, ejecuta sql/, carga datos de prueba y genera los logs.
├── exportar_datos_sql.py  Exporta los datos de la base a sql/03_datos_prueba.sql.
├── sql/
│   ├── 00_script_original_comentado.sql  Script original del grupo, explicado.
│   ├── 01_esquema.sql     Tablas. Cada cambio respecto a queryProyecto.sql está marcado [AJUSTE].
│   ├── 02_indices_vistas.sql  Índices agrupados/no agrupados + Query 1, 2 y 3.
│   ├── 03_datos_prueba.sql    Datos de prueba (generado con exportar_datos_sql.py).
│   └── 04_prueba_indices.sql  Prueba de rendimiento del índice agrupado (correr en SSMS).
├── docs/
│   ├── Manual_HospitalW.docx  Manual de uso y explicación del funcionamiento.
│   └── INDICES.md         Estrategia de índices y resultados de la prueba.
├── core/                  Piezas compartidas por todas las áreas
│   ├── config.py          Servidor SQL, Java, intervalos.
│   ├── db.py              pyodbc: transacciones para ESCRIBIR (INSERT/UPDATE/DELETE).
│   ├── spark.py           SparkSession + puente SQL Server -> DataFrame de PySpark.
│   ├── id_generators.py   IDs con PySpark + match: M1001 / E1001 / A1001 / PAC-0001 ...
│   ├── filtros.py         Botón "Filtrar" (filtros en PySpark).
│   └── graficos.py        Gráficas con pandas + matplotlib.
├── areas/                 Un archivo por área, todos con la misma forma (ver areas/__init__.py)
│   ├── personal.py        RF1 — alta, modificación, consulta, baja lógica, NSS único, antigüedad.
│   ├── pacientes.py       RF2 — ficha de ingreso (Figura 2) + historial de citas (Query 2).
│   ├── hospitalizacion.py RF3 — asignación de camas sin choques, fechas automáticas, nivel de riesgo.
│   ├── inventario.py      RF4 — entradas, salidas a un empleado, traslados entre almacenes, Query 3.
│   └── telemetria.py      Fase II dentro del dashboard: lee el CSV con PySpark y escribe nivelRiesgo.
├── bigdata/               Fase II — Big Data
│   ├── generar_logs.py    Ingesta masiva: logs_monitoreo_pacientes.csv (Figura 6, ≥ 5,000 registros)
│   │                      de los pacientes internados. Los logs NO se guardan en la base.
│   ├── pipeline.py        Query BD 1, 2, 3, nivel de riesgo por internación y exportación a Parquet.
│   ├── HospitalW_BigData.ipynb   Notebook para Google Colab / Databricks.
│   ├── construir_notebook.py     Regenera el notebook a partir de generar_logs.py.
│   └── datos/             CSV generado y logs_refinados.parquet.
├── templates/             HTML (Jinja2). _panel_datos.html = tabla/gráfico/filtros reutilizable.
└── static/                css/styles.css (diseño) y js/app.js (tabla en vivo, formularios).
```

## Dónde está cada requisito

| Requisito | Dónde |
|---|---|
| RF1 NSS único | `UQ_empleado_NSS` en SQL + `personal._validar()` |
| RF1 baja lógica | columna `activo` + `personal.dar_de_baja()` |
| RF1 turno/especialidad válidos | FKs en SQL + `personal._validar()` |
| ID por rol (M/E/A) con PySpark + match | `core/id_generators.py` |
| RF2 formulario Figura 2 | `templates/pacientes.html` + `pacientes.admitir()` |
| RF3 no asignar cama ocupada | `hospitalizacion.asignar()` (bloqueo) + índice `UX_asignacionCama_camaOcupada` |
| RF3 fechas automáticas | `DEFAULT GETDATE()` y `GETDATE()` en el alta |
| RF4 proveedor obligatorio | FK `FK_suministro_proveedor` + `inventario._validar_suministro()` |
| RF4 salida con destinatario / traslados | `inventario.registrar_movimiento()` → `informeSuministroSalida` |
| Query 1 / 2 / 3 | `sql/02_indices_vistas.sql` (y se ven en Personal / expediente del paciente / Inventario) |
| Índices agrupados y no agrupados | `sql/02_indices_vistas.sql` + `docs/INDICES.md` |
| Ecosistema nube (SparkSession) | `bigdata/HospitalW_BigData.ipynb`, celda 1 |
| Ingesta masiva (≥ 5,000 registros) | `bigdata/generar_logs.py` |
| Query Big Data 1 (limpieza) | `pipeline.bd1_limpieza()` + botón "Limpiar datos" (solo en Telemetría) |
| Query Big Data 2 (top 5, Spark SQL) | `pipeline.bd2_top5()` |
| Query Big Data 3 (UDF lambda) | `pipeline.bd3_riesgo()` |
| `asignacionCama.nivelRiesgo` | `pipeline.riesgo_por_internacion()` + `telemetria.paso_riesgo()` (paso 5 de Telemetría) |
| Exportación a Parquet | `pipeline.exportar_parquet()` (en Windows usa pyarrow; en Colab, `.write.parquet()`) |
| Tabla en tiempo real | `/api/<area>/firma` en `app.py` + `PanelDatos.vigilar()` en `static/js/app.js` |
| Gráficas con radio button | `GRAFICOS` de cada área + `core/graficos.py` |

## Cómo se calcula el nivel de riesgo

1. `generar_logs.py` simula lecturas de los pacientes **internados** (dentro de su estadía en `asignacionCama`).
2. Query BD 1 descarta nulos y fallas del sensor (un solo motivo por registro).
3. Query BD 3 (UDF lambda) clasifica **cada lectura**: Crítico (SpO2 < 90 % o FC > 130 o < 40),
   Moderado (SpO2 90–93 % o FC 101–130 o 40–49), Bajo.
4. Cada lectura se cruza con la internación vigente en ese momento y se calcula el nivel **sostenido**:
   Crítico si ≥ 25 % de sus lecturas fueron críticas; Moderado si ≥ 25 % fueron moderadas o críticas; si no, Bajo.
5. Solo ese resultado se escribe en `asignacionCama.nivelRiesgo`. Los logs nunca se insertan en la base.

## Agregar un filtro nuevo

En `filtros()` del área, sumá un diccionario. Ejemplo (pacientes por teléfono):

```python
{"id": "telefono", "tipo": "texto", "etiqueta": "Teléfono"},
```

El formulario y el filtrado en PySpark se generan solos (ver `core/filtros.py`).
