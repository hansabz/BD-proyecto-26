# Guía de estudio — Hospital W 2.0

Todo lo necesario para entender, explicar y defender el proyecto. Léela en orden:
cada sección se apoya en la anterior.

---

## 1. La idea en una página

El proyecto tiene **dos fases**, igual que la guía del curso:

| Fase | Qué es | Dónde vive |
|---|---|---|
| **Fase I – Transaccional** | La base de datos del hospital (SQL Server) y un programa web para manejarla: altas, bajas, consultas, internaciones, inventario | `sql/` + la aplicación Python |
| **Fase II – Big Data** | Procesar miles de lecturas de sensores de pacientes con PySpark: limpiar, sacar un top 5, clasificar riesgo y exportar a Parquet | `bigdata/` (en tu PC) y el cuaderno de **Google Colab** |

El programa web tiene **tres piezas que se hablan entre sí**:

```
 ┌──────────────┐   pide datos / manda formularios   ┌──────────────────┐   SELECT / INSERT   ┌──────────────┐
 │  NAVEGADOR   │ ─────────────────────────────────▶ │ SERVIDOR PYTHON  │ ──────────────────▶ │  SQL SERVER  │
 │ HTML/CSS/JS  │ ◀───────────────────────────────── │ (Flask, app.py)  │ ◀────────────────── │ hospitalProy.│
 └──────────────┘        JSON (datos) / PNG          └──────────────────┘     filas           └──────────────┘
                                                        │        ▲
                                                        ▼        │
                                                  PySpark (análisis) + matplotlib (gráficas)
```

- **El navegador** solo dibuja: no tiene datos guardados. Todo lo pide al servidor.
- **El servidor Python** es el intermediario: recibe el clic, valida las reglas del hospital,
  habla con la base y devuelve el resultado.
- **SQL Server** guarda todo de forma permanente. **Todo lo que ves en pantalla se lee en vivo
  de la base**: si alguien inserta un paciente desde SSMS, aparece en el dashboard en segundos.

---

## 2. Qué entregar y qué revisar a fondo

### Lo crucial (lo que se evalúa)

| Archivo | Por qué es crucial |
|---|---|
| `sql/01_esquema.sql` | **El modelo de datos**: 18 tablas, PK, FK, CHECK, DEFAULT. Es la Fase I (50 % de la nota). |
| `sql/02_indices_vistas.sql` | **Índices agrupados/no agrupados y las Queries 1, 2 y 3** que pide la guía. |
| `sql/03_datos_prueba.sql` | Datos para que la base no esté vacía al revisarla. |
| `bigdata/HospitalW_BigData.ipynb` | **El cuaderno de Google Colab**: Fase II completa (50 % de la nota). |
| La aplicación (`app.py`, `core/`, `areas/`, `templates/`, `static/`) | El CRUD: RF1 a RF4 funcionando. |
| `docs/INDICES.md` | Cómo y por qué se hicieron los índices (con la prueba de rendimiento). |

### Lo que conviene revisar a fondo antes de la revisión

1. **`sql/01_esquema.sql`** — saber explicar cada tabla y cada `[AJUSTE]` (por qué se cambió el script original).
2. **`sql/02_indices_vistas.sql`** — saber ejecutar y explicar las 3 queries.
3. **`bigdata/pipeline.py`** — las 3 queries de Big Data en PySpark (es el mismo código que el cuaderno).
4. **`core/id_generators.py`** — el generador de IDs con PySpark + `match` (lo pidió el grupo).
5. **`areas/hospitalizacion.py` → `asignar()`** — cómo se impide asignar una cama ocupada (RF3).

### Lo de apoyo (no hace falta dominarlo línea por línea)

`templates/` y `static/` (el diseño de las pantallas), `setup_db.py` (crea la base con datos),
`exportar_datos_sql.py`, `bigdata/construir_notebook.py`, `pruebas_sistema.py`.

---

## 3. Las tecnologías y para qué sirve cada una

| Tecnología | Qué es | Para qué la usamos |
|---|---|---|
| **SQL Server** | Motor de base de datos relacional | Guardar todo: personal, pacientes, camas, inventario |
| **Python** | Lenguaje de programación | Todo el programa del servidor |
| **Flask** | Librería de Python para hacer servidores web | Recibir cada clic del navegador y responder (`app.py`) |
| **pyodbc** | Librería que conecta Python con SQL Server | Ejecutar los `SELECT`, `INSERT`, `UPDATE` (`core/db.py`) |
| **PySpark** | Versión para Python de Apache Spark, un motor de procesamiento de datos masivos | Analizar datos: IDs, filtros y toda la Fase II |
| **pandas + matplotlib** | Librerías de tablas y de gráficas | Dibujar las gráficas (`core/graficos.py`) |
| **HTML / CSS / JavaScript** | Lenguajes del navegador | Las pantallas, el diseño y la interactividad |
| **Google Colab** | Cuadernos de Python gratuitos en la nube de Google | Correr la Fase II "en la nube", como pide la guía |

---

## 4. Recorrido completo de un clic

### Ejemplo A: dar de alta a un empleado (escritura)

1. **Navegador** (`templates/personal.html`): el usuario llena el formulario y presiona *Guardar*.
   JavaScript junta los campos y los manda al servidor: `POST /api/personal` con los datos en JSON.
2. **Servidor** (`app.py`, función `personal_crear`): recibe el pedido y llama a `personal.crear(datos)`.
3. **Validación** (`areas/personal.py`, `_validar`): revisa campos obligatorios, que el NSS tenga
   el formato `000000-X`, **que el NSS no exista ya** (consulta a SQL Server) y que puesto,
   especialidad y turno existan en sus catálogos. Si algo falla, lanza `ErrorNegocio` y el
   navegador marca el campo en rojo.
4. **ID con PySpark** (`core/id_generators.py`): lee el nombre del puesto, lo normaliza
   ("Enfermera Jefe" → "enfermera"), un `match` decide la letra (E) y busca el número más alto
   usado (E1007) para sumarle 1 → **E1008**.
5. **Guardado** (`core/db.py`, `transaccion()`): `INSERT INTO empleado ...` y sus salas, **en una
   sola transacción**: si algo falla, no se guarda nada (ROLLBACK).
6. **Respuesta**: el servidor devuelve "Empleado E1008 registrado" y la tabla se actualiza.

### Ejemplo B: el botón Filtrar (lectura con PySpark)

1. El usuario elige "Puesto = Médico" y presiona *Aplicar filtro*.
2. El navegador manda `POST /api/personal/filtrar` con `{"codigoPuesto": "MED"}`.
3. `app.py` llama a `personal.dataset()`, que ejecuta el `SELECT` del área en SQL Server y
   **convierte el resultado en un DataFrame de PySpark** (`core/spark.py`, `leer_sql`).
4. `core/filtros.py` aplica `df.filter(F.col("codigoPuesto") == "MED")`.
5. `.collect()` ejecuta el cálculo y el resultado vuelve como JSON a la tabla.

### Ejemplo C: la tabla en tiempo real

Cada 4 segundos el navegador pide `/api/<area>/firma`. El servidor calcula una "huella" de las
tablas con `COUNT(*)` y `CHECKSUM_AGG(BINARY_CHECKSUM(*))` (un número que cambia si cambia
cualquier fila). Si la huella es distinta a la anterior, el navegador recarga la tabla.

---

## 5. La base de datos

### Las tablas

| Grupo | Tablas | Qué guardan |
|---|---|---|
| Catálogos | `puesto`, `especialidad`, `turno` | Valores válidos para el personal |
| RF1 Personal | `empleado`, `sala`, `asignacionSala` | El personal y en qué salas trabaja |
| RF2 Pacientes | `paciente`, `registroIngreso`, `informeCita` | Pacientes, cada ingreso (con familiar y médico que refiere) y citas |
| RF3 Hospitalización | `cama`, `asignacionCama` | Camas (0 libre / 1 ocupada) e internaciones con su nivel de riesgo |
| RF4 Inventario | `ubicacion`, `proveedor`, `suministro`, `informeSuministroIngreso`, `suministroUbicacion`, `entradaSuministro`, `informeSuministroSalida` | Almacenes, proveedores, insumos, stock, entradas, salidas y traslados |

### Las restricciones (qué hace cada tipo)

| Restricción | Qué hace | Ejemplo en el proyecto |
|---|---|---|
| **PRIMARY KEY** | Identifica cada fila; no se repite | `numeroEmpleado` |
| **FOREIGN KEY** | El valor debe existir en otra tabla | No se puede asignar un empleado a una sala inexistente |
| **UNIQUE** | No se repite entre filas | `NSS` (RF1) |
| **CHECK** | Regla que toda fila debe cumplir | `CK_cita_hora`: citas de 07:00 a 19:00 |
| **DEFAULT** | Valor automático si el INSERT no lo manda | `DF_asignacionCama_fechaIngreso`: la fecha y hora actuales (RF3) |

**Todas las tablas terminan con un bloque `-- CHECK y DEFAULT`** con un CHECK y un DEFAULT
con nombre propio (ej. `CK_sala_codigo` y `DF_sala_camas`).

### Los `[AJUSTE]` (cambios respecto al script original del grupo)

Cada uno está marcado en `sql/01_esquema.sql` con su motivo. Los más importantes:

- `empleado.activo` y `fechaBaja` → **baja lógica** de RF1 (no se borra, se marca inactivo).
- Tamaños de `varchar` ampliados → `PAC-0001` no entraba en 5 caracteres, "Enfermera Jefe" no entraba en 10.
- Tabla `proveedor` → RF4 pide un proveedor "debidamente registrado" y la Query 3 sus datos de contacto.
- `suministroUbicacion.cantidadIdeal` → base del 15 % de la Query 3.
- `informeSuministroSalida` con `idSalida` autoincremental → con la PK original solo se podía despachar **una vez** cada insumo desde cada almacén.
- Índices únicos filtrados en `asignacionCama` → **el propio SQL Server impide** dos internaciones activas en la misma cama (RF3).

### Las 3 queries (en `sql/02_indices_vistas.sql`)

```sql
-- Query 1 (Figura 1): personal asignado a una sala, con JOIN entre empleado, sala y catálogos
SELECT * FROM VW_Asignacion_Personal WHERE codigoSala = 'S01';

-- Query 2 (Figura 4): procedimiento almacenado parametrizado por paciente, en orden cronológico
EXEC SP_HistorialCitas @ID_Paciente = 'PAC-0001';

-- Query 3: insumos con menos del 15 % de su cantidad ideal + contacto del proveedor
SELECT * FROM VW_Stock_Critico ORDER BY porcentaje;
```

- Una **vista** (`VIEW`) es un `SELECT` guardado con nombre: se consulta como si fuera una tabla.
- Un **procedimiento almacenado** (`PROCEDURE`) es un bloque de SQL guardado que recibe
  parámetros (`@ID_Paciente`).

### Los índices (detalle en `docs/INDICES.md`)

- **Agrupado (clustered)**: ordena físicamente la tabla; solo hay uno por tabla. Por defecto lo crea la PK.
- **No agrupado (nonclustered)**: una estructura aparte, como el índice de un libro.
- En `informeCita` el agrupado se puso sobre `(numeroPaciente, fecha, hora)` porque así lee la
  Query 2. Prueba con 200,000 citas: **2,506 páginas leídas sin el índice → 5 con el índice**.

---

## 6. PySpark explicado desde cero

### Qué es

**Apache Spark** es un motor para procesar volúmenes de datos muy grandes repartiendo el trabajo
entre muchas computadoras (un "clúster"). **PySpark** es la forma de usarlo desde Python.
En este proyecto Spark corre en **una sola computadora** (`local[2]` = 2 núcleos de tu PC, o la
máquina de Google en Colab), pero el código es exactamente el mismo que correría en un clúster.

### Los 4 conceptos que hay que saber

1. **SparkSession**: el "punto de entrada" a Spark. Se crea una vez:
   ```python
   spark = SparkSession.builder.master("local[2]").appName("HospitalW").getOrCreate()
   ```
   En la PC está en `core/spark.py`; en Colab, en la primera celda. Esto es lo que la guía llama
   "inicializar un objeto SparkSession".

2. **DataFrame**: una tabla de Spark (filas y columnas con tipos), repartida en partes para
   procesarla en paralelo. Se parece a una tabla de SQL.

3. **Transformaciones** (perezosas): `filter`, `select`, `withColumn`, `groupBy`, `join`.
   **No se ejecutan al escribirlas**: Spark solo anota el plan.

4. **Acciones**: `collect()`, `count()`, `first()`, `show()`, `toPandas()`, `write`.
   **Recién aquí Spark ejecuta todo el plan junto**, optimizado.

### Cómo llegan los datos de SQL Server a PySpark (`core/spark.py`, `leer_sql`)

```python
descripcion, filas = db.consultar_crudo("SELECT ... FROM empleado ...")   # 1. pyodbc ejecuta el SELECT
esquema = StructType([...])                                                # 2. tipo de cada columna
df = spark.createDataFrame(filas, esquema)                                 # 3. DataFrame de Spark
```

No se usa el conector JDBC de Spark porque necesita un archivo `.jar` adicional y usuario y
contraseña de SQL; así basta con la autenticación de Windows.

### Dónde se usa PySpark en el programa

| Uso | Archivo | Funciones de Spark que usa |
|---|---|---|
| Generar IDs por rol | `core/id_generators.py` | `trim`, `lower`, `translate`, `split`, `rlike`, `regexp_extract`, `max` + `match` de Python |
| Botón Filtrar | `core/filtros.py` | `filter`, `contains`, `between`, `to_date` |
| Gráficas | `areas/*.py` (`GRAFICOS`) | `groupBy`, `agg`, `toPandas` |
| Fase II completa | `bigdata/pipeline.py` | `read.csv`, `when`, `Window`, `createOrReplaceTempView`, `spark.sql`, `udf`, `join`, `write.parquet` |

**Lo que escribe en la base NO pasa por Spark**: Spark no maneja transacciones, así que todos
los `INSERT`/`UPDATE` van por pyodbc (`core/db.py`).

---

## 7. Big Data (Fase II) paso a paso

Archivo: `bigdata/pipeline.py` (y las mismas celdas en el cuaderno de Colab).

| Paso | Requisito de la guía | Qué hace el código |
|---|---|---|
| 1. Ingesta masiva | ≥ 5,000 registros con el esquema de la Figura 6 | `generar_logs.py` crea `logs_monitoreo_pacientes.csv`; `ingesta()` lo lee con `spark.read.schema(ESQUEMA).csv(...)` |
| 2. Query BD 1 | Quitar nulos y lecturas atípicas (FC < 30 o > 220) | `bd1_limpieza()`: cada lectura recibe un `motivo_descarte` con `F.when(...)`; las válidas tienen motivo `NULL` |
| 3. Query BD 2 | Top 5 de pacientes con más alertas de SpO2 < 90 % usando Spark SQL sobre una vista temporal | `createOrReplaceTempView("logs_limpios")` y luego `spark.sql("SELECT ... GROUP BY ID_Paciente ORDER BY ... LIMIT 5")` |
| 4. Query BD 3 | UDF lambda que evalúa FC y SpO2 y agrega `Nivel_Riesgo` | `F.udf(lambda fc, spo2: "Crítico" if ... else ...)` + `withColumn("Nivel_Riesgo", ...)` |
| 5. Riesgo a la base | (extra del grupo) | Cruza cada lectura con la internación del paciente y guarda el nivel en `asignacionCama.nivelRiesgo` |
| 6. Exportación | `.write.parquet()` | `exportar_parquet()` y luego lo relee para verificar |

**Reglas de riesgo:**

- **Por lectura (Query BD 3):** Crítico si SpO2 < 90 o FC > 130 o FC < 40; Moderado si SpO2 90–93 o FC 101–130 o 40–49; Bajo en otro caso.
- **Por paciente:** Crítico si al menos el 25 % de sus lecturas fueron críticas; Moderado si al menos el 25 % fueron moderadas o críticas; Bajo en otro caso.

**Los logs NO se guardan en SQL Server**: viven en el CSV. Lo único que vuelve a la base es el
nivel de riesgo de cada internación.

**Parquet** es un formato de archivo por columnas, comprimido y muy rápido de leer: el estándar
para guardar datos procesados en Big Data. En Windows, Spark necesita un programa extra
(`winutils.exe`) para escribir archivos; sin él, la app escribe el mismo Parquet con la librería
`pyarrow`. En Colab (Linux) funciona `.write.parquet()` directamente.

---

## 8. Google Colab y su relación con el código Python

**Google Colab** es un servicio gratuito de Google para escribir y ejecutar Python desde el
navegador, en una máquina de Google. Se organiza en **celdas** (código o texto) que se ejecutan
en orden. Un archivo de cuaderno tiene extensión `.ipynb`.

**Por qué lo usamos:** la guía pide *"configurar un notebook interactivo en Google Colab o
Databricks inicializando un objeto SparkSession"*. O sea, la Fase II se demuestra en la nube.

**Relación con el programa de la PC:**

| | Cuaderno de Colab (`bigdata/HospitalW_BigData.ipynb`) | Programa en la PC (página Telemetría) |
|---|---|---|
| Qué es | **El entregable de la Fase II** | La Fase II integrada al CRUD |
| Dónde corre Spark | Máquina de Google | Tu computadora |
| Datos | Genera los suyos (no puede ver tu SQL Server) | Lecturas de los pacientes internados en tu base |
| Resultado | Tablas, gráfica, Parquet y CSV de riesgo por paciente | Además escribe `nivelRiesgo` en SQL Server |
| Código | El mismo generador de logs y la misma lógica que `pipeline.py` | `bigdata/pipeline.py` |

**¿Por qué Colab no se conecta a la base?** Porque tu SQL Server está en tu computadora
(`localhost`) y una máquina de Google no tiene acceso a ella. Por eso el cuaderno genera sus
propios datos con el mismo código.

**Cómo usarlo:** abrir
https://colab.research.google.com/github/hansabz/BD-proyecto-26/blob/main/bigdata/HospitalW_BigData.ipynb
y *Entorno de ejecución → Ejecutar todas* (1 a 2 minutos).

---

## 9. Archivo por archivo

### Raíz
| Archivo | Qué hace |
|---|---|
| `app.py` | El servidor Flask. Define **todas las rutas**: las páginas (`/personal`...) y la API (`/api/...`). Es el "recepcionista": recibe el pedido y llama a la función del área que corresponde. |
| `setup_db.py` | Crea la base `hospitalProyecto`, ejecuta `01` y `02`, carga datos de prueba, genera los logs y calcula el riesgo. `--reset` **borra** la base anterior. |
| `exportar_datos_sql.py` | Exporta los datos de la base a `sql/03_datos_prueba.sql`. |
| `pruebas_sistema.py` | 41 pruebas automáticas sobre una base desechable. |
| `requirements.txt` | Las librerías de Python que hay que instalar. |

### `core/` — piezas compartidas
| Archivo | Qué hace |
|---|---|
| `config.py` | Nombre del servidor SQL, de la base, ruta de Java. Todo se puede cambiar con variables de entorno. |
| `db.py` | Conexión a SQL Server con pyodbc. `transaccion()` (todo o nada), `consultar()` (SELECT → lista de diccionarios). Define `ErrorNegocio`. |
| `spark.py` | Crea la SparkSession y convierte un SELECT de SQL Server en DataFrame de Spark (`leer_sql`). |
| `id_generators.py` | IDs con PySpark + `match`: M/E/A + número, PAC-0001, ING-001, Q/F/M + número, PR001. |
| `filtros.py` | Aplica los filtros del botón Filtrar en PySpark. |
| `graficos.py` | Funciones para dibujar dona, barras, histograma y línea con matplotlib. |

### `areas/` — un archivo por pantalla, todos con la misma forma
Cada uno tiene: `BASE_SQL` (el SELECT de la tabla), `tabla()`, `metricas()`, `dataset()` (para
Spark), `filtros()`, `GRAFICOS` y sus funciones de CRUD.

| Archivo | Funciones principales |
|---|---|
| `personal.py` | `crear`, `actualizar`, `dar_de_baja`, `reactivar`, `asignacion_por_sala` (Query 1) |
| `pacientes.py` | `admitir`, `actualizar`, `ficha`, `historial_citas` (Query 2), `agendar_cita` |
| `hospitalizacion.py` | `asignar` (bloquea la cama y verifica que esté libre), `dar_alta`, `mapa_camas`, `pendientes` |
| `inventario.py` | `crear_suministro`, `registrar_movimiento` (entrada / salida / traslado), `stock_critico` (Query 3), proveedores |
| `telemetria.py` | Los pasos del pipeline para la página Telemetría y el botón Limpiar datos (BD 1) |

### `bigdata/` — Fase II
| Archivo | Qué hace |
|---|---|
| `generar_logs.py` | Simula las lecturas de los sensores (Figura 6) con fallas y nulos a propósito. |
| `pipeline.py` | Ingesta, Query BD 1, 2, 3, riesgo por internación y Parquet. |
| `HospitalW_BigData.ipynb` | El cuaderno de Google Colab. |
| `construir_notebook.py` | Regenera el cuaderno a partir de `generar_logs.py` (para que usen el mismo código). |
| `datos/` | El CSV de logs y el Parquet exportado. |

### `sql/`
| Archivo | Qué es | ¿Lo usa la app? |
|---|---|---|
| `00_script_original_comentado.sql` | Script original del grupo, explicado. Documentación. | No |
| `01_esquema.sql` | Las 18 tablas con todas sus restricciones | Sí |
| `02_indices_vistas.sql` | Índices + Query 1, 2 y 3 | Sí |
| `03_datos_prueba.sql` | Datos de prueba | Solo si se arma la base desde SSMS |
| `04_prueba_indices.sql` | Prueba de rendimiento del índice agrupado | No (se corre en SSMS) |

### `templates/` y `static/` — las pantallas
- `templates/base.html`: el esqueleto común (barra lateral, cabecera).
- `templates/<area>.html`: cada pantalla, con su JavaScript propio (formularios, diálogos).
- `templates/_panel_datos.html`: el panel reutilizable (tabla en vivo, Tabla/Gráfico, Filtrar).
- `static/js/app.js`: la lógica común del navegador (pedir datos, dibujar la tabla, tiempo real).
- `static/css/styles.css`: el diseño.

---

## 10. Preguntas probables en la revisión (con respuesta)

**¿Dónde están las queries 1, 2 y 3?** En `sql/02_indices_vistas.sql`: vista `VW_Asignacion_Personal`,
procedimiento `SP_HistorialCitas` y vista `VW_Stock_Critico`. En la app se ven en Personal
(reporte por sala), en el expediente del paciente y en Inventario (alerta de stock crítico).

**¿Cómo se garantiza que el NSS sea único?** Dos veces: la restricción `UNIQUE` en SQL Server y
una validación en Python (`personal._validar`) que muestra a quién pertenece el NSS repetido.

**¿Qué es la baja lógica?** No se borra al empleado: se marca `activo = 0` con `fechaBaja`. Así
se conservan su historial y las referencias (citas, ubicaciones a cargo).

**¿Cómo se impide asignar una cama ocupada?** Doble candado:
(1) `hospitalizacion.asignar()` bloquea la fila de la cama (`WITH (UPDLOCK, HOLDLOCK)`) dentro
de una transacción y verifica que esté libre; (2) el índice único filtrado
`UX_asignacionCama_camaOcupada` hace que el propio SQL Server rechace una segunda internación
activa en esa cama.

**¿Quién pone las fechas de ingreso y salida?** SQL Server: `DEFAULT GETDATE()` al internar y
`GETDATE()` al dar el alta. El usuario no las escribe.

**¿Qué es un índice agrupado y cuál usaron?** Ordena físicamente la tabla. En `informeCita` está
sobre `(numeroPaciente, fecha, hora)`: la Query 2 pasó de 2,506 a 5 páginas leídas.

**¿Qué es una transacción?** Un grupo de operaciones que se guardan todas o ninguna. Ej.: una
salida de inventario descuenta el stock y registra la bitácora en una sola transacción.

**¿Qué es la SparkSession?** El punto de entrada a Spark; sin ella no se puede crear ningún
DataFrame.

**¿Qué diferencia hay entre una transformación y una acción en Spark?** Las transformaciones
(`filter`, `groupBy`) solo arman un plan; las acciones (`count`, `collect`, `show`) lo ejecutan.

**¿Qué es una UDF?** *User Defined Function*: una función de Python que Spark aplica a cada fila,
repartiendo el trabajo. La nuestra es una `lambda` que recibe FC y SpO2 y devuelve el nivel de riesgo.

**¿Qué es una vista temporal en Spark?** Un nombre para un DataFrame (`createOrReplaceTempView`)
que permite consultarlo con SQL normal mediante `spark.sql(...)`. Así se hace la Query BD 2.

**¿Por qué Parquet y no CSV?** Parquet guarda por columnas, comprimido y con los tipos de datos:
ocupa menos y se lee mucho más rápido. Es el estándar de Big Data.

**¿Los logs se guardan en la base?** No. Viven en el CSV; solo el nivel de riesgo calculado
vuelve a `asignacionCama.nivelRiesgo`.

**¿Por qué el programa usa pyodbc Y PySpark?** pyodbc para escribir con transacciones; PySpark
para analizar. Spark no maneja transacciones.

**¿Cómo se generan los IDs?** `core/id_generators.py`: Spark extrae el rol del puesto con
funciones nativas (`trim`, `lower`, `translate`, `split`), un `match` decide la letra y Spark
calcula el siguiente número con `regexp_extract` y `max`.

---

## 11. Guion para demostrar en vivo (10 minutos)

1. **Panel general**: mostrar que todo se lee de SQL Server en vivo.
2. **Personal**: dar de alta un médico (ID **M10xx**), intentar repetir el NSS `883920-K`
   (se rechaza), dar de baja y mostrar que sigue en la tabla. Mostrar el reporte de la Query 1.
3. **Admisión**: admitir un paciente (Figura 2), abrir su expediente (Query 2).
4. **Hospitalización**: intentar internarlo en una cama **ocupada** (se rechaza), luego en una libre
   (la fecha se pone sola), y dar el alta.
5. **Inventario**: hacer una salida sin destinatario (se rechaza), un traslado, y mostrar la
   alerta de stock crítico (Query 3).
6. **Tiempo real**: con el dashboard abierto, hacer un `UPDATE` en SSMS y ver cómo aparece solo.
7. **Telemetría**: ejecutar los pasos 2 a 6 del pipeline.
8. **Google Colab**: abrir el cuaderno y mostrar las celdas ya ejecutadas.
9. **SSMS**: ejecutar las 3 queries y mostrar los CHECK y DEFAULT del esquema.

---

## 12. Pruebas automáticas

```bash
.\.venv\Scripts\python pruebas_sistema.py
```

Crea una base desechable, ejecuta **41 pruebas** (base de datos, queries, RF1–RF4, pantallas,
gráficas y Big Data) y la borra al terminar. Al final muestra cuántas pasaron. No toca la base real.
