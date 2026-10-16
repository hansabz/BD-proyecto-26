# Indexación avanzada — Hospital W 2.0

Requisito de la guía (Fase I): *"identificar campos de alta concurrencia para implementar Índices
Agrupados (Clustered) y No Agrupados (Non-Clustered) que reduzcan los tiempos de respuesta del motor"*.

Todo está en `sql/01_esquema.sql` (PK) y `sql/02_indices_vistas.sql` (índices). La prueba de
rendimiento está en `sql/04_prueba_indices.sql` y se puede correr en SSMS sin tocar datos reales.

## 1. Qué es cada tipo

| | Agrupado (CLUSTERED) | No agrupado (NONCLUSTERED) |
|---|---|---|
| Qué es | La tabla **misma**, ordenada físicamente por la clave | Una estructura **aparte** con la clave y un puntero a la fila |
| Cuántos por tabla | **Uno solo** (las filas solo pueden estar en un orden) | Muchos |
| Ideal para | Leer **rangos** ordenados: "todas las citas del paciente X por fecha" | Búsquedas puntuales y JOIN por columnas que no son la clave |

En SQL Server, cada `PRIMARY KEY` crea el índice agrupado por defecto. Por eso la decisión de
diseño es: **¿la consulta más frecuente de esta tabla es "buscar por ID" o "leer un rango"?**

## 2. Cómo se decidió, tabla por tabla

### Tablas que se buscan por su ID → el agrupado queda en la PK (por defecto)
`empleado`, `paciente`, `cama`, `asignacionCama`, `registroIngreso`, `suministroUbicacion`,
`informeSuministroIngreso` y los catálogos. Sus operaciones de alta concurrencia son
"traer/bloquear **esta** fila" (por ejemplo, `UPDLOCK` sobre una cama o sobre el stock de un insumo
en una ubicación), y para eso el agrupado por PK es lo óptimo.

`asignacionSala` tiene PK `(codigoSala, numeroEmpleado)`: al estar agrupada empezando por
`codigoSala`, la **Query 1** (personal de una sala) lee a todo el personal de esa sala junto.

### Tablas de alta concurrencia que se leen por rango → PK NO agrupada + agrupado a medida

| Tabla | Índice agrupado | Por qué |
|---|---|---|
| `informeCita` | `CIX_informeCita_paciente_fecha (numeroPaciente, fecha, hora)` | La **Query 2** pide todas las citas de un paciente en orden cronológico. Con este orden físico, esas filas están contiguas y ya ordenadas: el motor no busca fila por fila ni ordena después. |
| `entradaSuministro` | `CIX_entradaSuministro_fecha (fecha, idEntrada)` | Bitácora con inserciones constantes. Al agrupar por fecha creciente, cada INSERT va al final (sin partir páginas a la mitad) y los reportes por período leen un rango. |
| `informeSuministroSalida` | `CIX_informeSuministroSalida_fecha (fecha, idSalida)` | Igual que la anterior: cada salida o traslado se agrega al final; "movimientos recientes" y "consumo semanal" leen por fecha. |

En estas tres tablas la PK se declaró `PRIMARY KEY NONCLUSTERED`, porque el único agrupado
posible se reservó para el orden de lectura. Se agregó `idEntrada` / `idSalida` al final de la
clave para que sea única aunque dos movimientos ocurran en el mismo instante.

### Índices no agrupados (apoyo a JOIN y filtros)
`IX_empleado_puesto`, `IX_empleado_especialidad`, `IX_registroIngreso_paciente`, `IX_cama_estado`,
`IX_asignacionCama_paciente`, `IX_suministro_proveedor`, `IX_salida_destinatario`.
Algunos usan `INCLUDE (...)` para que la consulta encuentre todo en el índice sin volver a la tabla.

Además hay dos **índices únicos filtrados** que son reglas de negocio de RF3:
`UX_asignacionCama_camaOcupada` y `UX_asignacionCama_pacienteInternado` (`WHERE codigoEstado = 1`):
el motor impide dos internaciones activas en la misma cama, o un paciente en dos camas.

## 3. Prueba de rendimiento (`sql/04_prueba_indices.sql`)

Se cargaron **200,000 citas** de 2,000 pacientes en dos tablas temporales idénticas, una con el
agrupado por defecto (`numeroCita`) y otra con el agrupado del proyecto
(`numeroPaciente, fecha, hora`). Luego se ejecutó la Query 2 para un paciente con
`SET STATISTICS IO ON`, que mide las **lecturas lógicas** (páginas de 8 KB que el motor tuvo que leer):

| Diseño | Lecturas lógicas |
|---|---|
| A) Agrupado por `numeroCita` (por defecto) | **2,506** |
| B) Agrupado por `(numeroPaciente, fecha, hora)` | **5** |

Con el diseño A, las citas de un paciente están desparramadas por toda la tabla y el motor
recorre **todas** las páginas. Con el diseño B están juntas: lee solo las 5 páginas que las
contienen, unas **500 veces menos** trabajo para la misma respuesta.

## 4. Cómo comprobarlo en la base

La última consulta de `sql/04_prueba_indices.sql` lista el índice agrupado de cada tabla:

```sql
SELECT t.name AS tabla, i.name AS indice, i.type_desc AS tipo
FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id
WHERE i.type_desc = 'CLUSTERED';
```
