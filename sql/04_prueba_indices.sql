/* =====================================================================
   HOSPITAL "W" 2.0 — Prueba de rendimiento del índice agrupado
   Se puede correr en SSMS sobre hospitalProyecto: trabaja con tablas
   temporales (#) y no modifica ningún dato real.

   Compara la Query 2 (historial de un paciente en orden cronológico)
   sobre 200,000 citas con dos diseños:
     A) agrupado por numeroCita (lo que SQL Server hace por defecto con la PK)
     B) agrupado por (numeroPaciente, fecha, hora)  <- el que usa el proyecto
   La medida es "lecturas lógicas": cuántas páginas de 8 KB tuvo que leer el motor.
   ===================================================================== */
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#citasA') IS NOT NULL DROP TABLE #citasA;
IF OBJECT_ID('tempdb..#citasB') IS NOT NULL DROP TABLE #citasB;

CREATE TABLE #citasA (numeroCita int NOT NULL, numeroPaciente varchar(10) NOT NULL, fecha date NOT NULL,
                      hora time NOT NULL, numeroEmpleado varchar(5) NOT NULL, relleno char(60) NOT NULL DEFAULT '',
                      CONSTRAINT PK_A PRIMARY KEY CLUSTERED (numeroCita));
CREATE TABLE #citasB (numeroCita int NOT NULL, numeroPaciente varchar(10) NOT NULL, fecha date NOT NULL,
                      hora time NOT NULL, numeroEmpleado varchar(5) NOT NULL, relleno char(60) NOT NULL DEFAULT '',
                      CONSTRAINT PK_B PRIMARY KEY NONCLUSTERED (numeroCita));
CREATE CLUSTERED INDEX CIX_B ON #citasB (numeroPaciente, fecha, hora);

-- 200,000 citas repartidas entre 2,000 pacientes (≈100 citas cada uno), en orden de llegada.
-- Generador de números sin tablas del sistema: 10 x 10 x 10 x 10 x 10 x 10 = 1,000,000 combinaciones.
;WITH d AS (SELECT v FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) t(v)),
n AS (SELECT a.v + 10*b.v + 100*c.v + 1000*e.v + 10000*f.v + 100000*g.v + 1 AS i
      FROM d a, d b, d c, d e, d f, d g
      WHERE a.v + 10*b.v + 100*c.v + 1000*e.v + 10000*f.v + 100000*g.v < 200000)
INSERT INTO #citasA (numeroCita, numeroPaciente, fecha, hora, numeroEmpleado)
SELECT i, 'PAC-' + RIGHT('0000' + CAST(ABS(CHECKSUM(i * 7919)) % 2000 + 1 AS varchar), 4),
       DATEADD(day, i / 150, '2020-01-01'), DATEADD(minute, (i % 20) * 30, CAST('08:00' AS time)),
       'M' + CAST(1001 + i % 8 AS varchar)
FROM n;
INSERT INTO #citasB SELECT * FROM #citasA;

PRINT '=== A) Agrupado por numeroCita (por defecto) ===';
SET STATISTICS IO ON;
SELECT numeroCita, fecha, hora, numeroEmpleado FROM #citasA
WHERE numeroPaciente = 'PAC-0001' ORDER BY fecha, hora;
SET STATISTICS IO OFF;

PRINT '=== B) Agrupado por (numeroPaciente, fecha, hora) ===';
SET STATISTICS IO ON;
SELECT numeroCita, fecha, hora, numeroEmpleado FROM #citasB
WHERE numeroPaciente = 'PAC-0001' ORDER BY fecha, hora;
SET STATISTICS IO OFF;

-- Resultado: comparar "logical reads" de cada tabla en la pestaña Mensajes de SSMS.

/* ---------------------------------------------------------------------
   Para ver qué índice agrupado tiene cada tabla de la base real:
   --------------------------------------------------------------------- */
SELECT t.name AS tabla, i.name AS indice, i.type_desc AS tipo,
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columnas
FROM sys.indexes i
JOIN sys.tables t         ON t.object_id = i.object_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
JOIN sys.columns c        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.type_desc = 'CLUSTERED'
GROUP BY t.name, i.name, i.type_desc
ORDER BY t.name;
