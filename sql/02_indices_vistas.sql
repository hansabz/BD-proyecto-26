/* =====================================================================
   HOSPITAL "W" 2.0 — Índices, vistas y consultas oficiales
   Explicación completa de la estrategia de índices: docs/INDICES.md
   ===================================================================== */

/* ---------------------------------------------------------------------
   1) ÍNDICES AGRUPADOS (CLUSTERED)

   Un índice agrupado ORDENA FÍSICAMENTE las filas de la tabla según su
   clave: la tabla misma es el índice. Por eso solo puede haber UNO por
   tabla, y conviene ponerlo en la columna por la que más se lee en rangos.

   En SQL Server cada PRIMARY KEY crea, por defecto, el índice agrupado.
   Eso ya es lo ideal en los catálogos y en las tablas que se buscan por su
   ID (empleado, paciente, cama, asignacionCama, suministroUbicacion...).

   Pero en tres tablas de MUCHA concurrencia la consulta frecuente NO es
   "buscar por ID" sino "leer un rango". Ahí la PK se declaró NONCLUSTERED
   (en 01_esquema.sql) y el agrupado va sobre la columna del rango:
   --------------------------------------------------------------------- */

-- informeCita: Query 2 trae TODAS las citas de un paciente en orden cronológico.
-- Agrupando por (numeroPaciente, fecha, hora), las citas de un paciente quedan
-- juntas y ya ordenadas en disco: una sola lectura secuencial, sin ORDER BY costoso.
CREATE CLUSTERED INDEX CIX_informeCita_paciente_fecha
    ON informeCita (numeroPaciente, fecha, hora);
GO

-- Bitácoras de inventario: reciben inserciones constantes (cada entrada, salida y
-- traslado) y se consultan por fecha ("movimientos recientes", "consumo semanal").
-- Agrupar por fecha creciente hace que cada INSERT vaya al final de la tabla
-- (sin partir páginas a la mitad) y que los reportes por período lean un rango contiguo.
CREATE CLUSTERED INDEX CIX_entradaSuministro_fecha
    ON entradaSuministro (fecha, idEntrada);
GO
CREATE CLUSTERED INDEX CIX_informeSuministroSalida_fecha
    ON informeSuministroSalida (fecha, idSalida);
GO

/* ---------------------------------------------------------------------
   2) ÍNDICES NO AGRUPADOS (NONCLUSTERED)
   Estructuras aparte que apuntan a las filas. Van sobre las FK por las
   que se hacen JOIN y los filtros frecuentes del dashboard.
   --------------------------------------------------------------------- */
CREATE NONCLUSTERED INDEX IX_empleado_puesto        ON empleado (codigoPuesto) INCLUDE (activo, codigoTurno);
GO
CREATE NONCLUSTERED INDEX IX_empleado_especialidad  ON empleado (codigoEspecialidad);
GO
CREATE NONCLUSTERED INDEX IX_registroIngreso_paciente ON registroIngreso (numeroPaciente) INCLUDE (fechaReg);
GO
CREATE NONCLUSTERED INDEX IX_cama_estado            ON cama (codigoEstado) INCLUDE (codigoSala);
GO
CREATE NONCLUSTERED INDEX IX_asignacionCama_paciente ON asignacionCama (numeroPaciente) INCLUDE (fechaIngreso, fechaSalida);
GO
CREATE NONCLUSTERED INDEX IX_suministro_proveedor   ON suministro (codigoProveedor);
GO
CREATE NONCLUSTERED INDEX IX_salida_destinatario    ON informeSuministroSalida (empleadoDestinatario);
GO

/* ---------------------------------------------------------------------
   Query 1 — VW_Asignacion_Personal (Figura 1)
   Personal activo unido a sus salas. "camasDisponibles" se calcula en
   vivo desde la tabla cama (codigoEstado = 0 = disponible).
   Uso: SELECT * FROM VW_Asignacion_Personal WHERE codigoSala = 'S01';
   --------------------------------------------------------------------- */
CREATE VIEW VW_Asignacion_Personal AS
SELECT
    s.codigoSala,
    s.nombre                                        AS sala,
    (SELECT COUNT(*) FROM cama c
      WHERE c.codigoSala = s.codigoSala AND c.codigoEstado = 0) AS camasDisponibles,
    e.numeroEmpleado,
    e.nombres + ' ' + e.apellidos                   AS nombreEmpleado,
    e.NSS,
    p.nombre                                        AS puesto,
    es.nombre                                       AS especialidad,
    t.nombre                                        AS turno,
    p.salario                                       AS salarioBase
FROM asignacionSala a
JOIN sala         s  ON s.codigoSala          = a.codigoSala
JOIN empleado     e  ON e.numeroEmpleado      = a.numeroEmpleado
JOIN puesto       p  ON p.codigoPuesto        = e.codigoPuesto
JOIN especialidad es ON es.codigoEspecialidad = e.codigoEspecialidad
JOIN turno        t  ON t.codigoTurno         = e.codigoTurno
WHERE e.activo = 1;
GO

/* ---------------------------------------------------------------------
   Query 2 — Historial consolidado de citas (Figura 4), parametrizado
   Uso: EXEC SP_HistorialCitas @ID_Paciente = 'PAC-0001';
   Lo resuelve el índice agrupado CIX_informeCita_paciente_fecha.
   --------------------------------------------------------------------- */
CREATE PROCEDURE SP_HistorialCitas
    @ID_Paciente varchar(10)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        pa.nombres + ' ' + pa.apellidos             AS paciente,      -- encabezado de la Figura 4
        ic.numeroPaciente,
        ic.numeroCita,
        ic.fecha,
        CONVERT(varchar(5), ic.hora, 108)           AS hora,
        e.nombres + ' ' + e.apellidos               AS consultor,
        pu.nombre + ' / ' + es.nombre               AS puestoEspecialidad,
        s.nombre                                    AS sala
    FROM informeCita ic
    JOIN paciente     pa ON pa.numeroPaciente     = ic.numeroPaciente
    JOIN empleado     e  ON e.numeroEmpleado      = ic.numeroEmpleado
    JOIN puesto       pu ON pu.codigoPuesto       = e.codigoPuesto
    JOIN especialidad es ON es.codigoEspecialidad = ic.codigoEspecialidad
    JOIN sala         s  ON s.codigoSala          = ic.codigoSala
    WHERE ic.numeroPaciente = @ID_Paciente
    ORDER BY ic.fecha, ic.hora;
END;
GO

/* ---------------------------------------------------------------------
   Query 3 — Alerta de stock crítico de insumos
   Existencia actual (suministroUbicacion.cantidadDisponible) por debajo del
   15 % de la capacidad normal (informeSuministroIngreso.cantidadOrdenada),
   con los datos de contacto del proveedor para reabastecer.
   Uso: SELECT * FROM VW_Stock_Critico ORDER BY porcentaje;
   --------------------------------------------------------------------- */
CREATE VIEW VW_Stock_Critico AS
SELECT
    su.codigoSuministro,
    su.nombre                                       AS suministro,
    su.tipo,
    u.nombre                                        AS ubicacion,
    sb.cantidadDisponible                           AS existencia,
    ig.cantidadOrdenada                             AS capacidadNormal,
    CAST(100.0 * sb.cantidadDisponible / ig.cantidadOrdenada AS dec(5,1)) AS porcentaje,
    pr.nombre                                       AS proveedor,
    pr.contacto,
    pr.telefono,
    pr.correo
FROM suministroUbicacion sb
JOIN informeSuministroIngreso ig ON ig.codigoUbicacion = sb.codigoUbicacion
                                AND ig.codigoSuministro = sb.codigoSuministro
JOIN suministro su ON su.codigoSuministro = sb.codigoSuministro
JOIN ubicacion  u  ON u.codigoUbicacion   = sb.codigoUbicacion
JOIN proveedor  pr ON pr.codigoProveedor  = su.codigoProveedor
WHERE sb.cantidadDisponible < 0.15 * ig.cantidadOrdenada;
GO

/* ---------------------------------------------------------------------
   Registro de asignación de camas (Figura 3)
   --------------------------------------------------------------------- */
CREATE VIEW VW_Registro_Camas AS
SELECT
    ac.numeroIngreso,
    ac.numeroPaciente,
    p.nombres + ' ' + p.apellidos                   AS paciente,
    ac.codigoSala,
    s.nombre                                        AS sala,
    ac.numeroCama,
    ac.fechaIngreso,
    ac.fechaSalida,
    CASE WHEN ac.codigoEstado = 1 THEN 'Internado' ELSE 'Alta Médica' END AS estado,
    ac.nivelRiesgo                                  -- calculado desde la telemetría (Fase II)
FROM asignacionCama ac
JOIN paciente p ON p.numeroPaciente = ac.numeroPaciente
JOIN sala     s ON s.codigoSala     = ac.codigoSala;
GO
