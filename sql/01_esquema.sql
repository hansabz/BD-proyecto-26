/* =====================================================================
   HOSPITAL "W" 2.0 — Esquema transaccional (SQL Server)
   Basado en queryProyecto.sql. Cada cambio respecto al script original
   está marcado con [AJUSTE] y el motivo, para poder defenderlo en el
   informe técnico.
   ===================================================================== */

-- Catálogos -----------------------------------------------------------

CREATE TABLE puesto(
    codigoPuesto    varchar(3)  NOT NULL,
    nombre          varchar(25) NOT NULL,   -- [AJUSTE] 10 -> 25: "Enfermera Jefe" (14) no entraba
    salario         dec(7,2)    NOT NULL,
    CONSTRAINT PK_puesto PRIMARY KEY (codigoPuesto),
    CONSTRAINT CK_puesto_salario CHECK (salario > 0)
);
GO

CREATE TABLE especialidad(
    codigoEspecialidad  varchar(5)  NOT NULL,
    nombre              varchar(25) NOT NULL,
    CONSTRAINT PK_especialidad PRIMARY KEY (codigoEspecialidad)
);
GO

CREATE TABLE turno(
    codigoTurno varchar(5)  NOT NULL,
    nombre      varchar(15) NOT NULL,
    CONSTRAINT PK_turno PRIMARY KEY (codigoTurno)
);
GO

-- RF1 — Personal ------------------------------------------------------

CREATE TABLE empleado(
    numeroEmpleado      varchar(5)  NOT NULL,   -- M1001 / E1001 / A1001 (generado por PySpark)
    nombres             varchar(25) NOT NULL,
    apellidos           varchar(25) NOT NULL,
    NSS                 varchar(8)  NOT NULL,
    codigoPuesto        varchar(3)  NOT NULL,
    codigoEspecialidad  varchar(5)  NOT NULL,
    codigoTurno         varchar(5)  NOT NULL,
    fechaContratacion   date        NOT NULL,   -- del script actualizado; base de la antigüedad
    activo              bit         NOT NULL CONSTRAINT DF_empleado_activo DEFAULT 1,       -- [AJUSTE] baja lógica (RF1)
    fechaBaja           datetime    NULL,                                                   -- [AJUSTE]
    CONSTRAINT PK_empleado PRIMARY KEY (numeroEmpleado),
    CONSTRAINT UQ_empleado_NSS UNIQUE (NSS),
    CONSTRAINT CK_empleado_NSS CHECK (NSS LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]-[0-9A-Z]'), -- formato Figura 1: 883920-K
    CONSTRAINT CK_empleado_contratacion CHECK (fechaContratacion <= CAST(GETDATE() AS date)),
    CONSTRAINT CK_empleado_baja CHECK ((activo = 1 AND fechaBaja IS NULL) OR (activo = 0 AND fechaBaja IS NOT NULL)),
    CONSTRAINT FK_empleado_puesto FOREIGN KEY (codigoPuesto) REFERENCES puesto(codigoPuesto),
    CONSTRAINT FK_empleado_especialidad FOREIGN KEY (codigoEspecialidad) REFERENCES especialidad(codigoEspecialidad),
    CONSTRAINT FK_empleado_turno FOREIGN KEY (codigoTurno) REFERENCES turno(codigoTurno)
);
GO

CREATE TABLE sala(
    codigoSala          varchar(5)  NOT NULL,
    nombre              varchar(45) NOT NULL,
    camasDisponibles    int         NOT NULL,   -- capacidad instalada de la sala
    CONSTRAINT PK_sala PRIMARY KEY (codigoSala),
    CONSTRAINT CK_sala_camas CHECK (camasDisponibles >= 0)
);
GO

CREATE TABLE asignacionSala(
    codigoSala      varchar(5) NOT NULL,
    numeroEmpleado  varchar(5) NOT NULL,
    CONSTRAINT PK_asignacionSala PRIMARY KEY (codigoSala, numeroEmpleado),
    CONSTRAINT FK_asignacionSala_sala FOREIGN KEY (codigoSala) REFERENCES sala(codigoSala),
    CONSTRAINT FK_asignacionSala_empleado FOREIGN KEY (numeroEmpleado) REFERENCES empleado(numeroEmpleado)
);
GO

-- RF2 — Pacientes -----------------------------------------------------

CREATE TABLE paciente(
    numeroPaciente  varchar(10) NOT NULL,   -- [AJUSTE] 5 -> 10: formato PAC-0001 (Figura 2)
    nombres         varchar(25) NOT NULL,
    apellidos       varchar(25) NOT NULL,
    fechaNac        date        NOT NULL,
    sexo            char(1)     NULL,       -- [AJUSTE] dato demográfico (RF2)
    telefono        varchar(20) NULL,
    direccion       varchar(50) NULL,
    CONSTRAINT PK_paciente PRIMARY KEY (numeroPaciente),
    CONSTRAINT CK_paciente_sexo CHECK (sexo IN ('M', 'F')),
    CONSTRAINT CK_paciente_fechaNac CHECK (fechaNac <= CAST(GETDATE() AS date))
);
GO

CREATE TABLE registroIngreso(
    numeroIngreso   varchar(10) NOT NULL,   -- [AJUSTE] 5 -> 10: formato ING-001 (Figura 3)
    numeroPaciente  varchar(10) NOT NULL,   -- [AJUSTE] NOT NULL: un ingreso sin paciente no tiene sentido
    fechaReg        datetime    NOT NULL CONSTRAINT DF_registroIngreso_fechaReg DEFAULT GETDATE(), -- [AJUSTE] automática
    nombreFam       varchar(40) NULL,       -- [AJUSTE] 25 -> 40: nombre completo del familiar
    parentescoFam   varchar(15) NULL,       -- [AJUSTE] 10 -> 15
    telefonoFam     varchar(20) NULL,
    medicoRef       varchar(30) NOT NULL,
    clinicaRef      varchar(35) NOT NULL,
    CONSTRAINT PK_registroIngreso PRIMARY KEY (numeroIngreso),
    CONSTRAINT FK_registroIngreso_paciente FOREIGN KEY (numeroPaciente) REFERENCES paciente(numeroPaciente)
);
GO

-- RF3 — Hospitalización ----------------------------------------------

CREATE TABLE cama(
    numeroCama      varchar(6) NOT NULL,    -- [AJUSTE] 4 -> 6: formato C-012 (Figura 3)
    codigoSala      varchar(5) NOT NULL,
    codigoEstado    bit        NOT NULL CONSTRAINT DF_cama_estado DEFAULT 0,  -- 0 = disponible, 1 = ocupada
    CONSTRAINT PK_cama PRIMARY KEY (numeroCama, codigoSala),
    CONSTRAINT FK_cama_sala FOREIGN KEY (codigoSala) REFERENCES sala(codigoSala)
);
GO

CREATE TABLE asignacionCama(
    numeroIngreso   varchar(10) NOT NULL,
    numeroPaciente  varchar(10) NOT NULL,
    numeroCama      varchar(6)  NOT NULL,
    codigoSala      varchar(5)  NOT NULL,
    fechaIngreso    datetime    NOT NULL CONSTRAINT DF_asignacionCama_fechaIngreso DEFAULT GETDATE(), -- [AJUSTE] automática (RF3)
    fechaSalida     datetime    NULL,
    nivelRiesgo     varchar(10) NULL,   -- del script: lo calcula PySpark con los logs de telemetría
                                        -- (Query BD 3). NULL = todavía sin lecturas.
    codigoEstado    bit         NOT NULL CONSTRAINT DF_asignacionCama_estado DEFAULT 1,  -- 1 = Internado, 0 = Alta médica
    CONSTRAINT PK_asignacionCama PRIMARY KEY (numeroIngreso),
    CONSTRAINT CK_asignacionCama_riesgo CHECK (nivelRiesgo IN ('Bajo', 'Moderado', 'Crítico')),
    CONSTRAINT CK_asignacionCama_fechas CHECK (fechaSalida IS NULL OR fechaSalida >= fechaIngreso),
    CONSTRAINT CK_asignacionCama_estado CHECK ((codigoEstado = 1 AND fechaSalida IS NULL) OR (codigoEstado = 0 AND fechaSalida IS NOT NULL)),
    CONSTRAINT FK_asignacionCama_registroIngreso FOREIGN KEY (numeroIngreso) REFERENCES registroIngreso(numeroIngreso),
    CONSTRAINT FK_asignacionCama_paciente FOREIGN KEY (numeroPaciente) REFERENCES paciente(numeroPaciente),
    CONSTRAINT FK_asignacionCama_cama FOREIGN KEY (numeroCama, codigoSala) REFERENCES cama(numeroCama, codigoSala)
);
GO

-- RF3: el motor mismo impide dos internaciones activas en la misma cama
-- (y un paciente en dos camas a la vez), aunque la aplicación falle.
CREATE UNIQUE NONCLUSTERED INDEX UX_asignacionCama_camaOcupada
    ON asignacionCama (numeroCama, codigoSala) WHERE codigoEstado = 1;
GO
CREATE UNIQUE NONCLUSTERED INDEX UX_asignacionCama_pacienteInternado
    ON asignacionCama (numeroPaciente) WHERE codigoEstado = 1;
GO


CREATE TABLE informeCita(
    numeroCita          varchar(5)  NOT NULL,
    numeroPaciente      varchar(10) NOT NULL,
    fecha               date        NOT NULL,
    hora                time        NOT NULL,
    numeroEmpleado      varchar(5)  NOT NULL,
    codigoEspecialidad  varchar(5)  NOT NULL,
    codigoSala          varchar(5)  NOT NULL,
    -- [AJUSTE] PK NO AGRUPADA: el índice agrupado de esta tabla se reserva para
    -- (numeroPaciente, fecha, hora), que es como la lee Query 2 (ver 02_indices_vistas.sql).
    CONSTRAINT PK_cita PRIMARY KEY NONCLUSTERED (numeroCita),
    CONSTRAINT FK_cita_paciente FOREIGN KEY (numeroPaciente) REFERENCES paciente(numeroPaciente),
    CONSTRAINT FK_cita_empleado FOREIGN KEY (numeroEmpleado) REFERENCES empleado(numeroEmpleado),
    CONSTRAINT FK_cita_especialidad FOREIGN KEY (codigoEspecialidad) REFERENCES especialidad(codigoEspecialidad),
    CONSTRAINT FK_cita_sala FOREIGN KEY (codigoSala) REFERENCES sala(codigoSala)
);
GO

-- RF4 — Inventario ----------------------------------------------------
-- Flujo con las tablas del script actualizado:
--   informeSuministroIngreso  qué insumo se maneja en qué ubicación y cuánto se ORDENA
--                             para tenerla llena (cantidadOrdenada = capacidad normal,
--                             base del 15 % de Query 3)
--   suministroUbicacion       cuánto hay HOY en esa ubicación (cantidadDisponible)
--   entradaSuministro         bitácora de entradas (reabastecimiento)
--   informeSuministroSalida   bitácora de salidas (a un empleado) y traslados entre almacenes

CREATE TABLE ubicacion(
    codigoUbicacion     varchar(5)  NOT NULL,
    nombre              varchar(25) NOT NULL,
    empleadoEncargado   varchar(5)  NOT NULL,
    CONSTRAINT PK_ubicacion PRIMARY KEY (codigoUbicacion),
    CONSTRAINT FK_ubicacion_empleado FOREIGN KEY (empleadoEncargado) REFERENCES empleado(numeroEmpleado)
);
GO

CREATE TABLE proveedor(                     -- [AJUSTE] tabla nueva: RF4 exige un proveedor "debidamente registrado"
    codigoProveedor varchar(5)  NOT NULL,   -- y Query 3 pide sus datos de contacto
    nombre          varchar(30) NOT NULL,
    contacto        varchar(40) NULL,
    telefono        varchar(20) NOT NULL,
    correo          varchar(50) NULL,
    CONSTRAINT PK_proveedor PRIMARY KEY (codigoProveedor),
    CONSTRAINT UQ_proveedor_nombre UNIQUE (nombre)
);
GO

CREATE TABLE suministro(
    codigoSuministro    varchar(5)  NOT NULL,
    nombre              varchar(30) NOT NULL,
    descripcion         varchar(50) NULL,
    tipo                char(1)     NOT NULL,   -- [AJUSTE] Q = quirúrgico, F = farmacéutico, M = material médico (prefijo del ID)
    costoUnitario       dec(7,2)    NOT NULL,   -- [AJUSTE] 5,2 -> 7,2: tope de Q.999.99 era muy bajo
    codigoProveedor     varchar(5)  NOT NULL,   -- [AJUSTE] antes "proveedor varchar(30)" sin FK
    CONSTRAINT PK_suministro PRIMARY KEY (codigoSuministro),
    CONSTRAINT CK_suministro_tipo CHECK (tipo IN ('Q', 'F', 'M')),
    CONSTRAINT CK_suministro_costo CHECK (costoUnitario > 0),
    CONSTRAINT FK_suministro_proveedor FOREIGN KEY (codigoProveedor) REFERENCES proveedor(codigoProveedor)
);
GO

CREATE TABLE informeSuministroIngreso(      -- tal cual el script
    codigoUbicacion     varchar(5) NOT NULL,
    codigoSuministro    varchar(5) NOT NULL,
    cantidadOrdenada    int        NOT NULL,   -- cantidad normal que se ordena = capacidad normal
    CONSTRAINT PK_informeSuministroIngreso PRIMARY KEY (codigoUbicacion, codigoSuministro),
    CONSTRAINT CK_informeSuministroIngreso_cantidad CHECK (cantidadOrdenada > 0),
    CONSTRAINT FK_informeSuministroIngreso_ubicacion FOREIGN KEY (codigoUbicacion) REFERENCES ubicacion(codigoUbicacion),
    CONSTRAINT FK_informeSuministroIngreso_suministro FOREIGN KEY (codigoSuministro) REFERENCES suministro(codigoSuministro)
);
GO

CREATE TABLE suministroUbicacion(           -- tal cual el script
    codigoUbicacion     varchar(5) NOT NULL,
    codigoSuministro    varchar(5) NOT NULL,
    cantidadDisponible  int        NOT NULL,
    CONSTRAINT PK_suministroUbicacion PRIMARY KEY (codigoUbicacion, codigoSuministro),
    CONSTRAINT CK_suministroUbicacion_cantidad CHECK (cantidadDisponible >= 0),   -- [AJUSTE] nunca stock negativo
    CONSTRAINT FK_suministroUbicacion_ubicacion FOREIGN KEY (codigoUbicacion, codigoSuministro)
        REFERENCES informeSuministroIngreso(codigoUbicacion, codigoSuministro)
);
GO

CREATE TABLE entradaSuministro(             -- [AJUSTE] bitácora de entradas (el script no tenía dónde registrarlas)
    idEntrada           int IDENTITY(1,1) NOT NULL,
    codigoUbicacion     varchar(5) NOT NULL,
    codigoSuministro    varchar(5) NOT NULL,
    cantidad            int        NOT NULL,
    fecha               datetime   NOT NULL CONSTRAINT DF_entrada_fecha DEFAULT GETDATE(),
    -- PK no agrupada: el índice agrupado va sobre fecha (ver 02_indices_vistas.sql)
    CONSTRAINT PK_entradaSuministro PRIMARY KEY NONCLUSTERED (idEntrada),
    CONSTRAINT CK_entrada_cantidad CHECK (cantidad > 0),
    CONSTRAINT FK_entrada_suministroUbicacion FOREIGN KEY (codigoUbicacion, codigoSuministro)
        REFERENCES suministroUbicacion(codigoUbicacion, codigoSuministro)
);
GO

CREATE TABLE informeSuministroSalida(
    idSalida                int IDENTITY(1,1) NOT NULL,  -- [AJUSTE] con PK (fuente, insumo) solo se podía
                                                         -- despachar UNA vez cada insumo desde cada ubicación
    codigoUbicacionFuente   varchar(5)  NOT NULL,
    codigoSuministro        varchar(5)  NOT NULL,
    cantidadDespachada      int         NOT NULL,
    codigoUbicacionDestino  varchar(5)  NULL,      -- [AJUSTE] NULL = salida/consumo; con valor = traslado entre almacenes
    empleadoDestinatario    varchar(5)  NOT NULL,  -- [AJUSTE] quién recibe (cualquier empleado registrado)
    fecha                   datetime    NOT NULL CONSTRAINT DF_salida_fecha DEFAULT GETDATE(),   -- [AJUSTE]
    CONSTRAINT PK_informeSuministroSalida PRIMARY KEY NONCLUSTERED (idSalida),
    CONSTRAINT CK_salida_cantidad CHECK (cantidadDespachada > 0),
    CONSTRAINT CK_salida_destino CHECK (codigoUbicacionDestino IS NULL OR codigoUbicacionDestino <> codigoUbicacionFuente),
    CONSTRAINT FK_informeSuministroSalida_suministroUbicacion FOREIGN KEY (codigoUbicacionFuente, codigoSuministro)
        REFERENCES suministroUbicacion(codigoUbicacion, codigoSuministro),
    CONSTRAINT FK_informeSuministroSalida_ubicacion FOREIGN KEY (codigoUbicacionDestino) REFERENCES ubicacion(codigoUbicacion),
    CONSTRAINT FK_informeSuministroSalida_empleado FOREIGN KEY (empleadoDestinatario) REFERENCES empleado(numeroEmpleado)
);
GO
