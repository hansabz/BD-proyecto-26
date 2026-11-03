/* =====================================================================
   HOSPITAL "W" 2.0 — Esquema transaccional (SQL Server)
   Basado en queryProyecto.sql. Cada cambio respecto al script original
   está marcado con [AJUSTE] y el motivo, para poder defenderlo en el
   informe técnico.

   Al final de cada tabla hay un bloque "CHECK y DEFAULT" con un CHECK y
   un DEFAULT con nombre propio:
     CHECK    regla que toda fila debe cumplir; si no, SQL Server rechaza
              el INSERT/UPDATE (ej. un código con formato incorrecto).
     DEFAULT  valor que se guarda cuando el INSERT no manda esa columna
              (ej. la fecha de hoy).
   Se declaran con ALTER TABLE porque en SQL Server un DEFAULT no se puede
   escribir como restricción de tabla dentro del CREATE TABLE.
   ===================================================================== */

-- Catálogos -----------------------------------------------------------

CREATE TABLE puesto(
    codigoPuesto    varchar(3)  NOT NULL,
    nombre          varchar(25) NOT NULL,   -- [AJUSTE] 10 -> 25: "Enfermera Jefe" (14) no entraba
    salario         dec(7,2)    NOT NULL,
    CONSTRAINT PK_puesto PRIMARY KEY (codigoPuesto),
    CONSTRAINT CK_puesto_salario CHECK (salario > 0)
);
-- CHECK y DEFAULT
ALTER TABLE puesto ADD CONSTRAINT CK_puesto_codigo  CHECK (LEN(codigoPuesto) = 3);          -- códigos de 3 letras: MED, ENF...
ALTER TABLE puesto ADD CONSTRAINT DF_puesto_salario DEFAULT 3500.00 FOR salario;            -- salario base mínimo
GO

CREATE TABLE especialidad(
    codigoEspecialidad  varchar(5)  NOT NULL,
    nombre              varchar(25) NOT NULL,
    CONSTRAINT PK_especialidad PRIMARY KEY (codigoEspecialidad)
);
-- CHECK y DEFAULT
ALTER TABLE especialidad ADD CONSTRAINT CK_especialidad_codigo CHECK (codigoEspecialidad LIKE 'ESP[0-9][0-9]');  -- ESP01..ESP99
ALTER TABLE especialidad ADD CONSTRAINT DF_especialidad_nombre DEFAULT 'Medicina General' FOR nombre;
GO

CREATE TABLE turno(
    codigoTurno varchar(5)  NOT NULL,
    nombre      varchar(15) NOT NULL,
    CONSTRAINT PK_turno PRIMARY KEY (codigoTurno)
);
-- CHECK y DEFAULT
ALTER TABLE turno ADD CONSTRAINT CK_turno_nombre CHECK (nombre IN ('Matutino', 'Vespertino', 'Nocturno', 'Mixto'));
ALTER TABLE turno ADD CONSTRAINT DF_turno_nombre DEFAULT 'Matutino' FOR nombre;
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
-- CHECK y DEFAULT
ALTER TABLE empleado ADD CONSTRAINT CK_empleado_nombres CHECK (LTRIM(RTRIM(nombres)) <> '' AND LTRIM(RTRIM(apellidos)) <> '');
ALTER TABLE empleado ADD CONSTRAINT DF_empleado_contratacion DEFAULT CAST(GETDATE() AS date) FOR fechaContratacion;  -- contratado hoy
GO

CREATE TABLE sala(
    codigoSala          varchar(5)  NOT NULL,
    nombre              varchar(45) NOT NULL,
    camasDisponibles    int         NOT NULL,   -- capacidad instalada de la sala
    CONSTRAINT PK_sala PRIMARY KEY (codigoSala),
    CONSTRAINT CK_sala_camas CHECK (camasDisponibles >= 0)
);
-- CHECK y DEFAULT
ALTER TABLE sala ADD CONSTRAINT CK_sala_codigo CHECK (codigoSala LIKE 'S[0-9][0-9]');      -- S01..S99
ALTER TABLE sala ADD CONSTRAINT DF_sala_camas  DEFAULT 0 FOR camasDisponibles;            -- sala sin camas (ej. consulta externa)
GO

CREATE TABLE asignacionSala(
    codigoSala      varchar(5) NOT NULL,
    numeroEmpleado  varchar(5) NOT NULL,
    fechaAsignacion date       NOT NULL,        -- [AJUSTE] desde cuándo trabaja el empleado en esa sala
    CONSTRAINT PK_asignacionSala PRIMARY KEY (codigoSala, numeroEmpleado),
    CONSTRAINT FK_asignacionSala_sala FOREIGN KEY (codigoSala) REFERENCES sala(codigoSala),
    CONSTRAINT FK_asignacionSala_empleado FOREIGN KEY (numeroEmpleado) REFERENCES empleado(numeroEmpleado)
);
-- CHECK y DEFAULT
ALTER TABLE asignacionSala ADD CONSTRAINT CK_asignacionSala_fecha CHECK (fechaAsignacion <= CAST(GETDATE() AS date));
ALTER TABLE asignacionSala ADD CONSTRAINT DF_asignacionSala_fecha DEFAULT CAST(GETDATE() AS date) FOR fechaAsignacion;
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
-- CHECK y DEFAULT
-- Teléfono: solo dígitos, espacios, guiones y "+" (el patrón busca CUALQUIER otro carácter).
ALTER TABLE paciente ADD CONSTRAINT CK_paciente_telefono CHECK (telefono IS NULL OR telefono NOT LIKE '%[^0-9 +-]%');
ALTER TABLE paciente ADD CONSTRAINT DF_paciente_direccion DEFAULT 'Sin dirección registrada' FOR direccion;
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
-- CHECK y DEFAULT
ALTER TABLE registroIngreso ADD CONSTRAINT CK_registroIngreso_telefonoFam CHECK (telefonoFam IS NULL OR telefonoFam NOT LIKE '%[^0-9 +-]%');
ALTER TABLE registroIngreso ADD CONSTRAINT DF_registroIngreso_parentesco DEFAULT 'Otro' FOR parentescoFam;
GO

-- RF3 — Hospitalización ----------------------------------------------

CREATE TABLE cama(
    numeroCama      varchar(6) NOT NULL,    -- [AJUSTE] 4 -> 6: formato C-012 (Figura 3)
    codigoSala      varchar(5) NOT NULL,
    codigoEstado    bit        NOT NULL,    -- 0 = disponible, 1 = ocupada
    CONSTRAINT PK_cama PRIMARY KEY (numeroCama, codigoSala),
    CONSTRAINT FK_cama_sala FOREIGN KEY (codigoSala) REFERENCES sala(codigoSala)
);
-- CHECK y DEFAULT
ALTER TABLE cama ADD CONSTRAINT CK_cama_numero CHECK (numeroCama LIKE 'C-[0-9][0-9][0-9]');  -- C-001..C-999
ALTER TABLE cama ADD CONSTRAINT DF_cama_estado DEFAULT 0 FOR codigoEstado;                   -- una cama nueva nace disponible
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
    codigoEstado    bit         NOT NULL,   -- 1 = Internado, 0 = Alta médica
    CONSTRAINT PK_asignacionCama PRIMARY KEY (numeroIngreso),
    CONSTRAINT CK_asignacionCama_riesgo CHECK (nivelRiesgo IN ('Bajo', 'Moderado', 'Crítico')),
    CONSTRAINT CK_asignacionCama_fechas CHECK (fechaSalida IS NULL OR fechaSalida >= fechaIngreso),
    CONSTRAINT CK_asignacionCama_estado CHECK ((codigoEstado = 1 AND fechaSalida IS NULL) OR (codigoEstado = 0 AND fechaSalida IS NOT NULL)),
    CONSTRAINT FK_asignacionCama_registroIngreso FOREIGN KEY (numeroIngreso) REFERENCES registroIngreso(numeroIngreso),
    CONSTRAINT FK_asignacionCama_paciente FOREIGN KEY (numeroPaciente) REFERENCES paciente(numeroPaciente),
    CONSTRAINT FK_asignacionCama_cama FOREIGN KEY (numeroCama, codigoSala) REFERENCES cama(numeroCama, codigoSala)
);
-- CHECK y DEFAULT
ALTER TABLE asignacionCama ADD CONSTRAINT CK_asignacionCama_ingreso CHECK (numeroIngreso LIKE 'ING-[0-9]%');
ALTER TABLE asignacionCama ADD CONSTRAINT DF_asignacionCama_estado DEFAULT 1 FOR codigoEstado;  -- al asignar, queda internado
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
-- CHECK y DEFAULT
ALTER TABLE informeCita ADD CONSTRAINT CK_cita_hora  CHECK (hora BETWEEN '07:00' AND '19:00');   -- horario de consulta externa
ALTER TABLE informeCita ADD CONSTRAINT DF_cita_fecha DEFAULT CAST(GETDATE() AS date) FOR fecha;
GO

-- RF4 — Inventario ----------------------------------------------------
-- Tres tablas separadas, según el diseño del grupo:
--   informeSuministroIngreso  una fila por cada ENTRADA (noEntrada): a qué almacén y cuánto ingresó
--   suministroUbicacion       cuánto hay HOY (cantidadDisponible) y cuánto DEBERÍA haber
--                             (cantidadIdeal, base del 15 % de la Query 3)
--   informeSuministroSalida   una fila por cada SALIDA (noSalida): consumo o traslado entre almacenes
-- Las tres usan idSuministro como FK hacia suministro(codigoSuministro).

CREATE TABLE ubicacion(
    codigoUbicacion     varchar(5)  NOT NULL,
    nombre              varchar(25) NOT NULL,
    empleadoEncargado   varchar(5)  NOT NULL,
    CONSTRAINT PK_ubicacion PRIMARY KEY (codigoUbicacion),
    CONSTRAINT FK_ubicacion_empleado FOREIGN KEY (empleadoEncargado) REFERENCES empleado(numeroEmpleado)
);
-- CHECK y DEFAULT
ALTER TABLE ubicacion ADD CONSTRAINT CK_ubicacion_codigo CHECK (codigoUbicacion LIKE 'U[0-9][0-9]');   -- U01..U99
ALTER TABLE ubicacion ADD CONSTRAINT DF_ubicacion_nombre DEFAULT 'Almacén General' FOR nombre;
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
-- CHECK y DEFAULT
ALTER TABLE proveedor ADD CONSTRAINT CK_proveedor_correo CHECK (correo IS NULL OR correo LIKE '%_@_%._%');  -- algo@algo.algo
ALTER TABLE proveedor ADD CONSTRAINT DF_proveedor_contacto DEFAULT 'Sin contacto asignado' FOR contacto;
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
-- CHECK y DEFAULT
ALTER TABLE suministro ADD CONSTRAINT CK_suministro_prefijo CHECK (LEFT(codigoSuministro, 1) = tipo);  -- F001 debe ser tipo F
ALTER TABLE suministro ADD CONSTRAINT DF_suministro_descripcion DEFAULT 'Sin descripción' FOR descripcion;
GO

CREATE TABLE informeSuministroIngreso(      -- bitácora de entradas: UNA fila por cada ingreso de mercadería
    noEntrada           int IDENTITY(1,1) NOT NULL,
    codigoUbicacion     varchar(5) NOT NULL,       -- almacén que recibe
    idSuministro        varchar(5) NOT NULL,
    cantidadOrdenada    int        NOT NULL,       -- unidades que ingresaron en esta entrada
    fecha               datetime   NOT NULL,       -- [AJUSTE] cuándo ingresó (bitácora, índice agrupado)
    -- PK no agrupada: el índice agrupado va sobre fecha (ver 02_indices_vistas.sql)
    CONSTRAINT PK_informeSuministroIngreso PRIMARY KEY NONCLUSTERED (noEntrada),
    CONSTRAINT FK_informeSuministroIngreso_ubicacion FOREIGN KEY (codigoUbicacion) REFERENCES ubicacion(codigoUbicacion),
    CONSTRAINT FK_informeSuministroIngreso_suministro FOREIGN KEY (idSuministro) REFERENCES suministro(codigoSuministro)
);
-- CHECK y DEFAULT
ALTER TABLE informeSuministroIngreso ADD CONSTRAINT CK_informeSuministroIngreso_cantidad CHECK (cantidadOrdenada > 0);
ALTER TABLE informeSuministroIngreso ADD CONSTRAINT CK_informeSuministroIngreso_fecha CHECK (fecha <= GETDATE());  -- no a futuro
ALTER TABLE informeSuministroIngreso ADD CONSTRAINT DF_informeSuministroIngreso_fecha DEFAULT GETDATE() FOR fecha;
GO

CREATE TABLE suministroUbicacion(           -- stock actual de cada insumo en cada almacén
    codigoUbicacion     varchar(5) NOT NULL,
    idSuministro        varchar(5) NOT NULL,
    cantidadDisponible  int        NOT NULL,   -- stock actual
    cantidadIdeal       int        NOT NULL,   -- nivel ideal en esta ubicación: la Query 3 compara contra este valor
    CONSTRAINT PK_suministroUbicacion PRIMARY KEY (codigoUbicacion, idSuministro),
    CONSTRAINT CK_suministroUbicacion_cantidad CHECK (cantidadDisponible >= 0),   -- nunca stock negativo
    CONSTRAINT FK_suministroUbicacion_ubicacion FOREIGN KEY (codigoUbicacion) REFERENCES ubicacion(codigoUbicacion),
    CONSTRAINT FK_suministroUbicacion_suministro FOREIGN KEY (idSuministro) REFERENCES suministro(codigoSuministro)
);
-- CHECK y DEFAULT
ALTER TABLE suministroUbicacion ADD CONSTRAINT CK_suministroUbicacion_ideal CHECK (cantidadIdeal > 0);  -- evita dividir entre 0
ALTER TABLE suministroUbicacion ADD CONSTRAINT DF_suministroUbicacion_disponible DEFAULT 0 FOR cantidadDisponible;
GO

CREATE TABLE informeSuministroSalida(       -- bitácora de salidas (consumo) y traslados entre almacenes
    noSalida                int IDENTITY(1,1) NOT NULL,
    codigoUbicacionFuente   varchar(5)  NOT NULL,
    idSuministro            varchar(5)  NOT NULL,
    cantidadDespachada      int         NOT NULL,
    codigoUbicacionDestino  varchar(5)  NULL,      -- NULL = salida/consumo; con valor = traslado a otro almacén
    fecha                   datetime    NOT NULL,  -- [AJUSTE] cuándo salió (bitácora, índice agrupado)
    CONSTRAINT PK_informeSuministroSalida PRIMARY KEY NONCLUSTERED (noSalida),
    CONSTRAINT CK_salida_cantidad CHECK (cantidadDespachada > 0),
    CONSTRAINT CK_salida_destino CHECK (codigoUbicacionDestino IS NULL OR codigoUbicacionDestino <> codigoUbicacionFuente),
    CONSTRAINT FK_informeSuministroSalida_fuente FOREIGN KEY (codigoUbicacionFuente) REFERENCES ubicacion(codigoUbicacion),
    CONSTRAINT FK_informeSuministroSalida_suministro FOREIGN KEY (idSuministro) REFERENCES suministro(codigoSuministro),
    CONSTRAINT FK_informeSuministroSalida_destino FOREIGN KEY (codigoUbicacionDestino) REFERENCES ubicacion(codigoUbicacion)
);
-- CHECK y DEFAULT
ALTER TABLE informeSuministroSalida ADD CONSTRAINT CK_salida_fecha CHECK (fecha <= GETDATE());   -- no se registran salidas a futuro
ALTER TABLE informeSuministroSalida ADD CONSTRAINT DF_salida_fecha DEFAULT GETDATE() FOR fecha;  -- fecha y hora reales del movimiento
GO
