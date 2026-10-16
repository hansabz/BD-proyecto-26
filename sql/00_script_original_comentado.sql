/* =====================================================================================
   HOSPITAL "W" 2.0 — Script original del grupo (queryProyecto.sql), COMENTADO
   -------------------------------------------------------------------------------------
   Este es el diseño de base de datos del grupo, explicado tabla por tabla.

   IMPORTANTE: la aplicación NO usa este archivo, usa sql/01_esquema.sql, que parte de
   este mismo diseño pero con algunos ajustes necesarios para cumplir la guía
   (marcados [AJUSTE] en ese archivo). Al final de cada tabla hay una nota
   "En 01_esquema.sql" que dice qué cambia y por qué.

   Conceptos que se repiten:
     PRIMARY KEY (PK)  identifica cada fila; no se repite ni puede ser NULL.
                       En SQL Server crea además el índice AGRUPADO de la tabla.
     FOREIGN KEY (FK)  obliga a que el valor exista en otra tabla (integridad referencial):
                       no se puede asignar un empleado a una sala que no existe.
     NOT NULL          el dato es obligatorio.
     UNIQUE            el valor no se puede repetir entre filas (ej. el NSS).
   ===================================================================================== */

-- Crea la base y se posiciona en ella. (En SSMS conviene ejecutar estas dos líneas solas
-- primero: el resto del script necesita que la base ya exista.)
create database hospitalProyecto
use hospitalProyecto


/* =====================================================================================
   CATÁLOGOS: listas fijas de valores válidos que usan las demás tablas.
   ===================================================================================== */

-- Puestos de trabajo (Médico, Enfermera, Auxiliar...) con su salario base.
-- El salario vive acá y no en empleado: todos los del mismo puesto cobran lo mismo
-- (evita repetir el dato y cumple la Tercera Forma Normal).
create table puesto(
codigoPuesto	varchar(3),             -- ej. 'MED', 'ENF', 'AUX'
nombre			varchar(10) not null,   -- nombre del puesto
salario			dec(7,2) not null,      -- salario base mensual en quetzales (hasta 99,999.99)
constraint PK_puesto primary key (codigoPuesto)
)
-- En 01_esquema.sql: nombre pasa a varchar(25) porque "Enfermera Jefe" (14 letras) no entraba.

-- Especialidades médicas (Cardiología, Geriatría, Nutrición...).
create table especialidad(
codigoEspecialidad	varchar(5),         -- ej. 'ESP01'
nombre				varchar(25) not null,
constraint PK_especialidad primary key (codigoEspecialidad)
)

-- Turnos laborales (Matutino, Vespertino, Nocturno, Mixto).
create table turno(
codigoTurno	varchar(5),                 -- ej. 'MAT'
nombre		varchar(15) not null,
constraint PK_turno primary key (codigoTurno)
)


/* =====================================================================================
   RF1 — PERSONAL
   ===================================================================================== */

-- Cada empleado del hospital (médicos, enfermeras, auxiliares).
-- RF1 exige: NSS único y turno + especialidad válidos según los catálogos -> por eso
-- NSS es UNIQUE y codigoPuesto/codigoEspecialidad/codigoTurno son FOREIGN KEY.
create table empleado(
numeroEmpleado		varchar(5),         -- ID: M1001 (médico), E1001 (enfermería), A1001 (auxiliar).
                                        -- Lo genera la app con PySpark según el puesto.
nombres				varchar(25) not null,
apellidos			varchar(25) not null,
NSS					varchar(8) UNIQUE not null,   -- Número de Seguridad Social, formato 883920-K
codigoPuesto		varchar(3) not null,          -- -> puesto
codigoEspecialidad	varchar(5) not null,          -- -> especialidad
codigoTurno			varchar(5) not null,          -- -> turno
fechaContratacion	date not null,                -- base para calcular la antigüedad
constraint PK_empleado primary key (numeroEmpleado),
constraint FK_empleado_puesto foreign key (codigoPuesto) references puesto(codigoPuesto),
constraint FK_empleado_especialidad foreign key (codigoEspecialidad) references especialidad(codigoEspecialidad),
constraint FK_empleado_turno foreign key (codigoTurno) references turno(codigoTurno)
)
-- En 01_esquema.sql se agregan: activo (bit) y fechaBaja, para la "baja lógica" de RF1
-- (el empleado se marca inactivo en vez de borrarse), y un CHECK del formato del NSS.

-- Salas del hospital (Unidad Gerontológica, Cuidados Intensivos...).
create table sala(
codigoSala			varchar(5),         -- ej. 'S01'
nombre				varchar(45) not null,
camasDisponibles	int not null,       -- capacidad de la sala (la app calcula las libres en vivo)
constraint PK_sala primary key (codigoSala)
)

-- Qué empleado trabaja en qué sala. Relación muchos a muchos: un empleado puede estar en
-- varias salas y una sala tiene varios empleados -> tabla intermedia con PK compuesta.
-- Es la base de la Query 1 (vista VW_Asignacion_Personal, Figura 1).
create table asignacionSala(
codigoSala		varchar(5),
numeroEmpleado	varchar(5),
constraint PK_asignacionSala primary key (codigoSala, numeroEmpleado),
constraint FK_asignacionSala_sala foreign key (codigoSala) references sala(codigoSala),
constraint FK_asignacionSala_empleado foreign key (numeroEMpleado) references empleado(numeroEmpleado)
)


/* =====================================================================================
   RF2 — PACIENTES Y ADMISIÓN (Figura 2)
   ===================================================================================== */

-- Datos demográficos del paciente (adulto mayor).
create table paciente(
numeroPaciente	varchar(5),             -- ID: PAC-0001 (lo genera la app con PySpark)
nombres			varchar(25) not null,
apellidos		varchar(25) not null,
fechaNac		date not null,
telefono		varchar(20),
direccion		varchar(50),
constraint PK_paciente primary key (numeroPaciente)
)
-- En 01_esquema.sql: numeroPaciente pasa a varchar(10) ("PAC-0001" tiene 8 caracteres)
-- y se agrega sexo (dato demográfico).

-- Cada vez que un paciente ingresa al hospital: fecha, familiar responsable y médico externo
-- que lo refiere (la ficha de la Figura 2). Un paciente puede tener varios ingresos.
create table registroIngreso(
numeroIngreso	varchar(5),             -- ID: ING-001
numeroPaciente	varchar(5),             -- -> paciente
fechaReg		datetime,               -- fecha y hora del registro
nombreFam		varchar(25),            -- familiar responsable / contacto de emergencia
parentescoFam	varchar(10),
telefonoFam		varchar(20),
medicoRef		varchar(30) not null,   -- médico externo que refiere
clinicaRef		varchar(35) not null,   -- clínica de ese médico
constraint PK_registroIngreso primary key (numeroIngreso),
constraint FK_registroIngreso_paciente foreign key (numeroPaciente) references paciente(numeroPaciente)
)
-- En 01_esquema.sql: fechaReg tiene DEFAULT GETDATE() (la pone SQL Server automáticamente).


/* =====================================================================================
   RF3 — HOSPITALIZACIÓN Y CAMAS (Figura 3)
   ===================================================================================== */

-- Camas de cada sala. La PK es (numeroCama, codigoSala) porque el mismo número de cama
-- (C-001) existe en varias salas.
create table cama(
numeroCama		varchar(4),
codigoSala		varchar(5) not null,    -- -> sala
codigoEstado	bit not null default 0,		-- 0 = disponible, 1 = ocupada
constraint PK_cama primary key (numeroCama, codigoSala),
constraint FK_cama_sala foreign key (codigoSala) references sala(codigoSala),
)
-- En 01_esquema.sql: numeroCama pasa a varchar(6) para el formato "C-012".

-- Internación: qué paciente (de qué ingreso) ocupa qué cama y desde/hasta cuándo.
create table asignacionCama(
numeroIngreso	varchar(5) not null,    -- -> registroIngreso (una internación por ingreso)
numeroPaciente	varchar(5) not null,    -- -> paciente
numeroCama		varchar(4) not null,    -- -> cama (junto con codigoSala)
codigoSala		varchar(5) not null,
fechaIngreso	datetime not null,      -- entrada real a la sala
fechaSalida		datetime,               -- salida real (NULL mientras sigue internado)
nivelRiesgo		varchar(10),            -- Bajo / Moderado / Crítico: lo calcula PySpark con los
                                        -- logs de telemetría (Fase II). Los logs NO se guardan acá.
codigoEstado	bit not null,           -- 1 = internado, 0 = alta médica
constraint PK_asignacionCama primary key (numeroIngreso),
constraint FK_asignacionCama_registroIngreso foreign key (numeroIngreso) references registroIngreso(numeroIngreso),
constraint FK_asignacionCama_paciente foreign key (numeroPaciente) references paciente(numeroPaciente),
constraint FK_asignacionCama_cama foreign key (numeroCama, codigoSala) references cama (numeroCama, codigoSala)
)
-- En 01_esquema.sql se agregan:
--   • fechaIngreso DEFAULT GETDATE() (RF3: fecha y hora automáticas)
--   • CHECKs de coherencia (salida >= ingreso; internado <-> sin fecha de salida)
--   • índices únicos filtrados que impiden dos internaciones activas en la misma cama
--     (RF3: "impedir asignar un paciente a una cama ya ocupada")

-- Citas ambulatorias: base de la Query 2 (historial de citas de un paciente, Figura 4).
create table informeCita(
numeroCita			varchar(5),
numeroPaciente		varchar(5) not null,    -- -> paciente
fecha				date not null,
hora				time not null,
numeroEmpleado		varchar(5) not null,    -- médico consultor -> empleado
codigoEspecialidad	varchar(5) not null,    -- -> especialidad
codigoSala			varchar(5) not null,    -- -> sala
constraint PK_cita primary key (numeroCita),
constraint FK_cita_paciente foreign key (numeroPaciente) references paciente(numeroPaciente),
constraint FK_cita_empleado foreign key (numeroEmpleado) references empleado(numeroEmpleado),
constraint FK_cita_especialidad foreign key (codigoEspecialidad) references especialidad(codigoEspecialidad),
constraint FK_cita_sala foreign key (codigoSala) references sala(codigoSala)
)
-- En 01_esquema.sql: el índice AGRUPADO va sobre (numeroPaciente, fecha, hora), que es
-- exactamente como lee la Query 2 (ver docs/INDICES.md: 2,506 -> 5 lecturas).


/* =====================================================================================
   RF4 — INVENTARIO DE SUMINISTROS (Figura 5)
   ===================================================================================== */

-- Almacenes / ubicaciones (Almacén Central, Farmacia, Quirófano...) con su encargado.
create table ubicacion(
codigoUbicacion		varchar(5),
nombre				varchar(25) not null,
empleadoEncargado	varchar(5) not null,    -- -> empleado
constraint PK_ubicacion primary key (codigoUbicacion),
constraint FK_ubicacion_empleado foreign key (empleadoEncargado) references empleado(numeroEmpleado)
)

-- Catálogo de insumos quirúrgicos y farmacéuticos.
create table suministro(
codigoSuministro	varchar(5),
nombre				varchar(30) not null,
descripcion			varchar(50),
costoUnitario		dec(5,2) not null,      -- costo por unidad (hasta 999.99)
proveedor			varchar(30) not null,   -- nombre del proveedor (texto libre)
constraint PK_suministro primary key (codigoSuministro)
)
-- En 01_esquema.sql: "proveedor" pasa a ser una FK hacia una tabla proveedor nueva
-- (RF4 pide un proveedor "debidamente registrado" y la Query 3 sus datos de contacto),
-- se agrega tipo (Q/F/M) y costoUnitario sube a dec(7,2).

-- Ficha de cada insumo en cada ubicación. cantidadOrdenada = la cantidad normal que se
-- ordena para tener la ubicación llena = "capacidad normal", base del 15 % de la Query 3.
create table informeSuministroIngreso(
codigoUbicacion		varchar(5) not null,
codigoSuministro	varchar(5) not null,
cantidadOrdenada	int	not null,
constraint PK_informeSuministroIngreso primary key (codigoUbicacion, codigoSuministro),
constraint FK_informeSuministroIngreso_ubicacion foreign key (codigoUbicacion) references ubicacion(codigoUbicacion),
constraint FK_informeSuministroIngreso_suministro foreign key (codigoSuministro) references suministro(codigoSuministro)
)

-- Existencia actual (stock) de cada insumo en cada ubicación.
create table suministroUbicacion(
codigoUbicacion		varchar(5) not null,
codigoSuministro	varchar(5) not null,
cantidadDisponible	int not null,
constraint PK_suministroUbicacion primary key (codigoUbicacion, codigoSuministro),
constraint FK_suministroUbicacion_ubicacion foreign key (codigoUbicacion, codigoSuministro) references informeSuministroIngreso(codigoUbicacion, codigoSuministro)
)

-- Salidas de insumos desde una ubicación hacia otra.
create table informeSuministroSalida(
codigoUbicacionFuente		varchar(5) not null,
codigoSuministro			varchar(5) not null,
cantidadDespachada			int not null,
codigoUbicacionDestino		varchar(5) not null,
constraint PK_informeSuministroSalida primary key (codigoUbicacionFuente, codigoSuministro),
constraint FK_informeSuministroSalida_suministroUbicacion foreign key (codigoUbicacionFuente, codigoSuministro) references suministroUbicacion(codigoUbicacion, codigoSuministro),
constraint FK_informeSuministroSalida_ubicacion foreign key (codigoUbicacionDestino) references ubicacion(codigoUbicacion)
)
-- En 01_esquema.sql:
--   • la PK pasa a ser idSalida (autoincremental): con (fuente, insumo) solo se podía
--     despachar UNA vez cada insumo desde cada ubicación
--   • se agregan empleadoDestinatario (quién recibe) y fecha
--   • codigoUbicacionDestino puede ser NULL: NULL = salida a un empleado; con valor =
--     traslado entre almacenes
--   • se agrega la tabla entradaSuministro (bitácora de entradas)

go

/* =====================================================================================
   Query 1 (versión del grupo).
   NOTA: esta vista une empleado con sus catálogos pero NO con asignacionSala/sala, así
   que no muestra en qué sala trabaja cada uno (la Figura 1 pide "Sala" y "Camas
   disponibles"). La versión que usa la app, con los JOIN de sala, está en
   sql/02_indices_vistas.sql.
   ===================================================================================== */
create view VW_Asignacion_Personal as
select e.numeroEmpleado as 'No. Emp',
e.nombres+' '+e.apellidos as 'Nombre Empleado',
e.NSS as NSS,
p.nombre as Puesto,
es.nombre as Especialidad,
t.nombre as Turno,
p.salario as 'Salario Base (Q.)'
from empleado e inner join puesto p on e.codigoPuesto = p.codigoPuesto
inner join especialidad es on e.codigoEspecialidad = es.codigoEspecialidad
inner join turno t on e.codigoTurno = t.codigoTurno
go
