"""
Crea la base hospitalProyecto, ejecuta los scripts de sql/, carga datos
de prueba y genera los logs de telemetría (bigdata/datos/, NO van a la base).
Al final corre el pipeline de PySpark para calcular asignacionCama.nivelRiesgo.

    python setup_db.py            crea todo (falla si la base ya existe)
    python setup_db.py --reset    BORRA la base y la vuelve a crear desde cero
"""
import argparse
import datetime as dt
import random
import re
import sys

from core import config, db

HOY = dt.datetime.now().replace(second=0, microsecond=0)
rnd = random.Random(2026)


# ── Ejecución de scripts ─────────────────────────────────────────────
def ejecutar_script(cur, ruta):
    texto = (config.BASE_DIR / "sql" / ruta).read_text(encoding="utf-8")
    for lote in re.split(r"^\s*GO\s*$", texto, flags=re.M | re.I):
        if lote.strip():
            cur.execute(lote)


def crear_base(reset: bool):
    conn = db.conectar("master", autocommit=True)
    cur = conn.cursor()
    existe = cur.execute("SELECT DB_ID(?)", config.DB_NAME).fetchone()[0] is not None
    if existe and not reset:
        sys.exit(f"La base '{config.DB_NAME}' ya existe. Usá --reset para recrearla (borra los datos).")
    if existe:
        cur.execute(f"ALTER DATABASE [{config.DB_NAME}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        cur.execute(f"DROP DATABASE [{config.DB_NAME}]")
    cur.execute(f"CREATE DATABASE [{config.DB_NAME}]")
    conn.close()


# ── Datos de prueba ──────────────────────────────────────────────────
PUESTOS = [("MED", "Médico", 12500), ("ENF", "Enfermera", 6800),
           ("EJF", "Enfermera Jefe", 8500), ("AUX", "Auxiliar", 4200)]

ESPECIALIDADES = [
    ("ESP01", "Cardiología"), ("ESP02", "Nutrición"), ("ESP03", "Geriatría"),
    ("ESP04", "Medicina Interna"), ("ESP05", "Neurología"), ("ESP06", "Traumatología"),
    ("ESP07", "Pediatría"), ("ESP08", "Medicina General"), ("ESP09", "Cuidados Intensivos"),
    ("ESP10", "Atención General"), ("ESP11", "Enfermería General"),
]

TURNOS = [("MAT", "Matutino"), ("VES", "Vespertino"), ("NOC", "Nocturno"), ("MIX", "Mixto")]

SALAS = [("S01", "Unidad Gerontológica (Clínica 2)", 15), ("S02", "Cuidados Intensivos", 8),
         ("S03", "Consulta Externa", 0), ("S04", "Medicina Interna", 12), ("S05", "Rehabilitación", 10)]

# id, nombres, apellidos, NSS (None = aleatorio), puesto, especialidad, turno, salas, activo
EMPLEADOS = [
    ("M1001", "Juan", "Pérez", "883920-K", "MED", "ESP01", "MAT", ["S01"], True),
    ("M1002", "Ana", "Pérez", "992837-A", "MED", "ESP02", "VES", ["S01"], True),
    ("M1003", "Luis", "García", None, "MED", "ESP07", "MAT", ["S03"], True),
    ("M1004", "Carlos", "Morales", None, "MED", "ESP04", "NOC", ["S04"], True),
    ("M1005", "Lucía", "Herrera", None, "MED", "ESP05", "VES", ["S02"], True),
    ("M1006", "Diego", "Castillo", None, "MED", "ESP06", "MAT", ["S05"], True),
    ("M1007", "Sofía", "Ramírez", None, "MED", "ESP03", "MIX", ["S01", "S02"], True),
    ("M1008", "Andrés", "López", None, "MED", "ESP09", "NOC", ["S02"], True),
    ("M1009", "Ricardo", "Paz", None, "MED", "ESP08", "VES", [], False),
    ("E1001", "Moira", "Samuel", "119283-B", "EJF", "ESP03", "MIX", ["S01"], True),
    ("E1002", "Gabriela", "Cifuentes", None, "ENF", "ESP11", "MAT", ["S02"], True),
    ("E1003", "Rosa", "Méndez", None, "ENF", "ESP03", "VES", ["S01"], True),
    ("E1004", "Patricia", "Gómez", None, "ENF", "ESP09", "NOC", ["S02"], True),
    ("E1005", "Karla", "Orellana", None, "ENF", "ESP11", "MAT", ["S04"], True),
    ("E1006", "Jorge", "Sandoval", None, "ENF", "ESP11", "VES", ["S05"], True),
    ("E1007", "Daniela", "Fuentes", None, "EJF", "ESP09", "MIX", ["S02"], True),
    ("A1001", "Marco", "Tzul", None, "AUX", "ESP10", "MAT", ["S03"], True),
    ("A1002", "Silvia", "Ajú", None, "AUX", "ESP10", "VES", ["S01"], True),
    ("A1003", "Pedro", "Xicará", None, "AUX", "ESP10", "NOC", ["S04"], True),
    ("A1004", "Luis", "López", None, "AUX", "ESP10", "MAT", [], False),               # dado de baja
    ("A1005", "Elena", "Batz", None, "AUX", "ESP10", "MIX", ["S05"], True),
]

NOMBRES_M = ["José", "Manuel", "Francisco", "Héctor", "Raúl", "Óscar", "Arturo", "Mario", "Edgar", "Rodolfo", "Julio", "Víctor"]
NOMBRES_F = ["María", "Carmen", "Gloria", "Teresa", "Elsa", "Olga", "Amalia", "Irma", "Leticia", "Marta", "Aura", "Blanca"]
APELLIDOS = ["López", "García", "Hernández", "Pérez", "Morales", "Castillo", "Rodas", "Monzón", "Estrada", "Barrios",
             "Cabrera", "Orozco", "Velásquez", "Recinos", "Solís", "Aguilar", "Chávez", "Girón", "Lemus", "Paredes"]
PARENTESCOS = ["Hija", "Hijo", "Esposa", "Esposo", "Nieta", "Nieto", "Sobrina", "Hermano"]
MEDICOS_REF = ["Dr. Fernando Asturias", "Dra. Claudia Rivas", "Dr. Ernesto Salguero", "Dra. Inés Montúfar", "Dr. Pablo Quan"]
CLINICAS_REF = ["Clínica Santa María", "Sanatorio El Pilar", "Centro Médico Zona 9", "Clínica Las Américas", "Hospital Privado Cayalá"]

PROVEEDORES = [
    ("PR001", "Medical Supplies Inc.", "Laura Méndez", "+502 2334-1100", "ventas@medsupplies.gt"),
    ("PR002", "CareTech Corp.", "Óscar Villeda", "+502 2290-4455", "pedidos@caretech.com"),
    ("PR003", "Farma Salud S.A.", "Andrea Pineda", "+502 2421-7788", "andrea.pineda@farmasalud.gt"),
    ("PR004", "Distribuidora Quirúrgica GT", "Mynor Ruano", "+502 2367-0099", "mruano@dquirurgica.gt"),
    ("PR005", "BioMed Logística", "Fabiola Ortiz", "+502 2505-3321", "logistica@biomed.gt"),
]

SUMINISTROS = [
    ("Q001", "Guantes Quirúrgicos", "Estériles, Látex, Talla M", "Q", 6.50, "PR001"),
    ("Q002", "Bisturí desechable #10", "Hoja de acero inoxidable, estéril", "Q", 4.75, "PR004"),
    ("Q003", "Sutura Nylon 3-0", "Caja de 12 unidades", "Q", 185.00, "PR004"),
    ("Q004", "Gasas estériles", "10x10 cm, paquete de 100", "Q", 38.00, "PR001"),
    ("Q005", "Jeringa 5 ml", "Con aguja 21G, caja de 100", "Q", 95.00, "PR001"),
    ("M001", "Mascarillas N95", "Certificadas, Caja de 50", "M", 115.00, "PR002"),
    ("M002", "Batas desechables", "Talla única, paquete de 10", "M", 85.00, "PR002"),
    ("M003", "Catéter IV 20G", "Caja de 50 unidades", "M", 210.00, "PR005"),
    ("M004", "Sonda Foley 16Fr", "Silicona, estéril", "M", 22.50, "PR005"),
    ("M005", "Oxímetro de pulso", "Portátil, de dedo", "M", 175.00, "PR002"),
    ("F001", "Paracetamol 500mg", "Tabletas x 100", "F", 42.00, "PR003"),
    ("F002", "Amoxicilina 500mg", "Cápsulas x 50", "F", 68.00, "PR003"),
    ("F003", "Suero fisiológico 0.9%", "Bolsa 1000 ml", "F", 18.50, "PR003"),
    ("F004", "Enalapril 10mg", "Tabletas x 30", "F", 35.00, "PR003"),
    ("F005", "Insulina NPH", "Frasco 10 ml", "F", 145.00, "PR005"),
    ("F006", "Omeprazol 20mg", "Cápsulas x 30", "F", 29.00, "PR003"),
    ("F007", "Heparina 5000 UI", "Ampolla 5 ml", "F", 62.00, "PR005"),
    ("F008", "Metformina 850mg", "Tabletas x 60", "F", 31.00, "PR003"),
]

UBICACIONES = [("U01", "Almacén Central", "E1001"), ("U02", "Farmacia Interna", "E1003"),
               ("U03", "Quirófano 1", "E1007"), ("U04", "Bodega UCI", "E1004")]


def nss_aleatorio(usados):
    while True:
        nss = f"{rnd.randint(100000, 999999)}-{rnd.choice('0123456789ABCDEFGHJKLMNPRSTUVWXYZ')}"
        if nss not in usados:
            usados.add(nss)
            return nss


def telefono():
    return f"+502 {rnd.randint(3000, 5999)}-{rnd.randint(1000, 9999)}"


def cargar_datos(cur):
    cur.executemany("INSERT INTO puesto VALUES (?,?,?)", PUESTOS)
    cur.executemany("INSERT INTO especialidad VALUES (?,?)", ESPECIALIDADES)
    cur.executemany("INSERT INTO turno VALUES (?,?)", TURNOS)
    cur.executemany("INSERT INTO sala VALUES (?,?,?)", SALAS)

    # Personal
    usados = {e[3] for e in EMPLEADOS if e[3]}
    for (num, nom, ape, nss, pue, esp, tur, salas, activo) in EMPLEADOS:
        contratacion = (HOY - dt.timedelta(days=rnd.randint(120, 9000))).date()   # entre 4 meses y ~25 años
        baja = None if activo else HOY - dt.timedelta(days=rnd.randint(5, 120))
        cur.execute(
            "INSERT INTO empleado (numeroEmpleado, nombres, apellidos, NSS, codigoPuesto, codigoEspecialidad,"
            " codigoTurno, fechaContratacion, activo, fechaBaja) VALUES (?,?,?,?,?,?,?,?,?,?)",
            num, nom, ape, nss or nss_aleatorio(usados), pue, esp, tur, contratacion, activo, baja)
        for s in salas:   # trabaja en esa sala desde que lo contrataron
            cur.execute("INSERT INTO asignacionSala (codigoSala, numeroEmpleado, fechaAsignacion) VALUES (?,?,?)",
                        s, num, contratacion)

    # Camas: C-001.. por sala, todas disponibles al inicio (codigoEstado bit: 0 libre, 1 ocupada)
    camas = []
    for cod, _, capacidad in SALAS:
        for i in range(1, capacidad + 1):
            camas.append([f"C-{i:03d}", cod, 0])
    cur.executemany("INSERT INTO cama VALUES (?,?,?)", camas)

    # Pacientes (adultos mayores)
    pacientes = [
        ("PAC-0001", "Roberto Carlos", "Mendoza Silva", dt.date(1950, 5, 12), "M", "+502 5555-1234",
         "14 Avenida 5-22 Zona 10, Ciudad"),
        ("PAC-0002", "Marta", "Juárez", dt.date(1946, 2, 3), "F", telefono(), "3a Calle 8-15 Zona 1, Ciudad"),
    ]
    for i in range(3, 41):
        sexo = rnd.choice("MF")
        nombre = rnd.choice(NOMBRES_M if sexo == "M" else NOMBRES_F)
        if rnd.random() < 0.4:
            nombre += " " + rnd.choice(NOMBRES_M if sexo == "M" else NOMBRES_F)
        nombre = nombre[:25]
        pacientes.append((
            f"PAC-{i:04d}", nombre, f"{rnd.choice(APELLIDOS)} {rnd.choice(APELLIDOS)}"[:25],
            dt.date(rnd.randint(1932, 1962), rnd.randint(1, 12), rnd.randint(1, 28)), sexo, telefono(),
            f"{rnd.randint(1, 20)}a Avenida {rnd.randint(1, 30)}-{rnd.randint(10, 99)} Zona {rnd.randint(1, 18)}",
        ))
    cur.executemany("INSERT INTO paciente VALUES (?,?,?,?,?,?,?)", pacientes)

    # Ingresos: uno por paciente (los 40 primeros) + segundo ingreso a algunos
    ingresos = []
    n_ing = 0

    def nuevo_ingreso(pac, fecha):
        nonlocal n_ing
        n_ing += 1
        fam_sexo = rnd.choice("MF")
        fam = f"{rnd.choice(NOMBRES_M if fam_sexo == 'M' else NOMBRES_F)} {rnd.choice(APELLIDOS)}"
        ingresos.append((f"ING-{n_ing:03d}", pac, fecha, fam, rnd.choice(PARENTESCOS), telefono(),
                         rnd.choice(MEDICOS_REF), rnd.choice(CLINICAS_REF)))
        return ingresos[-1][0]

    historial = []  # (ingreso, paciente, fecha)
    for pac in pacientes[:40]:
        historial.append((pac[0], HOY - dt.timedelta(days=rnd.randint(60, 240), hours=rnd.randint(0, 23))))
    for pac in rnd.sample(pacientes[2:40], 18):     # reingresos recientes: la mayoría siguen internados
        historial.append((pac[0], HOY - dt.timedelta(days=rnd.randint(1, 45), hours=rnd.randint(0, 23))))
    historial.sort(key=lambda h: h[1])
    ids_ingreso = []
    for pac, fecha in historial:
        if pac == "PAC-0001" and not any(i[1] == "PAC-0001" for i in ingresos):
            n_ing += 1
            ingresos.append((f"ING-{n_ing:03d}", pac, fecha, "Clara Mendoza", "Hija", "+502 4444-9876",
                             "Dr. Fernando Asturias", "Clínica Santa María"))
            ids_ingreso.append((ingresos[-1][0], pac, fecha))
        else:
            ids_ingreso.append((nuevo_ingreso(pac, fecha), pac, fecha))
    cur.executemany("INSERT INTO registroIngreso VALUES (?,?,?,?,?,?,?,?)", ingresos)

    # Asignación de camas: los antiguos ya tienen alta; los recientes siguen internados
    camas_libres = [c for c in camas if c[2] == 0]
    rnd.shuffle(camas_libres)
    internados = set()
    asignaciones = []
    recientes = [i for i in ids_ingreso if (HOY - i[2]).days <= 50]
    esperando_cama = {i[0] for i in sorted(recientes, key=lambda x: x[2])[-2:]}
    for ing, pac, fecha in ids_ingreso:
        entrada = fecha + dt.timedelta(hours=rnd.randint(1, 6))
        if ing in esperando_cama:
            continue
        if (ing, pac, fecha) in recientes and pac not in internados:
            cama = camas_libres.pop()
            cama[2] = 1
            internados.add(pac)
            asignaciones.append((ing, pac, cama[0], cama[1], entrada, None, 1))
        elif (ing, pac, fecha) not in recientes:
            cama = rnd.choice([c for c in camas if c[1] != "S02"] + [c for c in camas if c[1] == "S02"][:2])
            salida = min(entrada + dt.timedelta(days=rnd.randint(2, 21), hours=rnd.randint(0, 12)), HOY)
            asignaciones.append((ing, pac, cama[0], cama[1], entrada, salida, 0))
    cur.executemany("INSERT INTO asignacionCama (numeroIngreso, numeroPaciente, numeroCama, codigoSala,"
                    " fechaIngreso, fechaSalida, codigoEstado) VALUES (?,?,?,?,?,?,?)", asignaciones)
    for c in camas:
        if c[2] == 1:
            cur.execute("UPDATE cama SET codigoEstado = 1 WHERE numeroCama = ? AND codigoSala = ?", c[0], c[1])

    # Citas — las 4 primeras replican la Figura 4
    medicos = {e[0]: e for e in EMPLEADOS if e[4] == "MED" and e[8]}
    citas = [
        ("101", "PAC-0001", dt.date(2026, 10, 20), dt.time(9, 0), "M1001", "ESP01", "S01"),
        ("102", "PAC-0001", dt.date(2026, 10, 27), dt.time(10, 30), "M1001", "ESP01", "S01"),
        ("103", "PAC-0001", dt.date(2026, 11, 1), dt.time(11, 0), "M1003", "ESP07", "S03"),
        ("104", "PAC-0001", dt.date(2026, 11, 8), dt.time(9, 0), "M1002", "ESP02", "S01"),
    ]
    for n in range(105, 205):
        med = medicos[rnd.choice(list(medicos))]
        dia = HOY.date() + dt.timedelta(days=rnd.randint(-150, 40))
        hora = dt.time(rnd.randint(8, 16), rnd.choice([0, 30]))
        citas.append((str(n), rnd.choice(pacientes[:40])[0], dia, hora, med[0], med[5], (med[7] or ["S03"])[0]))
    cur.executemany("INSERT INTO informeCita VALUES (?,?,?,?,?,?,?)", citas)

    # Inventario
    cur.executemany("INSERT INTO ubicacion VALUES (?,?,?)", UBICACIONES)
    cur.executemany("INSERT INTO proveedor VALUES (?,?,?,?,?)", PROVEEDORES)
    cur.executemany("INSERT INTO suministro VALUES (?,?,?,?,?,?)", SUMINISTROS)
    stock = []
    for cod, *_ in SUMINISTROS:
        for ubi in rnd.sample([u[0] for u in UBICACIONES], rnd.randint(1, 3)):
            capacidad = rnd.choice([50, 80, 100, 150, 200, 300, 500])
            stock.append([ubi, cod, int(capacidad * rnd.uniform(0.2, 1.0)), capacidad])
    for fila in rnd.sample(stock, 6):          # stock crítico (< 15 %)
        fila[2] = int(fila[3] * rnd.uniform(0.0, 0.13))
    # Ficha de ingreso y luego el stock actual con su cantidad ideal (FK en ese orden).
    # cantidadOrdenada arranca con el stock inicial; abajo se le suman las entradas del historial.
    cur.executemany("INSERT INTO informeSuministroIngreso (codigoUbicacion, codigoSuministro, cantidadOrdenada)"
                    " VALUES (?,?,?)", [(u, c, ex) for u, c, ex, _ in stock])
    cur.executemany("INSERT INTO suministroUbicacion (codigoUbicacion, codigoSuministro, cantidadDisponible,"
                    " cantidadIdeal) VALUES (?,?,?,?)", [(u, c, ex, ideal) for u, c, ex, ideal in stock])

    # Historial: entradas, salidas a personal y traslados entre almacenes (últimos 90 días).
    activos = [e[0] for e in EMPLEADOS if e[8]]
    encargado = {u[0]: u[2] for u in UBICACIONES}
    entradas, salidas = [], []
    for _ in range(70):
        ubi, cod, _, capacidad = rnd.choice(stock)
        fecha = HOY - dt.timedelta(days=rnd.randint(0, 90), hours=rnd.randint(0, 23))
        cantidad = rnd.randint(1, max(2, capacidad // 4))
        match rnd.choice("EESST"):
            case "E":
                entradas.append((ubi, cod, cantidad, fecha))
            case "S":
                salidas.append((ubi, cod, cantidad, None, rnd.choice(activos), fecha))
            case "T":
                destinos = [s for s in stock if s[1] == cod and s[0] != ubi]
                if destinos:
                    destino = rnd.choice(destinos)[0]
                    salidas.append((ubi, cod, cantidad, destino, encargado[destino], fecha))
    cur.executemany("INSERT INTO entradaSuministro (codigoUbicacion, codigoSuministro, cantidad, fecha)"
                    " VALUES (?,?,?,?)", entradas)
    cur.executemany("INSERT INTO informeSuministroSalida (codigoUbicacionFuente, codigoSuministro, cantidadDespachada,"
                    " codigoUbicacionDestino, empleadoDestinatario, fecha) VALUES (?,?,?,?,?,?)", salidas)
    # Total ordenado = stock inicial + todas las entradas registradas.
    cur.execute("""UPDATE ig SET cantidadOrdenada = ig.cantidadOrdenada + e.total
                   FROM informeSuministroIngreso ig
                   JOIN (SELECT codigoUbicacion, codigoSuministro, SUM(cantidad) AS total
                         FROM entradaSuministro GROUP BY codigoUbicacion, codigoSuministro) e
                     ON e.codigoUbicacion = ig.codigoUbicacion AND e.codigoSuministro = ig.codigoSuministro""")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reset", action="store_true", help="borra y recrea la base")
    parser.add_argument("--sin-datos", action="store_true", help="solo estructura, sin datos de prueba")
    args = parser.parse_args()

    print(f"Servidor: {config.DB_SERVER}  ·  Base: {config.DB_NAME}")
    crear_base(args.reset)
    with db.transaccion() as cur:
        ejecutar_script(cur, "01_esquema.sql")
        ejecutar_script(cur, "02_indices_vistas.sql")
        if not args.sin_datos:
            cargar_datos(cur)
    if not args.sin_datos:
        # Fase II: archivo masivo de telemetría (Figura 6). Las lecturas caen dentro de las
        # internaciones reales; el archivo queda en bigdata/datos/ y NO se carga a la base.
        from bigdata import generar_logs
        r = generar_logs.generar(6000, generar_logs.estancias_de_la_base())
        print(f"Telemetría: {r['registros']} lecturas de {r['pacientes']} pacientes en {r['ruta']}")
        # Pipeline (BD 1 -> BD 3 -> nivel sostenido) para llenar asignacionCama.nivelRiesgo.
        print("Calculando el nivel de riesgo con PySpark (~30 s)...")
        from areas import telemetria
        print(telemetria.paso_riesgo()["mensaje"])
    print("Listo. Iniciá el dashboard con:  python app.py")


if __name__ == "__main__":
    main()
