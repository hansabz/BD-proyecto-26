"""
bigdata/generar_logs.py — Ingesta masiva: simula la telemetría de los sensores.

Genera logs_monitoreo_pacientes.csv con el esquema de la Figura 6:

    ID_Registro | ID_Paciente | Timestamp | Frecuencia_Cardiaca | Saturacion_Oxigeno | Alerta_Generada
    LOG-000001  | PAC-0001    | 2026-10-25 08:15:00 | 78 | 98 | No

Los sensores monitorean a pacientes HOSPITALIZADOS: cada lectura cae dentro
de la estadía real de un paciente en cama (asignacionCama), en los últimos 7
días. Así, después, cada lectura se puede cruzar con su internación para
calcular asignacionCama.nivelRiesgo. Los logs NO se guardan en la base: solo
viven en este archivo CSV.

Cada paciente tiene un "perfil" (estable, inestable o crítico) que define
cómo varían sus signos vitales. Además se inyectan a propósito:
  • ~2 % de valores nulos           (sensor desconectado)
  • ~3 % de lecturas imposibles     (FC < 30 o > 220, SpO2 > 100: falla del sensor)
que son las que debe remover la Query BD 1.

No usa Spark ni pandas: solo la librería estándar, así corre igual en
Google Colab, en Databricks o en la PC.

    python bigdata/generar_logs.py                 6,000 registros (mínimo pedido: 5,000)
    python bigdata/generar_logs.py --registros 20000
"""
import argparse
import csv
import datetime as dt
import random
from pathlib import Path

RUTA_POR_DEFECTO = Path(__file__).resolve().parent / "datos" / "logs_monitoreo_pacientes.csv"
COLUMNAS = ["ID_Registro", "ID_Paciente", "Timestamp", "Frecuencia_Cardiaca", "Saturacion_Oxigeno",
            "Alerta_Generada"]
DIAS_DE_MONITOREO = 7

# perfil -> (FC media, FC desvío, SpO2 media, SpO2 desvío)
PERFILES = {
    "estable": (76, 9, 96.5, 1.5),
    "inestable": (92, 16, 93.0, 2.8),
    "critico": (108, 22, 89.5, 3.5),
}


def generar(registros: int = 6000, estancias: list | None = None, ruta: Path = RUTA_POR_DEFECTO,
            semilla: int = 2026, fin: dt.datetime | None = None) -> dict:
    """
    estancias: lista de (ID_Paciente, desde, hasta) = período en que el paciente
    estuvo en cama. Si no se pasa (por ejemplo en Colab, sin base de datos), se
    simulan 12 pacientes internados durante toda la semana.
    """
    rnd = random.Random(semilla)
    fin = (fin or dt.datetime.now()).replace(second=0, microsecond=0)
    inicio = fin - dt.timedelta(days=DIAS_DE_MONITOREO)
    if not estancias:
        estancias = [(f"PAC-{i:04d}", inicio, None) for i in range(1, 13)]
    # Recorta cada estancia a la ventana de monitoreo (últimos 7 días).
    ventanas = []
    for paciente, desde, hasta in estancias:
        desde, hasta = max(desde, inicio), min(hasta or fin, fin)
        if hasta > desde:
            ventanas.append((paciente, desde, hasta))
    if not ventanas:
        raise ValueError("Ninguna internación cae dentro de los últimos 7 días: no hay a quién monitorear.")

    # Perfil por paciente: varios estables, algunos inestables y pocos críticos.
    perfil = {p: rnd.choices(list(PERFILES), weights=[40, 35, 25])[0] for p, _, _ in ventanas}
    # Las estancias más largas generan más lecturas (el sensor mide a intervalos regulares).
    pesos = [(hasta - desde).total_seconds() for _, desde, hasta in ventanas]

    lecturas = []
    for _ in range(registros):
        paciente, desde, hasta = rnd.choices(ventanas, weights=pesos)[0]
        momento = desde + (hasta - desde) * rnd.random()
        fc_media, fc_desvio, spo2_media, spo2_desvio = PERFILES[perfil[paciente]]
        lecturas.append([paciente, momento.replace(microsecond=0),
                         round(rnd.gauss(fc_media, fc_desvio)), min(100, round(rnd.gauss(spo2_media, spo2_desvio)))])
    lecturas.sort(key=lambda l: l[1])       # el archivo queda en orden cronológico

    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    nulos = fallas = alertas = 0
    with ruta.open("w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(COLUMNAS)
        for i, (paciente, momento, fc, spo2) in enumerate(lecturas, start=1):
            sorteo = rnd.random()
            if sorteo < 0.03:                  # falla del sensor: lectura fisiológicamente imposible
                fallas += 1
                fc, spo2 = rnd.choice([(0, spo2), (rnd.randint(5, 29), spo2), (rnd.randint(221, 320), spo2),
                                       (fc, rnd.randint(101, 130))])
            elif sorteo < 0.05:                # sensor desconectado: campo vacío
                nulos += 1
                if rnd.random() < 0.5:
                    fc = None
                else:
                    spo2 = None
            alerta = "Sí" if (spo2 is not None and spo2 < 90) or (fc is not None and (fc > 120 or fc < 50)) else "No"
            alertas += alerta == "Sí"
            escritor.writerow([f"LOG-{i:06d}", paciente, momento.strftime("%Y-%m-%d %H:%M:%S"),
                               "" if fc is None else fc, "" if spo2 is None else spo2, alerta])
    return {"ruta": str(ruta), "registros": registros, "pacientes": len(perfil),
            "nulos_inyectados": nulos, "fallas_inyectadas": fallas, "alertas": alertas}


def estancias_de_la_base() -> list | None:
    """
    Internaciones (asignacionCama) que se superponen con los últimos 7 días:
    las activas y las altas recientes. Solo LEE la base; los logs no se guardan ahí.
    """
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from core import db
        _, filas = db.consultar_crudo(f"""
            SELECT numeroPaciente, fechaIngreso, fechaSalida FROM asignacionCama
            WHERE fechaSalida IS NULL OR fechaSalida >= DATEADD(day, -{DIAS_DE_MONITOREO}, GETDATE())""")
        return [tuple(f) for f in filas] or None
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registros", type=int, default=6000)
    parser.add_argument("--salida", type=Path, default=RUTA_POR_DEFECTO)
    args = parser.parse_args()
    if args.registros < 5000:
        parser.error("La guía pide un mínimo de 5,000 registros.")
    print(generar(args.registros, estancias_de_la_base(), args.salida))
