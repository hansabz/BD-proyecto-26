"""
core/graficos.py — Gráficas con pandas + matplotlib (radio "Gráfico" de cada vista).

Se usa la API orientada a objetos (Figure) y no pyplot, porque pyplot
guarda estado global y Flask atiende pedidos en varios hilos a la vez.
Cada área agrega sus datos en Spark, los baja a pandas con toPandas()
y llama a uno de estos helpers según el tipo de dato:

    categorías con pocas clases      -> dona (proporciones)
    categorías con muchas clases     -> barras horizontales ordenadas
    variable numérica continua       -> histograma
    serie en el tiempo               -> línea
    parte vs. total por categoría    -> barras apiladas

Recorrido de una gráfica (ej. "Personal por puesto"):
    1. Spark agrupa:        df.groupBy("puesto").count()
    2. Baja a pandas:       .toPandas()   -> DataFrame chico de pandas
    3. matplotlib dibuja:   dona(pdf, "puesto", "count", "Personal por puesto")
    4. a_png() la convierte en bytes PNG y app.py la devuelve como imagen.
"""
import io

import matplotlib

matplotlib.use("Agg")   # "Agg" = dibuja en memoria, sin abrir ventanas (estamos en un servidor)

from matplotlib.figure import Figure  # noqa: E402
import pandas as pd  # noqa: E402

TINTA = "#16302f"
SUAVE = "#6b7b78"
REJILLA = "#e6e1d6"
PRINCIPAL = "#0f6e66"
ALERTA = "#d4573b"
PALETA = ["#0f6e66", "#e0a43a", "#5b7fa8", "#d4573b", "#8a6bb0", "#5f9e6e", "#b5737f", "#7a8b8a"]

matplotlib.rcParams.update({
    "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
    "font.size": 10,
    "text.color": TINTA,
    "axes.labelcolor": SUAVE,
    "xtick.color": SUAVE,
    "ytick.color": SUAVE,
    "axes.edgecolor": REJILLA,
})


def _figura(ancho=7.6, alto=3.9):
    """Lienzo base con el estilo del dashboard: fondo transparente, sin bordes arriba/derecha."""
    fig = Figure(figsize=(ancho, alto), dpi=110)
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    ax.set_facecolor("none")
    for lado in ("top", "right"):
        ax.spines[lado].set_visible(False)
    return fig, ax


def _vacio(titulo):
    fig, ax = _figura()
    ax.axis("off")
    ax.text(0.5, 0.5, "Sin datos para mostrar con los filtros actuales", ha="center", va="center",
            color=SUAVE, fontsize=11)
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA)
    return fig


def a_png(fig) -> bytes:
    """Figura -> bytes PNG en memoria (sin guardar archivos en disco)."""
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", bbox_inches="tight", transparent=True)
    return buffer.getvalue()


def dona(pdf: pd.DataFrame, etiqueta: str, valor: str, titulo: str):
    """Proporciones de pocas categorías (puesto, sexo, estado). Total en el centro."""
    if pdf.empty:
        return _vacio(titulo)
    pdf = pdf.sort_values(valor, ascending=False)
    fig, ax = _figura(7.6, 3.9)
    cuñas, _ = ax.pie(pdf[valor], colors=PALETA[: len(pdf)], startangle=90, counterclock=False,
                      wedgeprops={"width": 0.38, "edgecolor": "white", "linewidth": 2})
    total = pdf[valor].sum()
    ax.text(0, 0.06, f"{int(total)}", ha="center", va="center", fontsize=22, fontweight="bold", color=TINTA)
    ax.text(0, -0.2, "total", ha="center", va="center", fontsize=9, color=SUAVE)
    leyenda = [f"{e}  ·  {int(v)} ({v / total:.0%})" for e, v in zip(pdf[etiqueta], pdf[valor])]
    ax.legend(cuñas, leyenda, loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA)
    ax.axis("equal")
    return fig


def barras_h(pdf: pd.DataFrame, etiqueta: str, valor: str, titulo: str, formato="{:.0f}",
             resaltar=None, eje=""):
    """Comparar cantidades entre muchas categorías. `resaltar(fila)` pinta en rojo (ej. stock < 15 %)."""
    if pdf.empty:
        return _vacio(titulo)
    pdf = pdf.sort_values(valor)
    alto = max(2.6, 0.38 * len(pdf) + 1.1)
    fig, ax = _figura(7.6, alto)
    colores = [ALERTA if resaltar and resaltar(f) else PRINCIPAL for _, f in pdf.iterrows()]
    barras = ax.barh(pdf[etiqueta].astype(str), pdf[valor], color=colores, height=0.62)
    ax.bar_label(barras, labels=[formato.format(v) for v in pdf[valor]], padding=4, fontsize=9, color=TINTA)
    ax.xaxis.grid(True, color=REJILLA, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.set_xlabel(eje)
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA)
    return fig


def histograma(serie: pd.Series, titulo: str, eje: str, bins=10, linea=None, etiqueta_linea=""):
    """Distribución de una variable numérica continua (edad, días, costo). `linea` marca un umbral."""
    serie = serie.dropna()
    if serie.empty:
        return _vacio(titulo)
    fig, ax = _figura()
    ax.hist(serie, bins=bins, color=PRINCIPAL, edgecolor="white", linewidth=1.2)
    ax.yaxis.grid(True, color=REJILLA, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel(eje)
    ax.set_ylabel("frecuencia")
    if linea is not None:
        ax.axvline(linea, color=ALERTA, linestyle="--", linewidth=1.4)
        ax.text(linea, ax.get_ylim()[1] * 0.95, f"  {etiqueta_linea}", color=ALERTA, fontsize=9, va="top")
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA)
    return fig


def linea(pdf: pd.DataFrame, x: str, y: str, titulo: str, eje_y: str):
    """Evolución en el tiempo (ingresos por mes, consumo por semana)."""
    if pdf.empty:
        return _vacio(titulo)
    fig, ax = _figura()
    ax.plot(pdf[x], pdf[y], color=PRINCIPAL, linewidth=2.2, marker="o", markersize=4)
    ax.fill_between(pdf[x], pdf[y], color=PRINCIPAL, alpha=0.08)
    ax.yaxis.grid(True, color=REJILLA, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_ylabel(eje_y)
    ax.set_ylim(bottom=0)
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA)
    fig.autofmt_xdate()
    return fig


def apiladas(pdf: pd.DataFrame, etiqueta: str, series: list[tuple[str, str, str]], titulo: str):
    """series = [(columna, nombre_leyenda, color), ...]"""
    if pdf.empty:
        return _vacio(titulo)
    fig, ax = _figura(7.6, max(2.6, 0.5 * len(pdf) + 1.2))
    izquierda = pd.Series([0] * len(pdf), index=pdf.index, dtype=float)
    for col, nombre, color in series:
        barras = ax.barh(pdf[etiqueta].astype(str), pdf[col], left=izquierda, color=color, height=0.6, label=nombre)
        ax.bar_label(barras, labels=[str(int(v)) if v else "" for v in pdf[col]], label_type="center",
                     color="white", fontsize=9, fontweight="bold")
        izquierda += pdf[col]
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=len(series), frameon=False)
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA, pad=28)
    return fig
