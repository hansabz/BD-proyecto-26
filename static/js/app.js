/* ==========================================================================
   Hospital W 2.0 — lógica compartida del dashboard
   - API: fetch JSON con manejo de errores de negocio
   - PanelDatos: tabla en tiempo real + filtros (PySpark) + limpieza BD 1 (solo telemetría)
     + vista de gráficas (pandas/matplotlib) para cualquier área
   - Form: llenar / leer / marcar errores de formularios
   ========================================================================== */

// Cada cuántos ms se pregunta al servidor si cambiaron los datos (lo define config.py).
const INTERVALO = window.INTERVALO_MS || 4000;

// ── Utilidades ────────────────────────────────────────────────────────
// Atajos estilo jQuery: $("#id") devuelve UN elemento, $$(".clase") devuelve una LISTA.
const $ = (sel, raiz = document) => raiz.querySelector(sel);
const $$ = (sel, raiz = document) => [...raiz.querySelectorAll(sel)];

// Escapa < > & " ' antes de meter texto en el HTML. Evita que un dato (por ejemplo un
// nombre con "<script>") se interprete como código: protección contra XSS.
function esc(valor) {
  return String(valor ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// Formateadores de valores para mostrar: moneda en quetzales, números con separador de
// miles (formato de Guatemala), fechas ISO "2026-10-15" -> "15/10/2026".
const fmt = {
  moneda: (v) => v == null ? "—" : "Q." + Number(v).toLocaleString("es-GT", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
  numero: (v) => v == null ? "—" : Number(v).toLocaleString("es-GT", { maximumFractionDigits: 1 }),
  fecha: (v) => {
    if (!v) return "—";
    const [a, m, d] = String(v).slice(0, 10).split("-");
    return `${d}/${m}/${a}`;
  },
  fechahora: (v) => v ? `${fmt.fecha(v)} <small class="mono" style="color:var(--muted)">${esc(String(v).slice(11, 16))}</small>` : null,
};

// Dibuja un ID como "pulsera de paciente". El color depende del prefijo:
// M (médico, verde), E (enfermería, azul), A (auxiliar, ámbar), PAC (paciente, violeta).
function pulsera(id) {
  if (!id) return "—";
  const clase = id.startsWith("PAC") ? "P" : ["E", "A"].includes(id[0]) ? id[0] : "";
  return `<span class="pulsera ${clase}">${esc(id)}</span>`;
}

// Color del "badge" según el estado de un paciente o internación.
const TONOS_ESTADO = {
  "Internado": "info", "Alta": "ok", "Alta Médica": "ok", "En espera de cama": "aviso", "Sin ingreso": "gris",
};

/**
 * Convierte un valor en el HTML de una celda según el tipo de columna que declara
 * el área en COLUMNAS (Python). Ej.: tipo "moneda" -> "Q.12,500.00", tipo "riesgo" ->
 * badge rojo/ámbar/verde. `extra` muestra un segundo dato chico debajo.
 */
function celda(col, r) {
  const v = r[col.campo];
  const extra = col.extra && r[col.extra] ? `<span class="extra">${esc(r[col.extra])}</span>` : "";
  switch (col.tipo) {
    case "id": return pulsera(v);
    case "mono": return `<span class="mono">${esc(v ?? "—")}</span>${extra}`;
    case "moneda": return fmt.moneda(v);
    case "numero": return fmt.numero(v);
    case "fecha": return fmt.fecha(v);
    case "fechahora": return fmt.fechahora(v) ?? `<span style="color:var(--muted)">${esc(col.vacio || "—")}</span>`;
    case "activo": return v ? `<span class="badge ok">Activo</span>` : `<span class="badge gris">Baja</span>`;
    case "estado": return `<span class="badge ${TONOS_ESTADO[v] || "gris"}">${esc(v)}</span>`;
    case "turno": return `<span class="turno">${esc(v)}</span>`;
    case "riesgo": return v ? `<span class="badge ${{ "Crítico": "alerta", "Moderado": "aviso", "Bajo": "ok" }[v] || "gris"}">${esc(v)}</span>`
                            : `<span class="badge gris">Sin lecturas</span>`;
    case "alerta": return v === "Sí" ? `<span class="badge alerta">Sí</span>` : `<span class="badge gris">${esc(v ?? "—")}</span>`;
    case "stock": {
      const pct = Math.min(100, Number(r.porcentaje) || 0);
      return `<div class="stock ${r.critico ? "critico" : ""}"><b>${fmt.numero(v)}</b> <small>/ ${fmt.numero(r.capacidadNormal)} · ${fmt.numero(r.porcentaje)} %</small>
              <div class="barra"><i style="width:${pct}%"></i></div></div>`;
    }
    default: return `${esc(v ?? "—")}${extra}`;
  }
}

// ── Toasts ────────────────────────────────────────────────────────────
// Aviso flotante abajo a la derecha que desaparece solo (más tiempo si es un error).
function toast(mensaje, tipo = "ok") {
  const caja = $(".toasts");
  const t = document.createElement("div");
  t.className = `toast ${tipo === "error" ? "error" : ""}`;
  t.textContent = mensaje;
  caja.append(t);
  setTimeout(() => { t.classList.add("salir"); setTimeout(() => t.remove(), 260); }, tipo === "error" ? 6000 : 3800);
}

// ── API ───────────────────────────────────────────────────────────────
// Error que devuelve el servidor (HTTP 400). `campos` = errores por campo del formulario,
// tal como los arma ErrorNegocio en Python: {"NSS": "Ya existe"}.
class ErrorApi extends Error {
  constructor(mensaje, campos) { super(mensaje); this.campos = campos || null; }
}

// Envoltorio de fetch(): manda/recibe JSON y, si la respuesta no es 2xx, lanza ErrorApi
// con el mensaje del servidor. Uso: await API.post("/api/personal", datos).
const API = {
  async pedir(url, metodo = "GET", cuerpo) {
    const resp = await fetch(url, {
      method: metodo,
      headers: cuerpo !== undefined ? { "Content-Type": "application/json" } : {},
      body: cuerpo !== undefined ? JSON.stringify(cuerpo) : undefined,
    });
    let datos = null;
    try { datos = await resp.json(); } catch { /* respuesta vacía */ }
    if (!resp.ok) throw new ErrorApi(datos?.error || `Error ${resp.status}`, datos?.campos);
    return datos;
  },
  get: (u) => API.pedir(u),
  post: (u, c = {}) => API.pedir(u, "POST", c),
  put: (u, c = {}) => API.pedir(u, "PUT", c),
  del: (u) => API.pedir(u, "DELETE"),
};

// ── Estado del motor Spark (barra lateral) ───────────────────────────
// Punto verde/ámbar de la barra lateral: indica si la JVM de Spark ya terminó de arrancar.
function marcarSpark(listo) {
  const m = $(".motor");
  if (!m) return;
  m.classList.toggle("listo", !!listo);
  $(".motor-estado", m).textContent = listo ? "PySpark listo" : "Iniciando PySpark…";
}

// ── Diálogos ──────────────────────────────────────────────────────────
// Abre un <dialog> (ventana modal) limpiando los errores que hubieran quedado de antes.
function abrirDialogo(id) {
  const d = document.getElementById(id);
  const form = $("form", d);
  if (form) Form.limpiarErrores(form);
  d.showModal();
  return d;
}
// Cualquier botón con el atributo data-cerrar cierra el diálogo que lo contiene.
document.addEventListener("click", (e) => {
  if (e.target.matches("[data-cerrar]")) e.target.closest("dialog").close();
});

// ── Formularios ───────────────────────────────────────────────────────
/**
 * Utilidades de formularios:
 *   llenar(form, datos)        pone los valores de un objeto en los campos (para editar)
 *   leer(form)                 junta los campos en un objeto (checkbox "salas[]" -> lista)
 *   marcarErrores(form, error) pinta en rojo cada campo con el mensaje que mandó el servidor
 *   enviar(form, accion)       todo junto: deshabilita el botón, envía, muestra errores o aviso
 */
const Form = {
  llenar(form, datos) {
    for (const el of form.elements) {
      if (!el.name || !(el.name.replace("[]", "") in datos)) continue;
      const v = datos[el.name.replace("[]", "")];
      if (el.type === "checkbox") el.checked = Array.isArray(v) ? v.includes(el.value) : !!v;
      else if (el.type === "radio") el.checked = el.value === String(v);
      else el.value = v ?? "";
    }
  },
  leer(form) {
    const datos = {};
    for (const el of form.elements) {
      if (!el.name || el.disabled) continue;
      if (el.name.endsWith("[]")) {
        const clave = el.name.slice(0, -2);
        datos[clave] ??= [];
        if (el.checked) datos[clave].push(el.value);
      } else if (el.type === "radio") {
        if (el.checked) datos[el.name] = el.value;
      } else if (el.type === "checkbox") {
        datos[el.name] = el.checked;
      } else {
        datos[el.name] = el.value;
      }
    }
    return datos;
  },
  limpiarErrores(form) {
    $$(".campo.error", form).forEach((c) => c.classList.remove("error"));
    $$(".campo .msg", form).forEach((m) => (m.textContent = ""));
    const g = $(".error-general", form.closest("dialog") || form);
    if (g) g.textContent = "";
  },
  marcarErrores(form, error) {
    this.limpiarErrores(form);
    const g = $(".error-general", form.closest("dialog") || form);
    if (error.campos) {
      let primero = null;
      for (const [campo, msg] of Object.entries(error.campos)) {
        const el = form.querySelector(`[name="${campo}"], [name="${campo}[]"]`);
        const caja = el?.closest(".campo");
        if (caja) {
          caja.classList.add("error");
          let m = $(".msg", caja);
          if (!m) { m = document.createElement("span"); m.className = "msg"; caja.append(m); }
          m.textContent = msg;
          primero ??= el;
        } else if (g) {
          g.textContent = msg;
        }
      }
      primero?.focus();
      if (g && !g.textContent) g.textContent = error.message;
    } else if (g) {
      g.textContent = error.message;
    } else {
      toast(error.message, "error");
    }
  },
  /** Envía el formulario con `accion(datos)`; maneja botón, errores y toast. */
  async enviar(form, accion) {
    const boton = form.querySelector("[type=submit]") || form.closest("dialog")?.querySelector("[type=submit]");
    boton?.classList.add("cargando");
    boton && (boton.disabled = true);
    try {
      const r = await accion(this.leer(form));
      this.limpiarErrores(form);
      if (r?.mensaje) toast(r.mensaje);
      return r;
    } catch (e) {
      this.marcarErrores(form, e);
      return null;
    } finally {
      boton?.classList.remove("cargando");
      boton && (boton.disabled = false);
    }
  },
};

// Ejecuta una acción mostrando una ruedita en el botón y evitando doble clic; si falla, toast.
async function conBoton(boton, fn) {
  boton.classList.add("cargando");
  boton.disabled = true;
  try { return await fn(); }
  catch (e) { toast(e.message, "error"); return null; }
  finally { boton.classList.remove("cargando"); boton.disabled = false; }
}

// ── KPIs ──────────────────────────────────────────────────────────────
// Dibuja las tarjetas de métricas. Si un valor cambió desde la última vez, la tarjeta
// "parpadea" (clase cambio) para que se note la actualización en tiempo real.
function renderKpis(contenedor, metricas) {
  const previos = Object.fromEntries($$(".kpi", contenedor).map((k) => [k.dataset.k, k.dataset.v]));
  contenedor.innerHTML = metricas.map((m) => {
    const valor = m.formato === "texto" ? esc(m.valor)
      : m.formato === "moneda" ? "Q." + Math.round(m.valor).toLocaleString("es-GT")
      : m.formato === "porcentaje" ? `${m.valor} %` : fmt.numero(m.valor);
    const cambio = previos[m.etiqueta] !== undefined && previos[m.etiqueta] !== String(m.valor) ? "cambio" : "";
    return `<div class="kpi ${m.tono || ""} ${cambio}" data-k="${esc(m.etiqueta)}" data-v="${esc(m.valor)}">
              <span>${esc(m.etiqueta)}</span><b>${valor}</b><small>${esc(m.detalle || "")}</small></div>`;
  }).join("");
}

// ── Panel de datos genérico ───────────────────────────────────────────
class PanelDatos {
  /**
   * cfg   = window.AREA_CONFIG (columnas, filtros, gráficos — lo arma Flask desde el módulo del área)
   * hooks = { acciones(r) -> [{texto, clase, fn}], alClic(r), alActualizar(datos) }
   */
  constructor(cfg, hooks = {}) {
    this.cfg = cfg;
    this.hooks = hooks;
    this.raiz = $("#panel-datos");
    this.estado = { vista: "tabla", filtros: {}, limpio: false, grafico: cfg.graficos[0]?.id, firma: null };
    this.registros = [];        // filas que se están mostrando (para las acciones por fila)
    this.claves = null;         // IDs de la última carga: sirve para resaltar filas nuevas
    this.ocupado = false;       // true mientras Spark trabaja: pausa el chequeo en vivo
    this.construir();
    this.cargar();
    setInterval(() => this.vigilar(), INTERVALO);   // "tabla en tiempo real"
  }

  // Modo actual de la tabla: "vivo" (datos tal cual), "filtrado" o "limpio" (BD 1 aplicada).
  get modo() {
    if (this.estado.limpio) return "limpio";
    return Object.keys(this.estado.filtros).length ? "filtrado" : "vivo";
  }

  // Estructura ---------------------------------------------------------
  // Arma la estructura una sola vez: encabezados de la tabla, formulario de filtros
  // (a partir de la lista FILTROS del área) y los radio buttons de gráficas.
  construir() {
    const r = this.raiz;
    $(".vista-tabla thead", r).innerHTML = `<tr>${this.cfg.columnas.map((c) =>
      `<th class="${["moneda", "numero"].includes(c.tipo) ? "num" : ""}">${esc(c.etiqueta)}</th>`).join("")}
      ${this.hooks.acciones ? "<th></th>" : ""}</tr>`;

    $("#form-filtros", r).innerHTML = this.cfg.filtros.map((f) => this.htmlFiltro(f)).join("") +
      `<div class="acciones">
         <button type="button" class="btn" data-quitar>Quitar filtros</button>
         <button type="submit" class="btn btn-primario">Aplicar filtro (PySpark)</button>
       </div>`;

    $(".lista-graficos", r).innerHTML = this.cfg.graficos.map((g, i) =>
      `<label><input type="radio" name="grafico" value="${g.id}" ${i === 0 ? "checked" : ""}>
         <span><b>${esc(g.titulo)}</b><small>${esc(g.tipo)}</small></span></label>`).join("");

    // Eventos
    $$("input[name=vista]", r).forEach((el) => el.addEventListener("change", () => this.cambiarVista(el.value)));
    $$("input[name=grafico]", r).forEach((el) => el.addEventListener("change", () => {
      this.estado.grafico = el.value;
      this.cargarGrafico();
    }));
    $("[data-abrir-filtros]", r).addEventListener("click", () => {
      const p = $("#form-filtros", r);
      p.hidden = !p.hidden;
      if (!p.hidden) $("input, select", p)?.focus();
    });
    $("#form-filtros", r).addEventListener("submit", (e) => { e.preventDefault(); this.aplicarFiltros(); });
    $("[data-quitar]", r).addEventListener("click", () => this.quitarFiltros());
    const botonLimpiar = $("[data-limpiar]", r);
    botonLimpiar.hidden = !this.cfg.limpieza;
    botonLimpiar.addEventListener("click", (e) => this.limpiar(e.currentTarget));
    $("[data-vivo]", r).addEventListener("click", () => this.volverAVivo());
  }

  // HTML de un filtro según su tipo (texto, opción, booleano, rango, fechas).
  // Los nombres de los campos (ej. salario_min) son los que lee core/filtros.py.
  htmlFiltro(f) {
    const id = `f-${f.id}`;
    switch (f.tipo) {
      case "texto":
        return `<div class="campo"><label for="${id}">${esc(f.etiqueta)}</label>
                <input type="search" id="${id}" name="${f.id}" placeholder="${esc(f.ayuda || "")}"></div>`;
      case "opcion":
        return `<div class="campo"><label for="${id}">${esc(f.etiqueta)}</label><select id="${id}" name="${f.id}">
                <option value="">Todos</option>${f.opciones.map(([v, t]) => `<option value="${esc(v)}">${esc(t)}</option>`).join("")}
                </select></div>`;
      case "booleano":
        return `<div class="campo"><label for="${id}">${esc(f.etiqueta)}</label><select id="${id}" name="${f.id}">
                <option value="">Todos</option><option value="1">Sí</option><option value="0">No</option></select></div>`;
      case "rango":
        return `<div class="campo"><span class="rotulo">${esc(f.etiqueta)}</span><div class="par">
                <input type="number" step="any" name="${f.id}_min" placeholder="mín." aria-label="${esc(f.etiqueta)} mínimo">
                <input type="number" step="any" name="${f.id}_max" placeholder="máx." aria-label="${esc(f.etiqueta)} máximo"></div></div>`;
      case "fechas":
        return `<div class="campo"><span class="rotulo">${esc(f.etiqueta)}</span><div class="par">
                <input type="date" name="${f.id}_desde" aria-label="${esc(f.etiqueta)} desde">
                <input type="date" name="${f.id}_hasta" aria-label="${esc(f.etiqueta)} hasta"></div></div>`;
      default: return "";
    }
  }

  // Datos ----------------------------------------------------------------
  // Pide tabla + métricas a SQL Server (GET /api/<area>/tabla). Si hay filtros o limpieza
  // activos, en vez de mostrar los datos crudos los vuelve a calcular en PySpark.
  async cargar() {
    try {
      const d = await API.get(`/api/${this.cfg.area}/tabla`);
      this.estado.firma = d.firma;
      this.tick = new Date();
      marcarSpark(d.spark);
      renderKpis($("#kpis"), d.metricas);
      if (this.modo === "vivo") this.renderTabla(d.registros, d.total);
      else await this.recalcular(false);
      this.hooks.alActualizar?.(d);
      if (this.estado.vista === "grafico") this.cargarGrafico();
      this.indicador();
    } catch (e) {
      toast(e.message, "error");
    }
  }

  /** Consulta liviana: si la huella de las tablas cambió, recarga. Así la tabla es "en tiempo real". */
  async vigilar() {
    if (this.ocupado || document.hidden) return;
    try {
      const d = await API.get(`/api/${this.cfg.area}/firma`);
      marcarSpark(d.spark);
      if (d.firma !== this.estado.firma) {
        this.estado.firma = d.firma;
        await this.cargar();
      }
      this.tick = new Date();
      this.indicador();
    } catch { /* servidor reiniciando: se reintenta solo */ }
  }

  // Filtra (y limpia, si corresponde) en PySpark: POST /api/<area>/filtrar.
  async recalcular(mostrarCarga = true) {
    this.ocupado = true;
    if (mostrarCarga) this.indicador("Procesando en PySpark…");
    try {
      const d = await API.post(`/api/${this.cfg.area}/filtrar`, { filtros: this.estado.filtros, limpio: this.estado.limpio });
      this.renderTabla(d.registros, d.total);
      this.renderChips(d.activos, d.total, d.segundos);
      if (this.estado.vista === "grafico") this.cargarGrafico();
    } finally {
      this.ocupado = false;
      this.indicador();
    }
  }

  // Botón "Aplicar filtro": guarda solo los campos completados y recalcula.
  async aplicarFiltros() {
    const leidos = Form.leer($("#form-filtros", this.raiz));
    this.estado.filtros = Object.fromEntries(Object.entries(leidos).filter(([, v]) => v !== "" && v != null));
    const boton = $("#form-filtros [type=submit]", this.raiz);
    await conBoton(boton, () => this.recalcular());
    if (this.modo === "vivo") this.cargar();
  }

  quitarFiltros() {
    $("#form-filtros", this.raiz).reset();
    this.estado.filtros = {};
    this.renderChips([]);
    if (this.modo === "vivo") this.cargar(); else this.recalcular();
  }

  // Botón "Limpiar datos" (solo Telemetría): ejecuta la Query BD 1 y muestra el reporte.
  async limpiar(boton) {
    this.ocupado = true;
    this.indicador("Limpiando en PySpark…");
    const d = await conBoton(boton, () => API.post(`/api/${this.cfg.area}/limpiar`));
    this.ocupado = false;
    if (!d) return this.indicador();
    this.estado.limpio = true;
    this.renderReporte(d);
    if (Object.keys(this.estado.filtros).length) await this.recalcular();
    else { this.renderTabla(d.registros, d.total_limpio); this.renderChips([]); }
    if (this.estado.vista === "grafico") this.cargarGrafico();
    this.indicador();
    toast(`Limpieza lista: ${d.total_limpio} válidos, ${d.total_descartados} descartados (${d.segundos} s).`);
  }

  // Descarta filtros y limpieza: la tabla vuelve a mostrar los datos tal cual.
  volverAVivo() {
    this.estado.limpio = false;
    $("#reporte", this.raiz).hidden = true;
    this.quitarFiltros();
  }

  // Render ---------------------------------------------------------------
  /** `total` = cuántos registros hay en realidad (puede ser más de los que se envían al navegador). */
  renderTabla(registros, total = registros.length) {
    // ".vista-tabla": la tabla principal. Sin esto se tomaría el primer <tbody> del panel,
    // que puede ser la tabla de descartados del reporte de limpieza (bug ya corregido).
    const cuerpo = $(".vista-tabla tbody", this.raiz);
    const clave = this.cfg.columnas[0].campo;
    // Filas que no estaban en la carga anterior -> se resaltan en verde un momento.
    const nuevas = this.claves ? new Set(registros.map((r) => r[clave]).filter((k) => !this.claves.has(k))) : new Set();
    this.claves = new Set(registros.map((r) => r[clave]));
    this.registros = registros;
    $(".contador", this.raiz).textContent = total > registros.length
      ? `${fmt.numero(total)} registros · se muestran los primeros ${registros.length}`
      : `${registros.length} registro${registros.length === 1 ? "" : "s"}`;
    if (!registros.length) {
      cuerpo.innerHTML = `<tr><td colspan="${this.cfg.columnas.length + 1}" class="vacio">Sin registros para mostrar.</td></tr>`;
      return;
    }
    cuerpo.innerHTML = registros.map((r, i) => {
      const clases = [nuevas.has(r[clave]) ? "nuevo" : "", this.hooks.alClic ? "clic" : "", r.activo === false ? "inactivo" : ""].join(" ");
      const acciones = this.hooks.acciones
        ? `<td class="acciones-fila">${this.hooks.acciones(r).map((a, j) =>
            `<button type="button" class="btn btn-mini ${a.clase || ""}" data-i="${i}" data-a="${j}">${esc(a.texto)}</button>`).join("")}</td>`
        : "";
      return `<tr class="${clases}" data-i="${i}">${this.cfg.columnas.map((c) =>
        `<td class="${["moneda", "numero"].includes(c.tipo) ? "num" : ""}">${celda(c, r)}</td>`).join("")}${acciones}</tr>`;
    }).join("");
    // Un solo listener para toda la tabla (delegación de eventos): detecta si se hizo clic
    // en un botón de acción (Editar, Dar de baja...) o en la fila (abrir expediente).
    cuerpo.onclick = (e) => {
      const b = e.target.closest("button[data-a]");
      if (b) {
        e.stopPropagation();
        const r = this.registros[b.dataset.i];
        this.hooks.acciones(r)[b.dataset.a].fn(r, b);
        return;
      }
      const tr = e.target.closest("tr[data-i]");
      if (tr && this.hooks.alClic) this.hooks.alClic(this.registros[tr.dataset.i]);
    };
  }

  // "Chips" arriba de la tabla con los filtros activos y cuánto tardó Spark.
  renderChips(activos = [], total, segundos) {
    const chips = $(".chips", this.raiz);
    const lista = [...(this.estado.limpio ? [`<span class="chip limpio">Datos limpios (PySpark)</span>`] : []),
                   ...activos.map((a) => `<span class="chip">${esc(a)}</span>`)];
    if (lista.length && total !== undefined) lista.push(`<span class="chip" style="background:none;color:var(--muted)">${total} resultado(s) · ${segundos} s en Spark</span>`);
    chips.innerHTML = lista.join("");
    chips.hidden = !lista.length;
    $("[data-vivo]", this.raiz).hidden = this.modo === "vivo";
  }

  // Reporte de la Query BD 1: tarjetas por paso (leídos, cada motivo, válidos) y la lista
  // de registros descartados con su motivo. La suma de los motivos = total descartados.
  renderReporte(d) {
    const caja = $("#reporte", this.raiz);
    const columnas = this.cfg.columnas;   // todas: así se ve el valor que causó el descarte
    caja.innerHTML = `
      <div class="reporte-cab">
        <h3>Query Big Data 1 · Limpieza y filtrado de anomalías</h3>
        <span class="badge ok">${d.total_limpio} válidos</span>
        <span class="badge alerta">${d.total_descartados} descartados</span>
        <span class="espacio"></span><small style="color:var(--muted)">${d.segundos} s en PySpark · el archivo CSV no se modifica</small>
      </div>
      <ol class="pasos">${d.pasos.map((p, i) => `
        <li class="${p.accion}" style="animation-delay:${i * 70}ms"><b>${esc(p.paso)}</b>
          <span>${p.afectados}</span> <small>${esc(p.accion)} · ${esc(p.detalle)}</small></li>`).join("")}
      </ol>
      ${d.descartados.length ? `<details class="descartes" open><summary>Registros descartados (${d.descartados.length})</summary>
        <div class="tabla-wrap" style="max-height:220px"><table class="datos"><thead><tr>
        ${columnas.map((c) => `<th>${esc(c.etiqueta)}</th>`).join("")}<th>Motivo</th></tr></thead><tbody>
        ${d.descartados.map((r) => `<tr>${columnas.map((c) => `<td>${celda(c, r)}</td>`).join("")}
          <td><span class="badge alerta">${esc(r.motivo)}</span></td></tr>`).join("")}
        </tbody></table></div></details>` : ""}`;
    caja.hidden = false;
  }

  // Indicador "En vivo · verificado hace N s" (o un texto temporal mientras Spark trabaja).
  indicador(texto) {
    const el = $(".envivo", this.raiz);
    el.classList.toggle("pausa", this.modo === "filtrado" || !!texto);
    el.classList.toggle("limpio", this.modo === "limpio" && !texto);
    const etiqueta = texto || { vivo: "En vivo", filtrado: "Filtrado · en vivo", limpio: "Datos limpios · en vivo" }[this.modo];
    const hace = this.tick ? Math.round((Date.now() - this.tick) / 1000) : null;
    el.innerHTML = `<i></i>${esc(etiqueta)} ${hace !== null && !texto ? `<small>· verificado hace ${hace} s</small>` : ""}`;
  }

  // Gráficas -------------------------------------------------------------
  // Radio button Tabla / Gráfico.
  cambiarVista(vista) {
    this.estado.vista = vista;
    $(".vista-tabla", this.raiz).hidden = vista !== "tabla";
    $(".vista-grafico", this.raiz).hidden = vista !== "grafico";
    if (vista === "grafico") this.cargarGrafico();
  }

  // La gráfica es una imagen PNG que genera el servidor con matplotlib. Se le pasan los
  // filtros activos por la URL (y "t" para que el navegador no use una copia vieja).
  cargarGrafico() {
    const lienzo = $(".lienzo", this.raiz);
    const img = $("img", lienzo);
    const params = new URLSearchParams({ ...this.estado.filtros, t: Date.now() });
    if (this.estado.limpio) params.set("limpio", "1");
    lienzo.classList.add("cargando");
    img.onload = () => lienzo.classList.remove("cargando");
    img.onerror = () => { lienzo.classList.remove("cargando"); toast("No se pudo generar la gráfica.", "error"); };
    img.src = `/api/${this.cfg.area}/grafico/${this.estado.grafico}?${params}`;
    const g = this.cfg.graficos.find((x) => x.id === this.estado.grafico);
    $(".nota", lienzo).textContent = `${g.tipo} · pandas + matplotlib · ${this.modo === "vivo" ? "datos en vivo" : this.modo === "limpio" ? "datos limpios" : "datos filtrados"}`;
  }
}

// ── Reloj de la cabecera ─────────────────────────────────────────────
// Reloj en vivo de la cabecera de cada página.
function iniciarReloj() {
  const el = $(".reloj");
  if (!el) return;
  const pintar = () => {
    const ahora = new Date();
    el.innerHTML = `<b>${ahora.toLocaleTimeString("es-GT", { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</b>
                    ${ahora.toLocaleDateString("es-GT", { weekday: "long", day: "numeric", month: "long", year: "numeric" })}`;
  };
  pintar();
  setInterval(pintar, 1000);
}
document.addEventListener("DOMContentLoaded", iniciarReloj);

/** Llena un <select> con [{id, nombre}] */
function opciones(select, lista, { vacio = "Elegí…", texto = (x) => x.nombre, valor = (x) => x.id } = {}) {
  select.innerHTML = (vacio !== null ? `<option value="">${esc(vacio)}</option>` : "") +
    lista.map((x) => `<option value="${esc(valor(x))}">${esc(texto(x))}</option>`).join("");
}
