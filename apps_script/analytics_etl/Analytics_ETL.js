/**
 * ==========================================================================
 * SISTEMA ANALYTICS ETL - INMOBILIARIA CRM v1.1
 * Archivo: Analytics_ETL.gs  (va en el archivo Google Sheets ANALYTICS separado)
 * --------------------------------------------------------------------------
 * Conecta tres fuentes y genera tablas listas para Looker Studio:
 *   1. CRM principal     → FACT_INTERACCIONES + DIM_CLIENTES
 *   2. Manifiestos/mes   → Hoja "Consolidado Looker" (configurados por ID)
 *   3. Archivo Ventas    → ID fijo
 * --------------------------------------------------------------------------
 * TABLAS DE SALIDA (hojas en este archivo Analytics):
 *   CONFIG_FUENTES       → Registro manual de IDs de archivos fuente
 *   DIM_ASESORES         → Catálogo de asesores con aliases para matching
 *   DIM_OPC              → Catálogo de OPCs extraídos automáticamente
 *   DIM_PROYECTOS        → Catálogo de proyectos
 *   STG_INTERACCIONES    → Espejo incremental de FACT_INTERACCIONES del CRM
 *   STG_PRESENCIAS       → Todos los manifiestos consolidados
 *   STG_VENTAS           → Ventas normalizada
 *   DATA_EMBUDO_FULL     → Embudo completo 8 etapas (Unpivoted → Looker)
 *   RPT_LEAD_TIEMPOS     → Lead asignado: FECHA_PRESENCIA + días desde asignación hasta esa presencia (manifiesto)
 *   DATA_TIPIFICACIONES_DIA → Gestión diaria por interacción con tipificación regularizada (ASIGNADOS/GESTION)
 *   RPT_ASESORES         → Métricas por asesor + tipo de gestión + fecha
 *   RPT_OPC              → Métricas por OPC (calidad de dato + embudo + datos falsos)
 *   PENDIENTES_MAPEO     → Asesores/OPCs sin alias definido (revisar manualmente)
 *   LOG_ETL              → Historial de ejecuciones del pipeline
 * --------------------------------------------------------------------------
 * LÓGICA DE TIPIFICACIONES (Looker):
 *   Leads               → COUNT DISTINCT CELULAR
 *   Contacto No Efectivo→ TIPIF IN ('AP','FS','DF','NC','NEX/FS','BZ','N/A','')
 *   Contacto Efectivo   → TIPIF NOT IN (lista anterior)
 *   Lead Potencial      → TIPIF IN ('CC','VLL','IW','GW','SG','HP','VP','CP','CXC','CZ','NSHOW','ASISTIO')
 *   Citas Agendadas     → del Manifiesto (+ TIPIF IN ('CC','CZ','HP','VP','NSHOW','ASISTIO') como proxy CRM)
 *   Dato Falso (OPC)    → TIPIF = 'DF'
 * --------------------------------------------------------------------------
 * VENTAS SIN CELULAR / SIN MATCH: se cuentan en totales como REFERIDO/SIN CELULAR
 * (DATA_EMBUDO_FULL: ORIGEN_VENTA; RPT_OPC: fila REFERIDO / DIRECTO). Cuando agregues
 * CELULAR en Ventas, el siguiente ETL vinculará por número y dejará de ser referido.
 * RECAUDO: columna HOY de Ventas (valor en soles) → RPT_ASESORES RECAUDO_HOY por asesor/mes.
 * --------------------------------------------------------------------------
 * ETL y límite de tiempo (Apps Script ~6 min):
 *   - runETL_Completo usa cadena: Parte1 → Parte2 (embudo) → Parte3 (tipif) → Parte4 (RPT).
 *   - CFG.ETL.USAR_CADENA_ETL = false para forzar una sola ejecución (datasets pequeños).
 * ==========================================================================
 */

/** Handlers disparados por trigger (continuación del ETL); no renombrar sin actualizar triggers. */
var ETL_HANDLERS_CADENA = [
  'runETL_Completo_Parte2',
  'runETL_Completo_Parte2B',
  'runETL_Completo_Parte3',
  'runETL_Completo_Parte4',
  'runETL_Completo_Parte5'
];

/** Hojas historicas que ya no se generan ni se mantienen en el flujo principal. */
var ETL_REPORTES_OBSOLETOS = [
  'RPT_OPC_COHORTE_MAR1_HOY_CAPTACION',
  'RPT_OPC_COHORTE_MAR1_HOY',
  'RPT_EMBUDO_REAL_KPIS'
];

var ETL_CHAIN_KEYS = {
  ACTIVE_TS: 'ETL_CHAIN_ACTIVE_TS',
  UPDATED_TS: 'ETL_CHAIN_UPDATED_TS',
  STEP: 'ETL_CHAIN_STEP',
  EXT_SOURCES_UPDATED_TS: 'ETL_EXT_SOURCES_UPDATED_TS',
  DASH_TRIGGER_VERSION: 'ETL_DASHBOARD_TRIGGER_VERSION'
};

var ETL_TODAY_END_MS_CACHE = null;
var DASHBOARD_FAST_TODAY_KEY_CACHE = null;

function etlConfigValue_(key, fallback) {
  try {
    var value = PropertiesService.getScriptProperties().getProperty(key);
    if (value) return value;
  } catch (e) {}
  return fallback || '';
}

// ==========================================================================
// 0. CONFIGURACIÓN GLOBAL
// ==========================================================================

var CFG = {
  // ── IDs de archivos fuente ──────────────────────────────────────────────
  CRM_ID      : etlConfigValue_('TRIGAL_CRM_ID', ''),
  VENTAS_ID   : etlConfigValue_('TRIGAL_VENTAS_ID', ''),
  // ID del archivo ANALYTICS (donde se escriben DATA_EMBUDO_FULL, RPT_*, etc.)
  // Obtener de la URL: docs.google.com/spreadsheets/d/[ESTE_ID]/edit
  // Si está vacío o 'AUTO', usa el spreadsheet al que está vinculado el script
  ANALYTICS_ID: 'AUTO',

  // ── Nombres de hojas en el CRM ──────────────────────────────────────────
  CRM_FACT : 'FACT_INTERACCIONES',
  CRM_DIM  : 'DIM_CLIENTES',

  // ── Nombre de hoja de ventas ────────────────────────────────────────────
  VENTAS_HOJA: 'VENTAS',

  // ── Nombre de la hoja consolidada dentro de cada Manifiesto mensual ────
  MANIF_HOJA: 'Consolidado Looker',

  // ── Nombres de hojas de salida en ESTE archivo Analytics ───────────────
  OUT: {
    CONFIG    : 'CONFIG_FUENTES',
    DIM_AS    : 'DIM_ASESORES',
    DIM_OPC   : 'DIM_OPC',
    DIM_PROY  : 'DIM_PROYECTOS',
    STG_INT   : 'STG_INTERACCIONES',
    STG_PRES  : 'STG_PRESENCIAS',
    STG_VTAS  : 'STG_VENTAS',
    EMBUDO    : 'DATA_EMBUDO_FULL',
    RPT_TIEMPOS: 'RPT_LEAD_TIEMPOS',
    TIPIF_DIA : 'DATA_TIPIFICACIONES_DIA',
    RPT_AS    : 'RPT_ASESORES',
    RPT_OPC   : 'RPT_OPC',
    RPT_OPC_REAL: 'RPT_OPC_REAL_KPIS',
    DASH_TIPIF_MAP: 'DASH_TIPIFICACION_MAP',
    DASH_LEADS: 'DASH_LEAD_STAGE_CACHE',
    DASH_EVENTS: 'DASH_EVENT_DAILY_CACHE',
    DASH_FILTERS: 'DASH_FILTER_OPTIONS',
    DASH_ALERTS: 'DASH_ALERTS_CACHE',
    PEND      : 'PENDIENTES_MAPEO',
    LOG       : 'LOG_ETL'
  },

  // ── Reglas del embudo (tipificaciones del CRM) ─────────────────────────
  // CONTACTO_NO_EFECTIVO: lead no contestó / número inválido / dato falso
  // CONTACTO_EFECTIVO   : cualquier tipificación que NO sea inválida
  // POTENCIALES         : manifestó interés o se agendó cita
  // CITAS               : tipificaciones que implican cita confirmada (proxy CRM)
  //                       Las citas reales se sacan del Manifiesto (etapa 5+)
  // DATOS_FALSOS        : tipificación DF → el OPC entregó un dato falso
  EMBUDO: {
    INVALIDOS   : ['AP','FS','DF','NC','NEX/FS','BZ','N/A',''],
    POTENCIALES : ['CC','VLL','IW','GW','SG','HP','VP','CP','CXC','CZ','NSHOW','ASISTIO'],
    CITAS       : ['CC','CZ','HP','VP','NSHOW','ASISTIO'],
    DATOS_FALSOS: ['DF']
  },

  // ── Tipos de acción en FACT_INTERACCIONES ───────────────────────────────
  TIPO_ASIGNACION: ['ASIGNACION','ASIGNACION_MANUAL'],
  TIPO_GESTION   : ['LLAMADA','EDICION_MANUAL','MIGRACION'],

  // ── Resultado válido de presencia en Manifiesto ─────────────────────────
  RESULTADO_TOUR    : 'TOUR',
  RESULTADO_NO_TOUR : ['NO TOURS','NO TOUR'],
  RESULTADO_NO_SHOW : 'NO SHOW',
  SEGUIMIENTO_ASISTIO: ['ASISTIÓ','ASISTIO'],

  // ── Rendimiento / límite 6 min (Apps Script) ────────────────────────────
  ETL: {
    /** Si true (default), Parte1 termina en staging y Parte2 corre vía trigger. */
    USAR_CADENA_ETL: true,
    /** Segundos de espera antes de ejecutar Parte2 (evita solaparse con Parte1). */
    CADENA_SEGUNDOS: 30,
    /** Frecuencia del trigger periodico. El flujo rapido ya no relee manifiestos/ventas en cada corrida. */
    TRIGGER_CADA_MINUTOS: 15,
    /** Si una cadena queda con trigger/estado pendiente mas tiempo que esto, se considera colgada y se reinicia. */
    CHAIN_STALE_MINUTES: 45,
    /** Si filas de datos > este umbral, no autoajusta columnas (muy lento en hojas grandes). */
    AUTO_RESIZE_MAX_ROWS: 6000,
    /** DATA_TIPIFICACIONES_DIA es pesada y ya no debe bloquear el dashboard web. */
    GENERAR_TIPIF_DIA_EN_CADENA: false,
    /** El dashboard web consume snapshot JSON; las hojas DASH_* quedan como auditoria opcional. */
    ESCRIBIR_DASH_SHEETS: false,
    /** Presencias/ventas se refrescan desde archivos externos solo cuando vence este TTL. */
    FUENTES_EXTERNAS_TTL_MINUTES: 60,
    /** ALL evita que el dashboard se quede sin eventos al filtrar dias historicos. */
    DASHBOARD_EVENT_SCOPE: 'ALL',
    /** Versiona la instalacion automatica del trigger rapido. Cambiar si se modifica frecuencia/handler. */
    DASHBOARD_TRIGGER_VERSION: 'dashboard-fast-v2-20260610'
  }
};

/** Umbral de filas para autoResize; por defecto 6000. */
function etl_autoResizeMaxRows() {
  return (CFG.ETL && CFG.ETL.AUTO_RESIZE_MAX_ROWS != null) ? CFG.ETL.AUTO_RESIZE_MAX_ROWS : 6000;
}

/** Segundos entre Parte1 y Parte2 del ETL en cadena. */
function etl_cadenaSegundos() {
  var s = (CFG.ETL && CFG.ETL.CADENA_SEGUNDOS != null) ? CFG.ETL.CADENA_SEGUNDOS : 90;
  return Math.max(30, Math.min(300, s));
}

function etl_triggerCadaMinutos() {
  var m = (CFG.ETL && CFG.ETL.TRIGGER_CADA_MINUTOS != null) ? CFG.ETL.TRIGGER_CADA_MINUTOS : 180;
  var permitidos = [1, 5, 10, 15, 30];
  for (var i = 0; i < permitidos.length; i++) {
    if (m <= permitidos[i]) return permitidos[i];
  }
  return Math.max(60, Math.round(m));
}

function etl_generarTipifDiaEnCadena() {
  return CFG.ETL && CFG.ETL.GENERAR_TIPIF_DIA_EN_CADENA === true;
}

function dashboard_escribirDashSheets_() {
  return CFG.ETL && CFG.ETL.ESCRIBIR_DASH_SHEETS === true;
}

function etl_fuentesExternasTtlMs_() {
  var min = (CFG.ETL && CFG.ETL.FUENTES_EXTERNAS_TTL_MINUTES != null) ? CFG.ETL.FUENTES_EXTERNAS_TTL_MINUTES : 60;
  return Math.max(15, min) * 60 * 1000;
}

function dashboard_eventScopeMode_() {
  return String((CFG.ETL && CFG.ETL.DASHBOARD_EVENT_SCOPE) || 'ALL').toUpperCase().trim();
}

function dashboard_eventScopeLabel_() {
  var mode = dashboard_eventScopeMode_();
  return mode === 'DEFAULT_RANGE' ? 'DEFAULT_RANGE' : 'ALL';
}

function etl_chainStaleMs() {
  var min = (CFG.ETL && CFG.ETL.CHAIN_STALE_MINUTES != null) ? CFG.ETL.CHAIN_STALE_MINUTES : 45;
  return Math.max(15, min) * 60 * 1000;
}

/** Si false, runETL_Completo intenta todo en una sola ejecución. */
function etl_usarCadenaEtl() {
  return !CFG.ETL || CFG.ETL.USAR_CADENA_ETL !== false;
}

function etl_marcarPasoCadena(paso) {
  try {
    var props = PropertiesService.getScriptProperties();
    var now = String(Date.now());
    if (!props.getProperty(ETL_CHAIN_KEYS.ACTIVE_TS)) props.setProperty(ETL_CHAIN_KEYS.ACTIVE_TS, now);
    props.setProperty(ETL_CHAIN_KEYS.UPDATED_TS, now);
    props.setProperty(ETL_CHAIN_KEYS.STEP, paso || '');
  } catch (e) {}
}

function etl_iniciarPasoCadena(handlerName) {
  try { etl_borraSoloHandler(handlerName); } catch (e) {}
  etl_marcarPasoCadena(handlerName);
}

function etl_limpiarEstadoCadena(motivo) {
  try { etl_borraTriggersCadenaEtl(); } catch (e) {}
  try {
    var props = PropertiesService.getScriptProperties();
    props.deleteProperty(ETL_CHAIN_KEYS.ACTIVE_TS);
    props.deleteProperty(ETL_CHAIN_KEYS.UPDATED_TS);
    props.deleteProperty(ETL_CHAIN_KEYS.STEP);
  } catch (e2) {}
  if (motivo) etl_log('WARN', 'ETL_CADENA', 'Estado de cadena limpiado: ' + motivo);
}

function etl_finalizarCadena() {
  try { etl_borraTriggersCadenaEtl(); } catch (e0) {}
  try {
    var props = PropertiesService.getScriptProperties();
    props.deleteProperty(ETL_CHAIN_KEYS.ACTIVE_TS);
    props.deleteProperty(ETL_CHAIN_KEYS.UPDATED_TS);
    props.deleteProperty(ETL_CHAIN_KEYS.STEP);
  } catch (e) {}
}

/** autoResize solo en hojas medianas/pequeñas. */
function etl_maybeAutoResizeColumns(sheet, numCols, dataRowCount) {
  try {
    if (dataRowCount <= etl_autoResizeMaxRows()) sheet.autoResizeColumns(1, numCols);
  } catch (e) { /* no bloquear ETL */ }
}

/** Elimina triggers pendientes de Parte2/3/4 del ETL (evita duplicados y ejecuciones cruzadas). */
function etl_borraTriggersCadenaEtl() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = triggers.length - 1; i >= 0; i--) {
    var fn = triggers[i].getHandlerFunction();
    for (var j = 0; j < ETL_HANDLERS_CADENA.length; j++) {
      if (fn === ETL_HANDLERS_CADENA[j]) {
        ScriptApp.deleteTrigger(triggers[i]);
        break;
      }
    }
  }
}

/** Igual que etl_borraTriggersCadenaEtl; muestra confirmación en UI (menú Hoja). */
function etl_borraTriggersCadenaEtl_UI() {
  try {
    etl_limpiarEstadoCadena('limpieza manual desde menu');
    try {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        'Triggers de cadena Parte 2/3/4 eliminados.',
        'Cadena ETL',
        5
      );
    } catch (toastErr) {
      etl_log('INFO','etl_borraTriggersCadenaEtl_UI','Triggers cadena eliminados (sin UI disponible)');
    }
  } catch (e) {
    etl_alertUI('Error', 'No se pudieron borrar triggers: ' + e.message);
  }
}

/** Elimina solo triggers pendientes de una función concreta (evita duplicados del mismo paso). */
function etl_borraSoloHandler(handlerName) {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = triggers.length - 1; i >= 0; i--) {
    if (triggers[i].getHandlerFunction() === handlerName) {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
}

function etl_instalarTriggerDashboardRapido_() {
  var functionName = 'runETL_DashboardRapido';
  var legacyFunctionName = 'runETL_Completo';
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = triggers.length - 1; i >= 0; i--) {
    var handler = triggers[i].getHandlerFunction();
    if (handler === functionName || handler === legacyFunctionName) {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  var cadaMin = etl_triggerCadaMinutos();
  var tb = ScriptApp.newTrigger(functionName).timeBased();
  if (cadaMin < 60) {
    tb.everyMinutes(cadaMin).create();
  } else {
    tb.everyHours(Math.max(1, Math.round(cadaMin / 60))).create();
  }
  return cadaMin;
}

function etl_asegurarTriggerDashboardRapido_() {
  var version = (CFG.ETL && CFG.ETL.DASHBOARD_TRIGGER_VERSION) || 'dashboard-fast';
  try {
    var props = PropertiesService.getScriptProperties();
    if (props.getProperty(ETL_CHAIN_KEYS.DASH_TRIGGER_VERSION) === version) return;
    var cadaMin = etl_instalarTriggerDashboardRapido_();
    props.setProperty(ETL_CHAIN_KEYS.DASH_TRIGGER_VERSION, version);
    etl_log('INFO', 'etl_asegurarTriggerDashboardRapido_',
      'Trigger dashboard rapido autoajustado cada ' + cadaMin + ' min | version ' + version);
  } catch (e) {
    etl_log('WARN', 'etl_asegurarTriggerDashboardRapido_', e.message);
  }
}

function etl_stgTieneDatos_(sheetName) {
  try {
    var sh = getSpreadsheetDestino().getSheetByName(sheetName);
    return !!(sh && sh.getLastRow && sh.getLastRow() > 1);
  } catch (e) {
    return false;
  }
}

function etl_debeRefrescarFuentesExternas_() {
  if (!etl_stgTieneDatos_(CFG.OUT.STG_PRES) || !etl_stgTieneDatos_(CFG.OUT.STG_VTAS)) return true;
  try {
    var ts = Number(PropertiesService.getScriptProperties().getProperty(ETL_CHAIN_KEYS.EXT_SOURCES_UPDATED_TS) || 0);
    if (!ts) return true;
    return (Date.now() - ts) >= etl_fuentesExternasTtlMs_();
  } catch (e) {
    return true;
  }
}

function etl_marcarFuentesExternasActualizadas_() {
  try {
    PropertiesService.getScriptProperties().setProperty(ETL_CHAIN_KEYS.EXT_SOURCES_UPDATED_TS, String(Date.now()));
  } catch (e) {}
}

/**
 * Programa la siguiente fase. Solo borra triggers del MISMO handler que va a crearse (no borra Parte3/4 pendientes).
 * IMPORTANTE: No llamar etl_borraTriggersCadenaEtl() al INICIO de Parte2/3/4 — borraba la siguiente fase ya programada.
 */
function etl_programarSiguienteCadena(handlerName) {
  try {
    etl_borraSoloHandler(handlerName);
    etl_marcarPasoCadena(handlerName + ' pendiente');
    ScriptApp.newTrigger(handlerName).timeBased().after(etl_cadenaSegundos() * 1000).create();
    etl_log('INFO', 'ETL_CADENA', 'Trigger OK → ' + handlerName + ' en ~' + etl_cadenaSegundos() + 's');
  } catch (e) {
    etl_log('ERROR', 'ETL_CADENA',
      'No se pudo crear trigger para ' + handlerName + ': ' + e.message +
      '. Ejecuta manualmente esa función desde el menú «Cadena ETL» o el editor.');
    etl_limpiarEstadoCadena('no se pudo crear trigger para ' + handlerName);
  }
}


// ==========================================================================
// 1. FUNCIÓN ORQUESTADORA PRINCIPAL
// ==========================================================================

/**
 * Punto de entrada del ETL completo.
 * Por defecto corre en cadena (Parte1→2→3→4 con triggers) para no exceder el límite de 6 min.
 * Desactivar: CFG.ETL.USAR_CADENA_ETL = false
 */
function runETL_Completo() {
  if (etl_usarCadenaEtl()) {
    runETL_Completo_Parte1();
    return;
  }

  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) { etl_log('ERROR','runETL_Completo','Lock no disponible: ' + e.message); return; }

  try {
    var t0 = Date.now();
    etl_eliminarReportesObsoletos();
    etl_log('INFO','runETL_Completo','⚡ ETL iniciado (monolítico)...');

    var crm = fase_leerCRM();
    etl_log('INFO','runETL_Completo',
      '✅ CRM: ' + crm.fact.length + ' interacciones | ' + crm.dim.length + ' clientes');

    fase_stgInteracciones(crm.fact);

    var presencias = fase_leerManifiestos();
    etl_log('INFO','runETL_Completo','✅ Presencias: ' + presencias.length + ' registros');

    fase_stgPresencias(presencias);

    var ventas = fase_leerVentas();
    etl_log('INFO','runETL_Completo','✅ Ventas: ' + ventas.length + ' registros');

    fase_stgVentas(ventas);
    etl_marcarFuentesExternasActualizadas_();

    fase_actualizarDims(presencias, crm.fact);

    fase_embudoCompleto(crm.fact, presencias, ventas);

    if (etl_generarTipifDiaEnCadena()) {
      fase_dataTipificacionesDia(crm.fact, presencias);
    } else {
      etl_log('INFO','runETL_Completo','DATA_TIPIFICACIONES_DIA omitida: dashboard usa DASH_EVENT_DAILY_CACHE.');
    }

    fase_rptAsesores(crm.fact, presencias, ventas);

    fase_rptOPC(crm.fact, presencias, ventas);

    fase_rptOPCRealKPIs();
    fase_dashboardCaches(crm.fact, presencias, ventas);

    etl_log('INFO','runETL_Completo','🏁 ETL completado en ' + (Date.now() - t0) + 'ms');

  } catch (e) {
    etl_log('ERROR','runETL_Completo','Error crítico: ' + e.message);
  } finally {
    lock.releaseLock();
  }
}

/**
 * Parte 1: lectura CRM, staging interacciones, manifiestos, staging presencias,
 * ventas, staging ventas, dimensiones.
 *
 * Guard anti-colisión (doble capa):
 *  1. Triggers pendientes: si hay un trigger de Parte 2/3/4/5 ya programado,
 *     una cadena está en curso → abortar para no pisarla.
 *  2. Property flag: si ETL_CHAIN_ACTIVE_TS está dentro de los últimos 30 min,
 *     probable cadena viva aunque los triggers ya hayan disparado entre partes.
 */
function runETL_Completo_Parte1() {
  var props = PropertiesService.getScriptProperties();
  var chainTs = props.getProperty(ETL_CHAIN_KEYS.ACTIVE_TS);
  var chainUpdatedTs = props.getProperty(ETL_CHAIN_KEYS.UPDATED_TS) || chainTs;
  var now = Date.now();
  var chainAgeMs = chainUpdatedTs ? (now - Number(chainUpdatedTs)) : 0;
  var chainStale = !chainUpdatedTs || chainAgeMs > etl_chainStaleMs();

  // ── GUARD 1: triggers de cadena pendientes ─────────────────────────────
  var allTriggers = ScriptApp.getProjectTriggers();
  for (var gi = 0; gi < allTriggers.length; gi++) {
    var gfn = allTriggers[gi].getHandlerFunction();
    for (var gj = 0; gj < ETL_HANDLERS_CADENA.length; gj++) {
      if (gfn === ETL_HANDLERS_CADENA[gj]) {
        if (chainStale) {
          etl_log('WARN','runETL_Completo_Parte1',
            'Cadena ETL colgada (' + gfn + ' pendiente por ' +
            Math.round(chainAgeMs / 60000) + ' min). Se limpia y se reinicia.');
          etl_limpiarEstadoCadena('trigger pendiente stale: ' + gfn);
          break;
        }
        etl_log('WARN','runETL_Completo_Parte1',
          'Cadena ETL en progreso (' + gfn + ' pendiente) → trigger periódico ignorado. ' +
          'Si quieres forzar un reinicio usa el menú "⚡ Ejecutar ETL Completo".');
        return;
      }
    }
  }

  // ── GUARD 2: property flag con ventana de 30 min ───────────────────────
  var props   = PropertiesService.getScriptProperties();
  var CHAIN_KEY = 'ETL_CHAIN_ACTIVE_TS';
  var chainTs   = props.getProperty(CHAIN_KEY);
  var now       = Date.now();
  var VENTANA_MS = 30 * 60 * 1000; // 30 minutos
  if (chainTs && (now - Number(chainTs)) < VENTANA_MS) {
    etl_log('WARN','runETL_Completo_Parte1',
      'Property ETL_CHAIN_ACTIVE_TS indica cadena reciente (' +
      Math.round((now - Number(chainTs)) / 60000) + ' min ago) → trigger ignorado.');
    return;
  }
  // Marcar cadena activa
  etl_marcarPasoCadena('runETL_Completo_Parte1');

  // ── EJECUCIÓN NORMAL ───────────────────────────────────────────────────
  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_Completo_Parte1','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en Parte1');
    return;
  }

  try {
    var t0 = Date.now();
    etl_eliminarReportesObsoletos();
    etl_log('INFO','runETL_Completo_Parte1','⚡ ETL Parte 1/4 (staging)...');

    var crm = fase_leerCRM();
    etl_log('INFO','runETL_Completo_Parte1',
      '✅ CRM: ' + crm.fact.length + ' interacciones | ' + crm.dim.length + ' clientes');

    fase_stgInteracciones(crm.fact);

    var presencias = fase_leerManifiestos();
    etl_log('INFO','runETL_Completo_Parte1','✅ Presencias: ' + presencias.length + ' registros');

    fase_stgPresencias(presencias);

    var ventas = fase_leerVentas();
    etl_log('INFO','runETL_Completo_Parte1','✅ Ventas: ' + ventas.length + ' registros');

    fase_stgVentas(ventas);
    etl_marcarFuentesExternasActualizadas_();

    fase_actualizarDims(presencias, crm.fact);

    etl_log('INFO','runETL_Completo_Parte1',
      '✅ Parte 1 lista en ' + (Date.now() - t0) + 'ms. Programando Parte 2 (embudo)...');

    etl_borraTriggersCadenaEtl();
    etl_programarSiguienteCadena('runETL_Completo_Parte2');

  } catch (e) {
    etl_log('ERROR','runETL_Completo_Parte1','Error: ' + e.message + ' (no se programa Parte 2)');
    etl_limpiarEstadoCadena('error en Parte1');
  } finally {
    lock.releaseLock();
  }
}

/**
 * Flujo rapido para la Web App.
 * Actualiza STG desde las fuentes vivas y publica solo el payload compacto del dashboard.
 * No recalcula DATA_EMBUDO_FULL ni RPT_*; esos reportes quedan para runETL_Completo manual.
 */
function runETL_DashboardRapido() {
  etl_resetRuntimeDateCaches_();
  etl_asegurarTriggerDashboardRapido_();
  if (etl_hayCadenaActiva_('runETL_DashboardRapido')) return;
  etl_marcarPasoCadena('runETL_DashboardRapido');

  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_DashboardRapido','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en dashboard rapido');
    return;
  }

  try {
    var t0 = Date.now();
    etl_eliminarReportesObsoletos();
    etl_log('INFO','runETL_DashboardRapido','Dashboard rapido 1/2 (staging + payload web)...');

    var crm = fase_leerCRM();
    etl_log('INFO','runETL_DashboardRapido',
      'CRM: ' + crm.fact.length + ' interacciones | ' + crm.dim.length + ' clientes');
    fase_stgInteracciones(crm.fact);

    var refrescarExternas = etl_debeRefrescarFuentesExternas_();
    var presencias = [];
    var ventas = [];

    if (refrescarExternas) {
      etl_log('INFO','runETL_DashboardRapido','Fuentes externas vencidas: refrescando manifiestos y ventas...');
      presencias = fase_leerManifiestos();
      etl_log('INFO','runETL_DashboardRapido','Presencias: ' + presencias.length + ' registros');
      fase_stgPresencias(presencias);

      ventas = fase_leerVentas();
      etl_log('INFO','runETL_DashboardRapido','Ventas: ' + ventas.length + ' registros');
      fase_stgVentas(ventas);

      fase_actualizarDims(presencias, crm.fact);
      etl_marcarFuentesExternasActualizadas_();

      etl_log('INFO','runETL_DashboardRapido',
        'STG externo listo en ' + (Date.now() - t0) + 'ms. Programando Parte 5 para no arriesgar timeout...');

      etl_borraTriggersCadenaEtl();
      etl_programarSiguienteCadena('runETL_Completo_Parte5');
      return;
    }

    presencias = etl_leerPresenciasDesdeSTG();
    ventas = etl_leerVentasDesdeSTG();
    etl_log('INFO','runETL_DashboardRapido',
      'Fuentes externas desde STG: presencias ' + presencias.length +
      ' | ventas ' + ventas.length +
      ' | ' + (Date.now() - t0) + 'ms');

    fase_dashboardCaches(crm.fact, presencias, ventas);
    etl_log('INFO','runETL_DashboardRapido',
      'Dashboard rapido publicado en una sola ejecucion: ' + (Date.now() - t0) + 'ms');
    etl_finalizarCadena();

  } catch (e2) {
    etl_log('ERROR','runETL_DashboardRapido','Error: ' + e2.message);
    etl_limpiarEstadoCadena('error en dashboard rapido');
  } finally {
    lock.releaseLock();
  }
}

function runETL_DashboardRapido_Manual() {
  etl_borraSoloHandler('runETL_DashboardRapido_Manual');
  runETL_DashboardRapido();
}

function etl_hayCadenaActiva_(caller) {
  var props = PropertiesService.getScriptProperties();
  var chainTs = props.getProperty(ETL_CHAIN_KEYS.ACTIVE_TS);
  var chainUpdatedTs = props.getProperty(ETL_CHAIN_KEYS.UPDATED_TS) || chainTs;
  var now = Date.now();
  var chainAgeMs = chainUpdatedTs ? (now - Number(chainUpdatedTs)) : 0;
  var chainStale = !chainUpdatedTs || chainAgeMs > etl_chainStaleMs();
  var triggers = ScriptApp.getProjectTriggers();

  for (var i = 0; i < triggers.length; i++) {
    var fn = triggers[i].getHandlerFunction();
    for (var j = 0; j < ETL_HANDLERS_CADENA.length; j++) {
      if (fn === ETL_HANDLERS_CADENA[j]) {
        if (chainStale) {
          etl_log('WARN', caller,
            'Cadena ETL colgada (' + fn + ' pendiente por ' +
            Math.round(chainAgeMs / 60000) + ' min). Se limpia y se reinicia.');
          etl_limpiarEstadoCadena('trigger pendiente stale: ' + fn);
          return false;
        }
        etl_log('WARN', caller,
          'Cadena ETL en progreso (' + fn + ' pendiente) -> ejecucion ignorada para evitar cruce.');
        return true;
      }
    }
  }

  if (chainTs && !chainStale) {
    etl_log('WARN', caller,
      'Cadena ETL activa: actualizada hace ' +
      Math.round(chainAgeMs / 60000) + ' min; iniciada hace ' +
      Math.round((now - Number(chainTs)) / 60000) + ' min -> ejecucion ignorada.');
    return true;
  }

  if (chainStale && chainTs) etl_limpiarEstadoCadena('property stale sin trigger');
  return false;
}

/**
 * Parte 2: DATA_EMBUDO_FULL + RPT_LEAD_TIEMPOS.
 * Lee desde STG_ (mismo spreadsheet) para evitar re-llamadas a fuentes externas
 * y ahorrar ~2 min frente a la lectura cruda original.
 */
function runETL_Completo_Parte2() {
  etl_iniciarPasoCadena('runETL_Completo_Parte2');
  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_Completo_Parte2','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en Parte2');
    return;
  }

  try {
    var t0 = Date.now();
    etl_log('INFO','runETL_Completo_Parte2','⚡ ETL Parte 2/4 (embudo + tiempos) — lectura desde STG...');

    var factRows  = etl_leerFactDesdeSTG();
    var presencias = etl_leerPresenciasDesdeSTG();
    var ventas    = etl_leerVentasDesdeSTG();

    etl_log('INFO','runETL_Completo_Parte2',
      'STG leído: FACT ' + factRows.length + ' | Presencias ' + presencias.length + ' | Ventas ' + ventas.length);

    fase_embudoCompleto(factRows, presencias, ventas, { skipRptLeadTiempos: true });

    etl_log('INFO','runETL_Completo_Parte2',
      '✅ Parte 2 lista en ' + (Date.now() - t0) + 'ms. Programando Parte 3...');

    etl_log('INFO','runETL_Completo_Parte2','Siguiente paso real: Parte 2B (RPT_LEAD_TIEMPOS).');
    etl_programarSiguienteCadena('runETL_Completo_Parte2B');

  } catch (e) {
    etl_log('ERROR','runETL_Completo_Parte2','Error crítico: ' + e.message + ' (no se programa Parte 3)');
    etl_limpiarEstadoCadena('error en Parte2');
  } finally {
    lock.releaseLock();
  }
}

/**
 * Parte 3: solo DATA_TIPIFICACIONES_DIA (tabla más pesada en filas de salida).
 * Lee desde STG_ para ahorrar ~2 min de I/O externo.
 */
function runETL_Completo_Parte3() {
  etl_iniciarPasoCadena('runETL_Completo_Parte3');

  if (!etl_generarTipifDiaEnCadena()) {
    etl_log('INFO','runETL_Completo_Parte3',
      'DATA_TIPIFICACIONES_DIA omitida para evitar timeout. Programando Parte 4...');
    etl_programarSiguienteCadena('runETL_Completo_Parte4');
    return;
  }

  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_Completo_Parte3','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en Parte3');
    return;
  }

  try {
    var t0 = Date.now();
    etl_log('INFO','runETL_Completo_Parte3','⚡ ETL Parte 3/4 (tipif) — lectura desde STG...');

    var factRows   = etl_leerFactDesdeSTG();
    var presencias = etl_leerPresenciasDesdeSTG();

    etl_log('INFO','runETL_Completo_Parte3',
      'STG leído: FACT ' + factRows.length + ' | Presencias ' + presencias.length);

    fase_dataTipificacionesDia(factRows, presencias);

    etl_log('INFO','runETL_Completo_Parte3',
      '✅ Parte 3 lista en ' + (Date.now() - t0) + 'ms. Programando Parte 4...');

    etl_programarSiguienteCadena('runETL_Completo_Parte4');

  } catch (e) {
    etl_log('ERROR','runETL_Completo_Parte3','Error crítico: ' + e.message + ' (no se programa Parte 4)');
    etl_limpiarEstadoCadena('error en Parte3');
  } finally {
    lock.releaseLock();
  }
}

/** Generacion manual de la tabla historica DATA_TIPIFICACIONES_DIA.
 * No forma parte del flujo critico porque puede superar el limite de Apps Script.
 */
function runETL_TipificacionesDia_Manual() {
  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) { etl_log('ERROR','runETL_TipificacionesDia_Manual','Lock no disponible: ' + e.message); return; }

  try {
    var t0 = Date.now();
    etl_log('INFO','runETL_TipificacionesDia_Manual','Generacion manual DATA_TIPIFICACIONES_DIA...');
    var factRows = etl_leerFactDesdeSTG();
    var presencias = etl_leerPresenciasDesdeSTG();
    fase_dataTipificacionesDia(factRows, presencias);
    etl_log('INFO','runETL_TipificacionesDia_Manual','Lista en ' + (Date.now() - t0) + 'ms');
  } catch (e2) {
    etl_log('ERROR','runETL_TipificacionesDia_Manual', e2.message);
  } finally {
    lock.releaseLock();
  }
}

/** Parte 2B: RPT_LEAD_TIEMPOS separado para no forzar timeout en Parte 2. */
function runETL_Completo_Parte2B() {
  etl_iniciarPasoCadena('runETL_Completo_Parte2B');
  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_Completo_Parte2B','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en Parte2B');
    return;
  }

  try {
    var t0 = Date.now();
    etl_log('INFO','runETL_Completo_Parte2B','ETL Parte 2B/5 (tiempos asignacion -> presencia) - lectura desde STG...');

    var factRows = etl_leerFactDesdeSTG();
    var presencias = etl_leerPresenciasDesdeSTG();
    fase_rptLeadTiemposDesdeSTG(factRows, presencias);

    etl_log('INFO','runETL_Completo_Parte2B',
      'Parte 2B lista en ' + (Date.now() - t0) + 'ms. Programando Parte 3...');

    etl_programarSiguienteCadena('runETL_Completo_Parte3');

  } catch (e2) {
    etl_log('ERROR','runETL_Completo_Parte2B','Error critico: ' + e2.message + ' (no se programa Parte 3)');
    etl_limpiarEstadoCadena('error en Parte2B');
  } finally {
    lock.releaseLock();
  }
}

/**
 * Parte 4: RPT_ASESORES + RPT_OPC.
 * Lee desde STG_ para ahorrar ~2 min de I/O externo.
 */
function runETL_Completo_Parte4() {
  etl_iniciarPasoCadena('runETL_Completo_Parte4');
  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_Completo_Parte4','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en Parte4');
    return;
  }

  try {
    var t0 = Date.now();
    etl_log('INFO','runETL_Completo_Parte4','⚡ ETL Parte 4/4 (reportes asesor/OPC) — lectura desde STG...');

    var factRows   = etl_leerFactDesdeSTG();
    var presencias = etl_leerPresenciasDesdeSTG();
    var ventas     = etl_leerVentasDesdeSTG();

    etl_log('INFO','runETL_Completo_Parte4',
      'STG leído: FACT ' + factRows.length + ' | Presencias ' + presencias.length + ' | Ventas ' + ventas.length);

    fase_rptAsesores(factRows, presencias, ventas);
    fase_rptOPC(factRows, presencias, ventas);
    fase_rptOPCRealKPIs();

    etl_log('INFO','runETL_Completo_Parte4',
      '🏁 Cadena ETL finalizada en ' + (Date.now() - t0) + 'ms');

    etl_log('INFO','runETL_Completo_Parte4','Programando Parte 5 (dashboard cache)...');
    etl_programarSiguienteCadena('runETL_Completo_Parte5');

  } catch (e) {
    etl_log('ERROR','runETL_Completo_Parte4','Error crítico: ' + e.message);
    etl_limpiarEstadoCadena('error en Parte4');
  } finally {
    lock.releaseLock();
  }
}

/** Parte 5: cache optimizada para Dashboard Web. */
function runETL_Completo_Parte5() {
  etl_resetRuntimeDateCaches_();
  etl_borraSoloHandler('runETL_Completo_Parte5');
  etl_iniciarPasoCadena('runETL_Completo_Parte5');
  var lock = LockService.getScriptLock();
  try { lock.waitLock(300000); }
  catch (e) {
    etl_log('ERROR','runETL_Completo_Parte5','Lock no disponible: ' + e.message);
    etl_limpiarEstadoCadena('lock no disponible en Parte5');
    return;
  }

  try {
    var t0 = Date.now();
    etl_log('INFO','runETL_Completo_Parte5','ETL Parte 5/5 (dashboard cache) — lectura desde STG...');
    var factRows = etl_leerFactDesdeSTG();
    var presencias = etl_leerPresenciasDesdeSTG();
    var ventas = etl_leerVentasDesdeSTG();
    fase_dashboardCaches(factRows, presencias, ventas);
    etl_log('INFO','runETL_Completo_Parte5',
      'Cadena ETL finalizada en Parte 5: ' + (Date.now() - t0) + 'ms');
    etl_finalizarCadena();
  } catch (e2) {
    etl_log('ERROR','runETL_Completo_Parte5','Error critico: ' + e2.message);
    etl_limpiarEstadoCadena('error en Parte5');
  } finally {
    lock.releaseLock();
  }
}


// ==========================================================================
// LECTORES DESDE STG (evitan re-llamadas a fuentes externas en Parte 2/3/4)
// --------------------------------------------------------------------------
// En Parte 1 se cargan los datos crudos y se escriben en STG_INTERACCIONES,
// STG_PRESENCIAS y STG_VENTAS (mismo spreadsheet). Las partes siguientes
// leen desde esas hojas locales en lugar de repetir las llamadas externas,
// ahorrando ~2 minutos de I/O por parte y permitiendo completar la cadena
// completa dentro del límite de 6 minutos de Apps Script.
// ==========================================================================

/**
 * Lee STG_INTERACCIONES y reconstruye el array factRows con la misma
 * estructura que devuelve fase_leerCRM().fact.
 */
function etl_leerFactDesdeSTG() {
  var ss    = getSpreadsheetDestino();
  var sheet = ss.getSheetByName(CFG.OUT.STG_INT);
  var rows  = [];
  if (!sheet || sheet.getLastRow() < 2) {
    etl_log('WARN','etl_leerFactDesdeSTG','STG_INTERACCIONES vacío — ¿se ejecutó Parte 1?');
    return rows;
  }
  // Cabecera: ID_INTERACCION(0) ID_CLIENTE(1) CELULAR(2) NOMBRE_CLIENTE(3)
  //           ASESOR_NOMBRE(4) ASESOR_EMAIL(5) FECHA_INTERACCION(6) FECHA_REGISTRO_LEAD(7)
  //           PROYECTO(8) FUENTE_ORIGINAL(9) FUENTE_NORMALIZADA(10) NOMBRE_OPC(11)
  //           TIPO_ACCION(12) TIPIFICACION(13) COMENTARIO(14) METADATA(15)
  var data = sheet.getRange(1, 1, sheet.getLastRow(), 16).getValues();
  for (var i = 1; i < data.length; i++) {
    var r = data[i];
    var cel = String(r[2] || '').trim();
    if (!cel) continue;
    rows.push({
      id_interaccion   : r[0],
      id_cliente       : r[1],
      celular          : cel,
      nombre_cliente   : String(r[3]  || '').trim(),
      asesor_nombre    : String(r[4]  || '').trim(),
      asesor_email     : String(r[5]  || '').trim(),
      fecha_interaccion: r[6],
      fecha_registro   : r[7],
      proyecto         : String(r[8]  || '').trim().toUpperCase(),
      fuente_original  : String(r[9]  || '').trim(),
      fuente_norm      : String(r[10] || '').trim(),
      nombre_opc       : String(r[11] || '').trim(),
      tipo_accion      : String(r[12] || '').trim().toUpperCase(),
      tipificacion     : String(r[13] || '').trim().toUpperCase(),
      comentario       : String(r[14] || '').trim(),
      metadata         : String(r[15] || '').trim()
    });
  }
  etl_log('INFO','etl_leerFactDesdeSTG','\u2705 ' + rows.length + ' interacciones leídas desde STG');
  return rows;
}

/**
 * Lee STG_PRESENCIAS y reconstruye el array presencias con la misma
 * estructura que devuelve fase_leerManifiestos().
 */
function etl_leerPresenciasDesdeSTG() {
  var ss    = getSpreadsheetDestino();
  var sheet = ss.getSheetByName(CFG.OUT.STG_PRES);
  var rows  = [];
  if (!sheet || sheet.getLastRow() < 2) {
    etl_log('WARN','etl_leerPresenciasDesdeSTG','STG_PRESENCIAS vacío — ¿se ejecutó Parte 1?');
    return rows;
  }
  // Cabecera: MES_MANIFIESTO(0) ANIO(1) MES_NUM(2) ID_MANIFIESTO(3)
  //           FECHA_EVENTO(4) HOJA_REFERENCIA(5) ASESOR_RAW(6) ASESOR_CANONICO(7)
  //           PROYECTO(8) LUGAR(9) CLIENTE_RAW(10) CELULAR(11)
  //           FUENTE_RAW(12) FUENTE_NORMALIZADA(13) NOMBRE_OPC(14)
  //           ESTADO_CONFIRMACION(15) SEGUIMIENTO(16) RESULTADO(17) COMENTARIO(18)
  //           LINER(19) CLOSER(20)
  //           ES_PRESENCIA(21) ES_TOUR(22) ES_NO_TOUR(23) ES_NO_SHOW(24)
  var numCols = Math.min(sheet.getLastColumn(), 25);
  var data = sheet.getRange(1, 1, sheet.getLastRow(), numCols).getValues();
  for (var i = 1; i < data.length; i++) {
    var r = data[i];
    var clienteRaw = String(r[10] || '').trim();
    if (!clienteRaw) continue;
    rows.push({
      mes_manifiesto : String(r[0]  || '').trim(),
      anio           : r[1],
      mes_num        : r[2],
      id_manifiesto  : String(r[3]  || '').trim(),
      fecha_evento   : r[4],
      hoja_referencia: String(r[5]  || '').trim(),
      asesor_raw     : String(r[6]  || '').trim(),
      asesor_canonico: String(r[7]  || '').trim(),
      proyecto       : String(r[8]  || '').trim().toUpperCase(),
      lugar          : String(r[9]  || '').trim(),
      cliente_raw    : clienteRaw,
      celular        : String(r[11] || '').trim(),
      fuente_raw     : String(r[12] || '').trim(),
      fuente_norm    : String(r[13] || '').trim(),
      nombre_opc     : String(r[14] || '').trim(),
      estado_conf    : String(r[15] || '').trim(),
      seguimiento    : String(r[16] || '').trim(),
      resultado      : String(r[17] || '').trim().toUpperCase(),
      comentario     : String(r[18] || '').trim(),
      liner          : String(r[19] || '').trim(),
      closer         : String(r[20] || '').trim(),
      es_presencia   : (r[21] === true || String(r[21]).toUpperCase() === 'TRUE'),
      es_tour        : (r[22] === true || String(r[22]).toUpperCase() === 'TRUE'),
      es_no_tour     : (r[23] === true || String(r[23]).toUpperCase() === 'TRUE'),
      es_no_show     : (numCols > 24 && (r[24] === true || String(r[24]).toUpperCase() === 'TRUE'))
    });
  }
  etl_log('INFO','etl_leerPresenciasDesdeSTG','\u2705 ' + rows.length + ' presencias leídas desde STG');
  return rows;
}

/**
 * Lee STG_VENTAS y reconstruye el array ventas con la misma
 * estructura que devuelve fase_leerVentas().
 */
function etl_leerVentasDesdeSTG() {
  var ss    = getSpreadsheetDestino();
  var sheet = ss.getSheetByName(CFG.OUT.STG_VTAS);
  var rows  = [];
  if (!sheet || sheet.getLastRow() < 2) {
    etl_log('WARN','etl_leerVentasDesdeSTG','STG_VENTAS vacío — ¿se ejecutó Parte 1?');
    return rows;
  }
  // Cabecera: N(0) CLIENTE(1) CELULAR(2) MODALIDAD(3) PROYECTO(4) LOTE(5) MZ(6) PRECIO_VENTA(7)
  //           ESTADO2(8) FECHA_COMPRA(9) FECHA_PROCESA(10) HOY(11) MEDIO_PAGO(12)
  //           TLMK_RAW(13) TLMK_CANONICO(14) PROMOTORA(15) LINER(16) CLOSER(17)
  //           COMISION(18) MES(19) ORIGEN_RAW(20) ORIGEN_NORM(21) ES_NEGOCIO(22) ES_PROCESABLE(23)
  var data = sheet.getRange(1, 1, sheet.getLastRow(), 24).getValues();
  for (var i = 1; i < data.length; i++) {
    var r = data[i];
    var cliente = String(r[1] || '').trim().toUpperCase();
    if (!cliente) continue;
    var promotora  = String(r[15] || '').trim();
    var hoyRaw     = r[11];
    var recaudoVal = 0;
    if (hoyRaw != null && hoyRaw !== '') {
      recaudoVal = parseFloat(String(hoyRaw).replace(/[^0-9.,]/g, '').replace(',', '.')) || 0;
    }
    rows.push({
      n             : r[0],
      cliente       : cliente,
      celular       : String(r[2]  || '').trim(),
      modalidad     : String(r[3]  || '').trim().toUpperCase(),
      proyecto      : String(r[4]  || '').trim().toUpperCase(),
      lote          : r[5],
      mz            : String(r[6]  || '').trim(),
      precio_venta  : r[7],
      estado2       : String(r[8]  || '').trim().toUpperCase(),
      fecha_compra  : r[9],
      fecha_procesa : r[10],
      hoy           : hoyRaw,
      recaudo_val   : recaudoVal,
      medio_pago    : String(r[12] || '').trim(),
      tlmk_raw      : String(r[13] || '').trim().toUpperCase(),
      tlmk_canonico : String(r[14] || '').trim(),
      promotora     : promotora,
      promotora_norm: norm_nombre(promotora),
      liner         : String(r[16] || '').trim(),
      closer        : String(r[17] || '').trim(),
      comision      : String(r[18] || '').trim(),
      mes           : String(r[19] || '').trim(),
      origen_raw    : String(r[20] || '').trim().toUpperCase(),
      origen_norm   : String(r[21] || '').trim(),
      es_negocio    : (r[22] === true || String(r[22]).toUpperCase() === 'TRUE'),
      es_procesable : (r[23] === true || String(r[23]).toUpperCase() === 'TRUE')
    });
  }
  etl_log('INFO','etl_leerVentasDesdeSTG','\u2705 ' + rows.length + ' ventas leídas desde STG');
  return rows;
}

// ==========================================================================
// FASE 11 — RPT_EMBUDO_REAL_KPIS (KPIs “reales” por fecha y fuente)
//  - Leads asignados y etapas (desde DATA_EMBUDO_FULL) por FECHA_ASIGNACION_LEAD
//  - Citas/Presencias/Tour (desde STG_PRESENCIAS) por FECHA_EVENTO
//  - Negocios/Procesables/Recaudo (desde STG_VENTAS) por FECHA_COMPRA
//
// Objetivo: una sola tabla conectable a Looker Studio con 1 control de fecha + 1 filtro de fuente.
// ==========================================================================

function fase_rptEmbudoRealKPIs() {
  etl_log('INFO','fase_rptEmbudoRealKPIs','Reporte obsoleto: no se genera en el ETL actual.');
  return;
  var ss = getSpreadsheetDestino();
  var outSheet = getOrCreateSheet(ss, CFG.OUT.RPT_REAL);

  function fuenteGrupo(v) {
    var x = String(v || '').toUpperCase().trim();
    if (x === 'META' || x === 'WSP-FORM' || x === 'WSP') return 'REDES';
    if (x === 'OPC') return 'OPC';
    return 'Otros';
  }

  function key(fechaStr, fuenteGrp) {
    return fechaStr + '\u00a7' + (fuenteGrp || 'Otros');
  }

  function ensureAgg(map, k) {
    if (!map[k]) {
      map[k] = {
        fecha: '',
        fecha_date: null,
        fuente_grupo: '',
        // Embudo (asignación)
        leads_asignados: 0,
        contactos_efectivos: 0,
        leads_potenciales: 0,
        // Manifiesto (evento)
        citas_manifiesto: 0,
        presencias: 0,
        tour: 0,
        // Ventas (compra)
        negocios: 0,
        procesables: 0,
        recaudo: 0
      };
    }
    return map[k];
  }

  // Agregaciones por fecha+fuente con control de duplicados
  var agg = {};
  var seen = {}; // "tipo§fecha§fuente§cel" → true (para distinct por celular)

  // 1) Embudo (DATA_EMBUDO_FULL) — asignados y etapas por FECHA_ASIGNACION_LEAD
  var shEmb = ss.getSheetByName(CFG.OUT.EMBUDO);
  if (shEmb && shEmb.getLastRow() > 1) {
    var dataEmb = shEmb.getRange(1, 1, shEmb.getLastRow(), shEmb.getLastColumn()).getValues();
    var h = dataEmb[0].map(function(x) { return String(x || '').trim(); });
    var idx = {};
    for (var i = 0; i < h.length; i++) idx[h[i]] = i;

    var iCel = idx['CELULAR'];
    var iOrd = idx['ORDEN_ETAPA'];
    var iFte = idx['FUENTE'];
    var iFAs = idx['FECHA_ASIGNACION_LEAD'];

    for (var r = 1; r < dataEmb.length; r++) {
      var row = dataEmb[r];
      var cel = String(row[iCel] || '').trim();
      if (!cel) continue;
      var fAs = String(row[iFAs] || '').trim(); // yyyy-MM-dd
      if (!fAs) continue; // solo leads con asignación conocida
      var ord = Number(row[iOrd] || 0);
      if (ord !== 1 && ord !== 2 && ord !== 3) continue; // KPIs solicitados
      var fg = fuenteGrupo(row[iFte]);
      var k = key(fAs, fg);
      var a = ensureAgg(agg, k);
      a.fecha = fAs;
      a.fecha_date = toDate(fAs);
      a.fuente_grupo = fg;

      var sk = ord + '\u00a7' + fAs + '\u00a7' + fg + '\u00a7' + cel;
      if (seen[sk]) continue;
      seen[sk] = true;

      if (ord === 1) a.leads_asignados++;
      else if (ord === 2) a.contactos_efectivos++;
      else if (ord === 3) a.leads_potenciales++;
    }
  }

  // 2) Presencias (STG_PRESENCIAS) — citas/presencias/tour por FECHA_EVENTO
  var shPres = ss.getSheetByName(CFG.OUT.STG_PRES);
  if (shPres && shPres.getLastRow() > 1) {
    var dataPres = shPres.getRange(1, 1, shPres.getLastRow(), shPres.getLastColumn()).getValues();
    var hp = dataPres[0].map(function(x) { return String(x || '').trim(); });
    var ip = {};
    for (var j = 0; j < hp.length; j++) ip[hp[j]] = j;

    var pCel = ip['CELULAR'];
    var pFec = ip['FECHA_EVENTO'];
    var pFte = ip['FUENTE_NORMALIZADA'];
    var pPre = ip['ES_PRESENCIA'];
    var pTour = ip['ES_TOUR'];

    for (var pr = 1; pr < dataPres.length; pr++) {
      var prow = dataPres[pr];
      var pcel = String(prow[pCel] || '').trim();
      if (!pcel) continue;
      var fd = formatFecha(toDate(prow[pFec]));
      if (!fd) continue;
      var pfg = fuenteGrupo(prow[pFte]);
      var pk = key(fd, pfg);
      var pa = ensureAgg(agg, pk);
      pa.fecha = fd;
      pa.fecha_date = toDate(fd);
      pa.fuente_grupo = pfg;

      // Citas (evento agendado): distinct por celular+fecha+fuente
      var skc = 'CITA\u00a7' + fd + '\u00a7' + pfg + '\u00a7' + pcel;
      if (!seen[skc]) { seen[skc] = true; pa.citas_manifiesto++; }

      if (Boolean(prow[pPre]) === true) {
        var skp = 'PRES\u00a7' + fd + '\u00a7' + pfg + '\u00a7' + pcel;
        if (!seen[skp]) { seen[skp] = true; pa.presencias++; }
      }
      if (Boolean(prow[pTour]) === true) {
        var skt = 'TOUR\u00a7' + fd + '\u00a7' + pfg + '\u00a7' + pcel;
        if (!seen[skt]) { seen[skt] = true; pa.tour++; }
      }
    }
  }

  // 3) Ventas (STG_VENTAS) — negocios/procesables/recaudo por FECHA_COMPRA
  var shV = ss.getSheetByName(CFG.OUT.STG_VTAS);
  if (shV && shV.getLastRow() > 1) {
    var dataV = shV.getRange(1, 1, shV.getLastRow(), shV.getLastColumn()).getValues();
    var hv = dataV[0].map(function(x) { return String(x || '').trim(); });
    var iv = {};
    for (var z = 0; z < hv.length; z++) iv[hv[z]] = z;

    var vCel = iv['CELULAR'];
    var vFec = iv['FECHA_COMPRA'];
    var vOri = iv['ORIGEN_NORM'];
    var vNeg = iv['ES_NEGOCIO'];
    var vPro = iv['ES_PROCESABLE'];
    var vHoy = iv['HOY'];

    for (var vr = 1; vr < dataV.length; vr++) {
      var vrow = dataV[vr];
      var vcel = String(vrow[vCel] || '').trim();
      var vf = formatFecha(toDate(vrow[vFec]));
      if (!vf) continue;
      var vfg = fuenteGrupo(vrow[vOri]);
      var vk = key(vf, vfg);
      var va = ensureAgg(agg, vk);
      va.fecha = vf;
      va.fecha_date = toDate(vf);
      va.fuente_grupo = vfg;

      // Recaudo (suma)
      var hoyNum = Number(vrow[vHoy] || 0);
      if (!isNaN(hoyNum)) va.recaudo += hoyNum;

      if (vcel) {
        if (Boolean(vrow[vNeg]) === true) {
          var skn = 'NEG\u00a7' + vf + '\u00a7' + vfg + '\u00a7' + vcel;
          if (!seen[skn]) { seen[skn] = true; va.negocios++; }
        }
        if (Boolean(vrow[vPro]) === true) {
          var skpr = 'PROC\u00a7' + vf + '\u00a7' + vfg + '\u00a7' + vcel;
          if (!seen[skpr]) { seen[skpr] = true; va.procesables++; }
        }
      }
    }
  }

  // Construir filas finales + tasas (sin blends; tasas dentro de la misma fila fecha+fuente)
  function div0(a, b) { return (!b || b === 0) ? 0 : (a / b); }

  var HEADERS = [
    'FECHA','FUENTE_GRUPO',
    'LEADS_ASIGNADOS','CONTACTOS_EFECTIVOS','PCT_CONTACTO_EFECTIVO',
    'LEADS_POTENCIALES','PCT_POTENCIAL',
    'CITAS_MANIFIESTO','PCT_CITAS_SOBRE_POTENCIALES_ASIG','PCT_CITAS_SOBRE_LEADS_ASIG',
    'PRESENCIAS','PCT_CONFIRMACION',
    'TOUR','PCT_TOUR',
    'NEGOCIOS','PCT_NEGOCIOS',
    'PROCESABLES','PCT_CIERRE_SOBRE_NEGOCIOS','PCT_CIERRE_SOBRE_LEADS_ASIG',
    'RECAUDO'
  ];

  var keys = Object.keys(agg);
  keys.sort(); // fecha asc, fuente asc

  var rowsOut = [HEADERS];
  for (var kk = 0; kk < keys.length; kk++) {
    var o = agg[keys[kk]];
    var pctContacto = div0(o.contactos_efectivos, o.leads_asignados);
    var pctPot      = div0(o.leads_potenciales, o.leads_asignados);
    var pctCitaPot  = div0(o.citas_manifiesto, o.leads_potenciales);
    var pctCitaLead = div0(o.citas_manifiesto, o.leads_asignados);
    var pctConf     = div0(o.presencias, o.citas_manifiesto); // PRESENCIAS / CITAS
    var pctTour     = div0(o.tour, o.presencias);
    var pctNeg      = div0(o.negocios, o.tour);               // NEGOCIOS / TOUR (tu definición)
    var pctCNeg     = div0(o.procesables, o.negocios);
    var pctCLead    = div0(o.procesables, o.leads_asignados);

    rowsOut.push([
      (o.fecha_date || toDate(o.fecha) || o.fecha), o.fuente_grupo,
      o.leads_asignados, o.contactos_efectivos, pctContacto,
      o.leads_potenciales, pctPot,
      o.citas_manifiesto, pctCitaPot, pctCitaLead,
      o.presencias, pctConf,
      o.tour, pctTour,
      o.negocios, pctNeg,
      o.procesables, pctCNeg, pctCLead,
      o.recaudo
    ]);
  }

  etl_writeTable(outSheet, rowsOut);
  etl_maybeAutoResizeColumns(outSheet, HEADERS.length, rowsOut.length - 1);

  // Importante: NO aplicar formatos con símbolos (%, "S/") en Sheets.
  // Motivo: al exportar/leer como CSV, Looker puede interpretar esos valores como TEXTO
  // ("0.00%", "S/ 5000.00") y marca "métrica no válida".
  // El formateo de % y moneda se debe hacer en Looker Studio.

  // Mantener solo un formato neutro para FECHA (ISO) para que Looker lo reconozca como Date.
  try {
    outSheet.getRange(2, 1, Math.max(rowsOut.length - 1, 1), 1).setNumberFormat('yyyy-mm-dd');
  } catch (e) {}

  // Forzar formato numérico neutro en todas las columnas numéricas
  // (evita que Sheets herede formato % o moneda de ejecuciones antiguas / edición manual).
  try {
    var nRowsFmt = Math.max(rowsOut.length - 1, 1);
    // Conteos: 3..4, 6, 8, 11, 13, 15, 17 (según HEADERS)
    var countCols = [3,4,6,8,11,13,15,17];
    for (var cc = 0; cc < countCols.length; cc++) {
      outSheet.getRange(2, countCols[cc], nRowsFmt, 1).setNumberFormat('0');
    }
    // Porcentajes: 5,7,9,10,12,14,16,18,19 (deben ser números 0..1, sin %)
    var pctColsNeutral = [5,7,9,10,12,14,16,18,19];
    for (var pc2 = 0; pc2 < pctColsNeutral.length; pc2++) {
      outSheet.getRange(2, pctColsNeutral[pc2], nRowsFmt, 1).setNumberFormat('0.0000');
    }
    // Recaudo: 20 (número puro, sin "S/")
    outSheet.getRange(2, 20, nRowsFmt, 1).setNumberFormat('0.00');
  } catch (e) {}

  etl_log('INFO','fase_rptEmbudoRealKPIs','✅ ' + (rowsOut.length - 1) + ' filas en ' + CFG.OUT.RPT_REAL);
}


// ==========================================================================
// FASE 12 — RPT_OPC_REAL_KPIS (KPIs reales por OPC y fecha)
//  - FECHA: una columna para poder filtrar en Looker con 1 control.
//    * Leads captados: FECHA_ASIGNACION_LEAD (embudo etapa 1)
//    * Citas/Presencias/Tour: FECHA_EVENTO (manifiesto)
//  - Dimensiones: NOMBRE_OPC y PROYECTO.
// ==========================================================================
function fase_rptOPCRealKPIs() {
  var ss = getSpreadsheetDestino();
  var outSheet = getOrCreateSheet(ss, CFG.OUT.RPT_OPC_REAL);

  function isTrue(v) {
    if (v === true) return true;
    var s = String(v || '').trim().toUpperCase();
    return s === 'TRUE' || s === '1' || s === 'SI';
  }

  function keyFechaOpcProy(fechaISO, opc, proy) {
    return (fechaISO || '') + '\u00a7' + (opc || '') + '\u00a7' + (proy || '');
  }

  var agg = {}; // key → {fecha, opc, proy, leads, citas, pres, tours}
  var seenLead = {}; // dedupe por cel+key (lead etapa 1)
  var seenCita = {};
  var seenPres = {};
  var seenTour = {};

  // 1) Leads captados (embudo etapa 1) por FECHA_ASIGNACION_LEAD
  var shEmb = ss.getSheetByName(CFG.OUT.EMBUDO);
  if (shEmb && shEmb.getLastRow() > 1) {
    var dataEmb = shEmb.getRange(1, 1, shEmb.getLastRow(), shEmb.getLastColumn()).getValues();
    var h = dataEmb[0].map(function(x) { return String(x || '').trim(); });
    var idx = {};
    for (var i = 0; i < h.length; i++) idx[h[i]] = i;

    var iCel = idx['CELULAR'];
    var iOrd = idx['ORDEN_ETAPA'];
    var iOpc = idx['NOMBRE_OPC'];
    var iPro = idx['PROYECTO'];
    var iFas = idx['FECHA_ASIGNACION_LEAD'];

    for (var r = 1; r < dataEmb.length; r++) {
      var row = dataEmb[r];
      var cel = String(row[iCel] || '').trim();
      if (!cel) continue;
      var ord = Number(row[iOrd] || 0);
      if (ord !== 1) continue; // etapa 1 => Leads

      var opc = String(row[iOpc] || '').trim();
      if (!opc) continue;
      var proy = String(row[iPro] || '').trim();
      var fasStr = String(row[iFas] || '').trim(); // yyyy-mm-dd (idealmente)
      var fd = fasStr ? formatFecha(toDate(fasStr)) : '';
      if (!fd) continue;

      var k = keyFechaOpcProy(fd, opc, proy);
      if (!agg[k]) agg[k] = { fecha: fd, nombre_opc: opc, proyecto: proy, leads: 0, citas: 0, pres: 0, tours: 0 };

      var sk = 'L\u00a7' + k + '\u00a7' + cel;
      if (seenLead[sk]) continue;
      seenLead[sk] = true;
      agg[k].leads++;
    }
  }

  // 2) Manifiesto (STG_PRESENCIAS) por FECHA_EVENTO
  var shPres = ss.getSheetByName(CFG.OUT.STG_PRES);
  if (shPres && shPres.getLastRow() > 1) {
    var dataPres = shPres.getRange(1, 1, shPres.getLastRow(), shPres.getLastColumn()).getValues();
    var hp = dataPres[0].map(function(x) { return String(x || '').trim(); });
    var ip = {};
    for (var j = 0; j < hp.length; j++) ip[hp[j]] = j;

    var pCel = ip['CELULAR'];
    var pFec = ip['FECHA_EVENTO'];
    var pOpc = ip['NOMBRE_OPC'];
    var pPro = ip['PROYECTO'];
    var pPre = ip['ES_PRESENCIA'];
    var pTour = ip['ES_TOUR'];

    for (var pr = 1; pr < dataPres.length; pr++) {
      var prow = dataPres[pr];
      var cel = String(prow[pCel] || '').trim();
      if (!cel) continue;
      var opc = String(prow[pOpc] || '').trim();
      if (!opc) continue;
      var proy = String(prow[pPro] || '').trim();

      var fd = formatFecha(toDate(prow[pFec]));
      if (!fd) continue;

      var k = keyFechaOpcProy(fd, opc, proy);
      if (!agg[k]) agg[k] = { fecha: fd, nombre_opc: opc, proyecto: proy, leads: 0, citas: 0, pres: 0, tours: 0 };

      // Citas agendadas del Manifiesto: distinción por celular+fecha+opc
      var skc = 'C\u00a7' + k + '\u00a7' + cel;
      if (!seenCita[skc]) { seenCita[skc] = true; agg[k].citas++; }

      if (isTrue(prow[pPre])) {
        var skp = 'P\u00a7' + k + '\u00a7' + cel;
        if (!seenPres[skp]) { seenPres[skp] = true; agg[k].pres++; }
      }

      if (isTrue(prow[pTour])) {
        var skt = 'T\u00a7' + k + '\u00a7' + cel;
        if (!seenTour[skt]) { seenTour[skt] = true; agg[k].tours++; }
      }
    }
  }

  // Salida
  var HEADERS = ['FECHA','NOMBRE_OPC','PROYECTO','LEADS_CAPTADOS','CITAS_MANIFIESTO','PRESENCIAS_TOTALES','TOURS_VALIDOS'];
  var keys = Object.keys(agg).sort();
  var rowsOut = [HEADERS];
  for (var kk = 0; kk < keys.length; kk++) {
    var o = agg[keys[kk]];
    rowsOut.push([o.fecha, o.nombre_opc, o.proyecto, o.leads, o.citas, o.pres, o.tours]);
  }

  outSheet.clear();
  expandSheet(outSheet, rowsOut.length + 2, HEADERS.length);
  outSheet.getRange(1, 1, rowsOut.length, HEADERS.length).setValues(rowsOut);
  outSheet.getRange(1, 1, 1, HEADERS.length)
    .setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
  outSheet.setFrozenRows(1);
  etl_maybeAutoResizeColumns(outSheet, HEADERS.length, rowsOut.length - 1);

  try {
    var nRows = Math.max(rowsOut.length - 1, 1);
    outSheet.getRange(2, 1, nRows, 1).setNumberFormat('yyyy-mm-dd');
    // conteos: 4..7
    var countCols = [4,5,6,7];
    for (var ci = 0; ci < countCols.length; ci++) {
      outSheet.getRange(2, countCols[ci], nRows, 1).setNumberFormat('0');
    }
  } catch (e) {}

  etl_log('INFO','fase_rptOPCRealKPIs','✅ ' + (rowsOut.length - 1) + ' filas en ' + CFG.OUT.RPT_OPC_REAL);
}


// ==========================================================================
// FASE 13 — RPT_OPC_COHORTE_MAR1_HOY
// Cohorte por OPC:
//  - Denominador: leads captados cuya FECHA_ASIGNACION_LEAD cae en un rango
//    (en Looker tú filtras 1 al 4 de marzo).
//  - Numeradores: para esos mismos leads, si existe algún registro en
//    STG_PRESENCIAS entre (FECHA_EVENTO >= 1-mar) y (<= hoy):
//      * CITAS: existe cualquier fila del manifiesto (independiente de presencia)
//      * PRESENCIA: ES_PRESENCIA = true
//      * TOUR: ES_TOUR = true
//
// Salida: una fila por FECHA_ASIGNACION_LEAD + OPC + PROYECTO con conteos
// pre-deduplicados (cada lead cuenta una vez).
// ==========================================================================
function fase_rptOPCCohorteMar1Hoy() {
  etl_log('INFO','fase_rptOPCCohorteMar1Hoy','Reporte obsoleto: no se genera en el ETL actual.');
  return;
  var ss = getSpreadsheetDestino();
  var outSheet = getOrCreateSheet(ss, CFG.OUT.RPT_OPC_COHORTE_MAR1_HOY);

  function isTrue(v) {
    if (v === true) return true;
    var s = String(v || '').trim().toUpperCase();
    return s === 'TRUE' || s === '1' || s === 'SI';
  }

  // Ventana de eventos: desde 1 de marzo hasta hoy (ambas inclusive)
  // (Ajusta el año si tu archivo usa otro año; aquí asumimos 2026 por el contexto del proyecto)
  var today = new Date();
  today.setHours(0,0,0,0);
  var startEvt = new Date(2026, 2, 1); // 2 = marzo
  startEvt.setHours(0,0,0,0);

  // 1) Leads captados por FECHA_ASIGNACION_LEAD (etapa 1) para dedupe por celular
  var leads = {}; // celular → {fecha_asig, nombre_opc, proyecto}

  var shEmb = ss.getSheetByName(CFG.OUT.EMBUDO);
  if (shEmb && shEmb.getLastRow() > 1) {
    var dataEmb = shEmb.getRange(1, 1, shEmb.getLastRow(), shEmb.getLastColumn()).getValues();
    var h = dataEmb[0].map(function(x) { return String(x || '').trim(); });
    var idx = {};
    for (var i = 0; i < h.length; i++) idx[h[i]] = i;

    var iCel = idx['CELULAR'];
    var iOrd = idx['ORDEN_ETAPA'];
    var iOpc = idx['NOMBRE_OPC'];
    var iPro = idx['PROYECTO'];
    var iFAs = idx['FECHA_ASIGNACION_LEAD'];

    for (var r = 1; r < dataEmb.length; r++) {
      var row = dataEmb[r];
      var cel = String(row[iCel] || '').trim();
      if (!cel) continue;
      var ord = Number(row[iOrd] || 0);
      if (ord !== 1) continue; // solo leads captados (etapa 1)
      var fAsStr = String(row[iFAs] || '').trim(); // yyyy-mm-dd idealmente
      if (!fAsStr) continue;

      var fdAs = formatFecha(toDate(fAsStr));
      if (!fdAs) continue;

      var opc = String(row[iOpc] || '').trim();
      var proy = String(row[iPro] || '').trim();
      var k = cel; // dedupe por celular
      if (!leads[k]) {
        leads[k] = { fecha: fdAs, opc: opc, proy: proy };
      }
    }
  }

  // 2) Marcar flags de eventos por celular (desde 1-mar hasta hoy)
  var flags = {}; // celular → {cita, presencia, tour}

  var shPres = ss.getSheetByName(CFG.OUT.STG_PRES);
  if (shPres && shPres.getLastRow() > 1) {
    var dataPres = shPres.getRange(1, 1, shPres.getLastRow(), shPres.getLastColumn()).getValues();
    var hp = dataPres[0].map(function(x) { return String(x || '').trim(); });
    var ip = {};
    for (var j = 0; j < hp.length; j++) ip[hp[j]] = j;

    var pCel = ip['CELULAR'];
    var pFec = ip['FECHA_EVENTO'];
    var pPre = ip['ES_PRESENCIA'];
    var pTour = ip['ES_TOUR'];

    for (var pr = 1; pr < dataPres.length; pr++) {
      var prow = dataPres[pr];
      var cel = String(prow[pCel] || '').trim();
      if (!cel) continue;
      if (!leads[cel]) continue; // solo leads del embudo

      var fEvt = toDate(prow[pFec]);
      if (!fEvt || fEvt.getTime() === 0) continue;
      fEvt.setHours(0,0,0,0);
      if (fEvt < startEvt || fEvt > today) continue;

      if (!flags[cel]) flags[cel] = { cita: false, presencia: false, tour: false };

      // Cita: cualquier registro en Manifiesto dentro del rango (aunque no haya presencia)
      flags[cel].cita = true;
      if (isTrue(prow[pPre])) flags[cel].presencia = true;
      if (isTrue(prow[pTour])) flags[cel].tour = true;
    }
  }

  // 3) Agregar por fecha_asig + opc + proyecto
  var agg = {}; // key → counts
  for (var celK in leads) {
    var ld = leads[celK];
    var key = ld.fecha + '\u00a7' + (ld.opc || '') + '\u00a7' + (ld.proy || '');
    if (!agg[key]) agg[key] = { fecha: ld.fecha, opc: ld.opc, proy: ld.proy, leads: 0, cita: 0, presencia: 0, tour: 0 };
    agg[key].leads++;

    var fl = flags[ld.cel];
    if (fl) {
      if (fl.cita) agg[key].cita++;
      if (fl.presencia) agg[key].presencia++;
      if (fl.tour) agg[key].tour++;
    }
  }

  var HEADERS = ['FECHA_ASIGNACION','NOMBRE_OPC','PROYECTO','LEADS_CAPTADOS','LEADS_CON_CITA','LEADS_CON_PRESENCIA','LEADS_CON_TOUR'];
  var keys = Object.keys(agg).sort();
  var rowsOut = [HEADERS];
  for (var kk = 0; kk < keys.length; kk++) {
    var o = agg[keys[kk]];
    rowsOut.push([o.fecha, o.opc, o.proy, o.leads, o.cita, o.presencia, o.tour]);
  }

  outSheet.clear();
  expandSheet(outSheet, rowsOut.length + 2, HEADERS.length);
  outSheet.getRange(1, 1, rowsOut.length, HEADERS.length).setValues(rowsOut);
  outSheet.getRange(1, 1, 1, HEADERS.length)
    .setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
  outSheet.setFrozenRows(1);
  etl_maybeAutoResizeColumns(outSheet, HEADERS.length, rowsOut.length - 1);

  try {
    var nRows = Math.max(rowsOut.length - 1, 1);
    outSheet.getRange(2, 1, nRows, 1).setNumberFormat('yyyy-mm-dd');
    // conteos: cols 4..7
    var countCols = [4,5,6,7];
    for (var cc = 0; cc < countCols.length; cc++) {
      outSheet.getRange(2, countCols[cc], nRows, 1).setNumberFormat('0');
    }
  } catch (e) {}

  etl_log('INFO','fase_rptOPCCohorteMar1Hoy','✅ ' + (rowsOut.length - 1) + ' filas en ' + CFG.OUT.RPT_OPC_COHORTE_MAR1_HOY);
}


// ==========================================================================
// FASE 14 — RPT_OPC_COHORTE_MAR1_HOY_CAPTACION
// Igual que FASE 13, pero el denominador (leads captados) usa:
//   FECHA_ENTRADA_LEAD (captación) en vez de FECHA_ASIGNACION_LEAD.
// ==========================================================================
function fase_rptOPCCohorteMar1HoyCaptacion() {
  etl_log('INFO','fase_rptOPCCohorteMar1HoyCaptacion','Reporte obsoleto: no se genera en el ETL actual.');
  return;
  var ss = getSpreadsheetDestino();
  var outSheet = getOrCreateSheet(ss, CFG.OUT.RPT_OPC_COHORTE_MAR1_HOY_CAPTACION);

  function isTrue(v) {
    if (v === true) return true;
    var s = String(v || '').trim().toUpperCase();
    return s === 'TRUE' || s === '1' || s === 'SI';
  }

  // Ventana de eventos: desde la FECHA_CAPTACION de cada lead hasta hoy (inclusive)
  var today = new Date();
  today.setHours(0,0,0,0);

  // Denominador: leads captados por ASIGNACION_MANUAL con FECHA_REGISTRO_LEAD
  // (sin limitar a un rango fijo). Cada lead se cuenta una vez por (celular§opc§proy).
  var leads = {};              // key = celular§opc§proy → { fecha: 'yyyy-mm-dd', fecha_date: Date, opc, proy, cel }
  var leadsKeysByCel = {};    // cel → [key1, key2...]

  var shInt = ss.getSheetByName(CFG.OUT.STG_INT);
  if (shInt && shInt.getLastRow() > 1) {
    var dataInt = shInt.getRange(1, 1, shInt.getLastRow(), shInt.getLastColumn()).getValues();
    var hInt = dataInt[0].map(function(x) { return String(x || '').trim(); });
    var idxInt = {};
    for (var ii = 0; ii < hInt.length; ii++) idxInt[hInt[ii]] = ii;

    var iCelI = idxInt['CELULAR'];
    var iOpcI = idxInt['NOMBRE_OPC'];
    var iProI = idxInt['PROYECTO'];
    var iFReg = idxInt['FECHA_REGISTRO_LEAD'];
    var iTipo = idxInt['TIPO_ACCION'];

    if (iCelI !== undefined && iOpcI !== undefined && iFReg !== undefined && iTipo !== undefined) {
      for (var rInt = 1; rInt < dataInt.length; rInt++) {
        var rowI = dataInt[rInt];
        var celI = String(rowI[iCelI] || '').trim();
        if (!celI || celI.length < 9) continue;

        var tipoI = String(rowI[iTipo] || '').trim().toUpperCase();
        if (tipoI !== 'ASIGNACION_MANUAL') continue;

        var opcI = String(rowI[iOpcI] || '').trim();
        if (!opcI) continue;

        var proyI = (iProI !== undefined) ? String(rowI[iProI] || '').trim() : '';

        var fReg = toDate(rowI[iFReg]);
        if (!fReg || fReg.getTime() === 0) continue;
        fReg.setHours(0,0,0,0);

        var fdCap = formatFecha(fReg);
        if (!fdCap) continue;

        var key = celI + '\u00a7' + opcI + '\u00a7' + proyI;
        if (!leads[key] || fdCap < leads[key].fecha) {
          leads[key] = { fecha: fdCap, fecha_date: fReg, opc: opcI, proy: proyI, cel: celI };
          // Asegurar mapping cel → keys
          if (!leadsKeysByCel[celI]) leadsKeysByCel[celI] = [];
        }
        // Evitar duplicados en la lista por si el mismo key aparece múltiples veces
        if (leadsKeysByCel[celI] && leadsKeysByCel[celI].indexOf(key) === -1) {
          leadsKeysByCel[celI].push(key);
        }
      }
    }
  }

  // 1.5) DATOS FALSOS (DF) para los mismos leads captados (mismo conjunto de celulares)
  var leadCellsSet = {};
  for (var celLeadK in leads) {
    var ld0 = leads[celLeadK];
    if (ld0 && ld0.cel) leadCellsSet[ld0.cel] = true;
  }

  var dfByCell = {}; // celular → true si existe al menos una tipificación DF
  try {
    if (shInt && shInt.getLastRow() > 1) {
      var dataIntDF = shInt.getRange(1, 1, shInt.getLastRow(), shInt.getLastColumn()).getValues();
      var hDF = dataIntDF[0].map(function(x) { return String(x || '').trim(); });
      var idxDF = {};
      for (var idf = 0; idf < hDF.length; idf++) idxDF[hDF[idf]] = idf;
      var iCelDF = idxDF['CELULAR'];
      var iTipDF = idxDF['TIPIFICACION'];

      if (iCelDF !== undefined && iTipDF !== undefined) {
        for (var rDF = 1; rDF < dataIntDF.length; rDF++) {
          var rowDF = dataIntDF[rDF];
          var celDF = String(rowDF[iCelDF] || '').trim();
          if (!celDF) continue;
          if (!leadCellsSet[celDF]) continue;
          var tipDF = String(rowDF[iTipDF] || '').trim().toUpperCase();
          if (tipDF === 'DF') dfByCell[celDF] = true;
        }
      }
    }
  } catch (e) {}

  // 2) Flags de eventos por lead-key:
  //    - Una lead-key entra si su FECHA_CAPTACION (fecha_date) <= FECHA_EVENTO <= hoy
  //    - CITAS: si existe cualquier evento (aunque no sea presencia)
  //    - PRESENCIA: ES_PRESENCIA = true
  //    - TOUR: ES_TOUR = true
  var flags = {}; // key → {cita, presencia, tour}

  var shPres = ss.getSheetByName(CFG.OUT.STG_PRES);
  if (shPres && shPres.getLastRow() > 1) {
    var dataPres = shPres.getRange(1, 1, shPres.getLastRow(), shPres.getLastColumn()).getValues();
    var hp = dataPres[0].map(function(x) { return String(x || '').trim(); });
    var ip = {};
    for (var j = 0; j < hp.length; j++) ip[hp[j]] = j;

    var pCel = ip['CELULAR'];
    var pFec = ip['FECHA_EVENTO'];
    var pPre = ip['ES_PRESENCIA'];
    var pTour = ip['ES_TOUR'];

    for (var pr = 1; pr < dataPres.length; pr++) {
      var prow = dataPres[pr];
      var cel = String(prow[pCel] || '').trim();
      if (!cel) continue;
      // solo celulares que tienen lead-key (captados por ASIGNACION_MANUAL)
      if (!leadsKeysByCel[cel]) continue;

      var fEvt = toDate(prow[pFec]);
      if (!fEvt || fEvt.getTime() === 0) continue;
      fEvt.setHours(0,0,0,0);
      if (fEvt > today) continue;

      var keysForCel = leadsKeysByCel[cel];
      for (var kx = 0; kx < keysForCel.length; kx++) {
        var keyLead = keysForCel[kx];
        var ld = leads[keyLead];
        if (!ld || !ld.fecha_date) continue;
        if (fEvt < ld.fecha_date) continue; // evento antes de la captación de esa lead-key

        if (!flags[keyLead]) flags[keyLead] = { cita: false, presencia: false, tour: false };

        // Cita: cualquier evento posterior a la captación
        flags[keyLead].cita = true;
        if (isTrue(prow[pPre])) flags[keyLead].presencia = true;
        if (isTrue(prow[pTour])) flags[keyLead].tour = true;
      }
    }
  }

  // 3) Agregar por FECHA_CAPTACION + OPC + PROYECTO
  var agg = {}; // key → counts
  for (var celK in leads) {
    var ld = leads[celK];
    var key = ld.fecha + '\u00a7' + (ld.opc || '') + '\u00a7' + (ld.proy || '');
    if (!agg[key]) agg[key] = { fecha: ld.fecha, opc: ld.opc, proy: ld.proy, leads: 0, cita: 0, presencia: 0, tour: 0, df: 0 };
    agg[key].leads++;

    var fl = flags[celK];
    if (fl) {
      if (fl.cita) agg[key].cita++;
      if (fl.presencia) agg[key].presencia++;
      if (fl.tour) agg[key].tour++;
    }

    // DF: si el celular del lead tiene tipificación DF
    if (dfByCell[ld.cel]) agg[key].df++;
  }

  var HEADERS = ['FECHA_CAPTACION','NOMBRE_OPC','PROYECTO','LEADS_CAPTADOS','LEADS_CON_CITA','LEADS_CON_PRESENCIA','LEADS_CON_TOUR','DATOS_FALSOS'];
  var keys = Object.keys(agg).sort();
  var rowsOut = [HEADERS];
  for (var kk = 0; kk < keys.length; kk++) {
    var o = agg[keys[kk]];
    rowsOut.push([o.fecha, o.opc, o.proy, o.leads, o.cita, o.presencia, o.tour, o.df]);
  }

  outSheet.clear();
  expandSheet(outSheet, rowsOut.length + 2, HEADERS.length);
  outSheet.getRange(1, 1, rowsOut.length, HEADERS.length).setValues(rowsOut);
  outSheet.getRange(1, 1, 1, HEADERS.length)
    .setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
  outSheet.setFrozenRows(1);
  etl_maybeAutoResizeColumns(outSheet, HEADERS.length, rowsOut.length - 1);

  try {
    var nRows = Math.max(rowsOut.length - 1, 1);
    outSheet.getRange(2, 1, nRows, 1).setNumberFormat('yyyy-mm-dd');
    // conteos: cols 4..8 (incluye DATOS_FALSOS)
    var countCols = [4,5,6,7,8];
    for (var cc = 0; cc < countCols.length; cc++) {
      outSheet.getRange(2, countCols[cc], nRows, 1).setNumberFormat('0');
    }
  } catch (e) {}

  etl_log('INFO','fase_rptOPCCohorteMar1HoyCaptacion','✅ ' + (rowsOut.length - 1) + ' filas en ' + CFG.OUT.RPT_OPC_COHORTE_MAR1_HOY_CAPTACION);
}


/**
 * Diagnóstico del embudo: identifica por qué DATA_EMBUDO_FULL tiene pocas filas.
 * Ejecutar desde el menú para ver conteos en cada etapa del pipeline.
 * Resultados en LOG_ETL y en hoja DIAGNOSTICO_EMBUDO.
 */
function runDiagnosticoEmbudo() {
  var t0 = Date.now();
  var ss = getSpreadsheetDestino();
  var log = [];

  try {
    /* ── 1. Leer FACT_INTERACCIONES crudo ───────────────────────────────── */
    var ssCrm = SpreadsheetApp.openById(CFG.CRM_ID);
    var factSh = ssCrm.getSheetByName(CFG.CRM_FACT);
    var factRaw = 0;
    var factConCelularValido = 0;
    var factRows = [];

    if (factSh && factSh.getLastRow() > 1) {
      var raw = factSh.getRange(1, 1, factSh.getLastRow(), 16).getValues();
      factRaw = raw.length - 1;
      for (var i = 1; i < raw.length; i++) {
        var r = raw[i];
        var cel = norm_tel(r[2]);
        if (cel) {
          factConCelularValido++;
          factRows.push({
            celular: cel,
            nombre_cliente: String(r[3] || '').trim(),
            tipificacion: String(r[13] || '').trim().toUpperCase()
          });
        }
      }
    }
    log.push('1. FACT_INTERACCIONES: ' + factRaw + ' filas raw | ' + factConCelularValido + ' con celular válido (9 dígitos)');

    /* ── 2. Leads únicos por celular ─────────────────────────────────────── */
    var celsUnicos = {};
    for (var j = 0; j < factRows.length; j++) celsUnicos[factRows[j].celular] = true;
    var nLeadsCrm = Object.keys(celsUnicos).length;
    log.push('2. Leads únicos (CRM): ' + nLeadsCrm + ' celulares');

    /* ── 3. Presencias ───────────────────────────────────────────────────── */
    var presencias = fase_leerManifiestos();
    var presConCel = 0;
    for (var p = 0; p < presencias.length; p++) {
      if (presencias[p].celular && presencias[p].celular.length >= 9) presConCel++;
    }
    log.push('3. Presencias: ' + presencias.length + ' registros | ' + presConCel + ' con celular válido');

    /* ── 4. Ventas ───────────────────────────────────────────────────────── */
    var ventas = fase_leerVentas();
    var ventasNegocio = 0, ventasProcesable = 0, ventasConCel = 0;
    for (var v = 0; v < ventas.length; v++) {
      if (ventas[v].celular) ventasConCel++;
      if (ventas[v].es_negocio) ventasNegocio++;
      if (ventas[v].es_procesable) ventasProcesable++;
    }
    // Segmentar por año y detectar 2026 sin celular
    var ventas2025 = 0, ventas2026 = 0, ventas2026SinCel = 0;
    var pendCelular = [];
    for (var vd = 0; vd < ventas.length; vd++) {
      var fcd = toDate(ventas[vd].fecha_compra);
      var anioVd = fcd && fcd.getFullYear ? fcd.getFullYear() : 0;
      if (anioVd >= 2026) { ventas2026++; if (!ventas[vd].celular) { ventas2026SinCel++; pendCelular.push(ventas[vd].cliente + ' (' + ventas[vd].tlmk_raw + ' | ' + ventas[vd].proyecto + ' | ' + (ventas[vd].fecha_compra || '') + ')'); } }
      else ventas2025++;
    }
    log.push('4. Ventas: ' + ventas.length + ' registros | ' + ventasConCel + ' con celular | ' + ventasNegocio + ' Negocios (SEPARACION) | ' + ventasProcesable + ' Procesables (PROCESADO)');
    log.push('   Desglose: ' + ventas2025 + ' en 2025 (sin Manifiesto) | ' + ventas2026 + ' en 2026 | ' + ventas2026SinCel + ' de 2026 sin celular');
    if (pendCelular.length > 0 && pendCelular.length <= 20) {
      log.push('   2026 sin celular: ' + pendCelular.join(' | '));
    }
    if (ventas.length > 0) {
      // Muestra los valores reales leídos en las primeras filas para diagnóstico de columnas
      var muestras = Math.min(3, ventas.length);
      for (var vm = 0; vm < muestras; vm++) {
        log.push('  [Fila ' + (vm+1) + '] cliente="' + ventas[vm].cliente + '" celular="' + ventas[vm].celular + '" modalidad="' + ventas[vm].modalidad + '" estado2="' + ventas[vm].estado2 + '"');
      }
    }

    /* ── 5. Simular leadsMap (solo CRM + Presencias) ─────────────────────── */
    var leadsMap = {};
    for (var k = 0; k < factRows.length; k++) {
      var f = factRows[k];
      if (!leadsMap[f.celular]) {
        leadsMap[f.celular] = crearLead(f.celular, f.nombre_cliente, '', '', '', '', new Date());
      }
    }
    for (var pp = 0; pp < presencias.length; pp++) {
      var pres = presencias[pp];
      if (!pres.celular || pres.celular.length < 9) continue;
      if (!pres.es_presencia && !pres.es_tour) continue;
      if (!leadsMap[pres.celular]) {
        leadsMap[pres.celular] = crearLead(pres.celular, pres.cliente_raw, '', '', '', '', new Date());
      }
    }
    var nLeadsTotal = Object.keys(leadsMap).length;
    log.push('5. Leads totales (CRM + Manifiesto): ' + nLeadsTotal);

    /* ── 6. Matching Ventas (nombre en leadsMap + puente Manifiesto) ─────── */
    var nombreIdx = {};
    for (var cel2 in leadsMap) {
      var nn = norm_nombre(leadsMap[cel2].nombre);
      if (nn.length > 3 && !nombreIdx[nn]) nombreIdx[nn] = cel2;
    }
    var manifestIdxDiag = {};
    for (var pd = 0; pd < presencias.length; pd++) {
      var presD = presencias[pd];
      if (!presD.celular || presD.celular.length < 9) continue;
      var nnD = norm_nombre(presD.cliente_raw);
      if (nnD.length > 3) manifestIdxDiag[nnD] = true;
    }
    var matchesVentas = 0;
    for (var vv = 0; vv < ventas.length; vv++) {
      var nNorm = norm_nombre(ventas[vv].cliente);
      if (ventas[vv].celular || nombreIdx[nNorm] || manifestIdxDiag[nNorm]) matchesVentas++;
    }
    log.push('6. Match Ventas (CELULAR + CRM + Manifiesto puente): ' + matchesVentas + ' de ' + ventas.length + ' ventas');

    /* ── 7. Destino de escritura ─────────────────────────────────────────── */
    var dest = getSpreadsheetDestino();
    log.push('7. Destino escritura: ' + dest.getName() + ' (ID: ' + dest.getId() + ')');

    /* ── Escribir en LOG y hoja ─────────────────────────────────────────── */
    for (var l = 0; l < log.length; l++) etl_log('INFO', 'DiagnosticoEmbudo', log[l]);

    var diagSheet = getOrCreateSheet(ss, 'DIAGNOSTICO_EMBUDO');
    diagSheet.clear();
    diagSheet.appendRow(['DIAGNÓSTICO EMBUDO - ' + new Date().toLocaleString()]);
    diagSheet.getRange(1, 1, 1, 1).setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
    for (var ll = 0; ll < log.length; ll++) diagSheet.appendRow([log[ll]]);
    diagSheet.appendRow(['']);
    diagSheet.appendRow(['CONCLUSIÓN: Si "con celular válido" es bajo → revisa norm_tel y columna CELULAR (col 3) en FACT. Si "Match Ventas" es bajo → revisa CELULAR en Ventas; luego asesor/proyecto/fecha/nombre OPC para fallback por nombre.']);
    diagSheet.autoResizeColumns(1, 1);

    SpreadsheetApp.getUi().alert(
      '📋 Diagnóstico completado',
      log.join('\n') + '\n\nRevisa la hoja DIAGNOSTICO_EMBUDO y LOG_ETL para más detalle.',
      SpreadsheetApp.getUi().ButtonSet.OK
    );

  } catch (e) {
    etl_log('ERROR', 'DiagnosticoEmbudo', e.message);
    SpreadsheetApp.getUi().alert('Error en diagnóstico: ' + e.message);
  }
}


// ==========================================================================
// FASE 1 — LEER DATOS DEL CRM
// ==========================================================================

function fase_leerCRM() {
  try {
    var ss = SpreadsheetApp.openById(CFG.CRM_ID);

    /* ── FACT_INTERACCIONES ─────────────────────────────────────────────── */
    var factSh = ss.getSheetByName(CFG.CRM_FACT);
    var fact   = [];
    if (factSh && factSh.getLastRow() > 1) {
      var raw = factSh.getRange(1, 1, factSh.getLastRow(), 16).getValues();
      for (var i = 1; i < raw.length; i++) {
        var r = raw[i];
        var cel = norm_tel(r[2]);
        if (!cel) continue;
        fact.push({
          id_interaccion   : r[0],
          id_cliente       : r[1],
          celular          : cel,
          nombre_cliente   : String(r[3]  || '').trim(),
          asesor_nombre    : String(r[4]  || '').trim(),
          asesor_email     : String(r[5]  || '').trim(),
          fecha_interaccion: r[6],
          fecha_registro   : r[7],
          proyecto         : String(r[8]  || '').trim().toUpperCase(),
          fuente_original  : String(r[9]  || '').trim(),
          fuente_norm      : String(r[10] || '').trim(),
          nombre_opc       : String(r[11] || '').trim(),
          tipo_accion      : String(r[12] || '').trim().toUpperCase(),
          tipificacion     : String(r[13] || '').trim().toUpperCase(),
          comentario       : String(r[14] || '').trim(),
          metadata         : String(r[15] || '').trim()
        });
      }
    }

    /* ── DIM_CLIENTES ───────────────────────────────────────────────────── */
    var dimSh = ss.getSheetByName(CFG.CRM_DIM);
    var dim   = [];
    if (dimSh && dimSh.getLastRow() > 1) {
      var rawD = dimSh.getRange(1, 1, dimSh.getLastRow(), 16).getValues();
      for (var d = 1; d < rawD.length; d++) {
        var rd = rawD[d];
        var celD = norm_tel(rd[1]);
        if (!celD) continue;
        dim.push({
          id_cliente      : rd[0],
          celular         : celD,
          nombre          : String(rd[2]  || '').trim(),
          email           : String(rd[3]  || '').trim(),
          origen          : String(rd[4]  || '').trim(),
          proyecto        : String(rd[5]  || '').trim().toUpperCase(),
          fecha_registro  : rd[6],
          asesor_actual   : String(rd[7]  || '').trim(),
          ultima_tipif    : String(rd[8]  || '').trim().toUpperCase(),
          fecha_ult_gest  : rd[9],
          fuente_norm     : String(rd[10] || '').trim(),
          nombre_opc      : String(rd[11] || '').trim(),
          estado_civil    : String(rd[12] || '').trim(),
          ocupacion       : String(rd[13] || '').trim(),
          tiene_pareja    : String(rd[14] || '').trim(),
          distrito        : String(rd[15] || '').trim()
        });
      }
    }

    return { fact: fact, dim: dim };
  } catch (e) {
    etl_log('ERROR','fase_leerCRM','Error: ' + e.message);
    return { fact: [], dim: [] };
  }
}


// ==========================================================================
// FASE 2 — STG_INTERACCIONES (carga incremental por ID_INTERACCION)
// ==========================================================================

function fase_stgInteracciones(factRows) {
  var ss    = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.STG_INT);

  var HEADERS = [
    'ID_INTERACCION','ID_CLIENTE','CELULAR','NOMBRE_CLIENTE',
    'ASESOR_NOMBRE','ASESOR_EMAIL','FECHA_INTERACCION','FECHA_REGISTRO_LEAD',
    'PROYECTO','FUENTE_ORIGINAL','FUENTE_NORMALIZADA','NOMBRE_OPC',
    'TIPO_ACCION','TIPIFICACION','COMENTARIO','METADATA'
  ];

  // Inicializar encabezados si la hoja está vacía
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(HEADERS);
    sheet.getRange(1, 1, 1, HEADERS.length)
      .setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }

  // Construir Set de IDs ya presentes (para no duplicar)
  var existingIds = new Set();
  if (sheet.getLastRow() > 1) {
    var existData = sheet.getRange(2, 1, sheet.getLastRow() - 1, 1).getValues();
    for (var e = 0; e < existData.length; e++) {
      existingIds.add(String(existData[e][0]));
    }
  }

  // Filtrar solo los nuevos
  var newRows = [];
  for (var i = 0; i < factRows.length; i++) {
    var f = factRows[i];
    if (existingIds.has(String(f.id_interaccion))) continue;
    newRows.push([
      f.id_interaccion, f.id_cliente, f.celular, f.nombre_cliente,
      f.asesor_nombre, f.asesor_email, f.fecha_interaccion, f.fecha_registro,
      f.proyecto, f.fuente_original, f.fuente_norm, f.nombre_opc,
      f.tipo_accion, f.tipificacion, f.comentario, f.metadata
    ]);
  }

  if (newRows.length > 0) {
    var startRow = sheet.getLastRow() + 1;
    expandSheet(sheet, startRow + newRows.length, HEADERS.length);
    sheet.getRange(startRow, 1, newRows.length, HEADERS.length).setValues(newRows);
    etl_log('INFO','fase_stgInteracciones',
      '➕ ' + newRows.length + ' nuevas interacciones agregadas (total base: ' + (existingIds.size + newRows.length) + ')');
  } else {
    etl_log('INFO','fase_stgInteracciones','✅ STG_INTERACCIONES al día (sin registros nuevos)');
  }
}


// ==========================================================================
// FASE 3 — LEER MANIFIESTOS (vía CONFIG_FUENTES)
// ==========================================================================

function fase_leerManifiestos() {
  var ss         = getSpreadsheetDestino();
  var configSh   = ss.getSheetByName(CFG.OUT.CONFIG);
  var presencias = [];

  if (!configSh || configSh.getLastRow() < 2) {
    etl_log('WARN','fase_leerManifiestos','CONFIG_FUENTES vacía. Agrega los IDs de Manifiestos.');
    return presencias;
  }

  var cfg = configSh.getRange(2, 1, configSh.getLastRow() - 1, 7).getValues();

  for (var c = 0; c < cfg.length; c++) {
    var tipo    = String(cfg[c][0] || '').trim().toUpperCase();
    var anio    = cfg[c][1];
    var mesNum  = cfg[c][2];
    var nomMes  = String(cfg[c][3] || '').trim();
    var ssId    = String(cfg[c][4] || '').trim();
    var hojaNm  = String(cfg[c][5] || '').trim() || CFG.MANIF_HOJA;
    var activo  = cfg[c][6];

    if (tipo !== 'MANIFIESTO' || !activo || !ssId || ssId.indexOf('PEGAR') !== -1) continue;

    try {
      var ssManif = SpreadsheetApp.openById(ssId);
      var hoja    = ssManif.getSheetByName(hojaNm);

      if (!hoja || hoja.getLastRow() < 2) {
        etl_log('WARN','fase_leerManifiestos','Hoja no encontrada: "' + hojaNm + '" en manifiesto ' + nomMes);
        continue;
      }

      // La hoja Consolidado Looker tiene 24 columnas (ver estructura del ETL_ConsolidarParaLookerStudio)
      var numCols = Math.min(hoja.getLastColumn(), 24);
      var raw = hoja.getRange(1, 1, hoja.getLastRow(), numCols).getValues();

      for (var r = 1; r < raw.length; r++) {
        var row = raw[r];
        var clienteRaw = String(row[5] || '').trim();
        if (!clienteRaw) continue;

        var celularLimpio = norm_tel(row[6]);
        var fuenteRaw     = String(row[10] || '').trim();
        var fi            = parsear_fuente(fuenteRaw);
        var asesorRaw     = String(row[2] || '').trim().toUpperCase();
        var asesorCan     = mapear_asesor(asesorRaw);
        var seguimiento   = String(row[13] || '').trim();
        var resultado     = String(row[16] || '').trim().toUpperCase();

        var esPresencia = false;
        for (var si = 0; si < CFG.SEGUIMIENTO_ASISTIO.length; si++) {
          if (seguimiento.toUpperCase() === CFG.SEGUIMIENTO_ASISTIO[si]) { esPresencia = true; break; }
        }
        var esTour    = resultado === CFG.RESULTADO_TOUR;
        var esNoTour  = CFG.RESULTADO_NO_TOUR.indexOf(resultado) !== -1;
        var esNoShow  = resultado === CFG.RESULTADO_NO_SHOW;

        presencias.push({
          // Metadatos del manifiesto
          mes_manifiesto   : nomMes,
          anio             : anio,
          mes_num          : mesNum,
          id_manifiesto    : ssId,
          // Datos del registro
          fecha_evento     : row[0],
          hoja_referencia  : String(row[1] || '').trim(),
          asesor_raw       : asesorRaw,
          asesor_canonico  : asesorCan,
          proyecto         : String(row[3] || '').trim().toUpperCase(),
          lugar            : String(row[4] || '').trim(),
          cliente_raw      : clienteRaw,
          celular          : celularLimpio,
          hora             : String(row[7] || '').trim(),
          fuente_raw       : fuenteRaw,
          fuente_norm      : fi.normalizada,
          nombre_opc       : fi.nombreOPC,
          estado_conf      : String(row[11] || '').trim(),
          comentarios_conf : String(row[12] || '').trim(),
          seguimiento      : seguimiento,
          resultado        : resultado,
          comentario       : String(row[17] || '').trim(),
          liner            : String(row[18] || '').trim(),
          closer           : String(row[19] || '').trim(),
          // Flags calculados
          es_presencia     : esPresencia,
          es_tour          : esTour,
          es_no_tour       : esNoTour,
          es_no_show       : esNoShow
        });
      }
      etl_log('INFO','fase_leerManifiestos','✅ ' + nomMes + ': ' + (raw.length - 1) + ' registros');
    } catch (e) {
      etl_log('ERROR','fase_leerManifiestos','Error en ' + nomMes + ': ' + e.message);
    }
  }
  return presencias;
}


// ==========================================================================
// FASE 4 — STG_PRESENCIAS (full reload agrupado por mes)
// ==========================================================================

function fase_stgPresencias(presencias) {
  var ss    = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.STG_PRES);

  var HEADERS = [
    'MES_MANIFIESTO','ANIO','MES_NUM','ID_MANIFIESTO',
    'FECHA_EVENTO','HOJA_REFERENCIA','ASESOR_RAW','ASESOR_CANONICO',
    'PROYECTO','LUGAR','CLIENTE_RAW','CELULAR',
    'FUENTE_RAW','FUENTE_NORMALIZADA','NOMBRE_OPC',
    'ESTADO_CONFIRMACION','SEGUIMIENTO','RESULTADO','COMENTARIO',
    'LINER','CLOSER',
    'ES_PRESENCIA','ES_TOUR','ES_NO_TOUR','ES_NO_SHOW'
  ];

  if (presencias.length === 0) {
    etl_writeTable(sheet, [HEADERS]);
    etl_log('WARN','fase_stgPresencias','Sin registros de presencias para escribir');
    return;
  }

  var rows = presencias.map(function(p) {
    return [
      p.mes_manifiesto, p.anio, p.mes_num, p.id_manifiesto,
      p.fecha_evento, p.hoja_referencia, p.asesor_raw, p.asesor_canonico,
      p.proyecto, p.lugar, p.cliente_raw, p.celular,
      p.fuente_raw, p.fuente_norm, p.nombre_opc,
      p.estado_conf, p.seguimiento, p.resultado, p.comentario,
      p.liner, p.closer,
      p.es_presencia, p.es_tour, p.es_no_tour, p.es_no_show
    ];
  });

  etl_writeTable(sheet, [HEADERS].concat(rows));
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, rows.length);
  etl_log('INFO','fase_stgPresencias','✅ ' + rows.length + ' presencias escritas');
}


// ==========================================================================
// FASE 5 — LEER VENTAS
// ==========================================================================

function fase_leerVentas() {
  try {
    var ss    = SpreadsheetApp.openById(CFG.VENTAS_ID);
    var sheet = ss.getSheetByName(CFG.VENTAS_HOJA);

    if (!sheet || sheet.getLastRow() < 2) {
      etl_log('WARN','fase_leerVentas','Hoja de ventas vacía o no encontrada');
      return [];
    }

    var raw    = sheet.getRange(1, 1, sheet.getLastRow(), sheet.getLastColumn()).getValues();
    var ventas = [];
    var headers = raw[0] || [];

    var normH = function(h) {
      return String(h || '')
        .toUpperCase()
        .replace(/Á/g,'A').replace(/É/g,'E').replace(/Í/g,'I').replace(/Ó/g,'O').replace(/Ú/g,'U').replace(/Ñ/g,'N')
        .replace(/\s+/g, ' ')
        .trim();
    };
    var idxMap = {};
    for (var hi = 0; hi < headers.length; hi++) idxMap[normH(headers[hi])] = hi;
    var col = function(names, fallbackIdx) {
      for (var ni = 0; ni < names.length; ni++) {
        var k = normH(names[ni]);
        if (k in idxMap) return idxMap[k];
      }
      return fallbackIdx;
    };

    var iCliente    = col(['CLIENTE'], 1);
    // Fallback 2 porque CELULAR fue insertado en posición 2 (después de CLIENTE)
    var iCelular    = col(['CELULAR','CELULAR (LIMPIO)','TELEFONO','TELÉFONO'], 2);
    // Fallbacks actualizados al layout real: N°|Cliente|CELULAR|MODALIDAD|PROYECTO|LOTE|MZ|PRECIO VENTA|ESTADO2|FECHA DE COMPRA|FECHA PROCESA|HOY|MEDIO DE PAGO|OBRAS|FECHA DE PAGO DE OBRAS|TLMK|PROMOTORA|LINER|CLOSER|ORIGEN|COMISION|MES
    var iModalidad  = col(['MODALIDAD'], 3);
    var iProyecto   = col(['PROYECTO'], 4);
    var iLote       = col(['LOTE'], 5);
    var iMz         = col(['MZ'], 6);
    var iPrecio     = col(['PRECIO VENTA'], 7);
    var iEstado2    = col(['ESTADO2'], 8);
    var iFCompra    = col(['FECHA DE COMPRA'], 9);
    var iFProcesa   = col(['FECHA PROCESA'], 10);
    var iHoy        = col(['HOY'], 11);
    var iMedioPago  = col(['MEDIO DE PAGO'], 12);
    var iObras      = col(['OBRAS'], 13);
    var iFPagoObras = col(['FECHA DE PAGO DE OBRAS', 'FECHA PAGO OBRAS'], 14);
    var iTlmk       = col(['TLMK'], 15);
    var iPromotora  = col(['PROMOTORA'], 16);
    var iLiner      = col(['LINER'], 17);
    var iCloser     = col(['CLOSER'], 18);
    var iOrigen     = col(['ORIGEN'], 19);
    var iComision   = col(['COMISION'], 20);
    var iMes        = col(['MES'], 21);

    etl_log('INFO', 'fase_leerVentas',
      'Col map — CELULAR:' + iCelular +
      ' MODALIDAD:' + iModalidad +
      ' ESTADO2:' + iEstado2 +
      ' TLMK:' + iTlmk +
      ' PROMOTORA:' + iPromotora +
      ' OBRAS:' + iObras);

    for (var i = 1; i < raw.length; i++) {
      var r        = raw[i];
      var cliente  = String(r[iCliente] || '').trim().toUpperCase();
      if (!cliente) continue;
      var celular  = (iCelular >= 0) ? norm_tel(r[iCelular]) : '';

      var modalidad = String(r[iModalidad] || '').trim().toUpperCase();
      var estado2   = String(r[iEstado2] || '').trim().toUpperCase();
      var tlmkRaw   = String(r[iTlmk] || '').trim().toUpperCase();
      var promotora = String(r[iPromotora] || '').trim();

      // Normalización de ORIGEN: MD/MN → META (MN es typo de MD), PR → OPC
      var origenRaw  = String(r[iOrigen] || '').trim().toUpperCase();
      var origenNorm = (origenRaw === 'MD' || origenRaw === 'MN') ? 'META' :
                       origenRaw === 'PR' ? 'OPC'  : (origenRaw || 'SIN_ORIGEN');

      // HOY = RECAUDO (valor en soles); recaudo_val para sumas numéricas
      var hoyRaw = r[iHoy];
      var recaudoVal = 0;
      if (hoyRaw != null && hoyRaw !== '') {
        recaudoVal = parseFloat(String(hoyRaw).replace(/[^0-9.,]/g, '').replace(',', '.')) || 0;
      }
      ventas.push({
        n             : r[0],
        cliente       : cliente,
        celular       : celular,
        modalidad     : modalidad,
        proyecto      : String(r[iProyecto] || '').trim().toUpperCase(),
        lote          : r[iLote],
        mz            : String(r[iMz] || '').trim(),
        precio_venta  : r[iPrecio],
        estado2       : estado2,
        fecha_compra  : r[iFCompra],
        fecha_procesa : r[iFProcesa],
        hoy           : hoyRaw,
        recaudo_val   : recaudoVal,
        medio_pago    : String(r[iMedioPago] || '').trim(),
        tlmk_raw      : tlmkRaw,
        tlmk_canonico : mapear_asesor(tlmkRaw),
        promotora     : promotora,
        promotora_norm: norm_nombre(promotora),
        liner         : String(r[iLiner] || '').trim(),
        closer        : String(r[iCloser] || '').trim(),
        comision      : String(r[iComision] || '').trim(),
        mes           : String(r[iMes] || '').trim(),
        origen_raw    : origenRaw,
        origen_norm   : origenNorm,
        // Flags del embudo — incluye variantes con/sin acento
        es_negocio    : (modalidad === 'SEPARACION' || modalidad === 'SEPARACI\u00d3N'),
        es_procesable : (estado2   === 'PROCESADO'  || estado2   === 'PROCESADA')
      });
    }

    etl_log('INFO','fase_leerVentas','✅ ' + ventas.length + ' registros de ventas');
    return ventas;
  } catch (e) {
    etl_log('ERROR','fase_leerVentas','Error: ' + e.message);
    return [];
  }
}


// ==========================================================================
// FASE 6 — STG_VENTAS (full reload)
// ==========================================================================

function fase_stgVentas(ventas) {
  var ss    = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.STG_VTAS);

  var HEADERS = [
    'N','CLIENTE','CELULAR','MODALIDAD','PROYECTO','LOTE','MZ','PRECIO_VENTA',
    'ESTADO2','FECHA_COMPRA','FECHA_PROCESA','HOY','MEDIO_PAGO',
    'TLMK_RAW','TLMK_CANONICO','PROMOTORA','LINER','CLOSER',
    'COMISION','MES','ORIGEN_RAW','ORIGEN_NORM','ES_NEGOCIO','ES_PROCESABLE'
  ];

  if (ventas.length === 0) {
    etl_writeTable(sheet, [HEADERS]);
    etl_log('WARN','fase_stgVentas','Sin registros de ventas para escribir');
    return;
  }

  var rows = ventas.map(function(v) {
    return [
      v.n, v.cliente, v.celular, v.modalidad, v.proyecto, v.lote, v.mz, v.precio_venta,
      v.estado2, v.fecha_compra, v.fecha_procesa, v.hoy, v.medio_pago,
      v.tlmk_raw, v.tlmk_canonico, v.promotora, v.liner, v.closer,
      v.comision, v.mes, v.origen_raw, v.origen_norm, v.es_negocio, v.es_procesable
    ];
  });

  etl_writeTable(sheet, [HEADERS].concat(rows));
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, rows.length);
  etl_log('INFO','fase_stgVentas','✅ ' + rows.length + ' ventas escritas');
}


// ==========================================================================
// FASE 7 — ACTUALIZAR DIMENSIONES (DIM_OPC, DIM_PROYECTOS)
// ==========================================================================

function fase_actualizarDims(presencias, factRows) {
  var ss = getSpreadsheetDestino();

  /* ── DIM_OPC ─────────────────────────────────────────────────────────── */
  var opcSet = {};

  // OPCs desde presencias
  for (var p = 0; p < presencias.length; p++) {
    var opc = presencias[p].nombre_opc;
    if (opc && opc.length > 1) opcSet[opc.toUpperCase()] = { fuente: 'MANIFIESTO' };
  }
  // OPCs desde FACT_INTERACCIONES
  for (var f = 0; f < factRows.length; f++) {
    var opcF = factRows[f].nombre_opc;
    if (opcF && opcF.length > 1) {
      if (!opcSet[opcF.toUpperCase()]) opcSet[opcF.toUpperCase()] = { fuente: 'CRM' };
    }
  }

  var opcSheet = getOrCreateSheet(ss, CFG.OUT.DIM_OPC);
  if (opcSheet.getLastRow() === 0) {
    opcSheet.appendRow(['NOMBRE_OPC','FUENTE_DETECTADA','ACTIVO']);
    opcSheet.getRange(1,1,1,3).setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
  }

  // Leer OPCs ya existentes
  var existOPCs = new Set();
  if (opcSheet.getLastRow() > 1) {
    var existData = opcSheet.getRange(2,1,opcSheet.getLastRow()-1,1).getValues();
    for (var eo = 0; eo < existData.length; eo++) existOPCs.add(String(existData[eo][0]).toUpperCase());
  }

  var newOPCRows = [];
  for (var k in opcSet) {
    if (!existOPCs.has(k)) newOPCRows.push([k, opcSet[k].fuente, true]);
  }
  if (newOPCRows.length > 0) {
    opcSheet.getRange(opcSheet.getLastRow() + 1, 1, newOPCRows.length, 3).setValues(newOPCRows);
    etl_log('INFO','fase_actualizarDims','➕ ' + newOPCRows.length + ' nuevos OPCs en DIM_OPC');
  }

  /* ── DIM_PROYECTOS ───────────────────────────────────────────────────── */
  var proySet = {};
  for (var fi = 0; fi < factRows.length; fi++) {
    var p2 = factRows[fi].proyecto;
    if (p2 && p2.length > 1) proySet[p2] = true;
  }
  for (var pp = 0; pp < presencias.length; pp++) {
    var pp2 = presencias[pp].proyecto;
    if (pp2 && pp2.length > 1) proySet[pp2] = true;
  }

  var proySheet = getOrCreateSheet(ss, CFG.OUT.DIM_PROY);
  if (proySheet.getLastRow() === 0) {
    proySheet.appendRow(['NOMBRE_PROYECTO','ACTIVO']);
    proySheet.getRange(1,1,1,2).setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
  }

  var existProys = new Set();
  if (proySheet.getLastRow() > 1) {
    var epd = proySheet.getRange(2,1,proySheet.getLastRow()-1,1).getValues();
    for (var ep = 0; ep < epd.length; ep++) existProys.add(String(epd[ep][0]).toUpperCase());
  }

  var newProyRows = [];
  for (var pk in proySet) {
    if (!existProys.has(pk.toUpperCase())) newProyRows.push([pk, true]);
  }
  if (newProyRows.length > 0) {
    proySheet.getRange(proySheet.getLastRow() + 1, 1, newProyRows.length, 2).setValues(newProyRows);
    etl_log('INFO','fase_actualizarDims','➕ ' + newProyRows.length + ' nuevos proyectos en DIM_PROYECTOS');
  }
}


// ==========================================================================
// RPT_LEAD_TIEMPOS — asignación → presencia (fecha del manifiesto + días)
// ==========================================================================

/**
 * Genera RPT_LEAD_TIEMPOS: una fila por lead con ASIGNACION/ASIGNACION_MANUAL.
 *
 *   - FECHA_PRESENCIA: primera FECHA_EVENTO del manifiesto con ES_PRESENCIA (ASISTIÓ) y fecha ≥ día de asignación
 *   - DIAS_ASIGNACION_A_PRESENCIA: días calendario desde FECHA_ASIGNACION_LEAD hasta FECHA_PRESENCIA (vacío si no hubo presencia)
 *
 * Looker: AVG(DIAS_ASIGNACION_A_PRESENCIA); control de fechas sobre FECHA_ASIGNACION_LEAD o FECHA_PRESENCIA según necesites.
 */
function fase_rptLeadTiemposDesdeSTG(factRows, presencias) {
  var latestGestionByCel = {};
  for (var i = 0; i < factRows.length; i++) {
    var fg = factRows[i];
    var celG = String(fg.celular || '').trim();
    if (!celG) continue;
    var tipoG = String(fg.tipo_accion || '').toUpperCase().trim();
    if (tipoG !== 'LLAMADA' && tipoG !== 'EDICION_MANUAL') continue;
    var fechaG = toDate(fg.fecha_interaccion);
    if (!fechaG || fechaG.getTime() === 0) continue;
    if (!latestGestionByCel[celG] || fechaG.getTime() >= latestGestionByCel[celG].fecha.getTime()) {
      latestGestionByCel[celG] = {
        fecha: fechaG,
        asesor: fg.asesor_nombre || '',
        proyecto: fg.proyecto || '',
        fuente: fg.fuente_norm || '',
        nombre_opc: fg.nombre_opc || ''
      };
    }
  }

  var leadsMap = {};
  for (var r = 0; r < factRows.length; r++) {
    var f = factRows[r];
    var cel = String(f.celular || '').trim();
    if (!cel) continue;
    var fechaI = toDate(f.fecha_interaccion);
    if (!fechaI || fechaI.getTime() === 0) continue;

    if (!leadsMap[cel]) {
      leadsMap[cel] = crearLead(cel, f.nombre_cliente, f.asesor_nombre, f.proyecto, f.fuente_norm, f.nombre_opc, fechaI);
    }
    var lead = leadsMap[cel];
    if (fechaI < lead.fecha_entrada) lead.fecha_entrada = fechaI;

    if (CFG.TIPO_ASIGNACION.indexOf(String(f.tipo_accion || '').toUpperCase().trim()) !== -1) {
      lead.tieneAsignacion = true;
      if (!lead.fecha_asignacion || fechaI < lead.fecha_asignacion) {
        lead.fecha_asignacion = fechaI;
        lead.asesor = f.asesor_nombre || lead.asesor;
        lead.proyecto = f.proyecto || lead.proyecto;
        lead.fuente = f.fuente_norm || lead.fuente;
        lead.nombre_opc = f.nombre_opc || lead.nombre_opc;
      }
    }
  }

  for (var celKey in leadsMap) {
    var latest = latestGestionByCel[celKey];
    if (!latest) continue;
    if (latest.asesor) leadsMap[celKey].asesor = latest.asesor;
    if (latest.proyecto) leadsMap[celKey].proyecto = latest.proyecto;
    if (latest.fuente) leadsMap[celKey].fuente = latest.fuente;
    if (latest.nombre_opc) leadsMap[celKey].nombre_opc = latest.nombre_opc;
  }

  fase_writeRptLeadTiempos(leadsMap, presencias);
}

function fase_writeRptLeadTiempos(leadsMap, presencias) {
  var ss = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.RPT_TIEMPOS);

  function fuenteGrupo(v) {
    var x = String(v || '').toUpperCase().trim();
    if (x === 'META' || x === 'WSP-FORM' || x === 'WSP') return 'REDES';
    if (x === 'OPC') return 'OPC';
    return 'Otros';
  }

  function diasCalendario(fIni, fFin) {
    if (!fIni || !fFin) return '';
    var a = new Date(fIni); a.setHours(0, 0, 0, 0);
    var b = new Date(fFin); b.setHours(0, 0, 0, 0);
    if (b.getTime() < a.getTime()) return '';
    return Math.round((b.getTime() - a.getTime()) / 86400000);
  }

  var HEADERS = [
    'CELULAR', 'NOMBRE_CLIENTE', 'ASESOR', 'PROYECTO', 'FUENTE', 'FUENTE_GRUPO', 'NOMBRE_OPC',
    'FECHA_ASIGNACION_LEAD', 'FECHA_PRESENCIA', 'DIAS_ASIGNACION_A_PRESENCIA'
  ];

  var presenciasPorCel = {};
  for (var p = 0; p < presencias.length; p++) {
    var pr = presencias[p];
    if (!pr.es_presencia) continue;
    var celPres = String(pr.celular || '').trim();
    if (!celPres) continue;
    var fdPres = toDate(pr.fecha_evento);
    if (!fdPres || fdPres.getTime() === 0) continue;
    fdPres.setHours(0, 0, 0, 0);
    if (!presenciasPorCel[celPres]) presenciasPorCel[celPres] = [];
    presenciasPorCel[celPres].push(fdPres.getTime());
  }
  for (var celPresKey in presenciasPorCel) {
    presenciasPorCel[celPresKey].sort(function(a, b) { return a - b; });
  }

  var rows = [];
  var cels = Object.keys(leadsMap);
  for (var i = 0; i < cels.length; i++) {
    var lead = leadsMap[cels[i]];
    if (!lead.tieneAsignacion || !lead.fecha_asignacion) continue;
    var cel = lead.celular;
    if (!cel) continue;

    var fAsig = lead.fecha_asignacion;
    var fAsigDay = new Date(fAsig);
    fAsigDay.setHours(0, 0, 0, 0);
    var tAsig = fAsigDay.getTime();

    var primeraPres = null;
    var presList = presenciasPorCel[String(cel)] || [];
    for (var pp = 0; pp < presList.length; pp++) {
      if (presList[pp] >= tAsig) {
        primeraPres = new Date(presList[pp]);
        break;
      }
    }

    var dAsigPres = primeraPres ? diasCalendario(fAsig, primeraPres) : '';

    rows.push([
      cel,
      lead.nombre || '',
      lead.asesor || '',
      lead.proyecto || '',
      lead.fuente || '',
      fuenteGrupo(lead.fuente),
      lead.nombre_opc || '',
      fAsig,
      primeraPres || '',
      dAsigPres === '' ? '' : dAsigPres
    ]);
  }

  var out = [HEADERS].concat(rows);
  etl_writeTable(sheet, out);

  if (rows.length > 0) {
    sheet.getRange(2, 8, rows.length, 2).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 10, rows.length, 1).setNumberFormat('0');
  }
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, rows.length);
  etl_log('INFO', 'fase_writeRptLeadTiempos', rows.length + ' filas (asignación → presencia)');
}

// ==========================================================================
// DATA_TIPIFICACIONES_DIA — Gestión diaria por interacción/tipificación
// ==========================================================================

/**
 * Hoja detallada para Looker Studio orientada a gestión diaria:
 * - 1 fila por interacción de FACT_INTERACCIONES (con celular válido).
 * - Separa ASIGNADOS vs GESTION por TIPO_ACCION.
 * - Regulariza tipificaciones (misma lógica acordada en Looker).
 * - Enlaza con manifiesto por CELULAR (cita/presencia/tour) y primeras fechas.
 * - Incluye FECHA_ASIGNACION_LEAD y FECHA_ENTRADA_LEAD para filtros de cohorte.
 */
function fase_dataTipificacionesDia(factRows, presencias) {
  var t0 = Date.now();
  var ss = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.TIPIF_DIA);

  function normalizarTipificacionGestion(v) {
    var x = String(v || '').toUpperCase().trim();
    if (!x) return '';
    if (x === 'NC') return 'NO CONTESTA';
    if (x === 'NSHOW') return 'NO ASISTIO';
    if (x === 'NI' || x === 'NI-PROY' || x === 'NI-LEG' || x === 'NI-ECO') return 'NO INTERESADO';
    if (x === 'CC' || x === 'CXC') return 'CITA CONFIRMADA';
    if (x === 'AP') return 'APAGADO';
    if (x === 'AG INM' || x === 'NQ' || x === 'DD') return 'NO CALIFICA';
    if (x === 'VLL') return 'VOLVER A LLAMAR';
    if (x === 'DF') return 'DATO FALSO';
    if (x === 'IW') return 'INFO. WHATSAPP';
    if (x === 'FS') return 'FUERA DE SERVICIO';
    if (x === 'SG') return 'SEGUIMIENTO';
    if (x === 'HP') return 'CITA HP';
    if (x === 'CP' || x === 'VP') return 'CITA PROYECTO';
    if (x === 'HH') return 'CITA HOY';
    return 'REVISAR';
  }

  function diasCalendario(fIni, fFin) {
    if (!fIni || !fFin) return '';
    var a = new Date(fIni); a.setHours(0, 0, 0, 0);
    var b = new Date(fFin); b.setHours(0, 0, 0, 0);
    if (b.getTime() < a.getTime()) return '';
    return Math.round((b.getTime() - a.getTime()) / 86400000);
  }

  function esAsignacion(tipo) {
    return CFG.TIPO_ASIGNACION.indexOf(String(tipo || '').toUpperCase().trim()) !== -1;
  }

  function esGestion(tipo) {
    return CFG.TIPO_GESTION.indexOf(String(tipo || '').toUpperCase().trim()) !== -1;
  }

  // Índices por celular: entrada, asignación y datos de manifiesto.
  var leadByCel = {};          // { fecha_entrada, fecha_asignacion }
  var manifByCel = {};         // { fecha_cita, fecha_presencia, fecha_tour }
  var ultimoGestorPorCel = {}; // cel -> { asesor, fecha } usando LLAMADA/EDICION_MANUAL
  var asignadosBase = {};      // key(fecha+asesor+proy+fuente+opc) -> { cels:{} }

  // 1) Último gestor por celular (orden cronológico real, no el orden del array FACT)
  for (var u = 0; u < factRows.length; u++) {
    var fu = factRows[u];
    var celU = String(fu.celular || '').trim();
    if (!celU) continue;
    var tipoU = String(fu.tipo_accion || '').toUpperCase().trim();
    if (tipoU !== 'LLAMADA' && tipoU !== 'EDICION_MANUAL') continue;
    var asU = String(fu.asesor_nombre || '').trim();
    if (!asU) continue;
    var fU = toDate(fu.fecha_interaccion);
    if (!fU || fU.getTime() === 0) continue;
    if (!ultimoGestorPorCel[celU] || fU.getTime() >= ultimoGestorPorCel[celU].fecha.getTime()) {
      ultimoGestorPorCel[celU] = { asesor: asU, fecha: fU };
    }
  }

  // 2) Entrada, primera asignación y denominador ASIGNADOS_BASE_DIM
  for (var i = 0; i < factRows.length; i++) {
    var f = factRows[i];
    var cel = String(f.celular || '').trim();
    if (!cel) continue;
    var fi = toDate(f.fecha_interaccion);
    if (!fi || fi.getTime() === 0) continue;
    if (!leadByCel[cel]) {
      leadByCel[cel] = { fecha_entrada: fi, fecha_asignacion: null };
    } else if (fi < leadByCel[cel].fecha_entrada) {
      leadByCel[cel].fecha_entrada = fi;
    }
    if (esAsignacion(f.tipo_accion) && (!leadByCel[cel].fecha_asignacion || fi < leadByCel[cel].fecha_asignacion)) {
      leadByCel[cel].fecha_asignacion = fi;
    }

    if (esAsignacion(f.tipo_accion)) {
      var asigFinal = (ultimoGestorPorCel[cel] && ultimoGestorPorCel[cel].asesor)
        ? ultimoGestorPorCel[cel].asesor
        : String(f.asesor_nombre || '').trim();
      var kAsg = formatFecha(fi) + '§' +
        asigFinal + '§' +
        String(f.proyecto || '').trim().toUpperCase() + '§' +
        String(f.fuente_norm || '').trim() + '§' +
        String(f.nombre_opc || '').trim();
      if (!asignadosBase[kAsg]) asignadosBase[kAsg] = { cels: {} };
      asignadosBase[kAsg].cels[cel] = true;
    }
  }

  var asignadosBaseCount = {};
  for (var bk in asignadosBase) {
    asignadosBaseCount[bk] = Object.keys(asignadosBase[bk].cels).length;
  }

  for (var p = 0; p < presencias.length; p++) {
    var pr = presencias[p];
    var celP = String(pr.celular || '').trim();
    if (!celP || celP.length < 9) continue;
    var fd = toDate(pr.fecha_evento);
    if (!fd || fd.getTime() === 0) continue;
    fd.setHours(0, 0, 0, 0);
    if (!manifByCel[celP]) manifByCel[celP] = { fecha_cita: null, fecha_presencia: null, fecha_tour: null };
    var m = manifByCel[celP];
    if (!m.fecha_cita || fd < m.fecha_cita) m.fecha_cita = new Date(fd);
    if (pr.es_presencia && (!m.fecha_presencia || fd < m.fecha_presencia)) m.fecha_presencia = new Date(fd);
    if (pr.es_tour && (!m.fecha_tour || fd < m.fecha_tour)) m.fecha_tour = new Date(fd);
  }

  var HEADERS = [
    'ID_INTERACCION','ID_CLIENTE','CELULAR','NOMBRE_CLIENTE',
    'ASESOR_NOMBRE','PROYECTO','FUENTE','NOMBRE_OPC',
    'FECHA_INTERACCION','FECHA_REGISTRO_LEAD','FECHA_ENTRADA_LEAD','FECHA_ASIGNACION_LEAD',
    'TIPO_ACCION','TIPO_REPORTE_DIA','ES_ASIGNADO','ES_GESTION',
    'TIPIFICACION_RAW','TIPIFICACION',
    'ASIGNADOS_BASE_DIM',
    'COMENTARIO',
    'TIENE_CITA_MANIF','TIENE_PRESENCIA_MANIF','TIENE_TOUR_MANIF',
    'FECHA_CITA_MANIF','FECHA_PRESENCIA_MANIF','FECHA_TOUR_MANIF',
    'DIAS_ASIG_A_CITA','DIAS_ASIG_A_PRESENCIA','DIAS_ASIG_A_TOUR'
  ];

  var out = [HEADERS];
  for (var r = 0; r < factRows.length; r++) {
    var fr = factRows[r];
    var celR = String(fr.celular || '').trim();
    if (!celR) continue;

    var tipoAcc = String(fr.tipo_accion || '').toUpperCase().trim();
    var tipoRpt = esAsignacion(tipoAcc) ? 'ASIGNADOS' : (esGestion(tipoAcc) ? 'GESTION' : 'OTROS');
    if (tipoRpt === 'OTROS') continue;
    var fInter = toDate(fr.fecha_interaccion);
    var asesorOut = String(fr.asesor_nombre || '').trim();
    if (tipoRpt === 'ASIGNADOS' && ultimoGestorPorCel[celR] && ultimoGestorPorCel[celR].asesor) {
      asesorOut = ultimoGestorPorCel[celR].asesor;
    }
    var kRow = formatFecha(fInter) + '§' +
      asesorOut + '§' +
      String(fr.proyecto || '').trim().toUpperCase() + '§' +
      String(fr.fuente_norm || '').trim() + '§' +
      String(fr.nombre_opc || '').trim();
    var baseAsg = asignadosBaseCount[kRow] || 0;

    var lead = leadByCel[celR] || {};
    var mfc = manifByCel[celR] || {};
    var fAs = lead.fecha_asignacion || null;

    var fCita = mfc.fecha_cita || null;
    var fPres = mfc.fecha_presencia || null;
    var fTour = mfc.fecha_tour || null;

    // "Desde asignación": solo marca hitos en/tras la fecha de asignación.
    var okCita = fCita && (!fAs || diasCalendario(fAs, fCita) !== '');
    var okPres = fPres && (!fAs || diasCalendario(fAs, fPres) !== '');
    var okTour = fTour && (!fAs || diasCalendario(fAs, fTour) !== '');

    out.push([
      fr.id_interaccion || '',
      fr.id_cliente || '',
      celR,
      fr.nombre_cliente || '',
      asesorOut,
      fr.proyecto || '',
      fr.fuente_norm || '',
      fr.nombre_opc || '',
      fr.fecha_interaccion || '',
      fr.fecha_registro || '',
      lead.fecha_entrada || '',
      fAs || '',
      tipoAcc,
      tipoRpt,
      (tipoRpt === 'ASIGNADOS' ? 1 : 0),
      (tipoRpt === 'GESTION' ? 1 : 0),
      fr.tipificacion || '',
      normalizarTipificacionGestion(fr.tipificacion),
      baseAsg,
      fr.comentario || '',
      okCita ? 1 : 0,
      okPres ? 1 : 0,
      okTour ? 1 : 0,
      okCita ? fCita : '',
      okPres ? fPres : '',
      okTour ? fTour : '',
      okCita ? diasCalendario(fAs, fCita) : '',
      okPres ? diasCalendario(fAs, fPres) : '',
      okTour ? diasCalendario(fAs, fTour) : ''
    ]);
  }

  etl_writeTable(sheet, out);

  if (out.length > 1) {
    var n = out.length - 1;
    // Fechas
    var dateCols = [9, 10, 11, 12, 24, 25, 26];
    for (var d = 0; d < dateCols.length; d++) {
      sheet.getRange(2, dateCols[d], n, 1).setNumberFormat('yyyy-mm-dd');
    }
    // Flags/contadores
    var intCols = [15, 16, 19, 20, 21, 23, 27, 28, 29];
    for (var c = 0; c < intCols.length; c++) {
      sheet.getRange(2, intCols[c], n, 1).setNumberFormat('0');
    }
  }
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, out.length - 1);

  etl_log('INFO','fase_dataTipificacionesDia',
    '✅ ' + (out.length - 1) + ' filas en ' + CFG.OUT.TIPIF_DIA + ' | ' + (Date.now() - t0) + 'ms');
}


// ==========================================================================
// FASE 8 — DATA_EMBUDO_FULL (Unpivoted, 8 etapas, listo para Looker Studio)
// ==========================================================================

/**
 * Genera la tabla DATA_EMBUDO_FULL.
 *
 * Estrategia de vinculación (cadena de relaciones):
 *   - FACT ↔ Manifiesto: por CELULAR (ambos tienen número)
 *   - Ventas ↔ FACT/Manifiesto: por CELULAR (nueva llave en Ventas)
 *   - Fallback Ventas ↔ Manifiesto: NOMBRE + score por ASESOR/TLMK + OPC + FECHA + PROYECTO
 *
 * Flujo:
 *   1. Leads desde FACT (por CELULAR)
 *   2. Enriquecer con Manifiesto (por CELULAR): presencias, tours, leads externos
 *   3. Cruzar Ventas: primero por CELULAR; si falta, fallback por nombre con puente Manifiesto
 */
function fase_embudoCompleto(factRows, presencias, ventas, options) {
  options = options || {};
  var t0 = Date.now();
  var ss = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.EMBUDO);

  /* ── PASO 1: Construir mapa de leads desde FACT_INTERACCIONES ───────── */
  var leadsMap = {};

  for (var i = 0; i < factRows.length; i++) {
    var f    = factRows[i];
    var cel  = f.celular;
    if (!cel) continue;

    var tipif   = f.tipificacion;
    var fechaI  = toDate(f.fecha_interaccion);

    if (!leadsMap[cel]) {
      leadsMap[cel] = crearLead(cel, f.nombre_cliente, f.asesor_nombre,
                                f.proyecto, f.fuente_norm, f.nombre_opc, fechaI);
    }

    var lead = leadsMap[cel];

    // Fecha de asignación: primera interacción ASIGNACION/ASIGNACION_MANUAL (para Looker: filtrar por período "asignados en la semana")
    if (CFG.TIPO_ASIGNACION.indexOf(f.tipo_accion) !== -1) {
      lead.tieneAsignacion = true;
      if (!lead.fecha_asignacion || fechaI < lead.fecha_asignacion) {
        lead.fecha_asignacion = fechaI;
        // Alinear OPC con la primera asignación (si existe nombre_opc en ese registro)
        if (f.nombre_opc && String(f.nombre_opc).trim() !== '') lead.nombre_opc = f.nombre_opc;
      }
    }

    // Mantener fecha de entrada (primera interacción)
    if (fechaI < lead.fecha_entrada) lead.fecha_entrada = fechaI;

    // Actualizar a la interacción más reciente
    if (fechaI >= lead.ultima_fecha) {
      lead.ultima_fecha  = fechaI;
      lead.ultima_tipif  = tipif;
      lead.asesor        = f.asesor_nombre || lead.asesor;
      lead.proyecto      = f.proyecto      || lead.proyecto;
    }

    // Evaluar etapas del embudo para este lead
    if (CFG.EMBUDO.INVALIDOS.indexOf(tipif) !== -1)          lead.isNoEfectivo = true;
    if (tipif && CFG.EMBUDO.INVALIDOS.indexOf(tipif) === -1) lead.isContacto   = true;
    if (CFG.EMBUDO.POTENCIALES.indexOf(tipif) !== -1)        lead.isPotencial  = true;
    if (CFG.EMBUDO.CITAS.indexOf(tipif)       !== -1)        lead.isCita       = true;
    if (CFG.EMBUDO.DATOS_FALSOS.indexOf(tipif) !== -1)       lead.tieneDatoFalso = true;
  }

  /* ── PASO 2: Enriquecer con datos de PRESENCIAS (por CELULAR) ─────────
   *  Si el celular NO existe en CRM → se agrega como lead desde Manifiesto,
   *  marcándolo con etapas 1-4 implícitas (registró cita en el manifiesto).
   * ─────────────────────────────────────────────────────────────────────── */
  for (var p = 0; p < presencias.length; p++) {
    var pres  = presencias[p];
    var celP  = pres.celular;
    var fEvt  = toDate(pres.fecha_evento);

    if (!celP || celP.length < 9) continue;
    if (!pres.es_presencia && !pres.es_tour) continue;

    if (!leadsMap[celP]) {
      // Lead que no estaba en CRM (vino del Manifiesto directamente)
      leadsMap[celP] = crearLead(celP, pres.cliente_raw, pres.asesor_canonico,
                                  pres.proyecto, pres.fuente_norm, pres.nombre_opc, fEvt);
      leadsMap[celP].isContacto   = true;
      leadsMap[celP].isPotencial  = true;
      leadsMap[celP].isCita       = true; // implícito: tenía cita en el manifiesto
    }

    var leadP = leadsMap[celP];
    if (pres.es_presencia) {
      leadP.isPresencia = true;
      if (!leadP.fecha_presencia || fEvt > leadP.fecha_presencia) {
        leadP.fecha_presencia = fEvt;
        leadP.tipo_presencia  = pres.resultado;
      }
    }
    if (pres.es_tour) leadP.isTour = true;
    // Si la fuente del lead es REMARCADO/REASIGNADO/REFERIDO (CRM), priorizar la fuente del Manifiesto (OPC, META, WSP-FORM, WSP).
    actualizarFuenteDesdeManifiesto(leadP, pres.fuente_norm);
  }

  /* ── PASO 3: Cruzar con VENTAS ───────────────────────────────────────────
   *  Match 1 (prioritario): CELULAR en Ventas (llave directa)
   *  Match 2: nombre en leadsMap (FACT+Manifiesto ya cargados)
   *  Match 3: nombre en Manifiesto + scoring (asesor/opc/fecha/proyecto)
   * ─────────────────────────────────────────────────────────────────────── */
  var nombreIdx = {};
  for (var cel2 in leadsMap) {
    var nn = norm_nombre(leadsMap[cel2].nombre);
    if (nn.length > 3 && !nombreIdx[nn]) nombreIdx[nn] = cel2;
  }

  // Índice Manifiesto: nombre_norm → [{celular, asesor, fuente, nombre_opc_norm, fecha_evento, ...}]
  var manifestIdx = {};
  for (var pm = 0; pm < presencias.length; pm++) {
    var presM = presencias[pm];
    if (!presM.celular || presM.celular.length < 9) continue;
    var nnM = norm_nombre(presM.cliente_raw);
    if (nnM.length < 3) continue;
    if (!manifestIdx[nnM]) manifestIdx[nnM] = [];
    manifestIdx[nnM].push({
      celular     : presM.celular,
      proyecto    : String(presM.proyecto || '').toUpperCase().trim(),
      asesor      : presM.asesor_canonico,
      nombre_opc  : presM.nombre_opc,
      nombre_opc_norm: norm_nombre(presM.nombre_opc),
      fuente_norm : presM.fuente_norm,
      cliente_raw : presM.cliente_raw,
      fecha_evento: toDate(presM.fecha_evento),
      es_presencia: presM.es_presencia,
      es_tour     : presM.es_tour
    });
  }

  // Índice Manifiesto por FECHA+ASESOR+PROYECTO (para ventas sin celular y sin nombre conocido).
  // Clave: "yyyy-MM-dd§asesor_canon§proyecto" → [presencias]
  // Lógica: si la fecha_compra coincide con la fecha_evento del Manifiesto para el mismo
  // asesor/TLMK y proyecto, es muy probable que sea el mismo cliente.
  var fechaAsesorPrIdx = {};
  for (var pfap = 0; pfap < presencias.length; pfap++) {
    var presFAP = presencias[pfap];
    if (!presFAP.celular || presFAP.celular.length < 9) continue;
    var fEvtFAP = toDate(presFAP.fecha_evento);
    if (!fEvtFAP || fEvtFAP.getTime() === 0) continue;
    var keyFAP = formatFecha(fEvtFAP) + '\u00a7' +
                 (presFAP.asesor_canonico || '') + '\u00a7' +
                 String(presFAP.proyecto || '').toUpperCase().trim();
    if (!fechaAsesorPrIdx[keyFAP]) fechaAsesorPrIdx[keyFAP] = [];
    fechaAsesorPrIdx[keyFAP].push({
      celular     : presFAP.celular,
      nombre_opc  : presFAP.nombre_opc,
      nombre_opc_norm: norm_nombre(presFAP.nombre_opc),
      fuente_norm : presFAP.fuente_norm,
      es_presencia: presFAP.es_presencia,
      es_tour     : presFAP.es_tour,
      cliente_raw : presFAP.cliente_raw
    });
  }

  for (var v = 0; v < ventas.length; v++) {
    var vta      = ventas[v];
    var nNorm    = norm_nombre(vta.cliente);
    var fVta     = toDate(vta.fecha_compra);
    var vtaProy  = String(vta.proyecto || '').toUpperCase().trim();
    var celMatch = vta.celular || '';

    if (celMatch && !leadsMap[celMatch]) {
      leadsMap[celMatch] = crearLead(
        celMatch, vta.cliente, vta.tlmk_canonico, vtaProy, '', vta.promotora, fVta
      );
      // Si existe venta, asumimos que atravesó fases previas del embudo.
      leadsMap[celMatch].isContacto = true;
      leadsMap[celMatch].isPotencial = true;
      leadsMap[celMatch].isCita = true;
    }
    if (!celMatch) celMatch = nombreIdx[nNorm];

    // Si no hay match directo, buscar en Manifiesto (puente Ventas→Manifiesto→CELULAR)
    // Desempate por: asesor(TLMK), nombre_opc(promotora), fecha, proyecto
    if (!celMatch && manifestIdx[nNorm]) {
      var candidates = manifestIdx[nNorm];
      var best = null;
      var bestScore = -1;
      for (var ci = 0; ci < candidates.length; ci++) {
        var c = candidates[ci];
        var score = 0;
        if (c.asesor && vta.tlmk_canonico && c.asesor === vta.tlmk_canonico) score += 5;
        if (c.nombre_opc_norm && vta.promotora_norm && c.nombre_opc_norm === vta.promotora_norm) score += 4;
        // Proximidad fecha_evento ↔ fecha_compra: la presencia ocurre el día (o días antes) de la separación
        if (c.fecha_evento && fVta) {
          var diffDias = (fVta.getTime() - c.fecha_evento.getTime()) / 86400000;
          if (diffDias >= 0 && diffDias <= 3)  score += 6; // mismo día o ≤3d: señal muy fuerte
          else if (diffDias > 3 && diffDias <= 14) score += 3;
          else if (diffDias > 14 && diffDias <= 90) score += 1;
        }
        // ORIGEN de Ventas (MD=META, PR=OPC) vs fuente normalizada del Manifiesto
        if (vta.origen_norm && c.fuente_norm && vta.origen_norm === c.fuente_norm) score += 2;
        if (c.proyecto === vtaProy) score += 1; // tiebreak suave
        if (score > bestScore) { bestScore = score; best = c; }
      }
      if (!best) best = candidates[0];
      celMatch = best.celular;
      if (!leadsMap[celMatch]) {
        var fuenteBest = esFuenteManifiestoPrioritaria(best.fuente_norm) ? best.fuente_norm : '';
        leadsMap[celMatch] = crearLead(best.celular, best.cliente_raw, best.asesor,
                                        best.proyecto, fuenteBest, best.nombre_opc, fVta);
        leadsMap[celMatch].isContacto  = true;
        leadsMap[celMatch].isPotencial  = true;
        leadsMap[celMatch].isCita       = true;
        if (best.es_presencia) leadsMap[celMatch].isPresencia = true;
        if (best.es_tour)      leadsMap[celMatch].isTour      = true;
      } else {
        // Lead ya existía (CRM): priorizar fuente del Manifiesto si la del CRM es REMARCADO/REASIGNADO/REFERIDO.
        actualizarFuenteDesdeManifiesto(leadsMap[celMatch], best.fuente_norm);
      }
    }

    // Match 4 (si aún no hay celMatch): buscar por fecha_compra ± 3 días + asesor + proyecto.
    // Cubre los 2026 sin celular cuyo nombre en el Manifiesto es distinto al de VENTAS.
    if (!celMatch && vta.tlmk_canonico && fVta && fVta.getTime() > 0) {
      var bestFAP = null, bestFAPScore = -1;
      for (var deltaFAP = 0; deltaFAP <= 3; deltaFAP++) {
        for (var signFAP = -1; signFAP <= 1; signFAP += 2) {
          var fBusq = new Date(fVta.getTime() + deltaFAP * signFAP * 86400000);
          var kFAP  = formatFecha(fBusq) + '\u00a7' + vta.tlmk_canonico + '\u00a7' + vtaProy;
          if (!fechaAsesorPrIdx[kFAP]) continue;
          var candFAP = fechaAsesorPrIdx[kFAP];
          for (var cfap = 0; cfap < candFAP.length; cfap++) {
            var cFAP = candFAP[cfap];
            var scoreFAP = (6 - deltaFAP * 2); // 6 si mismo día, 4 si ±1d, 2 si ±2d, 0 si ±3d
            if (cFAP.nombre_opc_norm && vta.promotora_norm &&
                cFAP.nombre_opc_norm === vta.promotora_norm) scoreFAP += 4;
            if (vta.origen_norm && cFAP.fuente_norm &&
                vta.origen_norm === cFAP.fuente_norm) scoreFAP += 2;
            if (scoreFAP > bestFAPScore) { bestFAPScore = scoreFAP; bestFAP = cFAP; }
          }
        }
        if (deltaFAP === 0) break; // el día exacto solo se busca una vez (no repetir con signos)
      }
      if (bestFAP) {
        celMatch = bestFAP.celular;
        if (!leadsMap[celMatch]) {
          var fuenteFAP = esFuenteManifiestoPrioritaria(bestFAP.fuente_norm) ? bestFAP.fuente_norm : '';
          leadsMap[celMatch] = crearLead(celMatch, vta.cliente, vta.tlmk_canonico,
                                          vtaProy, fuenteFAP, bestFAP.nombre_opc, fVta);
          leadsMap[celMatch].isContacto  = true;
          leadsMap[celMatch].isPotencial = true;
          leadsMap[celMatch].isCita      = true;
          if (bestFAP.es_presencia) leadsMap[celMatch].isPresencia = true;
          if (bestFAP.es_tour)      leadsMap[celMatch].isTour      = true;
        } else {
          actualizarFuenteDesdeManifiesto(leadsMap[celMatch], bestFAP.fuente_norm);
        }
      }
    }

    // Ventas sin match (sin CELULAR o sin relación en CRM/Manifiesto): se cuentan igual como REFERIDO/SIN CELULAR.
    // Cuando más adelante agregues el número en Ventas, el ETL vinculará por CELULAR y dejará de ser referido.
    if (!celMatch) {
      var synthKey = 'VTA_REF_' + v;
      leadsMap[synthKey] = crearLead('', vta.cliente, vta.tlmk_canonico || '', vtaProy, '', vta.promotora || '', fVta);
      leadsMap[synthKey].origen_venta = 'REFERIDO/SIN CELULAR';
      leadsMap[synthKey].isContacto   = true;
      leadsMap[synthKey].isPotencial = true;
      leadsMap[synthKey].isCita       = true;
      celMatch = synthKey;
    }

    if (celMatch && leadsMap[celMatch]) {
      var leadV = leadsMap[celMatch];
      if (vta.es_negocio)    leadV.isNegocio    = true;
      if (vta.es_procesable) leadV.isProcesable  = true;
      if (!leadV.fecha_venta || fVta > leadV.fecha_venta) {
        leadV.fecha_venta    = fVta;
        leadV.modalidad_venta = vta.modalidad;
        leadV.tlmk_venta     = vta.tlmk_canonico;
      }
    }
  }

  // Log de diagnóstico post-match ventas
  var _negCount = 0, _procCount = 0, _vtaMatchCount = 0;
  for (var _ck in leadsMap) {
    if (leadsMap[_ck].isNegocio)    _negCount++;
    if (leadsMap[_ck].isProcesable) _procCount++;
  }
  for (var _vi = 0; _vi < ventas.length; _vi++) {
    var _vta = ventas[_vi];
    var _cm = _vta.celular || nombreIdx[norm_nombre(_vta.cliente)] || '';
    if (_cm && leadsMap[_cm]) _vtaMatchCount++;
  }
  etl_log('INFO', 'fase_embudoCompleto',
    'Post-match ventas: ' + _vtaMatchCount + '/' + ventas.length + ' matches' +
    ' | Negocios en mapa: ' + _negCount +
    ' | Procesables en mapa: ' + _procCount);

  if (!options.skipRptLeadTiempos) {
    fase_writeRptLeadTiempos(leadsMap, presencias);
  }

  /* ── PASO 4: Expandir (Unpivot) y escribir ───────────────────────────── */
  var HEADERS = [
    'CELULAR','NOMBRE_CLIENTE','ETAPA_EMBUDO','ORDEN_ETAPA',
    'ASESOR','PROYECTO','FUENTE','NOMBRE_OPC',
    'FECHA_ENTRADA_LEAD','ULTIMA_FECHA','ULTIMA_TIPIFICACION',
    'FECHA_PRESENCIA','TIPO_PRESENCIA',
    'FECHA_VENTA','MODALIDAD_VENTA','TLMK_VENTA','ORIGEN_VENTA',
    'TIPO_REPORTE_LEAD',     // ASIGNADOS | GESTION (global)
    'FECHA_ASIGNACION_LEAD'  // fecha primera asignación (Looker: filtrar "asignados en el período")
  ];

  var filas = [HEADERS];
  var cels  = Object.keys(leadsMap);

  for (var ci = 0; ci < cels.length; ci++) {
    var lead = leadsMap[cels[ci]];
    var tipoRptLead = (lead.tieneAsignacion === true) ? 'ASIGNADOS' : 'GESTION';
    var fechaAsigStr = lead.fecha_asignacion ? formatFecha(lead.fecha_asignacion) : '';
    var base = [
      lead.celular || '',
      lead.nombre,
      '', 0,                          // ETAPA_EMBUDO y ORDEN_ETAPA (se sobreescriben)
      lead.asesor,
      lead.proyecto,
      lead.fuente,
      lead.nombre_opc,
      lead.fecha_entrada,
      lead.ultima_fecha,
      lead.ultima_tipif,
      lead.fecha_presencia  || '',
      lead.tipo_presencia   || '',
      lead.fecha_venta      || '',
      lead.modalidad_venta  || '',
      lead.tlmk_venta       || '',
      lead.origen_venta     || '',
      tipoRptLead,
      fechaAsigStr
    ];

    // Etapa 1 — Todos los leads (100%)
    var r1 = base.slice(); r1[2] = '1. Leads'; r1[3] = 1;
    filas.push(r1);

    // Etapa 2 — Contactos Efectivos
    if (lead.isContacto) {
      var r2 = base.slice(); r2[2] = '2. Contactos Efectivos'; r2[3] = 2;
      filas.push(r2);
    }

    // Etapa 3 — Lead Potencial
    if (lead.isPotencial) {
      var r3 = base.slice(); r3[2] = '3. Leads Potenciales'; r3[3] = 3;
      filas.push(r3);
    }

    // Etapa 4 — Citas Agendadas (proxy CRM + confirmadas por Manifiesto)
    if (lead.isCita) {
      var r4 = base.slice(); r4[2] = '4. Citas Agendadas'; r4[3] = 4;
      filas.push(r4);
    }

    // Etapa 5 — Presencias Totales (del Manifiesto: Asistió)
    if (lead.isPresencia) {
      var r5 = base.slice(); r5[2] = '5. Presencias Totales'; r5[3] = 5;
      filas.push(r5);
    }

    // Etapa 6 — Presencias Tour Válidas (Resultado = TOUR)
    if (lead.isTour) {
      var r6 = base.slice(); r6[2] = '6. Presencias Tour (Válidas)'; r6[3] = 6;
      filas.push(r6);
    }

    // Etapa 7 — Negocios / Separaciones
    if (lead.isNegocio) {
      var r7 = base.slice(); r7[2] = '7. Negocios (Separaciones)'; r7[3] = 7;
      filas.push(r7);
    }

    // Etapa 8 — Procesables / Cierres
    if (lead.isProcesable) {
      var r8 = base.slice(); r8[2] = '8. Procesables (Cierres)'; r8[3] = 8;
      filas.push(r8);
    }
  }

  // Escribir sin sheet.clear(): clear() completo es muy lento en hojas grandes.
  etl_writeTable(sheet, filas);
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, filas.length - 1);

  etl_log('INFO','fase_embudoCompleto',
    '✅ ' + (filas.length - 1) + ' filas en DATA_EMBUDO_FULL (' + cels.length + ' leads únicos) | ' + (Date.now()-t0) + 'ms');
}


// ==========================================================================
// FASE 9 — RPT_ASESORES
// Genera métricas del embudo segmentadas por: ASESOR + FECHA + TIPO_REPORTE
//   TIPO_REPORTE = 'ASIGNADOS'  → leads que recibió ese asesor en esa fecha
//   TIPO_REPORTE = 'GESTION'    → leads que gestionó ese asesor en esa fecha
// Para cada grupo: cuántos llegaron a cada etapa del embudo (toda su historia)
// ==========================================================================

function fase_rptAsesores(factRows, presencias, ventas) {
  var t0 = Date.now();
  var ss = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.RPT_AS);

  /* ── PASO 1: Construir estado de embudo completo por CELULAR ─────────── */
  var estadoLeads = {};  // celular → { isContacto, isPotencial, isCita, isPresencia, isTour, isNegocio, isProcesable }

  for (var i = 0; i < factRows.length; i++) {
    var f    = factRows[i];
    var cel  = f.celular;
    if (!cel) continue;
    if (!estadoLeads[cel]) estadoLeads[cel] = estadoInicial();
    var tipif = f.tipificacion;
    if (CFG.EMBUDO.INVALIDOS.indexOf(tipif) !== -1)          estadoLeads[cel].isNoEfectivo  = true;
    if (tipif && CFG.EMBUDO.INVALIDOS.indexOf(tipif) === -1) estadoLeads[cel].isContacto    = true;
    if (CFG.EMBUDO.POTENCIALES.indexOf(tipif) !== -1)        estadoLeads[cel].isPotencial   = true;
    if (CFG.EMBUDO.CITAS.indexOf(tipif)       !== -1)        estadoLeads[cel].isCita        = true;
    if (CFG.EMBUDO.DATOS_FALSOS.indexOf(tipif) !== -1)       estadoLeads[cel].tieneDatoFalso = true;
  }
  for (var p = 0; p < presencias.length; p++) {
    var pres = presencias[p];
    var celP = pres.celular;
    if (!celP || celP.length < 9) continue;
    if (!estadoLeads[celP]) estadoLeads[celP] = estadoInicial();
    if (pres.es_presencia) estadoLeads[celP].isPresencia = true;
    if (pres.es_tour)      estadoLeads[celP].isTour      = true;
  }

  // Match ventas: 1) por CELULAR; 2) por nombre en FACT+Manifiesto; 3) por Manifiesto (puente)
  var nombreIdx2 = {};
  for (var fj = 0; fj < factRows.length; fj++) {
    var celFj = factRows[fj].celular;
    if (!celFj) continue;
    var nn2 = norm_nombre(factRows[fj].nombre_cliente);
    if (nn2.length > 3 && !nombreIdx2[nn2]) nombreIdx2[nn2] = celFj;
  }
  for (var pm2 = 0; pm2 < presencias.length; pm2++) {
    var pres2 = presencias[pm2];
    if (!pres2.celular || pres2.celular.length < 9) continue;
    var nnP = norm_nombre(pres2.cliente_raw);
    if (nnP.length > 3 && !nombreIdx2[nnP]) nombreIdx2[nnP] = pres2.celular;
  }
  var manifestIdx2 = {};
  for (var pm3 = 0; pm3 < presencias.length; pm3++) {
    var pres3 = presencias[pm3];
    if (!pres3.celular || pres3.celular.length < 9) continue;
    var nnM = norm_nombre(pres3.cliente_raw);
    if (nnM.length < 3) continue;
    if (!manifestIdx2[nnM]) manifestIdx2[nnM] = [];
    manifestIdx2[nnM].push({
      celular: pres3.celular,
      proyecto: String(pres3.proyecto || '').toUpperCase().trim(),
      asesor: pres3.asesor_canonico,
      nombre_opc_norm: norm_nombre(pres3.nombre_opc),
      fuente_norm: pres3.fuente_norm,
      fecha_evento: toDate(pres3.fecha_evento)
    });
  }
  // Índice fecha+asesor+proyecto para rptAsesores (mismo patrón que embudoCompleto)
  var fechaAsesorPrIdx2 = {};
  for (var pfap2 = 0; pfap2 < presencias.length; pfap2++) {
    var presFAP2 = presencias[pfap2];
    if (!presFAP2.celular || presFAP2.celular.length < 9) continue;
    var fEvtFAP2 = toDate(presFAP2.fecha_evento);
    if (!fEvtFAP2 || fEvtFAP2.getTime() === 0) continue;
    var kFAP2 = formatFecha(fEvtFAP2) + '\u00a7' +
                (presFAP2.asesor_canonico || '') + '\u00a7' +
                String(presFAP2.proyecto || '').toUpperCase().trim();
    if (!fechaAsesorPrIdx2[kFAP2]) fechaAsesorPrIdx2[kFAP2] = [];
    fechaAsesorPrIdx2[kFAP2].push({
      celular: presFAP2.celular,
      nombre_opc_norm: norm_nombre(presFAP2.nombre_opc),
      fuente_norm: presFAP2.fuente_norm
    });
  }

  for (var vv = 0; vv < ventas.length; vv++) {
    var vta2  = ventas[vv];
    var nN2   = norm_nombre(vta2.cliente);
    var vtaPy = String(vta2.proyecto || '').toUpperCase().trim();
    var fVta2 = toDate(vta2.fecha_compra);
    var cm2   = vta2.celular || '';
    if (!cm2) cm2 = nombreIdx2[nN2];
    if (cm2 && !estadoLeads[cm2]) estadoLeads[cm2] = estadoInicial();
    if (!cm2 && manifestIdx2[nN2]) {
      var cand = manifestIdx2[nN2];
      var best2 = null; var bestScore2 = -1;
      for (var ci2 = 0; ci2 < cand.length; ci2++) {
        var c2 = cand[ci2];
        var score2 = 0;
        if (c2.asesor && vta2.tlmk_canonico && c2.asesor === vta2.tlmk_canonico) score2 += 5;
        if (c2.nombre_opc_norm && vta2.promotora_norm && c2.nombre_opc_norm === vta2.promotora_norm) score2 += 4;
        if (c2.fecha_evento && fVta2) {
          var diffDias2 = (fVta2.getTime() - c2.fecha_evento.getTime()) / 86400000;
          if (diffDias2 >= 0 && diffDias2 <= 3)  score2 += 6;
          else if (diffDias2 > 3 && diffDias2 <= 14) score2 += 3;
          else if (diffDias2 > 14 && diffDias2 <= 90) score2 += 1;
        }
        if (vta2.origen_norm && c2.fuente_norm && vta2.origen_norm === c2.fuente_norm) score2 += 2;
        if (c2.proyecto === vtaPy) score2 += 1; // tiebreak suave
        if (score2 > bestScore2) { bestScore2 = score2; best2 = c2; }
      }
      cm2 = (best2 || cand[0]).celular;
      if (!estadoLeads[cm2]) estadoLeads[cm2] = estadoInicial();
    }
    // Match 4: fecha+asesor+proyecto (para ventas sin celular y sin nombre en manifiesto)
    if (!cm2 && vta2.tlmk_canonico && fVta2 && fVta2.getTime() > 0) {
      var bestFAP2 = null, bestFAPScore2 = -1;
      for (var delta2 = 0; delta2 <= 3; delta2++) {
        for (var sign2 = -1; sign2 <= 1; sign2 += 2) {
          var fBusq2 = new Date(fVta2.getTime() + delta2 * sign2 * 86400000);
          var kFAP2s = formatFecha(fBusq2) + '\u00a7' + vta2.tlmk_canonico + '\u00a7' + vtaPy;
          if (!fechaAsesorPrIdx2[kFAP2s]) continue;
          var candFAP2 = fechaAsesorPrIdx2[kFAP2s];
          for (var cfap2 = 0; cfap2 < candFAP2.length; cfap2++) {
            var cFAP2 = candFAP2[cfap2];
            var scoreFAP2 = (6 - delta2 * 2);
            if (cFAP2.nombre_opc_norm && vta2.promotora_norm &&
                cFAP2.nombre_opc_norm === vta2.promotora_norm) scoreFAP2 += 4;
            if (vta2.origen_norm && cFAP2.fuente_norm &&
                vta2.origen_norm === cFAP2.fuente_norm) scoreFAP2 += 2;
            if (scoreFAP2 > bestFAPScore2) { bestFAPScore2 = scoreFAP2; bestFAP2 = cFAP2; }
          }
        }
        if (delta2 === 0) break;
      }
      if (bestFAP2) {
        cm2 = bestFAP2.celular;
        if (!estadoLeads[cm2]) estadoLeads[cm2] = estadoInicial();
      }
    }
    if (cm2 && estadoLeads[cm2]) {
      if (vta2.es_negocio)    estadoLeads[cm2].isNegocio    = true;
      if (vta2.es_procesable) estadoLeads[cm2].isProcesable = true;
    }
  }

  /* ── PASO 2: Agrupar FACT por ASESOR + FECHA + TIPO_REPORTE ─────────── */
  // Estructura: rptMap[asesor|fecha|tipo] = Set de celulares
  var rptMap = {};

  for (var fi = 0; fi < factRows.length; fi++) {
    var ff      = factRows[fi];
    if (!ff.celular || !ff.asesor_nombre) continue;
    var tipo    = ff.tipo_accion;
    var fechaFi = toDate(ff.fecha_interaccion);
    var fechaStr = formatFecha(fechaFi);
    var asesor   = ff.asesor_nombre.trim();

    var tipoRpt = null;
    if (CFG.TIPO_ASIGNACION.indexOf(tipo) !== -1) tipoRpt = 'ASIGNADOS';
    else if (CFG.TIPO_GESTION.indexOf(tipo) !== -1) tipoRpt = 'GESTION';
    if (!tipoRpt) continue;

    var key = asesor + '§' + fechaStr + '§' + tipoRpt;
    if (!rptMap[key]) {
      rptMap[key] = {
        asesor    : asesor,
        fecha     : fechaFi,
        fecha_str : fechaStr,
        tipo      : tipoRpt,
        cels      : {}    // usar objeto como Set (más compatible con GAS)
      };
    }
    rptMap[key].cels[ff.celular] = true;
  }

  /* ── PASO 3: Calcular métricas por grupo ────────────────────────────── */
  var HEADERS = [
    'ASESOR','FECHA','TIPO_REPORTE',
    'LEADS',
    'CONTACTOS_NO_EFECTIVOS','CONTACTOS_EFECTIVOS',
    'LEADS_POTENCIALES','CITAS_AGENDADAS',
    'PRESENCIAS_TOTALES','TOURS_VALIDOS',
    'NEGOCIOS','PROCESABLES',
    'TASA_CONTACTO_EFECTIVO','TASA_POTENCIAL','TASA_CITA',
    'TASA_PRESENCIA','TASA_TOUR','TASA_CIERRE'
  ];

  var filas = [HEADERS];

  for (var key in rptMap) {
    var rpt    = rptMap[key];
    var celArr = Object.keys(rpt.cels);
    var leads  = celArr.length;
    var cnt    = { noef: 0, c: 0, po: 0, ci: 0, pr: 0, to: 0, ne: 0, proc: 0 };

    for (var cc = 0; cc < celArr.length; cc++) {
      var est = estadoLeads[celArr[cc]];
      if (!est) continue;
      if (est.isNoEfectivo)  cnt.noef++;
      if (est.isContacto)    cnt.c++;
      if (est.isPotencial)   cnt.po++;
      if (est.isCita)        cnt.ci++;
      if (est.isPresencia)   cnt.pr++;
      if (est.isTour)        cnt.to++;
      if (est.isNegocio)     cnt.ne++;
      if (est.isProcesable)  cnt.proc++;
    }

    var pct = function(n) { return leads > 0 ? parseFloat((n / leads).toFixed(4)) : 0; };

    filas.push([
      rpt.asesor, rpt.fecha, rpt.tipo,
      leads,
      cnt.noef, cnt.c,
      cnt.po, cnt.ci,
      cnt.pr, cnt.to,
      cnt.ne, cnt.proc,
      pct(cnt.c), pct(cnt.po), pct(cnt.ci),
      pct(cnt.pr), pct(cnt.to), pct(cnt.proc)
    ]);
  }

  /* ── PASO 4: BLOQUE VENTAS POR ASESOR Y MES (métricas adicionales) ──── */
  // Permite calcular conversión total de leads a cierres por TLMK
  var vtaHeaders = [
    '', // separador visual
    'ASESOR_TLMK','MES','TOTAL_NEGOCIOS','TOTAL_PROCESABLES',
    'VALOR_SEPARACIONES','VALOR_PROCESABLES','RECAUDO_HOY','VALOR_TOTAL'
  ];

  // Todas las filas deben tener HEADERS.length columnas para setValues
  var padRow = function(arr, len) {
    var r = arr.slice();
    while (r.length < len) r.push('');
    return r;
  };

  filas.push(padRow([''], HEADERS.length)); // fila vacía como separador
  filas.push(padRow(vtaHeaders, HEADERS.length));

  var vtaAsesorMes = {};
  for (var vt = 0; vt < ventas.length; vt++) {
    var venta = ventas[vt];
    var kv    = (venta.tlmk_canonico || 'SIN_TLMK') + '§' + (venta.mes || 'SIN_MES');
    if (!vtaAsesorMes[kv]) {
      vtaAsesorMes[kv] = {
        asesor: venta.tlmk_canonico, mes: venta.mes,
        negocios: 0, procesables: 0, valSep: 0, valProc: 0, recaudo: 0
      };
    }
    var grp = vtaAsesorMes[kv];
    if (venta.es_negocio) {
      grp.negocios++;
      grp.valSep += parseFloat(String(venta.precio_venta).replace(/[^0-9.]/g,'')) || 0;
    }
    if (venta.es_procesable) {
      grp.procesables++;
      grp.valProc += parseFloat(String(venta.precio_venta).replace(/[^0-9.]/g,'')) || 0;
    }
    grp.recaudo += (venta.recaudo_val != null ? venta.recaudo_val : (parseFloat(String(venta.hoy).replace(/[^0-9.,]/g,'').replace(',','.')) || 0));
  }

  for (var kv2 in vtaAsesorMes) {
    var g = vtaAsesorMes[kv2];
    filas.push(padRow(['', g.asesor, g.mes, g.negocios, g.procesables,
                       g.valSep, g.valProc, g.recaudo, g.valSep + g.valProc], HEADERS.length));
  }

  /* ── ESCRIBIR ─────────────────────────────────────────────────────────── */
  etl_writeTable(sheet, filas);

  // Formato encabezado principal
  sheet.getRange(1, 1, 1, HEADERS.length)
    .setBackground('#1e3a5f').setFontColor('white').setFontWeight('bold');
  // Formato encabezado de ventas
  var vtaHeadRow = filas.length - Object.keys(vtaAsesorMes).length - 1;
  if (vtaHeadRow > 1) {
    sheet.getRange(vtaHeadRow, 1, 1, vtaHeaders.length)
      .setBackground('#0f4c75').setFontColor('white').setFontWeight('bold');
  }

  sheet.setFrozenRows(1);
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, filas.length - 1);

  etl_log('INFO','fase_rptAsesores',
    '✅ RPT_ASESORES: ' + (filas.length - 1) + ' filas | ' + (Date.now()-t0) + 'ms');
}


// ==========================================================================
// FASE 10 — RPT_OPC
// Tabla de calidad de datos por OPC (personal de captación en campo).
//
// Lógica:
//   - Un lead pertenece a un OPC si su fuente_norm = 'OPC' y nombre_opc != ''
//   - El OPC se determina desde el TIPO_ACCION 'ASIGNACION'/'ASIGNACION_MANUAL'
//     (primera asignación del lead → ese es su OPC de origen)
//   - Para leads sin asignación (migrados), se usa el nombre_opc del registro más
//     antiguo disponible en FACT_INTERACCIONES
//   - DATOS_FALSOS = leads donde alguna vez se tipificó como 'DF'
//   - La tabla permite ver la calidad del dato captado por cada OPC:
//     cuántos leads captó, cuántos resultaron válidos, cuántos avanzaron
//     en el embudo hasta cierre
// ==========================================================================

/**
 * Genera RPT_OPC: métricas del embudo completo + datos falsos agrupado por OPC.
 * Estructura de salida: una fila por OPC (aggregated), apta para tabla en Looker.
 *
 * @param {Array} factRows    - Datos de STG/FACT_INTERACCIONES
 * @param {Array} presencias  - Datos de STG_PRESENCIAS
 * @param {Array} ventas      - Datos de STG_VENTAS
 */
function fase_rptOPC(factRows, presencias, ventas) {
  var t0    = Date.now();
  var ss    = getSpreadsheetDestino();
  var sheet = getOrCreateSheet(ss, CFG.OUT.RPT_OPC);

  /* ── PASO 1: Determinar el OPC de origen de cada celular ─────────────────
   * Prioridad: registro de ASIGNACION más antiguo con nombre_opc definido.
   * Fallback: cualquier registro con fuente_norm='OPC' y nombre_opc definido.
   * ───────────────────────────────────────────────────────────────────────── */
  var celToOPC     = {};  // celular → { nombre_opc, proyecto, fecha_asignacion }
  var celToOPCFech = {};  // celular → fecha más antigua con nombre_opc (para determinar primero)

  for (var i = 0; i < factRows.length; i++) {
    var f = factRows[i];
    if (!f.celular || !f.nombre_opc || f.nombre_opc.trim() === '') continue;

    var fechaF  = toDate(f.fecha_interaccion);
    var esAsig  = CFG.TIPO_ASIGNACION.indexOf(f.tipo_accion) !== -1;
    var cel     = f.celular;

    // Priorizar registros de ASIGNACION (son la fuente más confiable del OPC)
    if (!celToOPC[cel]) {
      celToOPC[cel]     = { nombre_opc: f.nombre_opc, proyecto: f.proyecto, fecha_asig: fechaF };
      celToOPCFech[cel] = { fecha: fechaF, esAsig: esAsig };
    } else {
      var existing = celToOPCFech[cel];
      // Reemplazar si: el nuevo es ASIGNACION y el existente no lo era,
      // O si ambos son del mismo tipo y el nuevo es más antiguo
      var reemplazar = (!existing.esAsig && esAsig) ||
                       (existing.esAsig === esAsig && fechaF < existing.fecha);
      if (reemplazar) {
        celToOPC[cel]     = { nombre_opc: f.nombre_opc, proyecto: f.proyecto, fecha_asig: fechaF };
        celToOPCFech[cel] = { fecha: fechaF, esAsig: esAsig };
      }
    }
  }

  // Complementar celToOPC con Manifiesto (leads que no están en FACT pero sí en Manifiesto)
  for (var pmOp = 0; pmOp < presencias.length; pmOp++) {
    var presOp = presencias[pmOp];
    if (!presOp.celular || presOp.celular.length < 9) continue;
    if (!presOp.nombre_opc || presOp.nombre_opc.trim() === '') continue;
    var celOp = presOp.celular;
    if (celToOPC[celOp]) continue;
    var fEvOp = toDate(presOp.fecha_evento);
    celToOPC[celOp] = { nombre_opc: presOp.nombre_opc, proyecto: presOp.proyecto, fecha_asig: fEvOp };
  }

  /* ── PASO 2: Construir estado del embudo por celular (misma lógica que RPT_ASESORES) */
  var estadoLeads = {};

  for (var fi = 0; fi < factRows.length; fi++) {
    var ff   = factRows[fi];
    var celF = ff.celular;
    if (!celF) continue;
    if (!estadoLeads[celF]) estadoLeads[celF] = estadoInicial();
    var tipif = ff.tipificacion;
    if (CFG.EMBUDO.INVALIDOS.indexOf(tipif) !== -1)          estadoLeads[celF].isNoEfectivo  = true;
    if (tipif && CFG.EMBUDO.INVALIDOS.indexOf(tipif) === -1) estadoLeads[celF].isContacto    = true;
    if (CFG.EMBUDO.POTENCIALES.indexOf(tipif) !== -1)        estadoLeads[celF].isPotencial   = true;
    if (CFG.EMBUDO.CITAS.indexOf(tipif) !== -1)              estadoLeads[celF].isCita        = true;
    if (CFG.EMBUDO.DATOS_FALSOS.indexOf(tipif) !== -1)       estadoLeads[celF].tieneDatoFalso = true;
  }
  for (var pp = 0; pp < presencias.length; pp++) {
    var pres = presencias[pp];
    var celP = pres.celular;
    if (!celP || celP.length < 9) continue;
    if (!estadoLeads[celP]) estadoLeads[celP] = estadoInicial();
    if (pres.es_presencia) estadoLeads[celP].isPresencia = true;
    if (pres.es_tour)      estadoLeads[celP].isTour      = true;
  }

  // Match ventas: 1) por CELULAR; 2) por nombre en FACT+Manifiesto; 3) por Manifiesto (puente)
  var nombreIdxOpc = {};
  for (var fj = 0; fj < factRows.length; fj++) {
    var celFj = factRows[fj].celular;
    if (!celFj) continue;
    var nnOpc = norm_nombre(factRows[fj].nombre_cliente);
    if (nnOpc.length > 3 && !nombreIdxOpc[nnOpc]) nombreIdxOpc[nnOpc] = celFj;
  }
  for (var pmOp2 = 0; pmOp2 < presencias.length; pmOp2++) {
    var presOp2 = presencias[pmOp2];
    if (!presOp2.celular || presOp2.celular.length < 9) continue;
    var nnOpP = norm_nombre(presOp2.cliente_raw);
    if (nnOpP.length > 3 && !nombreIdxOpc[nnOpP]) nombreIdxOpc[nnOpP] = presOp2.celular;
  }
  var manifestIdxOpc = {};
  for (var pmOp3 = 0; pmOp3 < presencias.length; pmOp3++) {
    var presOp3 = presencias[pmOp3];
    if (!presOp3.celular || presOp3.celular.length < 9) continue;
    var nnOpM = norm_nombre(presOp3.cliente_raw);
    if (nnOpM.length < 3) continue;
    if (!manifestIdxOpc[nnOpM]) manifestIdxOpc[nnOpM] = [];
    manifestIdxOpc[nnOpM].push({
      celular: presOp3.celular,
      proyecto: String(presOp3.proyecto || '').toUpperCase().trim(),
      asesor: presOp3.asesor_canonico,
      nombre_opc_norm: norm_nombre(presOp3.nombre_opc),
      fuente_norm: presOp3.fuente_norm,
      fecha_evento: toDate(presOp3.fecha_evento)
    });
  }
  // Índice fecha+asesor+proyecto para rptOPC
  var fechaAsesorPrIdxO = {};
  for (var pfapO = 0; pfapO < presencias.length; pfapO++) {
    var presFAPO = presencias[pfapO];
    if (!presFAPO.celular || presFAPO.celular.length < 9) continue;
    var fEvtFAPO = toDate(presFAPO.fecha_evento);
    if (!fEvtFAPO || fEvtFAPO.getTime() === 0) continue;
    var kFAPO = formatFecha(fEvtFAPO) + '\u00a7' +
                (presFAPO.asesor_canonico || '') + '\u00a7' +
                String(presFAPO.proyecto || '').toUpperCase().trim();
    if (!fechaAsesorPrIdxO[kFAPO]) fechaAsesorPrIdxO[kFAPO] = [];
    fechaAsesorPrIdxO[kFAPO].push({
      celular: presFAPO.celular,
      nombre_opc_norm: norm_nombre(presFAPO.nombre_opc),
      fuente_norm: presFAPO.fuente_norm
    });
  }

  var unmatchedVentasOpc = []; // ventas sin match para agrupar como REFERIDO / DIRECTO
  for (var vv = 0; vv < ventas.length; vv++) {
    var vtaOpc = ventas[vv];
    var nNOpc  = norm_nombre(vtaOpc.cliente);
    var vtaPyO = String(vtaOpc.proyecto || '').toUpperCase().trim();
    var fVtaOp = toDate(vtaOpc.fecha_compra);
    var cmOpc  = vtaOpc.celular || '';
    if (!cmOpc) cmOpc = nombreIdxOpc[nNOpc];
    if (cmOpc && !estadoLeads[cmOpc]) estadoLeads[cmOpc] = estadoInicial();
    if (!cmOpc && manifestIdxOpc[nNOpc]) {
      var candO = manifestIdxOpc[nNOpc];
      var bestO = null; var bestScoreO = -1;
      for (var ciO = 0; ciO < candO.length; ciO++) {
        var cO = candO[ciO];
        var scoreO = 0;
        if (cO.asesor && vtaOpc.tlmk_canonico && cO.asesor === vtaOpc.tlmk_canonico) scoreO += 5;
        if (cO.nombre_opc_norm && vtaOpc.promotora_norm && cO.nombre_opc_norm === vtaOpc.promotora_norm) scoreO += 4;
        if (cO.fecha_evento && fVtaOp) {
          var diffDiasO = (fVtaOp.getTime() - cO.fecha_evento.getTime()) / 86400000;
          if (diffDiasO >= 0 && diffDiasO <= 3)  scoreO += 6;
          else if (diffDiasO > 3 && diffDiasO <= 14) scoreO += 3;
          else if (diffDiasO > 14 && diffDiasO <= 90) scoreO += 1;
        }
        if (vtaOpc.origen_norm && cO.fuente_norm && vtaOpc.origen_norm === cO.fuente_norm) scoreO += 2;
        if (cO.proyecto === vtaPyO) scoreO += 1; // tiebreak suave
        if (scoreO > bestScoreO) { bestScoreO = scoreO; bestO = cO; }
      }
      cmOpc = (bestO || candO[0]).celular;
      if (!estadoLeads[cmOpc]) estadoLeads[cmOpc] = estadoInicial();
    }
    // Match 4: fecha+asesor+proyecto (para ventas sin celular y sin nombre en manifiesto)
    if (!cmOpc && vtaOpc.tlmk_canonico && fVtaOp && fVtaOp.getTime() > 0) {
      var bestFAPO = null, bestFAPScoreO = -1;
      for (var deltaO = 0; deltaO <= 3; deltaO++) {
        for (var signO = -1; signO <= 1; signO += 2) {
          var fBusqO = new Date(fVtaOp.getTime() + deltaO * signO * 86400000);
          var kFAPOs = formatFecha(fBusqO) + '\u00a7' + vtaOpc.tlmk_canonico + '\u00a7' + vtaPyO;
          if (!fechaAsesorPrIdxO[kFAPOs]) continue;
          var candFAPO = fechaAsesorPrIdxO[kFAPOs];
          for (var cfapO = 0; cfapO < candFAPO.length; cfapO++) {
            var cFAPO = candFAPO[cfapO];
            var scoreFAPO = (6 - deltaO * 2);
            if (cFAPO.nombre_opc_norm && vtaOpc.promotora_norm &&
                cFAPO.nombre_opc_norm === vtaOpc.promotora_norm) scoreFAPO += 4;
            if (vtaOpc.origen_norm && cFAPO.fuente_norm &&
                vtaOpc.origen_norm === cFAPO.fuente_norm) scoreFAPO += 2;
            if (scoreFAPO > bestFAPScoreO) { bestFAPScoreO = scoreFAPO; bestFAPO = cFAPO; }
          }
        }
        if (deltaO === 0) break;
      }
      if (bestFAPO) {
        cmOpc = bestFAPO.celular;
        if (!estadoLeads[cmOpc]) estadoLeads[cmOpc] = estadoInicial();
      }
    }
    if (cmOpc && estadoLeads[cmOpc]) {
      if (vtaOpc.es_negocio)    estadoLeads[cmOpc].isNegocio    = true;
      if (vtaOpc.es_procesable) estadoLeads[cmOpc].isProcesable = true;
    } else if (!cmOpc) {
      unmatchedVentasOpc.push(vv);
    }
  }

  // Ventas sin match (sin CELULAR / sin relación OPC): se agrupan como REFERIDO / DIRECTO para que los totales cuadren.
  for (var u = 0; u < unmatchedVentasOpc.length; u++) {
    var vvU = unmatchedVentasOpc[u];
    var vtaU = ventas[vvU];
    var synthCel = 'REF_OPC_' + vvU;
    celToOPC[synthCel] = { nombre_opc: 'REFERIDO / DIRECTO', proyecto: 'N/A', fecha_asig: toDate(vtaU.fecha_compra) };
    estadoLeads[synthCel] = estadoInicial();
    if (vtaU.es_negocio)    estadoLeads[synthCel].isNegocio    = true;
    if (vtaU.es_procesable) estadoLeads[synthCel].isProcesable = true;
  }

  /* ── PASO 3: Agrupar celulares por OPC + Proyecto ────────────────────────
   * Clave: nombre_opc + '§' + proyecto
   * De esta forma, en Looker puedes filtrar por OPC individual
   * y ver en qué proyectos captó leads.
   * ───────────────────────────────────────────────────────────────────────── */
  var opcGrupos = {};  // key → { nombre_opc, proyecto, cels: {}, fechas: [] }

  for (var cel2 in celToOPC) {
    var opcInfo = celToOPC[cel2];
    var opcNom  = String(opcInfo.nombre_opc || '').trim().toUpperCase();
    var opcProy = String(opcInfo.proyecto   || '').trim().toUpperCase();
    if (!opcNom) continue;

    var key = opcNom + '§' + opcProy;
    if (!opcGrupos[key]) {
      opcGrupos[key] = {
        nombre_opc : opcNom,
        proyecto   : opcProy,
        cels       : {}
      };
    }
    opcGrupos[key].cels[cel2] = true;
  }

  /* ── PASO 4: Calcular métricas y escribir ───────────────────────────────── */
  var HEADERS = [
    'NOMBRE_OPC','PROYECTO',
    'LEADS_CAPTADOS',
    'CONTACTOS_NO_EFECTIVOS','CONTACTOS_EFECTIVOS',
    'LEADS_POTENCIALES','CITAS_AGENDADAS',
    'PRESENCIAS_TOTALES','TOURS_VALIDOS',
    'NEGOCIOS','PROCESABLES',
    'DATOS_FALSOS',
    'TASA_EFECTIVO','TASA_POTENCIAL','TASA_CITA',
    'TASA_PRESENCIA','TASA_TOUR','TASA_CIERRE',
    'TASA_DATO_FALSO'
  ];

  var filas = [HEADERS];

  for (var gKey in opcGrupos) {
    var grp    = opcGrupos[gKey];
    var celArr = Object.keys(grp.cels);
    var leads  = celArr.length;
    var cnt    = {
      noef: 0, ef: 0, po: 0, ci: 0,
      pr: 0, to: 0, ne: 0, proc: 0, df: 0
    };

    for (var cci = 0; cci < celArr.length; cci++) {
      var est = estadoLeads[celArr[cci]];
      if (!est) continue;
      if (est.isNoEfectivo)    cnt.noef++;
      if (est.isContacto)      cnt.ef++;
      if (est.isPotencial)     cnt.po++;
      if (est.isCita)          cnt.ci++;
      if (est.isPresencia)     cnt.pr++;
      if (est.isTour)          cnt.to++;
      if (est.isNegocio)       cnt.ne++;
      if (est.isProcesable)    cnt.proc++;
      if (est.tieneDatoFalso)  cnt.df++;
    }

    var pct2 = function(n) { return leads > 0 ? parseFloat((n / leads).toFixed(4)) : 0; };

    filas.push([
      grp.nombre_opc,
      grp.proyecto,
      leads,
      cnt.noef, cnt.ef,
      cnt.po, cnt.ci,
      cnt.pr, cnt.to,
      cnt.ne, cnt.proc,
      cnt.df,
      pct2(cnt.ef),   pct2(cnt.po), pct2(cnt.ci),
      pct2(cnt.pr),   pct2(cnt.to), pct2(cnt.proc),
      pct2(cnt.df)
    ]);
  }

  // Ordenar por LEADS_CAPTADOS descendente (mejor legibilidad)
  var header = filas.shift();
  filas.sort(function(a, b) { return (b[2] || 0) - (a[2] || 0); });
  filas.unshift(header);

  // Escribir
  etl_writeTable(sheet, filas);
  sheet.getRange(1, 1, 1, HEADERS.length)
    .setBackground('#1a3a2a').setFontColor('white').setFontWeight('bold');
  sheet.setFrozenRows(1);
  etl_maybeAutoResizeColumns(sheet, HEADERS.length, filas.length - 1);

  etl_log('INFO','fase_rptOPC',
    '✅ RPT_OPC: ' + (filas.length - 1) + ' grupos OPC×Proyecto | ' + (Date.now()-t0) + 'ms');
}


// ==========================================================================
// UTILIDADES DE NORMALIZACIÓN Y MATCHING
// ==========================================================================

/** Normaliza un número telefónico a 9 dígitos peruanos (9XXXXXXXXX).
 *  Acepta: 9 dígitos, 10 con 0 inicial (0987654321), 11 con 51 (51987654321). */
function norm_tel(phone) {
  if (phone === null || phone === undefined) return '';
  var p = String(phone).replace(/\D/g, '');
  if (p.length === 11 && p.substring(0, 2) === '51') p = p.substring(2);
  if (p.length === 10 && p.charAt(0) === '0') p = p.substring(1);
  if (p.length === 9 && p.charAt(0) === '9') return p;
  return '';
}

/** Normaliza un nombre: mayúsculas, sin símbolos, espacios simples.
 *  Quita prefijos SR/SRA, normaliza Ñ→N para mejor matching CRM-Ventas. */
function norm_nombre(nombre) {
  if (!nombre) return '';
  var s = String(nombre)
    .toUpperCase().trim()
    .replace(/^S[RRA\.]+\s*/i, '')   // quitar SR, SRA, SR., SRA.
    .replace(/Ñ/g, 'N')
    .replace(/[^A-ZÁÉÍÓÚN\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return s;
}

/**
 * Parsea el campo FUENTE del Manifiesto y devuelve { normalizada, nombreOPC }.
 * Formatos detectados: "OPC YOSELIN", "WHATSAPP", "WSP-FORM", "FORM", "CC", etc.
 */
function parsear_fuente(fuenteRaw) {
  if (!fuenteRaw) return { normalizada: 'SIN_FUENTE', nombreOPC: '' };
  var raw = String(fuenteRaw).toUpperCase().trim();

  if (raw.indexOf('OPC') !== -1) {
    var partes = raw.split(/\s+/);
    var opcNombre = partes.slice(1).join(' ').trim();
    return { normalizada: 'OPC', nombreOPC: opcNombre };
  }
  if (raw.indexOf('WHATSAPP') !== -1 || raw === 'WSP' || raw.indexOf('WSP-FORM') !== -1) {
    return { normalizada: 'META', nombreOPC: '' };
  }
  if (raw === 'FORM' || raw.indexOf('FORM') !== -1) {
    return { normalizada: 'META', nombreOPC: '' };
  }
  if (raw === 'REFERIDO') return { normalizada: 'REFERIDO', nombreOPC: '' };
  if (raw === 'GOOGLE_ADS' || raw.indexOf('GOOGLE') !== -1) {
    return { normalizada: 'GOOGLE_ADS', nombreOPC: '' };
  }
  // Valores que son estados de confirmación, no fuentes reales
  if (['CC','HP','HH','HXH','VLL','PENDIENTE',''].indexOf(raw) !== -1) {
    return { normalizada: 'SIN_FUENTE', nombreOPC: '' };
  }
  return { normalizada: raw, nombreOPC: '' };
}

/** Fuentes que vienen del CRM y deben reemplazarse por la fuente del Manifiesto cuando exista colisión. */
var FUENTES_CRM_REEMPLAZABLES = ['REMARCADO', 'REASIGNADO', 'REFERIDO'];

/** Fuentes del Manifiesto que se priorizan al colisionar con CRM (OPC, META, WSP-FORM, WSP). */
var FUENTES_MANIFIESTO_PRIORITARIAS = ['OPC', 'META', 'WSP-FORM', 'WSP'];

function esFuenteCrmReemplazable(fuente) {
  if (!fuente) return false;
  return FUENTES_CRM_REEMPLAZABLES.indexOf(String(fuente).toUpperCase().trim()) !== -1;
}

function esFuenteManifiestoPrioritaria(fuente) {
  if (!fuente) return false;
  return FUENTES_MANIFIESTO_PRIORITARIAS.indexOf(String(fuente).toUpperCase().trim()) !== -1;
}

/** Si el lead tiene fuente CRM reemplazable (REMARCADO/REASIGNADO/REFERIDO), actualiza lead.fuente con la del Manifiesto cuando haya una fuente válida (no vacía, no SIN_FUENTE). Así se prioriza la fuente de la tabla de presencias y no se pierden conteos en reportes. */
function actualizarFuenteDesdeManifiesto(lead, fuenteManifiesto) {
  if (!lead || !esFuenteCrmReemplazable(lead.fuente)) return;
  var fn = String(fuenteManifiesto || '').trim();
  if (!fn || fn === 'SIN_FUENTE') return;
  lead.fuente = fn;
}

/**
 * Mapea un nombre de asesor RAW a su nombre canónico usando DIM_ASESORES.
 * Si no encuentra match, lo registra en PENDIENTES_MAPEO y retorna el raw.
 * NOTA: Esta función se llama durante la lectura del Manifiesto, 
 *       por eso lee la hoja DIM_ASESORES en batch la primera vez (caché en propiedad).
 */
var _dimAsesorCache = null;

function mapear_asesor(asesorRaw) {
  if (!asesorRaw || asesorRaw.trim() === '') return '';
  var rawNorm = asesorRaw.toUpperCase().trim();

  // Guard: detecta fechas serializadas (Date.toString o mm/dd/yyyy) y las ignora.
  // Ocurre cuando un índice de columna apunta a una celda de tipo fecha en lugar de un nombre.
  var esFormatoFecha =
    /\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b/.test(rawNorm) && /\b\d{4}\b/.test(rawNorm) ||
    /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(rawNorm.trim());
  if (esFormatoFecha) return '';

  // Cargar cache si no existe
  if (!_dimAsesorCache) {
    var ss    = getSpreadsheetDestino();
    var sheet = ss.getSheetByName(CFG.OUT.DIM_AS);
    _dimAsesorCache = [];
    if (sheet && sheet.getLastRow() > 1) {
      var data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 8).getValues();
      for (var i = 0; i < data.length; i++) {
        var aliases = [];
        for (var col = 1; col <= 5; col++) {  // cols 2-6 son NOMBRE_CANONICO y ALIAS_1..4
          var a = String(data[i][col] || '').toUpperCase().trim();
          if (a) aliases.push(a);
        }
        _dimAsesorCache.push({ canonico: String(data[i][1] || '').trim(), aliases: aliases });
      }
    }
  }

  // Buscar match
  for (var j = 0; j < _dimAsesorCache.length; j++) {
    var entry = _dimAsesorCache[j];
    for (var a = 0; a < entry.aliases.length; a++) {
      var alias = entry.aliases[a];
      if (!alias) continue;
      // Match exacto o contenido bidireccional
      if (rawNorm === alias || rawNorm.indexOf(alias) !== -1 || alias.indexOf(rawNorm) !== -1) {
        return entry.canonico;
      }
    }
  }

  // No encontrado → registrar en PENDIENTES_MAPEO
  registrar_pendiente('ASESOR', asesorRaw);
  return asesorRaw; // Retornar tal cual para no perder datos
}

/** Limpia el cache de DIM_ASESORES en memoria (llamar al inicio del ETL si actualizaste la tabla) */
function invalidar_cache_asesores() {
  _dimAsesorCache = null;
}

/** Registra en PENDIENTES_MAPEO para revisión manual (evita duplicados) */
function registrar_pendiente(tipo, valor) {
  try {
    var ss    = getSpreadsheetDestino();
    var sheet = getOrCreateSheet(ss, CFG.OUT.PEND);
    if (sheet.getLastRow() === 0) {
      sheet.appendRow(['TIPO','VALOR_RAW','MAPEAR_A','FECHA_DETECTADO']);
      sheet.getRange(1,1,1,4).setBackground('#c9321c').setFontColor('white').setFontWeight('bold');
    }
    // Evitar duplicados
    if (sheet.getLastRow() > 1) {
      var existing = sheet.getRange(2, 2, sheet.getLastRow() - 1, 1).getValues();
      for (var e = 0; e < existing.length; e++) {
        if (String(existing[e][0]).toUpperCase().trim() === valor.toUpperCase().trim()) return;
      }
    }
    sheet.appendRow([tipo, valor, '', new Date()]);
  } catch (e) { /* silencioso */ }
}

/** Convierte un valor a Date de forma segura */
function toDate(val) {
  if (!val) return new Date(0);
  if (val instanceof Date) return etl_normalizeDate_(val) || new Date(0);
  var repaired = etl_repairDateText_(String(val).trim());
  var d = etl_parseDateText_(repaired) || new Date(repaired);
  var normalized = etl_normalizeDate_(d);
  return normalized ? normalized : new Date(0);
}

/** Formatea fecha a string 'yyyy-MM-dd' */
function formatFecha(d) {
  try {
    d = etl_normalizeDate_(d);
    if (!etl_isSaneDate_(d)) return '';
    return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  } catch (e) {
    return '';
  }
}

function etl_normalizeDate_(value) {
  if (!value || !(value instanceof Date) || isNaN(value.getTime())) return null;
  var d = new Date(value);
  var fixedYear = etl_repairYear_(d.getFullYear());
  if (fixedYear !== d.getFullYear()) d.setFullYear(fixedYear);
  return d;
}

function etl_repairDateText_(value) {
  if (!value) return value;
  value = value.replace(/^(\d{5})([-\/])/, function(match, year, sep) {
    var fixed = etl_repairYear_(Number(year));
    return fixed !== Number(year) ? String(fixed) + sep : match;
  });
  value = value.replace(/([-\/])(\d{5})(\b|[T\s])/, function(match, sep, year, tail) {
    var fixed = etl_repairYear_(Number(year));
    return fixed !== Number(year) ? sep + String(fixed) + tail : match;
  });
  return value;
}

function etl_parseDateText_(value) {
  var s = String(value || '').trim();
  var m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  m = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);
  if (m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
  return null;
}

function etl_repairYear_(year) {
  year = Number(year || 0);
  if (year >= 20220 && year <= 20229) return 2020 + (year % 10);
  return year;
}

function etl_isSaneDate_(d) {
  if (!d || !(d instanceof Date) || isNaN(d.getTime()) || d.getTime() <= 0) return false;
  var year = d.getFullYear();
  return year >= 2020 && d.getTime() <= etl_todayEndMs_();
}

function etl_todayEndMs_() {
  if (ETL_TODAY_END_MS_CACHE != null) return ETL_TODAY_END_MS_CACHE;
  var today = etl_today_();
  today.setHours(23, 59, 59, 999);
  ETL_TODAY_END_MS_CACHE = today.getTime();
  return ETL_TODAY_END_MS_CACHE;
}

function etl_resetRuntimeDateCaches_() {
  ETL_TODAY_END_MS_CACHE = null;
  DASHBOARD_FAST_TODAY_KEY_CACHE = null;
}

function etl_today_() {
  var parts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd').split('-');
  return new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]));
}

/** Crea un objeto lead con estado inicial del embudo.
 *  origen_venta: '' = ligado a lead por CELULAR/Manifiesto; 'REFERIDO/SIN CELULAR' = venta sin match (cuenta igual en totales). */
function crearLead(celular, nombre, asesor, proyecto, fuente, nombreOpc, fechaEntrada) {
  return {
    celular          : celular || '',
    nombre           : nombre || '',
    asesor           : asesor || '',
    proyecto         : proyecto || '',
    fuente           : fuente || '',
    nombre_opc       : nombreOpc || '',
    fecha_entrada    : toDate(fechaEntrada),
    ultima_fecha     : toDate(fechaEntrada),
    ultima_tipif     : '',
    origen_venta     : '',  // 'REFERIDO/SIN CELULAR' si es venta sin match
    tieneAsignacion  : false,
    fecha_asignacion : null, // fecha (Date) de la primera ASIGNACION/ASIGNACION_MANUAL del lead (Looker: filtrar "asignados en el período")
    // ── Etapas del embudo ──────────────────────────────────────────────
    isNoEfectivo     : false,
    isContacto       : false,
    isPotencial      : false,
    isCita           : false,
    isPresencia      : false,
    isTour           : false,
    isNegocio        : false,
    isProcesable     : false,
    tieneDatoFalso   : false,
    fecha_presencia  : null,
    tipo_presencia   : null,
    fecha_venta      : null,
    modalidad_venta  : null,
    tlmk_venta       : null
  };
}

/** Estado inicial del embudo para RPT_ASESORES y RPT_OPC */
function estadoInicial() {
  return {
    isNoEfectivo : false,
    isContacto   : false,
    isPotencial  : false,
    isCita       : false,
    isPresencia  : false,
    isTour       : false,
    isNegocio    : false,
    isProcesable : false,
    tieneDatoFalso: false
  };
}

// ==========================================================================
// FASE DASHBOARD WEB - CACHES OPTIMIZADAS PARA APP SCRIPT
// ==========================================================================

function fase_dashboardCaches(factRows, presencias, ventas) {
  var t0 = Date.now();
  try {
    etl_log('INFO', 'fase_dashboardCaches', 'Iniciando payload web compacto...');
    var norm = dashboard_createNormMemo_();
    var tipifMap = dashboard_loadTipificacionMap_();
    var dimMap = dashboard_leerDimClientes_(norm);
    etl_log('INFO', 'fase_dashboardCaches',
      'Mapas listos: tipificaciones ' + Object.keys(tipifMap || {}).length +
      ' | dim clientes ' + Object.keys(dimMap || {}).length +
      ' | ' + (Date.now() - t0) + 'ms');

    var bundle = dashboard_buildLeadStageCache_(factRows, presencias, ventas, dimMap, tipifMap, norm);
    etl_log('INFO', 'fase_dashboardCaches',
      'Lead stage cache en memoria: leads ' + bundle.leads.length +
      ' | alerts ' + bundle.alerts.length +
      ' | ' + (Date.now() - t0) + 'ms');

    var payload = dashboard_buildWebPayloadDirect_(bundle, factRows, presencias, ventas, tipifMap, t0, norm);
    var payloadJson = JSON.stringify(payload);
    dashboardSaveWebPayloadJson_(payloadJson);
    var snapshotCounts = payload.counts || {};
    etl_log('INFO', 'fase_dashboardCaches',
      'Payload web publicado: leads ' + snapshotCounts.leads +
      ' | events ' + snapshotCounts.events +
      ' | alerts ' + snapshotCounts.alerts +
      ' | bytes ' + payloadJson.length +
      ' | ' + (Date.now() - t0) + 'ms');

    if (dashboard_escribirDashSheets_()) {
      var leadTable = dashboard_writeLeadStageCache_(bundle.leads, { skipSheetWrite: true });
      var eventTable = dashboard_writeEventDailyCache_(factRows, presencias, ventas, bundle.leadByCel, bundle.ventaMatches, tipifMap, { skipSheetWrite: true });
      var filterTable = dashboard_writeFilterOptions_(bundle.leads, { skipSheetWrite: true });
      var alertTable = dashboard_writeAlerts_(bundle.leads, bundle.alerts, { skipSheetWrite: true });
      dashboard_writeSnapshotTableToSheet_(CFG.OUT.DASH_LEADS, leadTable);
      dashboard_writeSnapshotTableToSheet_(CFG.OUT.DASH_EVENTS, eventTable);
      dashboard_writeSnapshotTableToSheet_(CFG.OUT.DASH_FILTERS, filterTable);
      dashboard_writeSnapshotTableToSheet_(CFG.OUT.DASH_ALERTS, alertTable);
    }

    etl_log('INFO', 'fase_dashboardCaches',
      'Dashboard caches OK: leads ' + bundle.leads.length +
      ' | events ' + snapshotCounts.events +
      ' | alerts ' + bundle.alerts.length +
      ' | ' + (Date.now() - t0) + 'ms');
  } catch (e) {
    etl_log('ERROR', 'fase_dashboardCaches', e && e.stack ? e.stack : e.message);
    throw e;
  }
}

function dashboard_buildWebPayloadDirect_(bundle, factRows, presencias, ventas, tipifMap, phaseStartMs, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  var generatedAt = new Date().toISOString();
  var leads = (bundle && bundle.leads) || [];
  var compactLeads = [];
  var catalogSets = dashboard_directCatalogSets_();
  var minDate = null;
  var maxDate = null;

  for (var i = 0; i < leads.length; i++) {
    var compact = dashboard_compactLeadDirect_(leads[i], norm);
    leads[i]._dashboardCompact = compact;
    compactLeads.push(compact);
    dashboard_directAddCatalogs_(catalogSets, compact);
    minDate = dashboard_pickMinDate_(minDate, leads[i].fecha_captacion);
    maxDate = dashboard_pickMaxDate_(maxDate, leads[i].fecha_captacion);
  }

  for (var rawTipif in (tipifMap || {})) {
    if (tipifMap[rawTipif] && tipifMap[rawTipif].agrupada) {
      dashboard_addOption_(catalogSets.TIPIFICACION, norm('TIPIFICACION', tipifMap[rawTipif].agrupada));
    }
  }

  var meta = {
    META_MIN_FECHA_CAPTACION: minDate ? formatFecha(minDate) : '',
    META_MAX_FECHA_CAPTACION: maxDate ? formatFecha(maxDate) : ''
  };
  var defaults = dashboardDefaultDateRange_(meta);
  etl_log('INFO', 'fase_dashboardCaches',
    'Leads compactos listos: ' + compactLeads.length +
    ' | rango ' + defaults.dateStart + ' a ' + defaults.dateEnd +
    ' | ' + (Date.now() - phaseStartMs) + 'ms');

  var compactEvents = dashboard_buildCompactEventsDirect_(
    factRows || [],
    presencias || [],
    ventas || [],
    (bundle && bundle.leadByCel) || {},
    (bundle && bundle.ventaMatches) || {},
    tipifMap || {},
    defaults,
    phaseStartMs,
    norm
  );
  var compactAlerts = dashboard_buildCompactAlertsDirect_(leads, (bundle && bundle.alerts) || []);
  var catalogs = dashboard_catalogsFromSetsDirect_(catalogSets);
  var filterCount = 2;
  for (var cat in catalogs) filterCount += (catalogs[cat] || []).length;

  var counts = {
    leads: compactLeads.length,
    leadCols: 28,
    events: compactEvents.length,
    eventCols: 21,
    filters: filterCount,
    filterCols: 3,
    alerts: compactAlerts.length,
    alertCols: 7
  };

  return {
    ok: true,
    generatedAt: generatedAt,
    snapshotGeneratedAt: generatedAt,
    defaults: {
      mode: 'COHORTE',
      tipoFecha: 'CAPTACION',
      dateStart: defaults.dateStart,
      dateEnd: defaults.dateEnd
    },
    meta: meta,
    catalogs: catalogs,
    counts: counts,
    diagnostics: dashboard_directDiagnostics_(compactLeads, compactEvents),
    dataScope: {
      leads: 'ALL',
      events: dashboard_eventScopeLabel_(),
      eventDateStart: defaults.dateStart,
      eventDateEnd: defaults.dateEnd
    },
    data: {
      leads: compactLeads,
      events: compactEvents,
      alerts: compactAlerts
    }
  };
}

function dashboard_directCatalogSets_() {
  return {
    FUENTE: {},
    PROYECTO: {},
    ESTADO_CIVIL: {},
    DISTRITO: {},
    NOMBRE_OPC: {},
    ASESOR: {},
    TIPIFICACION: {},
    ASIGNADO_ESTADO: { ASIGNADO: true, NO_ASIGNADO: true },
    TIPO_FECHA: {
      CAPTACION: true,
      ASIGNACION: true,
      GESTION: true,
      CITA: true,
      PRESENCIA: true,
      TOUR: true,
      SEPARACION: true,
      PROCESABLE: true
    }
  };
}

function dashboard_directAddCatalogs_(sets, r) {
  dashboard_addOption_(sets.FUENTE, r.fu);
  dashboard_addOption_(sets.PROYECTO, r.py);
  dashboard_addOption_(sets.ESTADO_CIVIL, r.ec);
  dashboard_addOption_(sets.DISTRITO, r.di);
  dashboard_addOption_(sets.NOMBRE_OPC, r.op);
  dashboard_addOption_(sets.ASESOR, r.as);
  dashboard_addOption_(sets.TIPIFICACION, r.ti);
  dashboard_addOption_(sets.ASIGNADO_ESTADO, r.ae);
}

function dashboard_catalogsFromSetsDirect_(sets) {
  var out = {};
  for (var cat in sets) out[cat] = dashboardSortOptions_(cat, Object.keys(sets[cat]).filter(function(v) { return v !== ''; }));
  return out;
}

function dashboard_createNormMemo_() {
  var cache = {};
  return function(category, value) {
    var cat = String(category || '').toUpperCase().trim();
    var raw = String(value == null ? '' : value).trim();
    var key = cat + '\u0001' + raw;
    if (cache.hasOwnProperty(key)) return cache[key];
    var normalized = dashboardNormalizeDimensionValue_(cat, value);
    cache[key] = normalized;
    return normalized;
  };
}

function dashboard_fastDateKey_(value) {
  if (!value) return '';
  if (value instanceof Date) {
    if (isNaN(value.getTime()) || value.getTime() <= 0) return '';
    var year = etl_repairYear_(value.getFullYear());
    if (year < 2020) return '';
    var todayKey = dashboard_fastTodayKey_();
    var valueKey = year * 10000 + (value.getMonth() + 1) * 100 + value.getDate();
    if (valueKey > todayKey) return '';
    return year + '-' + dashboard_pad2_(value.getMonth() + 1) + '-' + dashboard_pad2_(value.getDate());
  }
  return dashboardDateKey_(value);
}

function dashboard_fastTodayKey_() {
  if (DASHBOARD_FAST_TODAY_KEY_CACHE != null) return DASHBOARD_FAST_TODAY_KEY_CACHE;
  var today = etl_today_();
  DASHBOARD_FAST_TODAY_KEY_CACHE = today.getFullYear() * 10000 + (today.getMonth() + 1) * 100 + today.getDate();
  return DASHBOARD_FAST_TODAY_KEY_CACHE;
}

function dashboard_eventDateInPayloadRange_(dateKey, defaults) {
  if (!dateKey) return false;
  if (dashboard_eventScopeMode_() === 'DEFAULT_RANGE') {
    defaults = defaults || {};
    return dashboardCompactDateInRange_(dateKey, defaults.dateStart, defaults.dateEnd);
  }
  return true;
}

function dashboard_pad2_(value) {
  return value < 10 ? '0' + value : String(value);
}

function dashboard_compactLeadDirect_(l, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  l = l || {};
  return {
    c: String(l.celular || ''),
    fc: dashboard_fastDateKey_(l.fecha_captacion),
    fa: dashboard_fastDateKey_(l.fecha_asignacion),
    fg: dashboard_fastDateKey_(l.fecha_ult_gestion),
    fci: dashboard_fastDateKey_(l.fecha_cita),
    fp: dashboard_fastDateKey_(l.fecha_presencia),
    ft: dashboard_fastDateKey_(l.fecha_tour),
    fs: dashboard_fastDateKey_(l.fecha_separacion),
    fpr: dashboard_fastDateKey_(l.fecha_procesable),
    fu: norm('FUENTE', l.fuente),
    py: norm('PROYECTO', l.proyecto),
    ec: norm('ESTADO_CIVIL', l.estado_civil),
    di: norm('DISTRITO', l.distrito),
    op: norm('NOMBRE_OPC', l.nombre_opc),
    as: norm('ASESOR', l.asesor_ult_gestion || l.asesor_fallback),
    ae: l.asignado ? 'ASIGNADO' : 'NO_ASIGNADO',
    ti: norm('TIPIFICACION', l.tipif_agr_ultima),
    crm: l.es_lead_crm ? 1 : 0,
    co: l.es_contactable ? 1 : 0,
    po: l.es_potencial ? 1 : 0,
    ci: l.tiene_cita ? 1 : 0,
    pp: l.tiene_presencia ? 1 : 0,
    to: l.tiene_tour ? 1 : 0,
    se: l.tiene_separacion ? 1 : 0,
    pr: l.tiene_procesable ? 1 : 0
  };
}

function dashboard_compactEventLead_(lead, norm) {
  if (lead && lead._dashboardCompact) return lead._dashboardCompact;
  return dashboard_compactLeadDirect_(lead || {}, norm);
}

function dashboard_buildCompactEventsDirect_(factRows, presencias, ventas, leadByCel, ventaMatches, tipifMap, defaults, phaseStartMs, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  var agg = {};

  function add(fecha, tipoFecha, lead, tipifAgr, metrics) {
    var f = dashboard_fastDateKey_(fecha);
    if (!dashboard_eventDateInPayloadRange_(f, defaults)) return;
    var c = dashboard_compactEventLead_(lead, norm);
    var ti = tipifAgr ? norm('TIPIFICACION', tipifAgr) : (c.ti || 'TODAS');
    var key = [
      f, tipoFecha, c.fu || 'SIN_FUENTE', c.py || 'SIN_PROYECTO', c.ec || 'NO ESPECIFICADO',
      c.di || 'NO ESPECIFICADO', c.op || 'SIN_OPC', c.as || 'SIN_ASESOR', ti, c.ae || 'NO_ASIGNADO'
    ].join('|~|');
    if (!agg[key]) {
      agg[key] = {
        f: f,
        tf: tipoFecha,
        fu: c.fu || 'SIN_FUENTE',
        py: c.py || 'SIN_PROYECTO',
        ec: c.ec || 'NO ESPECIFICADO',
        di: c.di || 'NO ESPECIFICADO',
        op: c.op || 'SIN_OPC',
        as: c.as || 'SIN_ASESOR',
        ti: ti,
        ae: c.ae || 'NO_ASIGNADO',
        l: 0,
        ag: 0,
        ge: 0,
        co: 0,
        po: 0,
        ci: 0,
        pp: 0,
        to: 0,
        se: 0,
        pr: 0,
        df: 0
      };
    }
    var row = agg[key];
    if (metrics.l) row.l += metrics.l;
    if (metrics.ag) row.ag += metrics.ag;
    if (metrics.ge) row.ge += metrics.ge;
    if (metrics.co) row.co += metrics.co;
    if (metrics.po) row.po += metrics.po;
    if (metrics.ci) row.ci += metrics.ci;
    if (metrics.pp) row.pp += metrics.pp;
    if (metrics.to) row.to += metrics.to;
    if (metrics.se) row.se += metrics.se;
    if (metrics.pr) row.pr += metrics.pr;
    if (metrics.df) row.df += metrics.df;
  }

  var leadCount = 0;
  for (var cel in leadByCel) {
    var l = leadByCel[cel];
    if (l.es_lead_crm) add(l.fecha_captacion, 'CAPTACION', l, '', { l: 1 });
    if (l.asignado) add(l.fecha_asignacion, 'ASIGNACION', l, '', { ag: 1 });
    leadCount++;
  }
  etl_log('INFO', 'fase_dashboardCaches',
    'Eventos base agregados: leads ' + leadCount + ' | grupos ' + Object.keys(agg).length +
    ' | ' + (Date.now() - phaseStartMs) + 'ms');

  for (var i = 0; i < factRows.length; i++) {
    var f = factRows[i];
    var tipo = String(f.tipo_accion || '').toUpperCase().trim();
    if (CFG.TIPO_GESTION.indexOf(tipo) === -1) continue;
    var info = dashboard_tipifInfo_(f.tipificacion, tipifMap);
    var lead = leadByCel[String(f.celular || '').trim()] || null;
    add(f.fecha_interaccion, 'GESTION', lead, info.agrupada, {
      ge: 1,
      co: info.contactable ? 1 : 0,
      po: info.potencial ? 1 : 0,
      df: String(f.tipificacion || '').toUpperCase().trim() === 'DF' ? 1 : 0
    });
  }
  etl_log('INFO', 'fase_dashboardCaches',
    'Eventos gestion agregados: fact ' + factRows.length + ' | grupos ' + Object.keys(agg).length +
    ' | ' + (Date.now() - phaseStartMs) + 'ms');

  for (var p = 0; p < presencias.length; p++) {
    var pr = presencias[p];
    var leadP = leadByCel[String(pr.celular || '').trim()] || {
      fuente: pr.fuente_norm,
      proyecto: pr.proyecto,
      nombre_opc: pr.nombre_opc,
      asesor_ult_gestion: pr.asesor_canonico,
      asignado: false
    };
    add(pr.fecha_evento, 'CITA', leadP, '', { ci: 1 });
    if (pr.es_presencia) add(pr.fecha_evento, 'PRESENCIA', leadP, '', { pp: 1 });
    if (pr.es_tour) add(pr.fecha_evento, 'TOUR', leadP, '', { to: 1 });
  }

  for (var v = 0; v < ventas.length; v++) {
    var match = ventaMatches[v] || {};
    var leadV = leadByCel[match.celular] || {
      fuente: ventas[v].origen_norm,
      proyecto: ventas[v].proyecto,
      nombre_opc: ventas[v].promotora,
      asesor_ult_gestion: ventas[v].tlmk_canonico,
      asignado: false
    };
    if (ventas[v].es_negocio) add(ventas[v].fecha_compra, 'SEPARACION', leadV, '', { se: 1 });
    if (ventas[v].es_procesable) add(ventas[v].fecha_procesa || ventas[v].fecha_compra, 'PROCESABLE', leadV, '', { pr: 1 });
  }

  var keys = Object.keys(agg).sort();
  var out = [];
  for (var k = 0; k < keys.length; k++) out.push(agg[keys[k]]);
  etl_log('INFO', 'fase_dashboardCaches',
    'Eventos compactos listos: ' + out.length +
    ' | ' + (Date.now() - phaseStartMs) + 'ms');
  return out;
}

function dashboard_buildCompactAlertsDirect_(leads, alerts) {
  var agg = {};

  function add(tipo, prioridad, fecha, dimension, valor, metrica, detalle) {
    var key = [tipo || '', prioridad || '', dimension || '', valor || '', detalle || ''].join('|~|');
    if (!agg[key]) {
      agg[key] = {
        tipo: String(tipo || ''),
        prioridad: String(prioridad || ''),
        fecha: dashboard_fastDateKey_(fecha),
        dimension: String(dimension || ''),
        valor: String(valor || ''),
        metrica: 0,
        detalle: String(detalle || '')
      };
    }
    agg[key].metrica += Number(metrica || 0);
  }

  for (var i = 0; i < alerts.length; i++) {
    var a = alerts[i] || [];
    add(a[0], a[1], a[2], a[3], a[4], a[5], a[6]);
  }

  for (var j = 0; j < leads.length; j++) {
    var l = leads[j];
    if (l.tiene_dato_falso) {
      add('DATOS_FALSOS_OPC', 'MEDIA', l.fecha_ult_gestion || l.fecha_captacion, 'OPC', l.nombre_opc || 'SIN_OPC', 1, 'Leads con tipificacion DF');
    }
    if ((l.tiene_separacion || l.tiene_procesable) && !l.fecha_cita) {
      add('VENTA_SIN_CITA_MANIF', 'MEDIA', l.fecha_separacion || l.fecha_procesable, 'FUENTE', l.fuente || 'SIN_FUENTE', 1, 'Venta/procesable sin fecha de cita en manifiesto');
    }
  }

  var keys = Object.keys(agg).sort();
  var out = [];
  for (var k = 0; k < keys.length; k++) out.push(agg[keys[k]]);
  return out;
}

function dashboard_directDiagnostics_(leads, events) {
  var out = {
    leads: leads.length,
    events: events.length,
    crmLeads: 0,
    leadDateMin: '',
    leadDateMax: '',
    eventDateMin: '',
    eventDateMax: '',
    eventsByTipo: {},
    leadsByMonth: {},
    eventsByMonth: {}
  };
  for (var i = 0; i < leads.length; i++) {
    var l = leads[i];
    if (l.crm) out.crmLeads++;
    if (l.fc) {
      if (!out.leadDateMin || l.fc < out.leadDateMin) out.leadDateMin = l.fc;
      if (!out.leadDateMax || l.fc > out.leadDateMax) out.leadDateMax = l.fc;
      var lm = l.fc.substring(0, 7);
      out.leadsByMonth[lm] = (out.leadsByMonth[lm] || 0) + 1;
    }
  }
  for (var e = 0; e < events.length; e++) {
    var r = events[e];
    out.eventsByTipo[r.tf] = (out.eventsByTipo[r.tf] || 0) + 1;
    if (r.f) {
      if (!out.eventDateMin || r.f < out.eventDateMin) out.eventDateMin = r.f;
      if (!out.eventDateMax || r.f > out.eventDateMax) out.eventDateMax = r.f;
      var em = r.f.substring(0, 7);
      out.eventsByMonth[em] = (out.eventsByMonth[em] || 0) + 1;
    }
  }
  return out;
}

function dashboard_defaultTipifRows_() {
  return [
    ['NC','NO CONTESTA','NO CONTACTADO',0,0,0,1],
    ['NSHOW','NO ASISTIO','CITA',1,1,1,1],
    ['NI','NO INTERESADO','DESCARTE',1,0,0,1],
    ['CC','CITA CONFIRMADA','CITA',1,1,1,1],
    ['AP','APAGADO','NO CONTACTADO',0,0,0,1],
    ['AG INM','NO CALIFICA','DESCARTE',1,0,0,1],
    ['VLL','VOLVER A LLAMAR','POTENCIAL',1,1,0,1],
    ['DF','DATO FALSO','INVALIDO',0,0,0,1],
    ['IW','INFO. WHATSAPP','POTENCIAL',1,1,0,1],
    ['GW','INFO. WHATSAPP','POTENCIAL',1,1,0,1],
    ['NI-PROY','NO INTERESADO','DESCARTE',1,0,0,1],
    ['FS','FUERA DE SERVICIO','NO CONTACTADO',0,0,0,1],
    ['NEX/FS','FUERA DE SERVICIO','NO CONTACTADO',0,0,0,1],
    ['SG','SEGUIMIENTO','POTENCIAL',1,1,0,1],
    ['NI-LEG','NO INTERESADO','DESCARTE',1,0,0,1],
    ['NI-ECO','NO INTERESADO','DESCARTE',1,0,0,1],
    ['HP','CITA HP','CITA',1,1,1,1],
    ['CP','CITA PROYECTO','CITA',1,1,1,1],
    ['CXC','CITA CONFIRMADA','CITA',1,1,1,1],
    ['VP','CITA PROYECTO','CITA',1,1,1,1],
    ['CZ','CITA ZOOM','CITA',1,1,1,1],
    ['HH','CITA HOY','CITA',1,1,1,1],
    ['ASISTIO','ASISTIO','PRESENCIA_PROXY',1,1,1,1],
    ['NQ','NO CALIFICA','DESCARTE',1,0,0,1],
    ['DD','NO CALIFICA','DESCARTE',1,0,0,1],
    ['BZ','BUZON','NO CONTACTADO',0,0,0,1],
    ['N/A','NO APLICA','NO CONTACTADO',0,0,0,1]
  ];
}

function dashboard_loadTipificacionMap_() {
  var ss = getSpreadsheetDestino();
  var sh = getOrCreateSheet(ss, CFG.OUT.DASH_TIPIF_MAP);
  var headers = ['TIPIFICACION_RAW','TIPIFICACION_AGRUPADA','GRUPO_EMBUDO','ES_CONTACTABLE','ES_POTENCIAL','ES_CITA_PROXY','ACTIVO'];

  if (sh.getLastRow() === 0) {
    etl_writeTable(sh, [headers].concat(dashboard_defaultTipifRows_()));
  } else {
    var existing = {};
    var current = sh.getRange(1, 1, sh.getLastRow(), Math.max(sh.getLastColumn(), headers.length)).getValues();
    for (var r = 1; r < current.length; r++) {
      var raw = String(current[r][0] || '').trim().toUpperCase();
      if (raw) existing[raw] = true;
    }
    var missing = [];
    var defaults = dashboard_defaultTipifRows_();
    for (var i = 0; i < defaults.length; i++) {
      if (!existing[String(defaults[i][0]).toUpperCase()]) missing.push(defaults[i]);
    }
    if (missing.length > 0) {
      sh.getRange(sh.getLastRow() + 1, 1, missing.length, headers.length).setValues(missing);
    }
  }

  var data = sh.getRange(1, 1, sh.getLastRow(), headers.length).getValues();
  var map = {};
  for (var j = 1; j < data.length; j++) {
    var k = String(data[j][0] || '').trim().toUpperCase();
    if (!k) continue;
    var activo = dashboard_isTrue_(data[j][6]);
    if (!activo) continue;
    map[k] = {
      raw: k,
      agrupada: String(data[j][1] || '').trim() || 'REVISAR',
      grupo: String(data[j][2] || '').trim() || 'REVISAR',
      contactable: dashboard_isTrue_(data[j][3]),
      potencial: dashboard_isTrue_(data[j][4]),
      citaProxy: dashboard_isTrue_(data[j][5])
    };
  }
  return map;
}

function dashboard_tipifInfo_(rawTipif, tipifMap) {
  var raw = String(rawTipif || '').trim().toUpperCase();
  if (!raw) {
    return { raw: '', agrupada: '', grupo: '', contactable: false, potencial: false, citaProxy: false, revisar: false };
  }
  if (tipifMap && tipifMap[raw]) return tipifMap[raw];
  return { raw: raw, agrupada: 'REVISAR', grupo: 'REVISAR', contactable: false, potencial: false, citaProxy: false, revisar: true };
}

function dashboard_leerDimClientes_(norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  var map = {};
  try {
    var ss = SpreadsheetApp.openById(CFG.CRM_ID);
    var sh = ss.getSheetByName(CFG.CRM_DIM);
    if (!sh || sh.getLastRow() < 2) return map;
    var data = sh.getRange(1, 1, sh.getLastRow(), 16).getValues();
    for (var i = 1; i < data.length; i++) {
      var r = data[i];
      var cel = norm_tel(r[1]);
      if (!cel) continue;
      map[cel] = {
        id_cliente: r[0],
        celular: cel,
        nombre: String(r[2] || '').trim(),
        email: String(r[3] || '').trim(),
        origen: String(r[4] || '').trim(),
        proyecto: norm('PROYECTO', r[5]),
        fecha_registro: r[6],
        asesor_actual: norm('ASESOR', r[7]),
        ultima_tipif: String(r[8] || '').trim().toUpperCase(),
        fecha_ult_gest: r[9],
        fuente_norm: norm('FUENTE', r[10]),
        nombre_opc: norm('NOMBRE_OPC', r[11]),
        estado_civil: norm('ESTADO_CIVIL', r[12]),
        ocupacion: String(r[13] || '').trim().toUpperCase(),
        tiene_pareja: String(r[14] || '').trim().toUpperCase(),
        distrito: norm('DISTRITO', r[15])
      };
    }
  } catch (e) {
    etl_log('WARN', 'dashboard_leerDimClientes_', e.message);
  }
  return map;
}

function dashboard_buildLeadStageCache_(factRows, presencias, ventas, dimMap, tipifMap, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  var leadByCel = {};
  var alerts = [];

  function ensureLead(cel, seed, sourceType) {
    if (!cel) return null;
    if (!leadByCel[cel]) {
      leadByCel[cel] = {
        celular: cel,
        id_cliente: '',
        nombre: '',
        fecha_captacion: null,
        fecha_asignacion: null,
        fecha_ult_gestion: null,
        fecha_cita: null,
        fecha_presencia: null,
        fecha_tour: null,
        fecha_separacion: null,
        fecha_procesable: null,
        fuente: '',
        proyecto: '',
        estado_civil: '',
        distrito: '',
        nombre_opc: '',
        asesor_ult_gestion: '',
        asesor_fallback: '',
        asignado: false,
        tipif_raw_ultima: '',
        tipif_agr_ultima: '',
        tipif_grupo_ultima: '',
        es_contactable: false,
        es_potencial: false,
        tiene_cita: false,
        tiene_cita_proxy: false,
        tiene_presencia: false,
        tiene_tour: false,
        tiene_separacion: false,
        tiene_procesable: false,
        tiene_dato_falso: false,
        match_cita_metodo: '',
        match_venta_metodo: '',
        alerta_calidad: '',
        es_lead_crm: sourceType === 'CRM' ? 1 : 0
      };
    }
    var lead = leadByCel[cel];
    if (sourceType === 'CRM') lead.es_lead_crm = 1;
    if (seed) dashboard_mergeLeadSeed_(lead, seed, norm);
    return lead;
  }

  for (var celDim in dimMap) {
    ensureLead(celDim, dimMap[celDim], 'CRM');
  }

  for (var i = 0; i < factRows.length; i++) {
    var f = factRows[i];
    var cel = String(f.celular || '').trim();
    if (!cel) continue;
    var lead = ensureLead(cel, null, 'CRM');
    var fInter = toDate(f.fecha_interaccion);
    var fReg = toDate(f.fecha_registro);
    dashboard_setMinDate_(lead, 'fecha_captacion', dashboard_validDate_(fReg) ? fReg : fInter);

    if (!lead.id_cliente && f.id_cliente) lead.id_cliente = f.id_cliente;
    if (!lead.nombre && f.nombre_cliente) lead.nombre = String(f.nombre_cliente || '').trim();
    if (!lead.proyecto && f.proyecto) lead.proyecto = norm('PROYECTO', f.proyecto);
    if (!lead.fuente && f.fuente_norm) lead.fuente = norm('FUENTE', f.fuente_norm);
    if (!lead.nombre_opc && f.nombre_opc) lead.nombre_opc = norm('NOMBRE_OPC', f.nombre_opc);
    if (f.asesor_nombre) lead.asesor_fallback = norm('ASESOR', f.asesor_nombre);

    var tipo = String(f.tipo_accion || '').toUpperCase().trim();
    if (CFG.TIPO_ASIGNACION.indexOf(tipo) !== -1) {
      lead.asignado = true;
      dashboard_setMinDate_(lead, 'fecha_asignacion', fInter);
      if (f.nombre_opc) lead.nombre_opc = norm('NOMBRE_OPC', f.nombre_opc);
    }

    var isGestion = CFG.TIPO_GESTION.indexOf(tipo) !== -1;
    if (isGestion && dashboard_validDate_(fInter)) {
      if (!lead.fecha_ult_gestion || fInter.getTime() >= lead.fecha_ult_gestion.getTime()) {
        lead.fecha_ult_gestion = fInter;
        lead.asesor_ult_gestion = norm('ASESOR', f.asesor_nombre);
      }
    }

    var tipif = String(f.tipificacion || '').trim().toUpperCase();
    if (tipif) {
      var info = dashboard_tipifInfo_(tipif, tipifMap);
      if (info.contactable) lead.es_contactable = true;
      if (info.potencial) lead.es_potencial = true;
      if (info.citaProxy) lead.tiene_cita_proxy = true;
      if (tipif === 'DF') lead.tiene_dato_falso = true;
      if (!lead._ultimaTipifFecha || fInter.getTime() >= lead._ultimaTipifFecha.getTime()) {
        lead._ultimaTipifFecha = fInter;
        lead.tipif_raw_ultima = tipif;
        lead.tipif_agr_ultima = info.agrupada;
        lead.tipif_grupo_ultima = info.grupo;
      }
      if (info.revisar) {
        alerts.push(['TIPIFICACION_REVISAR', 'MEDIA', fInter, 'TIPIFICACION', tipif, 1, 'Tipificacion no existe en DASH_TIPIFICACION_MAP']);
      }
    }
  }

  for (var p = 0; p < presencias.length; p++) {
    var pr = presencias[p];
    var celP = String(pr.celular || '').trim();
    if (!celP) continue;
    var leadP = ensureLead(celP, {
      nombre: pr.cliente_raw,
      proyecto: pr.proyecto,
      fuente_norm: pr.fuente_norm,
      nombre_opc: pr.nombre_opc,
      asesor_actual: pr.asesor_canonico
    }, 'MANIFIESTO');
    var fEvt = toDate(pr.fecha_evento);
    if (!leadP.fecha_captacion) dashboard_setMinDate_(leadP, 'fecha_captacion', fEvt);
    leadP.tiene_cita = true;
    dashboard_setMinDate_(leadP, 'fecha_cita', fEvt);
    if (!leadP.match_cita_metodo) leadP.match_cita_metodo = 'CELULAR_MANIFIESTO';
    if (pr.es_presencia) {
      leadP.tiene_presencia = true;
      dashboard_setMinDate_(leadP, 'fecha_presencia', fEvt);
    }
    if (pr.es_tour) {
      leadP.tiene_tour = true;
      dashboard_setMinDate_(leadP, 'fecha_tour', fEvt);
    }
    if (esFuenteCrmReemplazable(leadP.fuente) && esFuenteManifiestoPrioritaria(pr.fuente_norm)) {
      leadP.fuente = norm('FUENTE', pr.fuente_norm);
    }
  }

  var ventaMatches = dashboard_applyVentasToLeadCache_(leadByCel, presencias, ventas, alerts, norm);

  var leads = [];
  for (var k in leadByCel) {
    var leadOut = leadByCel[k];
    if (!leadOut.asesor_ult_gestion) leadOut.asesor_ult_gestion = leadOut.asesor_fallback || '';
    if (!leadOut.tipif_agr_ultima && leadOut.tipif_raw_ultima) {
      var inf = dashboard_tipifInfo_(leadOut.tipif_raw_ultima, tipifMap);
      leadOut.tipif_agr_ultima = inf.agrupada;
      leadOut.tipif_grupo_ultima = inf.grupo;
    }
    if (leadOut.tiene_procesable) leadOut.tiene_separacion = true;
    if (leadOut.tiene_separacion || leadOut.tiene_procesable) {
      leadOut.es_contactable = true;
      leadOut.es_potencial = true;
      if (!leadOut.fecha_cita && leadOut.fecha_separacion) {
        leadOut.alerta_calidad = dashboard_appendAlertText_(leadOut.alerta_calidad, 'VENTA_SIN_CITA_MANIF');
      }
    }
    if (leadOut.asignado && !leadOut.fecha_ult_gestion && leadOut.es_lead_crm) {
      alerts.push(['LEAD_ASIGNADO_SIN_GESTION', 'ALTA', leadOut.fecha_asignacion || leadOut.fecha_captacion, 'ASESOR', leadOut.asesor_fallback || 'SIN_ASESOR', 1, leadOut.celular]);
    }
    leads.push(leadOut);
  }

  return { leads: leads, leadByCel: leadByCel, ventaMatches: ventaMatches, alerts: alerts };
}

function dashboard_mergeLeadSeed_(lead, seed, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  if (!seed) return;
  if (!lead.id_cliente && seed.id_cliente) lead.id_cliente = seed.id_cliente;
  if (!lead.nombre && seed.nombre) lead.nombre = String(seed.nombre || '').trim();
  if (!lead.proyecto && seed.proyecto) lead.proyecto = norm('PROYECTO', seed.proyecto);
  if (!lead.fuente && (seed.fuente_norm || seed.origen)) lead.fuente = norm('FUENTE', seed.fuente_norm || seed.origen);
  if (!lead.estado_civil && seed.estado_civil) lead.estado_civil = norm('ESTADO_CIVIL', seed.estado_civil);
  if (!lead.distrito && seed.distrito) lead.distrito = norm('DISTRITO', seed.distrito);
  if (!lead.nombre_opc && seed.nombre_opc) lead.nombre_opc = norm('NOMBRE_OPC', seed.nombre_opc);
  if (!lead.asesor_fallback && seed.asesor_actual) lead.asesor_fallback = norm('ASESOR', seed.asesor_actual);
  if (seed.asesor_actual) lead.asignado = true;
  if (seed.fecha_registro) dashboard_setMinDate_(lead, 'fecha_captacion', toDate(seed.fecha_registro));
  if (seed.fecha_ult_gest) dashboard_setMaxDate_(lead, 'fecha_ult_gestion', toDate(seed.fecha_ult_gest));
  if (seed.ultima_tipif && !lead.tipif_raw_ultima) lead.tipif_raw_ultima = String(seed.ultima_tipif || '').trim().toUpperCase();
}

function dashboard_applyVentasToLeadCache_(leadByCel, presencias, ventas, alerts, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  var ventaMatches = {};
  var nombreIdx = {};
  var manifestIdx = {};
  var fechaAsesorPrIdx = {};

  for (var cel in leadByCel) {
    var n = norm_nombre(leadByCel[cel].nombre);
    if (n.length > 3 && !nombreIdx[n]) nombreIdx[n] = cel;
  }

  for (var p = 0; p < presencias.length; p++) {
    var pr = presencias[p];
    var celP = String(pr.celular || '').trim();
    if (!celP) continue;
    var nn = norm_nombre(pr.cliente_raw);
    if (nn.length > 3 && !nombreIdx[nn]) nombreIdx[nn] = celP;
    if (nn.length > 3) {
      if (!manifestIdx[nn]) manifestIdx[nn] = [];
      manifestIdx[nn].push({
        celular: celP,
        proyecto: String(pr.proyecto || '').trim().toUpperCase(),
        asesor: pr.asesor_canonico,
        nombre_opc: pr.nombre_opc,
        nombre_opc_norm: norm_nombre(pr.nombre_opc),
        fuente_norm: pr.fuente_norm,
        fecha_evento: toDate(pr.fecha_evento)
      });
    }
    var fEvt = toDate(pr.fecha_evento);
    if (dashboard_validDate_(fEvt)) {
      var kFap = dashboard_fastDateKey_(fEvt) + '|~|' + (pr.asesor_canonico || '') + '|~|' + String(pr.proyecto || '').trim().toUpperCase();
      if (!fechaAsesorPrIdx[kFap]) fechaAsesorPrIdx[kFap] = [];
      fechaAsesorPrIdx[kFap].push({
        celular: celP,
        nombre_opc_norm: norm_nombre(pr.nombre_opc),
        fuente_norm: pr.fuente_norm
      });
    }
  }

  for (var v = 0; v < ventas.length; v++) {
    var venta = ventas[v];
    var celMatch = venta.celular || '';
    var metodo = celMatch ? 'CELULAR_VENTAS' : '';
    var fVta = toDate(venta.fecha_compra);
    var vtaProy = String(venta.proyecto || '').trim().toUpperCase();

    if (celMatch && !leadByCel[celMatch]) {
      leadByCel[celMatch] = dashboard_createSyntheticLead_(celMatch, venta, 'VENTA_CELULAR', norm);
    }

    if (!celMatch) {
      var nnV = norm_nombre(venta.cliente);
      if (nombreIdx[nnV]) {
        celMatch = nombreIdx[nnV];
        metodo = 'NOMBRE_CRM_MANIFIESTO';
      }
    }

    if (!celMatch && manifestIdx[norm_nombre(venta.cliente)]) {
      var candidates = manifestIdx[norm_nombre(venta.cliente)];
      var best = null, bestScore = -1;
      for (var c = 0; c < candidates.length; c++) {
        var cand = candidates[c];
        var score = 0;
        if (cand.asesor && venta.tlmk_canonico && cand.asesor === venta.tlmk_canonico) score += 5;
        if (cand.nombre_opc_norm && venta.promotora_norm && cand.nombre_opc_norm === venta.promotora_norm) score += 4;
        if (cand.fecha_evento && dashboard_validDate_(fVta)) {
          var diff = (fVta.getTime() - cand.fecha_evento.getTime()) / 86400000;
          if (diff >= 0 && diff <= 3) score += 6;
          else if (diff > 3 && diff <= 14) score += 3;
          else if (diff > 14 && diff <= 90) score += 1;
        }
        if (venta.origen_norm && cand.fuente_norm && venta.origen_norm === cand.fuente_norm) score += 2;
        if (cand.proyecto === vtaProy) score += 1;
        if (score > bestScore) { bestScore = score; best = cand; }
      }
      if (best) {
        celMatch = best.celular;
        metodo = 'MATCH_MANIFIESTO_SCORE';
      }
    }

    if (!celMatch && venta.tlmk_canonico && dashboard_validDate_(fVta)) {
      var bestFap = null, bestFapScore = -1;
      for (var delta = 0; delta <= 3; delta++) {
        for (var sign = -1; sign <= 1; sign += 2) {
          var fBusq = new Date(fVta.getTime() + delta * sign * 86400000);
          var key = dashboard_fastDateKey_(fBusq) + '|~|' + venta.tlmk_canonico + '|~|' + vtaProy;
          var arr = fechaAsesorPrIdx[key] || [];
          for (var a = 0; a < arr.length; a++) {
            var candF = arr[a];
            var scoreF = (6 - delta * 2);
            if (candF.nombre_opc_norm && venta.promotora_norm && candF.nombre_opc_norm === venta.promotora_norm) scoreF += 4;
            if (venta.origen_norm && candF.fuente_norm && venta.origen_norm === candF.fuente_norm) scoreF += 2;
            if (scoreF > bestFapScore) { bestFapScore = scoreF; bestFap = candF; }
          }
        }
        if (delta === 0) break;
      }
      if (bestFap) {
        celMatch = bestFap.celular;
        metodo = 'MATCH_FECHA_ASESOR_PROYECTO';
      }
    }

    if (!celMatch) {
      var synth = 'VTA_SIN_MATCH_' + v;
      leadByCel[synth] = dashboard_createSyntheticLead_(synth, venta, 'VENTA_SIN_MATCH', norm);
      celMatch = synth;
      metodo = 'VENTA_SIN_MATCH';
      alerts.push(['VENTA_SIN_MATCH', 'ALTA', fVta, 'VENTA', venta.cliente || 'SIN_NOMBRE', 1, 'No se encontro celular ni match confiable']);
    }

    var lead = leadByCel[celMatch];
    if (!lead) continue;
    lead.match_venta_metodo = metodo;
    ventaMatches[v] = { celular: celMatch, metodo: metodo };
    if (venta.es_negocio) {
      lead.tiene_separacion = true;
      dashboard_setMinDate_(lead, 'fecha_separacion', fVta);
    }
    if (venta.es_procesable) {
      lead.tiene_procesable = true;
      dashboard_setMinDate_(lead, 'fecha_procesable', dashboard_validDate_(toDate(venta.fecha_procesa)) ? toDate(venta.fecha_procesa) : fVta);
    }
  }
  return ventaMatches;
}

function dashboard_createSyntheticLead_(cel, venta, origen, norm) {
  norm = norm || dashboardNormalizeDimensionValue_;
  return {
    celular: cel,
    id_cliente: '',
    nombre: String(venta.cliente || '').trim(),
    fecha_captacion: toDate(venta.fecha_compra),
    fecha_asignacion: null,
    fecha_ult_gestion: null,
    fecha_cita: null,
    fecha_presencia: null,
    fecha_tour: null,
    fecha_separacion: null,
    fecha_procesable: null,
    fuente: norm('FUENTE', venta.origen_norm || ''),
    proyecto: norm('PROYECTO', venta.proyecto || ''),
    estado_civil: '',
    distrito: '',
    nombre_opc: norm('NOMBRE_OPC', venta.promotora || ''),
    asesor_ult_gestion: norm('ASESOR', venta.tlmk_canonico || ''),
    asesor_fallback: norm('ASESOR', venta.tlmk_canonico || ''),
    asignado: false,
    tipif_raw_ultima: '',
    tipif_agr_ultima: '',
    tipif_grupo_ultima: '',
    es_contactable: true,
    es_potencial: true,
    tiene_cita: false,
    tiene_cita_proxy: false,
    tiene_presencia: false,
    tiene_tour: false,
    tiene_separacion: false,
    tiene_procesable: false,
    tiene_dato_falso: false,
    match_cita_metodo: '',
    match_venta_metodo: origen,
    alerta_calidad: origen,
    es_lead_crm: 0
  };
}

function dashboard_writeLeadStageCache_(leads, options) {
  options = options || {};
  var headers = [
    'CELULAR_KEY','ES_LEAD_CRM','ID_CLIENTE','NOMBRE_CLIENTE',
    'FECHA_CAPTACION_LEAD','FECHA_PRIMERA_ASIGNACION','FECHA_ULTIMA_GESTION',
    'FECHA_PRIMERA_CITA','FECHA_PRIMERA_PRESENCIA','FECHA_PRIMER_TOUR',
    'FECHA_PRIMERA_SEPARACION','FECHA_PRIMER_PROCESABLE',
    'FUENTE_NORMALIZADA','PROYECTO','ESTADO_CIVIL','DISTRITO','NOMBRE_OPC',
    'ASESOR_ULTIMA_GESTION','ASIGNADO_ESTADO',
    'TIPIFICACION_RAW_ULTIMA','TIPIFICACION_AGRUPADA_ULTIMA','TIPIFICACION_GRUPO_ULTIMA',
    'ES_CONTACTABLE','ES_POTENCIAL','TIENE_CITA','TIENE_CITA_PROXY','TIENE_PRESENCIA','TIENE_TOUR',
    'TIENE_SEPARACION','TIENE_PROCESABLE','TIENE_DATO_FALSO',
    'MATCH_CITA_METODO','MATCH_VENTA_METODO','ALERTA_CALIDAD_DATO','UPDATED_AT'
  ];
  var now = new Date();
  var rows = [headers];
  for (var i = 0; i < leads.length; i++) {
    var l = leads[i];
    var fuente = dashboardNormalizeDimensionValue_('FUENTE', l.fuente);
    var proyecto = dashboardNormalizeDimensionValue_('PROYECTO', l.proyecto);
    var estadoCivil = dashboardNormalizeDimensionValue_('ESTADO_CIVIL', l.estado_civil);
    var distrito = dashboardNormalizeDimensionValue_('DISTRITO', l.distrito);
    var opc = dashboardNormalizeDimensionValue_('NOMBRE_OPC', l.nombre_opc);
    var asesor = dashboardNormalizeDimensionValue_('ASESOR', l.asesor_ult_gestion);
    var tipif = dashboardNormalizeDimensionValue_('TIPIFICACION', l.tipif_agr_ultima);
    rows.push([
      l.celular, l.es_lead_crm, l.id_cliente || '', l.nombre || '',
      dashboard_dateOrBlank_(l.fecha_captacion), dashboard_dateOrBlank_(l.fecha_asignacion), dashboard_dateOrBlank_(l.fecha_ult_gestion),
      dashboard_dateOrBlank_(l.fecha_cita), dashboard_dateOrBlank_(l.fecha_presencia), dashboard_dateOrBlank_(l.fecha_tour),
      dashboard_dateOrBlank_(l.fecha_separacion), dashboard_dateOrBlank_(l.fecha_procesable),
      fuente, proyecto, estadoCivil, distrito, opc,
      asesor, l.asignado ? 'ASIGNADO' : 'NO_ASIGNADO',
      l.tipif_raw_ultima || '', tipif, l.tipif_grupo_ultima || '',
      l.es_contactable ? 1 : 0, l.es_potencial ? 1 : 0, l.tiene_cita ? 1 : 0,
      l.tiene_cita_proxy ? 1 : 0, l.tiene_presencia ? 1 : 0, l.tiene_tour ? 1 : 0, l.tiene_separacion ? 1 : 0,
      l.tiene_procesable ? 1 : 0, l.tiene_dato_falso ? 1 : 0,
      l.match_cita_metodo || '', l.match_venta_metodo || '', l.alerta_calidad || '', now
    ]);
  }
  var snapshotTable = dashboardRowsToSnapshotTable_(rows);
  if (options.skipSheetWrite) return snapshotTable;

  var ss = getSpreadsheetDestino();
  var sh = getOrCreateSheet(ss, CFG.OUT.DASH_LEADS);
  etl_writeTable(sh, rows);
  if (rows.length > 1) {
    sh.getRange(2, 5, rows.length - 1, 8).setNumberFormat('yyyy-mm-dd');
    sh.getRange(2, 35, rows.length - 1, 1).setNumberFormat('yyyy-mm-dd hh:mm');
  }
  etl_maybeAutoResizeColumns(sh, headers.length, rows.length - 1);
  return snapshotTable;
}

function dashboard_writeEventDailyCache_(factRows, presencias, ventas, leadByCel, ventaMatches, tipifMap, options) {
  options = options || {};
  var agg = {};

  function add(fecha, tipoFecha, lead, tipifAgr, metrics) {
    var d = toDate(fecha);
    if (!dashboard_validDate_(d)) return;
    var dims = dashboard_eventDims_(lead, tipifAgr);
    var key = formatFecha(d) + '|~|' + tipoFecha + '|~|' + dims.join('|~|');
    if (!agg[key]) {
      agg[key] = {
        fecha: d,
        tipo_fecha: tipoFecha,
        dims: dims,
        leads: 0, asignados: 0, gestiones: 0, contactables: 0, potenciales: 0,
        citas: 0, presencias: 0, tours: 0, separaciones: 0, procesables: 0, datos_falsos: 0
      };
    }
    var a = agg[key];
    for (var m in metrics) if (metrics[m]) a[m] += metrics[m];
  }

  for (var cel in leadByCel) {
    var l = leadByCel[cel];
    if (l.es_lead_crm) add(l.fecha_captacion, 'CAPTACION', l, '', { leads: 1 });
    if (l.asignado) add(l.fecha_asignacion, 'ASIGNACION', l, '', { asignados: 1 });
  }

  for (var i = 0; i < factRows.length; i++) {
    var f = factRows[i];
    var lead = leadByCel[String(f.celular || '').trim()] || null;
    var tipo = String(f.tipo_accion || '').toUpperCase().trim();
    if (CFG.TIPO_GESTION.indexOf(tipo) === -1) continue;
    var info = dashboard_tipifInfo_(f.tipificacion, tipifMap);
    add(f.fecha_interaccion, 'GESTION', lead, info.agrupada, {
      gestiones: 1,
      contactables: info.contactable ? 1 : 0,
      potenciales: info.potencial ? 1 : 0,
      datos_falsos: String(f.tipificacion || '').toUpperCase().trim() === 'DF' ? 1 : 0
    });
  }

  for (var p = 0; p < presencias.length; p++) {
    var pr = presencias[p];
    var leadP = leadByCel[String(pr.celular || '').trim()] || {
      fuente: pr.fuente_norm, proyecto: pr.proyecto, nombre_opc: pr.nombre_opc,
      asesor_ult_gestion: pr.asesor_canonico, asignado: false
    };
    add(pr.fecha_evento, 'CITA', leadP, '', { citas: 1 });
    if (pr.es_presencia) add(pr.fecha_evento, 'PRESENCIA', leadP, '', { presencias: 1 });
    if (pr.es_tour) add(pr.fecha_evento, 'TOUR', leadP, '', { tours: 1 });
  }

  for (var v = 0; v < ventas.length; v++) {
    var match = ventaMatches[v] || {};
    var leadV = leadByCel[match.celular] || {
      fuente: ventas[v].origen_norm, proyecto: ventas[v].proyecto, nombre_opc: ventas[v].promotora,
      asesor_ult_gestion: ventas[v].tlmk_canonico, asignado: false
    };
    if (ventas[v].es_negocio) add(ventas[v].fecha_compra, 'SEPARACION', leadV, '', { separaciones: 1 });
    if (ventas[v].es_procesable) add(ventas[v].fecha_procesa || ventas[v].fecha_compra, 'PROCESABLE', leadV, '', { procesables: 1 });
  }

  var headers = [
    'FECHA','TIPO_FECHA','FUENTE_NORMALIZADA','PROYECTO','ESTADO_CIVIL','DISTRITO',
    'NOMBRE_OPC','ASESOR_ULTIMA_GESTION','TIPIFICACION_AGRUPADA','ASIGNADO_ESTADO',
    'LEADS','ASIGNADOS','GESTIONES','CONTACTABLES','POTENCIALES','CITAS',
    'PRESENCIAS','TOURS','SEPARACIONES','PROCESABLES','DATOS_FALSOS'
  ];
  var rows = [headers];
  var keys = Object.keys(agg).sort();
  for (var k = 0; k < keys.length; k++) {
    var a = agg[keys[k]];
    rows.push([
      a.fecha, a.tipo_fecha,
      a.dims[0], a.dims[1], a.dims[2], a.dims[3], a.dims[4], a.dims[5], a.dims[6], a.dims[7],
      a.leads, a.asignados, a.gestiones, a.contactables, a.potenciales, a.citas,
      a.presencias, a.tours, a.separaciones, a.procesables, a.datos_falsos
    ]);
  }
  var snapshotTable = dashboardRowsToSnapshotTable_(rows);
  if (options.skipSheetWrite) return snapshotTable;

  var ss = getSpreadsheetDestino();
  var sh = getOrCreateSheet(ss, CFG.OUT.DASH_EVENTS);
  etl_writeTable(sh, rows);
  if (rows.length > 1) sh.getRange(2, 1, rows.length - 1, 1).setNumberFormat('yyyy-mm-dd');
  etl_maybeAutoResizeColumns(sh, headers.length, rows.length - 1);
  return snapshotTable;
}

function dashboard_eventDims_(lead, tipifAgr) {
  lead = lead || {};
  return [
    dashboardNormalizeDimensionValue_('FUENTE', lead.fuente || 'SIN_FUENTE'),
    dashboardNormalizeDimensionValue_('PROYECTO', lead.proyecto || 'SIN_PROYECTO'),
    dashboardNormalizeDimensionValue_('ESTADO_CIVIL', lead.estado_civil || 'SIN_ESTADO'),
    dashboardNormalizeDimensionValue_('DISTRITO', lead.distrito || 'SIN_DISTRITO'),
    dashboardNormalizeDimensionValue_('NOMBRE_OPC', lead.nombre_opc || 'SIN_OPC'),
    dashboardNormalizeDimensionValue_('ASESOR', lead.asesor_ult_gestion || lead.asesor_fallback || 'SIN_ASESOR'),
    dashboardNormalizeDimensionValue_('TIPIFICACION', tipifAgr || lead.tipif_agr_ultima || 'TODAS'),
    lead.asignado ? 'ASIGNADO' : 'NO_ASIGNADO'
  ];
}

function dashboard_writeFilterOptions_(leads, options) {
  options = options || {};
  var sets = {
    FUENTE: {},
    PROYECTO: {},
    ESTADO_CIVIL: {},
    DISTRITO: {},
    NOMBRE_OPC: {},
    ASESOR: {},
    TIPIFICACION: {},
    ASIGNADO_ESTADO: { 'ASIGNADO': true, 'NO_ASIGNADO': true },
    TIPO_FECHA: {
      'CAPTACION': true, 'ASIGNACION': true, 'GESTION': true, 'CITA': true,
      'PRESENCIA': true, 'TOUR': true, 'SEPARACION': true, 'PROCESABLE': true
    }
  };
  var minDate = null, maxDate = null;
  for (var i = 0; i < leads.length; i++) {
    var l = leads[i];
    dashboard_addOption_(sets.FUENTE, dashboardNormalizeDimensionValue_('FUENTE', l.fuente));
    dashboard_addOption_(sets.PROYECTO, dashboardNormalizeDimensionValue_('PROYECTO', l.proyecto));
    dashboard_addOption_(sets.ESTADO_CIVIL, dashboardNormalizeDimensionValue_('ESTADO_CIVIL', l.estado_civil));
    dashboard_addOption_(sets.DISTRITO, dashboardNormalizeDimensionValue_('DISTRITO', l.distrito));
    dashboard_addOption_(sets.NOMBRE_OPC, dashboardNormalizeDimensionValue_('NOMBRE_OPC', l.nombre_opc));
    dashboard_addOption_(sets.ASESOR, dashboardNormalizeDimensionValue_('ASESOR', l.asesor_ult_gestion));
    dashboard_addOption_(sets.TIPIFICACION, dashboardNormalizeDimensionValue_('TIPIFICACION', l.tipif_agr_ultima));
    minDate = dashboard_pickMinDate_(minDate, l.fecha_captacion);
    maxDate = dashboard_pickMaxDate_(maxDate, l.fecha_captacion);
  }
  var rows = [['CATEGORIA','VALOR','ORDEN']];
  for (var cat in sets) {
    var vals = dashboardSortOptions_(cat, Object.keys(sets[cat]).filter(function(x) { return x !== ''; }));
    for (var v = 0; v < vals.length; v++) rows.push([cat, vals[v], v + 1]);
  }
  rows.push(['META_MIN_FECHA_CAPTACION', minDate ? formatFecha(minDate) : '', 1]);
  rows.push(['META_MAX_FECHA_CAPTACION', maxDate ? formatFecha(maxDate) : '', 1]);
  var snapshotTable = dashboardRowsToSnapshotTable_(rows);
  if (options.skipSheetWrite) return snapshotTable;

  var sh = getOrCreateSheet(getSpreadsheetDestino(), CFG.OUT.DASH_FILTERS);
  etl_writeTable(sh, rows);
  etl_maybeAutoResizeColumns(sh, 3, rows.length - 1);
  return snapshotTable;
}

function dashboard_writeAlerts_(leads, alerts, options) {
  options = options || {};
  var agg = {};
  for (var i = 0; i < alerts.length; i++) {
    var a = alerts[i];
    var key = a[0] + '|~|' + a[1] + '|~|' + a[3] + '|~|' + a[4] + '|~|' + a[6];
    if (!agg[key]) agg[key] = { tipo: a[0], prioridad: a[1], fecha: a[2], dimension: a[3], valor: a[4], metrica: 0, detalle: a[6] };
    agg[key].metrica += Number(a[5] || 0);
  }
  for (var j = 0; j < leads.length; j++) {
    var l = leads[j];
    if (l.tiene_dato_falso) {
      var kdf = 'DATOS_FALSOS_OPC|~|MEDIA|~|OPC|~|' + (l.nombre_opc || 'SIN_OPC') + '|~|Leads con DF';
      if (!agg[kdf]) agg[kdf] = { tipo: 'DATOS_FALSOS_OPC', prioridad: 'MEDIA', fecha: l.fecha_ult_gestion || l.fecha_captacion, dimension: 'OPC', valor: l.nombre_opc || 'SIN_OPC', metrica: 0, detalle: 'Leads con tipificacion DF' };
      agg[kdf].metrica++;
    }
    if ((l.tiene_separacion || l.tiene_procesable) && !l.fecha_cita) {
      var kv = 'VENTA_SIN_CITA_MANIF|~|MEDIA|~|FUENTE|~|' + (l.fuente || 'SIN_FUENTE') + '|~|Venta sin cita manifiesto';
      if (!agg[kv]) agg[kv] = { tipo: 'VENTA_SIN_CITA_MANIF', prioridad: 'MEDIA', fecha: l.fecha_separacion || l.fecha_procesable, dimension: 'FUENTE', valor: l.fuente || 'SIN_FUENTE', metrica: 0, detalle: 'Venta/procesable sin fecha de cita en manifiesto' };
      agg[kv].metrica++;
    }
  }
  var rows = [['TIPO_ALERTA','PRIORIDAD','FECHA_REFERENCIA','DIMENSION','VALOR','METRICA','DETALLE']];
  var keys = Object.keys(agg).sort();
  for (var k = 0; k < keys.length; k++) {
    var o = agg[keys[k]];
    rows.push([o.tipo, o.prioridad, dashboard_dateOrBlank_(o.fecha), o.dimension, o.valor, o.metrica, o.detalle]);
  }
  var snapshotTable = dashboardRowsToSnapshotTable_(rows);
  if (options.skipSheetWrite) return snapshotTable;

  var sh = getOrCreateSheet(getSpreadsheetDestino(), CFG.OUT.DASH_ALERTS);
  etl_writeTable(sh, rows);
  if (rows.length > 1) sh.getRange(2, 3, rows.length - 1, 1).setNumberFormat('yyyy-mm-dd');
  etl_maybeAutoResizeColumns(sh, 7, rows.length - 1);
  return snapshotTable;
}

function dashboard_writeSnapshotTableToSheet_(sheetName, table) {
  table = table || { headers: [], rows: [] };
  var rows = [table.headers || []].concat(table.rows || []);
  var sh = getOrCreateSheet(getSpreadsheetDestino(), sheetName);
  etl_writeTable(sh, rows);
}

function dashboardRowsToSnapshotTable_(rows) {
  rows = rows || [];
  if (!rows.length) return { headers: [], rows: [] };
  var headers = [];
  for (var h = 0; h < rows[0].length; h++) headers.push(String(rows[0][h] || '').trim());
  var data = [];
  for (var r = 1; r < rows.length; r++) {
    var out = [];
    for (var c = 0; c < headers.length; c++) out.push(dashboardSerializeSnapshotValue_(rows[r][c]));
    data.push(out);
  }
  return { headers: headers, rows: data };
}

function dashboard_isTrue_(v) {
  if (v === true || v === 1) return true;
  var s = String(v || '').trim().toUpperCase();
  return s === 'TRUE' || s === '1' || s === 'SI' || s === 'S';
}

function dashboard_validDate_(d) {
  return etl_isSaneDate_(d);
}

function dashboard_dateOrBlank_(d) {
  var normalized = toDate(d);
  return dashboard_validDate_(normalized) ? normalized : '';
}

function dashboard_setMinDate_(obj, prop, value) {
  var d = toDate(value);
  if (!dashboard_validDate_(d)) return;
  if (!obj[prop] || d.getTime() < obj[prop].getTime()) obj[prop] = d;
}

function dashboard_setMaxDate_(obj, prop, value) {
  var d = toDate(value);
  if (!dashboard_validDate_(d)) return;
  if (!obj[prop] || d.getTime() > obj[prop].getTime()) obj[prop] = d;
}

function dashboard_pickMinDate_(current, value) {
  var d = toDate(value);
  if (!dashboard_validDate_(d)) return current;
  if (!current || d.getTime() < current.getTime()) return d;
  return current;
}

function dashboard_pickMaxDate_(current, value) {
  var d = toDate(value);
  if (!dashboard_validDate_(d)) return current;
  if (!current || d.getTime() > current.getTime()) return d;
  return current;
}

function dashboard_addOption_(setObj, value) {
  var v = String(value || '').trim();
  if (v) setObj[v] = true;
}

function dashboard_appendAlertText_(base, value) {
  if (!value) return base || '';
  return base ? base + ';' + value : value;
}


// ==========================================================================
// UTILIDADES DE SHEETS
// ==========================================================================

function etl_eliminarReportesObsoletos() {
  try {
    var ss = getSpreadsheetDestino();
    for (var i = 0; i < ETL_REPORTES_OBSOLETOS.length; i++) {
      var nombre = ETL_REPORTES_OBSOLETOS[i];
      var sh = ss.getSheetByName(nombre);
      if (!sh) continue;
      if (ss.getSheets().length <= 1) {
        etl_log('WARN', 'etl_eliminarReportesObsoletos', 'No se puede borrar ' + nombre + ' porque es la unica hoja.');
        continue;
      }
      ss.deleteSheet(sh);
      etl_log('INFO', 'etl_eliminarReportesObsoletos', 'Hoja obsoleta eliminada: ' + nombre);
    }
  } catch (e) {
    etl_log('WARN', 'etl_eliminarReportesObsoletos', 'No se pudieron eliminar hojas obsoletas: ' + e.message);
  }
}

function etl_writeTable(sheet, rows) {
  if (!rows || rows.length === 0 || !rows[0] || rows[0].length === 0) return;
  var numRows = rows.length;
  var numCols = rows[0].length;

  expandSheet(sheet, numRows + 2, numCols);

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow > 0 && lastCol > 0) {
    sheet.getRange(1, 1, Math.max(lastRow, numRows), Math.max(lastCol, numCols)).clearContent();
  }

  sheet.getRange(1, 1, numRows, numCols).setValues(rows);
  sheet.getRange(1, 1, 1, numCols)
    .setBackground('#0f172a').setFontColor('white').setFontWeight('bold');
  sheet.setFrozenRows(1);
}

/**
 * Devuelve el spreadsheet DESTINO donde se escriben todos los reportes (Analytics).
 * Si CFG.ANALYTICS_ID está definido y es un ID válido, usa ese archivo.
 * Si no, usa getActiveSpreadsheet() (el archivo al que está vinculado el script).
 * IMPORTANTE: Configura ANALYTICS_ID con el ID de tu archivo Analytics para
 * garantizar que los datos se escriban siempre en el archivo correcto.
 */
function getSpreadsheetDestino() {
  var id = (typeof CFG !== 'undefined' && CFG.ANALYTICS_ID) ? CFG.ANALYTICS_ID : '';
  if (id && id !== 'AUTO' && id !== 'PEGAR_ID' && id.length > 10) {
    try {
      return SpreadsheetApp.openById(id);
    } catch (e) {
      etl_log('WARN','getSpreadsheetDestino','ANALYTICS_ID inválido, usando getActiveSpreadsheet: ' + e.message);
    }
  }
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    etl_log('ERROR','getSpreadsheetDestino','No hay spreadsheet activo. Configura CFG.ANALYTICS_ID con el ID de tu archivo Analytics.');
  }
  return ss;
}

/** Obtiene o crea una hoja en el spreadsheet dado */
function getOrCreateSheet(ss, nombre) {
  var sheet = ss.getSheetByName(nombre);
  if (!sheet) {
    sheet = ss.insertSheet(nombre);
    etl_log('INFO','getOrCreateSheet','Hoja "' + nombre + '" creada');
  }
  return sheet;
}

/** Asegura que la hoja tenga suficiente capacidad de filas y columnas */
function expandSheet(sheet, requiredRows, requiredCols) {
  var curRows = sheet.getMaxRows();
  var curCols = sheet.getMaxColumns();
  if (curCols < requiredCols) sheet.insertColumnsAfter(curCols, requiredCols - curCols);
  if (curRows < requiredRows) sheet.insertRowsAfter(curRows, (requiredRows - curRows) + 200);
}

// ==========================================================================
// LOGGER ETL
// ==========================================================================

function etl_log(nivel, funcion, mensaje) {
  try {
    var ss    = getSpreadsheetDestino();
    var sheet = getOrCreateSheet(ss, CFG.OUT.LOG);
    if (sheet.getLastRow() === 0) {
      sheet.appendRow(['TIMESTAMP','NIVEL','FUNCION','MENSAJE']);
      sheet.getRange(1,1,1,4).setBackground('#333').setFontColor('white').setFontWeight('bold');
    }
    sheet.appendRow([new Date(), nivel, funcion, mensaje]);
    // Conservar solo los últimos 2000 registros
    if (sheet.getLastRow() > 2002) sheet.deleteRows(2, sheet.getLastRow() - 2001);
  } catch (e) { /* silencioso — no causar loops */ }
}


// ==========================================================================
// SETUP INICIAL — Ejecutar UNA VEZ al crear el archivo Analytics
// ==========================================================================

function setup_ArchivoAnalytics() {
  var ss = getSpreadsheetDestino();
  var ui = SpreadsheetApp.getUi();

  // Invalidar cache de asesores para que se reconstruya fresco
  invalidar_cache_asesores();

  /* ── CONFIG_FUENTES ────────────────────────────────────────────────────── */
  var cfgSheet = getOrCreateSheet(ss, CFG.OUT.CONFIG);
  if (cfgSheet.getLastRow() === 0) {
    cfgSheet.appendRow(['TIPO','AÑO','MES','NOMBRE_MES','SPREADSHEET_ID','HOJA_NOMBRE','ACTIVO']);
    cfgSheet.getRange(1,1,1,7).setBackground('#1e3a5f').setFontColor('white').setFontWeight('bold');
    cfgSheet.setFrozenRows(1);
    // Filas de referencia precargadas
    cfgSheet.appendRow(['CRM',    2026, '', 'CRM PRINCIPAL',  CFG.CRM_ID,    CFG.CRM_FACT,    true]);
    cfgSheet.appendRow(['VENTAS', 2026, '', 'VENTAS',          CFG.VENTAS_ID, CFG.VENTAS_HOJA, true]);
    cfgSheet.appendRow(['MANIFIESTO', 2026, 1, 'ENERO 2026',    'PEGAR_ID_AQUI', CFG.MANIF_HOJA, false]);
    cfgSheet.appendRow(['MANIFIESTO', 2026, 2, 'FEBRERO 2026',  'PEGAR_ID_AQUI', CFG.MANIF_HOJA, false]);
    cfgSheet.appendRow(['MANIFIESTO', 2026, 3, 'MARZO 2026',    'PEGAR_ID_AQUI', CFG.MANIF_HOJA, false]);
    cfgSheet.autoResizeColumns(1, 7);
  }

  /* ── DIM_ASESORES ──────────────────────────────────────────────────────── */
  var dimAsSheet = getOrCreateSheet(ss, CFG.OUT.DIM_AS);
  if (dimAsSheet.getLastRow() === 0) {
    dimAsSheet.appendRow(['ID_ASESOR','NOMBRE_CANONICO','ALIAS_1','ALIAS_2','ALIAS_3','ALIAS_4','EMAIL','ESTADO']);
    dimAsSheet.getRange(1,1,1,8).setBackground('#1e3a5f').setFontColor('white').setFontWeight('bold');
    dimAsSheet.setFrozenRows(1);

    // Asesores actuales + históricos con aliases pre-cargados
    var asesores = [
      ['ASE_001','ANDREA A.',     'ANDREA',   'ANDREA A',            '',                  '',              '','ACTIVO'],
      ['ASE_002','MARILYN P.',    'MARILYN',  'MARILYN ROSALES',     'MARILYN P',         '',              '','ACTIVO'],
      ['ASE_003','EDITH P.',      'EDITH',    'EDITH P',             '',                  '',              '','ACTIVO'],
      ['ASE_004','DEBORA R.',     'DEBORAH',  'DEBORA',              'DEBORAH VALENZUELA','DEBORAH R.',    '','ACTIVO'],
      ['ASE_005','JACKY R.',      'JACKY',    'JAQUELINE',           'JAQUELINE ANGULO',  'JACKY R.',      '','ACTIVO'],
      ['ASE_006','LADY G.',       'LADY',     'LADY G',              '',                  '',              '','ACTIVO'],
      ['ASE_007','JUDITH R.',     'JUDITH',   'JUDITH R',            '',                  '',              '','INACTIVO'],
      ['ASE_008','KARINA P.',     'KARINA',   'KARINA P',            'KARINA ARCILA',     'KARINA A.',     '','INACTIVO'],
      ['ASE_009','LEONEL P.',     'LEONEL',   'LEONEL ORTEGA',       'LEONEL P',          '',              '','INACTIVO'],
      ['ASE_010','CRHISTIAN DOMINGUEZ','CRHISTIAN','CHRISTIAN DOMINGUEZ','CHRISTIAN',     '',              '','INACTIVO'],
      ['ASE_011','LUZ ODEAGA',    'LUZ',      'LUZ ODEAGA',          '',                  '',              '','INACTIVO'],
      ['ASE_012','JAQUELINE ANGULO','JAQUELINE ANGULO','JACQUELINE ANGULO','',            '',              '','INACTIVO']
    ];
    dimAsSheet.getRange(2, 1, asesores.length, 8).setValues(asesores);
    dimAsSheet.autoResizeColumns(1, 8);
  }

  /* ── PENDIENTES_MAPEO ──────────────────────────────────────────────────── */
  var pendSheet = getOrCreateSheet(ss, CFG.OUT.PEND);
  if (pendSheet.getLastRow() === 0) {
    pendSheet.appendRow(['TIPO','VALOR_RAW','MAPEAR_A','FECHA_DETECTADO']);
    pendSheet.getRange(1,1,1,4).setBackground('#c9321c').setFontColor('white').setFontWeight('bold');
    pendSheet.appendRow(['INFO','Esta hoja detecta automáticamente valores sin mapeo. Agrega el valor en MAPEAR_A y luego actualiza DIM_ASESORES o DIM_OPC.','','']);
  }

  /* ── LOG_ETL ───────────────────────────────────────────────────────────── */
  var logSheet = getOrCreateSheet(ss, CFG.OUT.LOG);
  if (logSheet.getLastRow() === 0) {
    logSheet.appendRow(['TIMESTAMP','NIVEL','FUNCION','MENSAJE']);
    logSheet.getRange(1,1,1,4).setBackground('#333').setFontColor('white').setFontWeight('bold');
  }

  ui.alert(
    '✅ Setup completado',
    'El archivo Analytics está configurado.\n\n' +
    '── PRÓXIMOS PASOS ──────────────────────\n' +
    '1. Ve a CONFIG_FUENTES y reemplaza "PEGAR_ID_AQUI"\n' +
    '   con los IDs reales de tus archivos de Manifiesto\n' +
    '   (extráelos de la URL de cada archivo: /d/[ID]/edit)\n' +
    '   Cambia ACTIVO a TRUE cuando estén listos.\n\n' +
    '2. Revisa DIM_ASESORES y ajusta aliases si necesitas.\n\n' +
    '3. Ejecuta "⚡ ETL Completo" del menú Analytics ETL\n' +
    '   para la primera carga de datos.\n\n' +
    '4. Ejecuta "⏰ Instalar Trigger (30 min)" para automatización.\n\n' +
    '5. Conecta este Sheets a Looker Studio y usa:\n' +
    '   • DATA_EMBUDO_FULL  → Embudo general + filtros\n' +
    '   • RPT_ASESORES      → Tabla dinámica por asesor\n' +
    '   • STG_PRESENCIAS    → Detalle de presencias\n' +
    '   • STG_VENTAS        → Detalle de ventas',
    ui.ButtonSet.OK
  );
}


// ==========================================================================
// TRIGGERS AUTOMÁTICOS
// ==========================================================================

/**
 * Instala el trigger de ejecución automática segun CFG.ETL.TRIGGER_CADA_MINUTOS.
 * Solo necesitas ejecutar esta función UNA VEZ manualmente.
 */
function instalar_Triggers() {
  var version = (CFG.ETL && CFG.ETL.DASHBOARD_TRIGGER_VERSION) || 'dashboard-fast';
  etl_limpiarEstadoCadena('reinstalando trigger periodico');

  var cadaMin = etl_instalarTriggerDashboardRapido_();
  try {
    PropertiesService.getScriptProperties().setProperty(ETL_CHAIN_KEYS.DASH_TRIGGER_VERSION, version);
  } catch (e) {}

  etl_alertUI(
    '⏰ Trigger instalado',
    'runETL_DashboardRapido se ejecutara automaticamente cada ' + cadaMin + ' minutos.\n\n' +
    'El dashboard corre STG -> payload web. El ETL completo queda manual para reportes pesados.\n' +
    'Puedes ver los registros de ejecucion en la hoja LOG_ETL.'
  );
}

/** Muestra un alert solo si estamos en contexto UI (no desde trigger). */
function etl_alertUI(titulo, mensaje) {
  try {
    SpreadsheetApp.getUi().alert(titulo, mensaje, SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (e) {
    // Llamado desde trigger o sin UI — registrar en log en su lugar
    etl_log('INFO', 'etl_alertUI', titulo + ': ' + mensaje);
  }
}

/** Desinstala todos los triggers de este script */
function desinstalar_Triggers() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i]);
  }
  etl_finalizarCadena();
  etl_alertUI('Triggers eliminados', 'Todos los triggers han sido eliminados.');
}


// ==========================================================================
// MENÚ PERSONALIZADO
// ==========================================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('📊 Analytics ETL')
    .addItem('⚡ Refrescar Dashboard Web',       'runETL_DashboardRapido')
    .addItem('⚡ Ejecutar ETL Completo',          'runETL_Completo')
    .addSubMenu(SpreadsheetApp.getUi().createMenu('⛓️ Cadena ETL (si se cortó el trigger)')
      .addItem('▶ Parte 2 — Embudo + tiempos',     'runETL_Completo_Parte2')
      .addItem('▶ Parte 3 — Tipif (DATA_TIPIF_DIA)',   'runETL_Completo_Parte3')
      .addItem('▶ Parte 4 — RPT Asesor / OPC',     'runETL_Completo_Parte4')
      .addItem('▶ Parte 5 — Payload Dashboard',     'runETL_Completo_Parte5')
      .addSeparator()
      .addItem('🧹 Borrar triggers cadena (2/3/4/5)',  'etl_borraTriggersCadenaEtl_UI'))
    .addSeparator()
    .addSubMenu(SpreadsheetApp.getUi().createMenu('📈 Ver Reportes')
      .addItem('📊 Embudo Completo',              'ir_a_embudo')
      .addItem('👤 Reporte Asesores',             'ir_a_rpt_asesores')
      .addItem('🏷️  Reporte OPC',                 'ir_a_rpt_opc'))
    .addSeparator()
    .addItem('🏗️  Setup Inicial (primera vez)',   'setup_ArchivoAnalytics')
    .addItem('⏰ Instalar Trigger Dashboard (30 min)', 'instalar_Triggers')
    .addItem('🔕 Desinstalar Triggers',             'desinstalar_Triggers')
    .addSeparator()
    .addItem('🔄 Limpiar Cache Asesores',           'invalidar_cache_asesores')
    .addItem('🔍 Diagnóstico Embudo',                'runDiagnosticoEmbudo')
    .addItem('📋 Ver Log ETL',                      'ir_a_log')
    .addItem('⚠️  Ver Pendientes de Mapeo',         'ir_a_pendientes')
    .addToUi();
}

function ir_a_log() {
  var ss = getSpreadsheetDestino();
  var sh = ss.getSheetByName(CFG.OUT.LOG);
  if (sh) ss.setActiveSheet(sh);
}

function ir_a_pendientes() {
  var ss = getSpreadsheetDestino();
  var sh = ss.getSheetByName(CFG.OUT.PEND);
  if (sh) ss.setActiveSheet(sh);
}

function ir_a_embudo() {
  var ss = getSpreadsheetDestino();
  var sh = ss.getSheetByName(CFG.OUT.EMBUDO);
  if (sh) ss.setActiveSheet(sh);
}

function ir_a_rpt_asesores() {
  var ss = getSpreadsheetDestino();
  var sh = ss.getSheetByName(CFG.OUT.RPT_AS);
  if (sh) ss.setActiveSheet(sh);
}

function ir_a_rpt_opc() {
  var ss = getSpreadsheetDestino();
  var sh = ss.getSheetByName(CFG.OUT.RPT_OPC);
  if (sh) ss.setActiveSheet(sh);
}
