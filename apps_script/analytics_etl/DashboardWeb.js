/**
 * Dashboard Web Trigal Sur
 * Modulo separado para consumir las tablas DASH_* generadas por Analytics_ETL.gs.
 */

var DASHBOARD_WEB_SNAPSHOT_CACHE_KEY = 'TRIGAL_DASHBOARD_WEB_SNAPSHOT_V6';
var DASHBOARD_WEB_SNAPSHOT_FILE_ID_KEY = 'TRIGAL_DASHBOARD_WEB_SNAPSHOT_FILE_ID';
var DASHBOARD_WEB_BOOTSTRAP_CACHE_KEY = 'TRIGAL_DASHBOARD_WEB_BOOTSTRAP_V6';
var DASHBOARD_WEB_BOOTSTRAP_FILE_ID_KEY = 'TRIGAL_DASHBOARD_WEB_BOOTSTRAP_FILE_ID';
var DASHBOARD_WEB_PAYLOAD_CACHE_KEY = 'TRIGAL_DASHBOARD_WEB_PAYLOAD_V3';
var DASHBOARD_WEB_PAYLOAD_FILE_ID_KEY = 'TRIGAL_DASHBOARD_WEB_PAYLOAD_FILE_ID_V3';
var DASHBOARD_WEB_RESPONSE_PREFIX = 'TRIGAL_DASHBOARD_RESPONSE_V6_';
var DASHBOARD_WEB_PRESET_RESPONSES_CACHE_KEY = 'TRIGAL_DASHBOARD_PRESET_RESPONSES_V6';
var DASHBOARD_WEB_PRESET_RESPONSES_FILE_ID_KEY = 'TRIGAL_DASHBOARD_PRESET_RESPONSES_FILE_ID';
var DASHBOARD_WEB_CACHE_TTL = 21600; // 6 horas, refrescado por cada ETL exitoso.
var DASHBOARD_WEB_RESPONSE_TTL = 180; // Respuestas agregadas por combinacion de filtros.
var DASHBOARD_WEB_CHUNK_SIZE = 85000;
var DASHBOARD_WEB_MEMO_SNAPSHOT = null;
var DASHBOARD_WEB_MEMO_GENERATED_AT = '';

function doGet(e) {
  var action = e && e.parameter ? String(e.parameter.action || '').trim() : '';
  if (action === 'publishSnapshot') {
    return dashboardJsonOutput_(publicarSnapshotDashboard());
  }
  if (action === 'health') {
    return dashboardJsonOutput_(getDashboardBootstrap());
  }
  if (action === 'debugSummary') {
    return dashboardJsonOutput_(JSON.parse(getDashboardPayloadSummary()));
  }
  if (action === 'publishWebPayload') {
    return dashboardJsonOutput_(dashboardProgramarWebPayload_());
  }
  if (action === 'payloadHealth') {
    return dashboardJsonOutput_(dashboardPayloadHealth_());
  }
  if (action === 'debugData') {
    return dashboardJsonOutput_(JSON.parse(getDashboardPayload()));
  }
  var tpl = HtmlService.createTemplateFromFile('Dashboard');
  tpl.appTitle = 'Trigal Sur BI';
  return tpl.evaluate()
    .setTitle('Trigal Sur BI')
    .setSandboxMode(HtmlService.SandboxMode.IFRAME)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function dashboardJsonOutput_(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload || {}))
    .setMimeType(ContentService.MimeType.JSON);
}

function dashboardInclude(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

function getDashboardBootstrap() {
  try {
    var boot = dashboardLoadBootstrap_();
    var filters = boot.filters || [];
    var optionSets = {};
    var meta = {};

    for (var i = 0; i < filters.length; i++) {
      var cat = String(filters[i].CATEGORIA || '').trim();
      var val = String(filters[i].VALOR || '').trim();
      if (!cat) continue;
      if (cat.indexOf('META_') === 0) {
        meta[cat] = dashboardNormalizeMetaValue_(cat, val);
        continue;
      }
      val = dashboardNormalizeDimensionValue_(cat, val);
      if (!val) continue;
      if (!optionSets[cat]) optionSets[cat] = {};
      optionSets[cat][val] = true;
    }

    var options = {};
    for (var optCat in optionSets) {
      options[optCat] = dashboardSortOptions_(optCat, Object.keys(optionSets[optCat]));
    }

    var health = dashboardBootstrapHealth_(boot);
    var defaultDates = dashboardDefaultDateRange_(meta);
    return {
      ok: true,
      options: options,
      meta: meta,
      health: health,
      defaults: {
        mode: 'COHORTE',
        tipoFecha: 'CAPTACION',
        dateStart: defaultDates.dateStart,
        dateEnd: defaultDates.dateEnd
      }
    };
  } catch (e) {
    return dashboardBootstrapError_(e);
  }
}

function dashboardSortOptions_(category, values) {
  var orderMap = {};
  var fixed = [];
  if (category === 'TIPO_FECHA') fixed = ['CAPTACION', 'ASIGNACION', 'GESTION', 'CITA', 'PRESENCIA', 'TOUR', 'SEPARACION', 'PROCESABLE'];
  if (category === 'ASIGNADO_ESTADO') fixed = ['ASIGNADO', 'NO_ASIGNADO'];
  for (var i = 0; i < fixed.length; i++) orderMap[fixed[i]] = i + 1;
  values.sort(function(a, b) {
    var oa = orderMap[a] || 999;
    var ob = orderMap[b] || 999;
    if (oa !== ob) return oa - ob;
    return dashboardTextKey_(a) < dashboardTextKey_(b) ? -1 : (dashboardTextKey_(a) > dashboardTextKey_(b) ? 1 : 0);
  });
  return values;
}

function dashboardNormalizeMetaValue_(category, value) {
  var cat = String(category || '').toUpperCase();
  if (cat.indexOf('FECHA') !== -1) {
    var d = dashboardNormalizeDate_(value);
    if (!d) return '';
    d.setHours(0, 0, 0, 0);
    if (cat.indexOf('MAX_') !== -1 && d.getTime() > dashboardToday_().getTime()) d = dashboardToday_();
    return dashboardIsSaneDate_(d) ? Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd') : '';
  }
  return value;
}

function dashboardDefaultDateRange_(meta) {
  meta = meta || {};
  var minDate = dashboardToDate_(meta.META_MIN_FECHA_CAPTACION);
  var maxDate = dashboardToDate_(meta.META_MAX_FECHA_CAPTACION);
  var today = dashboardToday_();
  var end = today;
  var anchor = maxDate && maxDate.getTime() <= today.getTime() ? maxDate : today;
  var start = new Date(anchor.getFullYear(), anchor.getMonth(), 1);
  if (minDate && start.getTime() < minDate.getTime()) start = minDate;
  return {
    dateStart: dashboardDateKey_(start),
    dateEnd: dashboardDateKey_(end)
  };
}

function dashboardToday_() {
  var parts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd').split('-');
  return new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]));
}

function dashboardNormalizeDimensionValue_(category, value) {
  var cat = String(category || '').toUpperCase().trim();
  if (cat === 'FUENTE' || cat === 'FUENTE_NORMALIZADA') return dashboardNormalizeFuente_(value);
  if (cat === 'PROYECTO') return dashboardNormalizeProyecto_(value);
  if (cat === 'ESTADO_CIVIL') return dashboardNormalizeEstadoCivil_(value);
  if (cat === 'DISTRITO') return dashboardNormalizeDistrito_(value);
  if (cat === 'NOMBRE_OPC' || cat === 'OPC') return dashboardNormalizePersonName_(value, 'SIN_OPC');
  if (cat === 'ASESOR' || cat === 'ASESOR_ULTIMA_GESTION') return dashboardNormalizePersonName_(value, 'SIN_ASESOR');
  if (cat === 'TIPIFICACION' || cat === 'TIPIFICACION_AGRUPADA' || cat === 'TIPIFICACION_AGRUPADA_ULTIMA') {
    return dashboardCleanLabel_(value, 'SIN_TIPIFICACION');
  }
  if (cat === 'ASIGNADO_ESTADO') {
    var asg = dashboardTextKey_(value);
    if (asg === 'ASIGNADO') return 'ASIGNADO';
    if (asg === 'NO ASIGNADO' || asg === 'NO_ASIGNADO') return 'NO_ASIGNADO';
    return '';
  }
  if (cat === 'TIPO_FECHA') return dashboardCleanLabel_(value, '');
  return dashboardCleanLabel_(value, '');
}

function dashboardNormalizeFuente_(value) {
  var key = dashboardTextKey_(value);
  if (!key || key === 'SIN FUENTE' || key === 'SIN_FUENTE' || key === 'NUEVO DESCONOCIDO' || key === 'NUEVO / DESCONOCIDO') return 'SIN_FUENTE';
  if (key === 'FB MSG' || key === 'MSG FB' || key === 'FACEBOOK' || key === 'FACEBOOK ADS' || key === 'FB' || key === 'META ADS') return 'META';
  if (key === 'WHATSAPP' || key === 'WSP' || key === 'WA') return 'WSP';
  if (key.indexOf('TIKTOK') !== -1 || key === 'TIK TOK') return 'TIKTOK';
  if (key.indexOf('MIGRACION') !== -1) return 'MIGRACION';
  if (key.indexOf('REASIGNADO') !== -1) return 'REASIGNADO';
  if (key.indexOf('REMARCADO') !== -1) return 'REMARCADO';
  if (key.indexOf('RECICLADO') !== -1) return 'RECICLADO';
  if (key.indexOf('OPC') !== -1) return 'OPC';
  return dashboardCleanLabel_(value, 'SIN_FUENTE');
}

function dashboardNormalizeProyecto_(value) {
  var key = dashboardTextKey_(value);
  if (!key || key === 'SIN PROYECTO' || key === 'SIN_PROYECTO' || key === 'N/A') return 'SIN_PROYECTO';
  if (key.indexOf('CARABAYL') !== -1) return 'CARABAYLLO';
  if (key.indexOf('CHANCHAMAYO') !== -1) return 'CHANCHAMAYO';
  if (key.indexOf('CHANCAY') !== -1) return 'CHANCAY';
  if (key.indexOf('CHILCA') !== -1) return 'CHILCA';
  if (key.indexOf('LURIN') !== -1) return 'LURIN';
  return dashboardCleanLabel_(value, 'SIN_PROYECTO');
}

function dashboardNormalizeEstadoCivil_(value) {
  var key = dashboardTextKey_(value);
  if (!key || key.indexOf('NO ESPEC') !== -1 || key.indexOf('NO MENCION') !== -1 || key === 'SIN ESTADO' || key === 'SIN_ESTADO') return 'NO ESPECIFICADO';
  if (key.indexOf('CASAD') !== -1) return 'CASADO/A';
  if (key.indexOf('SOLTER') !== -1) return 'SOLTERO/A';
  if (key.indexOf('SEPARAD') !== -1) return 'SEPARADO/A';
  if (key.indexOf('VIUD') !== -1) return 'VIUDO/A';
  if (key.indexOf('CONVIV') !== -1) return 'CONVIVIENTE';
  return dashboardCleanLabel_(value, 'NO ESPECIFICADO');
}

function dashboardNormalizeDistrito_(value) {
  var key = dashboardTextKey_(value);
  if (!key || /^[.\-\s]+$/.test(String(value || '')) || key.indexOf('NO ESPEC') !== -1 ||
      key.indexOf('NO MENCION') !== -1 || key.indexOf('NO LO MENCION') !== -1 ||
      key.indexOf('NO QUI') !== -1 || key === 'SIN DISTRITO' || key === 'SIN_DISTRITO') {
    return 'NO ESPECIFICADO';
  }
  var map = {
    'BRENA': 'BRENA',
    'CANETE': 'CANETE',
    'CALLAO CERCADO': 'CALLAO',
    'CALLAO (CERCADO)': 'CALLAO',
    'CANTA CALLAO': 'CALLAO',
    'CERCADO LIMA': 'CERCADO DE LIMA',
    'LIMA (CERCADO)': 'CERCADO DE LIMA',
    'CENTRO DE LIMA': 'CERCADO DE LIMA',
    'CARABAYLL': 'CARABAYLLO',
    'CHORRILOS': 'CHORRILLOS',
    'COMAS.': 'COMAS',
    'AGUSTINO': 'EL AGUSTINO',
    'INDEPENDIENTE': 'INDEPENDENCIA',
    'OLIVOS': 'LOS OLIVOS',
    'PTE PIEDRA': 'PUENTE PIEDRA',
    'SJL': 'SAN JUAN DE LURIGANCHO',
    'SJM': 'SAN JUAN DE MIRAFLORES',
    'SMP': 'SAN MARTIN DE PORRES',
    'LURIGANCHO': 'LURIGANCHO-CHOSICA',
    'LURIN': 'LURIN'
  };
  if (map[key]) return map[key];
  if (key.indexOf('CARMEN DE LA LEGUA') !== -1) return 'CARMEN DE LA LEGUA';
  if (key.indexOf('MAGDALENA') !== -1) return 'MAGDALENA DEL MAR';
  if (key.indexOf('VILLA MARIA') !== -1) return 'VILLA MARIA DEL TRIUNFO';
  if (key.indexOf('VILLA EL SALVADOR') !== -1) return 'VILLA EL SALVADOR';
  return dashboardCleanLabel_(value, 'NO ESPECIFICADO');
}

function dashboardNormalizePersonName_(value, fallback) {
  var key = dashboardTextKey_(value);
  if (!key || key === 'SIN DATO') return fallback || '';
  return key;
}

function dashboardCleanLabel_(value, fallback) {
  var s = String(value == null ? '' : value).trim();
  if (!s) return fallback || '';
  s = s.replace(/_/g, ' ').replace(/\s+/g, ' ').trim().toUpperCase();
  return s || fallback || '';
}

function dashboardTextKey_(value) {
  var s = String(value == null ? '' : value).trim().toUpperCase();
  try {
    s = s.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  } catch (e) {}
  return s.replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function getDashboardHealth() {
  return dashboardBootstrapHealth_(dashboardLoadBootstrap_());
}

function getDashboardPayload() {
  try {
    var cached = dashboardCacheGetChunked_(DASHBOARD_WEB_PAYLOAD_CACHE_KEY);
    if (cached) return cached;
    var driveJson = dashboardLoadDriveJsonByProperty_(DASHBOARD_WEB_PAYLOAD_FILE_ID_KEY);
    if (driveJson) {
      dashboardCachePutChunked_(DASHBOARD_WEB_PAYLOAD_CACHE_KEY, driveJson, DASHBOARD_WEB_CACHE_TTL);
      return driveJson;
    }
    return JSON.stringify(dashboardPayloadNotReady_());
  } catch (e) {
    return JSON.stringify({
      ok: false,
      error: dashboardErrorMessage_(e),
      generatedAt: new Date().toISOString(),
      data: { leads: [], events: [], alerts: [] },
      catalogs: {},
      defaults: { mode: 'COHORTE', tipoFecha: 'CAPTACION', dateStart: '', dateEnd: '' }
    });
  }
}

function dashboardPayloadNotReady_() {
  return {
    ok: false,
    error: 'PAYLOAD_WEB_NO_PUBLICADO',
    message: 'El payload web compacto aun no esta publicado. Ejecuta action=publishWebPayload y espera 1 a 3 minutos.',
    generatedAt: new Date().toISOString(),
    data: { leads: [], events: [], alerts: [] },
    catalogs: {},
    defaults: { mode: 'COHORTE', tipoFecha: 'CAPTACION', dateStart: '', dateEnd: '' }
  };
}

function dashboardBuildAndSaveWebPayloadFromSnapshot_() {
  var snapshot = dashboardLoadSnapshot_();
  var payload = dashboardBuildWebPayloadFromSnapshot_(snapshot);
  var json = JSON.stringify(payload);
  dashboardSaveWebPayloadJson_(json);
  return json;
}

function dashboardPublishWebPayloadOnly_() {
  var json = dashboardBuildAndSaveWebPayloadFromSnapshot_();
  var payload = JSON.parse(json);
  return {
    ok: payload.ok !== false,
    generatedAt: payload.generatedAt || new Date().toISOString(),
    snapshotGeneratedAt: payload.snapshotGeneratedAt || '',
    counts: payload.counts || {},
    diagnostics: payload.diagnostics || {}
  };
}

function dashboardProgramarWebPayload_() {
  try {
    if (typeof runETL_DashboardRapido_Manual === 'function') {
      var fastHandler = 'runETL_DashboardRapido_Manual';
      dashboardDeleteTriggersForHandler_(fastHandler);
      ScriptApp.newTrigger(fastHandler).timeBased().after(1000).create();
      return {
        ok: true,
        scheduled: fastHandler,
        message: 'Actualizacion rapida programada: STG -> payload web. Espera 3 a 6 minutos y abre action=payloadHealth.',
        generatedAt: new Date().toISOString()
      };
    }
    if (typeof etl_programarSiguienteCadena === 'function') {
      etl_programarSiguienteCadena('runETL_Completo_Parte5');
      return {
        ok: true,
        scheduled: 'runETL_Completo_Parte5',
        message: 'Payload web fresco programado desde STG. Espera 2 a 5 minutos y abre action=payloadHealth.',
        generatedAt: new Date().toISOString()
      };
    }
    var handler = 'dashboard_runPublishWebPayload';
    dashboardDeleteTriggersForHandler_(handler);
    ScriptApp.newTrigger(handler).timeBased().after(1000).create();
    return {
      ok: true,
      scheduled: handler,
      message: 'Payload web programado desde snapshot. Espera 1 a 3 minutos y abre action=payloadHealth.',
      generatedAt: new Date().toISOString()
    };
  } catch (e) {
    return {
      ok: false,
      error: dashboardErrorMessage_(e),
      generatedAt: new Date().toISOString()
    };
  }
}

function dashboard_runPublishWebPayload() {
  dashboardDeleteTriggersForHandler_('dashboard_runPublishWebPayload');
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(300000);
  } catch (e) {
    dashboardLog_('ERROR', 'dashboard_runPublishWebPayload', 'Lock no disponible: ' + dashboardErrorMessage_(e));
    return;
  }
  try {
    var t0 = Date.now();
    var summary = dashboardPublishWebPayloadOnly_();
    dashboardLog_(
      'INFO',
      'dashboard_runPublishWebPayload',
      'Payload web publicado en ' + (Date.now() - t0) + 'ms | leads ' +
        ((summary.counts && summary.counts.leads) || 0) + ' | events ' +
        ((summary.counts && summary.counts.events) || 0)
    );
  } catch (e2) {
    dashboardLog_('ERROR', 'dashboard_runPublishWebPayload', dashboardErrorMessage_(e2));
  } finally {
    try { lock.releaseLock(); } catch (e3) {}
  }
}

function dashboardPayloadHealth_() {
  var out = {
    ok: false,
    cacheKey: DASHBOARD_WEB_PAYLOAD_CACHE_KEY,
    fileProperty: DASHBOARD_WEB_PAYLOAD_FILE_ID_KEY,
    generatedAt: new Date().toISOString()
  };
  try {
    var props = PropertiesService.getScriptProperties();
    var fileId = props.getProperty(DASHBOARD_WEB_PAYLOAD_FILE_ID_KEY);
    out.fileId = fileId || '';
    if (!fileId) {
      out.error = 'SIN_FILE_ID';
      return out;
    }
    var file = DriveApp.getFileById(fileId);
    out.ok = true;
    out.name = file.getName();
    out.sizeBytes = file.getSize ? file.getSize() : null;
    out.updatedAt = file.getLastUpdated ? file.getLastUpdated().toISOString() : '';
    return out;
  } catch (e) {
    out.error = dashboardErrorMessage_(e);
    return out;
  }
}

function dashboardBuildWebPayloadFromSnapshot_(snapshot) {
  snapshot = snapshot || dashboardEmptySnapshot_('EMPTY');
  var boot = dashboardBuildBootstrapFromSnapshot_(snapshot);
  var filters = boot.filters || [];
  var optionSets = {};
  var meta = {};

  for (var i = 0; i < filters.length; i++) {
    var cat = String(filters[i].CATEGORIA || '').trim();
    var val = String(filters[i].VALOR || '').trim();
    if (!cat) continue;
    if (cat.indexOf('META_') === 0) {
      meta[cat] = dashboardNormalizeMetaValue_(cat, val);
      continue;
    }
    val = dashboardNormalizeDimensionValue_(cat, val);
    if (!val) continue;
    if (!optionSets[cat]) optionSets[cat] = {};
    optionSets[cat][val] = true;
  }

  var catalogs = {};
  for (var optCat in optionSets) {
    catalogs[optCat] = dashboardSortOptions_(optCat, Object.keys(optionSets[optCat]));
  }
  var defaults = dashboardDefaultDateRange_(meta);
  var eventScope = dashboardSnapshotEventScope_();
  var compactOptions = {};
  if (eventScope === 'DEFAULT_RANGE') {
    compactOptions.eventDateStart = defaults.dateStart;
    compactOptions.eventDateEnd = defaults.dateEnd;
  }
  var compact = dashboardCompactSnapshotForWeb_(snapshot, compactOptions);
  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    snapshotGeneratedAt: snapshot.generatedAt || '',
    defaults: {
      mode: 'COHORTE',
      tipoFecha: 'CAPTACION',
      dateStart: defaults.dateStart,
      dateEnd: defaults.dateEnd
    },
    meta: meta,
    catalogs: catalogs,
    counts: snapshot.counts || {},
    diagnostics: dashboardSnapshotDiagnostics_(snapshot),
    dataScope: {
      leads: 'ALL',
      events: eventScope,
      eventDateStart: defaults.dateStart,
      eventDateEnd: defaults.dateEnd
    },
    data: compact
  };
}

function getDashboardPayloadSummary() {
  try {
    var payload = JSON.parse(getDashboardPayload());
    var compact = payload.data || { leads: [], events: [], alerts: [] };
    var defaults = payload.defaults || { mode: 'COHORTE', tipoFecha: 'CAPTACION', dateStart: '', dateEnd: '' };
    var catalogs = payload.catalogs || {};
    var defaultFilters = {
      mode: 'COHORTE',
      tipoFecha: 'CAPTACION',
      dateStart: defaults.dateStart,
      dateEnd: defaults.dateEnd
    };
    return JSON.stringify({
      ok: true,
      generatedAt: new Date().toISOString(),
      snapshotGeneratedAt: payload.snapshotGeneratedAt || '',
      counts: payload.counts || {},
      diagnostics: payload.diagnostics || {},
      defaults: defaultFilters,
      catalogSizes: dashboardCatalogSizes_(catalogs),
      sampleLead: compact.leads[0] || null,
      sampleEvent: compact.events[0] || null,
      smoke: {
        cohorteDefault: dashboardSmokeLeadKpis_(compact.leads, defaultFilters),
        eventoCaptacionDefault: dashboardSmokeEventKpis_(compact.events, defaultFilters)
      }
    });
  } catch (e) {
    return JSON.stringify({
      ok: false,
      error: dashboardErrorMessage_(e),
      generatedAt: new Date().toISOString()
    });
  }
}

function refreshDashboardCache() {
  actualizarDashboardWeb();
  return getDashboardBootstrap();
}

function dashboard_publicarSnapshotDesdeDashSheets_Manual() {
  return dashboardProgramarActualizacionSnapshot_();
}

function publicarSnapshotDashboard() {
  return dashboardProgramarActualizacionSnapshot_();
}

function actualizarDashboardWeb() {
  return dashboardProgramarActualizacionSnapshot_();
}

function publicarSnapshotDashboard_UI() {
  var result = publicarSnapshotDashboard();
  etl_alertUI(
    'Actualizacion dashboard programada',
    'Se programo Parte 5 para generar el snapshot del dashboard en segundo plano. ' +
    'Espera 2 a 5 minutos y recarga la Web App. Estado: ' + (result.ok ? 'OK' : result.error)
  );
}

function dashboardProgramarActualizacionSnapshot_() {
  try {
    if (typeof runETL_DashboardRapido_Manual === 'function') {
      var fastHandler = 'runETL_DashboardRapido_Manual';
      dashboardDeleteTriggersForHandler_(fastHandler);
      ScriptApp.newTrigger(fastHandler).timeBased().after(1000).create();
      return { ok: true, scheduled: fastHandler, generatedAt: new Date() };
    }
    etl_programarSiguienteCadena('runETL_Completo_Parte5');
    return { ok: true, scheduled: 'runETL_Completo_Parte5', generatedAt: new Date() };
  } catch (e) {
    return { ok: false, error: dashboardErrorMessage_(e), generatedAt: new Date() };
  }
}

function getDashboardData(filters) {
  try {
    filters = filters || {};
    var mode = String(filters.mode || 'COHORTE').toUpperCase();
    var boot = dashboardLoadBootstrap_();
    var responseKey = dashboardResponseCacheKey_(filters, boot);
    var cached = dashboardCacheGet_(responseKey);
    if (cached) {
      try { return JSON.parse(cached); } catch (e0) {}
    }
    var precomputed = dashboardLoadPrecomputedResponse_(responseKey);
    if (precomputed) return precomputed;

    var snapshot = dashboardLoadSnapshot_();
    var leads = snapshot.leads || [];
    var events = snapshot.events || [];
    var alerts = snapshot.alerts || [];
    var response;

    if (mode === 'EVENTO') {
      response = dashboardBuildEventoResponse_(events, alerts, filters);
    } else {
      response = dashboardBuildCohorteResponse_(leads, events, alerts, filters);
    }
    response.snapshotGeneratedAt = snapshot.generatedAt || '';
    dashboardCachePut_(responseKey, JSON.stringify(response), DASHBOARD_WEB_RESPONSE_TTL);
    return response;
  } catch (e) {
    return dashboardDataError_(e);
  }
}

function dashboardBuildCohorteResponse_(leads, events, alerts, filters) {
  var selected = [];
  for (var i = 0; i < leads.length; i++) {
    var row = leads[i];
    if (!dashboardIsTruthy_(row.ES_LEAD_CRM)) continue;
    if (!dashboardDateInRange_(row.FECHA_CAPTACION_LEAD, filters.dateStart, filters.dateEnd)) continue;
    if (!dashboardPassDimensionFilters_(row, filters)) continue;
    selected.push(row);
  }

  var kpis = dashboardCalcLeadKpis_(selected);
  var funnel = dashboardBuildFunnel_(kpis);
  var sourceDonut = dashboardGroupLeads_(selected, 'FUENTE_NORMALIZADA', 'TOTAL');
  var districtDonut = dashboardGroupLeads_(selected, 'DISTRITO', 'TOTAL');
  var tipifDonut = dashboardGroupLeads_(selected, 'TIPIFICACION_AGRUPADA_ULTIMA', 'TOTAL');
  var advisorRank = dashboardGroupLeads_(selected, 'ASESOR_ULTIMA_GESTION', 'CONTACTABLES');
  var opcRank = dashboardGroupLeads_(selected, 'NOMBRE_OPC', 'TOTAL');
  var trendFilters = dashboardClone_(filters);
  trendFilters.tipoFecha = 'TODOS';
  var trends = dashboardBuildTrendFromEvents_(events, trendFilters);
  var alertRows = dashboardFilterAlerts_(alerts, filters);

  return {
    ok: true,
    mode: 'COHORTE',
    kpis: kpis,
    funnel: funnel,
    charts: {
      trends: trends,
      sourceDonut: sourceDonut.slice(0, 8),
      districtDonut: districtDonut.slice(0, 8),
      tipifDonut: tipifDonut.slice(0, 10),
      advisorRank: advisorRank.slice(0, 12),
      opcRank: opcRank.slice(0, 12)
    },
    alerts: alertRows.slice(0, 40),
    rows: selected.length,
    generatedAt: new Date()
  };
}

function dashboardBuildEventoResponse_(events, alerts, filters) {
  var selected = [];
  for (var i = 0; i < events.length; i++) {
    var row = events[i];
    if (!dashboardDateInRange_(row.FECHA, filters.dateStart, filters.dateEnd)) continue;
    if (filters.tipoFecha && filters.tipoFecha !== 'TODOS' && String(row.TIPO_FECHA || '') !== String(filters.tipoFecha)) continue;
    if (!dashboardPassDimensionFilters_(row, filters)) continue;
    selected.push(row);
  }

  var kpis = dashboardCalcEventKpis_(selected);
  var funnel = dashboardBuildFunnel_(kpis);
  var trends = dashboardRowsToTrend_(selected);
  var sourceDonut = dashboardGroupEvents_(selected, 'FUENTE_NORMALIZADA', 'LEADS');
  var districtDonut = dashboardGroupEvents_(selected, 'DISTRITO', 'LEADS');
  var tipifDonut = dashboardGroupEvents_(selected, 'TIPIFICACION_AGRUPADA', 'GESTIONES');
  var advisorRank = dashboardGroupEvents_(selected, 'ASESOR_ULTIMA_GESTION', 'GESTIONES');
  var opcRank = dashboardGroupEvents_(selected, 'NOMBRE_OPC', 'LEADS');
  var alertRows = dashboardFilterAlerts_(alerts, filters);

  return {
    ok: true,
    mode: 'EVENTO',
    kpis: kpis,
    funnel: funnel,
    charts: {
      trends: trends,
      sourceDonut: sourceDonut.slice(0, 8),
      districtDonut: districtDonut.slice(0, 8),
      tipifDonut: tipifDonut.slice(0, 10),
      advisorRank: advisorRank.slice(0, 12),
      opcRank: opcRank.slice(0, 12)
    },
    alerts: alertRows.slice(0, 40),
    rows: selected.length,
    generatedAt: new Date()
  };
}

function dashboardCalcLeadKpis_(rows) {
  var out = {
    totalLeads: rows.length,
    contactables: 0,
    potenciales: 0,
    citas: 0,
    presencias: 0,
    tours: 0,
    separaciones: 0,
    procesables: 0
  };
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    if (dashboardIsTruthy_(r.ES_CONTACTABLE)) out.contactables++;
    if (dashboardIsTruthy_(r.ES_POTENCIAL)) out.potenciales++;
    if (dashboardIsTruthy_(r.TIENE_CITA)) out.citas++;
    if (dashboardIsTruthy_(r.TIENE_PRESENCIA)) out.presencias++;
    if (dashboardIsTruthy_(r.TIENE_TOUR)) out.tours++;
    if (dashboardIsTruthy_(r.TIENE_SEPARACION)) out.separaciones++;
    if (dashboardIsTruthy_(r.TIENE_PROCESABLE)) out.procesables++;
  }
  dashboardAttachRates_(out);
  return out;
}

function dashboardCalcEventKpis_(rows) {
  var out = {
    totalLeads: 0,
    contactables: 0,
    potenciales: 0,
    citas: 0,
    presencias: 0,
    tours: 0,
    separaciones: 0,
    procesables: 0,
    gestiones: 0,
    asignados: 0
  };
  for (var i = 0; i < rows.length; i++) {
    out.totalLeads += Number(rows[i].LEADS || 0);
    out.asignados += Number(rows[i].ASIGNADOS || 0);
    out.gestiones += Number(rows[i].GESTIONES || 0);
    out.contactables += Number(rows[i].CONTACTABLES || 0);
    out.potenciales += Number(rows[i].POTENCIALES || 0);
    out.citas += Number(rows[i].CITAS || 0);
    out.presencias += Number(rows[i].PRESENCIAS || 0);
    out.tours += Number(rows[i].TOURS || 0);
    out.separaciones += Number(rows[i].SEPARACIONES || 0);
    out.procesables += Number(rows[i].PROCESABLES || 0);
  }
  dashboardAttachRates_(out);
  return out;
}

function dashboardAttachRates_(out) {
  out.leadToCita = dashboardDiv_(out.citas, out.totalLeads);
  out.leadToSeparacion = dashboardDiv_(out.separaciones, out.totalLeads);
  out.leadToProcesable = dashboardDiv_(out.procesables, out.totalLeads);
  out.contactRate = dashboardDiv_(out.contactables, out.totalLeads);
  return out;
}

function dashboardBuildFunnel_(kpis) {
  return [
    { label: 'Leads', value: kpis.totalLeads },
    { label: 'Contactables', value: kpis.contactables },
    { label: 'Potenciales', value: kpis.potenciales },
    { label: 'Citas reales', value: kpis.citas },
    { label: 'Presencias', value: kpis.presencias },
    { label: 'Tours validos', value: kpis.tours },
    { label: 'Separaciones', value: kpis.separaciones },
    { label: 'Procesables', value: kpis.procesables }
  ];
}

function dashboardBuildTrendFromEvents_(events, filters) {
  var selected = [];
  var tipoFecha = filters.tipoFecha || 'TODOS';
  for (var i = 0; i < events.length; i++) {
    var row = events[i];
    if (!dashboardDateInRange_(row.FECHA, filters.dateStart, filters.dateEnd)) continue;
    if (tipoFecha !== 'TODOS' && String(row.TIPO_FECHA || '') !== String(tipoFecha)) continue;
    if (!dashboardPassDimensionFilters_(row, filters)) continue;
    selected.push(row);
  }
  return dashboardRowsToTrend_(selected);
}

function dashboardRowsToTrend_(rows) {
  var map = {};
  for (var i = 0; i < rows.length; i++) {
    var d = dashboardDateKey_(rows[i].FECHA);
    if (!d) continue;
    if (!map[d]) map[d] = { date: d, leads: 0, gestiones: 0, asignados: 0, citas: 0, presencias: 0, separaciones: 0, procesables: 0 };
    map[d].leads += Number(rows[i].LEADS || 0);
    map[d].gestiones += Number(rows[i].GESTIONES || 0);
    map[d].asignados += Number(rows[i].ASIGNADOS || 0);
    map[d].citas += Number(rows[i].CITAS || 0);
    map[d].presencias += Number(rows[i].PRESENCIAS || 0);
    map[d].separaciones += Number(rows[i].SEPARACIONES || 0);
    map[d].procesables += Number(rows[i].PROCESABLES || 0);
  }
  var keys = Object.keys(map).sort();
  var out = [];
  for (var k = 0; k < keys.length; k++) out.push(map[keys[k]]);
  return out;
}

function dashboardGroupLeads_(rows, field, metric) {
  var map = {};
  var category = dashboardCategoryForField_(field);
  for (var i = 0; i < rows.length; i++) {
    var key = category ? dashboardNormalizeDimensionValue_(category, rows[i][field]) : (String(rows[i][field] || 'SIN_DATO').trim() || 'SIN_DATO');
    if (!map[key]) map[key] = { label: key, value: 0 };
    if (metric === 'CONTACTABLES') map[key].value += dashboardIsTruthy_(rows[i].ES_CONTACTABLE) ? 1 : 0;
    else map[key].value++;
  }
  return dashboardSortGroups_(map);
}

function dashboardGroupEvents_(rows, field, metric) {
  var map = {};
  var category = dashboardCategoryForField_(field);
  for (var i = 0; i < rows.length; i++) {
    var key = category ? dashboardNormalizeDimensionValue_(category, rows[i][field]) : (String(rows[i][field] || 'SIN_DATO').trim() || 'SIN_DATO');
    if (!map[key]) map[key] = { label: key, value: 0 };
    map[key].value += Number(rows[i][metric] || 0);
  }
  return dashboardSortGroups_(map);
}

function dashboardCategoryForField_(field) {
  if (field === 'FUENTE_NORMALIZADA') return 'FUENTE';
  if (field === 'PROYECTO') return 'PROYECTO';
  if (field === 'ESTADO_CIVIL') return 'ESTADO_CIVIL';
  if (field === 'DISTRITO') return 'DISTRITO';
  if (field === 'NOMBRE_OPC') return 'NOMBRE_OPC';
  if (field === 'ASESOR_ULTIMA_GESTION') return 'ASESOR';
  if (field === 'TIPIFICACION_AGRUPADA' || field === 'TIPIFICACION_AGRUPADA_ULTIMA') return 'TIPIFICACION';
  return '';
}

function dashboardSortGroups_(map) {
  var out = [];
  for (var key in map) out.push(map[key]);
  out.sort(function(a, b) { return b.value - a.value; });
  return out;
}

function dashboardClone_(obj) {
  var out = {};
  for (var k in (obj || {})) out[k] = obj[k];
  return out;
}

function dashboardFilterAlerts_(alerts, filters) {
  var out = [];
  for (var i = 0; i < alerts.length; i++) {
    if (!dashboardDateInRangeSoft_(alerts[i].FECHA_REFERENCIA, filters.dateStart, filters.dateEnd)) continue;
    out.push(alerts[i]);
  }
  out.sort(function(a, b) { return Number(b.METRICA || 0) - Number(a.METRICA || 0); });
  return out;
}

function dashboardPassDimensionFilters_(row, filters) {
  if (!dashboardEqFilter_(row.FUENTE_NORMALIZADA, filters.fuente, 'FUENTE')) return false;
  if (!dashboardEqFilter_(row.PROYECTO, filters.proyecto, 'PROYECTO')) return false;
  if (!dashboardEqFilter_(row.ESTADO_CIVIL, filters.estadoCivil, 'ESTADO_CIVIL')) return false;
  if (!dashboardEqFilter_(row.DISTRITO, filters.distrito, 'DISTRITO')) return false;
  if (!dashboardEqFilter_(row.NOMBRE_OPC, filters.opc, 'NOMBRE_OPC')) return false;
  if (!dashboardEqFilter_(row.ASESOR_ULTIMA_GESTION, filters.asesor, 'ASESOR')) return false;
  if (!dashboardEqFilter_(row.TIPIFICACION_AGRUPADA_ULTIMA || row.TIPIFICACION_AGRUPADA, filters.tipificacion, 'TIPIFICACION')) return false;
  if (!dashboardEqFilter_(row.ASIGNADO_ESTADO, filters.asignadoEstado, 'ASIGNADO_ESTADO')) return false;
  return true;
}

function dashboardEqFilter_(value, filterValue, category) {
  if (!filterValue || filterValue === 'TODOS') return true;
  return dashboardNormalizeDimensionValue_(category, value) === dashboardNormalizeDimensionValue_(category, filterValue);
}

function dashboardReadRows_(sheetName) {
  var ss = dashboardOpenSpreadsheetWithRetry_();
  var sh = ss.getSheetByName(sheetName);
  if (!sh || sh.getLastRow() < 2) return [];
  var values = sh.getRange(1, 1, sh.getLastRow(), sh.getLastColumn()).getValues();
  var headers = values[0].map(function(h) { return String(h || '').trim(); });
  var rows = [];
  for (var r = 1; r < values.length; r++) {
    var obj = {};
    for (var c = 0; c < headers.length; c++) obj[headers[c]] = values[r][c];
    rows.push(obj);
  }
  return rows;
}

function dashboardLoadSnapshot_(options) {
  options = options || {};
  if (!options.forceDrive && DASHBOARD_WEB_MEMO_SNAPSHOT) return DASHBOARD_WEB_MEMO_SNAPSHOT;

  var json = dashboardLoadDriveSnapshotJson_();
  if (!json && !options.forceDrive) {
    json = dashboardCacheGetChunked_(DASHBOARD_WEB_SNAPSHOT_CACHE_KEY);
  }

  if (!json) {
    return dashboardEmptySnapshot_('SIN_SNAPSHOT_PUBLICADO');
  }

  var snapshot = dashboardHydrateSnapshot_(JSON.parse(json));
  DASHBOARD_WEB_MEMO_SNAPSHOT = snapshot;
  DASHBOARD_WEB_MEMO_GENERATED_AT = snapshot.generatedAt || '';
  return snapshot;
}

function dashboardLoadBootstrap_() {
  var cached = dashboardCacheGet_(DASHBOARD_WEB_BOOTSTRAP_CACHE_KEY);
  if (cached) {
    try { return JSON.parse(cached); } catch (e0) {}
  }

  var json = dashboardLoadDriveJsonByProperty_(DASHBOARD_WEB_BOOTSTRAP_FILE_ID_KEY);
  if (json) {
    try {
      var boot = JSON.parse(json);
      dashboardCachePut_(DASHBOARD_WEB_BOOTSTRAP_CACHE_KEY, json, DASHBOARD_WEB_CACHE_TTL);
      return boot;
    } catch (e1) {}
  }

  var snap = dashboardLoadSnapshot_();
  var fallback = dashboardBuildBootstrapFromSnapshot_(snap);
  dashboardSaveBootstrapJson_(JSON.stringify(fallback));
  return fallback;
}

function dashboardEmptySnapshot_(source) {
  return {
    version: 1,
    source: source || 'EMPTY',
    generatedAt: '',
    counts: { leads: 0, leadCols: 0, events: 0, eventCols: 0, filters: 0, filterCols: 0, alerts: 0, alertCols: 0 },
    leads: [],
    events: [],
    filters: dashboardDefaultFilterRows_(),
    alerts: []
  };
}

function dashboardDefaultFilterRows_() {
  var tipos = ['CAPTACION', 'ASIGNACION', 'GESTION', 'CITA', 'PRESENCIA', 'TOUR', 'SEPARACION', 'PROCESABLE'];
  var rows = [];
  for (var i = 0; i < tipos.length; i++) rows.push({ CATEGORIA: 'TIPO_FECHA', VALOR: tipos[i], ORDEN: i + 1 });
  rows.push({ CATEGORIA: 'ASIGNADO_ESTADO', VALOR: 'ASIGNADO', ORDEN: 1 });
  rows.push({ CATEGORIA: 'ASIGNADO_ESTADO', VALOR: 'NO_ASIGNADO', ORDEN: 2 });
  return rows;
}

function dashboardBuildSnapshotFromSheets_() {
  var raw = {
    version: 1,
    generatedAt: new Date().toISOString(),
    source: 'SHEETS_FALLBACK',
    tables: {
      leads: dashboardObjectsToTable_(dashboardReadRows_(CFG.OUT.DASH_LEADS)),
      events: dashboardObjectsToTable_(dashboardReadRows_(CFG.OUT.DASH_EVENTS)),
      filters: dashboardObjectsToTable_(dashboardReadRows_(CFG.OUT.DASH_FILTERS)),
      alerts: dashboardObjectsToTable_(dashboardReadRows_(CFG.OUT.DASH_ALERTS))
    }
  };
  raw.counts = dashboardSnapshotCounts_(raw);
  return { rawSnapshot: raw, hydrated: dashboardHydrateSnapshot_(raw) };
}

function dashboardHydrateSnapshot_(snapshot) {
  snapshot = snapshot || {};
  var tables = snapshot.tables || {};
  var leads = dashboardTableToObjects_(tables.leads);
  var events = dashboardTableToObjects_(tables.events);
  var filters = dashboardTableToObjects_(tables.filters);
  if (!filters.length && (leads.length || events.length)) filters = dashboardBuildFilterRowsFromData_(leads, events);
  var counts = snapshot.counts || dashboardSnapshotCounts_(snapshot);
  counts.filters = filters.length;
  counts.filterCols = filters.length ? 3 : (counts.filterCols || 0);
  return {
    version: snapshot.version || 1,
    source: snapshot.source || 'SNAPSHOT',
    generatedAt: snapshot.generatedAt || '',
    counts: counts,
    leads: leads,
    events: events,
    filters: filters,
    alerts: dashboardTableToObjects_(tables.alerts)
  };
}

function dashboardBuildBootstrapFromSnapshot_(snapshot) {
  snapshot = snapshot || dashboardEmptySnapshot_('EMPTY');
  var filters = snapshot.filters || [];
  if (!filters.length && ((snapshot.leads || []).length || (snapshot.events || []).length)) {
    filters = dashboardBuildFilterRowsFromData_(snapshot.leads || [], snapshot.events || []);
  }
  var counts = snapshot.counts || {};
  counts.filters = filters.length;
  counts.filterCols = filters.length ? 3 : (counts.filterCols || 0);
  return {
    version: snapshot.version || 1,
    source: snapshot.source || 'SNAPSHOT',
    generatedAt: snapshot.generatedAt || '',
    counts: counts,
    filters: filters
  };
}

function dashboardBuildFilterRowsFromData_(leads, events) {
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
    dashboardAddSet_(sets.FUENTE, dashboardNormalizeDimensionValue_('FUENTE', l.FUENTE_NORMALIZADA));
    dashboardAddSet_(sets.PROYECTO, dashboardNormalizeDimensionValue_('PROYECTO', l.PROYECTO));
    dashboardAddSet_(sets.ESTADO_CIVIL, dashboardNormalizeDimensionValue_('ESTADO_CIVIL', l.ESTADO_CIVIL));
    dashboardAddSet_(sets.DISTRITO, dashboardNormalizeDimensionValue_('DISTRITO', l.DISTRITO));
    dashboardAddSet_(sets.NOMBRE_OPC, dashboardNormalizeDimensionValue_('NOMBRE_OPC', l.NOMBRE_OPC));
    dashboardAddSet_(sets.ASESOR, dashboardNormalizeDimensionValue_('ASESOR', l.ASESOR_ULTIMA_GESTION));
    dashboardAddSet_(sets.TIPIFICACION, dashboardNormalizeDimensionValue_('TIPIFICACION', l.TIPIFICACION_AGRUPADA_ULTIMA));
    minDate = dashboardPickDate_(minDate, l.FECHA_CAPTACION_LEAD, true);
    maxDate = dashboardPickDate_(maxDate, l.FECHA_CAPTACION_LEAD, false);
  }
  for (var e = 0; e < events.length; e++) {
    var ev = events[e];
    dashboardAddSet_(sets.FUENTE, dashboardNormalizeDimensionValue_('FUENTE', ev.FUENTE_NORMALIZADA));
    dashboardAddSet_(sets.PROYECTO, dashboardNormalizeDimensionValue_('PROYECTO', ev.PROYECTO));
    dashboardAddSet_(sets.ESTADO_CIVIL, dashboardNormalizeDimensionValue_('ESTADO_CIVIL', ev.ESTADO_CIVIL));
    dashboardAddSet_(sets.DISTRITO, dashboardNormalizeDimensionValue_('DISTRITO', ev.DISTRITO));
    dashboardAddSet_(sets.NOMBRE_OPC, dashboardNormalizeDimensionValue_('NOMBRE_OPC', ev.NOMBRE_OPC));
    dashboardAddSet_(sets.ASESOR, dashboardNormalizeDimensionValue_('ASESOR', ev.ASESOR_ULTIMA_GESTION));
    dashboardAddSet_(sets.TIPIFICACION, dashboardNormalizeDimensionValue_('TIPIFICACION', ev.TIPIFICACION_AGRUPADA));
  }
  var out = [];
  for (var cat in sets) {
    var vals = dashboardSortOptions_(cat, Object.keys(sets[cat]).filter(function(v) { return v !== ''; }));
    for (var v = 0; v < vals.length; v++) out.push({ CATEGORIA: cat, VALOR: vals[v], ORDEN: v + 1 });
  }
  out.push({ CATEGORIA: 'META_MIN_FECHA_CAPTACION', VALOR: minDate ? dashboardDateKey_(minDate) : '', ORDEN: 1 });
  out.push({ CATEGORIA: 'META_MAX_FECHA_CAPTACION', VALOR: maxDate ? dashboardDateKey_(maxDate) : '', ORDEN: 1 });
  return out;
}

function dashboardAddSet_(setObj, value) {
  var v = String(value || '').trim();
  if (v && v !== 'SIN_DATO') setObj[v] = true;
}

function dashboardPickDate_(current, value, minMode) {
  var d = dashboardToDate_(value);
  if (!d) return current;
  if (!current) return d;
  return minMode ? (d.getTime() < current.getTime() ? d : current) : (d.getTime() > current.getTime() ? d : current);
}

function dashboardSnapshotHealth_(snapshot) {
  snapshot = snapshot || {};
  var counts = snapshot.counts || {};
  return {
    spreadsheet: 'Snapshot API',
    generatedAt: snapshot.generatedAt || new Date(),
    source: snapshot.source || 'CACHE',
    sheets: [
      { name: CFG.OUT.DASH_LEADS, exists: (counts.leads || 0) > 0, rows: counts.leads || 0, cols: counts.leadCols || 0 },
      { name: CFG.OUT.DASH_EVENTS, exists: (counts.events || 0) > 0, rows: counts.events || 0, cols: counts.eventCols || 0 },
      { name: CFG.OUT.DASH_FILTERS, exists: (counts.filters || 0) > 0, rows: counts.filters || 0, cols: counts.filterCols || 0 },
      { name: CFG.OUT.DASH_ALERTS, exists: true, rows: counts.alerts || 0, cols: counts.alertCols || 0 }
    ]
  };
}

function dashboardBootstrapHealth_(boot) {
  boot = boot || {};
  var counts = boot.counts || {};
  return {
    spreadsheet: 'Snapshot API',
    generatedAt: boot.generatedAt || new Date(),
    source: boot.source || 'BOOTSTRAP',
    sheets: [
      { name: CFG.OUT.DASH_LEADS, exists: (counts.leads || 0) > 0, rows: counts.leads || 0, cols: counts.leadCols || 0 },
      { name: CFG.OUT.DASH_EVENTS, exists: (counts.events || 0) > 0, rows: counts.events || 0, cols: counts.eventCols || 0 },
      { name: CFG.OUT.DASH_FILTERS, exists: (boot.filters || []).length > 0, rows: (boot.filters || []).length, cols: 3 },
      { name: CFG.OUT.DASH_ALERTS, exists: true, rows: counts.alerts || 0, cols: counts.alertCols || 0 }
    ]
  };
}

function dashboardBootstrapError_(e) {
  var msg = dashboardErrorMessage_(e);
  return {
    ok: false,
    error: msg,
    options: {},
    meta: {},
    health: {
      spreadsheet: 'Snapshot API',
      generatedAt: new Date(),
      source: 'ERROR',
      sheets: [
        { name: CFG.OUT.DASH_LEADS, exists: false, rows: 0, cols: 0 },
        { name: CFG.OUT.DASH_EVENTS, exists: false, rows: 0, cols: 0 },
        { name: CFG.OUT.DASH_FILTERS, exists: false, rows: 0, cols: 0 },
        { name: CFG.OUT.DASH_ALERTS, exists: false, rows: 0, cols: 0 }
      ]
    },
    defaults: { mode: 'COHORTE', tipoFecha: 'CAPTACION', dateStart: '', dateEnd: '' }
  };
}

function dashboardDataError_(e) {
  return {
    ok: false,
    error: dashboardErrorMessage_(e),
    mode: 'ERROR',
    kpis: { totalLeads: 0, contactables: 0, potenciales: 0, citas: 0, presencias: 0, tours: 0, separaciones: 0, procesables: 0, leadToCita: 0, leadToSeparacion: 0, leadToProcesable: 0 },
    funnel: [],
    charts: { trends: [], sourceDonut: [], districtDonut: [], tipifDonut: [], advisorRank: [], opcRank: [] },
    alerts: [],
    rows: 0,
    generatedAt: new Date()
  };
}

function dashboardCompactSnapshotForWeb_(snapshot, options) {
  snapshot = snapshot || {};
  options = options || {};
  var leads = snapshot.leads || [];
  var events = snapshot.events || [];
  var alerts = snapshot.alerts || [];
  var eventDateStart = String(options.eventDateStart || '');
  var eventDateEnd = String(options.eventDateEnd || '');
  return {
    leads: leads.map(function(r) {
      return {
        c: String(r.CELULAR_KEY || ''),
        fc: dashboardDateKey_(r.FECHA_CAPTACION_LEAD),
        fa: dashboardDateKey_(r.FECHA_PRIMERA_ASIGNACION),
        fg: dashboardDateKey_(r.FECHA_ULTIMA_GESTION),
        fci: dashboardDateKey_(r.FECHA_PRIMERA_CITA),
        fp: dashboardDateKey_(r.FECHA_PRIMERA_PRESENCIA),
        ft: dashboardDateKey_(r.FECHA_PRIMER_TOUR),
        fs: dashboardDateKey_(r.FECHA_PRIMERA_SEPARACION),
        fpr: dashboardDateKey_(r.FECHA_PRIMER_PROCESABLE),
        fu: dashboardNormalizeDimensionValue_('FUENTE', r.FUENTE_NORMALIZADA),
        py: dashboardNormalizeDimensionValue_('PROYECTO', r.PROYECTO),
        ec: dashboardNormalizeDimensionValue_('ESTADO_CIVIL', r.ESTADO_CIVIL),
        di: dashboardNormalizeDimensionValue_('DISTRITO', r.DISTRITO),
        op: dashboardNormalizeDimensionValue_('NOMBRE_OPC', r.NOMBRE_OPC),
        as: dashboardNormalizeDimensionValue_('ASESOR', r.ASESOR_ULTIMA_GESTION),
        ae: dashboardNormalizeDimensionValue_('ASIGNADO_ESTADO', r.ASIGNADO_ESTADO),
        ti: dashboardNormalizeDimensionValue_('TIPIFICACION', r.TIPIFICACION_AGRUPADA_ULTIMA),
        crm: dashboardIsTruthy_(r.ES_LEAD_CRM) ? 1 : 0,
        co: dashboardIsTruthy_(r.ES_CONTACTABLE) ? 1 : 0,
        po: dashboardIsTruthy_(r.ES_POTENCIAL) ? 1 : 0,
        ci: dashboardIsTruthy_(r.TIENE_CITA) ? 1 : 0,
        pp: dashboardIsTruthy_(r.TIENE_PRESENCIA) ? 1 : 0,
        to: dashboardIsTruthy_(r.TIENE_TOUR) ? 1 : 0,
        se: dashboardIsTruthy_(r.TIENE_SEPARACION) ? 1 : 0,
        pr: dashboardIsTruthy_(r.TIENE_PROCESABLE) ? 1 : 0
      };
    }),
    events: events.filter(function(r) {
      if (!eventDateStart && !eventDateEnd) return true;
      return dashboardCompactDateInRange_(dashboardDateKey_(r.FECHA), eventDateStart, eventDateEnd);
    }).map(function(r) {
      return {
        f: dashboardDateKey_(r.FECHA),
        tf: String(r.TIPO_FECHA || ''),
        fu: dashboardNormalizeDimensionValue_('FUENTE', r.FUENTE_NORMALIZADA),
        py: dashboardNormalizeDimensionValue_('PROYECTO', r.PROYECTO),
        ec: dashboardNormalizeDimensionValue_('ESTADO_CIVIL', r.ESTADO_CIVIL),
        di: dashboardNormalizeDimensionValue_('DISTRITO', r.DISTRITO),
        op: dashboardNormalizeDimensionValue_('NOMBRE_OPC', r.NOMBRE_OPC),
        as: dashboardNormalizeDimensionValue_('ASESOR', r.ASESOR_ULTIMA_GESTION),
        ti: dashboardNormalizeDimensionValue_('TIPIFICACION', r.TIPIFICACION_AGRUPADA),
        ae: dashboardNormalizeDimensionValue_('ASIGNADO_ESTADO', r.ASIGNADO_ESTADO),
        l: Number(r.LEADS || 0),
        ag: Number(r.ASIGNADOS || 0),
        ge: Number(r.GESTIONES || 0),
        co: Number(r.CONTACTABLES || 0),
        po: Number(r.POTENCIALES || 0),
        ci: Number(r.CITAS || 0),
        pp: Number(r.PRESENCIAS || 0),
        to: Number(r.TOURS || 0),
        se: Number(r.SEPARACIONES || 0),
        pr: Number(r.PROCESABLES || 0),
        df: Number(r.DATOS_FALSOS || 0)
      };
    }),
    alerts: alerts.map(function(r) {
      return {
        tipo: String(r.TIPO_ALERTA || ''),
        prioridad: String(r.PRIORIDAD || ''),
        fecha: dashboardDateKey_(r.FECHA_REFERENCIA),
        dimension: String(r.DIMENSION || ''),
        valor: String(r.VALOR || ''),
        metrica: Number(r.METRICA || 0),
        detalle: String(r.DETALLE || '')
      };
    })
  };
}

function dashboardSnapshotEventScope_() {
  try {
    if (typeof CFG !== 'undefined' && CFG.ETL && CFG.ETL.DASHBOARD_EVENT_SCOPE) {
      var mode = String(CFG.ETL.DASHBOARD_EVENT_SCOPE || '').toUpperCase().trim();
      return mode === 'DEFAULT_RANGE' ? 'DEFAULT_RANGE' : 'ALL';
    }
  } catch (e) {}
  return 'ALL';
}

function dashboardSnapshotDiagnostics_(snapshot) {
  snapshot = snapshot || {};
  var leads = snapshot.leads || [];
  var events = snapshot.events || [];
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
  var minLead = null, maxLead = null, minEvent = null, maxEvent = null;
  for (var i = 0; i < leads.length; i++) {
    if (dashboardIsTruthy_(leads[i].ES_LEAD_CRM)) out.crmLeads++;
    var dl = dashboardToDate_(leads[i].FECHA_CAPTACION_LEAD);
    if (dl) {
      minLead = dashboardPickDate_(minLead, dl, true);
      maxLead = dashboardPickDate_(maxLead, dl, false);
      var ml = dashboardDateKey_(dl).substring(0, 7);
      out.leadsByMonth[ml] = (out.leadsByMonth[ml] || 0) + 1;
    }
  }
  for (var e = 0; e < events.length; e++) {
    var de = dashboardToDate_(events[e].FECHA);
    var tipo = String(events[e].TIPO_FECHA || '');
    out.eventsByTipo[tipo] = (out.eventsByTipo[tipo] || 0) + 1;
    if (de) {
      minEvent = dashboardPickDate_(minEvent, de, true);
      maxEvent = dashboardPickDate_(maxEvent, de, false);
      var me = dashboardDateKey_(de).substring(0, 7);
      out.eventsByMonth[me] = (out.eventsByMonth[me] || 0) + 1;
    }
  }
  out.leadDateMin = dashboardDateKey_(minLead);
  out.leadDateMax = dashboardDateKey_(maxLead);
  out.eventDateMin = dashboardDateKey_(minEvent);
  out.eventDateMax = dashboardDateKey_(maxEvent);
  return out;
}

function dashboardCatalogsFromBootstrap_(boot) {
  var filters = (boot && boot.filters) || [];
  var optionSets = {};
  for (var i = 0; i < filters.length; i++) {
    var cat = String(filters[i].CATEGORIA || '').trim();
    var val = String(filters[i].VALOR || '').trim();
    if (!cat || cat.indexOf('META_') === 0) continue;
    val = dashboardNormalizeDimensionValue_(cat, val);
    if (!val) continue;
    if (!optionSets[cat]) optionSets[cat] = {};
    optionSets[cat][val] = true;
  }
  var catalogs = {};
  for (var optCat in optionSets) {
    catalogs[optCat] = dashboardSortOptions_(optCat, Object.keys(optionSets[optCat]));
  }
  return catalogs;
}

function dashboardCatalogSizes_(catalogs) {
  var out = {};
  catalogs = catalogs || {};
  for (var k in catalogs) out[k] = (catalogs[k] || []).length;
  return out;
}

function dashboardSmokeLeadKpis_(leads, filters) {
  var out = dashboardSmokeBaseKpis_();
  leads = leads || [];
  for (var i = 0; i < leads.length; i++) {
    var r = leads[i];
    if (!r.crm) continue;
    if (!dashboardCompactDateInRange_(r.fc, filters.dateStart, filters.dateEnd)) continue;
    out.totalLeads++;
    out.contactables += r.co ? 1 : 0;
    out.potenciales += r.po ? 1 : 0;
    out.citas += r.ci ? 1 : 0;
    out.presencias += r.pp ? 1 : 0;
    out.tours += r.to ? 1 : 0;
    out.separaciones += r.se ? 1 : 0;
    out.procesables += r.pr ? 1 : 0;
  }
  return dashboardSmokeAttachRates_(out);
}

function dashboardSmokeEventKpis_(events, filters) {
  var out = dashboardSmokeBaseKpis_();
  events = events || [];
  for (var i = 0; i < events.length; i++) {
    var r = events[i];
    if (!dashboardCompactDateInRange_(r.f, filters.dateStart, filters.dateEnd)) continue;
    if (filters.tipoFecha && filters.tipoFecha !== 'TODOS' && r.tf !== filters.tipoFecha) continue;
    out.totalLeads += Number(r.l || 0);
    out.contactables += Number(r.co || 0);
    out.potenciales += Number(r.po || 0);
    out.citas += Number(r.ci || 0);
    out.presencias += Number(r.pp || 0);
    out.tours += Number(r.to || 0);
    out.separaciones += Number(r.se || 0);
    out.procesables += Number(r.pr || 0);
    out.gestiones += Number(r.ge || 0);
    out.asignados += Number(r.ag || 0);
  }
  return dashboardSmokeAttachRates_(out);
}

function dashboardSmokeBaseKpis_() {
  return {
    totalLeads: 0,
    contactables: 0,
    potenciales: 0,
    citas: 0,
    presencias: 0,
    tours: 0,
    separaciones: 0,
    procesables: 0,
    gestiones: 0,
    asignados: 0
  };
}

function dashboardSmokeAttachRates_(out) {
  out.leadToCita = out.totalLeads ? out.citas / out.totalLeads : 0;
  out.leadToSeparacion = out.totalLeads ? out.separaciones / out.totalLeads : 0;
  out.leadToProcesable = out.totalLeads ? out.procesables / out.totalLeads : 0;
  return out;
}

function dashboardCompactDateInRange_(dateKeyValue, start, end) {
  if (!dateKeyValue) return false;
  if (start && dateKeyValue < start) return false;
  if (end && dateKeyValue > end) return false;
  return true;
}

function dashboardErrorMessage_(e) {
  return e && e.message ? e.message : String(e || 'Error desconocido');
}

function dashboardSnapshotCounts_(snapshot) {
  var t = (snapshot && snapshot.tables) || {};
  return {
    leads: t.leads && t.leads.rows ? t.leads.rows.length : 0,
    leadCols: t.leads && t.leads.headers ? t.leads.headers.length : 0,
    events: t.events && t.events.rows ? t.events.rows.length : 0,
    eventCols: t.events && t.events.headers ? t.events.headers.length : 0,
    filters: t.filters && t.filters.rows ? t.filters.rows.length : 0,
    filterCols: t.filters && t.filters.headers ? t.filters.headers.length : 0,
    alerts: t.alerts && t.alerts.rows ? t.alerts.rows.length : 0,
    alertCols: t.alerts && t.alerts.headers ? t.alerts.headers.length : 0
  };
}

function dashboardTableToObjects_(table) {
  if (!table || !table.headers || !table.rows) return [];
  var headers = table.headers;
  var rows = [];
  for (var r = 0; r < table.rows.length; r++) {
    var obj = {};
    for (var c = 0; c < headers.length; c++) obj[headers[c]] = table.rows[r][c];
    rows.push(obj);
  }
  return rows;
}

function dashboardObjectsToTable_(objects) {
  objects = objects || [];
  var headerMap = {};
  var headers = [];
  for (var i = 0; i < objects.length; i++) {
    for (var k in objects[i]) {
      if (!headerMap[k]) {
        headerMap[k] = true;
        headers.push(k);
      }
    }
  }
  var rows = [];
  for (var r = 0; r < objects.length; r++) {
    var row = [];
    for (var c = 0; c < headers.length; c++) row.push(dashboardSerializeSnapshotValue_(objects[r][headers[c]]));
    rows.push(row);
  }
  return { headers: headers, rows: rows };
}

function dashboardLoadDriveSnapshotJson_() {
  return dashboardLoadDriveJsonByProperty_(DASHBOARD_WEB_SNAPSHOT_FILE_ID_KEY);
}

function dashboardLoadDriveJsonByProperty_(propertyKey) {
  try {
    var fileId = PropertiesService.getScriptProperties().getProperty(propertyKey);
    if (!fileId) return '';
    return DriveApp.getFileById(fileId).getBlob().getDataAsString('UTF-8');
  } catch (e) {
    return '';
  }
}

function dashboardPublishSnapshot_(tables) {
  var snapshot = {
    version: 1,
    source: 'ETL',
    generatedAt: new Date().toISOString(),
    tables: tables || {}
  };
  snapshot.counts = dashboardSnapshotCounts_(snapshot);
  var json = JSON.stringify(snapshot);
  DASHBOARD_WEB_MEMO_SNAPSHOT = dashboardHydrateSnapshot_(snapshot);
  DASHBOARD_WEB_MEMO_GENERATED_AT = DASHBOARD_WEB_MEMO_SNAPSHOT.generatedAt || '';
  dashboardSaveDriveSnapshotJson_(json);
  var bootstrapJson = JSON.stringify(dashboardBuildBootstrapFromSnapshot_(DASHBOARD_WEB_MEMO_SNAPSHOT));
  dashboardSaveBootstrapJson_(bootstrapJson);
  dashboardSaveWebPayloadJson_(JSON.stringify(dashboardBuildWebPayloadFromSnapshot_(DASHBOARD_WEB_MEMO_SNAPSHOT)));
  dashboardPrewarmDefaultResponses_(DASHBOARD_WEB_MEMO_SNAPSHOT);
  return snapshot.counts;
}

function dashboardPublishWebPayloadFastFromTables_(tables, sourceLabel) {
  var snapshot = {
    version: 2,
    source: sourceLabel || 'ETL_FAST',
    generatedAt: new Date().toISOString(),
    tables: tables || {}
  };
  snapshot.counts = dashboardSnapshotCounts_(snapshot);
  var hydrated = dashboardHydrateSnapshot_(snapshot);
  var payload = dashboardBuildWebPayloadFromSnapshot_(hydrated);
  var json = JSON.stringify(payload);
  dashboardSaveWebPayloadJson_(json);
  return {
    generatedAt: snapshot.generatedAt,
    counts: snapshot.counts,
    bytes: json.length
  };
}

function dashboardSaveDriveSnapshotJson_(json) {
  try {
    var props = PropertiesService.getScriptProperties();
    var fileId = props.getProperty(DASHBOARD_WEB_SNAPSHOT_FILE_ID_KEY);
    var file = null;
    if (fileId) {
      try { file = DriveApp.getFileById(fileId); } catch (e0) { file = null; }
    }
    if (!file) {
      file = DriveApp.createFile('TRIGAL_DASHBOARD_WEB_SNAPSHOT.json', json, MimeType.PLAIN_TEXT);
      props.setProperty(DASHBOARD_WEB_SNAPSHOT_FILE_ID_KEY, file.getId());
    } else {
      file.setContent(json);
    }
  } catch (e) {
    etl_log('WARN', 'dashboardSaveDriveSnapshotJson_', e.message);
  }
}

function dashboardSaveBootstrapJson_(json) {
  dashboardCachePut_(DASHBOARD_WEB_BOOTSTRAP_CACHE_KEY, json, DASHBOARD_WEB_CACHE_TTL);
  dashboardSaveDriveJsonByProperty_(DASHBOARD_WEB_BOOTSTRAP_FILE_ID_KEY, 'TRIGAL_DASHBOARD_WEB_BOOTSTRAP.json', json);
}

function dashboardSaveWebPayloadJson_(json) {
  dashboardCachePutChunked_(DASHBOARD_WEB_PAYLOAD_CACHE_KEY, json, DASHBOARD_WEB_CACHE_TTL);
  dashboardSaveDriveJsonByProperty_(DASHBOARD_WEB_PAYLOAD_FILE_ID_KEY, 'TRIGAL_DASHBOARD_WEB_PAYLOAD.json', json);
}

function dashboardSaveDriveJsonByProperty_(propertyKey, fileName, json) {
  try {
    var props = PropertiesService.getScriptProperties();
    var fileId = props.getProperty(propertyKey);
    var file = null;
    if (fileId) {
      try { file = DriveApp.getFileById(fileId); } catch (e0) { file = null; }
    }
    if (!file) {
      file = DriveApp.createFile(fileName, json, MimeType.PLAIN_TEXT);
      props.setProperty(propertyKey, file.getId());
    } else {
      file.setContent(json);
    }
  } catch (e) {
    etl_log('WARN', 'dashboardSaveDriveJsonByProperty_', e.message);
  }
}

function dashboardPrewarmDefaultResponses_(snapshot) {
  try {
    var boot = dashboardBuildBootstrapFromSnapshot_(snapshot);
    var meta = {};
    var filters = boot.filters || [];
    for (var i = 0; i < filters.length; i++) {
      var cat = String(filters[i].CATEGORIA || '').trim();
      if (cat.indexOf('META_') === 0) meta[cat] = dashboardNormalizeMetaValue_(cat, filters[i].VALOR);
    }
    var defaultDates = dashboardDefaultDateRange_(meta);
    var base = {
      dateStart: defaultDates.dateStart,
      dateEnd: defaultDates.dateEnd,
      fuente: 'TODOS',
      proyecto: 'TODOS',
      estadoCivil: 'TODOS',
      distrito: 'TODOS',
      asignadoEstado: 'TODOS',
      opc: 'TODOS',
      asesor: 'TODOS',
      tipificacion: 'TODOS'
    };
    var presets = [
      dashboardMerge_(base, { mode: 'COHORTE', tipoFecha: 'CAPTACION' }),
      dashboardMerge_(base, { mode: 'EVENTO', tipoFecha: 'TODOS' }),
      dashboardMerge_(base, { mode: 'EVENTO', tipoFecha: 'GESTION' })
    ];
    var precomputed = { generatedAt: snapshot.generatedAt || '', responses: {} };
    for (var p = 0; p < presets.length; p++) {
      var f = presets[p];
      var response = String(f.mode || '').toUpperCase() === 'EVENTO'
        ? dashboardBuildEventoResponse_(snapshot.events || [], snapshot.alerts || [], f)
        : dashboardBuildCohorteResponse_(snapshot.leads || [], snapshot.events || [], snapshot.alerts || [], f);
      response.snapshotGeneratedAt = snapshot.generatedAt || '';
      var responseKey = dashboardResponseCacheKey_(f, snapshot);
      precomputed.responses[responseKey] = response;
      dashboardCachePut_(responseKey, JSON.stringify(response), DASHBOARD_WEB_CACHE_TTL);
    }
    dashboardSavePrecomputedResponses_(precomputed);
  } catch (e) {
    etl_log('WARN', 'dashboardPrewarmDefaultResponses_', e.message);
  }
}

function dashboardLoadPrecomputedResponse_(responseKey) {
  try {
    var raw = dashboardCacheGet_(DASHBOARD_WEB_PRESET_RESPONSES_CACHE_KEY);
    if (!raw) raw = dashboardLoadDriveJsonByProperty_(DASHBOARD_WEB_PRESET_RESPONSES_FILE_ID_KEY);
    if (!raw) return null;
    var payload = JSON.parse(raw);
    var response = payload && payload.responses ? payload.responses[responseKey] : null;
    if (response) dashboardCachePut_(responseKey, JSON.stringify(response), DASHBOARD_WEB_CACHE_TTL);
    return response || null;
  } catch (e) {
    return null;
  }
}

function dashboardSavePrecomputedResponses_(payload) {
  var json = JSON.stringify(payload || { responses: {} });
  dashboardCachePut_(DASHBOARD_WEB_PRESET_RESPONSES_CACHE_KEY, json, DASHBOARD_WEB_CACHE_TTL);
  dashboardSaveDriveJsonByProperty_(DASHBOARD_WEB_PRESET_RESPONSES_FILE_ID_KEY, 'TRIGAL_DASHBOARD_WEB_RESPONSES.json', json);
}

function dashboardMerge_(base, patch) {
  var out = {};
  for (var k in base) out[k] = base[k];
  for (var p in patch) out[p] = patch[p];
  return out;
}

function dashboardCacheGet_(key) {
  try { return CacheService.getScriptCache().get(key); } catch (e) { return ''; }
}

function dashboardCachePut_(key, value, ttl) {
  try {
    if (value && value.length < 95000) CacheService.getScriptCache().put(key, value, ttl || DASHBOARD_WEB_RESPONSE_TTL);
  } catch (e) {}
}

function dashboardCacheGetChunked_(baseKey) {
  try {
    var cache = CacheService.getScriptCache();
    var metaRaw = cache.get(baseKey + ':meta');
    if (!metaRaw) return '';
    var meta = JSON.parse(metaRaw);
    var parts = [];
    for (var i = 0; i < meta.parts; i++) {
      var part = cache.get(baseKey + ':' + i);
      if (part === null || part === '') return '';
      parts.push(part);
    }
    return parts.join('');
  } catch (e) {
    return '';
  }
}

function dashboardCachePutChunked_(baseKey, value, ttl) {
  try {
    if (!value) return;
    if (value.length > 4500000) return;
    var cache = CacheService.getScriptCache();
    var parts = Math.ceil(value.length / DASHBOARD_WEB_CHUNK_SIZE);
    var meta = { parts: parts, updatedAt: new Date().toISOString(), length: value.length };
    cache.put(baseKey + ':meta', JSON.stringify(meta), ttl || DASHBOARD_WEB_CACHE_TTL);
    for (var i = 0; i < parts; i++) {
      cache.put(baseKey + ':' + i, value.substring(i * DASHBOARD_WEB_CHUNK_SIZE, (i + 1) * DASHBOARD_WEB_CHUNK_SIZE), ttl || DASHBOARD_WEB_CACHE_TTL);
    }
  } catch (e) {}
}

function dashboardDeleteTriggersForHandler_(handlerName) {
  try {
    var triggers = ScriptApp.getProjectTriggers();
    for (var i = triggers.length - 1; i >= 0; i--) {
      if (triggers[i].getHandlerFunction() === handlerName) ScriptApp.deleteTrigger(triggers[i]);
    }
  } catch (e) {}
}

function dashboardLog_(level, fn, message) {
  try {
    if (typeof etl_log === 'function') {
      etl_log(level, fn, message);
      return;
    }
  } catch (e0) {}
  try {
    console.log('[' + level + '] ' + fn + ': ' + message);
  } catch (e) {}
}

function dashboardResponseCacheKey_(filters, snapshot) {
  var payload = JSON.stringify(filters || {}) + '|' + String(snapshot.generatedAt || '');
  var hash = 0;
  for (var i = 0; i < payload.length; i++) {
    hash = ((hash << 5) - hash) + payload.charCodeAt(i);
    hash |= 0;
  }
  return DASHBOARD_WEB_RESPONSE_PREFIX + Math.abs(hash);
}

function dashboardOpenSpreadsheetWithRetry_() {
  var lastErr = null;
  for (var i = 0; i < 3; i++) {
    try {
      return getSpreadsheetDestino();
    } catch (e) {
      lastErr = e;
      Utilities.sleep(350 * (i + 1));
    }
  }
  throw lastErr;
}

function dashboardSerializeSnapshotValue_(value) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    var d = dashboardNormalizeDate_(value);
    return d ? Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd') : '';
  }
  return value === undefined || value === null ? '' : value;
}

function dashboardDateInRange_(value, start, end) {
  var d = dashboardToDate_(value);
  if (!d) return false;
  var s = dashboardToDate_(start);
  var e = dashboardToDate_(end);
  if (s && d.getTime() < s.getTime()) return false;
  if (e) {
    e.setHours(23, 59, 59, 999);
    if (d.getTime() > e.getTime()) return false;
  }
  return true;
}

function dashboardDateInRangeSoft_(value, start, end) {
  if (!value) return true;
  return dashboardDateInRange_(value, start, end);
}

function dashboardToDate_(value) {
  if (!value) return null;
  var d = dashboardNormalizeDate_(value);
  if (!d || isNaN(d.getTime())) return null;
  d.setHours(0, 0, 0, 0);
  return dashboardIsSaneDate_(d) ? d : null;
}

function dashboardNormalizeDate_(value) {
  if (!value) return null;
  var d = null;
  if (value instanceof Date && !isNaN(value.getTime())) {
    d = new Date(value);
  } else {
    var repaired = dashboardRepairDateText_(String(value).trim());
    d = dashboardParseDateText_(repaired) || new Date(repaired);
  }
  if (!d || isNaN(d.getTime())) return null;
  var fixedYear = dashboardRepairYear_(d.getFullYear());
  if (fixedYear !== d.getFullYear()) d.setFullYear(fixedYear);
  return d;
}

function dashboardRepairDateText_(value) {
  if (!value) return value;
  value = value.replace(/^(\d{5})([-\/])/, function(match, year, sep) {
    var fixed = dashboardRepairYear_(Number(year));
    return fixed !== Number(year) ? String(fixed) + sep : match;
  });
  value = value.replace(/([-\/])(\d{5})(\b|[T\s])/, function(match, sep, year, tail) {
    var fixed = dashboardRepairYear_(Number(year));
    return fixed !== Number(year) ? sep + String(fixed) + tail : match;
  });
  return value;
}

function dashboardParseDateText_(value) {
  var s = String(value || '').trim();
  var m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  m = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);
  if (m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
  return null;
}

function dashboardRepairYear_(year) {
  year = Number(year || 0);
  if (year >= 20220 && year <= 20229) return 2020 + (year % 10);
  return year;
}

function dashboardIsSaneDate_(d) {
  if (!d || !(d instanceof Date) || isNaN(d.getTime()) || d.getTime() <= 0) return false;
  var year = d.getFullYear();
  var today = dashboardToday_();
  today.setHours(23, 59, 59, 999);
  return year >= 2020 && d.getTime() <= today.getTime();
}

function dashboardDateKey_(value) {
  var d = dashboardToDate_(value);
  if (!d) return '';
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

function dashboardIsTruthy_(value) {
  if (value === true || value === 1) return true;
  var s = String(value || '').trim().toUpperCase();
  return s === 'TRUE' || s === '1' || s === 'SI' || s === 'ASIGNADO';
}

function dashboardDiv_(a, b) {
  a = Number(a || 0);
  b = Number(b || 0);
  return b ? a / b : 0;
}
