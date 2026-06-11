/**
 * ==========================================================================
 * SISTEMA CRM INMOBILIARIO - BACKEND ENTERPRISE v6.0
 * Arquitectura: Data Warehouse (DIM/FACT) + ETL + Asignación Round-Robin
 * Nivel: Producción / Robustez Alta + Looker Studio Ready
 * ==========================================================================
 * CAMBIOS EN v6.0:
 * - FACT_INTERACCIONES expandida a 16 columnas (BI-optimizado)
 * - Normalización de FUENTE (META/OPC/GOOGLE_ADS/REFERIDO)
 * - Extracción automática de NOMBRE_OPC desde "OPC NOMBRE"
 * - Sistema de asignación Round-Robin multi-nivel (fuente + distrito)
 * - Validación y asignación equitativa de leads
 * - Menú personalizado en barra superior
 * - Sync en tiempo real: sidebar → DIM + FACT + hoja asesor
 * ==========================================================================
 */

function crmConfigValue_(key, fallback) {
  try {
    var scriptValue = PropertiesService.getScriptProperties().getProperty(key);
    if (scriptValue) return scriptValue;
  } catch (e) {}
  try {
    var docValue = PropertiesService.getDocumentProperties().getProperty(key);
    if (docValue) return docValue;
  } catch (e2) {}
  return fallback || '';
}

var CONFIG_SYSTEM = {
  SPREADSHEETS: {
    MASTER_ID: crmConfigValue_('CRM_MASTER_ID', ''),
    PRESENCIAL_ID: crmConfigValue_('CRM_PRESENCIAL_ID', '')
  },
  SHEETS: {
    DIM: 'DIM_CLIENTES',
    FACT: 'FACT_INTERACCIONES',
    CONFIG: 'CONFIG_MAESTROS',
    LOG: 'LOG_SISTEMA',
    VALIDACION: 'VALIDACION_LEADS',
    OPC: 'LEADS_OPC',
    META: 'LEADS_META',
    GOOGLE_ADS: 'LEADS_GOOGLE_ADS',
    DUPLICADOS: 'LEADS_DUPLICADOS_CONSOLIDADO'
  },
  IGNORED_SHEETS: ['DATA_MAESTRA'],
  TIKTOK: {
    SPREADSHEET_ID: crmConfigValue_('CRM_TIKTOK_ID', ''),
    SHEET_NAME: 'TIKTOK_LEADS',
    VALIDATION_TRIGGER_HOURS: 3
  },
  COLS: {
    DIM_PHONE: 2,
    RAW_PHONE: 6,
    GENERIC_PHONE: 2
  },
  /** Bases de asesores: fila 1 = botón "GESTIONAR CLIENTE", fila 2 = encabezados, datos desde fila 3 */
  ADVISOR_SHEET: {
    ROW_BUTTON: 1,
    ROW_HEADERS: 2,
    ROW_FIRST_DATA: 3
  },
  ADVISOR_BASES: {
    DEFAULT_BASE: 'REMOTO',
    LEGACY_PRESENCIAL_ADVISORS: ['MARILYN P.', 'EDITH P.', 'ANDREA A.'],
    BASES: {
      REMOTO: {
        label: 'CRM maestro',
        modalidad: 'REMOTO',
        spreadsheetId: 'MASTER'
      },
      PRESENCIAL: {
        label: 'Base presencial',
        modalidad: 'PRESENCIAL',
        spreadsheetId: crmConfigValue_('CRM_PRESENCIAL_ID', '')
      }
    }
  },
  CACHE: {
    TTL_MEMORY: 5 * 60 * 1000,      // 5 minutos en memoria
    TTL_PERSISTENT: 30 * 60,         // 30 minutos en CacheService (segundos)
    KEY_PREFIX: 'crm_trigal_'
  },
  /** Límite de leads por asesor por día en asignación Round-Robin */
  LIMITE_DIARIO_ASIGNACION: 40
};

// ==========================================================================
// SISTEMA DE CACHÉ MULTI-NIVEL (NUEVO EN v5.5)
// ==========================================================================

var CACHE_CLIENTS = null;
var CACHE_TIMESTAMP = 0;
var CRM_SPREADSHEET_CACHE = {};

function getActiveSpreadsheetSafe_() {
  try {
    return SpreadsheetApp.getActiveSpreadsheet();
  } catch (e) {
    return null;
  }
}

function getConfiguredMasterSpreadsheetId_() {
  var cfgId = CONFIG_SYSTEM.SPREADSHEETS && CONFIG_SYSTEM.SPREADSHEETS.MASTER_ID;
  if (cfgId) return cfgId;
  try {
    return PropertiesService.getDocumentProperties().getProperty('CRM_SPREADSHEET_ID') || '';
  } catch (e) {
    return '';
  }
}

function getSpreadsheetByIdCached_(spreadsheetId) {
  if (!spreadsheetId) throw new Error('ID de spreadsheet vacio');
  var active = getActiveSpreadsheetSafe_();
  if (active && active.getId && active.getId() === spreadsheetId) {
    CRM_SPREADSHEET_CACHE[spreadsheetId] = active;
    return active;
  }
  if (CRM_SPREADSHEET_CACHE[spreadsheetId]) return CRM_SPREADSHEET_CACHE[spreadsheetId];
  var ss = SpreadsheetApp.openById(spreadsheetId);
  CRM_SPREADSHEET_CACHE[spreadsheetId] = ss;
  return ss;
}

function getMasterSpreadsheet_() {
  var masterId = getConfiguredMasterSpreadsheetId_();
  var active = getActiveSpreadsheetSafe_();
  if (active && (!masterId || active.getId() === masterId)) {
    try {
      PropertiesService.getDocumentProperties().setProperty('CRM_SPREADSHEET_ID', active.getId());
    } catch (e) {}
    CRM_SPREADSHEET_CACHE[active.getId()] = active;
    return active;
  }
  if (masterId) return getSpreadsheetByIdCached_(masterId);
  if (active) return active;
  throw new Error('No se pudo abrir el CRM maestro. Configura CONFIG_SYSTEM.SPREADSHEETS.MASTER_ID.');
}

function getMasterSheet_(sheetName) {
  var ss = getMasterSpreadsheet_();
  return ss.getSheetByName(sheetName);
}

function normalizeRouteText_(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ');
}

function normalizeAdvisorBaseCode_(value) {
  var txt = normalizeRouteText_(value);
  if (!txt) return '';
  if (txt.indexOf('PRESENCIAL') !== -1 || txt.indexOf('PRESENC') !== -1) return 'PRESENCIAL';
  if (txt.indexOf('REMOTO') !== -1 || txt.indexOf('VIRTUAL') !== -1) return 'REMOTO';
  return '';
}

function advisorIsLegacyPresencial_(advisorName) {
  var key = normalizarAsesorParaClave(advisorName);
  var list = (CONFIG_SYSTEM.ADVISOR_BASES && CONFIG_SYSTEM.ADVISOR_BASES.LEGACY_PRESENCIAL_ADVISORS) || [];
  for (var i = 0; i < list.length; i++) {
    if (normalizarAsesorParaClave(list[i]) === key) return true;
  }
  return false;
}

function getAdvisorRoute_(advisorName, descripcion) {
  var advisorBases = CONFIG_SYSTEM.ADVISOR_BASES || {};
  var baseCode = normalizeAdvisorBaseCode_(descripcion);
  if (!baseCode && advisorIsLegacyPresencial_(advisorName)) baseCode = 'PRESENCIAL';
  if (!baseCode) baseCode = advisorBases.DEFAULT_BASE || 'REMOTO';

  var bases = advisorBases.BASES || {};
  if (!bases[baseCode]) baseCode = advisorBases.DEFAULT_BASE || 'REMOTO';
  var baseCfg = bases[baseCode] || bases.REMOTO || {};
  var spreadsheetId = baseCfg.spreadsheetId || 'MASTER';
  if (spreadsheetId === 'MASTER') spreadsheetId = getConfiguredMasterSpreadsheetId_();

  return {
    baseCode: baseCode,
    modalidad: baseCfg.modalidad || baseCode,
    label: baseCfg.label || baseCode,
    spreadsheetId: spreadsheetId
  };
}

function enrichAdvisorWithRoute_(advisor) {
  var route = getAdvisorRoute_(advisor.nombre || advisor, advisor.descripcion || '');
  advisor.baseCode = route.baseCode;
  advisor.modalidad = route.modalidad;
  advisor.baseLabel = route.label;
  advisor.spreadsheetId = route.spreadsheetId;
  return advisor;
}

function getAdvisorSpreadsheet_(advisor) {
  var route = getAdvisorRoute_(advisor.nombre || advisor, advisor.descripcion || '');
  return getSpreadsheetByIdCached_(route.spreadsheetId);
}

function getAdvisorSheetContext_(advisor) {
  var name = advisor.nombre || advisor;
  var route = getAdvisorRoute_(name, advisor.descripcion || '');
  var ss = getSpreadsheetByIdCached_(route.spreadsheetId);
  var sheet = getSheetByNameIgnoreCase(ss, name);
  return {
    spreadsheet: ss,
    sheet: sheet,
    sheetName: sheet ? sheet.getName() : name,
    advisorName: name,
    baseCode: route.baseCode,
    modalidad: route.modalidad,
    baseLabel: route.label,
    spreadsheetId: route.spreadsheetId
  };
}

function getAdvisorSheetContextByName_(advisorName) {
  var advisors = getActiveAdvisors();
  var targetKey = normalizarAsesorParaClave(advisorName);
  for (var i = 0; i < advisors.length; i++) {
    if (normalizarAsesorParaClave(advisors[i].nombre) === targetKey) {
      return getAdvisorSheetContext_(advisors[i]);
    }
  }
  var active = getActiveSpreadsheetSafe_();
  if (active) {
    var activeSheet = getSheetByNameIgnoreCase(active, advisorName);
    if (activeSheet) {
      return {
        spreadsheet: active,
        sheet: activeSheet,
        sheetName: activeSheet.getName(),
        advisorName: advisorName,
        baseCode: 'ACTIVA',
        modalidad: '',
        baseLabel: 'Archivo activo',
        spreadsheetId: active.getId()
      };
    }
  }
  return getAdvisorSheetContext_({ nombre: advisorName, descripcion: '' });
}

/**
 * Obtiene el caché persistente de CacheService
 */
function getPersistentCache() {
  var cache = CacheService.getScriptCache();
  var key = CONFIG_SYSTEM.CACHE.KEY_PREFIX + 'clients';
  var cached = cache.get(key);
  
  if (cached) {
    try {
      logDebug('[CACHE-PERSISTENT] Cache hit | Tamaño: ' + cached.length + ' bytes');
      return JSON.parse(cached);
    } catch (e) {
      logError('[CACHE-PERSISTENT] Error al parsear cache: ' + e.message);
      return null;
    }
  }
  
  return null;
}

/**
 * Guarda en el caché persistente
 */
function setPersistentCache(data) {
  try {
    var cache = CacheService.getScriptCache();
    var key = CONFIG_SYSTEM.CACHE.KEY_PREFIX + 'clients';
    var serialized = JSON.stringify(data);
    
    cache.put(key, serialized, CONFIG_SYSTEM.CACHE.TTL_PERSISTENT);
    logDebug('[CACHE-PERSISTENT] Cache guardado | Tamaño: ' + serialized.length + ' bytes');
  } catch (e) {
    logError('[CACHE-PERSISTENT] Error al guardar cache: ' + e.message);
  }
}

/**
 * Asegura que una hoja tenga al menos N filas y M columnas en su grid.
 * Si no las tiene, inserta las faltantes automáticamente.
 * @param {Sheet} sheet - La hoja a verificar
 * @param {number} requiredRows - Filas mínimas necesarias
 * @param {number} requiredCols - Columnas mínimas necesarias
 */
function ensureSheetCapacity(sheet, requiredRows, requiredCols) {
  var currentRows = sheet.getMaxRows();
  var currentCols = sheet.getMaxColumns();
  
  if (currentCols < requiredCols) {
    sheet.insertColumnsAfter(currentCols, requiredCols - currentCols);
    logDebug('[SHEET] "' + sheet.getName() + '" expandida: cols ' + currentCols + ' → ' + requiredCols);
  }
  
  if (currentRows < requiredRows) {
    var rowsToAdd = requiredRows - currentRows + 100; // +100 buffer para evitar repetir
    sheet.insertRowsAfter(currentRows, rowsToAdd);
    logDebug('[SHEET] "' + sheet.getName() + '" expandida: filas ' + currentRows + ' → ' + (currentRows + rowsToAdd));
  }
}

/**
 * Busca la fila de encabezados en una hoja.
 * En bases de asesores: fila 1 = botón, fila 2 = encabezados (CELULAR, NOMBRES Y APELLIDOS, etc.).
 * Detecta headers conocidos: CELULAR, NOMBRE, PROYECTO, TIPIF, FECHA HOY.
 * @param {Sheet} sheet
 * @return {number} Fila (1-based) donde están los headers
 */
function findHeaderRow(sheet) {
  var maxCheck = Math.min(5, sheet.getLastRow());
  var lastCol = sheet.getLastColumn();
  if (lastCol < 1 || maxCheck < 1) return 1;
  
  var knownHeaders = ['CELULAR', 'NOMBRE', 'PROYECTO', 'TIPIF', 'TELEFONO', 'FUENTE', 'FECHA HOY'];
  
  for (var r = 1; r <= maxCheck; r++) {
    var rowVals = sheet.getRange(r, 1, 1, lastCol).getValues()[0];
    for (var c = 0; c < rowVals.length; c++) {
      var val = String(rowVals[c]).toUpperCase().trim();
      for (var k = 0; k < knownHeaders.length; k++) {
        if (val.indexOf(knownHeaders[k]) !== -1) {
          logDebug('[HEADER] Encabezados en fila ' + r + ' de "' + sheet.getName() + '"');
          return r;
        }
      }
    }
  }
  return 1;
}

/**
 * Construye un Hash Map con todos los clientes para búsquedas O(1).
 * Implementa caché de 2 niveles: Memoria (5min) + Persistente (30min).
 * NOTA: Actualmente searchClient usa TextFinder directamente (óptimo para búsquedas individuales).
 * Este caché está disponible para futuras optimizaciones (ej. validación masiva con DIM >10k filas).
 */
function buildClientCache() {
  var now = new Date().getTime();
  
  // Nivel 1: Memoria (más rápido)
  if (CACHE_CLIENTS && (now - CACHE_TIMESTAMP) < CONFIG_SYSTEM.CACHE.TTL_MEMORY) {
    logDebug('[CACHE-L1] Usando cache en memoria | Edad: ' + Math.round((now - CACHE_TIMESTAMP) / 1000) + 's');
    return CACHE_CLIENTS;
  }
  
  // Nivel 2: Persistente (CacheService)
  var persistentCache = getPersistentCache();
  if (persistentCache) {
    CACHE_CLIENTS = persistentCache;
    CACHE_TIMESTAMP = now;
    logDebug('[CACHE-L2] Cache restaurado desde CacheService');
    return persistentCache;
  }
  
  // Nivel 3: Reconstruir desde Sheets
  var t0 = new Date().getTime();
  logDebug('[CACHE-L3] Construyendo cache desde Sheets...');
  
  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  
  if (!dimSheet) {
    logError('[CACHE] Error critico: No existe la hoja DIM_CLIENTES');
    return {};
  }
  
  var data = dimSheet.getDataRange().getValues();
  var cache = {};
  var validCount = 0;
  
  for (var i = 1; i < data.length; i++) {
    var phone = String(data[i][1]).replace(/\D/g, '');
    if (!phone || phone.length < 5) continue;
    
    cache[phone] = {
      id: data[i][0],
      celular: data[i][1],
      nombre: data[i][2] || '',
      email: data[i][3] || '',
      origen: data[i][4] || '',
      proyecto: data[i][5] || '',
      fecha_registro: formatDate(data[i][6]),
      asesor: data[i][7] || '',
      ultima_tipif: data[i][8] || '',
      estado_civil: data[i][12] || '',
      ocupacion: data[i][13] || '',
      tiene_pareja: data[i][14] || '',
      distrito: data[i][15] || ''
    };
    validCount++;
  }
  
  CACHE_CLIENTS = cache;
  CACHE_TIMESTAMP = now;
  
  // Guardar en caché persistente
  setPersistentCache(cache);
  
  var t1 = new Date().getTime();
  logDebug('[CACHE-L3] Cache construido: ' + validCount + ' clientes | Tiempo: ' + (t1 - t0) + 'ms');
  
  return cache;
}

/**
 * Invalida TODOS los niveles de caché
 */
function invalidateCache() {
  CACHE_CLIENTS = null;
  CACHE_TIMESTAMP = 0;
  
  var cache = CacheService.getScriptCache();
  var key = CONFIG_SYSTEM.CACHE.KEY_PREFIX + 'clients';
  cache.remove(key);
  
  logDebug('[CACHE] Todos los niveles invalidados (Memoria + Persistente)');
}

// ==========================================================================
// 1. NÚCLEO: INICIALIZACIÓN Y DETECCIÓN DE CONTEXTO (SIDEBAR)
// ==========================================================================

function abrirGestorContextual() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getActiveSheet();
  var ui = SpreadsheetApp.getUi();
  var sheetName = sheet.getName();
  var row = sheet.getActiveRange().getRow();
  
  logDebug('[SIDEBAR] Abriendo gestor | Hoja: ' + sheetName + ' | Fila: ' + row);
  
  // CRÍTICO: No abrir sidebar desde hojas del sistema (DIM, FACT, CONFIG, etc.)
  // Si se abre desde ahí, las interacciones se guardan en FACT pero NO en la base del asesor
  var systemSheets = getSystemSheetNames();
  if (systemSheets.indexOf(sheetName) !== -1) {
    ui.alert(
      "Abrir desde tu base",
      "Para registrar gestiones correctamente debes abrir el gestor desde la hoja de TU BASE (tu nombre).\n\n" +
      "❌ Hoja actual: \"" + sheetName + "\" (hoja del sistema)\n\n" +
      "✅ Pasos:\n" +
      "1. Ve a la pestaña con tu nombre (ej: LEONEL P., MARILYN P.)\n" +
      "2. Selecciona la fila del cliente\n" +
      "3. Menú CRM Gestión → Abrir Gestor\n\n" +
      "Así la interacción se guardará en FACT y también en tu hoja.",
      ui.ButtonSet.OK
    );
    return;
  }
  
  // Bases de asesores: fila 1 = botón "GESTIONAR CLIENTE", fila 2 = encabezados, datos desde fila 3
  var minDataRow = CONFIG_SYSTEM.ADVISOR_SHEET.ROW_FIRST_DATA;
  if (row < minDataRow) {
    ui.alert("Selección inválida", "Selecciona una fila con datos de cliente (fila " + minDataRow + " en adelante).\n\nLa fila 1 es el botón y la fila 2 son los encabezados.", ui.ButtonSet.OK);
    return;
  }

  // ESTRATEGIA 1: Buscar header "CELULAR" usando detección dinámica de fila
  var hdrRow = findHeaderRow(sheet);
  var headers = sheet.getRange(hdrRow, 1, 1, sheet.getLastColumn()).getValues()[0];
  var phoneColIndex = -1;
  var headerVariants = ['CELULAR', 'TELEFONO', 'PHONE', 'MOVIL', 'TLF'];

  for (var i = 0; i < headers.length; i++) {
    var headerClean = String(headers[i]).toUpperCase().trim();
    for (var v = 0; v < headerVariants.length; v++) {
      if (headerClean.indexOf(headerVariants[v]) !== -1) {
        phoneColIndex = i + 1;
        logDebug('[SIDEBAR] Header encontrado: "' + headers[i] + '" en columna ' + phoneColIndex);
        break;
      }
    }
    if (phoneColIndex !== -1) break;
  }

  // ESTRATEGIA 2: Si no hay header, escanear la fila para encontrar un celular
  if (phoneColIndex === -1) {
    logDebug('[SIDEBAR] Header CELULAR no encontrado, escaneando fila...');
    var scanData = sheet.getRange(row, 1, 1, Math.min(20, sheet.getLastColumn())).getValues()[0];
    for (var s = 0; s < scanData.length; s++) {
      if (!scanData[s]) continue;
      var scanClean = String(scanData[s]).replace(/\D/g, '');
      if (scanClean.length >= 9 && scanClean.charAt(0) === '9') {
        phoneColIndex = s + 1;
        logDebug('[SIDEBAR] Celular encontrado en escaneo: Col ' + phoneColIndex);
        break;
      }
    }
    if (phoneColIndex === -1) phoneColIndex = 8; // último fallback
  }

  var rawPhone = sheet.getRange(row, phoneColIndex).getValue();
  
  // ESTRATEGIA 3: Escaneo de toda la fila si la celda objetivo está vacía
  if (!rawPhone || String(rawPhone).trim() === "") {
    logDebug('[SIDEBAR] Celda vacía, iniciando escaneo horizontal...');
    var rowData = sheet.getRange(row, 1, 1, Math.min(20, sheet.getLastColumn())).getValues()[0];
    
    for (var j = 0; j < rowData.length; j++) {
      var val = rowData[j];
      if (!val || val === "") continue;
      
      var clean = String(val).replace(/\D/g, '');
      // Validar que sea un número peruano: 9 dígitos, empieza con 9
      if (clean.length >= 9 && clean.charAt(0) === '9') {
        rawPhone = val;
        phoneColIndex = j + 1;
        logDebug('[SIDEBAR] Celular encontrado en escaneo: "' + rawPhone + '" (Col ' + phoneColIndex + ')');
        break;
      }
    }
  }

  // LIMPIEZA Y VALIDACIÓN ROBUSTA
  var cleanPhoneStr = sanitizePhoneForTemplate(rawPhone);

  if (!cleanPhoneStr || cleanPhoneStr.length < 9) {
    logDebug('[SIDEBAR] Telefono invalido: "' + cleanPhoneStr + '"');
    ui.alert(
      "❌ Celular No Detectado", 
      "No se encontró un número de celular válido en esta fila.\n\n" +
      "✅ Asegúrate de:\n" +
      "  • Seleccionar una fila con datos de cliente\n" +
      "  • La columna CELULAR contiene 9 dígitos\n" +
      "  • El número empieza con 9\n\n" +
      "📍 Fila: " + row + " | Columna detectada: " + phoneColIndex + "\n" +
      "📱 Valor encontrado: '" + (rawPhone || 'VACÍO') + "'",
      ui.ButtonSet.OK
    );
    return;
  }

  logDebug('[SIDEBAR] ✅ Telefono validado: ' + cleanPhoneStr);

  // Inyección SEGURA al HTML
  var htmlTemplate = HtmlService.createTemplateFromFile('Sidebar');
  htmlTemplate.initialPhone = cleanPhoneStr;  // Ya viene sanitizado
  htmlTemplate.initialSheetName = sheet.getName();
  htmlTemplate.initialRow = row;
  
  try {
    var html = htmlTemplate.evaluate()
      .setTitle('🏢 Gestión Comercial | Trigal v5.5')
      .setSandboxMode(HtmlService.SandboxMode.IFRAME)
      .setWidth(420);
      
    ui.showSidebar(html);
    logDebug('[SIDEBAR] ✅ Sidebar renderizado exitosamente');
    
  } catch (e) {
    logError('[SIDEBAR] Error al renderizar template: ' + e.message);
    ui.alert('Error', 'No se pudo abrir el sidebar. Error: ' + e.message, ui.ButtonSet.OK);
  }
}

/**
 * NUEVA FUNCIÓN: Sanitiza el teléfono para evitar inyección de código en templates
 */
function sanitizePhoneForTemplate(phone) {
  if (!phone) return "";
  
  // Convertir a string y limpiar TODO excepto dígitos
  var cleaned = String(phone).replace(/\D/g, '');
  
  // Remover prefijo 51 si tiene 11 dígitos
  if (cleaned.length === 11 && cleaned.substring(0, 2) === "51") {
    cleaned = cleaned.substring(2);
  }
  
  // Validación final: solo números, longitud correcta
  if (!/^9\d{8}$/.test(cleaned)) {
    return "";  // No es un número peruano válido
  }
  
  return cleaned;
}

// ==========================================================================
// 2. API DE DATOS: BÚSQUEDA Y RECUPERACIÓN (READ) - ULTRA OPTIMIZADO
// ==========================================================================

function searchClient(phoneInput) {
  var t0 = new Date().getTime();
  var cleanPhone = normalizePhoneETL(phoneInput);
  
  logDebug('[SEARCH] Inicio | Input: "' + phoneInput + '" | Limpio: "' + cleanPhone + '"');
  
  if (!cleanPhone || cleanPhone.length < 5) {
    logDebug('[SEARCH] Numero invalido');
    return { found: false, message: 'Numero invalido o vacio' };
  }

  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  
  if (!dimSheet) {
    logError('[SEARCH] Hoja DIM_CLIENTES no encontrada');
    return { found: false, message: 'Error: DIM_CLIENTES no existe' };
  }
  
  // TextFinder: búsqueda directa en columna CELULAR (col B)
  var t1 = new Date().getTime();
  var phoneCol = dimSheet.getRange('B:B');
  var finder = phoneCol.createTextFinder(cleanPhone).matchEntireCell(true);
  var found = finder.findNext();
  var t2 = new Date().getTime();
  logDebug('[SEARCH] TextFinder: ' + (t2 - t1) + 'ms | Encontrado: ' + !!found);

  if (!found) {
    logDebug('[SEARCH] Cliente nuevo | Total: ' + (t2 - t0) + 'ms');
    return { 
      found: false, 
      data: { 
        celular: cleanPhone, 
        origen: 'NUEVO / DESCONOCIDO',
        nombre: '',
        email: '',
        estado_civil: '',
        ocupacion: '',
        tiene_pareja: '',
        distrito: ''
      }, 
      history: [] 
    };
  }
  
  // Leer SOLO la fila encontrada (en vez de toda la hoja)
  var rowNum = found.getRow();
  var rowData = dimSheet.getRange(rowNum, 1, 1, 16).getValues()[0];
  
  var clientData = {
    id: rowData[0],
    celular: String(rowData[1]).replace(/\D/g, ''),
    nombre: rowData[2] || '',
    email: rowData[3] || '',
    origen: rowData[4] || '',
    proyecto: rowData[5] || '',
    fecha_registro: formatDate(rowData[6]),
    asesor: rowData[7] || '',
    ultima_tipif: rowData[8] || '',
    estado_civil: rowData[12] || '',
    ocupacion: rowData[13] || '',
    tiene_pareja: rowData[14] || '',
    distrito: rowData[15] || ''
  };

  // Recuperar historial (últimas 20 interacciones)
  var history = [];
  
  if (factSheet && clientData.id) {
    // TextFinder en columna B (ID_CLIENTE) — NO lee toda la tabla
    var factFinder = factSheet.getRange('B:B').createTextFinder(clientData.id).matchEntireCell(true);
    var matches = factFinder.findAll();
    
    // Leer solo las filas encontradas (máx 20, más recientes primero)
    var startIdx = Math.max(0, matches.length - 20);
    for (var i = matches.length - 1; i >= startIdx; i--) {
      var factRow = factSheet.getRange(matches[i].getRow(), 1, 1, 16).getValues()[0];
      history.push({
        fecha: formatDate(factRow[6]),         // Col 7: FECHA_INTERACCION
        asesor: factRow[4] || '',              // Col 5: ASESOR_NOMBRE
        tipo: factRow[13] || 'SIN TIPIF',      // Col 14: TIPIFICACION
        nota: factRow[14] || ''                // Col 15: COMENTARIO
      });
    }
  }

  var t3 = new Date().getTime();
  logDebug('[SEARCH] Historial: ' + history.length + ' items | Total: ' + (t3 - t0) + 'ms');

  return {
    found: true,
    data: clientData,
    history: history
  };
}

/**
 * Lee los datos de la fila actual de una hoja (base de asesor) para pre-llenar el sidebar.
 * Así el sidebar muestra lo que realmente está en la fila (ej. "ANGEL CRUZ") y no solo lo que hay en DIM.
 */
function getRowDataForSidebar(sheetName, row) {
  var ctx = getAdvisorSheetContextByName_(sheetName);
  var sheet = ctx.sheet;
  if (!sheet) return { nombre: '', origen: '', estadoCivil: '', ocupacion: '', tienePareja: '', distrito: '' };
  var r = parseInt(row, 10);
  if (isNaN(r) || r < CONFIG_SYSTEM.ADVISOR_SHEET.ROW_FIRST_DATA) return { nombre: '', origen: '', estadoCivil: '', ocupacion: '', tienePareja: '', distrito: '' };
  var hdrRow = findHeaderRow(sheet);
  var lastCol = Math.max(sheet.getLastColumn(), 16);
  var headers = sheet.getRange(hdrRow, 1, 1, lastCol).getValues()[0];
  var rowVals = sheet.getRange(r, 1, 1, lastCol).getValues()[0];
  var out = { nombre: '', origen: '', estadoCivil: '', ocupacion: '', tienePareja: '', distrito: '' };
  for (var i = 0; i < headers.length; i++) {
    var h = String(headers[i]).toUpperCase().trim();
    var v = rowVals[i] != null ? String(rowVals[i]).trim() : '';
    if (h.indexOf('NOMBRE') !== -1 && h.indexOf('OPC') === -1) out.nombre = v;
    else if (h === 'FUENTE' || (h.indexOf('FUENTE') !== -1 && h.indexOf('NORMALIZ') === -1)) out.origen = v;
    else if (h.indexOf('CIVIL') !== -1) out.estadoCivil = v;
    else if (h.indexOf('OCUPACION') !== -1) out.ocupacion = v;
    else if (h.indexOf('PAREJA') !== -1) out.tienePareja = v;
    else if (h.indexOf('DISTRITO') !== -1) out.distrito = v;
  }
  return out;
}

// ==========================================================================
// 3. API DE CONFIGURACIÓN: CATÁLOGOS DINÁMICOS (READ)
// ==========================================================================

function getConfigDropdowns() {
  var t0 = new Date().getTime();
  logDebug('[CONFIG] Solicitando dropdowns...');
  
  var ss = getMasterSpreadsheet_();
  var configSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.CONFIG);
  
  if (!configSheet) {
    logError('[CONFIG] Hoja CONFIG_MAESTROS no existe');
    return {
      tipificaciones: ['NC', 'VENTA', 'CITA', 'NI', 'VLL'],
      estadosCiviles: ['SOLTER@', 'CASAD@', 'CONVIVIENTE'],
      ocupaciones: ['DEPENDIENTE', 'INDEPENDIENTE'],
      ubicaciones: ['LOS OLIVOS', 'SMP', 'CARABAYLLO']
    };
  }

  var data = configSheet.getDataRange().getValues();
  var tipificaciones = [];
  var estadosCiviles = [];
  var ocupaciones = [];
  var ubicaciones = [];

  for (var i = 1; i < data.length; i++) {
    var tipo = data[i][0];
    var valor = data[i][1];
    var estado = data[i][3];
    
    if (estado !== 'ACTIVO') continue;
    
    switch (tipo) {
      case 'TIPIFICACION':
        tipificaciones.push(valor);
        break;
      case 'ESTADO_CIVIL':
        estadosCiviles.push(valor);
        break;
      case 'OCUPACION':
        ocupaciones.push(valor);
        break;
      case 'UBICACION':
        ubicaciones.push(valor);
        break;
    }
  }

  var t1 = new Date().getTime();
  logDebug('[CONFIG] ✅ Completado en ' + (t1 - t0) + 'ms | Tipificaciones: ' + tipificaciones.length);

  return {
    tipificaciones: tipificaciones,
    estadosCiviles: estadosCiviles,
    ocupaciones: ocupaciones,
    ubicaciones: ubicaciones
  };
}

// ==========================================================================
// 4. API DE ESCRITURA: GUARDAR INTERACCIÓN (WRITE) - TRANSACCIONAL
// ==========================================================================

function saveInteraction(formData) {
  var t0 = new Date().getTime();
  var lock = LockService.getScriptLock();
  
  try {
    lock.waitLock(30000);  // Esperar hasta 30 segundos
    logDebug('[SAVE] Lock adquirido');
    
  } catch (e) {
    logError('[SAVE] Timeout esperando lock: ' + e.message);
    return { success: false, message: 'Sistema ocupado, intenta nuevamente' };
  }
  
  try {
    var ss = getMasterSpreadsheet_();
    var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
    var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
    var logSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.LOG);
    
    var userEmail = Session.getActiveUser().getEmail();
    // Fallback: si email vacío (usuario anónimo), usar nombre de hoja
    if (!userEmail || userEmail === '') {
      userEmail = formData.sheetName || 'ANONIMO';
    }
    var timestamp = new Date();
    var cleanPhone = normalizePhoneETL(formData.phone);
    var clientId = formData.clientId;
    var isNewClient = !clientId || clientId === "";
    var rowIndex = -1;  // para clientes existentes se asigna en el else
    
    // Normalizar textos a MAYÚSCULA
    var nombreUpper = (formData.nombre || 'Sin Nombre').toUpperCase();
    var distritoUpper = normalizarDistrito(formData.distrito || '');
    var estadoCivilUpper = (formData.estadoCivil || '').toUpperCase();
    var ocupacionUpper = (formData.ocupacion || '').toUpperCase();
    var parejaUpper = (formData.tienePareja || '').toUpperCase();

    logDebug('[SAVE] Procesando | Nuevo: ' + isNewClient + ' | Tel: ' + cleanPhone);

    // OPERACIÓN 1: UPSERT en DIM_CLIENTES
    // Parsear fuente del formulario
    var fuenteInfo = parseFuente(formData.origen || '', cleanPhone);
    
    if (isNewClient) {
      clientId = 'CLI_' + Utilities.getUuid();
      
      var newRow = [
        clientId,
        cleanPhone,
        nombreUpper,
        (formData.email || '').toLowerCase(),
        fuenteInfo.original || 'MANUAL',
        (formData.proyecto || '').toUpperCase(),
        timestamp,
        userEmail,
        (formData.tipificacion || '').toUpperCase(),
        timestamp,
        fuenteInfo.normalizada,  // Col 11: FUENTE_NORMALIZADA
        fuenteInfo.nombreOPC,    // Col 12: NOMBRE_OPC
        estadoCivilUpper,
        ocupacionUpper,
        parejaUpper,
        distritoUpper
      ];
      
      // Asegurar que DIM tiene capacidad suficiente
      var newRowNum = dimSheet.getLastRow() + 1;
      ensureSheetCapacity(dimSheet, newRowNum, 16);
      var newRange = dimSheet.getRange(newRowNum, 1, 1, 16);
      newRange.clearDataValidations();  // DIM es hoja sistema — limpiar validaciones
      newRange.setValues([newRow]);
      logDebug('[SAVE] ✅ Cliente nuevo creado | ID: ' + clientId);
      
    } else {
      // Actualizar cliente existente — búsqueda por TextFinder
      var dimFinder = dimSheet.getRange('A:A').createTextFinder(clientId).matchEntireCell(true);
      var dimFound = dimFinder.findNext();
      rowIndex = dimFound ? dimFound.getRow() : -1;
      
      if (rowIndex !== -1) {
        var existingRow = dimSheet.getRange(rowIndex, 1, 1, 16).getValues()[0];
        
        // Batch update: construir fila actualizada y escribir una sola vez
        existingRow[2] = nombreUpper || existingRow[2];           // Col 3: NOMBRE
        existingRow[3] = formData.email || existingRow[3];        // Col 4: EMAIL
        existingRow[7] = userEmail;                               // Col 8: ASESOR
        existingRow[8] = (formData.tipificacion || '').toUpperCase(); // Col 9: TIPIF
        existingRow[9] = timestamp;                               // Col 10: ULT INTERACCION
        existingRow[12] = estadoCivilUpper || existingRow[12];    // Col 13: E.CIVIL
        existingRow[13] = ocupacionUpper || existingRow[13];      // Col 14: OCUPACION
        existingRow[14] = parejaUpper || existingRow[14];         // Col 15: PAREJA
        existingRow[15] = distritoUpper || existingRow[15];       // Col 16: DISTRITO
        
        var dimRange = dimSheet.getRange(rowIndex, 1, 1, 16);
        dimRange.clearDataValidations();  // DIM es hoja sistema
        dimRange.setValues([existingRow]);
        logDebug('[SAVE] ✅ Cliente actualizado (batch) | ID: ' + clientId);
      }
    }

    // OPERACIÓN 2: INSERT en FACT_INTERACCIONES (16 columnas)
    var interactionId = 'INT_' + Utilities.getUuid();
    
    // Recuperar datos del cliente para desnormalizar en FACT
    var clientNombre = formData.nombre || 'Sin Nombre';
    var clientProyecto = formData.proyecto || '';
    var fechaRegistro = timestamp;
    var advisorName = formData.sheetName || '';
    
    // Para clientes existentes, leer datos de DIM
    if (!isNewClient && rowIndex && rowIndex !== -1) {
      var dimRow = dimSheet.getRange(rowIndex, 1, 1, 16).getValues()[0];
      clientProyecto = clientProyecto || dimRow[5] || '';
      fechaRegistro = dimRow[6] || timestamp;
      // Si no tenemos fuente del form, usar la de DIM
      if (!fuenteInfo.normalizada && dimRow[10]) {
        fuenteInfo.normalizada = String(dimRow[10]);
        fuenteInfo.original = String(dimRow[4] || '');
        fuenteInfo.nombreOPC = String(dimRow[11] || '');
      }
    }
    
    var interactionRow = [
      interactionId,                             // 1: ID_INTERACCION
      clientId,                                  // 2: ID_CLIENTE
      cleanPhone,                                // 3: CELULAR
      clientNombre,                              // 4: NOMBRE_CLIENTE
      advisorName,                               // 5: ASESOR_NOMBRE
      userEmail,                                 // 6: ASESOR_EMAIL
      timestamp,                                 // 7: FECHA_INTERACCION
      fechaRegistro,                             // 8: FECHA_REGISTRO_LEAD
      clientProyecto,                            // 9: PROYECTO
      fuenteInfo.original,                       // 10: FUENTE_ORIGINAL
      fuenteInfo.normalizada,                    // 11: FUENTE_NORMALIZADA
      fuenteInfo.nombreOPC,                      // 12: NOMBRE_OPC
      'LLAMADA',                                 // 13: TIPO_ACCION
      formData.tipificacion,                     // 14: TIPIFICACION
      formData.notas ? ('[' + Utilities.formatDate(timestamp, Session.getScriptTimeZone(), 'dd/MM/yyyy') + '] ' + formData.notas) : '',  // 15: COMENTARIO
      JSON.stringify({ sidebar: true, v: '6.0' })  // 16: METADATA
    ];
    
    factSheet.appendRow(interactionRow);

    // OPERACIÓN 3: LOG DEL SISTEMA
    if (logSheet) {
      logSheet.appendRow([
        timestamp,
        userEmail,
        isNewClient ? 'CREAR_CLIENTE' : 'ACTUALIZAR_CLIENTE',
        cleanPhone,
        formData.tipificacion,
        'SUCCESS'
      ]);
    }

    // OPERACIÓN 4: Actualizar fila de la hoja del asesor (si se abrió desde base del asesor)
    var advisorResult = updateAdvisorSheetRow(formData, timestamp);

    // Invalidar caché para reflejar cambios
    invalidateCache();
    
    var t1 = new Date().getTime();
    logDebug('[SAVE] ✅ Transacción completada en ' + (t1 - t0) + 'ms');
    
    var message = 'Gestión guardada correctamente';
    if (!advisorResult.updated && advisorResult.reason && advisorResult.reason !== 'Hoja del sistema') {
      message += '. (La hoja del asesor no se actualizó: ' + (advisorResult.reason || 'contexto inválido') + ')';
    }
    
    return { 
      success: true, 
      clientId: clientId,
      message: message,
      advisorSheetUpdated: advisorResult.updated
    };
    
  } catch (e) {
    logError('[SAVE] Error en transacción: ' + e.message);
    return { 
      success: false, 
      message: 'Error al guardar: ' + e.message 
    };
    
  } finally {
    lock.releaseLock();
    logDebug('[SAVE] Lock liberado');
  }
}

// ==========================================================================
// 5. MÓDULOS ETL: VALIDACIÓN Y MIGRACIÓN DE DATOS
// ==========================================================================

function validarLeads() {
  var t0 = new Date().getTime();
  var ss = getMasterSpreadsheet_();
  var validSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.VALIDACION);
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  
  if (!validSheet || !dimSheet) {
    SpreadsheetApp.getUi().alert('Error: Faltan hojas VALIDACION_LEADS o DIM_CLIENTES');
    return;
  }

  logDebug('[VALIDACION] Iniciando proceso...');
  
  // Construir mapa CRM: solo bloquea si el celular tiene asesor ACTIVO en DIM o FACT.
  var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var crmOwnership = buildCrmLeadOwnershipMap(ss, dimSheet, factSheet);
  
  // Los celulares con solo historial de asesores inactivos quedan disponibles.
  
  logDebug('[VALIDACION] Base CRM evaluada: ' + Object.keys(crmOwnership).length + ' numeros unicos');
  
  // Procesar leads a validar
  var validData = validSheet.getDataRange().getValues();
  var results = [];
  
  for (var j = 1; j < validData.length; j++) {
    var rawInput = validData[j][0];
    if (!rawInput || String(rawInput).trim() === "") {
      results.push(['', '', '', '']);
      continue;
    }
    
    var cleanInput = normalizePhoneETL(rawInput);
    
    if (!cleanInput || cleanInput.length < 9) {
      results.push([rawInput, 'INVALIDO', 'Número muy corto o sin dígitos', '']);
      
    } else if (!/^9\d{8}$/.test(cleanInput)) {
      results.push([rawInput, 'INVALIDO', 'No es un número peruano válido (debe empezar con 9)', cleanInput]);
      
    } else if (crmPhoneHasActiveAdvisor_(crmOwnership[cleanInput])) {
      var info = crmOwnership[cleanInput];
      results.push([
        rawInput, 
        'DUPLICADO', 
        'Ya existe | Asesor: ' + (info.activeAdvisor || '') + ' | Cliente: ' + (info.activeNombre || info.dimNombre || info.lastAnyNombre || '') + ' | Fecha: ' + (info.activeFecha ? formatDate(info.activeFecha) : ''),
        cleanInput
      ]);
      
    } else {
      results.push([rawInput, 'NUEVO', 'Disponible para asignar', cleanInput]);
    }
  }
  
  // Escribir resultados
  if (results.length > 0) {
    validSheet.getRange(2, 1, results.length, 4).setValues(results);
  }
  
  var t1 = new Date().getTime();
  var nuevos = results.filter(function(r) { return r[1] === 'NUEVO'; }).length;
  var duplicados = results.filter(function(r) { return r[1] === 'DUPLICADO'; }).length;
  var invalidos = results.filter(function(r) { return r[1] === 'INVALIDO'; }).length;
  
  logDebug('[VALIDACION] ✅ Completado en ' + (t1 - t0) + 'ms');
  
  SpreadsheetApp.getUi().alert(
    'Validación Completada',
    'Procesados: ' + results.length + ' números\n\n' +
    '✅ Nuevos: ' + nuevos + '\n' +
    '⚠️ Duplicados: ' + duplicados + '\n' +
    '❌ Inválidos: ' + invalidos + '\n\n' +
    'Tiempo: ' + (t1 - t0) + 'ms'
  );
}

/**
 * =====================================================================
 * VALIDAR LEADS RAW (desde hoja activa LEADS_OPC/META/GOOGLE_ADS)
 * Valida celulares contra:
 *   1. CRM con asesor activo en DIM/FACT (DUPLICADO_DIM/DUPLICADO_FACT)
 *   2. Otras bases LEADS_OPC/META/GOOGLE_ADS (DUPLICADO_OTRA_BASE)
 *   3. Repeticiones dentro de la misma hoja (DUPLICADO_INTERNO)
 * Colorea la fila según estado para feedback visual rápido.
 * =====================================================================
 */
function validarLeadsRaw() {
  var t0 = new Date().getTime();
  var ss = getActiveSpreadsheetSafe_() || getMasterSpreadsheet_();
  var masterSs = getMasterSpreadsheet_();
  var sourceSheet = ss.getActiveSheet();
  var dimSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var ui = SpreadsheetApp.getUi();
  var sheetName = sourceSheet.getName();
  
  // Solo permitir en hojas LEADS_*
  var validSources = [CONFIG_SYSTEM.SHEETS.OPC, CONFIG_SYSTEM.SHEETS.META, CONFIG_SYSTEM.SHEETS.GOOGLE_ADS];
  if (validSources.indexOf(sheetName) === -1) {
    ui.alert('Error', 'Solo puedes validar desde hojas LEADS_OPC, LEADS_META o LEADS_GOOGLE_ADS.\n\nHoja actual: ' + sheetName, ui.ButtonSet.OK);
    return;
  }
  
  var data = sourceSheet.getDataRange().getValues();
  var headers = data[0];
  
  // Buscar columna CELULAR por header
  var colCelular = -1;
  for (var h = 0; h < headers.length; h++) {
    if (String(headers[h]).toUpperCase().trim().indexOf('CELULAR') !== -1) {
      colCelular = h;
      break;
    }
  }
  if (colCelular === -1) {
    ui.alert('Error', 'No se encontró columna CELULAR en esta hoja.', ui.ButtonSet.OK);
    return;
  }
  
  // Construir mapa CRM: solo bloquea si el celular tiene asesor ACTIVO en DIM o FACT.
  var crmOwnership = buildCrmLeadOwnershipMap(masterSs, dimSheet, factSheet);

  // Construir set de números en OTRAS hojas LEADS (duplicados entre bases)
  var phonesInOtherSheets = new Map();  // phone -> { hoja: nombre }
  var otherLeadSheets = validSources.filter(function(s) { return s !== sheetName; });
  for (var o = 0; o < otherLeadSheets.length; o++) {
    var otherSheet = masterSs.getSheetByName(otherLeadSheets[o]);
    if (!otherSheet) continue;
    var otherData = otherSheet.getDataRange().getValues();
    var otherColCelular = -1;
    for (var h = 0; h < (otherData[0] || []).length; h++) {
      if (String(otherData[0][h] || '').toUpperCase().indexOf('CELULAR') !== -1) {
        otherColCelular = h;
        break;
      }
    }
    if (otherColCelular === -1) continue;
    for (var or = 1; or < otherData.length; or++) {
      var raw = otherData[or][otherColCelular];
      if (!raw || String(raw).trim() === '') continue;
      var clean = normalizePhoneETL(raw);
      if (clean && clean.length >= 9 && /^9\d{8}$/.test(clean)) {
        if (!phonesInOtherSheets.has(clean)) phonesInOtherSheets.set(clean, []);
        phonesInOtherSheets.get(clean).push(otherLeadSheets[o]);
      }
    }
  }
  
  // Set para detectar duplicados dentro de la misma hoja: la 1ra aparicion queda NUEVO; la 2da+ queda DUPLICADO_INTERNO.
  var seenInSheet = new Set();
  var nuevos = 0, duplicados = 0, invalidos = 0, duplicadosInternos = 0, duplicadosOtraBase = 0;
  
  // Agregar columna de ESTADO si no existe
  var estadoCol = headers.length; // siguiente columna disponible
  // Buscar si ya existe columna ESTADO_VALIDACION
  for (var c = 0; c < headers.length; c++) {
    if (String(headers[c]).toUpperCase().trim() === 'ESTADO_VALIDACION') {
      estadoCol = c;
      break;
    }
  }
  if (estadoCol === headers.length) {
    sourceSheet.getRange(1, estadoCol + 1).setValue('ESTADO_VALIDACION');
  }
  
  var estadoResults = [];
  
  for (var row = 1; row < data.length; row++) {
    var rawPhone = data[row][colCelular];
    var cleanPhone = normalizePhoneETL(rawPhone);
    
    if (!rawPhone || String(rawPhone).trim() === '') {
      estadoResults.push(['']);
      continue;
    }
    
    if (!cleanPhone || cleanPhone.length < 9 || !/^9\d{8}$/.test(cleanPhone)) {
      estadoResults.push(['INVALIDO']);
      invalidos++;
    } else if (crmPhoneHasActiveAdvisor_(crmOwnership[cleanPhone])) {
      var detalle = buildCrmDuplicateDetail_(crmOwnership[cleanPhone]);
      estadoResults.push([detalle]);
      duplicados++;
    } else if (phonesInOtherSheets.has(cleanPhone)) {
      var hojas = phonesInOtherSheets.get(cleanPhone);
      var detalle = 'DUPLICADO_OTRA_BASE: ya está en ' + (hojas.join(', ') || 'otra hoja');
      estadoResults.push([detalle]);
      duplicadosOtraBase++;
      seenInSheet.add(cleanPhone);
    } else if (seenInSheet.has(cleanPhone)) {
      // Numero repetido dentro de la misma columna CELULAR (2da+ aparicion)
      estadoResults.push(['DUPLICADO_INTERNO']);
      duplicadosInternos++;
      seenInSheet.add(cleanPhone);
    } else {
      estadoResults.push(['NUEVO']);
      seenInSheet.add(cleanPhone);
      nuevos++;
    }
  }
  
  // Escribir columna de estado
  if (estadoResults.length > 0) {
    sourceSheet.getRange(2, estadoCol + 1, estadoResults.length, 1).setValues(estadoResults);
    
    // Colorear filas según estado
    for (var r = 0; r < estadoResults.length; r++) {
      var estado = estadoResults[r][0];
      var bgWidth = Math.min(sourceSheet.getLastColumn(), estadoCol + 1);
      var rowRange = sourceSheet.getRange(r + 2, 1, 1, bgWidth);
      
      if (estado === 'NUEVO') {
        rowRange.setBackground('#dcfce7'); // verde suave
      } else if (String(estado).indexOf('DUPLICADO') !== -1) {
        rowRange.setBackground('#fef3c7'); // amarillo suave
      } else if (estado === 'INVALIDO') {
        rowRange.setBackground('#fecaca'); // rojo suave
      }
    }
  }
  
  var t1 = new Date().getTime();
  ui.alert(
    'Validación de Leads Completada',
    'Hoja: ' + sheetName + '\n\n' +
    '✅ Nuevos (listos para asignar): ' + nuevos + '\n' +
    '⚠️ Duplicados con asesor activo en CRM: ' + duplicados + '\n' +
    '📋 Duplicados en otra base (OPC/META/GOOGLE_ADS): ' + duplicadosOtraBase + '\n' +
    '🔄 Duplicados internos (misma hoja): ' + duplicadosInternos + '\n' +
    '❌ Inválidos: ' + invalidos + '\n\n' +
    'Los resultados están en la columna ESTADO_VALIDACION.\n' +
    'Filas verdes = NUEVO | Amarillas = DUPLICADO | Rojas = INVÁLIDO\n\n' +
    'Tiempo: ' + (t1 - t0) + 'ms',
    ui.ButtonSet.OK
  );
}

/**
 * =====================================================================
 * VALIDACION AUTOMATICA TIKTOK_LEADS (SPREADSHEET EXTERNO)
 * Abre la hoja externa donde llegan los leads limpios de TikTok y actualiza
 * ESTADO_VALIDACION contra el CRM:
 *   - DUPLICADO: el celular ya tiene asesor activo en DIM/FACT
 *   - NUEVO: el celular no existe o solo tiene historial de asesores inactivos
 *   - NO VALIDO: telefono invalido o fila marcada previamente como prueba
 * =====================================================================
 */
function validarTikTokLeadsManual() {
  return actualizarEstadoValidacionTikTokLeadsCore_(true);
}

function actualizarEstadoValidacionTikTokLeads() {
  return actualizarEstadoValidacionTikTokLeadsCore_(false);
}

function instalarTriggerValidacionTikTokLeads() {
  var handler = 'actualizarEstadoValidacionTikTokLeads';
  var triggers = ScriptApp.getProjectTriggers();

  for (var i = triggers.length - 1; i >= 0; i--) {
    if (triggers[i].getHandlerFunction() === handler) {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  var horas = (CONFIG_SYSTEM.TIKTOK && CONFIG_SYSTEM.TIKTOK.VALIDATION_TRIGGER_HOURS) || 3;
  ScriptApp.newTrigger(handler)
    .timeBased()
    .everyHours(Math.max(1, horas))
    .create();

  tiktokMostrarMensaje_(
    'Trigger TikTok instalado',
    'Se actualizará ESTADO_VALIDACION en TIKTOK_LEADS cada ' + horas + ' horas.'
  );
}

function desinstalarTriggerValidacionTikTokLeads() {
  var handler = 'actualizarEstadoValidacionTikTokLeads';
  var triggers = ScriptApp.getProjectTriggers();
  var eliminados = 0;

  for (var i = triggers.length - 1; i >= 0; i--) {
    if (triggers[i].getHandlerFunction() === handler) {
      ScriptApp.deleteTrigger(triggers[i]);
      eliminados++;
    }
  }

  tiktokMostrarMensaje_(
    'Trigger TikTok eliminado',
    'Triggers eliminados: ' + eliminados
  );
}

function tiktokMostrarMensaje_(titulo, mensaje) {
  try {
    SpreadsheetApp.getUi().alert(titulo, mensaje, SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (uiErr) {
    Logger.log('[TIKTOK-VALIDACION] ' + titulo + ' | ' + String(mensaje || '').replace(/\n/g, ' | '));
  }
}

function actualizarEstadoValidacionTikTokLeadsCore_(mostrarUi) {
  var t0 = new Date().getTime();
  var lock = LockService.getScriptLock();

  try {
    lock.waitLock(30000);

    var ssCrm = getMasterSpreadsheet_();
    var dimSheet = ssCrm.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
    if (!dimSheet) throw new Error('No se encontró la hoja ' + CONFIG_SYSTEM.SHEETS.DIM + ' en el CRM.');

    var telefonosCrm = tiktokConstruirSetTelefonosCrm_(ssCrm, dimSheet);

    var ssTikTok = SpreadsheetApp.openById(CONFIG_SYSTEM.TIKTOK.SPREADSHEET_ID);
    var hojaTikTok = ssTikTok.getSheetByName(CONFIG_SYSTEM.TIKTOK.SHEET_NAME);
    if (!hojaTikTok) throw new Error('No se encontró la hoja externa ' + CONFIG_SYSTEM.TIKTOK.SHEET_NAME + '.');

    var lastRow = hojaTikTok.getLastRow();
    var lastCol = hojaTikTok.getLastColumn();
    if (lastRow < 1 || lastCol < 1) {
      return tiktokReportarValidacion_(mostrarUi, {
        procesados: 0, nuevos: 0, duplicados: 0, invalidos: 0, vacios: 0,
        ms: new Date().getTime() - t0
      });
    }

    var headers = hojaTikTok.getRange(1, 1, 1, lastCol).getValues()[0];
    var colCelular = tiktokBuscarColumna_(headers, [
      'CELULAR',
      'Número de teléfono',
      'Numero de telefono',
      'TELEFONO',
      'TELÉFONO',
      'PHONE'
    ]);
    if (colCelular === -1) {
      throw new Error('No se encontró columna CELULAR / Número de teléfono en TIKTOK_LEADS.');
    }

    var colEstado = tiktokBuscarColumna_(headers, ['ESTADO_VALIDACION']);
    if (colEstado === -1) {
      colEstado = lastCol; // 0-based: siguiente columna disponible
      ensureSheetCapacity(hojaTikTok, Math.max(lastRow, 1), colEstado + 1);
      hojaTikTok.getRange(1, colEstado + 1).setValue('ESTADO_VALIDACION');
      lastCol = Math.max(lastCol, colEstado + 1);
    }

    if (lastRow < 2) {
      return tiktokReportarValidacion_(mostrarUi, {
        procesados: 0, nuevos: 0, duplicados: 0, invalidos: 0, vacios: 0,
        ms: new Date().getTime() - t0
      });
    }

    var numRows = lastRow - 1;
    var readCols = Math.max(lastCol, colEstado + 1, colCelular + 1);
    var data = hojaTikTok.getRange(2, 1, numRows, readCols).getValues();

    var estados = [];
    var stats = {
      procesados: 0,
      nuevos: 0,
      duplicados: 0,
      invalidos: 0,
      vacios: 0,
      ms: 0
    };

    for (var r = 0; r < data.length; r++) {
      var row = data[r];
      var rowText = row.join('').trim();
      if (!rowText) {
        estados.push(['']);
        stats.vacios++;
        continue;
      }

      var rawPhone = row[colCelular];
      var cleanPhone = tiktokNormalizarTelefonoCrm_(rawPhone);
      var estadoActual = String(row[colEstado] || '').trim().toUpperCase();
      var estadoFinal = '';

      if (!cleanPhone || !/^9\d{8}$/.test(cleanPhone)) {
        estadoFinal = 'NO VALIDO';
        stats.invalidos++;
      } else if (telefonosCrm.has(cleanPhone)) {
        estadoFinal = 'DUPLICADO';
        stats.duplicados++;
      } else if (estadoActual === 'NO VALIDO') {
        // Conserva leads de prueba marcados por el transformador externo.
        estadoFinal = 'NO VALIDO';
        stats.invalidos++;
      } else {
        estadoFinal = 'NUEVO';
        stats.nuevos++;
      }

      estados.push([estadoFinal]);
      stats.procesados++;
    }

    hojaTikTok.getRange(2, colEstado + 1, estados.length, 1).setValues(estados);
    stats.ms = new Date().getTime() - t0;

    Logger.log(
      '[TIKTOK-VALIDACION] Procesados=' + stats.procesados +
      ' | Nuevos=' + stats.nuevos +
      ' | Duplicados=' + stats.duplicados +
      ' | No validos=' + stats.invalidos +
      ' | Tiempo=' + stats.ms + 'ms'
    );

    return tiktokReportarValidacion_(mostrarUi, stats);

  } catch (e) {
    Logger.log('[TIKTOK-VALIDACION][ERROR] ' + e.message);
    if (mostrarUi) {
      tiktokMostrarMensaje_('Error validando TikTok Leads', e.message);
    }
    return { success: false, message: e.message };

  } finally {
    try { lock.releaseLock(); } catch (lockErr) {}
  }
}

function tiktokConstruirSetTelefonosCrm_(ssCrm, dimSheet) {
  var telefonos = new Set();

  function addPhone(raw) {
    var phone = tiktokNormalizarTelefonoCrm_(raw);
    if (phone && /^9\d{8}$/.test(phone)) telefonos.add(phone);
  }

  var factSheet = ssCrm.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var advisorLookup = getAdvisorStatusLookup(ssCrm);
  var crmOwnership = buildCrmLeadOwnershipMap(ssCrm, dimSheet, factSheet, advisorLookup);
  for (var phone in crmOwnership) {
    if (crmPhoneHasActiveAdvisor_(crmOwnership[phone])) addPhone(phone);
  }

  var advisors = getActiveAdvisors(ssCrm);
  for (var s = 0; s < advisors.length; s++) {
    var ctx = getAdvisorSheetContext_(advisors[s]);
    var sh = ctx.sheet;
    var name = ctx.sheetName;
    if (!sh) {
      logDebug('[TIKTOK-VALIDACION] Base no encontrada para asesor activo: ' + advisors[s].nombre + ' (' + ctx.baseCode + ')');
      continue;
    }

    var headerRow = findHeaderRow(sh);
    var lastSheetRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastSheetRow <= headerRow || lastCol < 1) continue;

    var headers = sh.getRange(headerRow, 1, 1, lastCol).getValues()[0];
    var colPhone = tiktokBuscarColumna_(headers, ['CELULAR', 'TELEFONO', 'TELÉFONO']);
    if (colPhone === -1) continue;

    var advisorPhones = sh.getRange(headerRow + 1, colPhone + 1, lastSheetRow - headerRow, 1).getValues();
    for (var a = 0; a < advisorPhones.length; a++) addPhone(advisorPhones[a][0]);
  }

  Logger.log('[TIKTOK-VALIDACION] Telefonos CRM con asesor activo cargados para cruce: ' + telefonos.size);
  return telefonos;
}

function tiktokNormalizarTelefonoCrm_(phone) {
  if (phone === null || phone === undefined || phone === '') return '';
  var p = String(phone).replace(/\D/g, '');
  if (p.length === 11 && p.substring(0, 2) === '51') p = p.substring(2);
  if (p.length === 10 && p.charAt(0) === '0') p = p.substring(1);
  if (p.length > 9) p = p.slice(-9);
  return /^9\d{8}$/.test(p) ? p : '';
}

function tiktokBuscarColumna_(headers, posiblesNombres) {
  var normalizados = headers.map(function(h) { return tiktokNormalizarHeader_(h); });
  for (var i = 0; i < posiblesNombres.length; i++) {
    var buscado = tiktokNormalizarHeader_(posiblesNombres[i]);
    var idx = normalizados.indexOf(buscado);
    if (idx !== -1) return idx;
  }
  return -1;
}

function tiktokNormalizarHeader_(valor) {
  return String(valor || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ');
}

function tiktokReportarValidacion_(mostrarUi, stats) {
  var result = {
    success: true,
    procesados: stats.procesados,
    nuevos: stats.nuevos,
    duplicados: stats.duplicados,
    invalidos: stats.invalidos,
    vacios: stats.vacios,
    ms: stats.ms
  };

  if (mostrarUi) {
    tiktokMostrarMensaje_(
      'Validación TikTok completada',
      'Hoja externa: ' + CONFIG_SYSTEM.TIKTOK.SHEET_NAME + '\n\n' +
      'Procesados: ' + stats.procesados + '\n' +
      'NUEVO: ' + stats.nuevos + '\n' +
      'DUPLICADO: ' + stats.duplicados + '\n' +
      'NO VALIDO: ' + stats.invalidos + '\n' +
      'Filas vacías: ' + stats.vacios + '\n\n' +
      'Tiempo: ' + stats.ms + 'ms'
    );
  }

  return result;
}

/**
 * =====================================================================
 * EXPORTAR DUPLICADOS VALIDADOS → HOJA CONSOLIDADA DE SUPERVISIÓN
 * Lee la hoja activa (LEADS_*), filtra filas con ESTADO_VALIDACION DUPLICADO,
 * obtiene de FACT la última gestión (asesor, fecha, tipificación, comentario)
 * y escribe todo en LEADS_DUPLICADOS_CONSOLIDADO para supervisión.
 * =====================================================================
 */
function exportarDuplicadosValidados() {
  var t0 = new Date().getTime();
  var ss = getActiveSpreadsheetSafe_() || getMasterSpreadsheet_();
  var masterSs = getMasterSpreadsheet_();
  var sourceSheet = ss.getActiveSheet();
  var dimSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var ui = SpreadsheetApp.getUi();
  var sheetName = sourceSheet.getName();

  var validSources = [CONFIG_SYSTEM.SHEETS.OPC, CONFIG_SYSTEM.SHEETS.META, CONFIG_SYSTEM.SHEETS.GOOGLE_ADS];
  if (validSources.indexOf(sheetName) === -1) {
    ui.alert('Error', 'Solo puedes exportar desde hojas LEADS_OPC, LEADS_META o LEADS_GOOGLE_ADS.\n\nHoja actual: ' + sheetName, ui.ButtonSet.OK);
    return;
  }

  if (!dimSheet || !factSheet) {
    ui.alert('Error', 'Faltan hojas DIM_CLIENTES o FACT_INTERACCIONES.', ui.ButtonSet.OK);
    return;
  }

  var data = sourceSheet.getDataRange().getValues();
  var headers = data[0];

  var colMap = { celular: -1, nombre: -1, proyecto: -1, fuente: -1, fRegistro: -1,
                 fechaHoy: -1, estadoCivil: -1, distrito: -1, comentarioOPC: -1,
                 puntoCaptacion: -1, estadoValidacion: -1, pareja: -1, ocupacion: -1 };

  for (var h = 0; h < headers.length; h++) {
    var hu = String(headers[h]).toUpperCase().trim();
    if (hu.indexOf('CELULAR') !== -1) colMap.celular = h;
    if (hu.indexOf('NOMBRE') !== -1 && hu.indexOf('OPC') === -1) colMap.nombre = h;
    if (hu.indexOf('PROYECTO') !== -1) colMap.proyecto = h;
    if (hu === 'FUENTE' || (hu.indexOf('FUENTE') !== -1 && hu.indexOf('NORMALIZ') === -1)) colMap.fuente = h;
    if (hu.indexOf('REGISTRO') !== -1) colMap.fRegistro = h;
    if (hu === 'FECHA HOY' || (hu.indexOf('FECHA') !== -1 && hu.indexOf('REGISTRO') === -1 && colMap.fechaHoy === -1)) colMap.fechaHoy = h;
    if (hu.indexOf('CIVIL') !== -1) colMap.estadoCivil = h;
    if (hu.indexOf('PAREJA') !== -1) colMap.pareja = h;
    if (hu.indexOf('OCUPACION') !== -1) colMap.ocupacion = h;
    if (hu.indexOf('DISTRITO') !== -1) colMap.distrito = h;
    if (hu.indexOf('COMENTARIO') !== -1 && (hu.indexOf('OPC') !== -1 || hu.indexOf('WSP') !== -1)) colMap.comentarioOPC = h;
    if (hu.indexOf('PUNTO') !== -1 || hu.indexOf('CAPTACI') !== -1) colMap.puntoCaptacion = h;
    if (hu === 'ESTADO_VALIDACION') colMap.estadoValidacion = h;
  }

  if (colMap.celular === -1 || colMap.estadoValidacion === -1) {
    ui.alert('Error', 'Primero ejecuta "Validar Leads" en esta hoja para generar la columna ESTADO_VALIDACION.', ui.ButtonSet.OK);
    return;
  }

  var phoneToClientId = {};
  var dimData = dimSheet.getDataRange().getValues();
  for (var i = 1; i < dimData.length; i++) {
    var p = normalizePhoneETL(dimData[i][1]);
    if (p) phoneToClientId[p] = dimData[i][0];
  }

  var lastInteractionByClient = getLastInteractionByClientId(factSheet);

  var duplicados = [];
  for (var row = 1; row < data.length; row++) {
    var estado = String(data[row][colMap.estadoValidacion] || '').trim();
    if (estado.indexOf('DUPLICADO') === -1) continue;

    var rawPhone = data[row][colMap.celular];
    var cleanPhone = normalizePhoneETL(rawPhone);
    if (!cleanPhone || cleanPhone.length < 9) continue;

    var clientId = phoneToClientId[cleanPhone];
    var lastInt = clientId ? lastInteractionByClient[clientId] : null;

    var fechaHoy = colMap.fechaHoy !== -1 ? data[row][colMap.fechaHoy] : new Date();
    var fechaHoyStr = fechaHoy instanceof Date
      ? Utilities.formatDate(fechaHoy, Session.getScriptTimeZone(), 'dd/MM/yyyy')
      : String(fechaHoy || '');

    var ultimaGestionStr = '';
    var ultimoAsesor = '';
    var tipifUltima = '';
    var comentarioUltima = '';

    if (lastInt) {
      ultimoAsesor = lastInt.asesor || '';
      ultimaGestionStr = lastInt.fecha ? formatDate(lastInt.fecha) : '';
      tipifUltima = lastInt.tipificacion || '';
      comentarioUltima = lastInt.comentario || '';
    } else {
      ultimoAsesor = 'SIN GESTIÓN PREVIA';
      ultimaGestionStr = '';
      tipifUltima = '';
      comentarioUltima = '';
    }

    duplicados.push({
      it: '',
      num: row + 1,
      fechaHoy: fechaHoyStr,
      proyecto: colMap.proyecto !== -1 ? String(data[row][colMap.proyecto] || '') : '',
      fuente: colMap.fuente !== -1 ? String(data[row][colMap.fuente] || '') : '',
      fRegistro: colMap.fRegistro !== -1 ? (data[row][colMap.fRegistro] instanceof Date
        ? Utilities.formatDate(data[row][colMap.fRegistro], Session.getScriptTimeZone(), 'dd/MM/yyyy')
        : String(data[row][colMap.fRegistro] || '')) : '',
      nombre: colMap.nombre !== -1 ? String(data[row][colMap.nombre] || '') : '',
      celular: cleanPhone,
      estadoCivil: colMap.estadoCivil !== -1 ? String(data[row][colMap.estadoCivil] || '') : '',
      pareja: colMap.pareja !== -1 ? String(data[row][colMap.pareja] || '') : '',
      ocupacion: colMap.ocupacion !== -1 ? String(data[row][colMap.ocupacion] || '') : '',
      distrito: colMap.distrito !== -1 ? String(data[row][colMap.distrito] || '') : '',
      tipif: tipifUltima,
      comentario: comentarioUltima,
      comentarioOPC: colMap.comentarioOPC !== -1 ? String(data[row][colMap.comentarioOPC] || '') : '',
      puntoCaptacion: colMap.puntoCaptacion !== -1 ? String(data[row][colMap.puntoCaptacion] || '') : '',
      estadoValidacion: estado,
      ultimoAsesor: ultimoAsesor,
      fechaUltimaGestion: ultimaGestionStr
    });
  }

  if (duplicados.length === 0) {
    ui.alert('Sin duplicados', 'No hay filas con ESTADO_VALIDACION = DUPLICADO en esta hoja.\n\nEjecuta "Validar Leads" primero.', ui.ButtonSet.OK);
    return;
  }

  var consSheet = ensureDuplicadosConsolidadoSheet(masterSs);
  var lastRow = consSheet.getLastRow();
  var startRow = lastRow < 1 ? 2 : lastRow + 1;

  var CONSOLIDADO_HEADERS = [
    'IT', 'NUM', 'FECHA HOY', 'PROYECTO', 'FUENTE', 'F. DE REGISTRO CLIENTE', 'NOMBRES Y APELLIDOS', 'CELULAR',
    'E. CIVIL', 'PAREJA', 'OCUPACION', 'DISTRITO', 'TIPIF', 'COMENTARIO', 'COMENTARIO OPC / WSP', 'PUNTO DE CAPTACIÓN',
    'ESTADO_VALIDACION', 'ULTIMO_ASESOR', 'FECHA_ULTIMA_GESTION'
  ];

  if (lastRow < 1) {
    consSheet.getRange(1, 1, 1, CONSOLIDADO_HEADERS.length).setValues([CONSOLIDADO_HEADERS]);
    consSheet.getRange(1, 1, 1, CONSOLIDADO_HEADERS.length).setFontWeight('bold').setBackground('#fef3c7');
    startRow = 2;
  }

  var rowsToWrite = [];
  for (var d = 0; d < duplicados.length; d++) {
    var dup = duplicados[d];
    rowsToWrite.push([
      dup.it,
      dup.num,
      dup.fechaHoy,
      dup.proyecto,
      dup.fuente,
      dup.fRegistro,
      dup.nombre,
      dup.celular,
      dup.estadoCivil,
      dup.pareja,
      dup.ocupacion,
      dup.distrito,
      dup.tipif,
      dup.comentario,
      dup.comentarioOPC,
      dup.puntoCaptacion,
      dup.estadoValidacion,
      dup.ultimoAsesor,
      dup.fechaUltimaGestion
    ]);
  }

  var writeRange = consSheet.getRange(startRow, 1, rowsToWrite.length, CONSOLIDADO_HEADERS.length);
  writeRange.setValues(rowsToWrite);
  writeRange.setBackground('#fef3c7');

  var t1 = new Date().getTime();
  logDebug('[EXPORT-DUPLICADOS] Exportados ' + duplicados.length + ' en ' + (t1 - t0) + 'ms');

  ui.alert(
    'Exportación Completada',
    'Hoja origen: ' + sheetName + '\n\n' +
    'Duplicados exportados: ' + duplicados.length + '\n\n' +
    'Los registros se han añadido a:\n' + CONFIG_SYSTEM.SHEETS.DUPLICADOS + '\n\n' +
    'Columnas: datos del lead + ULTIMO_ASESOR + FECHA_ULTIMA_GESTION (con hora)\n\n' +
    'Tiempo: ' + (t1 - t0) + 'ms',
    ui.ButtonSet.OK
  );
}

/**
 * Construye map clientId -> { fecha, asesor, tipificacion, comentario } con la última interacción de FACT.
 */
function getLastInteractionByClientId(factSheet) {
  var result = {};
  if (!factSheet || factSheet.getLastRow() < 2) return result;

  var factData = factSheet.getDataRange().getValues();
  for (var i = 1; i < factData.length; i++) {
    var row = factData[i];
    var clientId = row[1];
    var fecha = row[6];
    if (!clientId) continue;

    if (!result[clientId] || (fecha && fecha instanceof Date && (!result[clientId].fecha || fecha > result[clientId].fecha))) {
      result[clientId] = {
        fecha: fecha,
        asesor: row[4] || '',
        tipificacion: row[13] || '',
        comentario: row[14] || ''
      };
    }
  }
  return result;
}

/**
 * Crea o obtiene la hoja LEADS_DUPLICADOS_CONSOLIDADO con headers.
 */
function ensureDuplicadosConsolidadoSheet(ss) {
  var sheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DUPLICADOS);
  if (!sheet) {
    sheet = ss.insertSheet(CONFIG_SYSTEM.SHEETS.DUPLICADOS);
    logDebug('[EXPORT-DUPLICADOS] Hoja creada: ' + CONFIG_SYSTEM.SHEETS.DUPLICADOS);
  }
  return sheet;
}

/**
 * =====================================================================
 * ASIGNACIÓN ROUND-ROBIN MULTI-NIVEL
 * Lee leads NUEVOS de la hoja activa (LEADS_*), los agrupa por
 * FUENTE_NORMALIZADA y luego por DISTRITO, y distribuye equitativamente
 * entre los asesores ACTIVOS de CONFIG_MAESTROS.
 * Escribe en la hoja de cada asesor + registra en DIM + FACT.
 * =====================================================================
 */
function asignarLeadsRoundRobin() {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(60000);  // 60s para asignaciones grandes
  } catch (e) {
    SpreadsheetApp.getUi().alert('Sistema ocupado', 'Otra asignación o proceso está en curso. Intenta en 1 minuto.', SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  try {
    return asignarLeadsRoundRobinCore();
  } finally {
    lock.releaseLock();
  }
}

function asignarLeadsRoundRobinCore() {
  var t0 = new Date().getTime();
  var ss = getActiveSpreadsheetSafe_() || getMasterSpreadsheet_();
  var masterSs = getMasterSpreadsheet_();
  var sourceSheet = ss.getActiveSheet();
  var dimSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var ui = SpreadsheetApp.getUi();
  var sheetName = sourceSheet.getName();
  
  // Solo permitir desde hojas LEADS_*
  var validSources = [CONFIG_SYSTEM.SHEETS.OPC, CONFIG_SYSTEM.SHEETS.META, CONFIG_SYSTEM.SHEETS.GOOGLE_ADS];
  if (validSources.indexOf(sheetName) === -1) {
    ui.alert('Error', 'Solo puedes asignar desde hojas LEADS_OPC, LEADS_META o LEADS_GOOGLE_ADS.', ui.ButtonSet.OK);
    return;
  }
  
  // 1. Obtener asesores activos
  var advisors = getActiveAdvisors(masterSs);
  if (advisors.length === 0) {
    ui.alert('Error', 'No hay asesores ACTIVOS en CONFIG_MAESTROS.\nAgrega entradas con TIPO=ASESOR.', ui.ButtonSet.OK);
    return;
  }
  
  // 2. Leer datos y filtrar solo NUEVO (debe haber ejecutado validarLeadsRaw primero)
  var data = sourceSheet.getDataRange().getValues();
  var headers = data[0];
  
  // Mapear columnas
  var colMap = { celular: -1, nombre: -1, proyecto: -1, fuente: -1, fRegistro: -1,
                 fechaHoy: -1, estadoCivil: -1, distrito: -1, comentarioOPC: -1,
                 puntoCaptacion: -1, estadoValidacion: -1 };
  
  for (var h = 0; h < headers.length; h++) {
    var hu = String(headers[h]).toUpperCase().trim();
    if (hu.indexOf('CELULAR') !== -1) colMap.celular = h;
    if (hu.indexOf('NOMBRE') !== -1 && hu.indexOf('OPC') === -1) colMap.nombre = h;
    if (hu.indexOf('PROYECTO') !== -1) colMap.proyecto = h;
    if (hu === 'FUENTE' || hu.indexOf('FUENTE') !== -1 && hu.indexOf('NORMALIZ') === -1) colMap.fuente = h;
    if (hu.indexOf('REGISTRO') !== -1) colMap.fRegistro = h;
    if (hu === 'FECHA HOY' || (hu.indexOf('FECHA') !== -1 && hu.indexOf('REGISTRO') === -1 && colMap.fechaHoy === -1)) colMap.fechaHoy = h;
    if (hu.indexOf('CIVIL') !== -1) colMap.estadoCivil = h;
    if (hu.indexOf('DISTRITO') !== -1) colMap.distrito = h;
    if (hu.indexOf('COMENTARIO') !== -1 && (hu.indexOf('OPC') !== -1 || hu.indexOf('WSP') !== -1)) colMap.comentarioOPC = h;
    if (hu.indexOf('PUNTO') !== -1 || hu.indexOf('CAPTACI') !== -1) colMap.puntoCaptacion = h;
    if (hu === 'ESTADO_VALIDACION') colMap.estadoValidacion = h;
  }
  
  if (colMap.celular === -1) {
    ui.alert('Error', 'No se encontró columna CELULAR.', ui.ButtonSet.OK);
    return;
  }
  
  if (colMap.estadoValidacion === -1) {
    ui.alert('Error', 'Primero ejecuta "Validar Leads" para marcar cuáles son NUEVOS.', ui.ButtonSet.OK);
    return;
  }
  
  // 3. Filtrar leads NUEVOS y construir objetos lead
  var leadsNuevos = [];
  for (var row = 1; row < data.length; row++) {
    var estado = String(data[row][colMap.estadoValidacion]).trim();
    if (estado !== 'NUEVO') continue;
    
    var rawPhone = data[row][colMap.celular];
    var cleanPhone = normalizePhoneETL(rawPhone);
    if (!cleanPhone || cleanPhone.length < 9) continue;
    
    var rawFuente = colMap.fuente !== -1 ? (data[row][colMap.fuente] || '') : '';
    var fuenteInfo = parseFuente(rawFuente, cleanPhone);
    
    // Normalizar datos al leer
    var rawDistrito = colMap.distrito !== -1 ? (data[row][colMap.distrito] || '') : '';
    var distritoNorm = normalizarDistrito(rawDistrito) || 'SIN_DISTRITO';
    
    leadsNuevos.push({
      celular: cleanPhone,
      nombre: colMap.nombre !== -1 ? String(data[row][colMap.nombre] || '').toUpperCase() : '',
      proyecto: colMap.proyecto !== -1 ? String(data[row][colMap.proyecto] || '').toUpperCase() : '',
      fuenteOriginal: fuenteInfo.original,
      fuenteNormalizada: fuenteInfo.normalizada || 'SIN_FUENTE',
      nombreOPC: fuenteInfo.nombreOPC,
      fRegistro: colMap.fRegistro !== -1 ? data[row][colMap.fRegistro] : '',
      fechaHoy: colMap.fechaHoy !== -1 ? data[row][colMap.fechaHoy] : '',
      estadoCivil: colMap.estadoCivil !== -1 ? String(data[row][colMap.estadoCivil] || '').toUpperCase() : '',
      distrito: distritoNorm,
      comentarioOPC: colMap.comentarioOPC !== -1 ? String(data[row][colMap.comentarioOPC] || '') : '',
      puntoCaptacion: colMap.puntoCaptacion !== -1 ? String(data[row][colMap.puntoCaptacion] || '').toUpperCase() : '',
      filaOrigen: row + 1
    });
  }
  
  if (leadsNuevos.length === 0) {
    ui.alert('Sin leads', 'No hay leads marcados como NUEVO para asignar.\nPrimero ejecuta "Validar Leads".', ui.ButtonSet.OK);
    return;
  }

  // Re-validar contra CRM: solo bloquear si ya existe asesor ACTIVO en DIM o FACT.
  var crmOwnership = buildCrmLeadOwnershipMap(masterSs, dimSheet, factSheet);
  var antesFiltro = leadsNuevos.length;
  var leadsRevalidados = [];
  for (var rv = 0; rv < leadsNuevos.length; rv++) {
    var crmStatus = crmOwnership[leadsNuevos[rv].celular];
    if (crmPhoneHasActiveAdvisor_(crmStatus)) continue;
    leadsNuevos[rv].crmStatus = crmStatus || null;
    leadsRevalidados.push(leadsNuevos[rv]);
  }
  leadsNuevos = leadsRevalidados;
  if (antesFiltro > leadsNuevos.length) {
    logDebug('[ASIGNACION] Filtrados ' + (antesFiltro - leadsNuevos.length) + ' leads con asesor ACTIVO en CRM (validacion desactualizada)');
    if (leadsNuevos.length === 0) {
      ui.alert('Sin leads', 'Todos los leads NUEVOS ya tienen asesor ACTIVO en DIM/FACT.\nEjecuta "Validar Leads" de nuevo para actualizar el estado.', ui.ButtonSet.OK);
      return;
    }
  }
  
  // Límite diario (configurable en CONFIG_SYSTEM)
  var LIMITE_DIARIO = CONFIG_SYSTEM.LIMITE_DIARIO_ASIGNACION || 40;
  var maxTotal = LIMITE_DIARIO * advisors.length;
  
  var resp = ui.alert(
    'Confirmar Asignación Round-Robin',
    'Leads NUEVOS a asignar: ' + leadsNuevos.length + '\n' +
    'Asesores activos: ' + advisors.length + ' (' + advisors.map(function(a) { return a.nombre; }).join(', ') + ')\n' +
    'Límite por asesor/día: ' + LIMITE_DIARIO + '\n' +
    'Máximo asignable: ' + maxTotal + '\n\n' +
    'Distribución: Round-Robin por FUENTE + DISTRITO\n\n' +
    '¿Proceder con la asignación?',
    ui.ButtonSet.YES_NO
  );
  
  if (resp !== ui.Button.YES) return;
  
  // 4. ALGORITMO ROUND-ROBIN MULTI-NIVEL
  logDebug('[ASIGNACION] Iniciando Round-Robin | Leads: ' + leadsNuevos.length + ' | Asesores: ' + advisors.length);
  
  // Inicializar contadores por asesor
  var advisorLeads = {};  // { "LEONEL P.": [] }
  var advisorCount = {};  // { "LEONEL P.": 0 }
  for (var a = 0; a < advisors.length; a++) {
    advisorLeads[advisors[a].nombre] = [];
    advisorCount[advisors[a].nombre] = 0;
  }
  
  // 4.a Agrupar por FUENTE_NORMALIZADA
  var byFuente = {};
  for (var f = 0; f < leadsNuevos.length; f++) {
    var fn = leadsNuevos[f].fuenteNormalizada;
    if (!byFuente[fn]) byFuente[fn] = [];
    byFuente[fn].push(leadsNuevos[f]);
  }
  
  // 4.b Para cada fuente, sub-agrupar por DISTRITO y distribuir RR
  var fuenteKeys = Object.keys(byFuente);
  for (var fk = 0; fk < fuenteKeys.length; fk++) {
    var fuenteLeads = byFuente[fuenteKeys[fk]];
    
    // Sub-agrupar por distrito
    var byDistrito = {};
    for (var dl = 0; dl < fuenteLeads.length; dl++) {
      var dist = fuenteLeads[dl].distrito || 'SIN_DISTRITO';
      if (!byDistrito[dist]) byDistrito[dist] = [];
      byDistrito[dist].push(fuenteLeads[dl]);
    }
    
    // Round-Robin dentro de cada sub-grupo de distrito
    var distritoKeys = Object.keys(byDistrito);
    for (var dk = 0; dk < distritoKeys.length; dk++) {
      var distritoLeads = byDistrito[distritoKeys[dk]];
      
      for (var ld = 0; ld < distritoLeads.length; ld++) {
        // Encontrar asesor con menor carga que no haya llegado al límite
        var minCount = Infinity;
        var targetAdvisor = null;
        
        for (var av = 0; av < advisors.length; av++) {
          var advName = advisors[av].nombre;
          if (advisorCount[advName] < LIMITE_DIARIO && advisorCount[advName] < minCount) {
            minCount = advisorCount[advName];
            targetAdvisor = advName;
          }
        }
        
        if (!targetAdvisor) {
          logDebug('[ASIGNACION] Todos los asesores alcanzaron el límite diario');
          break;
        }
        
        advisorLeads[targetAdvisor].push(distritoLeads[ld]);
        advisorCount[targetAdvisor]++;
      }
    }
  }
  
  // 5. ESCRIBIR EN HOJAS DE ASESORES + DIM + FACT
  var timestamp = new Date();
  var fechaHoy = timestamp;
  var userEmail = Session.getActiveUser().getEmail() || 'SISTEMA';
  var totalAsignados = 0;
  var newDimRows = [];
  var dimUpdateRows = [];
  var newFactRows = [];
  var assignedSourceRows = {};
  
  var advisorByName = {};
  for (var abn = 0; abn < advisors.length; abn++) {
    advisorByName[advisors[abn].nombre] = advisors[abn];
  }
  var advisorNames = Object.keys(advisorLeads);
  for (var an = 0; an < advisorNames.length; an++) {
    var advName = advisorNames[an];
    var advLeads = advisorLeads[advName];
    
    if (advLeads.length === 0) continue;
    
    // Obtener o crear hoja del asesor (fila 1 = botón "GESTIONAR CLIENTE", fila 2 = encabezados, datos desde fila 3)
    var advCtx = getAdvisorSheetContext_(advisorByName[advName] || { nombre: advName, descripcion: '' });
    var advSheet = advCtx.sheet;
    if (!advSheet) {
      advSheet = advCtx.spreadsheet.insertSheet(advName);
      var advHeaders = [
        "IT", "NUM", "FECHA HOY", "PROYECTO", "FUENTE",
        "F. DE REGISTRO CLIENTE", "NOMBRES Y APELLIDOS", "CELULAR",
        "E. CIVIL", "PAREJA", "OCUPACION", "DISTRITO",
        "TIPIF", "COMENTARIO", "COMENTARIO OPC / WSP", "PUNTO DE CAPTACIÓN"
      ];
      advSheet.insertRowsBefore(1, 1);
      advSheet.getRange(2, 1, 1, advHeaders.length).setValues([advHeaders]);
      formatHeaderRow(advSheet, CONFIG_SYSTEM.ADVISOR_SHEET.ROW_HEADERS, advHeaders.length, "#1e293b");
      advSheet.setFrozenRows(2);
      logDebug('[ASIGNACION] Hoja creada para "' + advName + '" en base ' + advCtx.baseCode);
    }
    
    // Determinar última fila de datos (encabezados en ROW_HEADERS=2, datos desde ROW_FIRST_DATA=3)
    var lastRow = advSheet.getLastRow();
    var startNum = 1;
    
    // Buscar el último NUM del día actual para continuar numeración
    // Detectar columna de FECHA HOY dinámicamente
    var advHeaderRow = findHeaderRow(advSheet);
    var advLastCol = advSheet.getLastColumn();
    if (advLastCol < 1) advLastCol = 16;
    var advHeaders2 = advSheet.getRange(advHeaderRow, 1, 1, advLastCol).getValues()[0];
    var colFechaIdx = -1;
    var colNumIdx = -1;
    for (var ch = 0; ch < advHeaders2.length; ch++) {
      var hh = String(advHeaders2[ch]).toUpperCase().trim();
      if (hh === 'FECHA HOY' || (hh.indexOf('FECHA') !== -1 && hh.indexOf('REGISTRO') === -1 && colFechaIdx === -1)) colFechaIdx = ch;
      if (hh === 'IT' || hh === 'NUM') colNumIdx = ch;
    }
    
    if (lastRow > advHeaderRow && colFechaIdx !== -1 && colNumIdx !== -1) {
      var lastData = advSheet.getRange(lastRow, 1, 1, Math.max(colFechaIdx, colNumIdx) + 1).getValues()[0];
      var lastFecha = lastData[colFechaIdx];
      if (lastFecha instanceof Date) {
        var today = new Date();
        if (lastFecha.toDateString() === today.toDateString()) {
          startNum = (parseInt(lastData[colNumIdx]) || 0) + 1;
        }
      }
    }
    
    // Detectar columnas para escribir datos
    var writeColMap = {};
    for (var wh = 0; wh < advHeaders2.length; wh++) {
      var whdr = String(advHeaders2[wh]).toUpperCase().trim();
      if (whdr === 'IT' || whdr === 'NUM') writeColMap.num = wh;
      if (whdr === 'FECHA HOY' || (whdr.indexOf('FECHA') !== -1 && whdr.indexOf('REGISTRO') === -1 && !writeColMap.fechaHoy)) writeColMap.fechaHoy = wh;
      if (whdr.indexOf('PROYECTO') !== -1) writeColMap.proyecto = wh;
      if (whdr.indexOf('FUENTE') !== -1 && whdr.indexOf('NORMALIZADA') === -1) writeColMap.fuente = wh;
      if (whdr.indexOf('REGISTRO') !== -1) writeColMap.fRegistro = wh;
      if (whdr.indexOf('NOMBRE') !== -1 && whdr.indexOf('OPC') === -1) writeColMap.nombre = wh;
      if (whdr.indexOf('CELULAR') !== -1) writeColMap.celular = wh;
      if (whdr.indexOf('CIVIL') !== -1) writeColMap.estadoCivil = wh;
      if (whdr.indexOf('PAREJA') !== -1) writeColMap.pareja = wh;
      if (whdr.indexOf('OCUPACION') !== -1) writeColMap.ocupacion = wh;
      if (whdr.indexOf('DISTRITO') !== -1) writeColMap.distrito = wh;
      if (whdr === 'TIPIF' || whdr === 'TIPIFICACION') writeColMap.tipif = wh;
      if (whdr.indexOf('COMENTARIO') !== -1 && whdr.indexOf('OPC') === -1 && whdr.indexOf('WSP') === -1) writeColMap.comentario = wh;
      if (whdr.indexOf('OPC') !== -1 && whdr.indexOf('COMENTARIO') !== -1) writeColMap.comentarioOPC = wh;
      if (whdr.indexOf('PUNTO') !== -1 || whdr.indexOf('CAPTACION') !== -1) writeColMap.puntoCaptacion = wh;
    }
    
    var totalCols = advHeaders2.length;
    
    // Construir filas para la hoja del asesor
    var advRows = [];
    for (var li = 0; li < advLeads.length; li++) {
      var lead = advLeads[li];
      var existingStatus = lead.crmStatus || null;
      var hasExistingDim = !!(existingStatus && existingStatus.dimRow);
      var clientId = (existingStatus && (existingStatus.dimClientId || existingStatus.factClientId)) || ('CLI_' + Utilities.getUuid());
      var fRegistro = lead.fRegistro || (existingStatus && existingStatus.dimFechaRegistro) || timestamp;
      
      // Fila vacía del tamaño correcto
      var advRow = new Array(totalCols);
      for (var z = 0; z < totalCols; z++) advRow[z] = '';
      
      // Llenar con datos mapeados
      if (writeColMap.num !== undefined) advRow[writeColMap.num] = startNum + li;
      if (writeColMap.fechaHoy !== undefined) advRow[writeColMap.fechaHoy] = fechaHoy;
      if (writeColMap.proyecto !== undefined) advRow[writeColMap.proyecto] = lead.proyecto;
      if (writeColMap.fuente !== undefined) advRow[writeColMap.fuente] = lead.fuenteOriginal;
      if (writeColMap.fRegistro !== undefined) advRow[writeColMap.fRegistro] = fRegistro;
      if (writeColMap.nombre !== undefined) advRow[writeColMap.nombre] = lead.nombre || '';
      if (writeColMap.celular !== undefined) advRow[writeColMap.celular] = lead.celular;
      if (writeColMap.estadoCivil !== undefined) advRow[writeColMap.estadoCivil] = lead.estadoCivil || '';
      if (writeColMap.distrito !== undefined) advRow[writeColMap.distrito] = lead.distrito || '';
      if (writeColMap.comentarioOPC !== undefined) advRow[writeColMap.comentarioOPC] = lead.comentarioOPC || '';
      if (writeColMap.puntoCaptacion !== undefined) advRow[writeColMap.puntoCaptacion] = lead.puntoCaptacion || '';
      
      advRows.push(advRow);
      
      // Fila para DIM_CLIENTES (16 cols)
      if (hasExistingDim) {
        var updatedDimRow = existingStatus.dimRowData.slice ? existingStatus.dimRowData.slice() : existingStatus.dimRowData.map(function(x) { return x; });
        while (updatedDimRow.length < 16) updatedDimRow.push('');
        updatedDimRow[0] = updatedDimRow[0] || clientId;
        updatedDimRow[1] = lead.celular;
        if (lead.nombre) updatedDimRow[2] = lead.nombre;
        if (lead.fuenteOriginal) updatedDimRow[4] = lead.fuenteOriginal;
        if (lead.proyecto) updatedDimRow[5] = lead.proyecto;
        updatedDimRow[6] = updatedDimRow[6] || fRegistro;
        updatedDimRow[7] = advName;
        updatedDimRow[8] = '';
        updatedDimRow[9] = fechaHoy;
        updatedDimRow[10] = lead.fuenteNormalizada || updatedDimRow[10] || 'SIN_FUENTE';
        if (lead.nombreOPC) updatedDimRow[11] = lead.nombreOPC;
        if (lead.estadoCivil) updatedDimRow[12] = lead.estadoCivil;
        if (lead.distrito) updatedDimRow[15] = lead.distrito;
        dimUpdateRows.push({ row: existingStatus.dimRow, before: existingStatus.dimRowData, data: updatedDimRow });
        existingStatus.dimRowData = updatedDimRow;
        existingStatus.dimClientId = clientId;
      } else {
        newDimRows.push([
          clientId,
          lead.celular,
          lead.nombre || 'Sin Nombre',
          '',
          lead.fuenteOriginal || 'ASIGNACION_' + sheetName,
          lead.proyecto,
          fRegistro,
          advName,    // ASESOR_ACTUAL = nombre del asesor asignado
          '',         // ULTIMA_TIPIF (vacio, lo llenara el asesor)
          fechaHoy,
          lead.fuenteNormalizada,
          lead.nombreOPC,
          lead.estadoCivil || '',
          '',
          '',
          lead.distrito || ''
        ]);
      }
      
      // Fila para FACT_INTERACCIONES (16 cols)
      newFactRows.push([
        'INT_' + Utilities.getUuid(),
        clientId,
        lead.celular,
        lead.nombre || 'Sin Nombre',
        advName,
        userEmail,
        fechaHoy,
        fRegistro,
        lead.proyecto,
        lead.fuenteOriginal,
        lead.fuenteNormalizada,
        lead.nombreOPC,
        'ASIGNACION',
        '',
        lead.comentarioOPC || '',
        JSON.stringify({ asignacion: sheetName, fila: lead.filaOrigen })
      ]);
      
      totalAsignados++;
      assignedSourceRows[lead.filaOrigen] = true;
    }
    
    // Batch write a hoja del asesor (notación A1 para evitar ambigüedad en getRange)
    if (advRows.length > 0) {
      try {
        var startRow = lastRow + 1;
        var endRow = lastRow + advRows.length;
        ensureSheetCapacity(advSheet, endRow, totalCols);
        var rangeA1 = 'A' + startRow + ':' + columnToLetter(totalCols) + endRow;
        var writeRange = advSheet.getRange(rangeA1);
        // 1. Escribir valores (sin clearDataValidations para no borrar estilos)
        writeRange.setValues(advRows);
        // 2. Copiar formato de la fila anterior si existe (mantener estilo de celda)
        if (lastRow >= CONFIG_SYSTEM.ADVISOR_SHEET.ROW_FIRST_DATA) {
          var srcA1 = 'A' + lastRow + ':' + columnToLetter(totalCols) + lastRow;
          var dstA1 = 'A' + startRow + ':' + columnToLetter(totalCols) + endRow;
          advSheet.getRange(srcA1).copyTo(advSheet.getRange(dstA1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
        }
        // 3. Aplicar validaciones de lista (TIPIF, E. CIVIL, OCUPACION, DISTRITO) desde CONFIG_MAESTROS
        aplicarValidacionesRangoAsesor(advSheet, lastRow + 1, lastRow + advRows.length, writeColMap);
        logDebug('[ASIGNACION] ✅ Escritas ' + advRows.length + ' filas en "' + advName + '" (cols: ' + totalCols + ', startRow: ' + (lastRow + 1) + ') con formato y dropdowns');
      } catch (writeErr) {
        logError('[ASIGNACION] ❌ Error escribiendo en "' + advName + '": ' + writeErr.message + 
                 ' | lastRow=' + lastRow + ' rows=' + advRows.length + ' cols=' + totalCols + 
                 ' rowLength=' + (advRows[0] ? advRows[0].length : 'empty'));
        throw new Error('Error escribiendo en hoja "' + advName + '": ' + writeErr.message);
      }
    }
  }
  
  // PASO 1: Marcar leads como ASIGNADO (evita duplicación si se re-ejecuta)
  var sheetLastCol = sourceSheet.getLastColumn();
  var rowsMarked = [];  // Para rollback si falla DIM/FACT
  for (var mr = 1; mr < data.length; mr++) {
    var estadoVal = String(data[mr][colMap.estadoValidacion]).trim();
    if (estadoVal === 'NUEVO' && assignedSourceRows[mr + 1]) {
      try {
        sourceSheet.getRange(mr + 1, colMap.estadoValidacion + 1).setValue('ASIGNADO');
        var bgCols = Math.min(sheetLastCol, colMap.estadoValidacion + 1);
        if (bgCols > 0) {
          sourceSheet.getRange(mr + 1, 1, 1, bgCols).setBackground('#e0e7ff');
        }
        rowsMarked.push(mr + 1);
      } catch (markErr) {
        logError('[ASIGNACION] Error marcando fila ' + (mr + 1) + ': ' + markErr.message);
      }
    }
  }
  SpreadsheetApp.flush();
  
  var dimLastRowBefore = dimSheet.getLastRow();
  var dimWritten = false;
  var dimUpdatesWritten = false;
  
  try {
    // PASO 2: Batch write a DIM_CLIENTES (notación A1)
    if (dimUpdateRows.length > 0) {
      var dimUpdatesByRow = {};
      for (var du = 0; du < dimUpdateRows.length; du++) {
        dimUpdatesByRow[dimUpdateRows[du].row] = dimUpdateRows[du];
      }
      var dimUpdateKeys = Object.keys(dimUpdatesByRow).map(Number).sort(function(a, b) { return a - b; });
      for (var duk = 0; duk < dimUpdateKeys.length; duk++) {
        var updateItem = dimUpdatesByRow[dimUpdateKeys[duk]];
        dimSheet.getRange(updateItem.row, 1, 1, 16).setValues([updateItem.data]);
      }
      dimUpdatesWritten = true;
      logDebug('[ASIGNACION] DIM: ' + dimUpdateKeys.length + ' filas existentes actualizadas');
    }

    if (newDimRows.length > 0) {
      var dimLastRow = dimSheet.getLastRow();
      var dimStartRow = dimLastRow + 1;
      var dimEndRow = dimLastRow + newDimRows.length;
      ensureSheetCapacity(dimSheet, dimEndRow, 16);
      var dimRangeA1 = 'A' + dimStartRow + ':' + columnToLetter(16) + dimEndRow;
      var dimRange = dimSheet.getRange(dimRangeA1);
      dimRange.clearDataValidations();
      dimRange.setValues(newDimRows);
      dimWritten = true;
      logDebug('[ASIGNACION] DIM: ' + newDimRows.length + ' filas escritas');
    }
    
    // PASO 3: Batch write a FACT_INTERACCIONES (notación A1)
    if (newFactRows.length > 0) {
      var factLastRow = factSheet.getLastRow();
      var factStartRow = factLastRow + 1;
      var factEndRow = factLastRow + newFactRows.length;
      ensureSheetCapacity(factSheet, factEndRow, 16);
      var factRangeA1 = 'A' + factStartRow + ':' + columnToLetter(16) + factEndRow;
      var factRange = factSheet.getRange(factRangeA1);
      factRange.clearDataValidations();
      factRange.setValues(newFactRows);
      logDebug('[ASIGNACION] FACT: ' + newFactRows.length + ' filas escritas');
    }
  } catch (writeErr) {
    logError('[ASIGNACION] Error en DIM/FACT: ' + writeErr.message);
    // ROLLBACK: Revertir marcas ASIGNADO → NUEVO
    for (var rb = 0; rb < rowsMarked.length; rb++) {
      try {
        sourceSheet.getRange(rowsMarked[rb], colMap.estadoValidacion + 1).setValue('NUEVO');
        var bgCols = Math.min(sheetLastCol, colMap.estadoValidacion + 1);
        if (bgCols > 0) {
          sourceSheet.getRange(rowsMarked[rb], 1, 1, bgCols).setBackground('#dcfce7');
        }
      } catch (rbErr) {
        logError('[ASIGNACION] Rollback fila ' + rowsMarked[rb] + ': ' + rbErr.message);
      }
    }
    if (dimWritten && newDimRows.length > 0) {
      try {
        dimSheet.deleteRows(dimLastRowBefore + 1, newDimRows.length);
        logDebug('[ASIGNACION] Rollback: filas DIM eliminadas');
      } catch (delErr) {
        logError('[ASIGNACION] No se pudo revertir DIM: ' + delErr.message);
      }
    }
    if (dimUpdatesWritten && dimUpdateRows.length > 0) {
      try {
        var rollbackByRow = {};
        for (var dur = 0; dur < dimUpdateRows.length; dur++) {
          rollbackByRow[dimUpdateRows[dur].row] = dimUpdateRows[dur].before;
        }
        var rollbackRows = Object.keys(rollbackByRow).map(Number).sort(function(a, b) { return a - b; });
        for (var rr = 0; rr < rollbackRows.length; rr++) {
          dimSheet.getRange(rollbackRows[rr], 1, 1, 16).setValues([rollbackByRow[rollbackRows[rr]]]);
        }
        logDebug('[ASIGNACION] Rollback: filas DIM existentes restauradas');
      } catch (updRbErr) {
        logError('[ASIGNACION] No se pudo revertir actualizaciones DIM: ' + updRbErr.message);
      }
    }
    SpreadsheetApp.getUi().alert('Error en asignación', 'No se pudo completar. Los leads se han revertido a NUEVO.\n\n' + writeErr.message, SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  
  invalidateCache();
  
  var t1 = new Date().getTime();
  
  // Construir resumen por asesor
  var resumenAsesores = '';
  for (var ra = 0; ra < advisors.length; ra++) {
    var advN = advisors[ra].nombre;
    resumenAsesores += '• ' + advN + ': ' + advisorCount[advN] + ' leads\n';
  }
  
  logDebug('[ASIGNACION] ✅ Completado en ' + (t1 - t0) + 'ms | Total: ' + totalAsignados);
  
  ui.alert(
    'Asignación Round-Robin Completada',
    'Fuente: ' + sheetName + '\n' +
    'Total asignados: ' + totalAsignados + '\n\n' +
    'DISTRIBUCIÓN POR ASESOR:\n' + resumenAsesores + '\n' +
    '📊 DIM_CLIENTES: ' + newDimRows.length + ' nuevos | ' + dimUpdateRows.length + ' actualizados\n' +
    '📝 FACT_INTERACCIONES: ' + newFactRows.length + ' registros\n\n' +
    'Tiempo: ' + (t1 - t0) + 'ms',
    ui.ButtonSet.OK
  );
}

function migrarHojaActiva() {
  var t0 = new Date().getTime();
  var ss = getActiveSpreadsheetSafe_() || getMasterSpreadsheet_();
  var masterSs = getMasterSpreadsheet_();
  var sourceSheet = ss.getActiveSheet();
  var dimSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = masterSs.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var ui = SpreadsheetApp.getUi();
  
  var sheetName = sourceSheet.getName();
  
  // Evitar migrar hojas del sistema
  var systemSheets = getSystemSheetNames();
  
  if (systemSheets.indexOf(sheetName) !== -1) {
    ui.alert('Error', 'No puedes migrar una hoja del sistema. Selecciona una hoja de asesor.', ui.ButtonSet.OK);
    return;
  }
  
  var response = ui.alert(
    'Confirmar Migración v6.0',
    '¿Migrar los datos de "' + sheetName + '" al Data Warehouse?\n\n' +
    'FACT_INTERACCIONES: 16 columnas (Looker Ready)\n' +
    '✓ Mapeo de FECHA HOY + F. REGISTRO\n' +
    '✓ Normalización automática de FUENTE\n' +
    '✓ No creará duplicados\n\n' +
    '¿Continuar?',
    ui.ButtonSet.YES_NO
  );
  
  if (response !== ui.Button.YES) return;
  
  logDebug('[MIGRACION] Iniciando desde: ' + sheetName);
  
  // Detectar fila de headers (puede ser fila 1 o 2 si hay botón en fila 1)
  var headerRow = findHeaderRow(sourceSheet);
  var lastCol = sourceSheet.getLastColumn();
  var lastRow = sourceSheet.getLastRow();
  var headers = sourceSheet.getRange(headerRow, 1, 1, lastCol).getValues()[0];
  
  // Leer datos desde fila siguiente a headers hasta el final
  var dataStartRow = headerRow + 1;
  var data = [];
  if (lastRow >= dataStartRow) {
    data = sourceSheet.getRange(dataStartRow, 1, lastRow - dataStartRow + 1, lastCol).getValues();
  }
  
  // Mapeo flexible de columnas por nombre de header
  var colMap = {
    celular: -1, nombre: -1, proyecto: -1, fuente: -1,
    tipif: -1, comentario: -1, comentarioOPC: -1,
    estadoCivil: -1, ocupacion: -1, distrito: -1,
    fechaHoy: -1, fRegistro: -1, pareja: -1, puntoCaptacion: -1
  };
  
  for (var h = 0; h < headers.length; h++) {
    var headerUpper = String(headers[h]).toUpperCase().trim();
    
    if (headerUpper.indexOf('CELULAR') !== -1 || headerUpper.indexOf('TELEFONO') !== -1) colMap.celular = h;
    if (headerUpper.indexOf('NOMBRE') !== -1 && headerUpper.indexOf('OPC') === -1) colMap.nombre = h;
    if (headerUpper.indexOf('PROYECTO') !== -1) colMap.proyecto = h;
    if (headerUpper === 'FUENTE' || headerUpper.indexOf('FUENTE') !== -1) colMap.fuente = h;
    if (headerUpper.indexOf('TIPIF') !== -1) colMap.tipif = h;
    if (headerUpper.indexOf('COMENTARIO') !== -1 && headerUpper.indexOf('OPC') === -1 && headerUpper.indexOf('WSP') === -1 && colMap.comentario === -1) colMap.comentario = h;
    if (headerUpper.indexOf('COMENTARIO') !== -1 && (headerUpper.indexOf('OPC') !== -1 || headerUpper.indexOf('WSP') !== -1)) colMap.comentarioOPC = h;
    if (headerUpper.indexOf('CIVIL') !== -1) colMap.estadoCivil = h;
    if (headerUpper.indexOf('OCUPACION') !== -1) colMap.ocupacion = h;
    if (headerUpper.indexOf('DISTRITO') !== -1) colMap.distrito = h;
    if (headerUpper === 'FECHA HOY' || (headerUpper.indexOf('FECHA') !== -1 && headerUpper.indexOf('REGISTRO') === -1 && colMap.fechaHoy === -1)) colMap.fechaHoy = h;
    if (headerUpper.indexOf('REGISTRO') !== -1) colMap.fRegistro = h;
    if (headerUpper.indexOf('PAREJA') !== -1) colMap.pareja = h;
    if (headerUpper.indexOf('PUNTO') !== -1 || headerUpper.indexOf('CAPTACIÓN') !== -1 || headerUpper.indexOf('CAPTACION') !== -1) colMap.puntoCaptacion = h;
  }
  
  if (colMap.celular === -1) {
    // Mostrar diagnóstico para debug
    var headerList = [];
    for (var hd = 0; hd < headers.length; hd++) {
      headerList.push((hd + 1) + ':' + String(headers[hd]).substring(0, 15));
    }
    ui.alert('Error', 'No se encontró columna CELULAR.\n\nColumnas detectadas (' + headers.length + '):\n' + headerList.join(' | '), ui.ButtonSet.OK);
    return;
  }
  
  logDebug('[MIGRACION] Mapeo de columnas: ' + JSON.stringify(colMap));
  
  // Obtener números existentes en DIM
  var dimData = dimSheet.getDataRange().getValues();
  var existingPhones = new Set();
  
  for (var i = 1; i < dimData.length; i++) {
    var phone = normalizePhoneETL(dimData[i][1]);
    if (phone) existingPhones.add(phone);
  }
  
  logDebug('[MIGRACION] Números ya en DIM: ' + existingPhones.size);
  
  // Procesar filas
  var newClients = [];
  var newInteractions = [];
  var timestamp = new Date();
  var userEmail = Session.getActiveUser().getEmail() || sheetName;
  var skipped = 0;
  var migrated = 0;
  
  for (var row = 0; row < data.length; row++) {
    var rawPhone = data[row][colMap.celular];
    var cleanPhone = normalizePhoneETL(rawPhone);
    
    // Saltar: inválidos o duplicados (ya en DIM o ya procesados en este batch)
    if (!cleanPhone || cleanPhone.length < 9 || existingPhones.has(cleanPhone)) {
      skipped++;
      continue;
    }
    
    var clientId = 'CLI_' + Utilities.getUuid();
    
    // Extraer valores de la fila (todo en MAYÚSCULAS + normalizar distrito)
    var rowNombre = colMap.nombre !== -1 ? String(data[row][colMap.nombre] || '').toUpperCase() : '';
    var rowProyecto = colMap.proyecto !== -1 ? String(data[row][colMap.proyecto] || '').toUpperCase() : '';
    var rawFuente = colMap.fuente !== -1 ? (data[row][colMap.fuente] || '') : '';
    var rowTipif = colMap.tipif !== -1 ? String(data[row][colMap.tipif] || '').toUpperCase() : '';
    var rowComentario = colMap.comentario !== -1 ? String(data[row][colMap.comentario] || '') : '';
    var rowComentarioOPC = colMap.comentarioOPC !== -1 ? String(data[row][colMap.comentarioOPC] || '') : '';
    var rowEstadoCivil = colMap.estadoCivil !== -1 ? String(data[row][colMap.estadoCivil] || '').toUpperCase() : '';
    var rowOcupacion = colMap.ocupacion !== -1 ? String(data[row][colMap.ocupacion] || '').toUpperCase() : '';
    var rowDistrito = normalizarDistrito(colMap.distrito !== -1 ? (data[row][colMap.distrito] || '') : '');
    var rowPareja = colMap.pareja !== -1 ? String(data[row][colMap.pareja] || '').toUpperCase() : '';
    var rowFechaHoy = colMap.fechaHoy !== -1 ? data[row][colMap.fechaHoy] : '';
    var rowFRegistro = colMap.fRegistro !== -1 ? data[row][colMap.fRegistro] : '';
    
    // Parsear fuente
    var fuenteInfo = parseFuente(rawFuente, cleanPhone);
    
    // Determinar fechas
    var fechaInteraccion = rowFechaHoy || timestamp;
    var fechaRegistro = rowFRegistro || timestamp;
    
    // Crear cliente nuevo en DIM (16 cols)
    newClients.push([
      clientId,
      cleanPhone,
      rowNombre || 'Sin Nombre',
      '',  // email
      fuenteInfo.original || 'MIGRACION_' + sheetName,
      rowProyecto,
      fechaRegistro,
      userEmail,
      rowTipif,
      fechaInteraccion,
      fuenteInfo.normalizada,  // Col 11: FUENTE_NORMALIZADA
      fuenteInfo.nombreOPC,    // Col 12: NOMBRE_OPC
      rowEstadoCivil,
      rowOcupacion,
      rowPareja,
      rowDistrito
    ]);
    
    // Crear interacción en FACT solo si hay datos relevantes
    if (rowTipif || rowComentario.trim() || rowComentarioOPC.trim()) {
      // Combinar comentarios: gestión + OPC/WSP
      var fullComment = '';
      if (rowComentario.trim()) fullComment = rowComentario.trim();
      if (rowComentarioOPC.trim()) {
        fullComment = fullComment ? fullComment + ' | [OPC/WSP] ' + rowComentarioOPC.trim() : '[OPC/WSP] ' + rowComentarioOPC.trim();
      }
      
      newInteractions.push([
        'INT_' + Utilities.getUuid(),          // 1: ID_INTERACCION
        clientId,                              // 2: ID_CLIENTE
        cleanPhone,                            // 3: CELULAR
        rowNombre || 'Sin Nombre',             // 4: NOMBRE_CLIENTE
        sheetName,                             // 5: ASESOR_NOMBRE (nombre de la hoja)
        userEmail,                             // 6: ASESOR_EMAIL
        fechaInteraccion,                      // 7: FECHA_INTERACCION
        fechaRegistro,                         // 8: FECHA_REGISTRO_LEAD
        rowProyecto,                           // 9: PROYECTO
        fuenteInfo.original,                   // 10: FUENTE_ORIGINAL
        fuenteInfo.normalizada,                // 11: FUENTE_NORMALIZADA
        fuenteInfo.nombreOPC,                  // 12: NOMBRE_OPC
        'MIGRACION',                           // 13: TIPO_ACCION
        rowTipif || 'N/A',                     // 14: TIPIFICACION
        fullComment,                           // 15: COMENTARIO
        JSON.stringify({ origen: sheetName, fila: dataStartRow + row })  // 16: METADATA
      ]);
    }
    
    existingPhones.add(cleanPhone);
    migrated++;
  }
  
  // Escritura en batch
  if (newClients.length > 0) {
    var dimMigRow = dimSheet.getLastRow();
    ensureSheetCapacity(dimSheet, dimMigRow + newClients.length, 16);
    var dimMigRange = dimSheet.getRange(dimMigRow + 1, 1, newClients.length, 16);
    dimMigRange.clearDataValidations();  // DIM es hoja sistema
    dimMigRange.setValues(newClients);
  }
  
  if (newInteractions.length > 0) {
    var factMigRow = factSheet.getLastRow();
    ensureSheetCapacity(factSheet, factMigRow + newInteractions.length, 16);
    var factMigRange = factSheet.getRange(factMigRow + 1, 1, newInteractions.length, 16);
    factMigRange.clearDataValidations();  // FACT es hoja sistema
    factMigRange.setValues(newInteractions);
  }
  
  // Invalidar caché
  invalidateCache();
  
  var t1 = new Date().getTime();
  logDebug('[MIGRACION] ✅ Completado en ' + (t1 - t0) + 'ms');
  
  ui.alert(
    'Migración v6.0 Exitosa',
    'Hoja: ' + sheetName + '\n\n' +
    '✅ Nuevos clientes: ' + migrated + '\n' +
    '⚠️ Omitidos (duplicados/inválidos): ' + skipped + '\n' +
    '📝 Interacciones FACT: ' + newInteractions.length + '\n' +
    '📊 Columnas FACT: 16 (Looker Ready)\n\n' +
    'Tiempo: ' + (t1 - t0) + 'ms',
    ui.ButtonSet.OK
  );
}

/**
 * =====================================================================
 * SINCRONIZAR BASES DE ASESORES CON DIM_CLIENTES
 * Lee TODAS las hojas de asesores (ACTIVOS en CONFIG_MAESTROS), busca
 * celulares que no tienen asesor activo y los agrega o reasigna. Evita duplicados
 * activos al asignar/validar cuando se pegan datos manualmente a un asesor.
 * Ejecutar después de cada asignación manual (desde LEADS_DUPLICADOS, etc.).
 * =====================================================================
 */
function sincronizarBasesAsesoresConDIM() {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(120000);  // 2 min para múltiples hojas
  } catch (e) {
    SpreadsheetApp.getUi().alert('Sistema ocupado', 'Otro proceso está en curso. Intenta más tarde.', SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  try {
    sincronizarBasesAsesoresConDIMCore();
  } finally {
    lock.releaseLock();
  }
}

function sincronizarBasesAsesoresConDIMCore() {
  var t0 = new Date().getTime();
  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  var ui = SpreadsheetApp.getUi();

  if (!dimSheet || !factSheet) {
    ui.alert('Error', 'Faltan hojas DIM_CLIENTES o FACT_INTERACCIONES.', ui.ButtonSet.OK);
    return;
  }

  var advisors = getActiveAdvisors(ss);
  if (advisors.length === 0) {
    ui.alert('Error', 'No hay asesores ACTIVOS en CONFIG_MAESTROS.', ui.ButtonSet.OK);
    return;
  }

  var systemSheets = getSystemSheetNames();
  var crmOwnership = buildCrmLeadOwnershipMap(ss, dimSheet, factSheet);

  var newClients = [];
  var dimUpdates = [];
  var newInteractions = [];
  var processedPhones = new Set();
  var timestamp = new Date();
  var userEmail = Session.getActiveUser().getEmail() || 'SISTEMA';

  for (var a = 0; a < advisors.length; a++) {
    var advName = advisors[a].nombre;
    var advCtx = getAdvisorSheetContext_(advisors[a]);
    var advSheet = advCtx.sheet;
    if (!advSheet) {
      logDebug('[SYNC-DIM] Hoja no encontrada para asesor: ' + advName + ' en base ' + advCtx.baseCode);
      continue;
    }
    var advSheetName = advSheet.getName();
    var isSystemSheet = systemSheets.some(function(s) { return s.toUpperCase() === advSheetName.toUpperCase(); });
    if (isSystemSheet) continue;

    var headerRow = findHeaderRow(advSheet);
    var lastCol = Math.max(advSheet.getLastColumn(), 16);
    var lastRow = advSheet.getLastRow();
    if (lastRow < headerRow + 1) {
      logDebug('[SYNC-DIM] Sin datos en "' + advSheetName + '" (lastRow=' + lastRow + ', headerRow=' + headerRow + ')');
      continue;
    }

    var headers = advSheet.getRange(headerRow, 1, 1, lastCol).getValues()[0];
    var colMap = { celular: -1, nombre: -1, proyecto: -1, fuente: -1, fRegistro: -1,
                   estadoCivil: -1, ocupacion: -1, distrito: -1, pareja: -1, puntoCaptacion: -1 };

    for (var h = 0; h < headers.length; h++) {
      var hu = String(headers[h]).toUpperCase().trim();
      if (hu.indexOf('CELULAR') !== -1 || hu.indexOf('TELEFONO') !== -1) colMap.celular = h;
      if (hu.indexOf('NOMBRE') !== -1 && hu.indexOf('OPC') === -1) colMap.nombre = h;
      if (hu.indexOf('PROYECTO') !== -1) colMap.proyecto = h;
      if (hu === 'FUENTE' || (hu.indexOf('FUENTE') !== -1 && hu.indexOf('NORMALIZ') === -1)) colMap.fuente = h;
      if (hu.indexOf('REGISTRO') !== -1) colMap.fRegistro = h;
      if (hu.indexOf('CIVIL') !== -1) colMap.estadoCivil = h;
      if (hu.indexOf('PAREJA') !== -1) colMap.pareja = h;
      if (hu.indexOf('OCUPACION') !== -1) colMap.ocupacion = h;
      if (hu.indexOf('DISTRITO') !== -1) colMap.distrito = h;
      if (hu.indexOf('PUNTO') !== -1 || hu.indexOf('CAPTACI') !== -1) colMap.puntoCaptacion = h;
    }

    if (colMap.celular === -1) {
      logDebug('[SYNC-DIM] Columna CELULAR no encontrada en "' + advSheetName + '"');
      continue;
    }

    var dataStartRow = headerRow + 1;
    var dataEndRow = lastRow;
    if (dataEndRow < dataStartRow) continue;
    var dataRangeA1 = 'A' + dataStartRow + ':' + columnToLetter(lastCol) + dataEndRow;
    var data = advSheet.getRange(dataRangeA1).getValues();

    for (var r = 0; r < data.length; r++) {
      var rawPhone = data[r][colMap.celular];
      var cleanPhone = normalizePhoneETL(rawPhone);
      if (!cleanPhone || cleanPhone.length < 9 || !/^9\d{8}$/.test(cleanPhone)) continue;
      var crmStatus = crmOwnership[cleanPhone];
      if (processedPhones.has(cleanPhone) || crmPhoneHasActiveAdvisor_(crmStatus)) continue;

      processedPhones.add(cleanPhone);

      var clientId = (crmStatus && (crmStatus.dimClientId || crmStatus.factClientId)) || ('CLI_' + Utilities.getUuid());
      var rawFuente = colMap.fuente !== -1 ? (data[r][colMap.fuente] || '') : '';
      var fuenteInfo = parseFuente(rawFuente, cleanPhone);
      var fRegistro = colMap.fRegistro !== -1 ? data[r][colMap.fRegistro] : timestamp;
      var nombre = colMap.nombre !== -1 ? String(data[r][colMap.nombre] || '').toUpperCase() : '';
      var proyecto = colMap.proyecto !== -1 ? String(data[r][colMap.proyecto] || '').toUpperCase() : '';
      var estadoCivil = colMap.estadoCivil !== -1 ? String(data[r][colMap.estadoCivil] || '').toUpperCase() : '';
      var ocupacion = colMap.ocupacion !== -1 ? String(data[r][colMap.ocupacion] || '').toUpperCase() : '';
      var pareja = colMap.pareja !== -1 ? String(data[r][colMap.pareja] || '').toUpperCase() : '';
      var distrito = normalizarDistrito(colMap.distrito !== -1 ? (data[r][colMap.distrito] || '') : '') || '';

      if (crmStatus && crmStatus.dimRow) {
        var syncDimRow = crmStatus.dimRowData.slice ? crmStatus.dimRowData.slice() : crmStatus.dimRowData.map(function(x) { return x; });
        while (syncDimRow.length < 16) syncDimRow.push('');
        syncDimRow[0] = syncDimRow[0] || clientId;
        syncDimRow[1] = cleanPhone;
        if (nombre) syncDimRow[2] = nombre;
        if (fuenteInfo.original) syncDimRow[4] = fuenteInfo.original;
        if (proyecto) syncDimRow[5] = proyecto;
        syncDimRow[6] = syncDimRow[6] || fRegistro;
        syncDimRow[7] = advName;
        syncDimRow[8] = '';
        syncDimRow[9] = timestamp;
        syncDimRow[10] = fuenteInfo.normalizada || syncDimRow[10] || 'SIN_FUENTE';
        if (fuenteInfo.nombreOPC) syncDimRow[11] = fuenteInfo.nombreOPC;
        if (estadoCivil) syncDimRow[12] = estadoCivil;
        if (ocupacion) syncDimRow[13] = ocupacion;
        if (pareja) syncDimRow[14] = pareja;
        if (distrito) syncDimRow[15] = distrito;
        dimUpdates.push({ row: crmStatus.dimRow, data: syncDimRow });
        crmStatus.dimRowData = syncDimRow;
        crmStatus.dimClientId = clientId;
      } else {
        newClients.push([
          clientId,
          cleanPhone,
          nombre || 'Sin Nombre',
          '',
          fuenteInfo.original || 'ASIGNACION_MANUAL_' + advName,
          proyecto,
          fRegistro,
          advName,
          '',
          timestamp,
          fuenteInfo.normalizada || 'SIN_FUENTE',
          fuenteInfo.nombreOPC || '',
          estadoCivil,
          ocupacion,
          pareja,
          distrito
        ]);
      }

      newInteractions.push([
        'INT_' + Utilities.getUuid(),
        clientId,
        cleanPhone,
        nombre || 'Sin Nombre',
        advName,
        userEmail,
        timestamp,
        fRegistro,
        proyecto,
        fuenteInfo.original || 'ASIGNACION_MANUAL',
        fuenteInfo.normalizada || 'SIN_FUENTE',
        fuenteInfo.nombreOPC || '',
        'ASIGNACION_MANUAL',
        '',
        'Sincronizado desde base de asesor (pegado manual)',
        JSON.stringify({ origen: advName, sync: 'sincronizarBasesAsesoresConDIM' })
      ]);

      crmOwnership[cleanPhone] = crmOwnership[cleanPhone] || {};
      crmOwnership[cleanPhone].hasActiveAdvisor = true;
      crmOwnership[cleanPhone].activeAdvisor = advName;
    }
  }

  var dimUpdatesCount = 0;
  if (dimUpdates.length > 0) {
    var syncUpdatesByRow = {};
    for (var su = 0; su < dimUpdates.length; su++) {
      syncUpdatesByRow[dimUpdates[su].row] = dimUpdates[su].data;
    }
    var syncUpdateRows = Object.keys(syncUpdatesByRow).map(Number).sort(function(a, b) { return a - b; });
    dimUpdatesCount = syncUpdateRows.length;
    for (var sk = 0; sk < syncUpdateRows.length; sk++) {
      dimSheet.getRange(syncUpdateRows[sk], 1, 1, 16).setValues([syncUpdatesByRow[syncUpdateRows[sk]]]);
    }
  }

  if (newClients.length > 0) {
    var dimLastRow = dimSheet.getLastRow();
    ensureSheetCapacity(dimSheet, dimLastRow + newClients.length, 16);
    var dimRange = dimSheet.getRange(dimLastRow + 1, 1, newClients.length, 16);
    dimRange.clearDataValidations();
    dimRange.setValues(newClients);
  }

  if (newInteractions.length > 0) {
    var factLastRow = factSheet.getLastRow();
    ensureSheetCapacity(factSheet, factLastRow + newInteractions.length, 16);
    var factRange = factSheet.getRange(factLastRow + 1, 1, newInteractions.length, 16);
    factRange.clearDataValidations();
    factRange.setValues(newInteractions);
  }

  if (newClients.length > 0 || dimUpdates.length > 0 || newInteractions.length > 0) {
    invalidateCache();
  }

  var t1 = new Date().getTime();
  logDebug('[SYNC-DIM] Sincronizados nuevos=' + newClients.length + ' actualizados=' + dimUpdatesCount + ' fact=' + newInteractions.length + ' en ' + (t1 - t0) + 'ms');

  ui.alert(
    'Sincronización con DIM Completada',
    'Bases de asesores revisadas: ' + advisors.length + '\n\n' +
    '✅ Nuevos clientes agregados a DIM: ' + newClients.length + '\n' +
    '🔁 Clientes existentes reasignados en DIM: ' + dimUpdatesCount + '\n' +
    (newInteractions.length > 0 ? '📝 Registros en FACT: ' + newInteractions.length + '\n' : '') +
    '\n' +
    ((newClients.length + dimUpdatesCount) === 0
      ? 'No se encontraron celulares disponibles para sincronizar.\n' +
        'Si pegaste datos manualmente, verifica que la columna CELULAR tenga números válidos (9 dígitos, empieza con 9).'
      : 'La validación y asignación evitará asignar duplicados a estos asesores.') +
    '\n\nTiempo: ' + (t1 - t0) + 'ms',
    ui.ButtonSet.OK
  );
}

// ==========================================================================
// 6. CONFIGURACIÓN INICIAL DEL SISTEMA
// ==========================================================================

function setupSystem() {
  var ss = getMasterSpreadsheet_();
  var ui = SpreadsheetApp.getUi();
  
  var requiredSheets = [
    CONFIG_SYSTEM.SHEETS.DIM,
    CONFIG_SYSTEM.SHEETS.FACT,
    CONFIG_SYSTEM.SHEETS.CONFIG,
    CONFIG_SYSTEM.SHEETS.LOG,
    CONFIG_SYSTEM.SHEETS.VALIDACION,
    CONFIG_SYSTEM.SHEETS.OPC,
    CONFIG_SYSTEM.SHEETS.META,
    CONFIG_SYSTEM.SHEETS.GOOGLE_ADS,
    CONFIG_SYSTEM.SHEETS.DUPLICADOS
  ];
  
  var createdCount = 0;

  for (var idx = 0; idx < requiredSheets.length; idx++) {
    var sheetName = requiredSheets[idx];
    var sheet = ss.getSheetByName(sheetName);
    
    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
      createdCount++;
    }
    
    if (sheet.getLastRow() === 0) {
      if (sheetName === CONFIG_SYSTEM.SHEETS.DIM) {
        var headers = [
          "ID_CLIENTE", "CELULAR", "NOMBRE", "EMAIL", "ORIGEN", 
          "PROYECTO", "FECHA_REGISTRO", "ASESOR_ACTUAL", "ULTIMA_TIPIF", 
          "FECHA_ULT_GESTION", "FUENTE_NORMALIZADA", "NOMBRE_OPC", 
          "ESTADO_CIVIL", "OCUPACION", "TIENE_PAREJA", "DISTRITO"
        ];
        sheet.appendRow(headers);
        formatHeader(sheet, headers.length, "#1e293b"); 
        sheet.setColumnWidth(1, 220); 
        sheet.setColumnWidth(2, 110);
        sheet.setColumnWidth(3, 200);
        
      } else if (sheetName === CONFIG_SYSTEM.SHEETS.FACT) {
        var headers = [
          "ID_INTERACCION", "ID_CLIENTE", "CELULAR", "NOMBRE_CLIENTE",
          "ASESOR_NOMBRE", "ASESOR_EMAIL", "FECHA_INTERACCION", "FECHA_REGISTRO_LEAD",
          "PROYECTO", "FUENTE_ORIGINAL", "FUENTE_NORMALIZADA", "NOMBRE_OPC",
          "TIPO_ACCION", "TIPIFICACION", "COMENTARIO", "METADATA"
        ];
        sheet.appendRow(headers);
        formatHeader(sheet, headers.length, "#059669"); 
        sheet.setColumnWidth(3, 110);
        sheet.setColumnWidth(4, 200);
        sheet.setColumnWidth(5, 150);
        sheet.setColumnWidth(10, 160);
        sheet.setColumnWidth(11, 160);
        sheet.setColumnWidth(15, 350); 
        
      } else if (sheetName === CONFIG_SYSTEM.SHEETS.VALIDACION) {
        var headers = ["CELULAR_INGRESO", "ESTADO", "DETALLE", "CELULAR_LIMPIO"];
        sheet.appendRow(headers);
        formatHeader(sheet, headers.length, "#ef4444");
        sheet.setColumnWidth(1, 150);
        sheet.setColumnWidth(2, 100);
        sheet.setColumnWidth(3, 300);
        sheet.setColumnWidth(4, 120);

      } else if (sheetName === CONFIG_SYSTEM.SHEETS.OPC || 
                 sheetName === CONFIG_SYSTEM.SHEETS.META || 
                 sheetName === CONFIG_SYSTEM.SHEETS.GOOGLE_ADS) {
        // Misma estructura que hojas de asesores para pegado directo
        var headers = [
          "IT", "NUM", "FECHA HOY", "PROYECTO", "FUENTE", 
          "F. DE REGISTRO CLIENTE", "NOMBRES Y APELLIDOS", "CELULAR", 
          "E. CIVIL", "PAREJA", "OCUPACION", "DISTRITO", 
          "TIPIF", "COMENTARIO", "COMENTARIO OPC / WSP", "PUNTO DE CAPTACIÓN"
        ];
        sheet.appendRow(headers);
        var color = sheetName === CONFIG_SYSTEM.SHEETS.OPC ? "#f97316" : 
                    sheetName === CONFIG_SYSTEM.SHEETS.META ? "#3b82f6" : "#8b5cf6";
        formatHeader(sheet, headers.length, color);
        
      } else if (sheetName === CONFIG_SYSTEM.SHEETS.DUPLICADOS) {
        var dupHeaders = [
          "IT", "NUM", "FECHA HOY", "PROYECTO", "FUENTE", "F. DE REGISTRO CLIENTE",
          "NOMBRES Y APELLIDOS", "CELULAR", "E. CIVIL", "PAREJA", "OCUPACION", "DISTRITO",
          "TIPIF", "COMENTARIO", "COMENTARIO OPC / WSP", "PUNTO DE CAPTACIÓN",
          "ESTADO_VALIDACION", "ULTIMO_ASESOR", "FECHA_ULTIMA_GESTION"
        ];
        sheet.appendRow(dupHeaders);
        formatHeader(sheet, dupHeaders.length, "#f59e0b");
        
      } else if (sheetName === CONFIG_SYSTEM.SHEETS.CONFIG) {
        var headers = ["TIPO", "VALOR", "DESCRIPCION", "ESTADO"];
        sheet.appendRow(headers);
        formatHeader(sheet, headers.length, "#475569"); 
        
        var seedData = [
          // Tipificaciones
          ["TIPIFICACION", "NC", "No Contesta", "ACTIVO"],
          ["TIPIFICACION", "VENTA", "Cierre / Separación", "ACTIVO"],
          ["TIPIFICACION", "CITA", "Cita Agendada", "ACTIVO"],
          ["TIPIFICACION", "NI", "No Interesado", "ACTIVO"],
          ["TIPIFICACION", "VLL", "Volver a Llamar", "ACTIVO"],
          ["TIPIFICACION", "SG", "Seguimiento", "ACTIVO"],
          ["TIPIFICACION", "BZ", "Buzón", "ACTIVO"],
          ["TIPIFICACION", "AP", "Apagado", "ACTIVO"],
          ["TIPIFICACION", "DF", "Dato Falso", "ACTIVO"],
          // Estado Civil
          ["ESTADO_CIVIL", "SOLTER@", "-", "ACTIVO"],
          ["ESTADO_CIVIL", "CASAD@", "-", "ACTIVO"],
          ["ESTADO_CIVIL", "CONVIVIENTE", "-", "ACTIVO"],
          ["ESTADO_CIVIL", "SEPARAD@", "-", "ACTIVO"],
          // Ocupación
          ["OCUPACION", "DEPENDIENTE", "-", "ACTIVO"],
          ["OCUPACION", "INDEPENDIENTE", "-", "ACTIVO"],
          // Ubicación
          ["UBICACION", "LOS OLIVOS", "-", "ACTIVO"],
          ["UBICACION", "SMP", "-", "ACTIVO"],
          ["UBICACION", "CARABAYLLO", "-", "ACTIVO"],
          ["UBICACION", "COMAS", "-", "ACTIVO"],
          ["UBICACION", "INDEPENDENCIA", "-", "ACTIVO"],
          ["UBICACION", "PUENTE PIEDRA", "-", "ACTIVO"],
          ["UBICACION", "VENTANILLA", "-", "ACTIVO"],
          ["UBICACION", "VES", "-", "ACTIVO"],
          // Asesores activos (VALOR = nombre exacto de la hoja del asesor)
          ["ASESOR", "LEONEL P.", "Nombre de hoja", "ACTIVO"],
          ["ASESOR", "MARILYN P.", "Nombre de hoja", "ACTIVO"],
          ["ASESOR", "DEBORA R.", "Nombre de hoja", "ACTIVO"],
          ["ASESOR", "JACKY R.", "Nombre de hoja", "ACTIVO"],
          ["ASESOR", "JUDITH R.", "Nombre de hoja", "ACTIVO"],
          ["ASESOR", "ANDREA A.", "Nombre de hoja", "ACTIVO"],
          // Fuentes válidas
          ["FUENTE", "OPC", "Captación presencial", "ACTIVO"],
          ["FUENTE", "META", "Facebook / Instagram / WSP / FORM", "ACTIVO"],
          ["FUENTE", "GOOGLE_ADS", "Google Ads", "ACTIVO"],
          ["FUENTE", "REFERIDO", "Referido por cliente", "ACTIVO"]
        ];
        sheet.getRange(2, 1, seedData.length, 4).setValues(seedData);
        
      } else if (sheetName === CONFIG_SYSTEM.SHEETS.LOG) {
        var headers = ["TIMESTAMP", "USUARIO", "ACCION", "TELEFONO", "TIPIFICACION", "STATUS"];
        sheet.appendRow(headers);
        formatHeader(sheet, headers.length, "#6366f1");
      }
    }
  }

  ui.alert(
    'Sistema Inicializado',
    '✅ v6.0\n\n' +
    'Hojas verificadas/creadas: ' + createdCount + '\n\n' +
    'MÓDULOS v6.0:\n' +
    '• FACT_INTERACCIONES: 16 columnas (Looker Ready)\n' +
    '• Normalización de FUENTE automática\n' +
    '• Asesores en CONFIG_MAESTROS (escalable)\n' +
    '• Asignación Round-Robin multi-nivel\n' +
    '• Sync en tiempo real sidebar → DW',
    ui.ButtonSet.OK
  );
}

// ==========================================================================
// 7. HELPERS Y UTILIDADES
// ==========================================================================

/** Convierte índice de columna (1-based) a letra A1: 1=A, 2=B, ..., 26=Z, 27=AA */
function columnToLetter(col) {
  if (!col || col < 1) return 'A';
  var letter = '';
  while (col > 0) {
    var mod = (col - 1) % 26;
    letter = String.fromCharCode(65 + mod) + letter;
    col = Math.floor((col - 1) / 26);
  }
  return letter || 'A';
}

function normalizePhoneETL(phone) {
  if (!phone) return "";
  var p = String(phone).replace(/\D/g, '');
  if (p.length === 11 && p.substring(0, 2) === "51") {
    p = p.substring(2);
  }
  // Canonónico 9 dígitos: quitar 0 inicial si viene 0966817363 (hoja asesor vs FACT)
  if (p.length === 10 && p.charAt(0) === '0') {
    p = p.substring(1);
  }
  return p;
}

/**
 * Normaliza comentario SOLO para comparar (hoja vs FACT). No modifica lo que se guarda.
 * Ver CRUCE_DEBORA_FACT_REPORTE.txt: 69 "diferencias" eran solo formato (hoja "8/1" vs FACT "08/01/2026").
 * Reglas:
 * - Date o string "Tue Jan 13 2026 GMT...": → dd/MM/yyyy.
 * - String que es solo una fecha (d/m, dd/m, d/mm, dd/mm, con o sin año): → dd/MM/yyyy para que coincidan.
 *   Ej.: "8/1","7/1","16/1","27/2","13/10","13/1","23/1","21/1","24/1","27/1","11/2" → "08/01/2026", "27/02/2026", etc.
 * - Resto: trim y colapsar espacios (sin tocar el texto).
 */
function normalizarComentarioParaComparacion(val) {
  if (val == null || val === '') return '';
  if (val instanceof Date) return Utilities.formatDate(val, Session.getScriptTimeZone(), 'dd/MM/yyyy');
  var s = String(val).replace(/\r\n/g, '\n').replace(/\r/g, '\n').replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (!s) return '';
  // Solo fecha: d/m, dd/m, d/mm, dd/mm, con o sin año (2 o 4 dígitos) — mismo canon que el cruce DEBORA/FACT
  var m = s.match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?$/);
  if (m) {
    var d = parseInt(m[1], 10);
    var mes = parseInt(m[2], 10);
    if (mes >= 1 && mes <= 12 && d >= 1 && d <= 31) {
      var y = m[3] ? (m[3].length === 2 ? (parseInt(m[3], 10) < 50 ? '20' + m[3] : '19' + m[3]) : m[3]) : (new Date().getFullYear()).toString();
      return (d < 10 ? '0' : '') + d + '/' + (mes < 10 ? '0' : '') + mes + '/' + y;
    }
  }
  // Fecha larga tipo "Tue Jan 13 2026 GMT..."
  var d = new Date(s);
  if (!isNaN(d.getTime())) return Utilities.formatDate(d, Session.getScriptTimeZone(), 'dd/MM/yyyy');
  return s;
}

/**
 * Valor a guardar en columna COMENTARIO de FACT. Solo texto; no convertir cadenas a fecha.
 * - Si la celda devuelve un Date (formato fecha en hoja): se escribe dd/MM/yyyy para no guardar "Wed Jan 07 2026 GMT-0500...".
 * - Todo lo demás (07/02/25, 07/01, etc.) se guarda como texto tal cual (recortado, espacios colapsados).
 */
function comentarioParaFACT(val) {
  if (val == null || val === '') return '';
  if (val instanceof Date) return Utilities.formatDate(val, Session.getScriptTimeZone(), 'dd/MM/yyyy');
  return String(val).replace(/\r\n/g, '\n').replace(/\r/g, '\n').replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
}

function normalizarComentarioOperativo_(val) {
  var norm = normalizarComentarioParaComparacion(val);
  return esComentarioArtefactoFechaVacia_(val, norm) ? '' : norm;
}

function comentarioParaFACTOperativo_(val) {
  var norm = normalizarComentarioOperativo_(val);
  if (!norm) return '';
  return comentarioParaFACT(val);
}

function esComentarioArtefactoFechaVacia_(val, norm) {
  var n = String(norm || '').trim();
  if (!n) return false;
  if (n === '30/12/1899' || n === '31/12/1899' || n === '01/01/1900') return true;
  if (val instanceof Date && val.getFullYear && val.getFullYear() <= 1901) return true;
  return false;
}

function buildFirmaEdicionManual_(compKey, tipif, comentarioNorm) {
  return [
    compKey,
    normalizarTipifParaComparacion(tipif),
    String(comentarioNorm || '').replace(/\s+/g, ' ').trim()
  ].join('|');
}

/**
 * Parsea FECHA_INTERACCION de FACT a timestamp (ms). Evita que new Date("DD/MM/YYYY") se interprete como MM/DD/YYYY (locale US).
 * - Si es Date: getTime().
 * - Si es string DD/MM/YYYY o DD/MM/YYYY HH:MM: parseo explícito. Fallback a new Date() si no coincide.
 * Devuelve número (ms) o NaN si no se pudo parsear.
 */
function parseFechaFactToMs(fecha) {
  if (fecha == null) return NaN;
  if (fecha instanceof Date) return isNaN(fecha.getTime()) ? NaN : fecha.getTime();
  var s = String(fecha).trim();
  var match = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (match) {
    var dia = parseInt(match[1], 10);
    var mes = parseInt(match[2], 10) - 1;
    var anio = parseInt(match[3], 10);
    var hh = 0, mm = 0, ss = 0;
    var timeMatch = s.match(/\s(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?/);
    if (timeMatch) {
      hh = parseInt(timeMatch[1], 10);
      mm = parseInt(timeMatch[2], 10);
      if (timeMatch[3]) ss = parseInt(timeMatch[3], 10);
    }
    var d = new Date(anio, mes, dia, hh, mm, ss);
    return isNaN(d.getTime()) ? NaN : d.getTime();
  }
  var fallback = new Date(fecha).getTime();
  return isNaN(fallback) ? NaN : fallback;
}

/** Clave asesor para comparar hoja vs FACT: mismo string en ambos lados (sin tildes, mayúsculas, espacios colapsados). */
function normalizarAsesorParaClave(asesor) {
  if (!asesor) return '';
  return quitarTildes(String(asesor).replace(/\s+/g, ' ').trim().toUpperCase());
}

/** Tipificación normalizada para comparación (evita falsos cambios por espacios/tildes). */
function normalizarTipifParaComparacion(val) {
  if (!val) return '';
  return quitarTildes(String(val).replace(/\s+/g, ' ').trim().toUpperCase());
}

/**
 * Normaliza el nombre de un OPC contra la lista canónica.
 * Quita tildes y compara prefijo para manejar apellidos extras.
 * "ANA MARÍA GUZMÁN" → "ANA MARIA", "DAYANNA PEREZ" → "DAYANNA"
 */
function normalizarNombreOPC(rawName) {
  if (!rawName) return '';
  
  // Lista canónica de nombres OPC (ordenada de más largo a más corto)
  var OPC_NOMBRES = [
    'ROOS MERY',   // compuesto: va primero
    'ANA MARIA',   // compuesto: va antes que "ANA"
    'YOSELIN',
    'JOSELYN',
    'MARIBEL',
    'MILAGROS',
    'JANNETHE',
    'DEYSI',
    'DAYANNA',
    'ALEXANDRA'
  ];
  
  // Alias: nombre corto → nombre canónico
  // Si "ANA" aparece sola, siempre es ANA MARIA
  var ALIAS = {
    'ANA': 'ANA MARIA'
  };
  
  // Quitar tildes para comparación
  var clean = quitarTildes(rawName.toUpperCase().trim());
  
  // Primero: revisar si es un alias exacto (nombre solo, sin apellido)
  if (ALIAS[clean]) {
    return ALIAS[clean];
  }
  
  // Segundo: buscar si empieza con un nombre canónico
  for (var i = 0; i < OPC_NOMBRES.length; i++) {
    var nombre = OPC_NOMBRES[i];
    if (clean.indexOf(nombre) === 0) {
      var nextChar = clean.charAt(nombre.length);
      if (nextChar === '' || nextChar === ' ') {
        return nombre;
      }
    }
  }
  
  // Tercero: revisar alias como prefijo (ANA + apellido = ANA MARIA)
  var aliasKeys = Object.keys(ALIAS);
  for (var a = 0; a < aliasKeys.length; a++) {
    var aliasKey = aliasKeys[a];
    if (clean.indexOf(aliasKey) === 0) {
      var nc = clean.charAt(aliasKey.length);
      if (nc === '' || nc === ' ') {
        return ALIAS[aliasKey];
      }
    }
  }
  
  // Si no coincide, devolver solo el primer nombre
  var palabras = rawName.toUpperCase().trim().split(/\s+/);
  return palabras[0];
}

/**
 * Elimina tildes y diacríticos de un string.
 */
function quitarTildes(str) {
  var map = {
    'Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U',
    'á':'a','é':'e','í':'i','ó':'o','ú':'u',
    'Ñ':'N','ñ':'n','Ü':'U','ü':'u'
  };
  return str.replace(/[ÁÉÍÓÚáéíóúÑñÜü]/g, function(c) { return map[c] || c; });
}

/**
 * Normaliza el nombre de un distrito a su forma canónica.
 * Maneja tildes, variantes, abreviaciones y errores ortográficos.
 * @param {string} rawDistrito - Nombre crudo del distrito
 * @return {string} Nombre canónico en MAYÚSCULAS
 */
function normalizarDistrito(rawDistrito) {
  if (!rawDistrito || String(rawDistrito).trim() === '') return '';
  
  var clean = quitarTildes(String(rawDistrito).toUpperCase().trim());
  
  // Mapa de alias → nombre canónico
  var ALIAS = {
    'LURIN':        'LURIN',
    'LURÍN':        'LURIN',
    'SMP':          'SMP',
    'S.M.P':        'SMP',
    'S.M.P.':       'SMP',
    'SAN MARTIN':   'SMP',
    'SAN MARTÍN':   'SMP',
    'SAN MARTIN DE PORRES': 'SMP',
    'SJL':          'SJL',
    'S.J.L':        'SJL',
    'S.J.L.':       'SJL',
    'SAN JUAN DE LURIGANCHO': 'SJL',
    'SJM':          'SJM',
    'S.J.M':        'SJM',
    'S.J.M.':       'SJM',
    'SAN JUAN DE MIRAFLORES': 'SJM',
    'VMT':          'VILLA MARIA DEL TRIUNFO', // No está en la lista de error pero mantenemos por si acaso
    'V.M.T':        'VILLA MARIA DEL TRIUNFO',
    'V.M.T.':       'VILLA MARIA DEL TRIUNFO',
    'VILLA MARIA':  'VILLA MARIA DEL TRIUNFO',
    'VILLA MARIA DEL TRIUNFO': 'VILLA MARIA DEL TRIUNFO',
    'VES':          'VILLA EL SALVADOR', // No está en lista error
    'V.E.S':        'VILLA EL SALVADOR',
    'V.E.S.':       'VILLA EL SALVADOR',
    'VILLA EL SALVADOR': 'VILLA EL SALVADOR',
    'CHORRILOS':    'CHORRILOS',
    'CHORRILLOS':   'CHORRILOS', // Mapear al valor del sheet (con typo)
    'LOS OLIVOS':   'OLIVOS',
    'OLIVOS':       'OLIVOS',
    'CARABAYLLO':   'CARABAYLLO',
    'COMAS':        'COMAS',
    'VENTANILLA':   'VENTANILLA',
    'CALLAO':       'CALLAO',
    'ATE':          'ATE',
    'ATE VITARTE':  'ATE',
    'SANTA ANITA':  'SANTA ANITA',
    'PUENTE PIEDRA': 'PUENTE PIEDRA',
    'LA VICTORIA':  'LA VICTORIA',
    'LINCE':        'LINCE',
    'MIRAFLORES':   'MIRAFLORES',
    'ANCASH':       'ANCASH',
    'JUNIN':        'JUNIN',
    'JUNÍN':        'JUNIN',
    'LA LIBERTAD':  'LA LIBERTAD',
    'INDEPENDENCIA': 'INDEPENDENCIA',
    'CERCADO':      'CERCADO DE LIMA',
    'CERCADO DE LIMA': 'CERCADO DE LIMA',
    'LIMA':         'CERCADO DE LIMA',
    'LIMA CERCADO': 'CERCADO DE LIMA',
    'JESUS MARIA':  'JESUS MARIA',
    'JESÚS MARÍA':  'JESUS MARIA',
    'SAN MIGUEL':   'SAN MIGUEL',
    'CHOSICA':      'CHOSICA',
    'BARRANCA':     'BARRANCA',
    'MANCHAY':      'MANCHAY',
    'HUARAZ':       'HUARAZ',
    'ANCON':        'ANCON',
    'ANCÓN':        'ANCON',
    'RIMAC':        'RIMAC',
    'RÍMAC':        'RIMAC',
    'SURQUILLO':    'SURQUILLO',
    'LA MERCED':    'LA MERCED',
    'HUANUCO':      'HUANUCO',
    'HUÁNUCO':      'HUANUCO',
    'EL AGUSTINO':  'EL AGUSTINO',
    'BREÑA':        'BRENA',
    'BRENA':        'BRENA',
    'SAN BORJA':    'SAN BORJA',
    'SURCO':        'SANTIAGO DE SURCO',
    'SANTIAGO DE SURCO': 'SANTIAGO DE SURCO',
    'MAGDALENA':    'MAGDALENA',
    'PUEBLO LIBRE':  'PUEBLO LIBRE',
    'SAN ISIDRO':   'SAN ISIDRO',
    'BARRANCO':     'BARRANCO',
    'LA MOLINA':    'LA MOLINA',
    'PACHACAMAC':   'PACHACAMAC',
    'CIENEGUILLA':  'CIENEGUILLA',
    'CHACLACAYO':   'CHACLACAYO',
    'PUCUSANA':     'PUCUSANA',
    'PUNTA HERMOSA': 'PUNTA HERMOSA',
    'SAN BARTOLO':  'SAN BARTOLO',
    'SANTA ROSA':   'SANTA ROSA',
    'MI PERU':      'MI PERU',
    'LA PERLA':     'LA PERLA',
    'BELLAVISTA':   'BELLAVISTA',
    'CARMEN DE LA LEGUA': 'CARMEN DE LA LEGUA'
  };
  
  // Buscar en alias (sin tildes)
  if (ALIAS[clean]) {
    return ALIAS[clean];
  }
  
  // Si no está en el alias, devolver limpio y en mayúsculas
  return clean;
}

/**
 * Normaliza la fuente raw a su canal principal + extrae NOMBRE_OPC.
 * Si es REASIGNADO/REMARCADO, busca la fuente original en DIM_CLIENTES.
 * 
 * @param {string} rawFuente - Valor crudo de la columna FUENTE
 * @param {string} celular - Número limpio (para buscar fuente original)
 * @return {{ original: string, normalizada: string, nombreOPC: string }}
 */
function parseFuente(rawFuente, celular) {
  var result = { original: '', normalizada: '', nombreOPC: '' };
  if (!rawFuente) return result;
  
  var fuente = String(rawFuente).trim().toUpperCase();
  result.original = String(rawFuente).trim();
  
  // 1. OPC + NOMBRE → FUENTE=OPC, extraer y normalizar nombre del captador
  if (fuente.indexOf('OPC') === 0) {
    result.normalizada = 'OPC';
    var parts = fuente.split(/\s+/);
    if (parts.length > 1) {
      var rawName = parts.slice(1).join(' ');
      result.nombreOPC = normalizarNombreOPC(rawName);
    }
    return result;
  }
  
  // 2. META / WSP / FORM → todo es META
  if (fuente === 'META' || fuente === 'WSP' || fuente === 'FORM') {
    result.normalizada = 'META';
    return result;
  }
  
  // 3. GOOGLE ADS / GOOGLE_ADS
  if (fuente === 'GOOGLE ADS' || fuente === 'GOOGLE_ADS') {
    result.normalizada = 'GOOGLE_ADS';
    return result;
  }
  
  // 4. REFERIDO
  if (fuente === 'REFERIDO') {
    result.normalizada = 'REFERIDO';
    return result;
  }
  
  // 5. REASIGNADO / REMARCADO → buscar fuente original del primer contacto
  if (fuente === 'REASIGNADO' || fuente === 'REMARCADO') {
    if (celular) {
      try {
        var ss = getMasterSpreadsheet_();
        var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
        if (dimSheet) {
          var phoneCol = dimSheet.getRange('B:B');
          var finder = phoneCol.createTextFinder(celular).matchEntireCell(true);
          var found = finder.findNext();
          if (found) {
            var rowNum = found.getRow();
            // Col 11 = FUENTE_NORMALIZADA en DIM_CLIENTES
            var originalFuente = dimSheet.getRange(rowNum, 11).getValue();
            if (originalFuente && String(originalFuente).trim() !== '') {
              result.normalizada = String(originalFuente).trim();
              // También recuperar NOMBRE_OPC del primer registro
              var originalOPC = dimSheet.getRange(rowNum, 12).getValue();
              if (originalOPC) result.nombreOPC = String(originalOPC).trim();
              return result;
            }
          }
        }
      } catch (e) {
        logDebug('[PARSE_FUENTE] Error buscando fuente original: ' + e.message);
      }
    }
    // Si no se encuentra, dejar como REASIGNADO/REMARCADO
    result.normalizada = fuente;
    return result;
  }
  
  // 6. Default: usar tal cual
  result.normalizada = fuente;
  return result;
}

/**
 * Construye un lookup de asesores por estado desde CONFIG_MAESTROS.
 * Todo asesor que no este ACTIVO se considera no operativo para validaciones.
 */
function getAdvisorStatusLookup(ss) {
  ss = ss || getMasterSpreadsheet_();
  var configSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.CONFIG);
  var lookup = { active: {}, inactive: {}, known: {}, rawByKey: {} };
  if (!configSheet) return lookup;

  var data = configSheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var tipo = String(data[i][0] || '').trim().toUpperCase();
    if (tipo !== 'ASESOR') continue;

    var valor = String(data[i][1] || '').trim();
    if (!valor) continue;

    var key = normalizarAsesorParaClave(valor);
    var estado = String(data[i][3] || '').trim().toUpperCase();
    lookup.known[key] = true;
    lookup.rawByKey[key] = valor;

    if (estado === 'ACTIVO') {
      lookup.active[key] = true;
    } else {
      lookup.inactive[key] = true;
    }
  }

  return lookup;
}

function isAdvisorActiveForValidation_(asesor, advisorLookup) {
  var key = normalizarAsesorParaClave(asesor);
  return !!(key && advisorLookup && advisorLookup.active[key]);
}

function getOrCreateCrmOwnershipStatus_(ownership, phone) {
  if (!ownership[phone]) {
    ownership[phone] = {
      celular: phone,
      existsInDim: false,
      existsInFact: false,
      hasActiveAdvisor: false,
      activeAdvisor: '',
      activeSource: '',
      activeNombre: '',
      activeFecha: null,
      activeFechaMs: NaN,
      activeTipif: '',
      dimClientId: '',
      dimRow: 0,
      dimRowData: null,
      dimAsesor: '',
      dimNombre: '',
      dimFechaRegistro: '',
      dimFechaUltima: '',
      dimFechaMs: NaN,
      dimTipif: '',
      dimActive: false,
      factClientId: '',
      lastAnyAsesor: '',
      lastAnyNombre: '',
      lastAnyFecha: null,
      lastAnyFechaMs: NaN,
      lastAnyTipif: '',
      lastAnyComentario: ''
    };
  }
  return ownership[phone];
}

function shouldReplaceCrmDateReference_(currentMs, nextMs) {
  if (isNaN(currentMs)) return true;
  if (isNaN(nextMs)) return false;
  return nextMs >= currentMs;
}

function markCrmActiveAdvisor_(status, asesor, source, fecha, fechaMs, tipif, nombre) {
  if (!status.hasActiveAdvisor || shouldReplaceCrmDateReference_(status.activeFechaMs, fechaMs)) {
    status.hasActiveAdvisor = true;
    status.activeAdvisor = asesor || '';
    status.activeSource = source || '';
    status.activeNombre = nombre || '';
    status.activeFecha = fecha || null;
    status.activeFechaMs = fechaMs;
    status.activeTipif = tipif || '';
  }
}

/**
 * Mapa celular -> estado CRM.
 * Regla: solo bloquea como duplicado si DIM o FACT tiene cualquier asesor ACTIVO.
 * Si el celular solo existe con asesores INACTIVOS/no reconocidos, queda reasignable.
 */
function buildCrmLeadOwnershipMap(ss, dimSheet, factSheet, advisorLookup) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  advisorLookup = advisorLookup || getAdvisorStatusLookup(ss);
  dimSheet = dimSheet || ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  factSheet = factSheet || ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);

  var ownership = {};

  if (dimSheet && dimSheet.getLastRow() >= 2) {
    var dimLastRow = dimSheet.getLastRow();
    var dimData = dimSheet.getRange(2, 1, dimLastRow - 1, 16).getValues();
    for (var d = 0; d < dimData.length; d++) {
      var dimRow = dimData[d];
      var dimPhone = normalizePhoneETL(dimRow[1]);
      if (!dimPhone || dimPhone.length < 9) continue;

      var status = getOrCreateCrmOwnershipStatus_(ownership, dimPhone);
      var asesorDim = String(dimRow[7] || '').trim();
      var dimFecha = dimRow[9] || dimRow[6] || '';
      var dimFechaMs = parseFechaFactToMs(dimFecha);
      var dimIsActive = isAdvisorActiveForValidation_(asesorDim, advisorLookup);
      var replaceDim = !status.dimRow ||
        (dimIsActive && !status.dimActive) ||
        (dimIsActive === status.dimActive && shouldReplaceCrmDateReference_(status.dimFechaMs, dimFechaMs));

      status.existsInDim = true;
      if (replaceDim) {
        status.dimClientId = dimRow[0] || '';
        status.dimRow = d + 2;
        status.dimRowData = dimRow.slice ? dimRow.slice() : dimRow.map(function(x) { return x; });
        status.dimAsesor = asesorDim;
        status.dimNombre = dimRow[2] || '';
        status.dimFechaRegistro = dimRow[6] || '';
        status.dimFechaUltima = dimFecha;
        status.dimFechaMs = dimFechaMs;
        status.dimTipif = dimRow[8] || '';
        status.dimActive = dimIsActive;
      }

      if (dimIsActive) {
        markCrmActiveAdvisor_(status, asesorDim, 'DIM_CLIENTES', dimFecha, dimFechaMs, dimRow[8] || '', dimRow[2] || '');
      }
    }
  }

  if (factSheet && factSheet.getLastRow() >= 2) {
    var factLastRow = factSheet.getLastRow();
    var factData = factSheet.getRange(2, 1, factLastRow - 1, 16).getValues();
    for (var f = 0; f < factData.length; f++) {
      var factRow = factData[f];
      var factPhone = normalizePhoneETL(factRow[2]);
      if (!factPhone || factPhone.length < 9) continue;

      var st = getOrCreateCrmOwnershipStatus_(ownership, factPhone);
      var asesorFact = String(factRow[4] || '').trim();
      var factFecha = factRow[6] || '';
      var factFechaMs = parseFechaFactToMs(factFecha);
      var factTipif = factRow[13] || '';

      st.existsInFact = true;
      if (!st.factClientId && factRow[1]) st.factClientId = factRow[1];

      if (shouldReplaceCrmDateReference_(st.lastAnyFechaMs, factFechaMs)) {
        st.lastAnyAsesor = asesorFact;
        st.lastAnyNombre = factRow[3] || '';
        st.lastAnyFecha = factFecha;
        st.lastAnyFechaMs = factFechaMs;
        st.lastAnyTipif = factTipif;
        st.lastAnyComentario = factRow[14] || '';
        if (!st.factClientId && factRow[1]) st.factClientId = factRow[1];
      }

      if (isAdvisorActiveForValidation_(asesorFact, advisorLookup)) {
        markCrmActiveAdvisor_(st, asesorFact, 'FACT_INTERACCIONES', factFecha, factFechaMs, factTipif, factRow[3] || '');
      }
    }
  }

  return ownership;
}

function crmPhoneHasActiveAdvisor_(crmStatus) {
  return !!(crmStatus && crmStatus.hasActiveAdvisor);
}

function buildCrmDuplicateDetail_(crmStatus) {
  var asesor = crmStatus.activeAdvisor || crmStatus.dimAsesor || crmStatus.lastAnyAsesor || '';
  var nombre = crmStatus.activeNombre || crmStatus.dimNombre || crmStatus.lastAnyNombre || '';
  var fecha = crmStatus.activeFecha || crmStatus.dimFechaUltima || crmStatus.lastAnyFecha || '';
  var tipif = crmStatus.activeTipif || crmStatus.dimTipif || crmStatus.lastAnyTipif || '';
  var fechaStr = fecha ? formatDate(fecha) : '';
  var detalle = (crmStatus.activeSource === 'FACT_INTERACCIONES' ? 'DUPLICADO_FACT: ' : 'DUPLICADO_DIM: ') +
    nombre + ' (' + asesor + ')';
  if (fechaStr) detalle += ' | Ult: ' + fechaStr;
  if (tipif) detalle += ' | Tipif: ' + String(tipif).trim();
  return detalle;
}

/**
 * Lee los asesores ACTIVOS desde CONFIG_MAESTROS.
 * @return {Array<{nombre: string, descripcion: string}>}
 */
function getActiveAdvisors(ss) {
  ss = ss || getMasterSpreadsheet_();
  var configSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.CONFIG);
  if (!configSheet) return [];
  
  var data = configSheet.getDataRange().getValues();
  var advisors = [];
  
  for (var i = 1; i < data.length; i++) {
    var tipo = String(data[i][0]).trim().toUpperCase();
    var valor = String(data[i][1]).trim();
    var desc = String(data[i][2]).trim();
    var estado = String(data[i][3]).trim().toUpperCase();
    
    if (tipo === 'ASESOR' && estado === 'ACTIVO') {
      advisors.push(enrichAdvisorWithRoute_({ nombre: valor, descripcion: desc }));
    }
  }
  
  return advisors;
}

/**
 * Retorna los nombres de las hojas del sistema (para evitar modificarlas accidentalmente).
 */
function getSystemSheetNames() {
  var names = [];
  for (var key in CONFIG_SYSTEM.SHEETS) {
    names.push(CONFIG_SYSTEM.SHEETS[key]);
  }
  var ignored = CONFIG_SYSTEM.IGNORED_SHEETS || [];
  for (var i = 0; i < ignored.length; i++) {
    names.push(ignored[i]);
  }
  return names;
}

/**
 * Obtiene una hoja por nombre (búsqueda case-insensitive).
 * getSheetByName es case-sensitive; si "Judith R." != "JUDITH R." falla.
 * @param {Spreadsheet} ss - Hoja de cálculo activa
 * @param {string} sheetName - Nombre buscado (ej. del CONFIG)
 * @return {Sheet|null} La hoja encontrada o null
 */
function getSheetByNameIgnoreCase(ss, sheetName) {
  var sheet = ss.getSheetByName(sheetName);
  if (sheet) return sheet;
  var targetUpper = String(sheetName || '').trim().toUpperCase();
  if (!targetUpper) return null;
  var sheets = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    if (String(sheets[i].getName()).trim().toUpperCase() === targetUpper) {
      return sheets[i];
    }
  }
  return null;
}

/**
 * Menú personalizado en la barra superior de Google Sheets.
 */
function diagnosticarRuteoBasesAsesores() {
  var health = healthRuteoBasesAsesores();
  var lines = [
    'RUTEO DE BASES DE ASESORES',
    'CRM maestro: ' + health.masterId,
    'Base presencial: ' + health.presencialId,
    ''
  ];

  for (var i = 0; i < health.advisors.length; i++) {
    var a = health.advisors[i];
    lines.push(
      a.nombre +
      ' | modalidad=' + a.modalidad +
      ' | base=' + a.baseCode +
      ' | hoja=' + (a.exists ? 'OK' : 'NO EXISTE') +
      ' | filas=' + a.rows +
      ' | archivo=' + a.spreadsheetId
    );
  }

  var msg = lines.join('\n');
  logDebug('[RUTEO-ASESORES] ' + msg.replace(/\n/g, ' | '));
  SpreadsheetApp.getUi().alert('Ruteo bases asesores', msg, SpreadsheetApp.getUi().ButtonSet.OK);
}

function healthRuteoBasesAsesores() {
  var advisors = getActiveAdvisors();
  var out = {
    ok: true,
    masterId: getConfiguredMasterSpreadsheetId_(),
    presencialId: (CONFIG_SYSTEM.SPREADSHEETS && CONFIG_SYSTEM.SPREADSHEETS.PRESENCIAL_ID) || '',
    advisors: []
  };

  for (var i = 0; i < advisors.length; i++) {
    var ctx = getAdvisorSheetContext_(advisors[i]);
    var rows = ctx.sheet ? Math.max(0, ctx.sheet.getLastRow() - CONFIG_SYSTEM.ADVISOR_SHEET.ROW_HEADERS) : 0;
    out.advisors.push({
      nombre: advisors[i].nombre,
      descripcion: advisors[i].descripcion || '',
      modalidad: ctx.modalidad,
      baseCode: ctx.baseCode,
      spreadsheetId: ctx.spreadsheetId,
      sheetName: ctx.sheetName,
      exists: !!ctx.sheet,
      rows: rows
    });
  }

  return out;
}

function probarRuteoBasesAsesores() {
  var health = healthRuteoBasesAsesores();
  Logger.log(JSON.stringify(health, null, 2));
  return health;
}

function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('🏢 CRM Gestión')
    .addItem('📋 Abrir Gestor (desde tu base)', 'abrirGestorContextual')
    .addSeparator()
    .addItem('⚙️ Setup Sistema', 'setupSystem')
    .addSeparator()
    .addItem('✅ Validar Leads (hoja activa)', 'validarLeadsRaw')
    .addItem('📤 Exportar Duplicados a Consolidado', 'exportarDuplicadosValidados')
    .addItem('📤 Asignar Leads Round-Robin', 'asignarLeadsRoundRobin')
    .addSeparator()
    .addItem('🔄 Sincronizar bases asesores → DIM', 'sincronizarBasesAsesoresConDIM')
    .addItem('🔄 Migrar Hoja Activa', 'migrarHojaActiva')
    .addItem('🔄 Sincronizar ediciones manuales → FACT', 'sincronizarEdicionesManuales')
    .addItem('🔬 Diagnóstico sync EDICION_MANUAL (sin escribir)', 'diagnosticarSyncEdicionManual')
    .addSeparator()
    .addItem('🔒 Verificar estructura (columnas)', 'verificarEstructura')
    .addItem('🧪 Simular escenarios pre-producción', 'simularEscenariosProduccion')
    .addItem('🧭 Diagnóstico mapeo hoja asesor', 'diagnosticarMapeoHojaAsesor')
    .addItem('📋 Sincronizar desplegables (CONFIG → hoja activa)', 'sincronizarDesplegablesDesdeConfig')
    .addItem('📊 Diagnóstico', 'diagnosticarSistema')
    .addItem('🗑️ Limpiar Caché', 'limpiarCacheManual')
    .addToUi();
}

/**
 * Diagnóstico rápido para verificar qué columnas detecta el sistema en la hoja activa.
 * Útil para validar TIPIF y DISTRITO.
 */
function diagnosticarMapeoHojaAsesor() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getActiveSheet();
  var ui = SpreadsheetApp.getUi();
  var hdrRow = findHeaderRow(sheet);
  var lastCol = Math.max(sheet.getLastColumn(), 16);
  var headers = sheet.getRange(hdrRow, 1, 1, lastCol).getValues()[0];
  var col = { nombre: -1, estadoCivil: -1, pareja: -1, ocupacion: -1, distrito: -1, tipif: -1, comentario: -1 };
  
  for (var h = 0; h < headers.length; h++) {
    var hdr = String(headers[h]).toUpperCase().trim();
    if (hdr.indexOf('NOMBRE') !== -1 && hdr.indexOf('OPC') === -1) col.nombre = h + 1;
    if (hdr.indexOf('CIVIL') !== -1) col.estadoCivil = h + 1;
    if (hdr.indexOf('PAREJA') !== -1) col.pareja = h + 1;
    if (hdr.indexOf('OCUPACION') !== -1) col.ocupacion = h + 1;
    if (hdr.indexOf('DISTRITO') !== -1) col.distrito = h + 1;
    if (hdr === 'TIPIF' || hdr === 'TIPIFICACION') col.tipif = h + 1;
    if (hdr.indexOf('COMENTARIO') !== -1 && hdr.indexOf('OPC') === -1 && hdr.indexOf('WSP') === -1) col.comentario = h + 1;
  }
  
  ui.alert(
    'Diagnóstico mapeo',
    'Hoja: ' + sheet.getName() + '\n' +
    'Fila headers detectada: ' + hdrRow + '\n\n' +
    'TIPIF: columna ' + col.tipif + '\n' +
    'DISTRITO: columna ' + col.distrito + '\n' +
    'NOMBRE: columna ' + col.nombre + '\n' +
    'E.CIVIL: columna ' + col.estadoCivil + '\n' +
    'OCUPACION: columna ' + col.ocupacion + '\n' +
    'COMENTARIO: columna ' + col.comentario,
    ui.ButtonSet.OK
  );
}

/**
 * Construye comentario acumulado: [DD/MM/YYYY] Nuevo | [DD/MM/YYYY] Anterior
 * Orden: más reciente primero.
 */
function buildAccumulatedComment(newComment, existingComment) {
  var dateStr = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd/MM/yyyy");
  var formattedNew = "[" + dateStr + "] " + newComment;
  
  if (existingComment && String(existingComment).trim() !== "") {
    return formattedNew + " | " + String(existingComment).trim();
  }
  return formattedNew;
}

/**
 * Actualiza la fila del asesor con los datos del sidebar.
 * Solo sobreescribe campos no vacíos (preserva datos existentes).
 * Usa detección dinámica de columnas por header para soportar cualquier layout.
 * @return {{ updated: boolean, reason?: string }} updated=true si se escribió en la hoja del asesor
 */
function updateAdvisorSheetRow(formData, timestamp) {
  if (!formData.sheetName || formData.row === undefined || formData.row === null || formData.row === '') {
    logDebug('[ADVISOR-SYNC] Sin contexto de hoja/fila, omitiendo actualización');
    return { updated: false, reason: 'Sin hoja o fila' };
  }
  
  // No actualizar hojas del sistema (DIM, FACT, CONFIG, etc.)
  var systemSheets = getSystemSheetNames();
  if (systemSheets.indexOf(formData.sheetName) !== -1) {
    logDebug('[ADVISOR-SYNC] Hoja del sistema detectada, omitiendo: ' + formData.sheetName);
    return { updated: false, reason: 'Hoja del sistema' };
  }
  
  var advCtx = getAdvisorSheetContextByName_(formData.sheetName);
  var advSheet = advCtx.sheet;
  
  if (!advSheet) {
    logError('[ADVISOR-SYNC] Hoja no encontrada: ' + formData.sheetName + ' en base ' + advCtx.baseCode);
    return { updated: false, reason: 'Hoja no encontrada' };
  }
  
  var row = parseInt(formData.row, 10);
  var minDataRow = CONFIG_SYSTEM.ADVISOR_SHEET.ROW_FIRST_DATA;
  if (isNaN(row) || row < minDataRow) {
    logError('[ADVISOR-SYNC] Fila inválida: ' + formData.row + ' (mínimo datos: ' + minDataRow + ')');
    return { updated: false, reason: 'Fila inválida' };
  }
  
  try {
    // Detectar columnas dinámicamente por header
    var hdrRow = findHeaderRow(advSheet);
    var lastCol = advSheet.getLastColumn();
    if (lastCol < 14) lastCol = 16;
    var headers = advSheet.getRange(hdrRow, 1, 1, lastCol).getValues()[0];
    
    // Mapear headers a índices (0-based)
    var col = { nombre: -1, estadoCivil: -1, pareja: -1, ocupacion: -1, distrito: -1, tipif: -1, comentario: -1 };
    
    for (var h = 0; h < headers.length; h++) {
      var hdr = String(headers[h]).toUpperCase().trim();
      if (hdr.indexOf('NOMBRE') !== -1 && hdr.indexOf('OPC') === -1) col.nombre = h;
      if (hdr.indexOf('CIVIL') !== -1) col.estadoCivil = h;
      if (hdr.indexOf('PAREJA') !== -1) col.pareja = h;
      if (hdr.indexOf('OCUPACION') !== -1) col.ocupacion = h;
      if (hdr.indexOf('DISTRITO') !== -1) col.distrito = h;
      if (hdr === 'TIPIF' || hdr === 'TIPIFICACION') col.tipif = h;
      if (hdr.indexOf('COMENTARIO') !== -1 && hdr.indexOf('OPC') === -1 && hdr.indexOf('WSP') === -1) col.comentario = h;
    }
    
    logDebug('[ADVISOR-SYNC] Columnas detectadas: NOMBRE=' + col.nombre + ' TIPIF=' + col.tipif + ' DISTRITO=' + col.distrito + ' COMENTARIO=' + col.comentario);
    
    // Leer fila actual solo para construir comentario acumulado
    var range = advSheet.getRange(row, 1, 1, lastCol);
    var rowData = range.getValues()[0];
    var updates = [];
    
    // Agregar SOLO campos realmente editados (no tocar celdas que no cambian)
    if (col.nombre !== -1 && formData.nombre && formData.nombre !== 'Sin Nombre' && formData.nombre.trim() !== '') {
      updates.push({ idx: col.nombre, val: formData.nombre.toUpperCase() });
    }
    if (col.estadoCivil !== -1 && formData.estadoCivil && formData.estadoCivil.trim() !== '') {
      updates.push({ idx: col.estadoCivil, val: formData.estadoCivil.toUpperCase() });
    }
    if (col.pareja !== -1 && formData.tienePareja && formData.tienePareja.trim() !== '') {
      updates.push({ idx: col.pareja, val: formData.tienePareja.toUpperCase() });
    }
    if (col.ocupacion !== -1 && formData.ocupacion && formData.ocupacion.trim() !== '') {
      updates.push({ idx: col.ocupacion, val: formData.ocupacion.toUpperCase() });
    }
    if (col.distrito !== -1 && formData.distrito && formData.distrito.trim() !== '') {
      updates.push({ idx: col.distrito, val: normalizarDistrito(formData.distrito) });
    }
    if (col.tipif !== -1 && formData.tipificacion && formData.tipificacion.trim() !== '') {
      updates.push({ idx: col.tipif, val: formData.tipificacion.toUpperCase() });
    }
    
    // COMENTARIO (acumulativo, más reciente arriba)
    if (col.comentario !== -1 && formData.notas && formData.notas.trim() !== '') {
      var existingComment = rowData[col.comentario] ? String(rowData[col.comentario]) : '';
      var newAccComment = buildAccumulatedComment(formData.notas.trim(), existingComment);
      updates.push({ idx: col.comentario, val: newAccComment });
      logDebug('[ADVISOR-SYNC] COMENTARIO: col=' + col.comentario + ' | existente="' + existingComment.substring(0, 50) + '" | nuevo="' + newAccComment.substring(0, 80) + '"');
    } else {
      logDebug('[ADVISOR-SYNC] COMENTARIO NO agregado: col.comentario=' + col.comentario + ' | notas="' + (formData.notas || 'NULL') + '"');
    }
    
    // Escribir valor sin alterar formato: mantener validación tipo lista, pero con allowInvalid=true
    // para aceptar valores fuera del dropdown sin romper estilo/celda.
    var writeErrors = [];
    for (var u = 0; u < updates.length; u++) {
      var item = updates[u];
      if (item.idx === -1) continue;
      try {
        var cell = advSheet.getRange(row, item.idx + 1);
        var current = cell.getValue();
        if (String(current) === String(item.val)) continue;
        setCellValuePreservingFormat(cell, item.val);
      } catch (writeErr) {
        writeErrors.push('Col' + (item.idx + 1) + ': ' + writeErr.message);
        logDebug('[ADVISOR-SYNC] Error col ' + item.idx + ': ' + writeErr.message);
      }
    }
    if (writeErrors.length > 0) {
      logDebug('[ADVISOR-SYNC] Escritura parcial en fila ' + row + ': ' + writeErrors.join(' | '));
    }
    
    logDebug('[ADVISOR-SYNC] Fila ' + row + ' actualizada en "' + formData.sheetName + '"');
    return { updated: true };
    
  } catch (e) {
    logError('[ADVISOR-SYNC] Error al actualizar fila: ' + e.message);
    return { updated: false, reason: e.message };
  }
}

/**
 * Escribe valor en una celda sin alterar el estilo del dropdown:
 * 1) guarda regla original
 * 2) aplica regla temporal allowInvalid=true para permitir escritura
 * 3) escribe valor
 * 4) restaura regla original exacta (look & behavior)
 */
function setCellValuePreservingFormat(cell, value) {
  // Camino principal: escribir directo SIN tocar validación
  // (preserva completamente el estilo del dropdown/chip).
  try {
    cell.setValue(value);
    return;
  } catch (directErr) {
    // Si falla por validación estricta, usar fallback controlado.
  }
  
  var originalRule = cell.getDataValidation();
  if (!originalRule) {
    // Sin regla y falló setValue por otro motivo: relanzar para diagnóstico.
    throw new Error('No se pudo escribir valor: ' + value);
  }
  
  var relaxedRule = null;
  try {
    relaxedRule = buildRuleWithAllowInvalid(originalRule, true);
    if (relaxedRule) {
      cell.setDataValidation(relaxedRule);
      cell.setValue(value);
      cell.setDataValidation(originalRule);
      return;
    }
  } catch (e) {
    // Continuar a fallback
  }
  
  // Fallback extremo: quitar validación, escribir y restaurar la original exacta
  try {
    cell.clearDataValidations();
    cell.setValue(value);
  } finally {
    try {
      cell.setDataValidation(originalRule);
    } catch (restoreErr) {
      logDebug('[ADVISOR-SYNC] No se pudo restaurar validación original: ' + restoreErr.message);
    }
  }
}

/**
 * Clona una regla de validación de lista/rango cambiando solo allowInvalid.
 */
function buildRuleWithAllowInvalid(rule, allowInvalid) {
  if (!rule) return null;
  
  var criteria = rule.getCriteriaType();
  var args = rule.getCriteriaValues();
  var help = rule.getHelpText();
  var b = SpreadsheetApp.newDataValidation().setAllowInvalid(allowInvalid);
  if (help) b.setHelpText(help);
  
  if (criteria === SpreadsheetApp.DataValidationCriteria.VALUE_IN_LIST) {
    b.requireValueInList(args[0], args[1]);
    return b.build();
  }
  if (criteria === SpreadsheetApp.DataValidationCriteria.VALUE_IN_RANGE) {
    b.requireValueInRange(args[0], args[1]);
    return b.build();
  }
  
  // Para otros tipos, no alterar.
  return null;
}

/** Aplica validación de lista en una sola celda (por tipo: tipif, estadoCivil, ocupacion, distrito). */
function aplicarValidacionCeldaAsesor(sheet, dataRow, col1Based, type) {
  try {
    var config = getConfigDropdowns();
    var list = [];
    if (type === 'tipif' && config.tipificaciones) list = config.tipificaciones;
    else if (type === 'estadoCivil' && config.estadosCiviles) list = config.estadosCiviles;
    else if (type === 'ocupacion' && config.ocupaciones) list = config.ocupaciones;
    else if (type === 'distrito' && config.ubicaciones) list = config.ubicaciones;
    if (list.length > 0) {
      sheet.getRange(dataRow, col1Based).setDataValidation(
        SpreadsheetApp.newDataValidation().requireValueInList(list, true).setAllowInvalid(false).build()
      );
    }
  } catch (e) {
    logDebug('[VALIDACION] Celda: ' + e.message);
  }
}

/**
 * Aplica validaciones de lista (TIPIF, E. CIVIL, OCUPACION, DISTRITO) a un rango de filas.
 * Usado por asignarLeadsRoundRobin para mantener los mismos dropdowns que al guardar interacción.
 */
function aplicarValidacionesRangoAsesor(sheet, startRow, endRow, colMap) {
  try {
    var config = getConfigDropdowns();
    if (!config) return;
    if (colMap.tipif !== undefined && colMap.tipif !== -1 && config.tipificaciones && config.tipificaciones.length > 0) {
      var rTipif = sheet.getRange(startRow, colMap.tipif + 1, endRow - startRow + 1, 1);
      rTipif.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.tipificaciones, true).setAllowInvalid(false).build());
    }
    if (colMap.estadoCivil !== undefined && colMap.estadoCivil !== -1 && config.estadosCiviles && config.estadosCiviles.length > 0) {
      var rCivil = sheet.getRange(startRow, colMap.estadoCivil + 1, endRow - startRow + 1, 1);
      rCivil.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.estadosCiviles, true).setAllowInvalid(false).build());
    }
    if (colMap.ocupacion !== undefined && colMap.ocupacion !== -1 && config.ocupaciones && config.ocupaciones.length > 0) {
      var rOcup = sheet.getRange(startRow, colMap.ocupacion + 1, endRow - startRow + 1, 1);
      rOcup.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.ocupaciones, true).setAllowInvalid(false).build());
    }
    if (colMap.distrito !== undefined && colMap.distrito !== -1 && config.ubicaciones && config.ubicaciones.length > 0) {
      var rDist = sheet.getRange(startRow, colMap.distrito + 1, endRow - startRow + 1, 1);
      rDist.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.ubicaciones, true).setAllowInvalid(false).build());
    }
  } catch (e) {
    logDebug('[ASIGNACION] Validaciones en rango: ' + e.message);
  }
}

/**
 * Aplica validación de lista (desplegable) en una fila de hoja de asesor, usando valores de CONFIG_MAESTROS.
 */
function aplicarValidacionesFilaAsesor(sheet, dataRow, headerRow, lastCol, colMap) {
  try {
    var config = getConfigDropdowns();
    if (colMap.tipif !== -1 && config.tipificaciones && config.tipificaciones.length > 0) {
      sheet.getRange(dataRow, colMap.tipif + 1).setDataValidation(
        SpreadsheetApp.newDataValidation().requireValueInList(config.tipificaciones, true).setAllowInvalid(false).build()
      );
    }
    if (colMap.estadoCivil !== -1 && config.estadosCiviles && config.estadosCiviles.length > 0) {
      sheet.getRange(dataRow, colMap.estadoCivil + 1).setDataValidation(
        SpreadsheetApp.newDataValidation().requireValueInList(config.estadosCiviles, true).setAllowInvalid(false).build()
      );
    }
    if (colMap.ocupacion !== -1 && config.ocupaciones && config.ocupaciones.length > 0) {
      sheet.getRange(dataRow, colMap.ocupacion + 1).setDataValidation(
        SpreadsheetApp.newDataValidation().requireValueInList(config.ocupaciones, true).setAllowInvalid(false).build()
      );
    }
    if (colMap.distrito !== -1 && config.ubicaciones && config.ubicaciones.length > 0) {
      sheet.getRange(dataRow, colMap.distrito + 1).setDataValidation(
        SpreadsheetApp.newDataValidation().requireValueInList(config.ubicaciones, true).setAllowInvalid(false).build()
      );
    }
  } catch (e) {
    logDebug('[VALIDACION] No se pudo reaplicar listas en fila: ' + e.message);
  }
}

/**
 * Sincroniza todas las listas desplegables de la hoja activa con CONFIG_MAESTROS.
 * Ejecutar desde una base de asesor (o cualquier hoja con headers TIPIF, E. CIVIL, etc.) para que
 * las celdas acepten exactamente los valores de tu tabla de configuración.
 */
function sincronizarDesplegablesDesdeConfig() {
  var ui = SpreadsheetApp.getUi();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getActiveSheet();
  var sheetName = sheet.getName();
  var systemSheets = getSystemSheetNames();
  if (systemSheets.indexOf(sheetName) !== -1) {
    ui.alert('No aplica', 'Ejecuta esta opción desde una hoja de asesor (tu base), no desde DIM, FACT ni CONFIG.', ui.ButtonSet.OK);
    return;
  }
  var hdrRow = findHeaderRow(sheet);
  var lastCol = Math.max(sheet.getLastColumn(), 16);
  var lastRow = sheet.getLastRow();
  var firstDataRow = CONFIG_SYSTEM.ADVISOR_SHEET.ROW_FIRST_DATA;
  if (lastRow < firstDataRow) {
    ui.alert('Sin datos', 'No hay filas de datos para aplicar validaciones.', ui.ButtonSet.OK);
    return;
  }
  var headers = sheet.getRange(hdrRow, 1, 1, lastCol).getValues()[0];
  var col = { estadoCivil: -1, ocupacion: -1, distrito: -1, tipif: -1 };
  for (var h = 0; h < headers.length; h++) {
    var hdr = String(headers[h]).toUpperCase().trim();
    if (hdr.indexOf('CIVIL') !== -1) col.estadoCivil = h;
    if (hdr.indexOf('OCUPACION') !== -1) col.ocupacion = h;
    if (hdr.indexOf('DISTRITO') !== -1) col.distrito = h;
    if (hdr === 'TIPIF' || hdr === 'TIPIFICACION') col.tipif = h;
  }
  var config = getConfigDropdowns();
  var count = 0;
  if (col.tipif !== -1 && config.tipificaciones && config.tipificaciones.length > 0) {
    var r = sheet.getRange(firstDataRow, col.tipif + 1, lastRow, col.tipif + 1);
    r.clearDataValidations();
    r.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.tipificaciones, true).setAllowInvalid(false).build());
    count++;
  }
  if (col.estadoCivil !== -1 && config.estadosCiviles && config.estadosCiviles.length > 0) {
    var r = sheet.getRange(firstDataRow, col.estadoCivil + 1, lastRow, col.estadoCivil + 1);
    r.clearDataValidations();
    r.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.estadosCiviles, true).setAllowInvalid(false).build());
    count++;
  }
  if (col.ocupacion !== -1 && config.ocupaciones && config.ocupaciones.length > 0) {
    var r = sheet.getRange(firstDataRow, col.ocupacion + 1, lastRow, col.ocupacion + 1);
    r.clearDataValidations();
    r.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.ocupaciones, true).setAllowInvalid(false).build());
    count++;
  }
  if (col.distrito !== -1 && config.ubicaciones && config.ubicaciones.length > 0) {
    var r = sheet.getRange(firstDataRow, col.distrito + 1, lastRow, col.distrito + 1);
    r.clearDataValidations();
    r.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(config.ubicaciones, true).setAllowInvalid(false).build());
    count++;
  }
  ui.alert('Listo', 'Desplegables sincronizados con CONFIG_MAESTROS en esta hoja.\nColumnas actualizadas: ' + count + ' (TIPIF, E. CIVIL, OCUPACION, DISTRITO).', ui.ButtonSet.OK);
}

function formatDate(dateObj) {
  if (!dateObj) return "";
  if (dateObj instanceof Date) {
    return Utilities.formatDate(dateObj, Session.getScriptTimeZone(), "dd/MM/yyyy HH:mm");
  }
  return String(dateObj);
}

function formatHeader(sheet, columns, colorHex) {
  var range = sheet.getRange(1, 1, 1, columns);
  range.setBackground(colorHex)
       .setFontColor("white")
       .setFontWeight("bold")
       .setFontSize(10)
       .setHorizontalAlignment("center");
  sheet.setFrozenRows(1);
}

/** Formatea una fila de encabezados en la fila indicada (ej. fila 2 en bases de asesores). */
function formatHeaderRow(sheet, headerRow, columns, colorHex) {
  var range = sheet.getRange(headerRow, 1, 1, columns);
  range.setBackground(colorHex)
       .setFontColor("white")
       .setFontWeight("bold")
       .setFontSize(10)
       .setHorizontalAlignment("center");
}

function extractNameFromEmail(email) {
  if (!email) return 'Sistema';
  var parts = String(email).split('@');
  if (parts.length > 0) {
    var name = parts[0].replace(/\./g, ' ');
    return name.charAt(0).toUpperCase() + name.slice(1);
  }
  return 'Sistema';
}

// ==========================================================================
// 8. SISTEMA DE LOGGING MEJORADO
// ==========================================================================

function logDebug(message) {
  Logger.log('[' + new Date().toISOString() + '] ' + message);
}

function logError(message) {
  Logger.log('[ERROR] [' + new Date().toISOString() + '] ' + message);
  
  // Opcionalmente, escribir errores críticos en LOG_SISTEMA
  try {
    var ss = getMasterSpreadsheet_();
    var logSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.LOG);
    if (logSheet) {
      logSheet.appendRow([
        new Date(),
        Session.getActiveUser().getEmail(),
        'ERROR_SISTEMA',
        '',
        '',
        message
      ]);
    }
  } catch (e) {
    // Evitar loops infinitos
  }
}

// ==========================================================================
// 9. HERRAMIENTAS DE DIAGNÓSTICO
// ==========================================================================

function diagnosticarSistema() {
  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  
  var dimRows = dimSheet ? dimSheet.getLastRow() - 1 : 0;
  var factRows = factSheet ? factSheet.getLastRow() - 1 : 0;
  
  var rendimiento = "";
  if (dimRows < 1000) rendimiento = "⚡ Excelente (< 500ms)";
  else if (dimRows < 5000) rendimiento = "✅ Bueno (< 2s)";
  else if (dimRows < 10000) rendimiento = "⚠️ Aceptable (2-5s)";
  else rendimiento = "🔴 Lento (>5s) - Cache activado";
  
  var cacheMemStatus = CACHE_CLIENTS ? '✅ Activo' : '⏸️ Inactivo';
  var cachePersistStatus = getPersistentCache() ? '✅ Activo' : '⏸️ Inactivo';
  
  var mensaje = 
    '📊 DIAGNÓSTICO DEL SISTEMA v5.5\n\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    'DATOS:\n' +
    '  DIM_CLIENTES: ' + dimRows + ' clientes\n' +
    '  FACT_INTERACCIONES: ' + factRows + ' interacciones\n\n' +
    'RENDIMIENTO:\n' +
    '  Proyectado: ' + rendimiento + '\n\n' +
    'CACHÉ:\n' +
    '  Nivel 1 (Memoria): ' + cacheMemStatus + '\n' +
    '  Nivel 2 (Persistente): ' + cachePersistStatus + '\n' +
    '  Clientes en caché: ' + (CACHE_CLIENTS ? Object.keys(CACHE_CLIENTS).length : 0) + '\n\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
  
  SpreadsheetApp.getUi().alert(mensaje);
  logDebug('[DIAGNOSTICO] ' + mensaje.replace(/\n/g, ' | '));
}

/**
 * Simula múltiples escenarios operativos para detectar incongruencias antes de producción.
 * No modifica datos reales; solo valida flujos y dependencias.
 */
function simularEscenariosProduccion() {
  var ui = SpreadsheetApp.getUi();
  var report = [];
  var ok = 0;
  var fail = 0;

  report.push('═══════════════════════════════════════');
  report.push('SIMULACIÓN PRE-PRODUCCIÓN CRM v6.0');
  report.push('═══════════════════════════════════════\n');

  // 1. Hojas del sistema
  var ss = getMasterSpreadsheet_();
  var sheets = ['DIM', 'FACT', 'CONFIG', 'OPC', 'META', 'GOOGLE_ADS', 'DUPLICADOS'];
  report.push('1. HOJAS DEL SISTEMA');
  for (var s = 0; s < sheets.length; s++) {
    var name = CONFIG_SYSTEM.SHEETS[sheets[s]] || sheets[s];
    var sh = ss.getSheetByName(name);
    if (sh) {
      report.push('   ✅ ' + name);
      ok++;
    } else {
      report.push('   ❌ ' + name + ' (no existe)');
      fail++;
    }
  }

  // 2. Config dropdowns
  report.push('\n2. CONFIGURACIÓN (CONFIG_MAESTROS)');
  try {
    var config = getConfigDropdowns();
    var tipCount = config.tipificaciones ? config.tipificaciones.length : 0;
    var ubCount = config.ubicaciones ? config.ubicaciones.length : 0;
    report.push('   ✅ Tipificaciones: ' + tipCount);
    report.push('   ✅ Ubicaciones: ' + ubCount);
    if (tipCount === 0 || ubCount === 0) {
      report.push('   ⚠️ Revisa que CONFIG tenga valores ACTIVOS');
    }
    ok++;
  } catch (e) {
    report.push('   ❌ Error: ' + e.message);
    fail++;
  }

  // 3. Asesores activos
  report.push('\n3. ASESORES ACTIVOS');
  try {
    var advisors = getActiveAdvisors();
    report.push('   ✅ Asesores: ' + advisors.length);
    if (advisors.length > 0) {
      advisors.forEach(function(a) { report.push('      • ' + a.nombre); });
    } else {
      report.push('   ⚠️ Sin asesores ACTIVOS en CONFIG');
    }
    ok++;
  } catch (e) {
    report.push('   ❌ Error: ' + e.message);
    fail++;
  }

  // 4. Normalización de teléfono
  report.push('\n4. NORMALIZACIÓN DE TELÉFONO');
  var testPhones = ['987654321', '51987654321', '(987) 654-321', '987-654-321', 'abc', '', '123'];
  for (var p = 0; p < testPhones.length; p++) {
    var clean = normalizePhoneETL(testPhones[p]);
    var valid = clean && /^9\d{8}$/.test(clean);
    report.push('   ' + (valid ? '✅' : '⚠️') + ' "' + testPhones[p] + '" → "' + (clean || '') + '"');
  }

  // 5. Búsqueda de cliente (si DIM tiene datos)
  report.push('\n5. BÚSQUEDA searchClient');
  try {
    var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
    var t0 = new Date().getTime();
    var res = searchClient('987654321');
    var t1 = new Date().getTime();
    report.push('   ✅ searchClient ejecutado en ' + (t1 - t0) + 'ms');
    report.push('   Resultado: ' + (res.found ? 'Encontrado' : 'No encontrado'));
    ok++;
  } catch (e) {
    report.push('   ❌ Error: ' + e.message);
    fail++;
  }

  // 6. Lock disponible
  report.push('\n6. LOCK (CONCURRENCIA)');
  try {
    var lock = LockService.getScriptLock();
    var gotLock = lock.tryLock(1000);
    if (gotLock) {
      lock.releaseLock();
      report.push('   ✅ LockService operativo');
      ok++;
    } else {
      report.push('   ⚠️ Lock ocupado (otro proceso en curso)');
    }
  } catch (e) {
    report.push('   ❌ Error: ' + e.message);
    fail++;
  }

  // 7. Estructura DIM/FACT
  report.push('\n7. ESTRUCTURA DIM/FACT');
  try {
    var dimSh = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
    var factSh = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
    if (dimSh && dimSh.getLastRow() >= 1) {
      var dimCols = dimSh.getLastColumn();
      report.push('   DIM: ' + (dimSh.getLastRow() - 1) + ' filas, ' + dimCols + ' cols');
      if (dimCols < 16) report.push('   ⚠️ DIM espera 16 columnas');
    }
    if (factSh && factSh.getLastRow() >= 1) {
      var factCols = factSh.getLastColumn();
      report.push('   FACT: ' + (factSh.getLastRow() - 1) + ' filas, ' + factCols + ' cols');
      if (factCols < 16) report.push('   ⚠️ FACT espera 16 columnas');
    }
    ok++;
  } catch (e) {
    report.push('   ❌ Error: ' + e.message);
    fail++;
  }

  // 8. CONFIG_SYSTEM
  report.push('\n8. CONFIG_SYSTEM');
  report.push('   LIMITE_DIARIO_ASIGNACION: ' + (CONFIG_SYSTEM.LIMITE_DIARIO_ASIGNACION || 40));

  report.push('\n═══════════════════════════════════════');
  report.push('RESUMEN: ' + ok + ' OK | ' + fail + ' fallos');
  report.push('═══════════════════════════════════════');

  var msg = report.join('\n');
  logDebug('[SIMULACION] ' + msg.replace(/\n/g, ' | '));
  ui.alert('Simulación Pre-Producción', msg, ui.ButtonSet.OK);
}

/**
 * Verifica que las hojas del sistema existan y que DIM/FACT tengan las cabeceras esperadas.
 * Útil después de proteger celdas para confirmar que la estructura no se rompió.
 */
function verificarEstructura() {
  var ss = getMasterSpreadsheet_();
  var problems = [];
  var expectedDimHeaders = ['ID_CLIENTE', 'CELULAR', 'NOMBRE', 'EMAIL', 'ORIGEN', 'PROYECTO', 'FECHA_REGISTRO', 'ASESOR_ACTUAL', 'ULTIMA_TIPIF', 'FECHA_ULT_GESTION', 'FUENTE_NORMALIZADA', 'NOMBRE_OPC', 'ESTADO_CIVIL', 'OCUPACION', 'TIENE_PAREJA', 'DISTRITO'];
  var expectedFactHeaders = ['ID_INTERACCION', 'ID_CLIENTE', 'CELULAR', 'NOMBRE_CLIENTE', 'ASESOR_NOMBRE', 'ASESOR_EMAIL', 'FECHA_INTERACCION', 'FECHA_REGISTRO_LEAD', 'PROYECTO', 'FUENTE_ORIGINAL', 'FUENTE_NORMALIZADA', 'NOMBRE_OPC', 'TIPO_ACCION', 'TIPIFICACION', 'COMENTARIO', 'METADATA'];

  for (var key in CONFIG_SYSTEM.SHEETS) {
    var name = CONFIG_SYSTEM.SHEETS[key];
    var sheet = ss.getSheetByName(name);
    if (!sheet) {
      problems.push('Falta hoja: ' + name);
      continue;
    }
    if (sheet.getLastRow() < 1) {
      problems.push('Hoja sin fila 1: ' + name);
      continue;
    }
    var row1 = sheet.getRange(1, 1, 1, Math.max(16, sheet.getLastColumn())).getValues()[0];
    if (name === CONFIG_SYSTEM.SHEETS.DIM) {
      for (var i = 0; i < expectedDimHeaders.length; i++) {
        if (String(row1[i] || '').trim() !== expectedDimHeaders[i]) {
          problems.push('DIM_CLIENTES col ' + (i + 1) + ': esperado "' + expectedDimHeaders[i] + '", tiene "' + (row1[i] || '') + '"');
        }
      }
    } else if (name === CONFIG_SYSTEM.SHEETS.FACT) {
      for (var j = 0; j < expectedFactHeaders.length; j++) {
        if (String(row1[j] || '').trim() !== expectedFactHeaders[j]) {
          problems.push('FACT_INTERACCIONES col ' + (j + 1) + ': esperado "' + expectedFactHeaders[j] + '", tiene "' + (row1[j] || '') + '"');
        }
      }
    }
  }

  var msg = problems.length === 0
    ? 'Estructura OK.\n\nTodas las hojas del sistema existen y DIM/FACT tienen las cabeceras correctas.'
    : 'Se encontraron ' + problems.length + ' problema(s):\n\n' + problems.slice(0, 15).join('\n') + (problems.length > 15 ? '\n... y más' : '');
  SpreadsheetApp.getUi().alert(problems.length === 0 ? 'Verificación OK' : 'Atención: estructura', msg);
}

function testRendimientoBusqueda() {
  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  
  if (!dimSheet || dimSheet.getLastRow() < 2) {
    SpreadsheetApp.getUi().alert("No hay datos para probar.");
    return;
  }
  
  var data = dimSheet.getDataRange().getValues();
  var randomIndex = Math.floor(Math.random() * (data.length - 1)) + 1;
  var testPhone = data[randomIndex][1];
  
  logDebug('[TEST] Probando búsqueda con: ' + testPhone);
  
  // Limpiar caché para test real
  invalidateCache();
  
  var t0 = new Date().getTime();
  var resultado = searchClient(testPhone);
  var t1 = new Date().getTime();
  var tiempoTotal = t1 - t0;
  
  var evaluacion = "";
  if (tiempoTotal < 500) evaluacion = "⚡ Excelente";
  else if (tiempoTotal < 2000) evaluacion = "✅ Bueno";
  else if (tiempoTotal < 5000) evaluacion = "⚠️ Aceptable";
  else evaluacion = "🔴 Necesita optimización";
  
  var mensaje = 
    '⏱️ TEST DE RENDIMIENTO\n\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
    'Teléfono: ' + testPhone + '\n' +
    'Encontrado: ' + (resultado.found ? '✅ SI' : '❌ NO') + '\n' +
    'Tiempo: ' + tiempoTotal + 'ms\n' +
    'Evaluación: ' + evaluacion + '\n\n' +
    'Historial: ' + (resultado.history ? resultado.history.length : 0) + ' items\n' +
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
  
  SpreadsheetApp.getUi().alert(mensaje);
  logDebug('[TEST] Resultado: ' + tiempoTotal + 'ms | Evaluación: ' + evaluacion);
}

function limpiarComentariosMultilinea() {
  var sheet = SpreadsheetApp.getActiveSheet();
  var ui = SpreadsheetApp.getUi();
  
  var response = ui.alert(
    "🧹 Limpieza de Comentarios",
    "Esta función eliminará los saltos de línea en la columna COMENTARIO.\n\n¿Continuar?",
    ui.ButtonSet.YES_NO
  );
  
  if (response !== ui.Button.YES) return;
  
  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var comentarioCol = -1;
  
  for (var i = 0; i < headers.length; i++) {
    if (String(headers[i]).toUpperCase().indexOf('COMENTARIO') !== -1) {
      comentarioCol = i + 1;
      break;
    }
  }
  
  if (comentarioCol === -1) {
    ui.alert('No se encontró una columna de COMENTARIO en esta hoja.');
    return;
  }
  
  var data = sheet.getRange(2, comentarioCol, sheet.getLastRow()-1, 1).getValues();
  
  var cleaned = [];
  for (var j = 0; j < data.length; j++) {
    var valor = String(data[j][0])
      .replace(/\n/g, ' | ')
      .replace(/\r/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    cleaned.push([valor]);
  }
  
  sheet.getRange(2, comentarioCol, cleaned.length, 1).setValues(cleaned);
  
  ui.alert("✅ Limpieza completada. " + cleaned.length + " celdas procesadas.");
  logDebug('[LIMPIEZA] Procesadas ' + cleaned.length + ' celdas en columna ' + comentarioCol);
}

/**
 * NUEVA FUNCIÓN v5.5: Limpiar caché manualmente
 */
function limpiarCacheManual() {
  invalidateCache();
  SpreadsheetApp.getUi().alert('✅ Caché limpiado exitosamente.\n\nEl sistema reconstruirá el caché en la próxima búsqueda.');
}

/**
 * =====================================================================
 * MÓDULO BI: GENERADOR DE DATOS PARA EMBUDO LOOKER STUDIO
 * Extrae datos de FACT_INTERACCIONES, agrupa por CELULAR y expande
 * en formato Unpivoted (1 fila por cada etapa alcanzada) manteniendo
 * las dimensiones para permitir filtros dinámicos en Looker.
 *
 * USO EN LOOKER STUDIO:
 * - NO mezclar (blending) con FACT_INTERACCIONES para el embudo.
 *   Usar DATA_EMBUDO_LOOKER como fuente única del embudo.
 * - FECHA_ENTRADA_LEAD: primera interacción del lead (para filtrar por cohorte).
 *   Ej: "Leads que entraron en enero" → filtro por FECHA_ENTRADA_LEAD.
 * - ULTIMA_FECHA: última interacción (para "actividad reciente").
 * - Métrica: COUNT_DISTINCT(CELULAR) en ETAPA_EMBUDO.
 * - Cuenta PERSONAS únicas por etapa, no eventos (evita inflar por gestiones).
 * =====================================================================
 */
function generarDataEmbudoLooker() {
  var t0 = new Date().getTime();
  var ss = getMasterSpreadsheet_();

  // Usar configuración global si existe, sino texto directo
  var factSheetName = (typeof CONFIG_SYSTEM !== 'undefined' && CONFIG_SYSTEM.SHEETS.FACT) ? CONFIG_SYSTEM.SHEETS.FACT : 'FACT_INTERACCIONES';
  var factSheet = ss.getSheetByName(factSheetName);

  if (!factSheet) {
    if (typeof logError === "function") logError('[EMBUDO] Error: No se encontró ' + factSheetName);
    return;
  }

  // Crear o limpiar hoja destino
  var targetSheetName = 'DATA_EMBUDO_LOOKER';
  var targetSheet = ss.getSheetByName(targetSheetName);
  if (!targetSheet) {
    targetSheet = ss.insertSheet(targetSheetName);
  }

  var data = factSheet.getDataRange().getValues();
  if (data.length < 2) return; // Si solo hay encabezados, salir.

  // Índices basados en la estructura FACT_INTERACCIONES (0-based)
  var idxCelular = 2;   // CELULAR
  var idxAsesor = 4;    // ASESOR_NOMBRE
  var idxFecha = 6;     // FECHA_INTERACCION
  var idxProyecto = 8;  // PROYECTO
  var idxFuente = 10;   // FUENTE_NORMALIZADA
  var idxOpc = 11;      // NOMBRE_OPC
  var idxTipif = 13;    // TIPIFICACION

  var leadsMap = {};

  // Listas de tipificaciones según tu regla de negocio
  var invalidContacts = ["AP", "FS", "DF", "NC", "NEX/FS", "BZ", "N/A", ""];
  var potenciales = ["CC", "VLL", "IW", "GW", "SG", "HP", "VP", "CP", "CXC", "CZ"];
  var citas = ["CC", "CZ", "HP", "VP"];

  // 1. Procesar todo el historial y agrupar la mejor etapa por CELULAR
  for (var i = 1; i < data.length; i++) {
    var row = data[i];
    var celular = String(row[idxCelular]).replace(/\D/g, '');
    if (!celular) continue;

    var tipif = String(row[idxTipif]).trim().toUpperCase();
    var fecha = row[idxFecha];

    var currentFecha = new Date(fecha);

    // Inicializar el cliente si no existe en el mapa
    if (!leadsMap[celular]) {
      leadsMap[celular] = {
        celular: celular,
        asesor: row[idxAsesor],
        proyecto: row[idxProyecto],
        fuente: row[idxFuente],
        opc: row[idxOpc],
        fecha: fecha,
        fechaEntrada: fecha,
        tipif: tipif,
        isContacto: false,
        isPotencial: false,
        isCita: false
      };
    } else {
      // Actualizar dimensiones siempre con la interacción MÁS RECIENTE
      var storedFecha = new Date(leadsMap[celular].fecha);
      if (currentFecha > storedFecha) {
         leadsMap[celular].asesor = row[idxAsesor];
         leadsMap[celular].proyecto = row[idxProyecto];
         leadsMap[celular].fuente = row[idxFuente];
         leadsMap[celular].opc = row[idxOpc];
         leadsMap[celular].fecha = fecha;
         leadsMap[celular].tipif = tipif;
      }
      // Mantener FECHA_ENTRADA = primera interacción (para filtrar por cohorte)
      var storedEntrada = new Date(leadsMap[celular].fechaEntrada);
      if (currentFecha < storedEntrada) {
        leadsMap[celular].fechaEntrada = fecha;
      }
    }

    // Evaluar qué etapas ha superado en CUALQUIER interacción de su historia
    if (tipif !== "" && invalidContacts.indexOf(tipif) === -1) {
      leadsMap[celular].isContacto = true;
    }
    if (potenciales.indexOf(tipif) !== -1) {
      leadsMap[celular].isPotencial = true;
    }
    if (citas.indexOf(tipif) !== -1) {
      leadsMap[celular].isCita = true;
    }
  }

  // 2. Expandir datos (Unpivot) a formato Embudo para Looker Studio
  var filasEmbudo = [];
  var headers = ["CELULAR", "ETAPA_EMBUDO", "ASESOR", "PROYECTO", "FUENTE", "NOMBRE_OPC", "FECHA_ENTRADA_LEAD", "ULTIMA_FECHA", "ULTIMA_TIPIFICACION"];

  var celulares = Object.keys(leadsMap);
  for (var c = 0; c < celulares.length; c++) {
    var lead = leadsMap[celulares[c]];

    var baseRow = [
      lead.celular,
      "", // Se llena en cada etapa
      lead.asesor,
      lead.proyecto,
      lead.fuente,
      lead.opc,
      lead.fechaEntrada,
      lead.fecha,
      lead.tipif
    ];

    // Etapa 1: Todos cuentan como Leads
    var r1 = baseRow.slice();
    r1[1] = "1. Leads";
    filasEmbudo.push(r1);

    // Etapa 2: Si contestó alguna vez
    if (lead.isContacto) {
      var r2 = baseRow.slice();
      r2[1] = "2. Contacto Efectivo";
      filasEmbudo.push(r2);
    }

    // Etapa 3: Si alguna vez fue potencial
    if (lead.isPotencial) {
      var r3 = baseRow.slice();
      r3[1] = "3. Lead Potencial";
      filasEmbudo.push(r3);
    }

    // Etapa 4: Si agendó cita
    if (lead.isCita) {
      var r4 = baseRow.slice();
      r4[1] = "4. Citas Agendadas";
      filasEmbudo.push(r4);
    }
  }

  // 3. Escribir resultados masivamente en la hoja destino
  targetSheet.clear(); // Limpiar data anterior
  targetSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  targetSheet.getRange(1, 1, 1, headers.length).setBackground("#0f172a").setFontColor("white").setFontWeight("bold");

  if (filasEmbudo.length > 0) {
    targetSheet.getRange(2, 1, filasEmbudo.length, headers.length).setValues(filasEmbudo);
  }

  var t1 = new Date().getTime();
  if (typeof logDebug === "function") logDebug('[EMBUDO] Generados ' + filasEmbudo.length + ' registros en ' + (t1-t0) + 'ms');
}

/**
 * Función auxiliar para crear un Trigger (Activador) que ejecute
 * la actualización del embudo automáticamente cada HORA.
 * Solo necesitas ejecutar esta función UNA VEZ manualmente desde el editor.
 */
function instalarActivadorEmbudo() {
  var functionName = 'generarDataEmbudoLooker';

  // Revisar si ya existe para no duplicarlo
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === functionName) {
      SpreadsheetApp.getUi().alert('El activador automático ya está instalado.');
      return;
    }
  }

  // Crear trigger de 1 hora
  ScriptApp.newTrigger(functionName)
    .timeBased()
    .everyHours(1)
    .create();

  SpreadsheetApp.getUi().alert('✅ Activador instalado. La hoja DATA_EMBUDO_LOOKER se actualizará automáticamente cada hora.');
}

// ==========================================================================
// 11. SINCRONIZAR EDICIONES MANUALES DE ASESORES → DIM + FACT
// ==========================================================================

/**
 * Detecta cambios manuales en hojas de asesores usando FACT como fuente de verdad.
 * Indicadores de cambio: TIPIFICACION y/o COMENTARIO.
 * - TIPIF: compara advisor vs última TIPIF en FACT (cualquier TIPO_ACCION)
 * - COMENTARIO: compara advisor vs último EDICION_MANUAL previo (si existe)
 * Cuando detecta cambio, sincroniza TODOS los campos a DIM + crea EDICION_MANUAL en FACT.
 * Triggers: cada hora (permite matriz día de la semana × hora para contactabilidad).
 */
function sincronizarEdicionesManuales() {
  var t0 = new Date().getTime();
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(120000);
  } catch (e) {
    logError('[SYNC-MANUAL] No se pudo adquirir lock: ' + e.message);
    return;
  }

  try {
    var ss = getMasterSpreadsheet_();
    // Cuando corre por trigger no hay hoja "activa"; el CRM maestro se abre por ID configurado.
    if (!ss) {
      logError('[SYNC-MANUAL] No se pudo abrir la hoja. Ejecuta desde el menú del CRM o vuelve a instalar el activador.');
      try { SpreadsheetApp.getUi().alert('Sync ediciones manuales', 'No se pudo abrir la hoja. Ejecuta desde el menú del CRM (con la hoja abierta) o reinstala el activador.'); } catch (_) {}
      return;
    }
    if (ss) {
      PropertiesService.getDocumentProperties().setProperty('CRM_SPREADSHEET_ID', ss.getId());
    }
    var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
    var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
    if (!dimSheet || !factSheet) {
      logError('[SYNC-MANUAL] Faltan hojas DIM o FACT');
      try { SpreadsheetApp.getUi().alert('Sync ediciones manuales', 'Faltan hojas DIM o FACT (revisa CONFIG_SYSTEM.SHEETS).'); } catch (_) {}
      return;
    }

    var advisors = getActiveAdvisors(ss);
    if (advisors.length === 0) {
      logDebug('[SYNC-MANUAL] Sin asesores activos');
      try { SpreadsheetApp.getUi().alert('Sync ediciones manuales', 'No hay asesores activos en CONFIG. Revisa la hoja de configuración de asesores.'); } catch (_) {}
      return;
    }

    var userEmail = Session.getActiveUser().getEmail() || 'SISTEMA';
    var hoy = new Date();
    var systemSheets = getSystemSheetNames();

    // --- PASO 1: Cargar DIM en Map (CELULAR → datos + rowIndex) ---
    var dimData = dimSheet.getDataRange().getValues();
    var dimMap = {};
    for (var d = 1; d < dimData.length; d++) {
      var dPhone = normalizePhoneETL(dimData[d][1]);
      if (!dPhone || dPhone.length < 9) continue;
      var dr = dimData[d];
      dimMap[dPhone] = {
        row: d + 1,
        rowData: dr.slice ? dr.slice() : dr.map(function(x) { return x; }), // copia de la fila para no leer sheet en el bucle
        id: dr[0],
        nombre: String(dr[2] || '').toUpperCase().trim(),
        origen: dr[4] || '',
        proyecto: String(dr[5] || ''),
        fechaRegistro: dr[6],
        tipif: String(dr[8] || '').toUpperCase().trim(),
        fuenteNorm: String(dr[10] || ''),
        nombreOPC: String(dr[11] || ''),
        eCivil: String(dr[12] || '').toUpperCase().trim(),
        ocupacion: String(dr[13] || '').toUpperCase().trim(),
        pareja: String(dr[14] || '').toUpperCase().trim(),
        distrito: normalizarDistrito(dr[15] || '')
      };
    }
    logDebug('[SYNC-MANUAL] DIM cargado: ' + Object.keys(dimMap).length + ' clientes');

    // --- PASO 2: Referencia desde FACT. Una sola fila por clave (phone|asesor): la MÁS RECIENTE por FECHA. ---
    // Leer hasta la última fila explícita (getDataRange a veces no incluye filas recién añadidas en la ejecución anterior).
    var factLastRow = factSheet.getLastRow();
    var factData = factLastRow >= 2 ? factSheet.getRange(2, 1, factLastRow - 1, 16).getValues() : [];
    var lastByKey = {}; // key -> { fechaMs, tipif, comentario } — último registro de CUALQUIER tipo (para TIPIF)
    var lastEdicionManualByKey = {}; // key -> { fechaMs, comentario } — solo EDICION_MANUAL (para comparar COMENTARIO y evitar falsos cambios vs ASIGNACION/MIGRACION)
    var edicionManualSignatureMap = {};
    var hoyStart = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate()).getTime();
    var hoyEnd = hoyStart + 24 * 60 * 60 * 1000 - 1;
    for (var f = 0; f < factData.length; f++) {
      var fPhone = normalizePhoneETL(factData[f][2]);
      if (!fPhone || fPhone.length < 9) continue;
      var fAsesor = normalizarAsesorParaClave(factData[f][4]);
      var fKey = fPhone + '|' + fAsesor;
      var fecha = factData[f][6];
      var fechaMs = parseFechaFactToMs(fecha);
      if (isNaN(fechaMs)) continue;
      var fTipif = normalizarTipifParaComparacion(factData[f][13]);
      var rawFC = factData[f][14];
      var comentarioStr = normalizarComentarioOperativo_(rawFC);
      if (!lastByKey[fKey] || fechaMs > lastByKey[fKey].fechaMs) {
        lastByKey[fKey] = { fechaMs: fechaMs, tipif: fTipif, comentario: comentarioStr, esHoy: (fechaMs >= hoyStart && fechaMs <= hoyEnd) };
      }
      // Solo EDICION_MANUAL: para comparar comentario contra el último EDICION_MANUAL (evita 1696 falsos por comparar vs ASIGNACION/MIGRACION)
      if (String(factData[f][12] || '').trim() === 'EDICION_MANUAL') {
        edicionManualSignatureMap[buildFirmaEdicionManual_(fKey, fTipif, comentarioStr)] = true;
        if (!lastEdicionManualByKey[fKey] || fechaMs > lastEdicionManualByKey[fKey].fechaMs) {
          lastEdicionManualByKey[fKey] = { fechaMs: fechaMs, comentario: comentarioStr, esHoy: (fechaMs >= hoyStart && fechaMs <= hoyEnd) };
        }
      }
    }
    logDebug('[SYNC-MANUAL] FACT referencia: ' + Object.keys(lastByKey).length + ' claves (phone|asesor), EDICION_MANUAL: ' + Object.keys(lastEdicionManualByKey).length);

    // --- PASO 3: Iterar asesores y detectar cambios ---
    var dimUpdates = [];
    var newFactRows = [];
    var totalRevisados = 0;
    var skippedNoChange = 0;

    for (var a = 0; a < advisors.length; a++) {
      var advName = advisors[a].nombre;
      var advCtx = getAdvisorSheetContext_(advisors[a]);
      var advSheet = advCtx.sheet;
      if (!advSheet) {
        logDebug('[SYNC-MANUAL] Hoja no encontrada para asesor: ' + advName + ' en base ' + advCtx.baseCode);
        continue;
      }
      var sheetRealName = advSheet.getName();
      if (systemSheets.some(function(s) { return s.toUpperCase() === sheetRealName.toUpperCase(); })) continue;

      var headerRow = findHeaderRow(advSheet);
      var lastCol = Math.max(advSheet.getLastColumn(), 16);
      var lastRow = advSheet.getLastRow();
      if (lastRow <= headerRow) continue;

      var headers = advSheet.getRange(headerRow, 1, 1, lastCol).getValues()[0];
      var col = { celular: -1, nombre: -1, tipif: -1, comentario: -1,
                  eCivil: -1, ocupacion: -1, pareja: -1, distrito: -1 };

      for (var h = 0; h < headers.length; h++) {
        var hu = String(headers[h]).toUpperCase().trim();
        // CELULAR: usar columna del CLIENTE (exacto CELULAR/TELEFONO o que no sea CELULAR OPC)
        if (col.celular === -1) {
          if (hu === 'CELULAR' || hu === 'TELEFONO') col.celular = h;
          else if ((hu.indexOf('CELULAR') !== -1 || hu.indexOf('TELEFONO') !== -1) && hu.indexOf('OPC') === -1) col.celular = h;
        }
        if (hu.indexOf('NOMBRE') !== -1 && hu.indexOf('OPC') === -1) col.nombre = h;
        if (hu === 'TIPIF' || hu === 'TIPIFICACION') col.tipif = h;
        // Preferir columna exacta COMENTARIO; si no existe, cualquiera que tenga COMENTARIO y no sea OPC/WSP
        if (hu === 'COMENTARIO') col.comentario = h;
        else if (col.comentario === -1 && hu.indexOf('COMENTARIO') !== -1 && hu.indexOf('OPC') === -1 && hu.indexOf('WSP') === -1) col.comentario = h;
        if (hu.indexOf('CIVIL') !== -1) col.eCivil = h;
        if (hu.indexOf('OCUPACION') !== -1) col.ocupacion = h;
        if (hu.indexOf('PAREJA') !== -1) col.pareja = h;
        if (hu.indexOf('DISTRITO') !== -1) col.distrito = h;
      }
      if (col.celular === -1) continue;

      var dataStart = headerRow + 1;
      var data = advSheet.getRange(dataStart, 1, lastRow - headerRow, lastCol).getValues();

      // Por cada (teléfono, asesor) solo usar la ÚLTIMA fila de la hoja (estado actual). Evita falsos cambios por filas antiguas del historial.
      var lastRowIndexByCompKey = {};
      for (var ri = 0; ri < data.length; ri++) {
        var p = normalizePhoneETL(data[ri][col.celular]);
        if (!p || !dimMap[p]) continue;
        var ck = p + '|' + normalizarAsesorParaClave(sheetRealName);
        lastRowIndexByCompKey[ck] = ri;
      }

      for (var r = 0; r < data.length; r++) {
        var phone = normalizePhoneETL(data[r][col.celular]);
        if (!phone || !dimMap[phone]) continue;
        totalRevisados++;

        var compKey = phone + '|' + normalizarAsesorParaClave(sheetRealName);
        if (lastRowIndexByCompKey[compKey] !== r) continue; // solo comparar la última fila de este (teléfono, asesor)

        var dim = dimMap[phone];
        var advTipifRaw = col.tipif !== -1 ? data[r][col.tipif] : '';
        var advTipif = normalizarTipifParaComparacion(advTipifRaw);
        // Valor efectivo que se escribirá en FACT (hoja vacía → fallback DIM "NC"): usar mismo valor para comparar y evitar duplicados infinitos
        var advTipifEscrito = advTipif || normalizarTipifParaComparacion(dim.tipif) || '';

        var rawComment = col.comentario !== -1 ? data[r][col.comentario] : '';
        var advComentarioNorm = normalizarComentarioOperativo_(rawComment);

        // Sincronizar si la clave YA EXISTE en FACT, o si es la primera vez (asesor nuevo) y la hoja tiene datos (tipif o comentario).
        var ref = lastByKey[compKey];
        if (!ref) {
          // Primera vez (phone|asesor) en FACT: solo insertar si la hoja tiene tipif o comentario (evita filas vacías; permite asesores nuevos como EDITH R.)
          if ((advTipif || '').trim() === '' && (advComentarioNorm || '').trim() === '') {
            skippedNoChange++;
            continue;
          }
          ref = { tipif: '', comentario: '' }; // referencia vacía para que el resto de la lógica inserte una EDICION_MANUAL
        }
        var refEdManual = lastEdicionManualByKey[compKey];
        var refTipif = ref.tipif || '';
        // Comparar COMENTARIO solo contra el último EDICION_MANUAL (evita 1696 falsos vs ASIGNACION/MIGRACION con "Asignación registrada...")
        var comentarioChanged = refEdManual
          ? (advComentarioNorm !== (refEdManual.comentario || ''))
          : (advComentarioNorm !== ''); // si nunca hubo EDICION_MANUAL, solo "cambio" si hay comentario en hoja (una vez)
        var tipifChanged = (advTipifEscrito !== refTipif);
        // refComentarioNorm para la condición "no registrar si no cambió" (Fix 2: sin depender de esHoy)
        var refComentarioNorm = refEdManual ? (refEdManual.comentario || '') : normalizarComentarioOperativo_(ref.comentario || '');

        if (!tipifChanged && !comentarioChanged) {
          skippedNoChange++;
          continue;
        }

        // No crear EDICION_MANUAL si en la hoja no hay ni tipif ni comentario (evita 1800+ filas vacías; en tu CSV 1130 tenían COMENTARIO vacío)
        if ((advTipif || '').trim() === '' && (advComentarioNorm || '').trim() === '') {
          skippedNoChange++;
          continue;
        }

        // No duplicar: si el contenido (tipif+comentario) ya coincide con la ref, no insertar — sin importar si es de hoy o ayer (evita pico cada amanecer)
        if (refTipif === advTipifEscrito && refComentarioNorm === advComentarioNorm) {
          skippedNoChange++;
          continue;
        }

        var firmaEdicionManual = buildFirmaEdicionManual_(compKey, advTipifEscrito, advComentarioNorm);
        if (edicionManualSignatureMap[firmaEdicionManual]) {
          skippedNoChange++;
          continue;
        }

        // --- CAMBIO DETECTADO: sincronizar TODOS los campos ---
        var advNombre = col.nombre !== -1 ? String(data[r][col.nombre] || '').toUpperCase().trim() : '';
        var advECivil = col.eCivil !== -1 ? String(data[r][col.eCivil] || '').toUpperCase().trim() : '';
        var advOcupacion = col.ocupacion !== -1 ? String(data[r][col.ocupacion] || '').toUpperCase().trim() : '';
        var advPareja = col.pareja !== -1 ? String(data[r][col.pareja] || '').toUpperCase().trim() : '';
        var advDistrito = col.distrito !== -1 ? normalizarDistrito(data[r][col.distrito] || '').trim() : '';

        // Actualizar DIM desde copia en memoria (evita 1 lectura al sheet por fila → menos timeout)
        var dimRow = dim.rowData.slice ? dim.rowData.slice() : dim.rowData.map(function(x) { return x; });
        if (advNombre) dimRow[2] = advNombre;
        dimRow[7] = sheetRealName;
        if (advTipif || (advTipifRaw != null && String(advTipifRaw).trim() !== '')) dimRow[8] = (advTipifRaw != null && String(advTipifRaw).trim() !== '') ? String(advTipifRaw).trim() : advTipif;
        dimRow[9] = hoy;
        if (advECivil) dimRow[12] = advECivil;
        if (advOcupacion) dimRow[13] = advOcupacion;
        if (advPareja) dimRow[14] = advPareja;
        if (advDistrito) dimRow[15] = advDistrito;
        dim.rowData = dimRow; // por si el mismo cliente se repite en otro asesor
        dimUpdates.push({ row: dim.row, data: dimRow });

        var motivo = (tipifChanged ? 'TIPIF' : '') + (tipifChanged && comentarioChanged ? '+' : '') + (comentarioChanged ? 'COMENTARIO' : '');

        // Crear fila EDICION_MANUAL en FACT. COMENTARIO siempre como texto (07/02/25, 07/01, etc. no se convierten a fecha).
        var comentarioFact = comentarioParaFACTOperativo_(rawComment);
        newFactRows.push([
          'INT_' + Utilities.getUuid(),
          dim.id,
          phone,
          advNombre || dim.nombre || 'Sin Nombre',
          sheetRealName,
          userEmail,
          hoy,
          dim.fechaRegistro || hoy,
          dim.proyecto,
          dim.origen,
          dim.fuenteNorm,
          dim.nombreOPC,
          'EDICION_MANUAL',
          advTipif || dim.tipif || '',
          comentarioFact,
          JSON.stringify({ sync: 'edicionManual', motivo: motivo, asesor: sheetRealName })
        ]);
        edicionManualSignatureMap[firmaEdicionManual] = true;

        dim.nombre = advNombre || dim.nombre;
        dim.tipif = advTipif || dim.tipif;
        dim.eCivil = advECivil || dim.eCivil;
        dim.ocupacion = advOcupacion || dim.ocupacion;
        dim.pareja = advPareja || dim.pareja;
        dim.distrito = advDistrito || dim.distrito;
      }
    }

    // --- PASO 4: Escribir DIM (deduplicar por fila + bloques consecutivos para reducir tiempo) ---
    if (dimUpdates.length > 0) {
      var byRow = {};
      for (var u = 0; u < dimUpdates.length; u++) {
        byRow[dimUpdates[u].row] = dimUpdates[u].data;
      }
      var rows = Object.keys(byRow).map(Number).sort(function(a, b) { return a - b; });
      var idx = 0;
      while (idx < rows.length) {
        var startRow = rows[idx];
        var block = [byRow[startRow]];
        var j = idx + 1;
        while (j < rows.length && rows[j] === rows[j - 1] + 1) {
          block.push(byRow[rows[j]]);
          j++;
        }
        dimSheet.getRange(startRow, 1, block.length, 16).setValues(block);
        idx = j;
      }
      SpreadsheetApp.flush();
    }

    // --- PASO 5: Append batch a FACT (una sola EDICION_MANUAL por phone|asesor por ejecución) ---
    if (newFactRows.length > 0) {
      var byKey = {};
      for (var ni = 0; ni < newFactRows.length; ni++) {
        var r = newFactRows[ni];
        var key = String(r[2]) + '|' + normalizarAsesorParaClave(r[4]);
        byKey[key] = r;
      }
      newFactRows = Object.keys(byKey).map(function(k) { return byKey[k]; });
      var factLastRow = factSheet.getLastRow();
      var factStartRow = factLastRow + 1;
      var numRows = newFactRows.length;
      ensureSheetCapacity(factSheet, factLastRow + numRows, 16);
      // getRange(row, col, numRows, numCols): 3er parámetro es CANTIDAD de filas, no fila final
      factSheet.getRange(factStartRow, 1, numRows, 16).setValues(newFactRows);
      factSheet.getRange(factStartRow, 7, numRows, 1).setNumberFormat('dd/MM/yyyy HH:mm');
    }

    if (dimUpdates.length > 0 || newFactRows.length > 0) {
      SpreadsheetApp.flush();
      invalidateCache();
    }

    var t1 = new Date().getTime();
    var resumen = '[SYNC-MANUAL] Revisados: ' + totalRevisados +
      ' | Sin cambio: ' + skippedNoChange +
      ' | DIM actualizados: ' + dimUpdates.length +
      ' | FACT insertados: ' + newFactRows.length +
      ' | Tiempo: ' + (t1 - t0) + 'ms';
    logDebug(resumen);

    var msg = 'Sincronización de Ediciones Manuales\n\n' +
      'Asesores revisados: ' + advisors.length + '\n' +
      'Filas analizadas: ' + totalRevisados + '\n' +
      'Sin cambios: ' + skippedNoChange + '\n\n' +
      'DIM actualizados: ' + dimUpdates.length + '\n' +
      'FACT insertados: ' + newFactRows.length + '\n\n' +
      'Tiempo: ' + (t1 - t0) + ' ms';
    try {
      var ui = SpreadsheetApp.getUi();
      if (ui) ui.alert(msg);
    } catch (e) {
      Logger.log('[SYNC-MANUAL] Resumen (alert no disponible): ' + msg);
    }

  } catch (e) {
    logError('[SYNC-MANUAL] Error: ' + e.message);
    try {
      SpreadsheetApp.getUi().alert('Error en sincronización manual: ' + e.message);
    } catch (_) {}
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

/**
 * Instala 1 trigger que ejecuta sincronizarEdicionesManuales cada hora cerca de los :55.
 * nearMinute(55) hace que corra aproximadamente a las XX:55 (±15 min). Sin esto, Google
 * usa un minuto aleatorio y el sync no corre cuando esperas.
 */
function instalarTriggerEdicionesManuales() {
  var functionName = 'sincronizarEdicionesManuales';
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = triggers.length - 1; i >= 0; i--) {
    if (triggers[i].getHandlerFunction() === functionName) {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
  ScriptApp.newTrigger(functionName)
    .timeBased()
    .everyHours(1)
    .nearMinute(55)
    .create();
  // Guardar ID para que el trigger pueda abrir la hoja cuando no hay "activa"
  var ss = getMasterSpreadsheet_();
  if (ss) {
    PropertiesService.getDocumentProperties().setProperty('CRM_SPREADSHEET_ID', ss.getId());
  }
  var msg = '✅ Activador instalado: sync ediciones manuales cada hora cerca de los :55 (XX:55 ±15 min).';
  msg += '\n\n🕐 Revisa en Extensión → Apps Script → Proyecto activos que aparezca el trigger.';
  msg += '\n\n📌 Prueba desde el menú CRM → "Sincronizar ediciones manuales → FACT" para ver el resultado sin esperar al trigger.';
  SpreadsheetApp.getUi().alert(msg);
}

/**
 * DIAGNÓSTICO: replica la lógica del sync SIN escribir. Muestra por qué compKey
 * no encuentra match en FACT (clave distinta, teléfono, asesor). Ejecutar desde menú CRM.
 */
function diagnosticarSyncEdicionManual() {
  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  if (!dimSheet || !factSheet) {
    SpreadsheetApp.getUi().alert('Faltan hojas DIM o FACT.');
    return;
  }
  var advisors = getActiveAdvisors(ss);
  if (advisors.length === 0) {
    SpreadsheetApp.getUi().alert('No hay asesores activos en CONFIG.');
    return;
  }
  var systemSheets = getSystemSheetNames();

  // DIM: solo necesitamos saber qué phones existen
  var dimData = dimSheet.getDataRange().getValues();
  var dimPhones = {};
  for (var d = 1; d < dimData.length; d++) {
    var dPhone = normalizePhoneETL(dimData[d][1]);
    if (dPhone && dPhone.length >= 9) dimPhones[dPhone] = true;
  }

  // FACT: misma clave que el sync (normalizarAsesorParaClave)
  var factData = factSheet.getDataRange().getValues();
  var lastTipifMap = {};
  var lastEdicionMap = {};
  var phoneToAsesoresEnFACT = {};
  for (var f = 1; f < factData.length; f++) {
    var fPhone = normalizePhoneETL(factData[f][2]);
    if (!fPhone || fPhone.length < 9) continue;
    var fAsesor = normalizarAsesorParaClave(factData[f][4]);
    var fKey = fPhone + '|' + fAsesor;
    var fTipif = normalizarTipifParaComparacion(factData[f][13]);
    if (fTipif) lastTipifMap[fKey] = fTipif;
    if (String(factData[f][12]) === 'EDICION_MANUAL') {
      var rawFC = factData[f][14];
      lastEdicionMap[fKey] = normalizarComentarioOperativo_(rawFC);
    }
    if (!phoneToAsesoresEnFACT[fPhone]) phoneToAsesoresEnFACT[fPhone] = {};
    phoneToAsesoresEnFACT[fPhone][fAsesor] = true;
  }

  var totalRevisados = 0;
  var conClaveEnTipif = 0;
  var sinClaveEnTipif = 0;
  var conClaveEnEdicion = 0;
  var sinClaveEnEdicion = 0;
  var ejemplosSinMatch = [];
  var maxEjemplos = 20;

  for (var a = 0; a < advisors.length; a++) {
    var advCtx = getAdvisorSheetContext_(advisors[a]);
    var advSheet = advCtx.sheet;
    if (!advSheet) continue;
    var sheetRealName = advSheet.getName();
    if (systemSheets.some(function(s) { return s.toUpperCase() === sheetRealName.toUpperCase(); })) continue;
    var headerRow = findHeaderRow(advSheet);
    var lastCol = Math.max(advSheet.getLastColumn(), 16);
    var lastRow = advSheet.getLastRow();
    if (lastRow <= headerRow) continue;

    var headers = advSheet.getRange(headerRow, 1, 1, lastCol).getValues()[0];
    var colCelular = -1;
    for (var h = 0; h < headers.length; h++) {
      var hu = String(headers[h]).toUpperCase().trim();
      if (colCelular === -1) {
        if (hu === 'CELULAR' || hu === 'TELEFONO') colCelular = h;
        else if ((hu.indexOf('CELULAR') !== -1 || hu.indexOf('TELEFONO') !== -1) && hu.indexOf('OPC') === -1) colCelular = h;
      }
    }
    if (colCelular === -1) continue;

    var data = advSheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol).getValues();
    for (var r = 0; r < data.length; r++) {
      var rawCelular = data[r][colCelular];
      var phone = normalizePhoneETL(rawCelular);
      if (!phone || !dimPhones[phone]) continue;
      totalRevisados++;
      var compKey = phone + '|' + normalizarAsesorParaClave(sheetRealName);
      var estaEnTipif = lastTipifMap.hasOwnProperty(compKey);
      var estaEnEdicion = lastEdicionMap.hasOwnProperty(compKey);
      if (estaEnTipif) conClaveEnTipif++; else sinClaveEnTipif++;
      if (estaEnEdicion) conClaveEnEdicion++; else sinClaveEnEdicion++;

      if (!estaEnTipif && ejemplosSinMatch.length < maxEjemplos) {
        var asesoresFACT = phoneToAsesoresEnFACT[phone] ? Object.keys(phoneToAsesoresEnFACT[phone]).sort().join(', ') : '(ninguno)';
        ejemplosSinMatch.push({
          compKey: compKey,
          phone: phone,
          phoneLen: (phone && phone.length) || 0,
          rawCelular: String(rawCelular).substring(0, 20),
          sheetRealName: sheetRealName,
          asesoresEnFACT: asesoresFACT
        });
      }
    }
  }

  var lineas = [
    '--- DIAGNÓSTICO SYNC EDICION_MANUAL (sin escribir nada) ---',
    'Filas revisadas (en DIM y en alguna hoja asesor): ' + totalRevisados,
    'compKey SÍ está en lastTipifMap (FACT): ' + conClaveEnTipif,
    'compKey NO está en lastTipifMap: ' + sinClaveEnTipif + ' ← estas dispararían EDICION_MANUAL',
    'compKey SÍ está en lastEdicionMap: ' + conClaveEnEdicion,
    'compKey NO está en lastEdicionMap: ' + sinClaveEnEdicion,
    '',
    'Total claves en lastTipifMap (FACT): ' + Object.keys(lastTipifMap).length,
    'Total claves en lastEdicionMap: ' + Object.keys(lastEdicionMap).length,
    '',
    '--- Ejemplos donde compKey NO está en lastTipifMap (primeros ' + ejemplosSinMatch.length + ') ---'
  ];
  for (var i = 0; i < ejemplosSinMatch.length; i++) {
    var e = ejemplosSinMatch[i];
    lineas.push('compKey="' + e.compKey + '" | phone="' + e.phone + '" (len=' + e.phoneLen + ') | rawCelular="' + e.rawCelular + '" | hoja="' + e.sheetRealName + '" | Para este phone en FACT asesores: ' + e.asesoresEnFACT);
  }
  var texto = lineas.join('\n');
  Logger.log(texto);
  try {
    var logSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.LOG);
    if (logSheet) {
      logSheet.appendRow([new Date(), 'DIAG-SYNC-EDICION', texto.replace(/\n/g, ' | ')]);
    }
  } catch (_) {}
  SpreadsheetApp.getUi().alert('Diagnóstico terminado.\n\nRevisa:\n1) Ver → Registros de ejecución (Logger)\n2) Hoja ' + (CONFIG_SYSTEM.SHEETS.LOG || 'LOG_SISTEMA') + ' (última fila)\n\nResumen: ' + sinClaveEnTipif + ' compKeys NO están en FACT → por eso se generan tantas EDICION_MANUAL.\nEjemplos en el log.');
}

/**
 * EJECUTAR UNA VEZ: Genera ASIGNACION_MANUAL en FACT para leads que no la tienen.
 * Lee FECHA HOY de cada hoja de asesor como FECHA_INTERACCION (día real de asignación).
 */
function generarAsignacionManualFaltantes() {
  var ss = getMasterSpreadsheet_();
  var dimSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.DIM);
  var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
  if (!dimSheet || !factSheet) { SpreadsheetApp.getUi().alert('No se encontró DIM o FACT'); return; }

  // PASO 1: Identificar phones que YA tienen ASIGNACION_MANUAL en FACT
  var factData = factSheet.getDataRange().getValues();
  var phonesConAsignacion = {};
  for (var f = 1; f < factData.length; f++) {
    if (String(factData[f][12] || '').trim() === 'ASIGNACION_MANUAL') {
      var fp = normalizePhoneETL(factData[f][2]);
      if (fp) phonesConAsignacion[fp] = true;
    }
  }

  // PASO 2: Cargar DIM como referencia (phone → datos del cliente)
  var dimData = dimSheet.getDataRange().getValues();
  var dimMap = {};
  for (var d = 1; d < dimData.length; d++) {
    var dp = normalizePhoneETL(dimData[d][1]);
    if (!dp) continue;
    dimMap[dp] = {
      id: dimData[d][0],
      nombre: String(dimData[d][2] || ''),
      proyecto: String(dimData[d][5] || ''),
      fechaRegistro: dimData[d][6],
      origen: dimData[d][4] || '',
      fuenteNorm: String(dimData[d][10] || ''),
      nombreOPC: String(dimData[d][11] || '')
    };
  }

  // PASO 3: Recorrer hojas de asesores y leer FECHA HOY + CELULAR
  var advisors = getActiveAdvisors(ss);
  var systemSheets = getSystemSheetNames();
  var userEmail = Session.getActiveUser().getEmail() || 'SISTEMA';
  var newRows = [];
  var processed = {};

  for (var a = 0; a < advisors.length; a++) {
    var advCtx = getAdvisorSheetContext_(advisors[a]);
    var advSheet = advCtx.sheet;
    if (!advSheet) continue;
    var sheetName = advSheet.getName();
    if (systemSheets.some(function(s) { return s.toUpperCase() === sheetName.toUpperCase(); })) continue;

    var headerRow = findHeaderRow(advSheet);
    var lastCol = Math.max(advSheet.getLastColumn(), 16);
    var lastRow = advSheet.getLastRow();
    if (lastRow <= headerRow) continue;

    var headers = advSheet.getRange(headerRow, 1, 1, lastCol).getValues()[0];
    var colCelular = -1, colFechaHoy = -1, colNombre = -1, colFRegistro = -1;
    for (var h = 0; h < headers.length; h++) {
      var hu = String(headers[h]).toUpperCase().trim();
      if (hu.indexOf('CELULAR') !== -1 || hu.indexOf('TELEFONO') !== -1) colCelular = h;
      if (hu === 'FECHA HOY' || hu === 'FECHA_HOY') colFechaHoy = h;
      if (hu.indexOf('NOMBRE') !== -1 && hu.indexOf('OPC') === -1) colNombre = h;
      if (hu.indexOf('REGISTRO') !== -1) colFRegistro = h;
    }
    if (colCelular === -1 || colFechaHoy === -1) continue;

    var data = advSheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol).getValues();

    for (var r = 0; r < data.length; r++) {
      var phone = normalizePhoneETL(data[r][colCelular]);
      if (!phone || phonesConAsignacion[phone] || processed[phone]) continue;

      var fechaHoy = data[r][colFechaHoy];
      if (!fechaHoy) continue;

      var dim = dimMap[phone];
      if (!dim) continue;

      processed[phone] = true;
      var nombre = colNombre !== -1 ? String(data[r][colNombre] || '').toUpperCase().trim() : '';
      var fRegistro = colFRegistro !== -1 ? data[r][colFRegistro] : fechaHoy;

      newRows.push([
        'INT_' + Utilities.getUuid(),
        dim.id,
        phone,
        nombre || dim.nombre || 'Sin Nombre',
        sheetName,
        userEmail,
        fechaHoy,
        fRegistro || fechaHoy,
        dim.proyecto,
        dim.origen,
        dim.fuenteNorm,
        dim.nombreOPC,
        'ASIGNACION_MANUAL',
        '',
        'Asignación registrada retroactivamente',
        JSON.stringify({ sync: 'retroAsignacion', asesor: sheetName })
      ]);
    }
  }

  if (newRows.length === 0) {
    SpreadsheetApp.getUi().alert('Todos los leads ya tienen ASIGNACION_MANUAL. No hay cambios.');
    return;
  }

  var factLastRow = factSheet.getLastRow();
  ensureSheetCapacity(factSheet, factLastRow + newRows.length, 16);
  factSheet.getRange(factLastRow + 1, 1, newRows.length, 16).setValues(newRows);
  SpreadsheetApp.flush();

  SpreadsheetApp.getUi().alert(
    'Asignaciones retroactivas generadas',
    '✅ ' + newRows.length + ' entradas ASIGNACION_MANUAL creadas en FACT.\n\n' +
    'Se usó la columna FECHA HOY de cada hoja de asesor como fecha de asignación.\n' +
    'Ahora puedes filtrar por FECHA_INTERACCION en Looker Studio.',
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

/**
 * One-shot: elimina TODAS las filas EDICION_MANUAL cuya FECHA_INTERACCION es el 04/03/2026 a las 19:50 (cualquier segundo).
 * Cubre "04/03/2026 19:50:15" y "4/03/2026 19:50" por igual (fecha/hora parseada, no texto).
 * Crear respaldo manual antes (o usar el que crea esta función: BK_FACT_yyyyMMdd_HHmmss).
 * Orden obligatorio: 1) Esta función (Paso 1), 2) limpiarDuplicadosEdicionManualFACT_OneShot (Paso 2).
 */
function eliminarPicoEdicionManualPorTimestamp_OneShot() {
  var lock = LockService.getScriptLock();
  try { lock.waitLock(120000); } catch (e) {
    SpreadsheetApp.getUi().alert('No se pudo obtener lock: ' + e.message);
    return;
  }
  try {
    var ss = getMasterSpreadsheet_();
    var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
    if (!factSheet) {
      SpreadsheetApp.getUi().alert('No se encontro la hoja FACT_INTERACCIONES');
      return;
    }
    var lastRow = factSheet.getLastRow();
    var lastCol = Math.max(factSheet.getLastColumn(), 16);
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('FACT no tiene filas.');
      return;
    }
    var stamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
    var backup = factSheet.copyTo(ss);
    backup.setName('BK_FACT_' + stamp);
    var values = factSheet.getRange(1, 1, lastRow, lastCol).getValues();
    var output = [values[0]];
    var removed = 0;
    // Pico a eliminar: 4 de marzo 2026, 19:50 (cualquier segundo) — detectado por fecha parseada
    var picoAnio = 2026;
    var picoMes = 2;   // marzo = 2 (0-based)
    var picoDia = 4;
    var picoHora = 19;
    var picoMin = 50;
    for (var i = 1; i < values.length; i++) {
      var row = values[i];
      var tipo = String(row[12] || '').trim();
      if (tipo !== 'EDICION_MANUAL') {
        output.push(row);
        continue;
      }
      var fechaMs = parseFechaFactToMs(row[6]);
      if (isNaN(fechaMs)) {
        output.push(row);
        continue;
      }
      var d = new Date(fechaMs);
      if (d.getFullYear() === picoAnio && d.getMonth() === picoMes && d.getDate() === picoDia &&
          d.getHours() === picoHora && d.getMinutes() === picoMin) {
        removed++;
        continue;
      }
      output.push(row);
    }
    factSheet.getRange(2, 1, lastRow - 1, lastCol).clearContent();
    factSheet.getRange(1, 1, output.length, lastCol).setValues(output);
    SpreadsheetApp.flush();
    SpreadsheetApp.getUi().alert(
      'Pico eliminado',
      'Respaldo: BK_FACT_' + stamp + '\nFilas EDICION_MANUAL eliminadas (04/03/2026 19:50): ' + removed + '\nFilas restantes en FACT: ' + (output.length - 1),
      SpreadsheetApp.getUi().ButtonSet.OK
    );
  } catch (e2) {
    SpreadsheetApp.getUi().alert('Error: ' + e2.message);
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

/**
 * Limpia duplicados de EDICION_MANUAL en FACT_INTERACCIONES.
 * Regla: ASESOR + CELULAR + TIPIF + COMENTARIO (normalizado) + METADATA iguales = duplicado.
 * Se conserva la fila MÁS RECIENTE por FECHA_INTERACCION; el resto se elimina.
 * COMENTARIO se normaliza con normalizarComentarioOperativo_ (8/1 = 08/01/2026; 30/12/1899 = vacio).
 * Antes de limpiar crea copia de respaldo (BK_FACT_yyyyMMdd_HHmmss).
 */
function limpiarDuplicadosEdicionManualFACT_OneShot() {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(120000);
  } catch (e) {
    SpreadsheetApp.getUi().alert('No se pudo obtener lock: ' + e.message);
    return;
  }

  try {
    var ss = getMasterSpreadsheet_();
    var factSheet = ss.getSheetByName(CONFIG_SYSTEM.SHEETS.FACT);
    if (!factSheet) {
      SpreadsheetApp.getUi().alert('No se encontro la hoja FACT_INTERACCIONES');
      return;
    }

    var lastRow = factSheet.getLastRow();
    var lastCol = Math.max(factSheet.getLastColumn(), 16);
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('FACT no tiene filas para limpiar.');
      return;
    }

    // 1) Respaldo automatico
    var stamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
    var backupName = 'BK_FACT_' + stamp;
    var backup = factSheet.copyTo(ss);
    backup.setName(backupName);

    // 2) Cargar data. Clave duplicado = ASESOR + CELULAR + TIPIF + COMENTARIO (normalizado) + METADATA.
    //    Por cada clave, conservar la fila con FECHA_INTERACCION más reciente.
    var values = factSheet.getRange(1, 1, lastRow, lastCol).getValues();
    var bestIdxByKey = {}; // key -> índice de la fila con fecha más reciente

    for (var i = 1; i < values.length; i++) {
      var row = values[i];
      var tipo = String(row[12] || '').trim();
      if (tipo !== 'EDICION_MANUAL') continue;

      var phone = normalizePhoneETL(row[2]);
      var asesor = normalizarAsesorParaClave(row[4]);
      var tipif = normalizarTipifParaComparacion(row[13]);
      var comentarioNorm = normalizarComentarioOperativo_(row[14]);
      var metadata = String(row[15] || '').trim();
      var key = [phone, asesor, tipif, comentarioNorm, metadata].join('|');

      var fecha = row[6];
      var fechaMs = parseFechaFactToMs(fecha);
      if (isNaN(fechaMs)) fechaMs = 0;
      if (!bestIdxByKey[key] || fechaMs > (bestIdxByKey[key].fechaMs || 0)) {
        bestIdxByKey[key] = { idx: i, fechaMs: fechaMs };
      }
    }

    // 3) Reconstruir FACT: no-EDICION_MANUAL se mantienen; EDICION_MANUAL solo la elegida por clave
    var output = [values[0]];
    var removed = 0;
    var keptEdicionManual = 0;

    for (var j = 1; j < values.length; j++) {
      var r = values[j];
      var t = String(r[12] || '').trim();

      if (t !== 'EDICION_MANUAL') {
        output.push(r);
        continue;
      }

      var p = normalizePhoneETL(r[2]);
      var a = normalizarAsesorParaClave(r[4]);
      var tf = normalizarTipifParaComparacion(r[13]);
      var comNorm = normalizarComentarioOperativo_(r[14]);
      var meta = String(r[15] || '').trim();
      var k = [p, a, tf, comNorm, meta].join('|');

      if (bestIdxByKey[k] && bestIdxByKey[k].idx === j) {
        output.push(r);
        keptEdicionManual++;
      } else {
        removed++;
      }
    }

    // 4) Escribir: borrar desde fila 2 hasta lastRow (inclusive), luego escribir output
    factSheet.getRange(2, 1, lastRow - 1, lastCol).clearContent();
    factSheet.getRange(1, 1, output.length, lastCol).setValues(output);
    SpreadsheetApp.flush();

    SpreadsheetApp.getUi().alert(
      'Limpieza EDICION_MANUAL completada',
      'Respaldo: ' + backupName + '\n' +
      'Filas eliminadas: ' + removed + '\n' +
      'EDICION_MANUAL conservadas: ' + keptEdicionManual + '\n' +
      'Filas finales en FACT: ' + (output.length - 1),
      SpreadsheetApp.getUi().ButtonSet.OK
    );
  } catch (e2) {
    SpreadsheetApp.getUi().alert('Error en limpieza: ' + e2.message);
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}
