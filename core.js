// === SIGNALEX CORE SHARED HELPERS ===

// ── Severity badge (used by VMS and Pharma card renders)
const sevBadge = s => `<span class="badge badge-${s}">${s.toUpperCase()}</span>`;

// ── DI/CAPA keyword constant + test
// ── Noise filter
// ── Unified citation filter — single source of truth for VMS Evidence + all Pharma tabs
const DI_CAPA_KWS = ['computerised systems validation','data integrity','deviation management','audit trail','capa','csv'];

// ── Clean signals — ingredient-valid only ────────────────────────────────────
// Use for all ingredient aggregations; never use raw SIGNALS for ingredient math.
const _INVALID_ING_SET = new Set(['none','n/a','na','unknown','-','–','—','other','tbd','tba','various','multiple']);
function getCleanSignals() {
  return SIGNALS.filter(s => {
    const ing = (s.ingredient_name || '').trim().toLowerCase();
    if (!ing || ing.length < 3) return false;
    if (_INVALID_ING_SET.has(ing)) return false;
    if (!/[a-z]/.test(ing)) return false;
    return true;
  });
}

// ── Pharma citation summary family — maps boilerplate text to a canonical code ──
function getPharmaSummaryFamily(text) {
  const t = text || '';
  if (/family smoking prevention|tobacco control act/i.test(t)) return 'tobacco_compliance';
  if (/cgmp/i.test(t) && /adulterat/i.test(t)) return 'cgmp_adulterated';
  if (/adulterated.*misbranded|misbranded.*adulterated/i.test(t)) return 'adulterated_misbranded';
  if (/adulterated/i.test(t) && !(/cgmp/i.test(t))) return 'adulterated_misbranded';
  if (/unapproved new drug/i.test(t)) return 'unapproved_new_drug';
  if (/foreign supplier verification|fsvp/i.test(t)) return 'fsvp';
  if (/import alert|detention without physical examination/i.test(t)) return 'import_alert_dwpe';
  return null;
}

const PHARMA_FAMILY_LABELS = {
  tobacco_compliance:    'Tobacco compliance / adulterated or misbranded product',
  cgmp_adulterated:      'cGMP compliance issue / adulterated product',
  adulterated_misbranded:'Adulterated or misbranded product',
  unapproved_new_drug:   'Unapproved new drug / misbranding',
  fsvp:                  'Foreign Supplier Verification Program issue',
  import_alert_dwpe:     'Import alert / detention without physical examination',
};

// Builds a stable dedupe key for pharma citation display grouping.
// Does NOT mutate the raw dataset — used only in filteredCits().
function normalisePharmaCitationKey(c) {
  const rawText = (c.summary || '') + ' ' + (c.violation_details || '');
  const family = getPharmaSummaryFamily(rawText);
  const summaryPart = family || (c.summary || '').trim().toLowerCase().slice(0, 100);
  return [
    c.authority   || '',
    c.source_type || '',
    c.category    || '',
    (c.company    || '').toLowerCase(),
    summaryPart,
  ].join('|');
}

// Legacy alias kept so nothing breaks if still referenced anywhere
function cleanSummary(text) {
  const family = getPharmaSummaryFamily(text || '');
  return family ? PHARMA_FAMILY_LABELS[family] : (text || '').slice(0, 120);
}

// ── DI/CAPA keyword test — single source of truth ────────────────────────────
function isDiCapa(c) {
  const hay = [(c.category||''),(c.summary||''),(c.violation_details||'')].join(' ').toLowerCase();
  return DI_CAPA_KWS.some(kw => hay.includes(kw));
}

// ── Noise / low-value content filter ────────────────────────────────────────
function isLowValueContent(item) {
  const noisePatterns = ['about','more information','subscribe','report','publication',
    'resource','education','conference','opinion','communication'];
  const text = ((item.title||'')+' '+(item.summary||'')).toLowerCase();
  return noisePatterns.some(p => text.includes(p));
}

// ── Enforcement quality filter — rejects nav pages and non-enforcement citations ─
const _ENFORCEMENT_SOURCE_TYPES = new Set(['warning_letter','inspection_finding','recall','import_alert','safety_alert','483','compliance_action','drug_enforcement','device_enforcement','food_enforcement']);
// Non-enforcement source types — guidance/scientific content, not enforcement findings
const _NON_ENFORCEMENT_SOURCE_TYPES = new Set(['scientific_opinion','guidance','regulatory_update','consultation']);
const _ENFORCEMENT_JUNK_PHRASES = ['subscribe to','press release','general information','to the medium','newsletter','comics','more about'];
const _ENFORCEMENT_ACTION_VERBS = ['warning','inspection','recall','violation','alert','detention','contamination','adulterat','mislabel','misbrand','defect','unsafe','prohibited','unapproved','finding','enforcement','non-compliance','gmp','cgmp','gdp','import'];

function isValidEnforcementItem(c) {
  // Reject guidance/scientific records — they are not enforcement findings
  if (_NON_ENFORCEMENT_SOURCE_TYPES.has(c.source_type||'')) return false;
  if (!_ENFORCEMENT_SOURCE_TYPES.has(c.source_type||'')) return false;
  const summary = (c.summary||'').trim();
  const text = (summary + ' ' + (c.category||'')).toLowerCase();
  // Reject exact junk matches (About, Subscribe, etc.)
  const JUNK_EXACT = new Set(['about','comics','subscribe','newsletter','publications','resources','other']);
  if (JUNK_EXACT.has(summary.toLowerCase())) return false;
  // Reject summaries that start with junk phrases
  if (_ENFORCEMENT_JUNK_PHRASES.some(p => text.startsWith(p))) return false;
  // Reject very short summaries with no enforcement context
  const words = summary.split(/\s+/).filter(Boolean);
  if (words.length < 5 && !_ENFORCEMENT_ACTION_VERBS.some(v => text.includes(v))) return false;
  return true;
}

// ── Unified citation filter — single source of truth for both VMS Evidence
//    and all Pharma tabs. Pass noiseFilter:false to skip isLowValueContent.
function unifiedFilteredCitations(opts) {
  const o = opts || {};
  const noiseFilter = o.noiseFilter !== false;
  const auth        = o.auth      !== undefined ? o.auth      : 'all';
  const sev         = o.sev       !== undefined ? o.sev       : 'all';
  const srctype     = o.srctype   !== undefined ? o.srctype   : 'all';
  const factype     = o.factype   !== undefined ? o.factype   : 'all';
  const catFilter   = o.catFilter !== undefined ? o.catFilter : null;
  const dicapa      = o.dicapa    || false;
  const company     = o.company   || '';
  const query       = o.query     || '';
  const queryFields = o.queryFields || null;
  const dateFrom    = o.dateFrom  || '';
  const dateTo      = o.dateTo    || '';
  const sortCol     = o.sortCol   || 'date';
  const sortDir     = o.sortDir   !== undefined ? o.sortDir   : -1;
  // New decision-model filters
  const priority    = o.priority  || 'all';   // 'all'|'P1'|'P2'|'P3'|'P4'|array
  const failureMode = o.failureMode || '';
  // Use JSON-loaded Pharma citations when available; fall back to embedded array.
  return (window.PHARMA_CITATIONS !== null && window.PHARMA_CITATIONS !== undefined
    ? window.PHARMA_CITATIONS : CITATIONS).filter(c => {
    if (noiseFilter && isLowValueContent(c)) return false;
    if (noiseFilter && !isValidEnforcementItem(c)) return false;
    if (noiseFilter && c.is_noise) return false;
    if (auth !== 'all' && c.authority !== auth) return false;
    if (sev !== 'all' && (c.severity||'').toLowerCase() !== sev) return false;
    if (srctype !== 'all' && (c.source_type||'') !== srctype) return false;
    if (factype !== 'all' && (c.facility_type||'') !== factype) return false;
    // catFilter matches either legacy category or primary_gmp_category
    if (catFilter) {
      const legacyCat = c.category || 'Other';
      const newCat    = c.primary_gmp_category || '';
      if (legacyCat !== catFilter && newCat !== catFilter) return false;
    }
    if (dicapa && !isDiCapa(c)) return false;
    if (company) {
      const q = company.toLowerCase();
      const hay = [(c.company||''),(c.cluster_label||''),(c.facility_type||'')].join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }
    if (priority !== 'all') {
      const pArr = Array.isArray(priority) ? priority : [priority];
      if (!pArr.includes(c.priority||'')) return false;
    }
    if (failureMode && (c.failure_mode||'') !== failureMode) return false;
    if (query) {
      const q = query.toLowerCase();
      const hay = queryFields
        ? queryFields.map(f => c[f]||'').join(' ').toLowerCase()
        : [c.summary,c.category,c.primary_gmp_category,c.failure_mode,c.company,c.authority,c.source_type,c.facility_type,c.violation_details,c.decision_summary].filter(Boolean).join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }
    if (dateFrom && c.date && c.date < dateFrom) return false;
    if (dateTo && c.date && c.date > dateTo) return false;
    return true;
  }).sort((a, b) => {
    const va = a[sortCol]||'', vb = b[sortCol]||'';
    return va < vb ? sortDir : va > vb ? -sortDir : 0;
  });
}
