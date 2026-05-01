// === SIGNALEX CORE SHARED HELPERS ===

// ── Severity badge (used by VMS and Pharma card renders)
const sevBadge = s => `<span class="badge badge-${s}">${s.toUpperCase()}</span>`;

// ── DI/CAPA keyword constant + test
// ── Noise filter
// ── Unified citation filter — single source of truth for VMS Evidence + all Pharma tabs
const DI_CAPA_KWS = ['computerised systems validation','data integrity','deviation management','audit trail','capa','csv'];

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
const _ENFORCEMENT_SOURCE_TYPES = new Set(['warning_letter','inspection_finding','recall','import_alert','safety_alert','483']);
const _ENFORCEMENT_JUNK_PHRASES = ['subscribe to','press release','general information','to the medium','newsletter','comics','more about'];
const _ENFORCEMENT_ACTION_VERBS = ['warning','inspection','recall','violation','alert','detention','contamination','adulterat','mislabel','misbrand','defect','unsafe','prohibited','unapproved','finding','enforcement','non-compliance','gmp','cgmp','gdp','import'];

function isValidEnforcementItem(c) {
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
  const auth      = o.auth      !== undefined ? o.auth      : 'all';
  const sev       = o.sev       !== undefined ? o.sev       : 'all';
  const srctype   = o.srctype   !== undefined ? o.srctype   : 'all';
  const factype   = o.factype   !== undefined ? o.factype   : 'all';
  const catFilter = o.catFilter !== undefined ? o.catFilter : null;
  const dicapa    = o.dicapa    || false;
  const company   = o.company   || '';
  const query     = o.query     || '';
  const queryFields = o.queryFields || null;
  const dateFrom  = o.dateFrom  || '';
  const dateTo    = o.dateTo    || '';
  const sortCol   = o.sortCol   || 'date';
  const sortDir   = o.sortDir   !== undefined ? o.sortDir   : -1;
  return CITATIONS.filter(c => {
    if (noiseFilter && isLowValueContent(c)) return false;
    if (noiseFilter && !isValidEnforcementItem(c)) return false;
    if (auth !== 'all' && c.authority !== auth) return false;
    if (sev !== 'all' && (c.severity||'').toLowerCase() !== sev) return false;
    if (srctype !== 'all' && (c.source_type||'') !== srctype) return false;
    if (factype !== 'all' && (c.facility_type||'') !== factype) return false;
    if (catFilter && (c.category||'Other') !== catFilter) return false;
    if (dicapa && !isDiCapa(c)) return false;
    if (company) {
      const q = company.toLowerCase();
      if (!((c.company||'').toLowerCase().includes(q)||(c.facility_type||'').toLowerCase().includes(q))) return false;
    }
    if (query) {
      const q = query.toLowerCase();
      const hay = queryFields
        ? queryFields.map(f => c[f]||'').join(' ').toLowerCase()
        : [c.summary,c.category,c.company,c.authority,c.source_type,c.facility_type,c.violation_details].filter(Boolean).join(' ').toLowerCase();
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
