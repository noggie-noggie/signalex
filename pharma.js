// === SIGNALEX PHARMA HELPERS ===

// ── Pharma citation source of truth ──────────────────────────────────────────
// window.PHARMA_CITATIONS is set by loadPharmaCitations() after async JSON load.
// null  = not yet loaded or load failed → getPharmaCitations() falls back to
//         the embedded window.CITATIONS array (offline / local-file use).
// [...] = JSON-loaded array → used for all Pharma rendering.
window.PHARMA_CITATIONS  = null;
window.PHARMA_META       = null;
window.PHARMA_DATA_SOURCE = null;

// Returns the authoritative Pharma citation array.
// Prefer JSON-loaded data; fall back to embedded CITATIONS.
function getPharmaCitations() {
  return (window.PHARMA_CITATIONS !== null && window.PHARMA_CITATIONS !== undefined)
    ? window.PHARMA_CITATIONS
    : CITATIONS;
}

// Debug helper — callable from the browser console.
window.signalexDataDebug = function() {
  return {
    pharmaDataSource:      window.PHARMA_DATA_SOURCE || null,
    pharmaCitationCount:   Array.isArray(window.PHARMA_CITATIONS) ? window.PHARMA_CITATIONS.length : null,
    embeddedCitationCount: typeof CITATIONS !== 'undefined' && Array.isArray(CITATIONS) ? CITATIONS.length : null,
    meta:                  window.SIGNALEX_META || null,
  };
};

// Async loader — tries reports/citation_database.json first (live deploy path),
// then ./reports/ relative variant, then data/, then falls back to embedded CITATIONS.
// Sets window.PHARMA_CITATIONS, window.PHARMA_DATA_SOURCE, and window.PHARMA_META.
async function loadPharmaCitations() {
  const PATHS = [
    { path: 'reports/citation_database.json',   source: 'json:reports'  },
    { path: './reports/citation_database.json',  source: 'json:reports'  },
    { path: 'data/citation_database.json',       source: 'json:data'     },
  ];
  for (const { path, source } of PATHS) {
    try {
      const resp = await fetch(path);
      if (!resp.ok) continue;
      const raw  = await resp.json();
      const cits = Array.isArray(raw) ? raw : (raw.citations || []);
      const meta = Array.isArray(raw) ? {} : (({ citations: _, ...rest }) => rest)(raw);
      window.PHARMA_CITATIONS  = cits;
      window.PHARMA_DATA_SOURCE = source;
      window.PHARMA_META = {
        ...SIGNALEX_META,
        ...meta,
        citationCount:    cits.length,
        pharmaDataSource: source,
      };
      if (window.SIGNALEX_DEBUG) console.log('[Signalex] Pharma data loaded', {
        source, count: cits.length,
        generatedAt:      meta.generated_at || '',
        citationsSha256:  meta.citations_sha256 || SIGNALEX_META.citationsSha256 || '',
      });
      return { citations: cits, meta: window.PHARMA_META, source };
    } catch (e) {
      if (window.SIGNALEX_DEBUG) console.warn('[Signalex] Could not load', path, e.message);
    }
  }
  // All JSON paths failed — fall back to embedded CITATIONS
  window.PHARMA_CITATIONS  = null;
  window.PHARMA_DATA_SOURCE = 'embedded_fallback';
  window.PHARMA_META = { ...SIGNALEX_META, pharmaDataSource: 'embedded_fallback' };
  if (window.SIGNALEX_DEBUG) console.warn(
    '[Signalex] Pharma falling back to embedded CITATIONS (' + CITATIONS.length + ' records)'
  );
  return { citations: CITATIONS, meta: window.PHARMA_META, source: 'embedded_fallback' };
}

// ── CAT_INTEL + FORWARD_SIGNALS + INTEL_ITEMS ──
// ── Consulting intelligence data per category ────────────────────────
const CAT_INTEL = {
  'GMP violations': {
    urgency:'near-term',
    implication:'Highest-volume finding across all authorities. Repeat documentation and process compliance gaps suggest systemic QMS weaknesses, not isolated incidents.',
    action:'Review SOP compliance programme and self-inspection schedule. Prioritise sites with repeat deviations in documentation, cleaning, and batch records.',
    opportunity:'QMS gap assessment, SOP system redesign, self-inspection programme',
    client:'"Your site profile likely shares the GMP basics gaps most frequently cited — documentation, batch record discipline, and procedural compliance under operational pressure."',
  },
  'Labelling & claims': {
    urgency:'immediate',
    implication:'Second-highest finding category. Supplement and nutraceutical firms face disproportionate exposure — labelling claims are the top VMS enforcement trigger across all authorities.',
    action:'Audit promotional and labelling substantiation packages. Verify claims against applicable jurisdiction standards (AU, US, EU). Priority: therapeutic or structure-function claims.',
    opportunity:'Labelling compliance review, claims substantiation audit, cross-border claims alignment',
    client:'"Companies supplying into multiple markets face growing exposure as regulators align on unsupported health claims and labelling misrepresentation."',
  },
  'Computerised systems validation': {
    urgency:'immediate',
    implication:'Disproportionately high in inspection findings relative to volume. Data integrity and validated system controls are a clear rising regulator priority.',
    action:'Assess CSV documentation currency for GxP systems. Review audit trail integrity, access governance, and periodic review status. Prioritise legacy systems and admin rights governance.',
    opportunity:'Data integrity assessment, CSV remediation programme, audit trail governance review',
    client:'"Sites with legacy computerised systems face escalating inspection risk. Incomplete validation documentation and weak access controls are the most cited failures."',
  },
  'Sterility assurance': {
    urgency:'immediate',
    implication:'Lower in count than GMP basics but carries disproportionate operational and remediation burden. A single sterility finding can trigger product recall and facility suspension.',
    action:'Review contamination control strategy (CCS) ownership and completeness. Assess EM trending, aseptic behaviour programme, and intervention qualification status.',
    opportunity:'Sterility assurance programme review, CCS development, aseptic behaviour training',
    client:'"Sterile and parenteral facilities operating without a current, regulator-aligned CCS face significant inspection exposure. Remediation burden is high once findings emerge."',
  },
  'Contamination & sterility': {
    urgency:'near-term',
    implication:'Contamination findings span sterile and non-sterile products. Supplement and food co-manufacturers carry elevated cross-contamination risk that regulators are actively scrutinising.',
    action:'Conduct targeted contamination risk assessment across facility zoning, equipment sharing, and cleaning validation. Review incoming material release controls.',
    opportunity:'Contamination control programme, cleaning validation review, facility zoning assessment',
    client:'"Sites managing both food and supplement production lines face cross-contamination risk. Cleaning validation gaps and shared equipment are the most common triggers."',
  },
  'Equipment & facilities': {
    urgency:'near-term',
    implication:'Consistent mid-tier finding. Equipment qualification gaps and facility maintenance failures compound into batch release risk and frequently open broader QMS scrutiny.',
    action:'Review preventive maintenance programme completeness and equipment qualification lifecycle. Assess calibration records for critical instruments and qualification currency.',
    opportunity:'Equipment qualification audit, maintenance programme assessment, facility compliance review',
    client:'"Equipment and facility observations are frequently the gateway finding that opens broader QMS scrutiny during inspections."',
  },
  'Ingredient safety': {
    urgency:'near-term',
    implication:'Significant in the supplement segment. Ingredient adulteration and safety-of-use gaps create regulatory and product liability exposure across multiple authorities.',
    action:'Strengthen incoming raw material testing and supplier qualification. Review CoA verification process and identity testing programme. Address high-risk ingredient origins.',
    opportunity:'Supplier qualification remediation, ingredient safety audit, raw material testing programme',
    client:'"Ingredient sourcing from unqualified or high-risk suppliers is a structural vulnerability that regulators are consistently actioning."',
  },
  'Supply chain & procurement': {
    urgency:'near-term',
    implication:'Import alert volume and foreign supplier verification failures are an increasing trend. FDA FSVP enforcement is a key driver. Companies relying on imported ingredients are exposed.',
    action:'Conduct FSVP (or equivalent) programme review. Assess foreign supplier audit status, incoming material controls, and approved supplier list currency.',
    opportunity:'FSVP programme support, supplier qualification remediation, import compliance review',
    client:'"Companies relying on imported ingredients without robust supplier qualification face compounding risk as import alert enforcement increases."',
  },
  'Container closure integrity': {
    urgency:'near-term',
    implication:'Frequently cited in sterile product enforcement. CCI failures create direct product safety risk and are difficult to remediate post-market.',
    action:'Review CCI testing methodology for sterile products. Assess method validation status, acceptance criteria, and lifecycle testing frequency.',
    opportunity:'CCI programme development, method validation support, sterile product quality review',
    client:'"Container closure integrity failures in sterile products are a direct regulatory and patient safety risk — regulators expect validated, lifecycle-based testing."',
  },
  'Batch release': {
    urgency:'near-term',
    implication:'Batch release findings indicate QP/QA oversight gaps or production documentation failures that are reaching the release stage uncorrected.',
    action:'Assess batch review and release SOP adequacy. Review QP oversight model for outsourced or imported products. Evaluate batch record completeness criteria.',
    opportunity:'Batch release process review, QP support, batch documentation system upgrade',
    client:'"Batch release deficiencies suggest that deviation management and documentation gaps are not being caught before product leaves the site."',
  },
};

// ── Forward signals (regulatory focus outlook) ────────────────────────
const FORWARD_SIGNALS = [
  { cat:'Computerised systems validation', dir:'rising', conf:'high',
    signal:'Inspection findings disproportionately high relative to volume. Audit trail integrity and access governance are a clear enforcement priority across FDA and MHRA.',
    action:'CSV remediation and data integrity assessment' },
  { cat:'Sterility assurance', dir:'rising', conf:'high',
    signal:'Sterility and aseptic process observations increasing in inspection findings. Contamination control strategy completeness is the consistent gap.',
    action:'CCS development, sterility assurance programme review' },
  { cat:'Supply chain & procurement', dir:'rising', conf:'medium',
    signal:'Import alert volume trending upward. FDA FSVP enforcement and foreign supplier verification failures are driving this category higher.',
    action:'FSVP programme review, supplier qualification audit' },
  { cat:'Labelling & claims', dir:'stable', conf:'high',
    signal:'Second-highest category and consistently enforced across FDA, TGA, and EFSA. Supplement-adjacent firms face ongoing exposure with no sign of regulator fatigue.',
    action:'Claims substantiation review, cross-border labelling audit' },
  { cat:'GMP violations', dir:'stable', conf:'high',
    signal:'Highest-volume category and baseline enforcement activity. Core GMP documentation and procedural compliance will remain the primary inspection focus.',
    action:'Self-inspection programme, SOP compliance review' },
  { cat:'Batch release', dir:'emerging', conf:'medium',
    signal:'Batch release deficiencies appearing more frequently in inspection findings. May indicate tightening expectations on QP oversight and batch record discipline.',
    action:'Batch release process audit, QP oversight model review' },
  { cat:'Deviation management', dir:'emerging', conf:'low',
    signal:'Low absolute count but appearing in targeted inspection clusters. Regulators are scrutinising CAPA effectiveness and deviation closure quality, not just SOP existence.',
    action:'CAPA effectiveness review, deviation closure programme' },
  { cat:'Equipment & facilities', dir:'stable', conf:'medium',
    signal:'Mid-tier consistent finding. Not markedly increasing, but infrastructure gap observations reliably open broader QMS scrutiny during inspections.',
    action:'Preventive maintenance programme review' },
];

// ── Consulting Intelligence items (rich schema) ───────────────────────
const INTEL_ITEMS = [
  // IMMEDIATE ───────────────────────────────────────────────────────
  { id:'csv-di', title:'Data Integrity & Computerised Systems',
    category:'Computerised systems validation', priority:'immediate',
    trend:'rising', confidence:'high', impact:'high',
    summary:'Inspection findings disproportionately high relative to citation volume. Audit trail integrity and access governance are a clear rising enforcement priority.',
    whyItMatters:'Regulators are finding CSV gaps in GxP inspections at a rate disproportionate to total citation count. Sites without current validation documentation, clean audit trails, and governed access controls face escalating risk. Remediation programmes are intensive — prevention is far cheaper.',
    recommendedAction:'Assess CSV documentation currency for GxP systems. Review audit trail integrity, access governance, and periodic review status. Prioritise legacy systems and admin rights governance.',
    likelyClientConversation:'"Sites with legacy computerised systems face escalating inspection risk. Incomplete validation documentation and weak access controls are the most cited failures."',
    commercialOpportunity:'Data integrity assessment, CSV remediation programme, audit trail governance review',
    evidenceSummary:'147 citations in Computerised systems validation + 24 in Deviation management. Concentrated in inspection findings and warning letters across FDA and MHRA.',
    authorityTags:['FDA','MHRA'], facilityTypeTags:['General Pharma','Sterile / Parenteral'], topicTags:['Data integrity','Validation','Audit trail'] },

  { id:'sterility', title:'Sterility Assurance & Contamination Control',
    category:'Sterility assurance', priority:'immediate',
    trend:'rising', confidence:'high', impact:'high',
    summary:'Sterility and aseptic process observations increasing in inspection findings. Contamination control strategy completeness is the consistent gap.',
    whyItMatters:'A single sterility finding can trigger product recall and facility suspension. Remediation burden is high once observations emerge. Sites operating without a current, regulator-aligned contamination control strategy have significant inspection exposure.',
    recommendedAction:'Review contamination control strategy (CCS) ownership and completeness. Assess environmental monitoring trending, aseptic behaviour programme, and intervention qualification status.',
    likelyClientConversation:'"Sterile and parenteral facilities operating without a current CCS face significant inspection exposure. The remediation burden is high once findings emerge."',
    commercialOpportunity:'Sterility assurance programme review, CCS development, aseptic behaviour training',
    evidenceSummary:'198 citations in sterility assurance + 210 in contamination categories. FDA enforcement and inspection findings primary drivers.',
    authorityTags:['FDA','TGA'], facilityTypeTags:['Sterile / Parenteral','Biologics / Vaccine'], topicTags:['Sterility','Contamination','CCS','Aseptic'] },

  { id:'gmp-violations', title:'GMP Documentation & Process Compliance',
    category:'GMP violations', priority:'immediate',
    trend:'stable', confidence:'high', impact:'high',
    summary:'Highest-volume finding across all authorities. Repeat documentation and process compliance gaps indicate systemic QMS weaknesses, not isolated incidents.',
    whyItMatters:'GMP basics failures remain the foundation of most regulatory actions. High recurrence across sites suggests the issue is execution discipline under operational pressure. Regulatory fatigue is unlikely — enforcement baseline is stable at high volume.',
    recommendedAction:'Review SOP compliance programme and self-inspection schedule. Prioritise sites with repeat deviations in documentation, cleaning, and batch records.',
    likelyClientConversation:'"Your site profile likely shares the GMP basics gaps most frequently cited — documentation, batch record discipline, and procedural compliance under operational pressure."',
    commercialOpportunity:'QMS gap assessment, SOP system redesign, self-inspection programme',
    evidenceSummary:'744 citations — highest volume category. FDA, TGA, MHRA, EFSA. Spans all facility types. Consistent across all enforcement source types.',
    authorityTags:['FDA','TGA','MHRA','EFSA'], facilityTypeTags:['General Pharma','Sterile / Parenteral','API Manufacturer'], topicTags:['GMP','Documentation','QMS','SOP compliance'] },

  { id:'labelling', title:'Labelling & Claims Exposure',
    category:'Labelling & claims', priority:'immediate',
    trend:'stable', confidence:'high', impact:'medium',
    summary:'Second-highest category. Cross-border supply chains create compounding labelling risk as regulatory standards diverge across markets.',
    whyItMatters:'Supplement-adjacent firms face the highest exposure, particularly on structure-function and therapeutic claims. Multi-market supply chains amplify risk as jurisdictions enforce different substantiation standards. Enforcement is consistent — no sign of regulator fatigue.',
    recommendedAction:'Audit promotional and labelling substantiation packages. Verify claims against applicable jurisdiction standards (AU, US, EU). Priority: therapeutic and structure-function claims.',
    likelyClientConversation:'"Companies supplying into multiple markets face growing exposure as regulators align on unsupported health claims and labelling misrepresentation."',
    commercialOpportunity:'Labelling compliance review, claims substantiation audit, cross-border claims alignment programme',
    evidenceSummary:'462 citations — FDA, TGA, EFSA, BfR. Concentrated in Supplement / Nutraceutical and General Pharma facilities.',
    authorityTags:['FDA','TGA','EFSA','BfR'], facilityTypeTags:['Supplement / Nutraceutical','General Pharma'], topicTags:['Labelling','Claims','Cross-border','Substantiation'] },

  // NEAR-TERM ──────────────────────────────────────────────────────
  { id:'supply-chain', title:'Supply Chain & Import Controls',
    category:'Supply chain & procurement', priority:'near_term',
    trend:'rising', confidence:'medium', impact:'high',
    summary:'Import alert volume trending upward. FDA FSVP enforcement and foreign supplier verification failures are driving this category higher.',
    whyItMatters:'Import alert placement removes product from the US market without court order. Companies relying on imported ingredients without robust supplier qualification face compounding risk. FSVP enforcement intensity has increased.',
    recommendedAction:'Conduct FSVP programme review. Assess foreign supplier audit status, incoming material controls, and approved supplier list currency.',
    likelyClientConversation:'"Import alert exposure is a structural risk for companies with unqualified offshore suppliers. FSVP enforcement has intensified — self-audit before regulators do."',
    commercialOpportunity:'FSVP programme support, supplier qualification remediation, import compliance review',
    evidenceSummary:'82 citations — primarily FDA import alerts and warning letters. Spans multiple facility types and product categories.',
    authorityTags:['FDA'], facilityTypeTags:['General Pharma','Supplement / Nutraceutical','API Manufacturer'], topicTags:['Supply chain','Import','FSVP','Supplier qualification'] },

  { id:'container-closure', title:'Container Closure Integrity',
    category:'Container closure integrity', priority:'near_term',
    trend:'stable', confidence:'medium', impact:'high',
    summary:'Frequently cited in sterile product enforcement. CCI failures create direct product safety risk and are difficult to remediate post-market.',
    whyItMatters:'Regulators expect validated, lifecycle-based CCI testing. Deficient programmes attract repeat observations. Post-market CCI failures carry recall risk and difficult remediation timelines for sterile products.',
    recommendedAction:'Review CCI testing methodology for sterile products. Assess method validation status, acceptance criteria, and lifecycle testing frequency.',
    likelyClientConversation:'"Container closure integrity failures in sterile products are a direct regulatory and patient safety risk — regulators expect validated, lifecycle-based testing."',
    commercialOpportunity:'CCI programme development, method validation support, sterile product quality review',
    evidenceSummary:'119 citations — primarily sterile and parenteral facilities. Concentrated in FDA enforcement.',
    authorityTags:['FDA'], facilityTypeTags:['Sterile / Parenteral','Biologics / Vaccine'], topicTags:['CCI','Sterile products','Method validation'] },

  { id:'batch-release', title:'Batch Release & QP Oversight',
    category:'Batch release', priority:'near_term',
    trend:'emerging', confidence:'medium', impact:'medium',
    summary:'Batch release deficiencies appearing more frequently in inspection findings. May reflect tightening expectations on QP oversight and batch record discipline.',
    whyItMatters:'Batch release findings indicate that QA oversight gaps and documentation failures are not being caught before product leaves the site. This is a QMS maturity indicator regulators use to assess systemic programme quality.',
    recommendedAction:'Assess batch review and release SOP adequacy. Review QP oversight model for outsourced or imported products. Evaluate batch record completeness criteria.',
    likelyClientConversation:'"Batch release deficiencies suggest that deviation management and documentation gaps are not being caught before product leaves the site."',
    commercialOpportunity:'Batch release process review, QP support, batch documentation system upgrade',
    evidenceSummary:'98 citations — increasing in recent inspection findings. General Pharma and API Manufacturer facilities.',
    authorityTags:['FDA','MHRA'], facilityTypeTags:['General Pharma','API Manufacturer'], topicTags:['Batch release','QP','Documentation'] },

  { id:'ingredient-safety', title:'Ingredient Safety & Raw Material Controls',
    category:'Ingredient safety', priority:'near_term',
    trend:'stable', confidence:'medium', impact:'medium',
    summary:'Significant in the supplement segment. Ingredient adulteration and safety-of-use gaps create regulatory and product liability exposure across multiple authorities.',
    whyItMatters:'Ingredient sourcing from unqualified or high-risk suppliers is a structural vulnerability. Adulteration findings carry product recall risk and potential criminal liability under FDCA. CoA verification gaps are the most cited failure.',
    recommendedAction:'Strengthen incoming raw material testing and supplier qualification. Review CoA verification process and identity testing programme. Address high-risk ingredient origins.',
    likelyClientConversation:'"Ingredient sourcing from unqualified or high-risk suppliers is a structural vulnerability that regulators are consistently actioning."',
    commercialOpportunity:'Supplier qualification remediation, ingredient safety audit, raw material testing programme',
    evidenceSummary:'195 citations — Supplement / Nutraceutical and General Pharma. FDA and TGA enforcement.',
    authorityTags:['FDA','TGA'], facilityTypeTags:['Supplement / Nutraceutical','General Pharma'], topicTags:['Ingredient safety','Adulteration','Raw materials','CoA'] },

  // MONITOR ────────────────────────────────────────────────────────
  { id:'equipment', title:'Equipment & Facility Compliance',
    category:'Equipment & facilities', priority:'monitor',
    trend:'stable', confidence:'medium', impact:'medium',
    summary:'Consistent mid-tier finding. Equipment qualification gaps frequently open broader QMS scrutiny during inspections.',
    whyItMatters:'Equipment and facility observations are often gateway findings that escalate into broader QMS scrutiny. Preventive maintenance programme gaps and qualification lifecycle lapses signal systemic weaknesses regulators escalate from.',
    recommendedAction:'Review preventive maintenance programme completeness and equipment qualification lifecycle. Assess calibration records for critical instruments and qualification currency.',
    likelyClientConversation:'"Equipment and facility observations are frequently the gateway finding that opens broader QMS scrutiny during inspections."',
    commercialOpportunity:'Equipment qualification audit, maintenance programme assessment, facility compliance review',
    evidenceSummary:'251 citations — spans all facility types and authorities. High volume in device and drug enforcement.',
    authorityTags:['FDA','TGA'], facilityTypeTags:['General Pharma','Medical Device','Sterile / Parenteral'], topicTags:['Equipment','Facilities','Qualification','Maintenance'] },

  { id:'deviation-capa', title:'Deviation Management & CAPA Effectiveness',
    category:'Deviation management', priority:'monitor',
    trend:'emerging', confidence:'low', impact:'medium',
    summary:'Low absolute count but appearing in targeted inspection clusters. Regulators scrutinising CAPA effectiveness, not just SOP existence.',
    whyItMatters:'Deviation and CAPA findings signal that remediation programmes are not translating into sustained improvement. Regulators are moving from checking whether a CAPA system exists to whether it actually works and closes issues effectively.',
    recommendedAction:'Review CAPA effectiveness metrics and closure quality. Assess whether deviation trending identifies genuine root causes or documents symptoms. Evaluate closure timelines and recurrence rates.',
    likelyClientConversation:'"Repeat CAPA and documentation findings suggest remediation should focus on execution discipline, not just SOP updates."',
    commercialOpportunity:'CAPA effectiveness review, deviation closure programme, QMS maturity assessment',
    evidenceSummary:'24 citations — targeted inspection findings. Early-stage signal with low absolute count but increasing in inspection clusters.',
    authorityTags:['FDA','MHRA'], facilityTypeTags:['General Pharma','API Manufacturer'], topicTags:['CAPA','Deviation','Remediation'] },
];

// ── Task 8: Display-label maps for internal field values ─────────────
const _FM_LABELS = {
  sterility_assurance:          'Sterility assurance',
  supplier_qualification:       'Supplier qualification',
  contamination_chemical:       'Chemical contamination',
  contamination_microbial:      'Microbial contamination',
  contamination_cross:          'Cross-contamination',
  labelling_claims:             'Labelling & claims',
  documentation_data_integrity: 'Documentation / data integrity',
  computer_systems_validation:  'Computer systems validation',
  equipment_facilities:         'Equipment & facilities',
  batch_release:                'Batch release',
  deviation_capa:               'Deviation & CAPA',
  process_validation:           'Process validation',
  ingredient_safety:            'Ingredient safety',
  import_controls:              'Import controls',
  container_closure:            'Container closure integrity',
  adulteration:                 'Adulteration',
  mislabelling:                 'Mislabelling',
  insufficient_detail:          'Limited detail',
};
const _AU_LABELS = {
  direct:    'Direct AU relevance',
  indirect:  'Indirect AU/supply-chain',
  reference: 'Global regulatory reference',
};
const _PRIORITY_COLORS = { P1:'#c0392b', P2:'#e67e22', P3:'#7f8c8d', P4:'#aab' };
function _fmLabel(fm) { return _FM_LABELS[fm] || (fm||'').replace(/_/g,' '); }
function _topFailureMode(cits) {
  const fm = {};
  cits.forEach(c => {
    if (c.failure_mode && c.failure_mode !== 'insufficient_detail' && c.failure_mode !== 'other')
      fm[c.failure_mode] = (fm[c.failure_mode]||0)+1;
  });
  const top = Object.entries(fm).sort((a,b) => b[1]-a[1])[0];
  return top ? top[0] : null;
}

// ── Focus + limited-detail toggle state ──────────────────────────────
let selectedEnforcementFocus = null; // { type:'failure_mode'|'category'|'source_type'|'authority', value }
let selectedFacilityFocus    = null; // facility_type string
let _showLimitedDetail       = false;
let _overviewFocus           = null; // { label, filterObj, groupedCount } — set by What Changed / Top Risk clicks

// ── Task 7: Shared decision-UX helpers ───────────────────────────────

// Build a flat filter object from a focus descriptor.
function buildPharmaFilterFromFocus(focus) {
  if (!focus) return {};
  const map = { failure_mode:'failure_mode', category:'primary_gmp_category',
                source_type:'source_type',   authority:'authority',
                facility_type:'facility_type', display_issue:'display_issue' };
  const key = map[focus.type];
  return key ? { [key]: focus.value } : {};
}

// Cluster-primary records only (singletons included).
function getGroupedDecisionRecords(records) {
  return records.filter(c => c.cluster_primary !== false);
}

// Compact rows for a list of primary records — used in focus panels.
// Sorted: P1 → P2 → P3 → P4 → unranked; confirmed → provisional → unconfirmed; with decision_summary first; newest date.
function renderTopGroupedRecords(records, limit) {
  const _PO = { P1:0, P2:1, P3:2, P4:3 };
  const _SO = { confirmed:0, provisional:1, unconfirmed:2 };
  const sorted = getGroupedDecisionRecords(records).sort((a, b) => {
    const pa = _PO[a.priority] ?? 4, pb = _PO[b.priority] ?? 4;
    if (pa !== pb) return pa - pb;
    const sa = _SO[a.classification_status] ?? 3, sb = _SO[b.classification_status] ?? 3;
    if (sa !== sb) return sa - sb;
    const da = a.decision_summary ? 0 : 1, db = b.decision_summary ? 0 : 1;
    if (da !== db) return da - db;
    return (b.date || '') > (a.date || '') ? 1 : -1;
  });
  const primaries = sorted.slice(0, limit || 5);
  if (!primaries.length) return '<div style="font-size:11px;color:#7A92A8;padding:4px 0">No grouped records available.</div>';
  return primaries.map(c => {
    const entity  = (c.company || c.cluster_label || c.authority || '').slice(0, 44);
    const prio    = c.priority || '';
    const fm      = (c.failure_mode && c.failure_mode !== 'insufficient_detail') ? _fmLabel(c.failure_mode) : '';
    const ds      = (c.decision_summary || c.ai_summary || '').slice(0, 120);
    const clBadge = (c.cluster_size||1) > 1 ? `<span class="cit-group-badge" style="font-size:9px">&#215;${c.cluster_size}</span>` : '';
    const prioCol = _PRIORITY_COLORS[prio] || '#7f8c8d';
    return `<div style="display:flex;align-items:baseline;gap:6px;padding:3px 0;border-bottom:1px solid rgba(47,69,88,.06)">
      <span style="color:${prioCol};font-weight:700;font-size:9px;min-width:18px">${prio||'—'}</span>
      <span style="flex:1;font-size:11px;color:#2A3E52">${entity}${clBadge}</span>
      ${fm ? `<span style="font-size:9px;color:#c0392b;white-space:nowrap">${fm}</span>` : ''}
      <span style="font-size:9px;color:#7A92A8;white-space:nowrap">${c.date ? c.date.slice(0,7) : '—'}</span>
    </div>${ds ? `<div style="font-size:10px;color:#7A92A8;padding:1px 0 3px 24px">${ds}</div>` : ''}`;
  }).join('');
}

// Apply filter and navigate to Citations.
// Returns true if the filter has results, false otherwise (caller should show a message).
function navigateToCitationsWithFilter(filterObj) {
  if (!hasResultsForFilter(filterObj)) return false;
  applyPharmaFilter(filterObj);
  scrollToCitationsTable();
  return true;
}

// ── detectPattern ──
// ── Pattern detection — flag repeated company / category (≥3 occurrences) ───
function detectPattern(cits) {
  const compFreq={}, catFreq={};
  cits.forEach(c=>{
    if(c.company){compFreq[c.company]=(compFreq[c.company]||0)+1;}
    if(c.category){catFreq[c.category]=(catFreq[c.category]||0)+1;}
  });
  const out={};
  Object.entries(compFreq).forEach(([k,n])=>{if(n>=3)out[k]={type:'company',count:n};});
  Object.entries(catFreq).forEach(([k,n])=>{if(n>=3)out[k]={type:'category',count:n};});
  return out;
}

// ── formatTrendMovement ──
// ── Trend movement label (avoids silly percentages on low counts) ────
function formatTrendMovement(cur, prev) {
  if(cur===0 && prev===0) return '';
  if(prev===0 && cur>0) return `<span class="enf-trend-new">new this period</span>`;
  if(cur===0) return `<span class="enf-trend-down">no recent activity</span>`;
  const diff=cur-prev;
  if(Math.abs(diff)<=2) return `<span class="enf-trend-stable">stable</span>`;
  if(diff>0) {
    if(prev<4) return `<span class="enf-trend-up">+${diff} vs prior</span>`;
    return `<span class="enf-trend-up">+${diff} vs prior</span>`;
  }
  return `<span class="enf-trend-down">${diff} vs prior</span>`;
}

// ── renderSoWhatLine + getTopImmediateActions + rankPharmaRisks + getRiskImpact + formatVolumeBucket ──
// ── Decision-first overview helpers ───────────────────────────────────

function renderSoWhatLine(data) {
  const el=document.getElementById('pharma-so-what'); if(!el) return;
  const catMap=data.reduce((m,c)=>{const k=c.category||'Other';m[k]=(m[k]||0)+1;return m},{});
  const topEntry=Object.entries(catMap).sort((a,b)=>b[1]-a[1])[0];
  if(!topEntry){el.style.display='none';return;}
  const highCount=data.filter(c=>c.severity==='high').length;
  const intel=CAT_INTEL[topEntry[0]];
  const oppty=intel?'. '+intel.opportunity:'';
  el.style.display='block';
  el.innerHTML=`<span class="so-what-label">So what?</span>${topEntry[0]} leads with ${topEntry[1]} citation${topEntry[1]!==1?'s':''} and ${highCount} high-severity flag${highCount!==1?'s':''}${oppty}.`;
}

function getTopImmediateActions(data) {
  const actions = [];
  // Action 1: P1/P2 clusters needing review
  const p1 = data.filter(c => c.priority==='P1' && c.cluster_primary!==false);
  const p2 = data.filter(c => c.priority==='P2' && c.cluster_primary!==false);
  if (p1.length + p2.length > 0) {
    const topFm = _topFailureMode([...p1, ...p2]);
    const total = p1.length + p2.length;
    actions.push({
      priority: 'now',
      title: `Review ${p1.length} P1${p2.length?' + '+p2.length+' P2':''} GMP cluster${total!==1?'s':''} requiring decision`,
      why: topFm ? `Top issue: ${_fmLabel(topFm)}` : 'High-priority enforcement actions require compliance team review',
      onclick: `applyPharmaFilter({priority:'P1'}); scrollToCitationsTable()`
    });
  } else {
    // Fallback: top category
    const catMap = data.reduce((m,c)=>{const k=c.primary_gmp_category||c.category||'Other';m[k]=(m[k]||0)+1;return m},{});
    const topCat = Object.entries(catMap).filter(([k])=>k!=='Other'&&k!=='Other / Insufficient Detail').sort((a,b)=>b[1]-a[1])[0];
    if (topCat) {
      const intel = CAT_INTEL[topCat[0]];
      actions.push({
        priority: 'now',
        title: `Review ${topCat[0]} — ${topCat[1]} citation${topCat[1]!==1?'s':''}`,
        why: intel ? intel.action : 'Highest citation volume this period',
        onclick: `applyPharmaFilter({primary_gmp_category:'${topCat[0].replace(/'/g,"\\'")}'}); scrollToCitationsTable()`
      });
    }
  }
  // Action 2: High severity
  const highItems = data.filter(c => c.severity==='high');
  if (highItems.length) {
    actions.push({
      priority: 'urgent',
      title: `${highItems.length} high-severity action${highItems.length!==1?'s':''} require immediate attention`,
      why: 'High severity citations indicate enforcement escalation risk — act before inspection',
      onclick: `applyPharmaFilter({severity:'high'}); scrollToCitationsTable()`
    });
  }
  // Action 3: Supplement / Nutraceutical or TGA
  const suppCt = data.filter(c => c.facility_type==='Supplement / Nutraceutical').length;
  const tgaCt  = data.filter(c => c.authority==='TGA').length;
  if (suppCt >= tgaCt && suppCt > 0) {
    actions.push({
      priority: 'this-week',
      title: `${suppCt} Supplement / Nutraceutical action${suppCt!==1?'s':''}`,
      why: 'Labelling & claims leads this facility type — audit substantiation packages',
      onclick: `applyPharmaFilter({facility_type:'Supplement / Nutraceutical'}); scrollToCitationsTable()`
    });
  } else if (tgaCt > 0) {
    actions.push({
      priority: 'this-week',
      title: `${tgaCt} TGA action${tgaCt!==1?'s':''} on file — AU market priority`,
      why: 'Self-audit against AU GMP Code Part 3 for all AU-registered products',
      onclick: `applyPharmaFilter({authority:'TGA'}); scrollToCitationsTable()`
    });
  }
  return actions.slice(0, 3);
}


function rankPharmaRisks(data) {
  // Use cluster primaries (or singletons) only — avoids inflating counts with members
  const primaries = data.filter(c => c.cluster_primary !== false);
  const _SKIP_FM  = new Set(['', 'insufficient_detail', 'other', 'unknown']);
  // "GMP violations" is a broad legacy bucket — too vague to be a meaningful top-risk label.
  // Records in that bucket surface through their specific failure_mode instead.
  const _SKIP_CAT = new Set(['', 'Other', 'Other / Insufficient Detail', 'GMP violations']);
  const riskMap = {};
  primaries.forEach(c => {
    // Exclude non-enforcement records (guidance, scientific_opinion) from risk ranking —
    // they are not enforcement findings and inflate risk area counts.
    if (_isNonEnforcement(c)) return;
    // Exclude unconfirmed categories: the category cannot be trusted for risk reporting.
    const status = c.classification_status || '';
    const fm  = c.failure_mode || '';
    const cat = c.primary_gmp_category || c.category || '';
    let key, label;
    if (fm && !_SKIP_FM.has(fm) && (c.failure_mode_confidence||0) >= 0.6) {
      key = 'fm:' + fm; label = _fmLabel(fm);
    } else if (cat && !_SKIP_CAT.has(cat) && status !== 'unconfirmed') {
      key = 'cat:' + cat; label = cat;
    } else {
      return;
    }
    if (!riskMap[key]) riskMap[key] = { key, label, p1:0, p2:0, p3:0, confirmed:0, provisional:0, total:0, withSummary:0, clusterSize:0, exampleSummary:'', exampleAction:'' };
    const r = riskMap[key];
    r.total++;
    if (c.priority === 'P1') r.p1++;
    if (c.priority === 'P2') r.p2++;
    if (c.priority === 'P3') r.p3++;
    const cs = c.classification_status || '';
    if (cs === 'confirmed')   r.confirmed++;
    if (cs === 'provisional') r.provisional++;
    if (c.decision_summary) { r.withSummary++; if (!r.exampleSummary) r.exampleSummary = c.decision_summary; }
    if (c.recommended_action && !r.exampleAction) r.exampleAction = c.recommended_action;
    r.clusterSize += (c.cluster_size || 1);
  });
  // Score: P1 strongly dominates, then P2, then P3, then confirmed evidence, then capped volume bonus.
  // P1*1000 ensures any single P1 outranks even high-volume P2-only risks.
  return Object.values(riskMap)
    .map(r => ({ ...r, score: r.p1*1000 + r.p2*50 + r.p3*5 + r.confirmed*2 + Math.min(r.total, 10) }))
    .sort((a,b) => b.score - a.score)
    .slice(0, 6);
}

function getRiskImpact(count, high) {
  const pct=count>0?high/count:0;
  if(pct>0.4||high>5) return {label:'Critical',cls:'risk-critical'};
  if(pct>0.2||high>2) return {label:'High',cls:'risk-high'};
  return {label:'Watch',cls:'risk-watch'};
}

function formatVolumeBucket(count) {
  if(count>=200) return 'Very High';
  if(count>=100) return 'High';
  if(count>=40)  return 'Moderate';
  if(count>=10)  return 'Low';
  return 'Minimal';
}

// ── FAC_PROFILES + getFacilityRiskProfile ──
// ── Facility risk profiles ───────────────────────────────────────────
const FAC_PROFILES = {
  'General Pharma': {
    exposure: 'GMP violations, labelling non-conformances, contamination controls',
    focus: 'Documentation discipline, batch record completeness, cleaning validation',
    action: 'Review SOP compliance programme, cleaning validation, and self-inspection schedule against current findings.',
  },
  'Sterile / Parenteral': {
    exposure: 'Sterility assurance, contamination & sterility, container closure integrity',
    focus: 'Aseptic technique, contamination control strategy completeness, CCI testing',
    action: 'Audit CCS ownership, EM trending programme, and aseptic behaviour qualification status.',
  },
  'Biological / Biotech': {
    exposure: 'Computerised systems validation, sterility assurance, data integrity',
    focus: 'Process validation, analytical method validation, CSV documentation',
    action: 'Review CSV documentation currency for critical systems and analytical method validation lifecycle.',
  },
  'Medical Device': {
    exposure: 'Computerised systems, supplier control, post-market surveillance',
    focus: 'Validation evidence, alarm behaviour, complaint linkage to CAPA',
    action: 'Review validation package, risk file, and post-market issue handling process for each device category.',
  },
  'Supplement / Nutraceutical': {
    exposure: 'Labelling & claims, ingredient safety, supply chain & procurement',
    focus: 'Claims substantiation, identity testing, supplier qualification',
    action: 'Audit labelling substantiation packages across all jurisdictions. Strengthen incoming raw material identity testing.',
  },
  'API / Active Pharmaceutical Ingredient': {
    exposure: 'GMP violations, contamination, supply chain & procurement',
    focus: 'Impurity profiling, process validation, foreign supplier verification',
    action: 'Review FSVP programme and supplier qualification audit status. Assess impurity profiling completeness.',
  },
};
function getFacilityRiskProfile(name) {
  return FAC_PROFILES[name] || null;
}

// ── Action line generator — ensures every facility card has a recommended action ─
function generateFacilityAction(topIssueCat) {
  const cat = (topIssueCat || '').toLowerCase();
  if (cat.includes('gmp violation') || cat.includes('gmp non-compliance'))
    return 'Audit SOP adherence, deviation management, and cleaning validation records against current GMP expectations.';
  if (cat.includes('documentation') || cat.includes('record') || cat.includes('batch record'))
    return 'Review documentation controls, batch record completeness, and ALCOA+ compliance across production steps.';
  if (cat.includes('steril') || cat.includes('aseptic') || cat.includes('contamination'))
    return 'Audit container closure integrity, environmental monitoring programme, and aseptic process controls.';
  if (cat.includes('label') || cat.includes('claim'))
    return 'Verify label claims against registered specifications and confirm regulatory alignment across target jurisdictions.';
  if (cat.includes('ingredient safety') || cat.includes('adulterat') || cat.includes('prohibited'))
    return 'Review ingredient sourcing and safety dossiers; confirm no prohibited substances are present in the supply chain.';
  if (cat.includes('supply') || cat.includes('import') || cat.includes('procure') || cat.includes('fsvp'))
    return 'Review supplier qualification programme, FSVP documentation, and incoming material testing coverage.';
  if (cat.includes('equipment') || cat.includes('maintenance') || cat.includes('facilit'))
    return 'Audit preventive maintenance schedules, equipment qualification lifecycle, and facility hygiene controls.';
  if (cat.includes('csv') || cat.includes('data integrity') || cat.includes('computerised') || cat.includes('audit trail'))
    return 'Assess CSV documentation currency, audit trail completeness, and access control governance.';
  if (cat.includes('deviation') || cat.includes('capa') || cat.includes('corrective'))
    return 'Review CAPA effectiveness, deviation closure rates, and systemic root cause trending.';
  if (cat.includes('testing') || cat.includes('analytical') || cat.includes('laboratory'))
    return 'Review laboratory controls, OOS investigation procedures, and analytical method validation status.';
  return 'Review site-specific enforcement history and prioritise corrective actions based on repeat finding patterns.';
}


// === PHARMA OVERVIEW RENDER FUNCTIONS ===

// Renders the Recent Enforcement Actions feed and count label.
// Extracted so setOverviewFocus/clearOverviewFocus can update it without a full re-render.
function _renderOvFeed(data, isFiltered) {
  const focused = !!_overviewFocus;
  const oc = document.getElementById('pharma-ov-count');
  if (oc) oc.textContent = focused ? `${data.length} focused` : `${data.length} total`;

  // Update section heading to show focus context
  const hd = document.getElementById('pharma-ov-feed-hd');
  if (hd) hd.textContent = focused
    ? `Recent Enforcement Actions: ${_overviewFocus.label}`
    : 'Recent Enforcement Actions';

  buildChipBar('pharma-ov-chip-bar');
  const el = document.getElementById('pharma-ov-feed'); if (!el) return;

  // When focused: show evidence-backed records first; broader records appended with separator.
  let feed, viewAllLabel;
  if (focused) {
    const { evidenceBacked, broader } = _focusEvidenceBacked(data, _overviewFocus.label);
    const limit = 10;
    const evSlice = evidenceBacked.slice(0, limit);
    const broaderNote = broader.length > 0
      ? `<div style="margin:8px 0 4px;padding:6px 10px;background:rgba(47,69,88,.05);
            border-left:3px solid rgba(122,146,168,.3);border-radius:0 4px 4px 0">
          <span style="font-size:10px;color:#7A92A8;font-style:italic">
            &#9432; ${broader.length} record${broader.length!==1?'s':''} in this focus
            lack direct equipment/facility evidence — not shown above.
            <button class="btn-secondary" style="font-size:9px;margin-left:6px"
              onclick="showPTab('pharma-citations')">View all ${data.length} in Citations &rarr;</button>
          </span></div>`
      : '';
    const viewAllBtn = evidenceBacked.length > limit
      ? `<button class="ov-feed-view-all" onclick="showPTab('pharma-citations')">View all ${data.length} focused citations &rarr;</button>`
      : '';
    el.innerHTML = evSlice.length
      ? evSlice.map(c => citCard(c)).join('') + broaderNote + viewAllBtn
      : broaderNote || '<div class="empty"><div class="empty-icon">&#128270;</div><div class="empty-text">No evidence-backed citations for this focus</div></div>';
    return;
  }

  const limit = isFiltered ? 15 : 8;
  const slice = data.slice(0, limit);
  const viewAllBtn = data.length > limit
    ? `<button class="ov-feed-view-all" onclick="showPTab('pharma-citations')">View all ${data.length} citations &rarr;</button>`
    : '';
  el.innerHTML = slice.length
    ? slice.map(c => citCard(c)).join('') + viewAllBtn
    : '<div class="empty"><div class="empty-icon">&#128270;</div><div class="empty-text">No citations match filters</div></div>';
}

// ── Pharma Overview ──────────────────────────────────────────────────
function renderPharmaOverview() {
  const data=filteredCits();
  const isFiltered = pActiveKpi || pCatFilter || pF.auth!=='all' || pF.srctype!=='all' || pF.priority!=='all' || pF.dicapa;

  // KPI row — single pass over data
  // p1Groups = cluster-primary P1 records (deduplicated action groups)
  // p1Raw    = all P1 records before cluster grouping (matches sidebar pill count)
  let p1Groups=0,p1Raw=0,wl=0,insp=0,diCapaFilt=0,tga=0;
  for(const c of data){
    if(c.priority==='P1'){ p1Raw++; if(c.cluster_primary!==false) p1Groups++; }
    if(c.source_type==='warning_letter')wl++;
    if(c.source_type==='inspection_finding')insp++;
    if(isDiCapa(c))diCapaFilt++;
    if(c.authority==='TGA')tga++;
  }
  const _setKpi=(id,val)=>{const e=document.getElementById(id);if(e){const v=e.querySelector('.kpi-val');if(v){v.textContent=val;v.classList.add('kpi-updated');setTimeout(()=>v.classList.remove('kpi-updated'),600);}}};
  const _setKpiSub=(id,text)=>{const e=document.getElementById(id);if(e)e.textContent=text;};
  _setKpi('pk-high', p1Groups);
  _setKpiSub('pk-high-sub', `${p1Raw} raw P1 citation${p1Raw!==1?'s':''}`);
  _setKpi('pk-wl', wl);
  _setKpi('pk-483', insp || '—');
  _setKpiSub('pk-483-sub', insp > 0 ? `${insp} inspection finding${insp!==1?'s':''}` : 'No records in current dataset');
  _setKpi('pk-total', data.length);
  _setKpi('pk-diCapa', diCapaFilt);
  _setKpi('pk-tga', tga);

  // Tab badge — always show full dataset size
  const tabBadge=document.getElementById('pharma-tab-count');
  if(tabBadge) tabBadge.textContent=`${getPharmaCitations().length} citations`;

  // AI summary + So What + What Changed
  renderWhatChanged();
  renderPharmaInsights();
  renderSoWhatLine(data);

  // Focus banner — must render after filteredCits() is settled
  renderPharmaFocusBanner();

  // Decision-first sections
  renderStartHereActions(data);
  renderTopRiskBlocks(rankPharmaRisks(data));
  renderAllCategoriesCollapsed(data);

  // Authority activity
  renderAuthBars('pharma-auth-bars');

  // Recent feed
  _renderOvFeed(data, isFiltered);
}

// context = 'overview' (default) | 'enforcement'
// In enforcement context, card clicks set the enforcement focus panel instead of
// jumping to citations.
function renderCatGrid(gridId, countId, context) {
  const data = filteredCits();
  const el = document.getElementById(gridId); if (!el) return;

  // Bucket each cluster-primary record by its canonical display issue label.
  // "Provisional: X" records are merged into the "X" bucket so counts match the click filter.
  const primaries = data.filter(c => c.cluster_primary !== false);

  const cardMap = {};
  primaries.forEach(c => {
    const issue = getDisplayIssue(c);
    // Normalize provisional variant to base label for bucketing.
    const isProvVariant = issue.startsWith('Provisional: ');
    const key   = isProvVariant ? issue.slice('Provisional: '.length) : issue;
    const label = key;
    // Track dominant type (fm vs cat) from the first record in each bucket.
    const fm = c.failure_mode || '';
    const derivedType = (fm && fm !== 'insufficient_detail' && (c.failure_mode_confidence || 0) >= 0.6)
      ? 'fm' : (key === 'Unclassified' ? 'limited' : 'cat');

    if (!cardMap[key]) cardMap[key] = { type: derivedType, key, label, p1:0, p2:0, total:0, rawCitCount:0, withSummary:0, exampleAction:'', hasProvisional:false };
    const g = cardMap[key];
    g.total++;
    g.rawCitCount += (c.cluster_size || 1);
    if (c.priority === 'P1') g.p1++;
    if (c.priority === 'P2') g.p2++;
    if (c.decision_summary || c.ai_summary) g.withSummary++;
    if (!g.exampleAction && c.recommended_action) g.exampleAction = c.recommended_action;
    if (isProvVariant) g.hasProvisional = true;
  });

  // Sort by priority-weighted score; 'Unclassified' always last.
  const sorted = Object.values(cardMap).sort((a, b) => {
    if (a.key === 'Unclassified' && b.key !== 'Unclassified') return 1;
    if (b.key === 'Unclassified' && a.key !== 'Unclassified') return -1;
    return (b.p1*5 + b.p2*2 + b.withSummary) - (a.p1*5 + a.p2*2 + a.withSummary);
  });

  // Filter out limited detail from main render when toggle is off.
  const visibleCards = _showLimitedDetail ? sorted : sorted.filter(g => g.key !== 'Unclassified');

  el.innerHTML = visibleCards.map(g => {
    const intel     = CAT_INTEL[g.label] || null;
    const isLimited = g.key === 'Unclassified';
    const labelSafe = g.label.replace(/'/g, "\\'");

    // Both contexts use display_issue as the canonical filter key.
    // Enforcement: opens the focus panel. Overview: navigates to filtered citations.
    const filterFn = isLimited ? '' : (context === 'enforcement'
      ? `setEnforcementFocus({type:'display_issue',value:'${labelSafe}'})`
      : `applyPharmaFilter({display_issue:'${labelSafe}'}); scrollToCitationsTable()`);

    // Active state: enforcement checks selectedEnforcementFocus; overview checks pF.displayIssue.
    const isAct = context === 'enforcement'
      ? !!(selectedEnforcementFocus && selectedEnforcementFocus.value === g.label)
      : (pF.displayIssue === g.label);

    const rawNote  = g.rawCitCount > g.total
      ? ` <span style="font-size:9px;color:#7A92A8">(${g.rawCitCount} related citations)</span>` : '';
    const p1html   = g.p1 ? `<span style="color:${_PRIORITY_COLORS.P1};font-size:9px;font-weight:600">${g.p1}&thinsp;P1</span>` : '';
    const p2html   = g.p2 ? `<span style="color:${_PRIORITY_COLORS.P2};font-size:9px">${g.p2}&thinsp;P2</span>` : '';
    const p1p2     = [p1html, p2html].filter(Boolean).join(' &middot; ');
    const provNote = g.hasProvisional ? '<span style="font-size:9px;color:#7A92A8;font-style:italic">incl. provisional</span>' : '';

    const whyLine   = intel ? `<div class="cat-why-line">${intel.implication.split('.')[0]}.</div>` : '';
    const intelHtml = (intel && !isLimited) ? `<div class="cat-cell-intel">
      <div class="action-tag ${intel.urgency}">${intel.urgency}</div>
      <div class="cat-implication">${intel.implication}</div>
      <div class="cat-action">${intel.action}</div>
    </div>` : '';
    const entityBtn = g.type === 'cat'
      ? `<button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${labelSafe}','category')" title="Open ${g.label} detail" style="font-size:12px;margin-top:1px;flex-shrink:0">&#9432;</button>`
      : '';

    const wrapStyle = filterFn
      ? `cursor:pointer${isLimited ? ';opacity:.55' : ''}`
      : 'opacity:.5';
    return `<div class="cat-cell${isAct?' c-active':''}" onclick="${filterFn}" style="${wrapStyle}">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:4px">
        <div class="cat-cell-name">${g.label}</div>
        ${entityBtn}
      </div>
      <div class="cat-cell-ct">${g.total} grouped finding${g.total!==1?'s':''}${rawNote}</div>
      <div class="cat-cell-sub">${p1p2 || '<span style="font-size:9px;color:#7A92A8">no priority flags</span>'}${provNote ? ' &middot; ' + provNote : ''}</div>
      ${whyLine}
      ${intelHtml}
    </div>`;
  }).join('');

  // Unclassified: hidden by default behind a toggle button.
  const limitedGrp = cardMap['Unclassified'];
  if (limitedGrp && !_showLimitedDetail) {
    el.innerHTML += `<div style="margin-top:6px"><button class="btn-secondary" style="font-size:10px;opacity:.6;padding:3px 8px"
      onclick="_showLimitedDetail=true;renderCatGrid('${gridId}','${countId}','${context||''}')">
      Show limited detail (${limitedGrp.total} grouped finding${limitedGrp.total!==1?'s':''})</button></div>`;
  }

  const cc = document.getElementById(countId);
  if (cc) cc.textContent = `${primaries.length} decision record${primaries.length!==1?'s':''}`;
}
// ── Pharma AI insights ────────────────────────────────────────────────
function renderPharmaInsights() {
  const data=filteredCits();
  const isFiltered = pActiveKpi || pCatFilter || pF.auth!=='all' || pF.srctype!=='all' || pF.priority!=='all' || pF.factype!=='all' || pF.dicapa;
  const n = (ct, noun) => ct===0 ? `No ${noun} match current filters` : `${ct} ${noun}`;
  let wlCt=0,inspCt=0,importCt=0,suppCt=0,tgaCt=0,csvCt=0,sterCt=0,labelCt=0;
  for(const c of data){
    if(c.source_type==='warning_letter')wlCt++;
    if(c.source_type==='inspection_finding')inspCt++;
    if(c.source_type==='import_alert')importCt++;
    if(c.facility_type==='Supplement / Nutraceutical')suppCt++;
    if(c.authority==='TGA')tgaCt++;
    if(c.category==='Computerised systems validation')csvCt++;
    if(c.category==='Sterility assurance')sterCt++;
    if(c.category==='Labelling & claims')labelCt++;
  }
  const sc = isFiltered ? 'in current view' : 'on file';
  const insights=[
    {col:'#D04444',label:'Warning Letters',
     text:wlCt===0&&isFiltered
       ? `No warning letters match the current filters.`
       : `<b>${wlCt} warning letter${wlCt!==1?'s':''}</b> ${sc}. GMP violations and labelling &amp; claims lead by volume. Action: Review repeat citation patterns against your site's enforcement risk profile.`},
    {col:'#C87020',label:'483 / Inspection Findings',
     text:inspCt===0&&isFiltered
       ? `No inspection findings match the current filters.`
       : `<b>${inspCt} inspection finding${inspCt!==1?'s':''}</b> ${sc}. CSV (${csvCt}) and sterility assurance (${sterCt}) are systemic gaps across General Pharma and Sterile facilities. Action: Prioritise CSV remediation and CCS completeness.`},
    {col:'#C87020',label:'Import Alert Exposure',
     text:importCt===0&&isFiltered
       ? `No import alerts match the current filters.`
       : `<b>${importCt} import alert${importCt!==1?'s':''}</b> ${sc}. Contamination &amp; sterility is the primary trigger. Action: Audit incoming raw material controls and foreign supplier verification programme.`},
    {col:'#3D5268',label:'Supplement / Nutra. Risk',
     text:suppCt===0&&isFiltered
       ? `No Supplement / Nutraceutical actions match the current filters.`
       : `<b>${suppCt} enforcement action${suppCt!==1?'s':''}</b> targeting Supplement / Nutraceutical facilities. Labelling &amp; claims (${labelCt}) is the dominant category. Action: Audit labelling substantiation packages against applicable jurisdiction standards.`},
    {col:'rgba(13,148,136,.75)',label:'TGA Enforcement Watch',
     text:tgaCt===0&&isFiltered
       ? `No TGA actions match the current filters.`
       : `<b>${tgaCt} TGA enforcement action${tgaCt!==1?'s':''}</b> ${sc}. Trend is increasing. Action: Run proactive self-audit against AU GMP Code Part 3 for all AU-registered products.`},
  ];
  const el=document.getElementById('pharma-ai-body'); if(!el)return;
  el.innerHTML=insights.map((i,idx)=>{
    const actions=[
      `pF.srctype='warning_letter';syncPFPills();renderPharmaOverview();document.getElementById('pharma-ov-feed').scrollIntoView({behavior:'smooth',block:'start'})`,
      `pF.srctype='inspection_finding';syncPFPills();renderPharmaOverview();document.getElementById('pharma-ov-feed').scrollIntoView({behavior:'smooth',block:'start'})`,
      `pF.srctype='import_alert';syncPFPills();renderPharmaOverview();document.getElementById('pharma-ov-feed').scrollIntoView({behavior:'smooth',block:'start'})`,
      `pF.factype='Supplement / Nutraceutical';syncPFPills();renderPharmaOverview();document.getElementById('pharma-ov-feed').scrollIntoView({behavior:'smooth',block:'start'})`,
      `pF.auth='TGA';syncPFPills();renderPharmaOverview();document.getElementById('pharma-ov-feed').scrollIntoView({behavior:'smooth',block:'start'})`,
    ];
    return `<div class="pharma-insight-row" onclick="${actions[idx]}" title="Click to filter to this signal">
      <div class="pharma-insight-dot" style="background:${i.col}"></div>
      <span class="pharma-insight-label" style="color:${i.col}">${i.label}:</span>
      <span class="pharma-insight-text">${i.text}</span>
    </div>`;
  }).join('');
  // Consultant mode overlay — top category interpretation
  const topCat=Object.entries(
    data.reduce((m,c)=>{const k=c.category||'Other';m[k]=(m[k]||0)+1;return m},{})
  ).sort((a,b)=>b[1]-a[1])[0];
  const intel=topCat&&CAT_INTEL[topCat[0]];
  if(intel){
    const _set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v;};
    _set('pcl-implication', intel.implication);
    _set('pcl-action', intel.action);
    _set('pcl-oppty', intel.opportunity);
    _set('pcl-client', intel.client);
  }
}
function renderStartHereActions(data) {
  const el=document.getElementById('pharma-start-here'); if(!el) return;
  const actions=getTopImmediateActions(data);
  if(!actions.length){el.innerHTML='<div style="font-size:11px;color:#2F4558;padding:8px">No actions — adjust filters or add more data</div>';return;}
  el.innerHTML=actions.map(a=>`<div class="start-here-action">
    <div class="sha-priority sha-${a.priority}">${a.priority.replace('-',' ')}</div>
    <div class="sha-body"><div class="sha-title">${a.title}</div><div class="sha-why">${a.why}</div></div>
    ${a.onclick?`<button class="sha-btn" onclick="${a.onclick}">Drill in &rarr;</button>`:''}
  </div>`).join('');
}

function renderTopRiskBlocks(risks) {
  const el=document.getElementById('pharma-top-risks'); if(!el) return;
  if(!risks.length){
    el.innerHTML='<div class="empty"><div class="empty-text">No prioritised risk groups found — adjust filters or check AI summary coverage</div></div>';
    return;
  }
  el.innerHTML=risks.map((r,i)=>{
    const intel    = CAT_INTEL[r.label];
    const meaning  = r.exampleSummary ? r.exampleSummary.slice(0,180) : (intel ? intel.implication.split('.')[0]+'.' : '');
    const action   = r.exampleAction  || (intel ? intel.action : 'Review enforcement pattern and assess site exposure');
    const p1p2txt  = (r.p1||r.p2) ? `${r.p1} P1 · ${r.p2} P2` : '';
    const filterJs = `navigateToCitationsWithFilter({display_issue:'${r.label.replace(/'/g,"\\'")}'})`

    // Build top grouped records using the canonical display_issue predicate.
    const allMatched = filteredCits().filter(c => matchesDisplayIssue(c, r.label));
    const ftCts={}, authCts={};
    allMatched.forEach(c=>{
      if(c.facility_type) ftCts[c.facility_type]=(ftCts[c.facility_type]||0)+1;
      if(c.authority)    authCts[c.authority]=(authCts[c.authority]||0)+1;
    });
    const topFts   = Object.entries(ftCts).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k).join(', ');
    const topAuths = Object.entries(authCts).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k).join(', ');

    const detailHtml = `<div class="trb-detail" style="display:none;margin-top:10px;padding-top:10px;border-top:1px solid rgba(47,69,88,.1)">
      ${meaning?`<div style="font-size:11px;color:#2F4558;margin-bottom:8px">${meaning}</div>`:''}
      <div style="font-size:11px;color:#2A3E52;margin-bottom:6px"><b>Recommended action:</b> ${action}</div>
      ${topFts?`<div style="font-size:10px;color:#7A92A8;margin-bottom:3px">Facility types: ${topFts}</div>`:''}
      ${topAuths?`<div style="font-size:10px;color:#7A92A8;margin-bottom:8px">Authorities: ${topAuths}</div>`:''}
      <div style="font-size:10px;color:#4a6278;font-weight:600;margin-bottom:4px">Top grouped records</div>
      ${renderTopGroupedRecords(allMatched, 4)}
      <div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn-secondary" onclick="event.stopPropagation();${filterJs}" style="font-size:10px">View matching citations &rarr;</button>
        <button class="btn-secondary" onclick="event.stopPropagation();_toggleTopRisk(this.closest('.top-risk-block'))" style="font-size:10px">&#10005; Close</button>
      </div>
    </div>`;

    return `<div class="top-risk-block" onclick="_toggleTopRisk(this)" style="cursor:pointer" title="Click for detail on ${r.label}">
      <div class="trb-rank">#${i+1}</div>
      <div class="trb-body">
        <div class="trb-hd">
          <span class="trb-cat">${r.label}</span>
          ${r.p1>0?`<span class="risk-impact-badge risk-critical">P1&times;${r.p1}</span>`:''}
          ${r.p2>0?`<span class="risk-impact-badge risk-high">P2&times;${r.p2}</span>`:''}
          <span class="vol-bucket">${r.total} grouped finding${r.total!==1?'s':''}</span>
        </div>
        <div class="trb-action">&#8594; ${action.slice(0,140)} <span style="color:#0d9488;font-size:10px">Expand &rarr;</span></div>
        <div class="trb-stats">${r.clusterSize} related citation${r.clusterSize!==1?'s':''}${p1p2txt?' &middot; '+p1p2txt:''}</div>
        ${detailHtml}
      </div>
    </div>`;
  }).join('');
  const countEl=document.getElementById('top-risks-count');
  if(countEl) countEl.textContent=`(${risks.length} action priority groups)`;
}

function _toggleTopRisk(card) {
  const detail = card && card.querySelector('.trb-detail');
  if (!detail) return;
  const isOpen = detail.style.display !== 'none';
  document.querySelectorAll('.top-risk-block .trb-detail').forEach(d => { d.style.display='none'; });
  document.querySelectorAll('.top-risk-block').forEach(b => b.classList.remove('trb-expanded'));
  if (!isOpen) { detail.style.display='block'; card.classList.add('trb-expanded'); }
}

function renderAllCategoriesCollapsed(data) {
  renderCatGrid('pharma-cat-grid','pharma-cat-count');
}

function toggleAllCategories() {
  const wrap=document.getElementById('pharma-cats-collapsed');
  const icon=document.getElementById('all-cats-toggle-icon');
  if(!wrap) return;
  const isOpen=wrap.style.display!=='none';
  wrap.style.display=isOpen?'none':'block';
  if(icon) icon.innerHTML=isOpen?'&#9660; Show all':'&#9650; Collapse';
}

// ── Phase-1 helpers: result-counting, fallback, inline messaging ─────

// Count records in a given array that match a filter object (no side effects).
function _countMatchingInPool(filterObj, pool) {
  return pool.filter(c => {
    if (filterObj.display_issue !== undefined && !matchesDisplayIssue(c, filterObj.display_issue)) return false;
    if (filterObj.failure_mode !== undefined && (c.failure_mode||'') !== filterObj.failure_mode) return false;
    if (filterObj.primary_gmp_category !== undefined) {
      if ((c.primary_gmp_category||'') !== filterObj.primary_gmp_category && (c.category||'') !== filterObj.primary_gmp_category) return false;
    }
    if (filterObj.authority     !== undefined && (c.authority||'')     !== filterObj.authority)     return false;
    if (filterObj.source_type   !== undefined && (c.source_type||'')   !== filterObj.source_type)   return false;
    if (filterObj.facility_type !== undefined && (c.facility_type||'') !== filterObj.facility_type) return false;
    if (filterObj.query !== undefined) {
      const q = (filterObj.query||'').toLowerCase();
      if (q && ![(c.summary||''),(c.category||''),(c.primary_gmp_category||''),(c.violation_details||''),(c.company||'')].some(f=>f.toLowerCase().includes(q))) return false;
    }
    return true;
  }).length;
}

// Returns true if the filter would produce at least one record in the current pool.
function hasResultsForFilter(filterObj) {
  return _countMatchingInPool(filterObj, filteredCits()) > 0;
}

// Returns the first filter from [mapped cats → mapped fms → legacy text] that
// yields > 0 results in filteredCits(), or null if none does.
function getBroaderFallbackFilter(label) {
  const pool    = filteredCits();
  const mapping = _TREND_FILTER_MAP[label];
  if (mapping) {
    for (const cat of (mapping.cats || [])) {
      if (_countMatchingInPool({ primary_gmp_category: cat }, pool) > 0)
        return { primary_gmp_category: cat };
    }
    for (const fm of (mapping.fms || [])) {
      if (_countMatchingInPool({ failure_mode: fm }, pool) > 0)
        return { failure_mode: fm };
    }
  }
  // Legacy text-search fallback: use query filter so applyPharmaFilter can handle it
  const q = label.toLowerCase();
  const textHits = pool.filter(c =>
    [(c.summary||''),(c.category||''),(c.primary_gmp_category||''),(c.violation_details||'')]
      .some(f => f.toLowerCase().includes(q))
  );
  if (textHits.length > 0) return { query: label };
  return null;
}

// Insert a self-dismissing inline message into containerEl.
// broaderFilter: if non-null, shows "View broader related records" button.
function showInlineNoResultsMessage(containerEl, msg, broaderFilter) {
  if (!containerEl) return;
  let el = containerEl.querySelector('.inline-no-results-msg');
  if (!el) {
    el = document.createElement('div');
    el.className = 'inline-no-results-msg';
    el.style.cssText = 'margin-top:8px;padding:8px 12px;background:rgba(47,69,88,.07);border-radius:5px;font-size:11px;color:#2F4558';
    containerEl.appendChild(el);
  }
  let html = msg;
  if (broaderFilter) {
    const fs = JSON.stringify(broaderFilter).replace(/"/g,"'");
    html += ` <button style="font-size:10px;color:#0d9488;background:none;border:none;cursor:pointer;margin-left:6px"
      onclick="navigateToCitationsWithFilter(${fs});this.closest('.inline-no-results-msg').remove()">
      View broader related records &rarr;</button>`;
  }
  html += ` <button style="font-size:10px;color:#7A92A8;background:none;border:none;cursor:pointer;margin-left:4px"
    onclick="this.closest('.inline-no-results-msg').remove()">&#10005;</button>`;
  el.innerHTML = html;
  clearTimeout(el._t);
  el._t = setTimeout(() => el.remove(), 10000);
}

// ── Task 1: Trend filter mapping ─────────────────────────────────────
// Maps legacy c.category labels (used by What Changed) to new-model filter fields.
// cats: possible primary_gmp_category values; fms: failure_mode values.
const _TREND_FILTER_MAP = {
  'Equipment & facilities':          { cats:['Equipment & facilities','Equipment / Facilities'],
                                       fms:['equipment_facilities','equipment_facility','cleaning_validation','calibration'],
                                       evidenceTerms:['equipment','facilit','calibration','maintenance','hvac','utilities','water system','premises','sanitation','qualification','installation','repair','preventive'] },
  'Computerised systems validation': { cats:['Computerised systems validation','Computerised systems','Computer systems'],
                                       fms:['computer_systems_validation','computerised_systems','data_integrity','documentation_data_integrity'] },
  'Labelling & claims':              { cats:['Labelling & claims','Labelling and claims'],
                                       fms:['labelling_claims','labelling_error','mislabelling'] },
  'Ingredient safety':               { cats:['Ingredient safety'],
                                       fms:['ingredient_safety','identity_purity_testing','undeclared_drug_substance','contamination_chemical','adulteration'] },
  'Contamination & sterility':       { cats:['Contamination & sterility','Sterility assurance'],
                                       fms:['sterility_assurance','contamination_microbial','contamination_cross','contamination_chemical'] },
  'Sterility assurance':             { cats:['Sterility assurance'], fms:['sterility_assurance'] },
  'Supply chain & procurement':      { cats:['Supply chain & procurement','Supply chain'],
                                       fms:['supplier_qualification','import_controls'] },
  'Batch release':                   { cats:['Batch release'], fms:['batch_release'] },
  'Container closure integrity':     { cats:['Container closure integrity'], fms:['container_closure'] },
  'GMP violations':                  { cats:['GMP violations'],
                                       fms:['deviation_capa','process_validation','documentation_data_integrity'], gmv:true },
};

function _trendMatchRecord(c, m) {
  const cat = c.primary_gmp_category || c.category || '';
  const fm  = c.failure_mode || '';
  return (m.cats||[]).includes(cat) || (m.fms||[]).includes(fm);
}

// Returns the best single filter object for the matched records.
function _trendBestFilter(matched, mapping) {
  const fmCts = {}, catCts = {};
  matched.forEach(c => {
    const fm  = c.failure_mode || '';
    const cat = c.primary_gmp_category || c.category || '';
    if ((mapping.fms||[]).includes(fm))  fmCts[fm]  = (fmCts[fm]||0)+1;
    if ((mapping.cats||[]).includes(cat)) catCts[cat] = (catCts[cat]||0)+1;
  });
  const topFm  = Object.entries(fmCts).sort((a,b)=>b[1]-a[1])[0];
  const topCat = Object.entries(catCts).sort((a,b)=>b[1]-a[1])[0];
  if (topFm && topCat) return topFm[1] >= topCat[1] ? { failure_mode: topFm[0] } : { primary_gmp_category: topCat[0] };
  if (topFm)  return { failure_mode: topFm[0] };
  if (topCat) return { primary_gmp_category: topCat[0] };
  return null;
}

function trendItemClick(label) {
  const mapping = _TREND_FILTER_MAP[label];
  const pool    = filteredCits();

  // Determine the best specific filter to use
  let best = null;
  if (mapping) {
    const matched = pool.filter(c => _trendMatchRecord(c, mapping));
    if (matched.length) best = _trendBestFilter(matched, mapping);
  }
  if (!best) {
    // Direct match on primary_gmp_category or legacy category
    const direct = pool.filter(c => (c.primary_gmp_category||c.category||'') === label);
    if (direct.length) best = { primary_gmp_category: label };
  }

  // Count how many records the chosen filter would actually return
  const count = best ? _countMatchingInPool(best, pool) : 0;
  if (count > 0) {
    navigateToCitationsWithFilter(best);
    return;
  }

  // Zero results — find a broader fallback and show an inline message instead
  const broader = getBroaderFallbackFilter(label);
  const cols = document.getElementById('what-changed-cols');
  showInlineNoResultsMessage(
    cols ? cols.parentNode : null,
    `No grouped records match "${label}" in the current period.`,
    broader
  );
}

// Show a self-dismissing inline message below What Changed.
// Delegates to showInlineNoResultsMessage; broader button only shown when fallback exists.
function _showWcMessage(msg, label) {
  const cols = document.getElementById('what-changed-cols');
  const container = cols ? cols.parentNode : null;
  const broader = label ? getBroaderFallbackFilter(label) : null;
  showInlineNoResultsMessage(container, msg, broader);
}

// ── Overview focus: clean base pool for What Changed count calculations ──
// Returns full noise-filtered dataset with NO active pF/pCatFilter applied.
// This prevents an active focus from distorting the trend counts.
function _wcBasePool() {
  return unifiedFilteredCitations({ noiseFilter: true });
}

// Set an overview focus: applies the filter, shows the banner, re-renders
// overview sections.  groupedCount is pre-computed from _wcBasePool so the
// banner count is not affected by the filter being applied.
function setOverviewFocus(label, filterObj, groupedCount) {
  _overviewFocus = { label, filterObj, groupedCount };
  applyPharmaFilter(filterObj);
  renderPharmaFocusBanner();
  const data = filteredCits();
  renderStartHereActions(data);
  renderTopRiskBlocks(rankPharmaRisks(data));
  renderAuthBars('pharma-auth-bars');
  _renderOvFeed(data, true);
}

// Clear the overview focus: reverses only the filter fields the focus set.
function clearOverviewFocus() {
  const f = _overviewFocus;
  _overviewFocus = null;
  if (f && f.filterObj) {
    if (f.filterObj.primary_gmp_category !== undefined) pCatFilter = null;
    if (f.filterObj.failure_mode         !== undefined) pF.failureMode = '';
    if (f.filterObj.authority            !== undefined) pF.auth = 'all';
    if (f.filterObj.source_type          !== undefined) pF.srctype = 'all';
    _dirty = true;
  }
  renderPharmaFocusBanner();
  syncPFPills();
  const data = filteredCits();
  renderStartHereActions(data);
  renderTopRiskBlocks(rankPharmaRisks(data));
  renderAuthBars('pharma-auth-bars');
  _renderOvFeed(data, false);
}

// Render (or hide) the overview focus banner into #pharma-focus-banner.
function renderPharmaFocusBanner() {
  const el = document.getElementById('pharma-focus-banner'); if (!el) return;
  const f  = _overviewFocus;
  if (!f) { el.style.display = 'none'; el.innerHTML = ''; return; }

  const allCits = filteredCits();
  const { evidenceBacked, broader } = _focusEvidenceBacked(allCits, f.label);
  const evCount = evidenceBacked.length;
  const brCount = broader.length;
  const fs = JSON.stringify(f.filterObj).replace(/"/g, "'");

  // Count label: show evidence-backed vs broader split if the focus has evidenceTerms mapping.
  const hasSplit = !!(_TREND_FILTER_MAP[f.label] || {}).evidenceTerms;
  const countLine = hasSplit
    ? `<span style="font-size:11px;color:#4a6278;white-space:nowrap">
        ${evCount} evidence-backed record${evCount!==1?'s':''}
        &middot; ${f.groupedCount} in source trend pool
        ${brCount > 0 ? `<span style="color:#9aacbb"> (${brCount} without direct evidence)</span>` : ''}
       </span>`
    : `<span style="font-size:11px;color:#4a6278;white-space:nowrap">
        ${f.groupedCount} grouped finding${f.groupedCount!==1?'s':''} &middot;
        ${allCits.length} citation${allCits.length!==1?'s':''}
       </span>`;

  el.style.display = 'block';
  el.innerHTML = `<div style="display:flex;align-items:center;flex-wrap:wrap;gap:10px;
      padding:10px 14px;margin-bottom:12px;background:rgba(13,148,136,.07);
      border:1px solid rgba(13,148,136,.25);border-radius:7px">
    <button class="btn-secondary" style="font-size:11px;font-weight:700;padding:4px 10px;
        flex-shrink:0;border-color:rgba(13,148,136,.4);color:#0d9488"
      onclick="clearOverviewFocus()">&#10005; Clear focus</button>
    <span style="font-size:12px;font-weight:700;color:#2A3E52;flex:1;min-width:160px">
      Focused view: ${f.label}</span>
    ${countLine}
    <button class="btn-secondary" style="font-size:11px;flex-shrink:0"
      onclick="navigateToCitationsWithFilter(${fs})">View all ${allCits.length} in Citations &rarr;</button>
  </div>`;
}

// Expand an inline broader-evidence panel inside the What Changed widget.
// Toggle: clicking the same label again closes it.
function _showWcBroaderPanel(label, broaderCount) {
  const panel = document.getElementById('what-changed-panel'); if (!panel) return;
  const existing = panel.querySelector('.wc-broader-panel');
  if (existing) {
    const same = existing.dataset.label === label;
    existing.remove();
    if (same) return;
  }

  const pool    = _wcBasePool().filter(c => (c.category||'') === label);
  const auths   = {}, srcs = {};
  pool.forEach(c => {
    if (c.authority) auths[c.authority] = (auths[c.authority]||0)+1;
    const s = (c.source_type||'other').replace(/_/g,' ');
    srcs[s] = (srcs[s]||0)+1;
  });
  const topAuths = Object.entries(auths).sort((a,b)=>b[1]-a[1]).slice(0,4)
    .map(([k,v])=>`${k} (${v})`).join(' · ');
  const topSrcs  = Object.entries(srcs).sort((a,b)=>b[1]-a[1]).slice(0,3)
    .map(([k,v])=>`${k} (${v})`).join(' · ');
  const top5 = pool.slice(0,5);

  const broader = getBroaderFallbackFilter(label);
  const viewBtn = broader
    ? `<button class="btn-secondary" style="font-size:10px"
        onclick="navigateToCitationsWithFilter(${JSON.stringify(broader).replace(/"/g,"'")})">View source records &rarr;</button>`
    : '';

  const el = document.createElement('div');
  el.className   = 'wc-broader-panel';
  el.dataset.label = label;
  el.style.cssText = 'margin-top:10px;padding:12px 14px;background:rgba(47,69,88,.06);' +
    'border-radius:6px;border:1px solid rgba(47,69,88,.12)';
  el.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
      <div style="font-size:12px;font-weight:700;color:#2A3E52">Source trend: ${label}</div>
      <button class="btn-secondary" style="font-size:10px;padding:2px 8px"
        onclick="this.closest('.wc-broader-panel').remove()">&#10005; Close</button>
    </div>
    <div style="font-size:11px;color:#2F4558;margin-bottom:8px;line-height:1.5">
      ${broaderCount} record${broaderCount!==1?'s':''} appear in the source trend data for this
      category, but are not currently classified as decision-ready grouped records.
    </div>
    ${topAuths?`<div style="font-size:10px;color:#7A92A8;margin-bottom:3px"><b>Authorities:</b> ${topAuths}</div>`:''}
    ${topSrcs ?`<div style="font-size:10px;color:#7A92A8;margin-bottom:8px"><b>Source types:</b> ${topSrcs}</div>`:''}
    ${top5.length?`<div style="font-size:10px;font-weight:600;color:#2A3E52;margin-bottom:4px">Sample records</div>
      ${top5.map(c=>`<div style="font-size:10px;color:#2F4558;padding:3px 0;
        border-bottom:1px solid rgba(47,69,88,.06)">
        ${(c.summary||c.category||'').slice(0,110)}
        <span style="color:#7A92A8;margin-left:4px">${c.date?c.date.slice(0,10):''}</span>
      </div>`).join('')}`:''}
    <div style="margin-top:10px">${viewBtn}</div>`;
  panel.appendChild(el);
}

// ── What Changed This Period ──────────────────────────────────────────
function renderWhatChanged() {
  const el=document.getElementById('what-changed-cols'); if(!el)return;
  const {recentFrom,priorFrom,priorTo}=_computeDateWindows();
  const _pc=getPharmaCitations();
  const recent=_pc.filter(c=>c.date&&c.date>=recentFrom);
  const prior=_pc.filter(c=>c.date&&c.date>=priorFrom&&c.date<priorTo);

  // Category changes — only show substantive shifts
  const catR={},catP={};
  recent.forEach(c=>{const k=c.category||'Other';catR[k]=(catR[k]||0)+1;});
  prior.forEach(c=>{const k=c.category||'Other';catP[k]=(catP[k]||0)+1;});
  const changes=Object.keys({...catR,...catP})
    .map(k=>({k,r:catR[k]||0,p:catP[k]||0,d:(catR[k]||0)-(catP[k]||0)}))
    .filter(x=>x.p>10&&Math.abs(x.d)>15)
    .sort((a,b)=>Math.abs(b.d)-Math.abs(a.d)).slice(0,5);
  const newCats=Object.keys(catR).filter(k=>!catP[k]&&catR[k]>=50).slice(0,3);

  // Authority changes
  const authR={},authP={};
  recent.forEach(c=>{authR[c.authority]=(authR[c.authority]||0)+1;});
  prior.forEach(c=>{authP[c.authority]=(authP[c.authority]||0)+1;});
  const authChanges=Object.keys({...authR,...authP})
    .map(k=>({k,r:authR[k]||0,p:authP[k]||0,d:(authR[k]||0)-(authP[k]||0)}))
    .filter(x=>Math.abs(x.d)>5)
    .sort((a,b)=>Math.abs(b.d)-Math.abs(a.d)).slice(0,4);

  // Clean base pool for count calculations — no pF filters, noise-filtered only.
  // Computed once here so _mkCatItem calls are cheap.
  const basePool       = _wcBasePool();
  const basePrimaries  = basePool.filter(c => c.cluster_primary !== false);

  // Pre-compute grouped and broader counts for a category label.
  function _wcCounts(label) {
    const mapping = _TREND_FILTER_MAP[label];
    let best = null;
    if (mapping) {
      const matched = basePrimaries.filter(c => _trendMatchRecord(c, mapping));
      if (matched.length) best = _trendBestFilter(matched, mapping);
    }
    if (!best) {
      const direct = basePrimaries.filter(c => (c.primary_gmp_category||c.category||'') === label);
      if (direct.length) best = { primary_gmp_category: label };
    }
    const groupedCount  = best ? _countMatchingInPool(best, basePrimaries) : 0;
    const broaderCount  = basePool.filter(c => (c.category||'') === label).length;
    return { groupedCount, broaderCount, best };
  }

  // Render one category trend row with the correct type (A/B/C).
  function _mkCatItem(arrow, cls, label, delta) {
    const { groupedCount, broaderCount, best } = _wcCounts(label);
    const labelSafe  = label.replace(/'/g, "\\'");
    const deltaStr   = delta !== null ? (delta>0?`+${delta}`:`${delta}`) : 'new';
    const isActive   = _overviewFocus && _overviewFocus.label === label;

    if (groupedCount > 0) {
      // Type A — has grouped decision records: set overview focus on click
      const fs = JSON.stringify(best).replace(/"/g, "'");
      return `<div class="wc-item${isActive?' wc-item-active':''}" style="cursor:pointer"
          onclick="setOverviewFocus('${labelSafe}',${fs},${groupedCount})"
          title="${groupedCount} grouped decision record${groupedCount!==1?'s':''} — click to focus overview">
        <span class="wc-arrow ${cls}">${arrow}</span>
        <span class="wc-label">${label}</span>
        <span class="wc-delta">${deltaStr}</span>
        <span style="font-size:9px;color:#0d9488;margin-left:4px;white-space:nowrap">&#9679; ${groupedCount} grouped</span>
      </div>`;
    }
    if (broaderCount > 0) {
      // Type B — source trend only, no grouped records: show inline panel on click
      return `<div class="wc-item" style="cursor:pointer;opacity:.75"
          onclick="_showWcBroaderPanel('${labelSafe}',${broaderCount})"
          title="Source trend data only — no grouped decision records">
        <span class="wc-arrow ${cls}" style="opacity:.5">${arrow}</span>
        <span class="wc-label">${label}</span>
        <span class="wc-delta">${deltaStr}</span>
        <span style="font-size:9px;color:#7A92A8;margin-left:4px;white-space:nowrap">source trend only</span>
      </div>`;
    }
    // Type C — nothing to show: omit
    return '';
  }

  const upChanges=changes.filter(x=>x.d>0);
  const dnChanges=changes.filter(x=>x.d<0);

  const col1=`<div class="wc-col-hd">Rising categories</div>`
    +upChanges.map(x=>_mkCatItem('▲','up',x.k,x.d)).join('')
    +(newCats.length?newCats.map(k=>_mkCatItem('★','new',k,null)).join(''):'');
  const col2=`<div class="wc-col-hd">Decreasing categories</div>`
    +(dnChanges.length?dnChanges.map(x=>_mkCatItem('▼','down',x.k,x.d)).join('')
      :'<div class="wc-item" style="color:#2F4558;font-size:10px">No significant decreases</div>');

  // TODO(Phase 2): authority trend clicks navigate unconditionally — add hasResultsForFilter guard
  // and replace with an inline focus panel (same pattern as Enforcement category cards).
  const authFn = k => `applyPharmaFilter({authority:'${k}'}); scrollToCitationsTable()`;
  const col3=`<div class="wc-col-hd">Authority activity</div>`
    +authChanges.map(x=>`<div class="wc-item" style="cursor:pointer"
      onclick="${authFn(x.k)}" title="Click to filter to ${x.k}">
      <span class="wc-arrow ${x.d>0?'up':'down'}">${x.d>0?'▲':'▼'}</span>
      <span class="wc-label">${x.k}</span>
      <span class="wc-delta">${x.d>0?'+'+x.d:x.d}</span>
    </div>`).join('');

  el.innerHTML=col1?`<div>${col1}</div><div>${col2}</div><div>${col3}</div>`
    :'<div class="wc-item" style="color:#2F4558">Insufficient data for comparison</div>';
}

// === PHARMA CITATIONS + ENFORCEMENT RENDER ===

function renderAuthBars(elId) {
  // Update heading to show focus context so counts are not mistaken for full-dataset totals.
  const hd = document.getElementById('pharma-auth-bars-hd');
  if (hd) hd.textContent = _overviewFocus
    ? `Authority Activity: ${_overviewFocus.label}`
    : 'Authority Activity';

  const data=filteredCits();
  const ac={}, catByAuth={};
  data.forEach(c=>{
    ac[c.authority]=(ac[c.authority]||0)+1;
    if(!catByAuth[c.authority]) catByAuth[c.authority]={};
    const k=c.category||'Other';
    catByAuth[c.authority][k]=(catByAuth[c.authority][k]||0)+1;
  });
  const mx=Math.max(...Object.values(ac),1);
  const el=document.getElementById(elId); if(!el) return;
  el.innerHTML=Object.entries(ac).sort((a,b)=>b[1]-a[1]).map(([auth,ct])=>{
    const topCats=Object.entries(catByAuth[auth]||{}).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k,v])=>`${k} (${v})`).join(', ');
    const tip=`${auth}: ${ct} citation${ct!==1?'s':''}. Top: ${topCats}. Click to filter — click again to clear.`;
    const isActive=pF.auth===auth;
    // TODO(Phase 2): authority bar clicks navigate unconditionally — replace with
    // setEnforcementFocus({type:'authority', value}) to stay in context (Phase 2 rewrite).
    return `<div class="enf-row${isActive?' er-active':''}" onclick="applyPharmaFilter({authority:'${auth}'}); scrollToCitationsTable()" style="cursor:pointer" title="${tip}">
      <span class="enf-name">${auth}</span>
      <div class="enf-bar-wrap"><div class="enf-bar-fill" style="width:${Math.round(ct/mx*100)}%"></div></div>
      <span class="enf-ct">${ct}</span>
    </div>`;
  }).join('');
}

// ── Cluster-aware citation grouping ──────────────────────────────────
// Uses cluster_id / cluster_primary / cluster_size fields written by compute_clusters()
// (Python Step 7) to collapse multi-record clusters into a single primary row with
// a _clusterMembers array.  Falls back to the legacy coarse grouping for singletons.
function _pharmaClusterGroupCits(cits) {
  const clusterMap = {};   // cluster_id -> { primary, members }
  const result = [];
  const seenClusters = new Set();

  // First pass: organise clustered records
  cits.forEach(c => {
    if (!c.cluster_id || (c.cluster_size || 1) <= 1) return;
    if (!clusterMap[c.cluster_id]) clusterMap[c.cluster_id] = { primary: null, members: [] };
    if (c.cluster_primary) clusterMap[c.cluster_id].primary = c;
    clusterMap[c.cluster_id].members.push(c);
  });

  // If primary was filtered out, promote the best available member
  Object.values(clusterMap).forEach(g => {
    if (!g.primary && g.members.length) g.primary = g.members[0];
  });

  // Second pass: output in original order, collapsing clusters
  cits.forEach(c => {
    if (!c.cluster_id || (c.cluster_size || 1) <= 1) {
      result.push({ ...c, _clusterMembers: [] });
      return;
    }
    if (seenClusters.has(c.cluster_id)) return;
    seenClusters.add(c.cluster_id);
    const g = clusterMap[c.cluster_id];
    const primary = g.primary;
    const others  = g.members.filter(m => m !== primary);
    result.push({ ...primary, _clusterMembers: others });
  });
  return result;
}

// ── Display-level citation grouping (legacy fallback) ─────────────────
// Groups filteredCits() output by (authority, source_type, month, facility_type, category)
// so that rows with the same category in the same period collapse to one
// visible row with a ×N count badge.  Does NOT touch the raw data.
function _groupCitsForDisplay(cits) {
  const groupMap = {};
  const order = [];
  cits.forEach(c => {
    const category = (c.category||'Other').toLowerCase();
    const dateMonth = (c.date||'').slice(0, 7);  // YYYY-MM
    const facType = (c.facility_type||'').toLowerCase();
    const key = [c.authority||'', c.source_type||'', dateMonth, facType, category].join('|');
    if (!groupMap[key]) {
      groupMap[key] = { ...c, _groupCount: 1 };
      order.push(key);
    } else {
      groupMap[key]._groupCount++;
    }
  });
  return order.map(k => groupMap[k]);
}

// ── Citation card helpers (new fields — all additive, all null-guarded) ──
const _IMPORT_ALERT_TIP = 'Import Alert: regulatory action preventing products from entering a country due to safety or compliance issues (e.g. FDA Import Alert — Detention Without Physical Examination)';

function _citPriorityBadge(c) {
  if (!c.priority) return '';
  const cols = {P1:'#c0392b',P2:'#e67e22',P3:'#7f8c8d',P4:'#aab'};
  const col = cols[c.priority] || '#7f8c8d';
  return `<span class="badge" style="background:transparent;color:${col};border:1px solid ${col};font-size:9px;padding:1px 4px">${c.priority}</span>`;
}

function _citRecurrenceBadge(c) {
  const n = c.recurrence_count_company_90d || 0;
  if (n < 2 || !c.company) return '';
  return `<span title="${n} citations for ${c.company} in last 90 days" style="color:#c0392b;font-size:9px;margin-left:4px">&#9679; ${n} in 90d</span>`;
}

function _citDirectionBadge(c) {
  if (c.signal_direction === 'escalating')
    return `<span style="color:#e67e22;font-size:9px;margin-left:6px">&#8593; Escalating</span>`;
  if (c.signal_direction === 'resolving')
    return `<span style="color:#27ae60;font-size:9px;margin-left:6px">&#8595; Resolving</span>`;
  return '';
}

function _citSecondaryCategories(c) {
  const cats = Array.isArray(c.secondary_gmp_categories) ? c.secondary_gmp_categories : [];
  if (!cats.length) return '';
  return cats.slice(0,3).map(cat =>
    `<span class="badge" style="background:rgba(47,69,88,.05);color:#4a6278;border:1px solid rgba(47,69,88,.12);font-size:9px">${cat}</span>`
  ).join('');
}

// ── Classification trust helpers ─────────────────────────────────────────────
// _NON_ENFORCEMENT_SOURCE_TYPES is defined in core.js (loaded first)

function _isNonEnforcement(c) {
  return _NON_ENFORCEMENT_SOURCE_TYPES.has(c.source_type || '');
}

// Returns display label for a category, applying trust qualifiers.
// confirmed → raw category; provisional → "Provisional: {cat}"; unconfirmed → "Limited detail"
function _displayCategory(c) {
  const cat    = c.primary_gmp_category || c.category || '';
  const status = c.classification_status || '';
  if (!cat || cat === 'Other / Insufficient Detail') return cat;
  if (status === 'unconfirmed') return 'Limited detail';
  if (status === 'provisional') return `Provisional: ${cat}`;
  return cat;
}

// Broad or uninformative categories that should not surface as the primary Issue label.
const _BROAD_CATS = new Set(['GMP violations', 'Other / Insufficient Detail', 'Other', '']);

// Returns the most specific, user-facing "Issue" label for a citation card.
// Priority: specific failure_mode > specific primary_gmp_category > legacy category > "Limited detail"
function getDisplayIssue(c) {
  const fm = c.failure_mode || '';
  if (fm && fm !== 'insufficient_detail' && (c.failure_mode_confidence || 0) >= 0.6) {
    return _fmLabel(fm);
  }
  const pgc    = c.primary_gmp_category || '';
  const status = c.classification_status || '';
  if (pgc && !_BROAD_CATS.has(pgc)) {
    if (status === 'provisional') return `Provisional: ${pgc}`;
    return pgc;
  }
  const cat = c.category || '';
  if (cat && !_BROAD_CATS.has(cat)) {
    if (status === 'unconfirmed') return 'Unclassified';
    if (status === 'provisional') return `Provisional: ${cat}`;
    return cat;
  }
  return 'Unclassified';
}

// Returns a human-readable evidence/classification status for a citation.
function getEvidenceStatus(c) {
  const status = c.classification_status || '';
  if (status === 'confirmed')   return 'Confirmed';
  if (status === 'provisional') return 'Provisional';
  if (status === 'unconfirmed') return 'Limited evidence';
  return 'Unknown';
}

// Canonical predicate: does this citation match a given display issue label?
// Matches the base label AND its "Provisional: X" variant so a card aggregates all evidence levels.
function matchesDisplayIssue(c, issueLabel) {
  if (!issueLabel) return false;
  const issue = getDisplayIssue(c);
  if (issue === issueLabel) return true;
  if (issue === `Provisional: ${issueLabel}`) return true;
  return false;
}

// Tooltip text explaining limited/provisional classification when applicable
function _classificationNote(c) {
  const status = c.classification_status || '';
  const st     = c.source_type || '';
  if (_isNonEnforcement(c))
    return 'Regulatory update / scientific opinion — not an enforcement finding';
  if (status === 'unconfirmed')
    return 'Category based on limited source text — treat as indicative only';
  if (status === 'provisional')
    return 'Category supported by listing-level keywords only — verify against source';
  return '';
}

// Small inline badge shown on non-enforcement and unconfirmed records
function _citTrustNote(c) {
  const note = _classificationNote(c);
  if (!note) return '';
  return `<div style="margin-top:3px"><span style="font-size:9px;color:#7A92A8;font-style:italic" title="${note}">&#9432; ${note}</span></div>`;
}

function _citFailureMode(c) {
  if (!c.failure_mode || (c.failure_mode_confidence || 0) < 0.7) return '';
  return `<div style="margin-top:3px"><span style="font-size:9px;color:#c0392b;opacity:.75">&#9888; ${c.failure_mode.replace(/_/g,' ')}</span></div>`;
}

// ── URL quality classification ────────────────────────────────────────────────
// IRES URLs (accessdata.fda.gov/scripts/ires) open the FDA Enforcement Reports
// search interface, not a specific detail page — classified as search_landing.
// api.fda.gov URLs are raw API endpoints not meant for browser viewing.
function _urlQuality(url) {
  if (!url) return 'missing';
  const u = url.toLowerCase();
  if (u.startsWith('https://api.fda.gov')) return 'api_endpoint';
  if (u.includes('accessdata.fda.gov/scripts/ires')) return 'search_landing';
  return 'direct_detail';
}

// ── Copy-to-clipboard helpers ────────────────────────────────────────────────

function _copyTextFallback(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;top:-9999px;left:-9999px;opacity:0';
  document.body.appendChild(ta);
  ta.select();
  try { document.execCommand('copy'); } catch(e) {}
  document.body.removeChild(ta);
}

// Copy text to clipboard. Briefly changes btn label to "Copied ✓" as feedback.
function _copyText(text, btn) {
  if (!text) return;
  const orig = btn ? btn.textContent : null;
  const done = () => {
    if (!btn || !orig) return;
    btn.textContent = 'Copied ✓';
    setTimeout(() => { btn.textContent = orig; }, 1500);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(done).catch(() => { _copyTextFallback(text); done(); });
  } else {
    _copyTextFallback(text); done();
  }
}

// Escape a string for use in a single-quoted JS inline onclick attribute.
function _escOnclick(s) { return (s||'').replace(/\\/g,'\\\\').replace(/'/g,"\\'"); }

// Extract RecallNumber query param from FDA IRES URL, e.g. "D-0483-2026".
function _citRecallNum(url) {
  const m = (url||'').match(/[?&]RecallNumber=([^&]+)/i);
  return m ? decodeURIComponent(m[1]) : '';
}

// Returns footer action buttons: copy affordances + view link, per URL quality.
function _citViewBtn(c) {
  const url = c.url || '';
  const q   = url ? _urlQuality(url) : 'missing';
  const copyUrlBtn = url
    ? `<button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(url)}',this)" title="Copy source URL">⎘ URL</button>`
    : '';
  if (q === 'search_landing') {
    const recall = _citRecallNum(url);
    const copyRecallBtn = recall
      ? `<button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(recall)}',this)" title="Copy recall number for FDA search">⎘ Recall #</button>`
      : copyUrlBtn;
    return `${copyRecallBtn}<a class="card-action" href="${url}" target="_blank"
        onclick="event.stopPropagation()"
        title="Opens the FDA Enforcement Reports search page &#8212; not a direct record page"
      >FDA search &#8599;</a>`;
  }
  if (q === 'api_endpoint')
    return `${copyUrlBtn}<a class="card-action" href="${url}" target="_blank" onclick="event.stopPropagation()"
        title="API data source &#8212; not a human-readable page">Source data &#8599;</a>`;
  if (q === 'missing') return '';
  return `${copyUrlBtn}<a class="card-action card-action-primary" href="${url}" target="_blank" onclick="event.stopPropagation()">View source &#8599;</a>`;
}

// ── Focus evidence split ──────────────────────────────────────────────────────
// Splits a record array into evidence-backed vs broader for the current focus label.
// evidence-backed = confirmed or provisional AND has ≥1 focus-relevant evidence term.
// broader         = unconfirmed OR no evidence terms.
function _focusEvidenceBacked(data, label) {
  const mapping = _TREND_FILTER_MAP[label];
  if (!mapping || !mapping.evidenceTerms) return { evidenceBacked: data, broader: [] };
  const terms = mapping.evidenceTerms;
  const evidenceBacked = [], broader = [];
  data.forEach(c => {
    const status = c.classification_status || '';
    if (status === 'unconfirmed') { broader.push(c); return; }
    const evArr  = [].concat(c.category_evidence || [], c.failure_mode_evidence || []);
    const evText = evArr.join(' ').toLowerCase();
    const hasEv  = terms.some(t => evText.includes(t));
    (hasEv ? evidenceBacked : broader).push(c);
  });
  return { evidenceBacked, broader };
}

// NOTE: c.regulatory_pressure is intentionally not rendered at citation-card level.
// Citation-level trend direction is not reliable enough for user-facing display
// (84% of records show "decreasing" on a full corpus run due to baseline skew).
// The field is audit/debug data only — see citation_audit.json regulatory_pressure section.

function _citAiBox(c) {
  // Only render when accepted AI output is present (classification_confidence is AI-only).
  // Fall back to ai_confidence for records written before this schema change.
  // For cluster primaries with no AI, fall back to best-confidence member AI (conf >= 0.7).
  let conf = c.classification_confidence || c.ai_confidence || 0;
  let summary = c.decision_summary || c.ai_summary;
  let fromMember = false;

  if ((!summary || conf < 0.5) && Array.isArray(c._clusterMembers) && c._clusterMembers.length) {
    let bestConf = 0.7;  // minimum threshold for member fallback
    c._clusterMembers.forEach(m => {
      const mc = m.classification_confidence || m.ai_confidence || 0;
      const ms = m.decision_summary || m.ai_summary;
      if (ms && mc >= bestConf) { bestConf = mc; conf = mc; summary = ms; fromMember = true; }
    });
  }

  if (!summary || conf < 0.5) return '';
  const lowConf = conf < 0.7;
  const style = lowConf
    ? 'background:rgba(47,69,88,.04);border:1px solid rgba(47,69,88,.1);border-radius:4px;padding:6px 8px;margin-top:5px;font-size:10px;color:#6b8099;font-style:italic'
    : 'background:rgba(41,128,185,.06);border:1px solid rgba(41,128,185,.12);border-radius:4px;padding:6px 8px;margin-top:5px;font-size:10px;color:#2c4a63';
  const suffix = lowConf ? ' <em style="opacity:.6">(low confidence)</em>'
    : fromMember ? ' <em style="opacity:.55;font-size:9px">(best available summary from related record)</em>'
    : '';
  const actionText = c.recommended_action || (!lowConf && c.ai_recommended_action) || '';
  const action = (!lowConf && actionText && !fromMember)
    ? `<div style="margin-top:3px;font-size:9px;color:#1a6b8a"><b>Action:</b> ${actionText}</div>`
    : '';
  return `<div style="${style}"><span style="font-size:9px;opacity:.6;font-weight:600;font-style:normal">Intelligence</span> ${summary}${suffix}${action}</div>`;
}

// ── Citation card ────────────────────────────────────────────────────
function citCard(c) {
  const sc=c.severity==='high'?'high-sev':'medium-sev';
  const st=(c.source_type||'').replace(/_/g,' ');
  const isImport=(c.source_type||'')==='import_alert';
  // Prefer LLM clean_title; fall back to family dedup label, then raw summary
  const rawText=(c.summary||'') + ' ' + (c.violation_details||'');
  const family=getPharmaSummaryFamily(rawText);
  const displaySumm=c.clean_title
    || (family ? PHARMA_FAMILY_LABELS[family] : (c.summary||c.category||'Enforcement action').slice(0,140));
  const clusterMembers = c._clusterMembers || [];
  const groupBadge = clusterMembers.length > 0
    ? `<span class="cit-group-badge" title="${clusterMembers.length+1} related records grouped (${c.cluster_reason||''})">×${clusterMembers.length+1}</span>`
    : (c._groupCount||1) > 1
    ? `<span class="cit-group-badge" title="${c._groupCount} similar citations grouped">×${c._groupCount}</span>`
    : '';
  // Issue: most specific useful classification (failure_mode > primary_gmp_category > legacy > "Limited detail")
  // Evidence: human-readable classification_status
  const displayIssue  = getDisplayIssue(c);
  const evidenceStatus = getEvidenceStatus(c);
  const isLimited     = displayIssue === 'Unclassified';
  const isProvisional = displayIssue.startsWith('Provisional:');
  const issueStyle    = isLimited || isProvisional ? ' style="color:#7A92A8;font-style:italic"' : '';
  const evStyle       = evidenceStatus === 'Confirmed' ? 'color:#2ecc71' : evidenceStatus === 'Provisional' ? 'color:#f39c12' : 'color:#7A92A8';
  // Raw classification fields shown in collapsible detail (power-user transparency)
  const rawCat    = c.category || '';
  const rawPgc    = c.primary_gmp_category || '';
  const rawFm     = c.failure_mode || '';
  const rawStatus = c.classification_status || '';
  const rawDetail = [
    rawCat  ? `Legacy category: ${rawCat}` : '',
    rawPgc  ? `GMP category: ${rawPgc}` : '',
    rawFm   ? `Failure mode: ${rawFm}` : '',
    rawStatus ? `Class. status: ${rawStatus}` : '',
  ].filter(Boolean).join(' · ');
  // Debug tooltip shows enrichment status when window.SIGNALEX_DEBUG = true
  const debugTip = (typeof SIGNALEX_DEBUG !== 'undefined' && SIGNALEX_DEBUG)
    ? ` title="enrichment: ${c.enrichment_status||'—'} | ${c.enrichment_source||'—'} | priority: ${c.priority||'—'} | class_status: ${c.classification_status||'—'}"` : '';
  return `<div class="signal-card ${sc}" style="margin-bottom:8px"${debugTip}>
    <div class="card-top"><div class="card-title">${displaySumm}${groupBadge}</div></div>
    <div class="card-badges">
      ${_citPriorityBadge(c)}
      <span class="badge badge-authority">${c.authority||'—'}</span>
      <span class="badge badge-type"${isImport?` title="${_IMPORT_ALERT_TIP}"`:''} style="${isImport?'cursor:help':''}">${st}</span>
      ${c.facility_type?`<span class="badge" style="background:rgba(139,92,246,.06);color:rgba(139,92,246,.55);border:1px solid rgba(139,92,246,.1)">${c.facility_type}</span>`:''}
    </div>
    <div class="card-summary" style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap">
      <span${issueStyle}>${displayIssue}</span>
      <span style="font-size:9px;${evStyle};font-style:italic">${evidenceStatus}</span>
      ${_citSecondaryCategories(c)}
    </div>
    ${_citTrustNote(c)}
    ${c.company?`<div class="card-summary" style="color:#3D5268;font-size:10px">Company: ${c.company}${_citRecurrenceBadge(c)}${_citDirectionBadge(c)}<button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(c.company)}',this)" title="Copy entity name">&#8669;</button></div>`:''}
    ${_citAiBox(c)}
    <div class="card-footer">
      <div class="card-meta">${c.date?c.date.slice(0,10):'—'} &middot; ${c.country||c.authority||''}</div>
      <div class="card-actions">
        ${_citViewBtn(c)}
      </div>
    </div>
    <details style="margin-top:4px">
      <summary style="font-size:9px;color:#7A92A8;cursor:pointer;list-style:none">&#9432; Details &amp; copy</summary>
      <div style="font-size:9px;color:#7A92A8;padding:4px 0 2px;display:flex;flex-direction:column;gap:3px">
        ${rawDetail?`<span>${rawDetail}</span>`:''}
        ${(c.summary||c.clean_title)?`<span style="display:flex;align-items:center;gap:4px">Summary <button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(c.clean_title||c.summary||'')}',this)">&#8669; Copy</button></span>`:''}
        ${c.violation_details?`<span style="display:flex;align-items:center;gap:4px">Violation detail <button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(c.violation_details)}',this)">&#8669; Copy</button></span>`:''}
        ${c.url?`<span style="display:flex;align-items:center;gap:4px;word-break:break-all">${c.url.slice(0,60)}${c.url.length>60?'…':''} <button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(c.url)}',this)">&#8669; Copy URL</button></span>`:''}
      </div>
    </details>
  </div>`;
}

// ── Citations insight strip ──────────────────────────────────────────
function renderCitationsInsightStrip() {
  const el=document.getElementById('cit-insight-strip'); if(!el) return;
  const data=filteredCits();
  if(!data.length){el.innerHTML='';return;}
  const catCounts={},authCounts={},srcCounts={};
  data.forEach(c=>{
    const k=c.category||'Other'; catCounts[k]=(catCounts[k]||0)+1;
    authCounts[c.authority]=(authCounts[c.authority]||0)+1;
    const s=(c.source_type||'other').replace(/_/g,' '); srcCounts[s]=(srcCounts[s]||0)+1;
  });
  const topCat=Object.entries(catCounts).sort((a,b)=>b[1]-a[1])[0];
  const topAuth=Object.entries(authCounts).sort((a,b)=>b[1]-a[1])[0];
  const topSrc=Object.entries(srcCounts).sort((a,b)=>b[1]-a[1])[0];
  const p1Count=data.filter(c=>c.priority==='P1').length;
  const action=topCat&&CAT_INTEL[topCat[0]]?CAT_INTEL[topCat[0]].action:'Review enforcement actions relevant to your site profile.';
  el.innerHTML=`<div class="insight-strip">
    <div class="insight-strip-hd">What this table shows</div>
    <div class="insight-strip-bullets">
      <div class="isb"><div class="isb-dot${p1Count>0?' isb-dot-red':''}"></div><div class="isb-text"><b>${data.length} citation${data.length!==1?'s':''}</b> match current filters${p1Count>0?` — <b>${p1Count} P1 priority</b>`:''}.${data.length===getPharmaCitations().length?' Full dataset — use sidebar or KPI cards to filter.':''}</div></div>
      ${topCat?`<div class="isb"><div class="isb-dot isb-dot-amber"></div><div class="isb-text"><b>${topCat[0]}</b> is the dominant category (${topCat[1]} citations). ${action}</div></div>`:''}
      ${topAuth?`<div class="isb"><div class="isb-dot"></div><div class="isb-text"><b>${topAuth[0]}</b> leads by authority (${topAuth[1]} actions). Primary source type: <b>${topSrc?topSrc[0]:'—'}</b>.</div></div>`:''}
    </div>
  </div>`;
}

// ── Citations table ──────────────────────────────────────────────────
// Compact source cell for the citations table: copy affordance + view link.
function _citTableSource(c) {
  const url = c.url || '';
  if (!url) return '—';
  const q = _urlQuality(url);
  const recall = q === 'search_landing' ? _citRecallNum(url) : '';
  const copyTarget = recall || url;
  const copyLbl = recall ? `⎘ ${recall.slice(0,10)}` : '⎘';
  const copyBtn = `<button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(copyTarget)}',this)" title="${recall ? 'Copy recall number' : 'Copy URL'}">${copyLbl}</button>`;
  if (q === 'search_landing')
    return `${copyBtn} <a class="cit-link" href="${url}" target="_blank" onclick="event.stopPropagation()" title="Opens FDA search page — not a direct record">FDA &#8599;</a>`;
  if (q === 'api_endpoint')
    return `${copyBtn} <a class="cit-link" href="${url}" target="_blank" onclick="event.stopPropagation()" title="API data source">API &#8599;</a>`;
  return `${copyBtn} <a class="cit-link cit-link-primary" href="${url}" target="_blank" onclick="event.stopPropagation()">&#8599;</a>`;
}

function _citTableRow(c, isExpanded) {
  const st = (c.source_type||'').replace(/_/g,' ');
  const entity = (c.company || c.cluster_label || c.authority || '').slice(0, 45);
  const displayIssue = getDisplayIssue(c);
  const evidSt = getEvidenceStatus(c);
  const isLimitedIssue = displayIssue === 'Unclassified';
  const isProvIssue = displayIssue.startsWith('Provisional:');
  const issueStyle = isLimitedIssue || isProvIssue ? 'font-size:10px;max-width:120px;color:#9aacbb;font-style:italic' : 'font-size:10px;max-width:120px';
  const evStyle = evidSt === 'Confirmed' ? 'font-size:9px;color:#2ecc71;font-style:italic' : evidSt === 'Provisional' ? 'font-size:9px;color:#f39c12;font-style:italic' : 'font-size:9px;color:#7A92A8;font-style:italic';
  const ds = (c.decision_summary || '').slice(0,130) || (c.raw_listing_summary || c.summary || '').slice(0,110);
  const members = c._clusterMembers || [];
  const hasCluster = members.length > 0;
  const clusterBadge = hasCluster
    ? ` <span class="cit-group-badge" title="${members.length+1} related records — ${c.cluster_reason||''}">&#215;${members.length+1}</span>`
    : '';
  const toggleBtn = hasCluster
    ? `<button class="cit-cluster-toggle" onclick="event.stopPropagation();_toggleClusterRow(this)" data-expanded="0" style="background:none;border:none;cursor:pointer;font-size:9px;color:#4a6278;padding:0 4px;vertical-align:middle" title="Expand related records">&#9654;</button>`
    : '';
  const prio = c.priority || '';
  const prioHtml = prio
    ? `<span style="color:${_PRIORITY_COLORS[prio]||'#7f8c8d'};font-weight:700;font-size:10px">${prio}</span>` : '—';
  const entityCopy = entity
    ? ` <button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(entity)}',this)" title="Copy entity name">&#8669;</button>`
    : '';
  // Row click expands cluster children (if any); does NOT apply filters.
  const rowClick = hasCluster ? '' : '';
  return `<tr class="cit-row${hasCluster?' cit-cluster-primary':''}">
    <td>${prioHtml}</td>
    <td><span class="badge badge-authority">${c.authority||'—'}</span></td>
    <td><span class="badge badge-type" style="white-space:nowrap">${st}</span></td>
    <td style="color:#3D5268;font-size:10px;max-width:120px;overflow:hidden;text-overflow:ellipsis" title="${entity}">${entity||'—'}${entityCopy}</td>
    <td style="${issueStyle}">${displayIssue}</td>
    <td style="${evStyle}">${evidSt}</td>
    <td style="white-space:nowrap;font-size:10px;color:#3D5268">${c.date?c.date.slice(0,10):'—'}</td>
    <td style="max-width:260px;font-size:11px;color:#7A92A8">${toggleBtn}${ds}${clusterBadge}</td>
    <td style="white-space:nowrap">${_citTableSource(c)}</td>
  </tr>`;
}

function _citMemberRow(m) {
  const st = (m.source_type||'').replace(/_/g,' ');
  const entity = (m.company || m.authority || '').slice(0, 40);
  const displayIssue = getDisplayIssue(m);
  const evidSt = getEvidenceStatus(m);
  const ds = (m.decision_summary || m.raw_listing_summary || m.summary || '').slice(0, 110);
  const prio = m.priority || '';
  const prioHtml = prio ? `<span style="color:${_PRIORITY_COLORS[prio]||'#7f8c8d'};font-size:9px">${prio}</span>` : '';
  return `<tr class="cit-cluster-member" style="background:rgba(74,98,120,.035);font-size:10px">
    <td>${prioHtml}</td>
    <td><span class="badge badge-authority" style="opacity:.7">${m.authority||'—'}</span></td>
    <td><span class="badge badge-type" style="white-space:nowrap;opacity:.7">${st}</span></td>
    <td style="color:#6b8099;font-size:9px;padding-left:14px" title="${entity}">${entity||'—'}</td>
    <td style="font-size:9px;color:#9aacbb;font-style:italic">${displayIssue}</td>
    <td style="font-size:9px;color:#7A92A8;font-style:italic">${evidSt}</td>
    <td style="white-space:nowrap;font-size:9px;color:#6b8099">${m.date?m.date.slice(0,10):'—'}</td>
    <td style="max-width:260px;font-size:10px;color:#9aacbb">${ds}</td>
    <td>${m.url?`<a class="cit-link" href="${m.url}" target="_blank" onclick="event.stopPropagation()">&#8599;</a>`:'—'}</td>
  </tr>`;
}

function _toggleClusterRow(btn) {
  const expanded = btn.dataset.expanded === '1';
  const newState = !expanded;
  btn.dataset.expanded = newState ? '1' : '0';
  btn.textContent = newState ? '▼' : '▶';
  // Show/hide sibling member rows following this primary row
  let sibling = btn.closest('tr').nextElementSibling;
  while (sibling && sibling.classList.contains('cit-cluster-member')) {
    sibling.style.display = newState ? '' : 'none';
    sibling = sibling.nextElementSibling;
  }
}

function _syncIssueFilterDropdown() {
  const sel = document.getElementById('cit-issue-select');
  const clrBtn = document.getElementById('cit-issue-clear');
  if (!sel) return;

  // Collect distinct display issues from all citations (not just filtered)
  const all = getPharmaCitations();
  const issueSet = new Set();
  const _SKIP_ISSUES = new Set(['Unclassified', 'Limited detail', 'GMP violations', 'Other / Insufficient Detail', 'Other', '']);
  all.forEach(c => {
    let issue = getDisplayIssue(c);
    if (issue.startsWith('Provisional: ')) issue = issue.slice('Provisional: '.length);
    if (!_SKIP_ISSUES.has(issue)) issueSet.add(issue);
  });
  const sorted = Array.from(issueSet).sort();

  // Rebuild options preserving current selection
  const current = pF.displayIssue || '';
  sel.innerHTML = '<option value="">All issues</option>' +
    sorted.map(iss => `<option value="${iss}"${iss === current ? ' selected' : ''}>${iss}</option>`).join('');

  // Show/hide clear button
  if (clrBtn) clrBtn.style.display = current ? 'inline-block' : 'none';
}

function renderCitations() {
  buildChipBar('cit-chip-bar');
  _syncIssueFilterDropdown();
  renderCitationsInsightStrip();
  const clustered = _pharmaClusterGroupCits(filteredCits());
  const data = clustered;
  const pages=Math.ceil(data.length/CIT_PP);
  citPg=Math.min(citPg,pages||1);
  const slice=data.slice((citPg-1)*CIT_PP,citPg*CIT_PP);
  const tb=document.getElementById('cit-tbody'); if(!tb) return;
  const cc=document.getElementById('cit-count');
  if(cc) {
    const clusterCount = data.filter(c=>(c._clusterMembers||[]).length>0).length;
    cc.textContent = clusterCount
      ? `${data.length} citation rows (${clusterCount} clusters)`
      : `${data.length} citation rows`;
  }
  if(!slice.length) {
    // Build readable list of active filters
    const activeFilters = [];
    if (pCatFilter)             activeFilters.push(['Category',           pCatFilter]);
    if (pF.failureMode)         activeFilters.push(['Failure mode',       _fmLabel(pF.failureMode)]);
    if (pF.auth    !== 'all')   activeFilters.push(['Authority',          pF.auth]);
    if (pF.srctype !== 'all')   activeFilters.push(['Source type',        pF.srctype.replace(/_/g,' ')]);
    if (pF.priority !== 'all' && pF.priority)  activeFilters.push(['Priority',           pF.priority]);
    if (pF.factype !== 'all')   activeFilters.push(['Facility type',      pF.factype]);
    if (pF.company)             activeFilters.push(['Entity',             pF.company]);
    if (pF.query)               activeFilters.push(['Search',             `"${pF.query}"`]);
    if (pF.dateFrom)            activeFilters.push(['From',               pF.dateFrom]);
    if (pF.dateTo)              activeFilters.push(['To',                 pF.dateTo]);
    const filterRows = activeFilters.length
      ? `<div style="display:inline-block;text-align:left;margin:8px auto 10px;font-size:11px">
          ${activeFilters.map(([k,v])=>`<div style="padding:1px 0"><span style="color:#7A92A8;min-width:90px;display:inline-block">${k}:</span> <span style="color:#2A3E52">${v}</span></div>`).join('')}
        </div>`
      : '';
    tb.innerHTML=`<tr><td colspan="9" style="text-align:center;padding:24px 0;color:#2A3E52;font-size:12px">
      <div style="font-size:22px;margin-bottom:6px">&#128270;</div>
      <div>No grouped records match${activeFilters.length ? ':' : ' the current filters.'}</div>
      ${filterRows}
      <button class="btn-secondary" style="display:inline-block;margin:4px auto 0" onclick="resetPharmaFilters()">Clear filters</button>
    </td></tr>`;
    document.getElementById('cit-pagination').innerHTML='';
    return;
  }
  const rows = [];
  slice.forEach(c => {
    rows.push(_citTableRow(c, false));
    // Render member rows hidden; toggled by _toggleClusterRow
    (c._clusterMembers || []).forEach(m => {
      rows.push(_citMemberRow(m).replace('<tr ', '<tr style="display:none" '));
    });
  });
  tb.innerHTML = rows.join('');
  const pg=document.getElementById('cit-pagination'); if(!pg) return; pg.innerHTML='';
  if(pages>1){
    const mkB=(l,p,a)=>{const b=document.createElement('button');b.className='page-btn'+(a?' active':'');b.textContent=l;b.onclick=()=>{citPg=p;renderCitations()};return b};
    if(citPg>1)pg.appendChild(mkB('‹',citPg-1,false));
    for(let i=1;i<=pages;i++){
      if(i===1||i===pages||Math.abs(i-citPg)<=2)pg.appendChild(mkB(i,i,i===citPg));
      else if(Math.abs(i-citPg)===3){const s=document.createElement('span');s.textContent='…';s.style.color='#2F4558';pg.appendChild(s);}
    }
    if(citPg<pages)pg.appendChild(mkB('›',citPg+1,false));
  }
}


// ── Enforcement insight strip ─────────────────────────────────────────
function renderEnfInsightStrip() {
  const el=document.getElementById('enf-insight-strip'); if(!el) return;
  const data=filteredCits();
  if(!data.length){el.innerHTML='';return;}
  const srcCounts={},catCounts={};
  data.forEach(c=>{
    const s=(c.source_type||'other').replace(/_/g,' ');srcCounts[s]=(srcCounts[s]||0)+1;
    const k=c.category||'Other';catCounts[k]=(catCounts[k]||0)+1;
  });
  const topSrc=Object.entries(srcCounts).sort((a,b)=>b[1]-a[1])[0];
  const topCat=Object.entries(catCounts).sort((a,b)=>b[1]-a[1])[0];
  const importCt=data.filter(c=>c.source_type==='import_alert').length;
  const action=topCat&&CAT_INTEL[topCat[0]]?CAT_INTEL[topCat[0]].action:'Review enforcement actions and prioritise based on your site profile.';
  el.innerHTML=`<div class="insight-strip">
    <div class="insight-strip-hd">Enforcement movement</div>
    <div class="insight-strip-bullets">
      ${topSrc?`<div class="isb"><div class="isb-dot isb-dot-amber"></div><div class="isb-text"><b>${topSrc[0]}</b> is the dominant enforcement source (${topSrc[1]} of ${data.length} actions).</div></div>`:''}
      ${topCat?`<div class="isb"><div class="isb-dot isb-dot-red"></div><div class="isb-text"><b>${topCat[0]}</b> is the primary category. Action: ${action}</div></div>`:''}
      ${importCt>0?`<div class="isb"><div class="isb-dot"></div><div class="isb-text"><b>${importCt} import alert${importCt!==1?'s':''}</b> — low in count but high in operational impact. Review foreign supplier verification and incoming material controls.</div></div>`:''}
    </div>
  </div>`;
}

// ── Enforcement page ─────────────────────────────────────────────────
function renderEnfPage() {
  buildChipBar('enf-chip-bar');
  renderEnfInsightStrip();
  renderAuthBars('enf-auth-rows');
  renderCatGrid('enf-cat-grid','enf-cat-count','enforcement');
  const data=filteredCits();
  const el=document.getElementById('enf-srctype-rows'); if(!el) return;
  const sc={};
  data.forEach(c=>{const k=(c.source_type||'other').replace(/_/g,' ');sc[k]=(sc[k]||0)+1;});
  const mx=Math.max(...Object.values(sc),1);
  el.innerHTML=Object.entries(sc).sort((a,b)=>b[1]-a[1]).map(([st,ct])=>{
    const stRaw = st.replace(/ /g,'_');
    return `<div class="enf-row" onclick="setEnforcementFocus({type:'source_type',value:'${stRaw}'})" style="cursor:pointer" title="Click to focus on ${st}">
      <span class="enf-name">${st}</span>
      <div class="enf-bar-wrap"><div class="enf-bar-fill" style="width:${Math.round(ct/mx*100)}%"></div></div>
      <span class="enf-ct">${ct}</span>
    </div>`;
  }).join('');
  renderGroupedEnforcement('enf-grouped-feed');
  renderEnfFocusPanel();
}

// ── Task 2: Enforcement focus panel ──────────────────────────────────
function setEnforcementFocus(focus) {
  const same = selectedEnforcementFocus &&
    selectedEnforcementFocus.type === focus.type &&
    selectedEnforcementFocus.value === focus.value;
  selectedEnforcementFocus = same ? null : focus;
  renderEnfFocusPanel();
  if (selectedEnforcementFocus) {
    requestAnimationFrame(() => {
      const el = document.getElementById('enf-focus-panel');
      if (el) el.scrollIntoView({ behavior:'smooth', block:'start' });
    });
  }
  // Re-render cat grid so active state updates
  renderCatGrid('enf-cat-grid','enf-cat-count','enforcement');
}

function clearEnforcementFocus() {
  selectedEnforcementFocus = null;
  renderEnfFocusPanel();
  renderCatGrid('enf-cat-grid','enf-cat-count','enforcement');
}

function renderEnfFocusPanel() {
  const el = document.getElementById('enf-focus-panel'); if (!el) return;
  const focus = selectedEnforcementFocus;
  if (!focus) { el.style.display='none'; el.innerHTML=''; return; }

  const all = filteredCits();
  const matches = all.filter(c => {
    if (focus.type==='display_issue') return matchesDisplayIssue(c, focus.value);
    if (focus.type==='failure_mode')  return (c.failure_mode||'')===focus.value;
    if (focus.type==='category')      return (c.primary_gmp_category||c.category||'')===focus.value;
    if (focus.type==='source_type')   return (c.source_type||'')===focus.value;
    if (focus.type==='authority')     return (c.authority||'')===focus.value;
    return false;
  });
  const primaries = matches.filter(c => c.cluster_primary !== false);
  const p1 = primaries.filter(c => c.priority==='P1').length;
  const p2 = primaries.filter(c => c.priority==='P2').length;

  const ftCts={}, authCts={};
  matches.forEach(c=>{
    if(c.facility_type) ftCts[c.facility_type]=(ftCts[c.facility_type]||0)+1;
    if(c.authority)    authCts[c.authority]=(authCts[c.authority]||0)+1;
  });
  const topFts   = Object.entries(ftCts).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k).join(', ');
  const topAuths = Object.entries(authCts).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k).join(', ');

  const label  = focus.type==='failure_mode' ? _fmLabel(focus.value) : focus.value;
  const intel  = CAT_INTEL[label];
  const topRec = primaries.find(c=>c.recommended_action);
  const action = (topRec&&topRec.recommended_action)||(intel&&intel.action)||'';
  const filterObj = buildPharmaFilterFromFocus(focus);

  el.style.display = 'block';
  el.innerHTML = `<div style="background:#f7f9fb;border:1px solid rgba(47,69,88,.14);border-radius:6px;padding:14px 16px;margin-top:12px">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:8px">
      <div>
        <div style="font-size:13px;font-weight:700;color:#2A3E52">${label}</div>
        <div style="display:flex;gap:8px;margin-top:3px;flex-wrap:wrap">
          <span style="font-size:10px;color:#4a6278">${primaries.length} grouped finding${primaries.length!==1?'s':''}</span>
          ${p1?`<span style="font-size:10px;color:${_PRIORITY_COLORS.P1};font-weight:600">${p1} P1</span>`:''}
          ${p2?`<span style="font-size:10px;color:${_PRIORITY_COLORS.P2}">${p2} P2</span>`:''}
          ${topFts?`<span style="font-size:10px;color:#7A92A8">${topFts}</span>`:''}
          ${topAuths&&topAuths!==topFts?`<span style="font-size:10px;color:#7A92A8">${topAuths}</span>`:''}
        </div>
      </div>
      <button class="btn-secondary" onclick="clearEnforcementFocus()" style="font-size:10px;white-space:nowrap;flex-shrink:0">&#10005; Clear</button>
    </div>
    ${action?`<div style="font-size:11px;color:#2A3E52;margin-bottom:10px"><b>Recommended action:</b> ${action}</div>`:''}
    <div style="font-size:10px;color:#4a6278;font-weight:600;margin-bottom:4px">Top grouped records</div>
    ${renderTopGroupedRecords(primaries, 5)}
    <div style="margin-top:10px">
      <button class="btn-secondary" onclick="navigateToCitationsWithFilter(${JSON.stringify(filterObj).replace(/"/g,"'")})" style="font-size:11px">View matching citations &rarr;</button>
    </div>
  </div>`;
}

function renderGroupedEnforcement(elId) {
  const el=document.getElementById(elId); if(!el) return;
  const data=filteredCits();
  const {recentFrom,priorFrom,priorTo}=_computeDateWindows();
  const _pc2=getPharmaCitations();
  const recent=_pc2.filter(c=>c.date&&c.date>=recentFrom);
  const prior=_pc2.filter(c=>c.date&&c.date>=priorFrom&&c.date<priorTo);
  const catR={},catP={};
  recent.forEach(c=>{const k=c.category||'Other';catR[k]=(catR[k]||0)+1;});
  prior.forEach(c=>{const k=c.category||'Other';catP[k]=(catP[k]||0)+1;});
  const groups={};
  data.forEach(c=>{const k=c.category||'Other';if(!groups[k])groups[k]=[];groups[k].push(c);});
  const sorted=Object.entries(groups).sort((a,b)=>b[1].length-a[1].length);
  if(!sorted.length){el.innerHTML='<div class="empty"><div class="empty-icon">&#128270;</div><div class="empty-text">No enforcement actions match filters</div></div>';return;}
  el.innerHTML=sorted.map(([cat,items])=>{
    const rCt=catR[cat]||0, pCt=catP[cat]||0;
    const trendHtml=formatTrendMovement(rCt,pCt);
    const intel=CAT_INTEL[cat];
    const actionLine=intel?`<div class="enf-cat-action"><b>Action:</b> ${intel.action}</div>`:'';
    const topItems=items.slice(0,3);
    const safe=cat.replace(/'/g,"\\'");
    return `<details class="enf-group">
      <summary class="enf-group-hd">
        <span class="enf-group-name">${cat}</span>
        <span class="enf-group-ct">${items.length}</span>
        ${trendHtml}
        <span class="enf-group-arrow">&#9660;</span>
      </summary>
      <div class="enf-group-body">
        ${actionLine}
        ${topItems.map(c=>citCard(c)).join('')}
        ${items.length>3?`<button class="enf-view-all-btn" onclick="pFilterByCat('${safe}');showPTab('pharma-citations')">View all ${items.length} &rarr;</button>`:''}
      </div>
    </details>`;
  }).join('');
}

// === PHARMA FACILITIES + ALERTS RENDER ===

function renderFacilityRiskStrip() {
  const el=document.getElementById('fac-risk-strip'); if(!el) return;
  const data=filteredCits();
  const map={};
  data.forEach(c=>{
    const ft=c.facility_type||'Unknown';
    if(!map[ft])map[ft]={total:0,cats:{}};
    map[ft].total++;
    const k=c.category||'Other';map[ft].cats[k]=(map[ft].cats[k]||0)+1;
  });
  const items=Object.entries(map).sort((a,b)=>b[1].total-a[1].total);
  if(!items.length){el.innerHTML='';return;}
  const cards=items.map(([name,d])=>{
    const profile=getFacilityRiskProfile(name);
    const topCat=Object.entries(d.cats).sort((a,b)=>b[1]-a[1])[0];
    const nameSafe = name.replace(/'/g,"\\'");
    const isActive = selectedFacilityFocus === name;
    return `<div class="fac-risk-card${isActive?' enf-focus-active':''}" onclick="setFacilityFocus('${nameSafe}')" style="cursor:pointer" title="View action summary for ${name}">
      <div class="fac-risk-name">${name} <span style="font-size:10px;color:#2A3E52;font-weight:400">(${d.total} citation${d.total!==1?'s':''})</span></div>
      ${topCat?`<div class="fac-risk-row"><div class="fac-risk-lbl">Top finding</div><div class="fac-risk-val">${topCat[0]}</div></div>`:''}
      ${profile?`<div class="fac-risk-row"><div class="fac-risk-lbl">Exposure</div><div class="fac-risk-val">${profile.exposure}</div></div>`:''}
      ${profile?`<div class="fac-risk-row"><div class="fac-risk-lbl">Insp. focus</div><div class="fac-risk-val">${profile.focus}</div></div>`:''}
      <div class="fac-risk-action">Action: ${profile?profile.action:topCat?generateFacilityAction(topCat[0]):'Review facility enforcement profile and prioritise corrective actions.'}</div>
    </div>`;
  }).join('');
  el.innerHTML=`<div class="insight-strip" style="padding-bottom:4px">
    <div class="insight-strip-hd">Facility risk exposure</div>
  </div>
  <div class="fac-risk-grid">${cards}</div>`;
}

// ── Task 5: Facility focus panel ──────────────────────────────────────
function setFacilityFocus(ft) {
  selectedFacilityFocus = selectedFacilityFocus === ft ? null : ft;
  renderFacilityFocusPanel();
  if (selectedFacilityFocus) {
    requestAnimationFrame(() => {
      const el = document.getElementById('fac-focus-panel');
      if (el) el.scrollIntoView({ behavior:'smooth', block:'start' });
    });
  }
  // Re-render strip so active state updates
  renderFacilityRiskStrip();
}

function clearFacilityFocus() {
  selectedFacilityFocus = null;
  renderFacilityFocusPanel();
  renderFacilityRiskStrip();
}

function renderFacilityFocusPanel() {
  const el = document.getElementById('fac-focus-panel'); if (!el) return;
  const ft = selectedFacilityFocus;
  if (!ft) { el.style.display='none'; el.innerHTML=''; return; }

  const all = filteredCits();
  const records = all.filter(c => (c.facility_type||'') === ft);
  if (!records.length) { el.style.display='none'; el.innerHTML=''; return; }

  const primaries  = getGroupedDecisionRecords(records);
  const p1         = records.filter(c => c.priority==='P1').length;
  const p2         = records.filter(c => c.priority==='P2').length;
  const auths      = [...new Set(records.map(c => c.authority).filter(Boolean))];
  const cats       = {};
  records.forEach(c => { const k=c.primary_gmp_category||c.category||'Other'; cats[k]=(cats[k]||0)+1; });
  const topCats    = Object.entries(cats).sort((a,b)=>b[1]-a[1]).slice(0,4);
  const profile    = getFacilityRiskProfile(ft);
  const action     = profile ? profile.action : (topCats.length ? generateFacilityAction(topCats[0][0]) : 'Review enforcement profile and prioritise corrective actions.');

  const filterObj = { facility_type: ft };
  const filterStr = JSON.stringify(filterObj).replace(/"/g,"'");

  el.style.display='block';
  el.innerHTML = `<div class="enf-focus-panel" style="margin-top:12px">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
      <div style="font-size:13px;font-weight:700;color:#2A3E52">${ft}</div>
      <button class="btn-secondary" style="font-size:10px;padding:2px 8px" onclick="clearFacilityFocus()">&#10005; Clear</button>
    </div>
    <div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:8px">
      <span style="font-size:11px;color:#2A3E52"><b>${records.length}</b> citation${records.length!==1?'s':''}</span>
      ${p1?`<span style="font-size:11px;color:#c0392b"><b>${p1}</b> P1</span>`:''}
      ${p2?`<span style="font-size:11px;color:#e67e22"><b>${p2}</b> P2</span>`:''}
      <span style="font-size:11px;color:#7A92A8">${auths.join(' / ')}</span>
    </div>
    ${topCats.length?`<div style="margin-bottom:8px">
      <div style="font-size:10px;font-weight:600;color:#2A3E52;margin-bottom:3px;text-transform:uppercase;letter-spacing:.04em">Top finding categories</div>
      ${topCats.map(([cat,n])=>`<div style="display:flex;justify-content:space-between;font-size:11px;padding:2px 0;border-bottom:1px solid rgba(47,69,88,.05)"><span style="color:#2A3E52">${cat}</span><span style="color:#7A92A8">${n}</span></div>`).join('')}
    </div>`:''}
    <div style="background:rgba(47,69,88,.04);border-radius:6px;padding:8px 10px;margin-bottom:8px">
      <div style="font-size:10px;font-weight:600;color:#2A3E52;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em">Recommended action</div>
      <div style="font-size:11px;color:#2A3E52">${action}</div>
    </div>
    ${primaries.length?`<div style="margin-bottom:8px">
      <div style="font-size:10px;font-weight:600;color:#2A3E52;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em">Top grouped records</div>
      ${renderTopGroupedRecords(primaries,5)}
    </div>`:''}
    <button class="btn-secondary" style="font-size:10px;margin-top:2px"
      onclick="navigateToCitationsWithFilter(${filterStr})">View matching citations &#8594;</button>
  </div>`;
}

// ── Facilities ───────────────────────────────────────────────────────
// TOP: facility type cards (clickable) via renderFacilityRiskStrip()
// BOTTOM: per-company/entity list using cluster + priority data
function renderFacilities() {
  buildChipBar('fac-chip-bar');
  renderFacilityRiskStrip();
  const q = (document.getElementById('fac-search')||{value:''}).value.toLowerCase();
  const data = filteredCits();
  const el = document.getElementById('facility-grid'); if (!el) return;

  const _SKIP = new Set(['unknown','n/a','na','various','multiple','other','—','-','']);
  const coMap = {};
  data.forEach(c => {
    // Prefer explicit company; fall back to cluster_label entity
    const co = (c.company || c.cluster_label || '').trim();
    if (!co || _SKIP.has(co.toLowerCase())) return;
    if (!coMap[co]) coMap[co] = { name:co, total:0, p1:0, p2:0, auths:new Set(), fms:{}, cats:{}, factypes:{}, clusters:new Set(), latestDate:'' };
    const e = coMap[co];
    e.total++;
    if (c.priority==='P1') e.p1++;
    if (c.priority==='P2') e.p2++;
    e.auths.add(c.authority);
    const fm = c.failure_mode; if (fm && fm!=='insufficient_detail') e.fms[fm] = (e.fms[fm]||0)+1;
    const cat = c.primary_gmp_category||c.category; if (cat) e.cats[cat] = (e.cats[cat]||0)+1;
    const ft = c.facility_type; if (ft) e.factypes[ft] = (e.factypes[ft]||0)+1;
    if (c.cluster_id) e.clusters.add(c.cluster_id);
    if (c.date && (!e.latestDate || c.date > e.latestDate)) e.latestDate = c.date;
  });

  Object.values(coMap).forEach(e => {
    const sorted = Object.entries(e.factypes).sort((a,b)=>b[1]-a[1]);
    e.factype = sorted.length ? sorted[0][0] : 'Unknown';
    delete e.factypes;
  });

  let coItems = Object.values(coMap)
    .filter(e => e.total >= 1 || e.p1 >= 1 || e.clusters.size >= 1)
    .sort((a,b) => (b.p1*3 + b.p2 + b.total) - (a.p1*3 + a.p2 + a.total));

  if (q) coItems = coItems.filter(e => e.name.toLowerCase().includes(q) || e.factype.toLowerCase().includes(q));

  const fc = document.getElementById('fac-count');
  if (fc) fc.textContent = coItems.length ? `${coItems.length} entit${coItems.length!==1?'ies':'y'} / record${coItems.length!==1?'s':''}` : 'No entity records';

  renderFacilityFocusPanel();

  if (!coItems.length) {
    el.innerHTML = q
      ? '<div class="empty"><div class="empty-icon">&#127981;</div><div class="empty-text">No entities match search</div></div>'
      : '<div class="facility-limited-msg">No entity-level records available for current filters. Click a facility type above to view the action summary, or clear filters to see all.</div>';
    return;
  }

  el.innerHTML = '<div class="facility-data-note">Entity / company list — click any row to view citations. Data inferred from enforcement records.</div>'
    + coItems.map(e => {
      const sortedFm = Object.entries(e.fms).sort((a,b)=>b[1]-a[1]);
      const topFmLabel = sortedFm.length ? _fmLabel(sortedFm[0][0]) : '';
      const authStr = [...e.auths].join(' / ');
      const clusterStr = e.clusters.size > 1 ? ` · ${e.clusters.size} clusters` : '';
      const nSafe = e.name.replace(/'/g, "\\'");
      // TODO(Phase 3): Facilities entity clicks navigate unconditionally — replace with
      // an entity-level focus panel that stays in context (Phase 3 rewrite).
      return `<div class="facility-card" onclick="applyPharmaFilter({entity:'${nSafe}'}); scrollToCitationsTable()" style="cursor:pointer" title="View citations for ${e.name}">
        <div class="facility-name">${e.name}</div>
        <div class="facility-type-label">${e.factype} &middot; ${authStr}</div>
        <div class="facility-stats">
          <span class="facility-stat">${e.total} citation${e.total!==1?'s':''}</span>
          ${e.p1>0?`<span class="facility-stat fsr">${e.p1} P1</span>`:''}
          ${e.p2>0?`<span class="facility-stat" style="color:#e67e22">${e.p2} P2</span>`:''}
          ${clusterStr?`<span class="facility-stat">${clusterStr}</span>`:''}
        </div>
        ${topFmLabel?`<div class="facility-cats">Top issue: ${topFmLabel}</div>`:''}
        ${e.latestDate?`<div style="font-size:9px;color:#2F4558;margin-top:2px">Latest: ${e.latestDate.slice(0,10)}</div>`:''}
      </div>`;
    }).join('');
}

// ── Alert card (compact, action-oriented) ────────────────────────────
function alertCard(c, patterns={}) {
  const prio = c.priority || '';
  const isP1 = prio === 'P1';
  const isP2 = prio === 'P2';
  const acClass = isP1 ? 'ac-high' : (isP2 ? 'ac-medium' : '');
  const st = (c.source_type || '').replace(/_/g, ' ');
  const isImport = (c.source_type || '') === 'import_alert';
  const summ = (c.decision_summary || c.summary || 'Enforcement action').slice(0, 160);
  const displayIssue = getDisplayIssue(c);
  const evidSt = getEvidenceStatus(c);
  const entity = c.company || c.entity || '';
  const entityCopy = entity
    ? `<button class="cit-copy-btn" onclick="event.stopPropagation();_copyText('${_escOnclick(entity)}',this)" title="Copy entity name">&#9138;</button>`
    : '';
  const intel = CAT_INTEL[c.category || ''] || CAT_INTEL[displayIssue] || null;
  const action = c.recommended_action || (intel ? intel.action : `Review exposure for ${displayIssue} and monitor for similar enforcement patterns across your site types.`);
  const pBadge = patterns[c.company]
    ? `<span class="pattern-badge">Pattern: recurring ${(c.company || '').slice(0, 28)}</span>`
    : '';
  const prioBadge = prio ? `<span class="badge" style="background:${isP1?'rgba(239,68,68,.1)':isP2?'rgba(251,146,60,.08)':'rgba(20,50,80,.15)'};color:${isP1?'#ef4444':isP2?'#fb923c':'#7A92A8'};border:1px solid ${isP1?'rgba(239,68,68,.25)':isP2?'rgba(251,146,60,.2)':'rgba(20,50,80,.3)'}">${prio}</span>` : '';
  const issueBadge = displayIssue !== 'Unclassified'
    ? `<span class="badge" style="background:rgba(20,50,80,.2);color:#3A5570;border:1px solid rgba(20,50,80,.3)">${displayIssue}</span>`
    : '';
  const viewBtns = _citViewBtn(c);
  return `<div class="alert-card${acClass?' '+acClass:''}">
    <div class="alert-card-title">${summ}</div>
    <div class="alert-card-meta">
      ${prioBadge}
      <span class="badge badge-authority">${c.authority || '—'}</span>
      <span class="badge badge-type"${isImport ? ` title="${_IMPORT_ALERT_TIP}"` : ''} style="${isImport ? 'cursor:help' : ''}">${st}</span>
      ${c.facility_type ? `<span class="badge" style="background:rgba(139,92,246,.06);color:rgba(139,92,246,.5);border:1px solid rgba(139,92,246,.1)">${c.facility_type}</span>` : ''}
      ${issueBadge}
      <span class="badge" style="background:rgba(20,50,80,.1);color:#3A5570;border:1px solid rgba(20,50,80,.2)">${evidSt}</span>
      ${pBadge}
    </div>
    <div class="alert-card-action"><b>Action:</b> ${action}</div>
    <div class="alert-card-footer">
      <div class="alert-card-date">${c.date ? c.date.slice(0, 10) : '—'}${entity ? ' · ' + entity : ''}${entityCopy}</div>
      <div style="display:flex;gap:4px;align-items:center">${viewBtns}</div>
    </div>
  </div>`;
}

// ── Pharma alerts ────────────────────────────────────────────────────
function renderPharmaAlerts() {
  const el = document.getElementById('pharma-alerts-feed'); if (!el) return;
  // Show P1 regardless of severity; show confirmed/provisional P2; exclude unconfirmed non-P1
  const all = getPharmaCitations();
  const _PO = { P1: 0, P2: 1, P3: 2, P4: 3 };
  const _SO = { confirmed: 0, provisional: 1, unconfirmed: 2 };
  const alerts = all.filter(c => {
    const p = c.priority || '';
    const st = c.classification_status || '';
    if (p === 'P1') return true;
    if (p === 'P2' && st !== 'unconfirmed') return true;
    if (c.severity === 'high' && st === 'confirmed') return true;
    return false;
  }).sort((a, b) => {
    const pa = _PO[a.priority] ?? 4, pb = _PO[b.priority] ?? 4;
    if (pa !== pb) return pa - pb;
    const sa = _SO[a.classification_status] ?? 3, sb = _SO[b.classification_status] ?? 3;
    if (sa !== sb) return sa - sb;
    return (b.date || '') > (a.date || '') ? 1 : -1;
  }).slice(0, 40);
  const patterns = detectPattern(alerts);
  el.innerHTML = alerts.length
    ? alerts.map(c => alertCard(c, patterns)).join('')
    : '<div class="empty"><div class="empty-icon">&#9989;</div><div class="empty-text">No P1/P2 alerts</div></div>';
}

// === PHARMA INTELLIGENCE + PLUMBING ===

// ── Intelligence page ─────────────────────────────────────────────────
// ── Intelligence tab — badge helpers ─────────────────────────────────
function mbTrend(t){ return `<span class="mb mb-trend-${t}">${t}</span>`; }
function mbConf(c){ return `<span class="mb mb-conf-${c}">${c === 'high' ? '✓ high' : c + ' conf'}</span>`; }
function mbImpact(i){ return `<span class="mb mb-impact-${i}">${i} impact</span>`; }

// ── Intelligence card expand/collapse ─────────────────────────────────
function toggleIntelCard(id) {
  if(intelExpanded.has(id)) intelExpanded.delete(id); else intelExpanded.add(id);
  const c=document.getElementById('icard-'+id); if(c) c.classList.toggle('expanded', intelExpanded.has(id));
}

// ── Intelligence filter actions ───────────────────────────────────────
function applyIntelFilter(dim, val) {
  intelFilters[dim] = intelFilters[dim]===val ? 'all' : val;
  renderIntelligencePage();
}
function resetIntelFilters() {
  intelFilters = { priority:'all', trend:'all', confidence:'all', impact:'all' };
  renderIntelligencePage();
}

// ── Intelligence page — render functions ──────────────────────────────
function renderIntelligencePage() {
  // Top band — always shows immediate-priority items regardless of filter
  const topItems = INTEL_ITEMS.filter(i=>i.priority==='immediate');
  const tb = document.getElementById('intel-top-band');
  if(tb) tb.innerHTML = topItems.map((item,i)=>`
    <div class="top-sig">
      <div class="top-sig-rank">Priority ${i+1}</div>
      <div class="top-sig-title">${item.title}</div>
      <div class="top-sig-meta">${mbTrend(item.trend)} ${mbConf(item.confidence)} ${mbImpact(item.impact)}</div>
      <div class="top-sig-summary">${item.summary}</div>
      <div class="top-sig-action">${item.recommendedAction}</div>
    </div>`).join('');

  // Filters bar
  const fb = document.getElementById('intel-filters-bar');
  if(fb) {
    const mk=(dim,val,label)=>`<span class="ifp${intelFilters[dim]===val?' active':''}" onclick="applyIntelFilter('${dim}','${val}')">${label}</span>`;
    const hasF = Object.values(intelFilters).some(v=>v!=='all');
    fb.innerHTML =
      `<span class="intel-filter-label">Priority</span>
       ${mk('priority','immediate','Immediate')}${mk('priority','near_term','Near-Term')}${mk('priority','monitor','Monitor')}
       <div class="intel-filter-sep"></div>
       <span class="intel-filter-label">Trend</span>
       ${mk('trend','rising','Rising')}${mk('trend','emerging','Emerging')}${mk('trend','stable','Stable')}
       <div class="intel-filter-sep"></div>
       <span class="intel-filter-label">Impact</span>
       ${mk('impact','high','High')}${mk('impact','medium','Medium')}
       ${hasF?'<button class="ifp-reset" onclick="resetIntelFilters()">✕ Clear</button>':''}`;
  }

  // Priority-grouped sections
  const filtered = INTEL_ITEMS.filter(item=>{
    if(intelFilters.priority!=='all'&&item.priority!==intelFilters.priority) return false;
    if(intelFilters.trend!=='all'&&item.trend!==intelFilters.trend) return false;
    if(intelFilters.confidence!=='all'&&item.confidence!==intelFilters.confidence) return false;
    if(intelFilters.impact!=='all'&&item.impact!==intelFilters.impact) return false;
    return true;
  });

  const body = document.getElementById('intel-priority-body');
  if(!body) return;

  if(!filtered.length){
    body.innerHTML='<div class="empty"><div class="empty-icon">&#128269;</div><div class="empty-text">No items match the current filters</div><button class="btn-secondary" style="margin-top:12px" onclick="resetIntelFilters()">Reset filters</button></div>';
    return;
  }

  const SECTION_META = {
    immediate:{ label:'Immediate Action', note:'Act on these before your next inspection' },
    near_term: { label:'Near-Term Risk',   note:'Address within the next quarter' },
    monitor:   { label:'Monitor',          note:'Track for escalating regulator focus' },
  };
  const groups = ['immediate','near_term','monitor']
    .map(key=>({ key, ...SECTION_META[key], items:filtered.filter(i=>i.priority===key) }))
    .filter(g=>g.items.length);

  body.innerHTML = groups.map(g=>`
    <div class="intel-priority-section">
      <div class="intel-priority-hd">
        <span class="intel-priority-hd-label ${g.key}">${g.label}</span>
        <span class="intel-priority-hd-ct">${g.note} &mdash; ${g.items.length} item${g.items.length!==1?'s':''}</span>
      </div>
      ${g.items.map(item=>{
        const exp = intelExpanded.has(item.id);
        const auth = item.authorityTags.map(a=>`<span class="badge badge-authority" style="font-size:8px;padding:1px 5px">${a}</span>`).join(' ');
        return `<div class="icard${exp?' expanded':''}" id="icard-${item.id}">
          <div class="icard-top" onclick="toggleIntelCard('${item.id}')">
            <div class="icard-left">
              <div class="icard-title">${item.title}</div>
              <div class="icard-why-brief">${item.summary.split('. ')[0]}.</div>
              <div class="icard-action-preview">${item.recommendedAction}</div>
            </div>
            <div class="icard-right">
              <div class="icard-badges">${mbTrend(item.trend)} ${mbConf(item.confidence)} ${mbImpact(item.impact)}</div>
              <div style="display:flex;gap:4px;justify-content:flex-end;flex-wrap:wrap">${auth}</div>
              <div class="icard-expand">&#9662;</div>
            </div>
          </div>
          <div class="icard-detail">
            <div class="icard-detail-row">
              <div class="icard-detail-lbl">Why it matters</div>
              <div class="icard-detail-txt">${item.whyItMatters}</div>
            </div>
            <div class="icard-detail-row">
              <div class="icard-detail-lbl">Recommended action</div>
              <div class="icard-detail-action">${item.recommendedAction}</div>
            </div>
            <div class="icard-detail-row cmode-only">
              <div class="icard-detail-lbl">Likely client conversation</div>
              <div class="icard-detail-client">${item.likelyClientConversation}</div>
            </div>
            <div class="icard-detail-footer">
              <div class="icard-detail-opp cmode-only">Commercial opportunity: ${item.commercialOpportunity}</div>
              <div class="icard-detail-evid">Evidence: ${item.evidenceSummary}</div>
            </div>
          </div>
        </div>`;
      }).join('')}
    </div>`).join('');
}

function resetPharmaFilters() {
  pF={auth:'all',factype:'all',srctype:'all',sev:'all',priority:'all',failureMode:'',displayIssue:'',company:'',query:'',dateFrom:'',dateTo:'',dicapa:false};
  pCatFilter=null;
  selectedEnforcementFocus = null;
  selectedFacilityFocus    = null;
  _showLimitedDetail       = false;
  _overviewFocus           = null;
  ['cit-search','p-company'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  _setPActiveKpi(null);
  syncPFPills(); renderPAll();
}
function _setPActiveKpi(id) {
  _dirty = true;
  if(pActiveKpi) { const prev=document.getElementById(pActiveKpi); if(prev) prev.classList.remove('pk-active'); }
  pActiveKpi=id;
  if(id) { const el=document.getElementById(id); if(el) el.classList.add('pk-active'); }
}
function pKpiClick(kpiId, dim, val) {
  pCatFilter=null;
  const toggling = pActiveKpi===kpiId;
  pF={auth:'all',factype:'all',srctype:'all',sev:'all',priority:'all',failureMode:'',displayIssue:'',company:'',query:'',dateFrom:'',dateTo:'',dicapa:false};
  if(!toggling) {
    if(dim==='dicapa') pF.dicapa=true;
    else if(dim) pF[dim]=val;
    _setPActiveKpi(kpiId);
  } else {
    _setPActiveKpi(null);
  }
  syncPFPills();
  renderPharmaOverview();
  const feed=document.getElementById('pharma-ov-feed');
  if(feed) feed.scrollIntoView({behavior:'smooth',block:'start'});
}
function syncPFPills() {
  _dirty = true;
  document.querySelectorAll('.pf-pill[data-pf]').forEach(el=>{
    const dim=el.dataset.pf, val=el.dataset.val;
    el.classList.toggle('active', pF[dim]===val||(val==='all'&&pF[dim]==='all'));
  });
}

function pSetFilter(dim, val) {
  pF[dim] = pF[dim]===val ? 'all' : val;
  syncPFPills(); renderPAll();
}
// All filter functions toggle: clicking the same value again clears the filter
function pFilterBySev(v)     { pF.sev     = pF.sev===v     ? 'all' : v; syncPFPills(); renderPAll(); }
function pFilterBySrc(v)     { pF.srctype = pF.srctype===v  ? 'all' : v; syncPFPills(); renderPAll(); }
function pFilterByAuth(v)    { pF.auth    = pF.auth===v     ? 'all' : v; syncPFPills(); renderPAll(); }
function pFilterByFacType(v) { pF.factype = pF.factype===v  ? 'all' : v; syncPFPills(); renderPAll(); }
function pFilterByCat(cat) {
  pCatFilter = pCatFilter===cat ? null : cat;
  _dirty = true;
  syncPFPills();
  scrollToCitationsTable();
}

// ── Task 7: Shared filter + navigation helpers ────────────────────────
function applyPharmaFilter(filterObj) {
  if (filterObj.authority      !== undefined) pF.auth        = filterObj.authority;
  if (filterObj.source_type    !== undefined) pF.srctype     = filterObj.source_type;
  if (filterObj.severity       !== undefined) pF.sev         = filterObj.severity;
  if (filterObj.facility_type  !== undefined) pF.factype     = filterObj.facility_type;
  if (filterObj.priority       !== undefined) pF.priority    = filterObj.priority;
  if (filterObj.failure_mode   !== undefined) pF.failureMode = filterObj.failure_mode;
  if (filterObj.primary_gmp_category !== undefined) pCatFilter = filterObj.primary_gmp_category;
  if (filterObj.display_issue  !== undefined) {
    pF.displayIssue = filterObj.display_issue;
    // Clear legacy issue filters so display_issue is the sole issue predicate.
    pF.failureMode = '';
    pCatFilter = null;
  }
  if (filterObj.entity !== undefined) {
    pF.company = filterObj.entity;
    const el = document.getElementById('p-company'); if(el) el.value = filterObj.entity;
  }
  if (filterObj.query !== undefined) {
    pF.query = filterObj.query;
    const el = document.getElementById('cit-search'); if(el) el.value = filterObj.query;
  }
  syncPFPills();
  _dirty = true;
}
function scrollToCitationsTable() {
  showPTab('pharma-citations');
  requestAnimationFrame(() => {
    const el = document.getElementById('pharma-page-citations');
    if (el) el.scrollIntoView({behavior:'smooth', block:'start'});
  });
}

// ── Shared chip bar renderer ─────────────────────────────────────────
function buildChipBar(elId) {
  const el=document.getElementById(elId); if(!el) return;
  const chips=[];
  if(pF.srctype!=='all')    chips.push({label:`Type: ${pF.srctype.replace(/_/g,' ')}`,        fn:`pF.srctype='all';_setPActiveKpi(null);syncPFPills();renderPAll()`});
  if(pF.auth!=='all')       chips.push({label:`Authority: ${pF.auth}`,                        fn:`pF.auth='all';syncPFPills();renderPAll()`});
  if(pF.factype!=='all')    chips.push({label:`Facility: ${pF.factype}`,                      fn:`pF.factype='all';syncPFPills();renderPAll()`});
  if(pF.priority!=='all')   chips.push({label:`Priority: ${pF.priority}`,                     fn:`pF.priority='all';_dirty=true;syncPFPills();renderPAll()`});
  if(pF.displayIssue)       chips.push({label:`Issue: ${pF.displayIssue}`,                     fn:`pF.displayIssue='';_dirty=true;syncPFPills();renderPAll()`});
  if(pF.failureMode)        chips.push({label:`Issue: ${_fmLabel(pF.failureMode)}`,            fn:`pF.failureMode='';_dirty=true;syncPFPills();renderPAll()`});
  if(pF.company)            chips.push({label:`Entity: ${pF.company.slice(0,28)}`,             fn:`pF.company='';const e=document.getElementById('p-company');if(e)e.value='';_dirty=true;syncPFPills();renderPAll()`});
  if(pF.dicapa)             chips.push({label:'DI / CAPA / CSV',                              fn:`pF.dicapa=false;_setPActiveKpi(null);syncPFPills();renderPAll()`});
  if(pCatFilter)            chips.push({label:pCatFilter==='GMP violations'?`Broad group: GMP violations`:`Category: ${pCatFilter}`, fn:`pCatFilter=null;_dirty=true;renderPAll()`});
  if(!chips.length){el.innerHTML='';return;}
  el.innerHTML=chips.map(ch=>
    `<span class="pharma-chip">${ch.label}<button class="pharma-chip-x" onclick="${ch.fn}">&#10005;</button></span>`
  ).join('')+`<button class="pharma-chip-clear-all" onclick="resetPharmaFilters()">&#10005; Clear all</button>`;
}
function sortCit(col) {
  _dirty = true;
  if (citSortState.col===col) citSortState.dir=-citSortState.dir;
  else { citSortState.col=col; citSortState.dir=-1; }
  renderCitations();
}

function renderPAll() {
  console.time('renderPAll');
  citPg=1;
  const _at=document.querySelector('#pharma-nav .nav-tab.active');
  const _tab=_at?_at.dataset.ptab:'pharma-overview';
  if(_tab==='pharma-overview') renderPharmaOverview();
  else if(_tab==='pharma-citations') renderCitations();
  else if(_tab==='pharma-enforcement') renderEnfPage();
  else if(_tab==='pharma-facilities') renderFacilities();
  else if(_tab==='pharma-alerts') renderPharmaAlerts();
  else if(_tab==='pharma-intelligence') renderIntelligencePage();
  else renderPharmaOverview();
  console.timeEnd('renderPAll');
}
