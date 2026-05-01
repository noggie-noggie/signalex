// === SIGNALEX PHARMA HELPERS ===

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
  const actions=[];
  const catMap=data.reduce((m,c)=>{const k=c.category||'Other';m[k]=(m[k]||0)+1;return m},{});
  const topCat=Object.entries(catMap).sort((a,b)=>b[1]-a[1])[0];
  if(topCat){
    const intel=CAT_INTEL[topCat[0]];
    const safe=topCat[0].replace(/'/g,"\\'");
    actions.push({
      priority:'now',
      title:`Review ${topCat[0]} controls — ${topCat[1]} citation${topCat[1]!==1?'s':''}`,
      why: intel?intel.action:'Highest citation volume this period',
      onclick:`pFilterByCat('${safe}')`
    });
  }
  const highItems=data.filter(c=>c.severity==='high');
  if(highItems.length){
    actions.push({
      priority:'urgent',
      title:`${highItems.length} high-severity action${highItems.length!==1?'s':''} require immediate attention`,
      why:'High severity citations indicate enforcement escalation risk — act before inspection',
      onclick:`pF.sev='high';syncPFPills();renderPharmaOverview()`
    });
  }
  const tgaCt=data.filter(c=>c.authority==='TGA').length;
  const suppCt=data.filter(c=>c.facility_type==='Supplement / Nutraceutical').length;
  if(tgaCt>=suppCt&&tgaCt>0){
    actions.push({
      priority:'this-week',
      title:`${tgaCt} TGA action${tgaCt!==1?'s':''} on file — AU market priority`,
      why:'Self-audit against AU GMP Code Part 3 for all AU-registered products',
      onclick:`pF.auth='TGA';syncPFPills();renderPharmaOverview()`
    });
  } else if(suppCt>0){
    actions.push({
      priority:'this-week',
      title:`${suppCt} Supplement / Nutraceutical action${suppCt!==1?'s':''} on file`,
      why:'Labelling & claims leads this facility type — audit substantiation packages',
      onclick:`pF.factype='Supplement / Nutraceutical';syncPFPills();renderPharmaOverview()`
    });
  }
  return actions.slice(0,3);
}


function rankPharmaRisks(data) {
  const catMap={};
  data.forEach(c=>{
    const k=c.category||'Other';
    if(!catMap[k]) catMap[k]={count:0,high:0};
    catMap[k].count++;
    if(c.severity==='high') catMap[k].high++;
  });
  return Object.entries(catMap)
    .map(([cat,v])=>({cat,count:v.count,high:v.high,score:v.count+v.high*3}))
    .sort((a,b)=>b.score-a.score)
    .slice(0,5);
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
  if (cat.includes('gmp') || cat.includes('documentation') || cat.includes('batch') || cat.includes('deviation'))
    return 'Review SOP compliance, documentation controls, and cleaning validation.';
  if (cat.includes('steril') || cat.includes('contamination') || cat.includes('aseptic') || cat.includes('container closure'))
    return 'Audit CCS, EM program, and aseptic behaviour controls.';
  if (cat.includes('label') || cat.includes('claim') || cat.includes('ingredient safety'))
    return 'Verify claim substantiation and regulatory alignment across jurisdictions.';
  if (cat.includes('supply') || cat.includes('import') || cat.includes('procure') || cat.includes('fsvp'))
    return 'Review supplier qualification and incoming material controls.';
  if (cat.includes('equipment') || cat.includes('facilit') || cat.includes('maintenance'))
    return 'Review preventive maintenance programme and equipment qualification lifecycle.';
  if (cat.includes('csv') || cat.includes('data integrity') || cat.includes('computerised'))
    return 'Assess CSV documentation currency, audit trail integrity, and access governance.';
  return 'Review site-specific enforcement profile and prioritise corrective actions.';
}


// === PHARMA OVERVIEW RENDER FUNCTIONS ===

// ── Pharma Overview ──────────────────────────────────────────────────
function renderPharmaOverview() {
  const data=filteredCits();
  const isFiltered = pActiveKpi || pCatFilter || pF.auth!=='all' || pF.srctype!=='all' || pF.sev!=='all' || pF.dicapa;

  // KPI row — single pass over data
  let high=0,wl=0,insp=0,diCapaFilt=0,tga=0;
  for(const c of data){
    if(c.severity==='high')high++;
    if(c.source_type==='warning_letter')wl++;
    if(c.source_type==='inspection_finding')insp++;
    if(isDiCapa(c))diCapaFilt++;
    if(c.authority==='TGA')tga++;
  }
  const _setKpi=(id,val)=>{const e=document.getElementById(id);if(e){const v=e.querySelector('.kpi-val');if(v){v.textContent=val;v.classList.add('kpi-updated');setTimeout(()=>v.classList.remove('kpi-updated'),600);}}};
  _setKpi('pk-high', high);
  _setKpi('pk-wl', wl);
  _setKpi('pk-483', insp);
  _setKpi('pk-total', data.length);
  _setKpi('pk-diCapa', diCapaFilt);
  _setKpi('pk-tga', tga);

  // Tab badge — always show full dataset size
  const tabBadge=document.getElementById('pharma-tab-count');
  if(tabBadge) tabBadge.textContent=`${CITATIONS.length} citations`;

  // AI summary + So What + What Changed
  renderWhatChanged();
  renderPharmaInsights();
  renderSoWhatLine(data);

  // Decision-first sections
  renderStartHereActions(data);
  renderTopRiskBlocks(rankPharmaRisks(data));
  renderAllCategoriesCollapsed(data);

  // Authority activity
  renderAuthBars('pharma-auth-bars');

  // Recent feed
  const oc=document.getElementById('pharma-ov-count'); if(oc) oc.textContent=`${data.length} total`;
  buildChipBar('pharma-ov-chip-bar');
  const el=document.getElementById('pharma-ov-feed'); if(!el) return;
  const limit = isFiltered ? 15 : 8;
  const feed=data.slice(0,limit);
  const viewAllBtn = data.length>limit
    ? `<button class="ov-feed-view-all" onclick="showPTab('pharma-citations')">View all ${data.length} citations &rarr;</button>`
    : '';
  el.innerHTML=feed.length
    ? feed.map(c=>citCard(c)).join('')+viewAllBtn
    :'<div class="empty"><div class="empty-icon">&#128270;</div><div class="empty-text">No citations match filters</div></div>';
}

function renderCatGrid(gridId, countId) {
  const data=filteredCits();
  const catCounts={};
  data.forEach(c=>{const k=c.category||'Other';catCounts[k]=(catCounts[k]||0)+1;});
  const cats=Object.entries(catCounts).sort((a,b)=>b[1]-a[1]);
  const el=document.getElementById(gridId); if(!el) return;
  el.innerHTML=cats.map(([cat,ct])=>{
    const isAct=pCatFilter===cat;
    const high=data.filter(c=>(c.category||'Other')===cat&&c.severity==='high').length;
    const safe=cat.replace(/'/g,"\\'");
    const intel = CAT_INTEL[cat];
    const intelHtml = intel ? `<div class="cat-cell-intel">
      <div class="action-tag ${intel.urgency}">${intel.urgency}</div>
      <div class="cat-implication">${intel.implication}</div>
      <div class="cat-action">${intel.action}</div>
    </div>` : '';
    const whyLine=intel?`<div class="cat-why-line">${intel.implication.split('.')[0]}.</div>`:'';
    return `<div class="cat-cell${isAct?' c-active':''}" onclick="pFilterByCat('${safe}')">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:4px">
        <div class="cat-cell-name">${cat}</div>
        <button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${safe}','category')" title="Open ${cat} detail" style="font-size:12px;margin-top:1px;flex-shrink:0">&#9432;</button>
      </div>
      <div class="cat-cell-ct">${ct}</div>
      <div class="cat-cell-sub">${high?high+' high sev':'all medium'}</div>
      ${whyLine}
      ${intelHtml}
    </div>`;
  }).join('');
  const cc=document.getElementById(countId); if(cc) cc.textContent=`${data.length} citations`;
}
// ── Pharma AI insights ────────────────────────────────────────────────
function renderPharmaInsights() {
  const data=filteredCits();
  const isFiltered = pActiveKpi || pCatFilter || pF.auth!=='all' || pF.srctype!=='all' || pF.sev!=='all' || pF.factype!=='all' || pF.dicapa;
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
    el.innerHTML='<div class="empty"><div class="empty-text">No data — adjust filters</div></div>';
    return;
  }
  el.innerHTML=risks.map((r,i)=>{
    const intel=CAT_INTEL[r.cat];
    const meaning=intel?intel.implication.split('.')[0]+'.':'';
    const action=intel?intel.action:'Review enforcement pattern';
    const impact=getRiskImpact(r.count,r.high);
    const vol=formatVolumeBucket(r.count);
    const safe=r.cat.replace(/'/g,"\\'");
    return `<div class="top-risk-block" onclick="pFilterByCat('${safe}')">
      <div class="trb-rank">#${i+1}</div>
      <div class="trb-body">
        <div class="trb-hd">
          <span class="trb-cat">${r.cat}</span>
          <span class="risk-impact-badge ${impact.cls}">${impact.label}</span>
          <span class="vol-bucket">${vol} volume</span>
        </div>
        <div class="trb-meaning">${meaning}</div>
        <div class="trb-action">&#8594; ${action}</div>
        <div class="trb-stats">${r.count} citation${r.count!==1?'s':''} &middot; ${r.high} high severity</div>
      </div>
    </div>`;
  }).join('');
  const allCats=new Set(filteredCits().map(c=>c.category||'Other')).size;
  const countEl=document.getElementById('top-risks-count');
  if(countEl) countEl.textContent=`(${risks.length} of ${allCats} categories)`;
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

// ── What Changed This Period ──────────────────────────────────────────
function renderWhatChanged() {
  const el=document.getElementById('what-changed-cols'); if(!el)return;
  const {recentFrom,priorFrom,priorTo}=_computeDateWindows();
  const recent=CITATIONS.filter(c=>c.date&&c.date>=recentFrom);
  const prior=CITATIONS.filter(c=>c.date&&c.date>=priorFrom&&c.date<priorTo);
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
  const upChanges=changes.filter(x=>x.d>0);
  const dnChanges=changes.filter(x=>x.d<0);
  const mkItem=(arrow,cls,label,delta)=>
    `<div class="wc-item"><span class="wc-arrow ${cls}">${arrow}</span><span class="wc-label">${label}</span><span class="wc-delta">${delta>0?'+'+delta:delta}</span></div>`;
  const col1=`<div class="wc-col-hd">Rising categories</div>`
    +upChanges.map(x=>mkItem('▲','up',x.k,x.d)).join('')
    +(newCats.length?newCats.map(k=>`<div class="wc-item"><span class="wc-arrow new">★</span><span class="wc-label">${k}</span><span class="wc-delta">new</span></div>`).join(''):'');
  const col2=`<div class="wc-col-hd">Decreasing categories</div>`
    +(dnChanges.length?dnChanges.map(x=>mkItem('▼','down',x.k,x.d)).join('')
      :'<div class="wc-item" style="color:#2F4558;font-size:10px">No significant decreases</div>');
  const col3=`<div class="wc-col-hd">Authority activity</div>`
    +authChanges.map(x=>mkItem(x.d>0?'▲':'▼',x.d>0?'up':'down',x.k,x.d)).join('');
  el.innerHTML=col1?`<div>${col1}</div><div>${col2}</div><div>${col3}</div>`
    :'<div class="wc-item" style="color:#2F4558">Insufficient data for comparison</div>';
}

// === PHARMA CITATIONS + ENFORCEMENT RENDER ===

function renderAuthBars(elId) {
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
    return `<div class="enf-row${isActive?' er-active':''}" onclick="pFilterByAuth('${auth}')" title="${tip}">
      <span class="enf-name">${auth}</span>
      <div class="enf-bar-wrap"><div class="enf-bar-fill" style="width:${Math.round(ct/mx*100)}%"></div></div>
      <span class="enf-ct">${ct}</span>
    </div>`;
  }).join('');
}

// ── Citation card ────────────────────────────────────────────────────
const _IMPORT_ALERT_TIP = 'Import Alert: regulatory action preventing products from entering a country due to safety or compliance issues (e.g. FDA Import Alert — Detention Without Physical Examination)';
function citCard(c) {
  const sc=c.severity==='high'?'high-sev':'medium-sev';
  const st=(c.source_type||'').replace(/_/g,' ');
  const isImport=(c.source_type||'')==='import_alert';
  const summ=(c.summary||c.category||'Enforcement action').slice(0,140);
  return `<div class="signal-card ${sc}" style="margin-bottom:8px">
    <div class="card-top"><div class="card-title">${summ}</div></div>
    <div class="card-badges">
      ${sevBadge(c.severity||'medium')}
      <span class="badge badge-authority">${c.authority||'—'}</span>
      <span class="badge badge-type"${isImport?` title="${_IMPORT_ALERT_TIP}"`:''} style="${isImport?'cursor:help':''}">${st}</span>
      ${c.facility_type?`<span class="badge" style="background:rgba(139,92,246,.06);color:rgba(139,92,246,.55);border:1px solid rgba(139,92,246,.1)">${c.facility_type}</span>`:''}
    </div>
    ${c.category?`<div class="card-summary">${c.category}</div>`:''}
    ${c.company?`<div class="card-summary" style="color:#3D5268;font-size:10px">Company: ${c.company}</div>`:''}
    <div class="card-footer">
      <div class="card-meta">${c.date?c.date.slice(0,10):'—'} &middot; ${c.country||c.authority||''}</div>
      <div class="card-actions">
        ${c.url?`<a class="card-action card-action-primary" href="${c.url}" target="_blank" onclick="event.stopPropagation()">View &#8599;</a>`:''}
      </div>
    </div>
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
  const high=data.filter(c=>c.severity==='high').length;
  const action=topCat&&CAT_INTEL[topCat[0]]?CAT_INTEL[topCat[0]].action:'Review enforcement actions relevant to your site profile.';
  el.innerHTML=`<div class="insight-strip">
    <div class="insight-strip-hd">What this table shows</div>
    <div class="insight-strip-bullets">
      <div class="isb"><div class="isb-dot${high>0?' isb-dot-red':''}"></div><div class="isb-text"><b>${data.length} citation${data.length!==1?'s':''}</b> match current filters${high>0?` — <b>${high} high severity</b>`:''}.${data.length===CITATIONS.length?' Full dataset — use sidebar or KPI cards to filter.':''}</div></div>
      ${topCat?`<div class="isb"><div class="isb-dot isb-dot-amber"></div><div class="isb-text"><b>${topCat[0]}</b> is the dominant category (${topCat[1]} citations). ${action}</div></div>`:''}
      ${topAuth?`<div class="isb"><div class="isb-dot"></div><div class="isb-text"><b>${topAuth[0]}</b> leads by authority (${topAuth[1]} actions). Primary source type: <b>${topSrc?topSrc[0]:'—'}</b>.</div></div>`:''}
    </div>
  </div>`;
}

// ── Citations table ──────────────────────────────────────────────────
function renderCitations() {
  buildChipBar('cit-chip-bar');
  renderCitationsInsightStrip();
  const data=filteredCits();
  const pages=Math.ceil(data.length/CIT_PP);
  citPg=Math.min(citPg,pages||1);
  const slice=data.slice((citPg-1)*CIT_PP,citPg*CIT_PP);
  const tb=document.getElementById('cit-tbody'); if(!tb) return;
  const cc=document.getElementById('cit-count'); if(cc) cc.textContent=`${data.length} citations`;
  if(!slice.length) {
    tb.innerHTML=`<tr><td colspan="8" style="text-align:center;padding:24px 0;color:#2A3E52;font-size:12px">
      <div style="font-size:22px;margin-bottom:6px">&#128270;</div>No citations match the current filters.
      <button class="btn-secondary" style="display:block;margin:10px auto 0" onclick="resetPharmaFilters()">Clear filters</button>
    </td></tr>`;
    document.getElementById('cit-pagination').innerHTML='';
    return;
  }
  tb.innerHTML=slice.map(c=>{
    const st=(c.source_type||'').replace(/_/g,' ');
    return `<tr onclick="pFilterByCat('${(c.category||'Other').replace(/'/g,"\\'")}')" title="Filter by ${c.category||'this category'}">
      <td><span class="badge badge-authority">${c.authority||'—'}</span></td>
      <td><span class="badge badge-type" style="white-space:nowrap">${st}</span></td>
      <td style="color:#3D5268;font-size:10px;min-width:90px">${c.facility_type||'—'}</td>
      <td style="min-width:110px">${c.category||'—'}</td>
      <td>${sevBadge(c.severity||'medium')}</td>
      <td style="white-space:nowrap;font-size:10px;color:#3D5268">${c.date?c.date.slice(0,10):'—'}</td>
      <td style="max-width:280px;font-size:11px;color:#7A92A8">${(c.summary||'—').slice(0,100)}</td>
      <td>${c.url?`<a class="cit-link" href="${c.url}" target="_blank" onclick="event.stopPropagation()">&#8599;</a>`:'—'}</td>
    </tr>`;
  }).join('');
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
  renderCatGrid('enf-cat-grid','enf-cat-count');
  const data=filteredCits();
  const el=document.getElementById('enf-srctype-rows'); if(!el) return;
  const sc={};
  data.forEach(c=>{const k=(c.source_type||'other').replace(/_/g,' ');sc[k]=(sc[k]||0)+1;});
  const mx=Math.max(...Object.values(sc),1);
  el.innerHTML=Object.entries(sc).sort((a,b)=>b[1]-a[1]).map(([st,ct])=>
    `<div class="enf-row">
      <span class="enf-name">${st}</span>
      <div class="enf-bar-wrap"><div class="enf-bar-fill" style="width:${Math.round(ct/mx*100)}%"></div></div>
      <span class="enf-ct">${ct}</span>
    </div>`).join('');
  renderGroupedEnforcement('enf-grouped-feed');
}

function renderGroupedEnforcement(elId) {
  const el=document.getElementById(elId); if(!el) return;
  const data=filteredCits();
  const {recentFrom,priorFrom,priorTo}=_computeDateWindows();
  const recent=CITATIONS.filter(c=>c.date&&c.date>=recentFrom);
  const prior=CITATIONS.filter(c=>c.date&&c.date>=priorFrom&&c.date<priorTo);
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
    return `<div class="fac-risk-card">
      <div class="fac-risk-name">${name} <span style="font-size:10px;color:#2A3E52;font-weight:400">(${d.total} citation${d.total!==1?'s':''})</span></div>
      ${topCat?`<div class="fac-risk-row"><div class="fac-risk-lbl">Top finding</div><div class="fac-risk-val">${topCat[0]}</div></div>`:''}
      ${profile?`<div class="fac-risk-row"><div class="fac-risk-lbl">Exposure</div><div class="fac-risk-val">${profile.exposure}</div></div>`:''}
      ${profile?`<div class="fac-risk-row"><div class="fac-risk-lbl">Insp. focus</div><div class="fac-risk-val">${profile.focus}</div></div>`:''}
      ${profile?`<div class="fac-risk-action">Action: ${profile.action}</div>`:''}
    </div>`;
  }).join('');
  el.innerHTML=`<div class="insight-strip" style="padding-bottom:4px">
    <div class="insight-strip-hd">Facility risk exposure</div>
  </div>
  <div class="fac-risk-grid">${cards}</div>`;
}

// ── Facilities ───────────────────────────────────────────────────────
// TOP: type-level aggregation via renderFacilityRiskStrip() — exposure, top finding, action
// BOTTOM: per-company when data available; falls back to type-level if < 5 companies
function renderFacilities() {
  buildChipBar('fac-chip-bar');
  renderFacilityRiskStrip();
  const q=(document.getElementById('fac-search')||{value:''}).value.toLowerCase();
  const data=filteredCits();
  const el=document.getElementById('facility-grid'); if(!el)return;

  // ── Try per-company view first ──
  const coMap={};
  data.forEach(c=>{
    const co=(c.company||'').trim();
    if(!co) return;
    if(!coMap[co])coMap[co]={name:co,factype:c.facility_type||'Unknown',total:0,high:0,auths:new Set(),cats:{}};
    coMap[co].total++;
    if(c.severity==='high')coMap[co].high++;
    coMap[co].auths.add(c.authority);
    const k=c.category||'Other';
    coMap[co].cats[k]=(coMap[co].cats[k]||0)+1;
  });
  let coItems=Object.values(coMap).sort((a,b)=>b.total-a.total);
  if(q) coItems=coItems.filter(i=>i.name.toLowerCase().includes(q)||i.factype.toLowerCase().includes(q));

  // ── Fallback: group by facility type when company data is sparse ──
  if(coItems.length < 5 && !q) {
    console.warn('[Signalex] Low company count ('+coItems.length+') — using facility-type fallback');
    const typeMap={};
    data.forEach(c=>{
      const ft=c.facility_type||'Unknown';
      if(!typeMap[ft])typeMap[ft]={name:ft,total:0,high:0,auths:new Set(),cats:{}};
      typeMap[ft].total++;
      if(c.severity==='high')typeMap[ft].high++;
      typeMap[ft].auths.add(c.authority);
      const k=c.category||'Other';
      typeMap[ft].cats[k]=(typeMap[ft].cats[k]||0)+1;
    });
    const typeItems=Object.values(typeMap).sort((a,b)=>b.total-a.total);
    const fc=document.getElementById('fac-count'); if(fc)fc.textContent=`${typeItems.length} facility type${typeItems.length!==1?'s':''}`;
    if(!typeItems.length){
      el.innerHTML='<div class="empty"><div class="empty-icon">&#127981;</div><div class="empty-text">No facility data matches filters</div></div>';
      return;
    }
    const noteHtml='<div class="facility-data-note">Facility-level data not yet structured — showing category-level risk only</div>';
    el.innerHTML=noteHtml+typeItems.map(f=>{
      const safe=f.name.replace(/'/g,"\\'");
      const topCatEntry=Object.entries(f.cats).sort((a,b)=>b[1]-a[1])[0];
      const topCatName=topCatEntry?topCatEntry[0]:'';
      const topIssues=Object.entries(f.cats).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k).join(', ');
      const action=generateFacilityAction(topCatName);
      return `<div class="facility-card${pF.factype===f.name?' fc-active':''}" onclick="pFilterByFacType('${safe}')" title="Click to filter">
        <div class="facility-name">${f.name}</div>
        <div class="facility-type-label">${[...f.auths].join(' · ')}</div>
        <div class="facility-stats">
          <span class="facility-stat">${f.total} citation${f.total!==1?'s':''}</span>
          ${f.high?`<span class="facility-stat fsr">${f.high} high sev</span>`:''}
        </div>
        ${topIssues?`<div class="facility-cats">Top: ${topIssues}</div>`:''}
        <div class="facility-action">&#8594; ${action}</div>
      </div>`;
    }).join('');
    return;
  }

  // ── Per-company view ──
  const fc=document.getElementById('fac-count'); if(fc)fc.textContent=`${coItems.length} facilit${coItems.length!==1?'ies':'y'}`;
  if(!coItems.length) {
    el.innerHTML='<div class="empty"><div class="empty-icon">&#127981;</div><div class="empty-text">No facilities match</div></div>';
    return;
  }
  el.innerHTML=coItems.map(f=>{
    const topCatEntry=Object.entries(f.cats).sort((a,b)=>b[1]-a[1])[0];
    const topCatName=topCatEntry?topCatEntry[0]:'';
    const topIssues=Object.entries(f.cats).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k).join(', ');
    const action=generateFacilityAction(topCatName);
    return `<div class="facility-card">
      <div class="facility-name">${f.name}</div>
      <div class="facility-type-label">${f.factype} &middot; ${[...f.auths].join(' / ')}</div>
      <div class="facility-stats">
        <span class="facility-stat">${f.total} citation${f.total!==1?'s':''}</span>
        ${f.high?`<span class="facility-stat fsr">${f.high} high sev</span>`:''}
      </div>
      ${topIssues?`<div class="facility-cats">Top issues: ${topIssues}</div>`:''}
      <div class="facility-action">&#8594; ${action}</div>
    </div>`;
  }).join('');
}

// ── Alert card (compact, action-oriented) ────────────────────────────
function alertCard(c, patterns={}) {
  const isHigh=c.severity==='high';
  const st=(c.source_type||'').replace(/_/g,' ');
  const isImport=(c.source_type||'')==='import_alert';
  const summ=(c.summary||c.category||'Enforcement action').slice(0,160);
  const cat=c.category||'Other';
  const intel=CAT_INTEL[cat];
  const action=intel?intel.action:`Review exposure for ${cat} and monitor for similar enforcement patterns across your site types.`;
  const pBadge=patterns[c.company]?`<span class="pattern-badge">Pattern: recurring ${(c.company||'').slice(0,28)}</span>`
    :patterns[c.category]?`<span class="pattern-badge">Pattern: repeated ${c.category}</span>`:'';
  return `<div class="alert-card${isHigh?' ac-high':' ac-medium'}">
    <div class="alert-card-title">${summ}</div>
    <div class="alert-card-meta">
      ${sevBadge(c.severity||'medium')}
      <span class="badge badge-authority">${c.authority||'—'}</span>
      <span class="badge badge-type"${isImport?` title="${_IMPORT_ALERT_TIP}"`:''} style="${isImport?'cursor:help':''}">${st}</span>
      ${c.facility_type?`<span class="badge" style="background:rgba(139,92,246,.06);color:rgba(139,92,246,.5);border:1px solid rgba(139,92,246,.1)">${c.facility_type}</span>`:''}
      ${c.category?`<span class="badge" style="background:rgba(20,50,80,.2);color:#3A5570;border:1px solid rgba(20,50,80,.3)">${c.category}</span>`:''}
      ${pBadge}
    </div>
    <div class="alert-card-action"><b>Action:</b> ${action}</div>
    <div class="alert-card-footer">
      <div class="alert-card-date">${c.date?c.date.slice(0,10):'—'}${c.company?' · '+c.company:''}</div>
      ${c.url?`<a class="card-action card-action-primary" href="${c.url}" target="_blank" style="font-size:10px">View &#8599;</a>`:''}
    </div>
  </div>`;
}

// ── Pharma alerts ────────────────────────────────────────────────────
function renderPharmaAlerts() {
  const el=document.getElementById('pharma-alerts-feed'); if(!el)return;
  const highs=unifiedFilteredCitations({sev:'high',sortCol:'date',sortDir:-1});
  const patterns=detectPattern(highs);
  el.innerHTML=highs.length?highs.map(c=>alertCard(c,patterns)).join('')
    :'<div class="empty"><div class="empty-icon">&#9989;</div><div class="empty-text">No high severity alerts</div></div>';
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
  pF={auth:'all',factype:'all',srctype:'all',sev:'all',company:'',query:'',dateFrom:'',dateTo:'',dicapa:false};
  pCatFilter=null;
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
  pF={auth:'all',factype:'all',srctype:'all',sev:'all',company:'',query:'',dateFrom:'',dateTo:'',dicapa:false};
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
  syncPFPills();
  const activeTab = document.querySelector('#pharma-nav .nav-tab.active');
  const tab = activeTab ? activeTab.dataset.ptab : 'pharma-overview';
  if(tab==='pharma-overview') {
    renderPharmaOverview();
    const feed=document.getElementById('pharma-ov-feed');
    if(feed) feed.scrollIntoView({behavior:'smooth',block:'start'});
  } else {
    renderPAll(); // stays on current tab — no tab switch
  }
}

// ── Shared chip bar renderer ─────────────────────────────────────────
function buildChipBar(elId) {
  const el=document.getElementById(elId); if(!el) return;
  const chips=[];
  if(pF.sev!=='all')     chips.push({label:`Severity: ${pF.sev}`,                    fn:`pF.sev='all';_setPActiveKpi(null);syncPFPills();renderPAll()`});
  if(pF.srctype!=='all') chips.push({label:`Type: ${pF.srctype.replace(/_/g,' ')}`,  fn:`pF.srctype='all';_setPActiveKpi(null);syncPFPills();renderPAll()`});
  if(pF.auth!=='all')    chips.push({label:`Authority: ${pF.auth}`,                  fn:`pF.auth='all';syncPFPills();renderPAll()`});
  if(pF.factype!=='all') chips.push({label:`Facility: ${pF.factype}`,                fn:`pF.factype='all';syncPFPills();renderPAll()`});
  if(pF.dicapa)          chips.push({label:'DI / CAPA / CSV',                        fn:`pF.dicapa=false;_setPActiveKpi(null);syncPFPills();renderPAll()`});
  if(pCatFilter)         chips.push({label:`Category: ${pCatFilter}`,                fn:`pCatFilter=null;_dirty=true;renderPAll()`});
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
