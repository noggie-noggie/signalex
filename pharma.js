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
