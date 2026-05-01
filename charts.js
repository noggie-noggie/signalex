// ─── CHARTS ───────────────────────────────────────────────────────────────────
const PAL=['#0D9488','#3B82F6','#F97316','#8B5CF6','#F59E0B','#EF4444','#10B981','#EC4899','#06B6D4','#84CC16','#A3E635'];
const CA='#8FA5BC',CG='rgba(40,90,150,.25)';
const sc={x:{ticks:{color:CA,font:{size:11}},grid:{color:CG}},y:{ticks:{color:CA,font:{size:11}},grid:{color:CG}}};
// Store all chart refs so we can resize them when tabs become visible
const _charts = {};
const mk=(id,cfg)=>{const c=document.getElementById(id);if(c){_charts[id]=new Chart(c,cfg);return _charts[id];}};
// chart-sources — computed from SIGNALS by authority
const _srcAuthMap={'PubMed':'pubmed','Europe PMC':'europe_pmc','ClinTrials':'clinical_trials','TGA Consult':'tga_consultations','Cochrane':'cochrane','ARTG':'artg','FDA':'fda','TGA':'tga','EFSA':'efsa','SemanticScholar':'semantic_scholar','Adv.Events':'adverse_events'};
const _srcLabels=Object.keys(_srcAuthMap);
const _srcData=_srcLabels.map(lbl=>SIGNALS.filter(s=>s.authority===_srcAuthMap[lbl]).length);
mk('chart-sources',{type:'bar',data:{labels:_srcLabels,datasets:[{data:_srcData,backgroundColor:PAL,borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:sc}});
// chart-sentiment — computed from SIGNALS
const _sentData=[SIGNALS.filter(s=>(s.sentiment||'').toLowerCase()==='positive').length,SIGNALS.filter(s=>(s.sentiment||'').toLowerCase()==='neutral').length,SIGNALS.filter(s=>(s.sentiment||'').toLowerCase()==='negative').length];
mk('chart-sentiment',{type:'doughnut',data:{labels:['Positive','Neutral','Negative'],datasets:[{data:_sentData,backgroundColor:['#059669','#334155','#DC2626'],borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{color:'#B4C8D8',font:{size:11},padding:10}}}}});
// chart-severity — computed from SIGNALS
const _sevData=[SIGNALS.filter(s=>(s.severity||'').toLowerCase()==='low').length,SIGNALS.filter(s=>(s.severity||'').toLowerCase()==='medium').length,SIGNALS.filter(s=>(s.severity||'').toLowerCase()==='high').length];
mk('chart-severity',{type:'doughnut',data:{labels:['Low','Medium','High'],datasets:[{data:_sevData,backgroundColor:['#334155','#D97706','#DC2626'],borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{color:'#B4C8D8',font:{size:11},padding:10}}}}});
// chart-events — computed from SIGNALS
const _evtKnown=['recall','safety_alert','warning','new_listing'];
const _evtData=[SIGNALS.filter(s=>!_evtKnown.includes((s.event_type||'').toLowerCase())).length,SIGNALS.filter(s=>(s.event_type||'').toLowerCase()==='safety_alert').length,SIGNALS.filter(s=>(s.event_type||'').toLowerCase()==='warning').length,SIGNALS.filter(s=>(s.event_type||'').toLowerCase()==='new_listing').length,SIGNALS.filter(s=>(s.event_type||'').toLowerCase()==='recall').length];
mk('chart-events',{type:'bar',data:{labels:['Research/Other','Safety Alert','Warning','New Listing','Recall'],datasets:[{data:_evtData,backgroundColor:['#1E3A5F','#F59E0B','#D97706','#3B82F6','#EF4444'],borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:sc}});
// chart-categories — top 8 categories from CITATIONS
const _catMap8={};CITATIONS.forEach(c=>{const k=c.category||'Other';_catMap8[k]=(_catMap8[k]||0)+1;});
const _catE8=Object.entries(_catMap8).sort((a,b)=>b[1]-a[1]).slice(0,8);
mk('chart-categories',{type:'bar',data:{labels:_catE8.map(x=>x[0]),datasets:[{data:_catE8.map(x=>x[1]),backgroundColor:'#0D9488',borderRadius:4}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:CA,font:{size:11}},grid:{color:CG}},y:{ticks:{color:CA,font:{size:11}},grid:{display:false}}}}});
const ingMap={};SIGNALS.forEach(s=>{const i=(s.ingredient_name||'').trim();if(!i||i==='unknown')return;ingMap[i]=(ingMap[i]||0)+1});
const sortedIngs=Object.entries(ingMap).sort((a,b)=>b[1]-a[1]).slice(0,15);
mk('trend-ingredients',{type:'bar',data:{labels:sortedIngs.map(x=>x[0]),datasets:[{data:sortedIngs.map(x=>x[1]),backgroundColor:'#0D9488',borderRadius:4}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>`${ctx.parsed.x} signals`}}},scales:{x:{ticks:{color:CA,font:{size:11}},grid:{color:CG}},y:{ticks:{color:CA,font:{size:11}},grid:{display:false}}},onClick:(evt,els)=>{if(els.length){const name=sortedIngs[els[0].index][0];hideTip();filterByIngredient(name);}},onHover:(evt,els)=>{if(evt.native)evt.native.target.style.cursor=els.length?'pointer':'default';}}});
const auths2=['tga','tga_consultations','fda','artg','pubmed','europe_pmc','clinical_trials','cochrane'];
const aLbls=['TGA','TGA Consult.','FDA','ARTG','PubMed','Europe PMC','ClinTrials','Cochrane'];
mk('trend-auth-sent',{type:'bar',data:{labels:aLbls,datasets:[{label:'Positive',data:auths2.map(a=>SIGNALS.filter(s=>s.authority===a&&s.sentiment==='positive').length),backgroundColor:'#059669'},{label:'Neutral',data:auths2.map(a=>SIGNALS.filter(s=>s.authority===a&&s.sentiment==='neutral').length),backgroundColor:'#334155'},{label:'Negative',data:auths2.map(a=>SIGNALS.filter(s=>s.authority===a&&s.sentiment==='negative').length),backgroundColor:'#DC2626'}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{color:'#B4C8D8',font:{size:11}}}},scales:{x:{stacked:true,ticks:{color:CA,font:{size:11}},grid:{display:false}},y:{stacked:true,ticks:{color:CA,font:{size:11}},grid:{color:CG}}},onClick:(evt,els)=>{if(els.length){const auth=auths2[els[0].index];hideTip();filterByAuthority(auth);}},onHover:(evt,els)=>{if(evt.native)evt.native.target.style.cursor=els.length?'pointer':'default';}}});
const _newSrcAuths=['europe_pmc','clinical_trials','cochrane','efsa','semantic_scholar'];
mk('trend-new-sources',{type:'bar',data:{labels:['Europe PMC','ClinTrials','Cochrane','EFSA','Semantic Scholar'],datasets:[{data:_newSrcAuths.map(a=>SIGNALS.filter(s=>s.authority===a).length),backgroundColor:PAL,borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:sc,onClick:(evt,els)=>{if(els.length){const auth=_newSrcAuths[els[0].index];hideTip();filterByAuthority(auth);}},onHover:(evt,els)=>{if(evt.native)evt.native.target.style.cursor=els.length?'pointer':'default';}}});
(()=>{
  const auths3=['FDA','EFSA','TGA','BfR','MHRA'];
  // Map CITATIONS category names to CLAIM_KW keys for VMS filter links
  const _hmClaimMap={'GMP violations':'GMP & manufacturing','Labelling & claims':'Labelling & claims','Ingredient safety':'Ingredient safety','Contamination':'Contamination risk','Contamination risk':'Contamination risk','Sterility':'Sterility','Sterility assurance':'Sterility'};
  // Compute top-10 categories and per-cell counts from CITATIONS
  const _hmCats={};CITATIONS.forEach(c=>{const k=c.category||'Other';_hmCats[k]=(_hmCats[k]||0)+1;});
  const _hmCatList=Object.entries(_hmCats).sort((a,b)=>b[1]-a[1]).slice(0,10).map(x=>x[0]);
  const max=Math.max(...Object.values(_hmCats),1);
  let html=`<div class="heatmap-col-labels">${auths3.map(a=>`<div class="heatmap-col-label">${a}</div>`).join('')}</div>`;
  _hmCatList.forEach(cat=>{
    const vals=auths3.map(a=>CITATIONS.filter(c=>(c.category||'Other')===cat&&c.authority===a).length);
    const safe=cat.replace(/'/g,"\\'");
    const claimCat=_hmClaimMap[cat]||null;
    const clickFn=claimCat?`filterByClaim('${claimCat.replace(/'/g,"\\'")}')`:`gotoEvidence('${safe}')`;
    html+=`<div class="heatmap-row" onclick="${clickFn}" title="Click to filter: ${cat}">
      <div class="heatmap-label">${cat}</div>
      <div class="heatmap-cells">${vals.map(v=>`<div class="heatmap-cell" style="background:rgba(13,148,136,${(.08+v/max*.85).toFixed(2)})" title="${v}"></div>`).join('')}
      </div></div>`;
  });
  document.getElementById('heatmap-container').innerHTML=html;
})();
// report-chart-auth — computed from CITATIONS by authority
const _rptAuthKeys=['FDA','EFSA','TGA','BfR','MHRA'];
const _rptAuthData=_rptAuthKeys.map(a=>CITATIONS.filter(c=>c.authority===a).length);
mk('report-chart-auth',{type:'bar',data:{labels:_rptAuthKeys,datasets:[{data:_rptAuthData,backgroundColor:PAL,borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:sc,onClick:(evt,els)=>{if(els.length){gotoEvidence(undefined,_rptAuthKeys[els[0].index]);}},onHover:(evt,els)=>{if(evt.native)evt.native.target.style.cursor=els.length?'pointer':'default';}}});
// report-chart-cats — top 10 categories from CITATIONS (dynamic labels + data)
const _rptCatMap={};CITATIONS.forEach(c=>{const k=c.category||'Other';_rptCatMap[k]=(_rptCatMap[k]||0)+1;});
const _rptCatEntries=Object.entries(_rptCatMap).sort((a,b)=>b[1]-a[1]).slice(0,10);
const _rptCatLabels=_rptCatEntries.map(x=>x[0]);
const _rptCatData=_rptCatEntries.map(x=>x[1]);
mk('report-chart-cats',{type:'bar',data:{labels:_rptCatLabels,datasets:[{data:_rptCatData,backgroundColor:PAL,borderRadius:4}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:CA,font:{size:11}},grid:{color:CG}},y:{ticks:{color:CA,font:{size:11}},grid:{display:false}}},onClick:(evt,els)=>{if(els.length){gotoEvidence(_rptCatLabels[els[0].index]);}},onHover:(evt,els)=>{if(evt.native)evt.native.target.style.cursor=els.length?'pointer':'default';}}});

// ─── TREND COMPUTATIONS ───────────────────────────────────────────────────────
function computeIngredientTrends() {
  const now=Date.now(),d7=now-7*864e5,d14=now-14*864e5;
  const curr={},prev={},highM={};
  SIGNALS.forEach(s=>{
    const t=new Date(s.scraped_at||s.created_at).getTime();
    const ing=(s.ingredient_name||'').trim().toLowerCase();
    if(!ing||ing==='unknown') return;
    if(t>=d7){curr[ing]=(curr[ing]||0)+1;if(s.severity==='high')highM[ing]=(highM[ing]||0)+1;}
    else if(t>=d14) prev[ing]=(prev[ing]||0)+1;
  });
  return Object.entries(curr)
    .sort((a,b)=>{const ca=a[1]-(prev[a[0]]||0),cb=b[1]-(prev[b[0]]||0);return cb-ca||b[1]-a[1];})
    .slice(0,6).map(([ing,c])=>({name:ing,curr:c,prev:prev[ing]||0,high:highM[ing]||0}));
}
// Returns all CLAIM_CATEGORIES keys matched by a single signal.
function getClaimCategoryMatches(signal) {
  return Object.keys(CLAIM_CATEGORIES).filter(cat => signalMatchesClaimCategory(signal, cat));
}

// Classify all signals, count matches per category, remove 0-match categories, sort by count desc.
function getRankedClaimCategories(signals) {
  const counts = {};
  signals.forEach(s => {
    getClaimCategoryMatches(s).forEach(cat => { counts[cat] = (counts[cat] || 0) + 1; });
  });
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .map(([cat, count]) => ({ key: cat, label: CLAIM_CATEGORIES[cat].label, count }));
}

// Returns signals that match zero categories.
function getUnclassifiedSignals(signals) {
  return signals.filter(s => getClaimCategoryMatches(s).length === 0);
}

// Console-only debug tool. Call debugClaimCategoryMatches() in browser console.
function debugClaimCategoryMatches() {
  const ranked = getRankedClaimCategories(SIGNALS);
  console.group('[Signalex] Claim Category Debug');
  ranked.forEach(({ key, label, count }) => {
    const top5 = SIGNALS.filter(s => signalMatchesClaimCategory(s, key)).slice(0, 5).map(s => s.title || '(no title)');
    console.group(`${label} — ${count} signals`);
    top5.forEach(t => console.log(' •', t));
    console.groupEnd();
  });
  const unclassified = getUnclassifiedSignals(SIGNALS);
  console.group(`Unclassified — ${unclassified.length} signals`);
  unclassified.slice(0, 5).forEach(s => console.log(' •', s.title || '(no title)'));
  console.groupEnd();
  const multi = SIGNALS.filter(s => getClaimCategoryMatches(s).length > 1);
  console.log(`Multi-category signals: ${multi.length}`, multi.slice(0, 3).map(s => ({ title: (s.title || '').slice(0, 50), cats: getClaimCategoryMatches(s) })));
  console.groupEnd();
}

function computeClaimRisk() {
  return getRankedClaimCategories(SIGNALS).slice(0, 5).map(({ key: cat, count: total }) => {
    const m = SIGNALS.filter(s => signalMatchesClaimCategory(s, cat));
    const high = m.filter(s => s.severity === 'high').length;
    const ratio = high / total;
    const risk = ratio > .5 ? 'H' : ratio > .2 ? 'M' : 'L';
    const auths = [...new Set(m.map(s => authLabel(s.authority)))].slice(0, 3).join(', ');
    return { cat, total, high, ratio, risk, auths };
  });
}
function computeEnforcementTrends() {
  const now=Date.now(),d7=now-7*864e5,d14=now-14*864e5;
  const regs=['tga','fda','artg','adverse_events','tga_consultations','efsa','cochrane'];
  const curr={},prev={},highM={};
  SIGNALS.forEach(s=>{
    if(!regs.includes(s.authority)) return;
    const t=new Date(s.scraped_at||s.created_at).getTime();
    if(t>=d7){curr[s.authority]=(curr[s.authority]||0)+1;if(s.severity==='high')highM[s.authority]=(highM[s.authority]||0)+1;}
    else if(t>=d14) prev[s.authority]=(prev[s.authority]||0)+1;
  });
  return regs.map(a=>({auth:a,label:AUTH_LABELS[a]||a.toUpperCase(),curr:curr[a]||0,prev:prev[a]||0,change:(curr[a]||0)-(prev[a]||0),high:highM[a]||0}))
    .filter(t=>t.curr>0).sort((a,b)=>b.change-a.change||b.curr-a.curr).slice(0,6);
}

// ─── TREND PANELS RENDER ──────────────────────────────────────────────────────
function renderTrendPanels() {
  // Ingredient
  const ings=computeIngredientTrends();
  const ingEl=document.getElementById('tp-ingredients');
  if(ingEl){
    if(!ings.length){
      const allIngs={};
      SIGNALS.forEach(s=>{const ing=(s.ingredient_name||'').trim().toLowerCase();if(!ing||ing==='unknown')return;allIngs[ing]=(allIngs[ing]||0)+1;});
      const top6=Object.entries(allIngs).sort((a,b)=>b[1]-a[1]).slice(0,6);
      if(!top6.length){ingEl.innerHTML='<div class="trend-none">No significant ingredient trends</div>';}
      else{ingEl.innerHTML=''
        +'<div style="font-size:9px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--slate-dim);margin-bottom:3px;pointer-events:none;cursor:default">Baseline ingredient watchlist</div>'
        +'<div style="font-size:10px;color:#527292;margin-bottom:6px;padding-bottom:5px;border-bottom:1px solid rgba(30,70,110,.4);pointer-events:none">Tracking volume until meaningful trend movement emerges</div>'
        +top6.map(([ing,count])=>{
          const nm=ing.replace(/'/g,"\\'");
          const isAct=filters.ingredient.toLowerCase()===ing;
          return `<div class="trend-row${isAct?' tr-active':''}" onclick="filterByIngredient('${nm}')" onmouseenter="showIngTip(event,'${nm}')" onmouseleave="hideTip()">
            <div style="flex:1;min-width:0"><span class="trend-rname">${ing}</span></div>
            <div style="display:flex;align-items:center;gap:4px;flex-shrink:0"><span class="t-ct">${count}</span><span class="t-conf t-conf-low">all time</span></div>
          </div>`;
        }).join('')
        +'<div style="font-size:10px;color:rgba(13,148,136,.82);padding-top:5px;margin-top:3px;border-top:1px solid rgba(30,70,110,.4)"><b style="color:rgba(13,148,136,.95)">Action:</b> Monitor these ingredients for emerging regulatory or claim signals</div>';}
    }
    else{ingEl.innerHTML=ings.map(i=>{
      const chg=i.curr-i.prev,pct=i.prev>0?Math.round(chg/i.prev*100):null;
      const ind=chg>0?`<span class="t-up">&#8679; ${pct!==null?'+'+pct+'%':'+'+chg}</span>`:chg<0?`<span class="t-dn">&#8681; ${pct!==null?pct+'%':chg}</span>`:`<span class="t-flat">&mdash;</span>`;
      const hb=i.high>0?` <span class="rbadge rbadge-H">${i.high}!</span>`:'';
      const conf=i.curr>=5?'high':i.curr>=2?'medium':'low';
      const why=chg>0?`Rising${pct!==null?' +'+pct+'%':''} vs last week${i.high?' · '+i.high+' high-sev':''}`:chg<0?`Down${pct!==null?' '+pct+'%':''} vs last week${i.high?' · '+i.high+' high-sev':''}`:i.high>0?`Stable · ${i.high} high-severity alert${i.high>1?'s':''}`:null;
      const nm=i.name.replace(/'/g,"\\'");
      const isAct=filters.ingredient.toLowerCase()===i.name;
      return `<div class="trend-row${isAct?' tr-active':''}" onclick="filterByIngredient('${nm}')" onmouseenter="showIngTip(event,'${nm}')" onmouseleave="hideTip()">
        <div style="flex:1;min-width:0">
          <div style="display:flex;align-items:center;gap:5px">
            <span class="trend-rname">${i.name}</span>
            <button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${nm}','ingredient')" title="Open ${i.name} detail">&#9432;</button>
          </div>
          ${why?`<span class="trend-row-why">${why}</span>`:''}
        </div>
        <div style="display:flex;align-items:center;gap:4px;flex-shrink:0">${ind}<span class="t-ct">${i.curr}${hb}</span><span class="t-conf t-conf-${conf}">${conf}</span></div>
      </div>`;
    }).join('');}
  }
  // Claim risk
  const risks=computeClaimRisk();
  const claimEl=document.getElementById('tp-claim');
  if(claimEl){
    if(!risks.length){claimEl.innerHTML='<div class="trend-none">No significant claim risk trends</div>';}
    else{claimEl.innerHTML=risks.map(r=>{
      const cls=r.risk==='H'?'rbadge-H':r.risk==='M'?'rbadge-M':'rbadge-L';
      const lbl=r.risk==='H'?'HIGH':r.risk==='M'?'MED':'LOW';
      const conf=r.total>=10?'high':r.total>=4?'medium':'low';
      const pctH=Math.round(r.ratio*100);
      const why=r.risk==='H'?`${pctH}% high-sev — elevated compliance exposure`:r.risk==='M'?`${pctH}% high-sev — monitor closely`:r.total>3?`Low risk ratio — substantiation pathway open`:null;
      const isAct=activeClaimCat===r.cat;
      const catSafe=r.cat.replace(/'/g,"\\'");
      const authsSafe=r.auths.replace(/'/g,"\\'");
      return `<div class="trend-row${isAct?' tr-active':''}" onclick="filterByClaim('${catSafe}')" onmouseenter="showClaimTip(event,'${catSafe}',${r.total},${r.high},'${authsSafe}')" onmouseleave="hideTip()">
        <div style="flex:1;min-width:0">
          <div style="display:flex;align-items:center;gap:5px">
            <span class="trend-rname">${r.cat}</span>
            <button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${catSafe}','category')" title="Open ${r.cat} detail">&#9432;</button>
          </div>
          ${why?`<span class="trend-row-why">${why}</span>`:''}
        </div>
        <div style="display:flex;align-items:center;gap:4px;flex-shrink:0"><span class="rbadge ${cls}">${lbl}</span><span class="t-ct">${r.total}</span><span class="t-conf t-conf-${conf}">${conf}</span></div>
      </div>`;
    }).join('');}
  }
  // Enforcement
  const enfs=computeEnforcementTrends();
  const enfEl=document.getElementById('tp-enforcement');
  if(enfEl){
    if(!enfs.length){enfEl.innerHTML='<div class="trend-none">No significant enforcement changes</div>';}
    else{enfEl.innerHTML=enfs.map(e=>{
      const chg=e.change;
      const ind=chg>0?`<span class="t-up">&#8679; +${chg}</span>`:chg<0?`<span class="t-dn">&#8681; ${chg}</span>`:`<span class="t-flat">&mdash;</span>`;
      const hb=e.high>0?` <span class="rbadge rbadge-H">${e.high}!</span>`:'';
      const conf=e.curr>=5?'high':e.curr>=2?'medium':'low';
      const why=chg>0?`+${chg} vs prior week${e.high?' · '+e.high+' high-sev':''}`:chg<0?`${chg} vs prior week — activity declining`:e.high>0?`Stable · ${e.high} high-severity alert${e.high>1?'s':''}`:null;
      const isAct=filters.auth.size===1&&filters.auth.has(e.auth);
      return `<div class="trend-row${isAct?' tr-active':''}" onclick="filterByAuthority('${e.auth}')" onmouseenter="showAuthTip(event,'${e.auth}')" onmouseleave="hideTip()">
        <div style="flex:1;min-width:0">
          <div style="display:flex;align-items:center;gap:5px">
            <span class="trend-rname">${e.label}</span>
            <button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${e.auth}','authority')" title="Open ${e.label} detail">&#9432;</button>
          </div>
          ${why?`<span class="trend-row-why">${why}</span>`:''}
        </div>
        <div style="display:flex;align-items:center;gap:4px;flex-shrink:0">${ind}<span class="t-ct">${e.curr}${hb}</span><span class="t-conf t-conf-${conf}">${conf}</span></div>
      </div>`;
    }).join('');}
  }
}
