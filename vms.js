// === SIGNALEX VMS HELPERS ===

// ── Text and matching helpers ──────────────────────────────────────────────────
// ─── MULTI-FIELD TEXT HELPER ─────────────────────────────────────────────────
function _sigText(s) {
  return [s.title,s.summary,s.ai_summary,s.ingredient_name,s.ingredient_relevance,s.trend_relevance,s.signal_type].filter(Boolean).join(' ').toLowerCase();
}
// Claim category matcher — used by BOTH computeClaimRisk() and filteredSignals()
// Searches title + summary + ai_summary only; excludes noisy metadata fields
function signalMatchesClaimCategory(s, cat) {
  const kws = CLAIM_KW[cat] || [];
  if (!kws.length) return false;
  const txt = [s.title, s.summary, s.ai_summary].filter(Boolean).join(' ').toLowerCase();
  return kws.some(k => txt.includes(k));
}
function _ingMatch(hay, q) {
  const ql = q.toLowerCase();
  if (hay.includes(ql)) return true;
  // partial: split multi-word/slash names and match any meaningful term
  const terms = ql.split(/[\s\/\-]+/).filter(t=>t.length>2);
  return terms.length > 0 && terms.some(t => hay.includes(t));
}
// Strict word-boundary matcher for entity tab — prevents "vitamin d" matching "vitamin b"
function _entityIngMatch(signal, name) {
  const nameL = name.toLowerCase().trim();
  // 1. Exact ingredient_name field match
  if ((signal.ingredient_name || '').toLowerCase().trim() === nameL) return true;
  const hay = _sigText(signal);
  // 2. Exact substring match (catches "vitamin d3" when query is "vitamin d")
  //    followed by word-boundary OR digit (for D3, D2 etc.) but NOT a letter
  //    that would form a different compound (e.g. "vitamin b")
  const escaped = nameL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  // Match name preceded by start/space/punct and followed by end/space/punct/digit
  const re = new RegExp('(?:^|[\\s,.(\\/\\-])' + escaped + '(?:[\\s,.)\\/\\-\\d]|$)', 'i');
  return re.test(hay);
}

// ─── UNIQUE AI SUMMARIES ──────────────────────────────────────────────────────
// Detect generic/duplicate AI summaries and replace with signal-specific ones
const _GENERIC_PATTERNS = [
  'clinical trial on vitamin b',
  'positive results could substantiate efficacy claims for premium',
  'efficacy claims for premium vms',
  'positive signal for vms',
  'monitor for regulatory implications',
];
function _isGenericWhy(text) {
  if (!text) return true;
  const t = text.toLowerCase();
  return _GENERIC_PATTERNS.some(p => t.includes(p));
}
function _uniqueWhy(s) {
  const raw = s.ai_summary || s.sentiment_reasoning || '';
  if (!_isGenericWhy(raw)) return raw;
  // Generate a unique summary from signal properties
  const src = authLabel(s.authority);
  const ing = s.ingredient_name && s.ingredient_name !== 'unknown' ? s.ingredient_name : '';
  const evType = s.event_type && s.event_type !== 'other' ? s.event_type.replace(/_/g,' ') : 'research signal';
  const sent = (s.sentiment || '').toLowerCase();
  const sev = (s.severity || '').toLowerCase();
  if (sev === 'high') {
    const action = sent === 'negative' ? 'Immediate portfolio review required' : 'Monitor for compliance impact';
    return `${src}: ${evType}${ing ? ' on ' + ing : ''} — ${action}`;
  }
  if (ing) {
    const action = sent === 'positive' ? 'Supports efficacy dossiers and marketing claims'
      : sent === 'negative' ? 'Flag for regulatory risk assessment'
      : 'Track for market and formulation implications';
    return `${src}: ${evType} on ${ing} — ${action}`;
  }
  // Fallback: derive something unique from title prefix
  const titleSnip = (s.title || evType).slice(0, 55).replace(/\s+\S*$/, '');
  const impact = sent === 'positive' ? 'Positive evidence for claims substantiation'
    : sent === 'negative' ? 'Adverse signal requiring compliance monitoring'
    : 'Track for VMS regulatory implications';
  return `${src}: ${titleSnip} — ${impact}`;
}

// ─── KNOWN INGREDIENT TERMS (for title extraction) ─────────────────────────────
const KNOWN_INGREDIENTS = ['ashwagandha', 'turmeric', 'curcumin', 'melatonin', 'probiotic', 'probiotics', 'omega-3', 'omega 3', 'collagen', 'magnesium', 'vitamin d', 'vitamin c', 'vitamin b', 'zinc', 'iron', 'calcium', 'creatine', 'caffeine', 'cbd', 'cannabidiol', 'berberine', 'resveratrol', 'coq10', 'coenzyme q10', 'nmn', 'nad+', 'glutathione', 'quercetin', 'folic acid', 'folate', 'biotin', 'selenium', 'iodine', 'fish oil', 'krill oil', 'spirulina', 'chlorella', 'maca', 'ginseng', 'echinacea', 'valerian', 'chamomile', 'st. john', 'ginkgo', 'ginkgo biloba', 'saw palmetto', 'black cohosh', 'evening primrose', 'milk thistle', 'rhodiola', 'holy basil', 'bacopa', "lion's mane", 'reishi', 'chaga', 'saffron', 'garlic', 'cinnamon', 'ginger', 'beetroot', 'acai', 'elderberry', 'lutein', 'lycopene', 'beta-carotene', 'astaxanthin', 'phosphatidylserine', 'alpha lipoic', 'acetyl-l-carnitine', 'l-carnitine', 'l-theanine', '5-htp', 'tryptophan', 'glucosamine', 'chondroitin', 'msm', 'hyaluronic acid', 'colostrum', 'whey protein', 'pea protein', 'betaine', 'arginine', 'citrulline', 'beet', 'nitrate', 'peptide', 'bpc-157', 'tb-500', 'ghk-cu', 'artri ajo', 'sea buckthorn', 'inositol', 'chromium', 'boron', 'silicon', 'strontium', 'lithium', 'manganese', 'molybdenum', 'copper', 'potassium', 'sodium bicarbonate', 'sodium phosphate', 'beta-alanine', 'hmb', 'cla', 'green tea', 'egcg', 'raspberry ketone', 'glucomannan', 'psyllium', 'inulin', 'fos', 'mos', 'lactulose', 'xylitol', 'stevia', 'monk fruit'];

// ── Known ingredient extraction ────────────────────────────────────────────────
function _extractIngFromTitle(title) {
  const t = (title||'').toLowerCase();
  for (const kw of KNOWN_INGREDIENTS) { if (t.includes(kw)) return kw; }
  return null;
}

// ── Ingredient insight helpers ──────────────────────────────────────────────────
function _computeIngTrend(name) {
  const now=Date.now(),d7=now-7*864e5,d14=now-14*864e5;
  let curr=0,prev=0,high=0,neg=0,pos=0;
  SIGNALS.forEach(s=>{
    if(!_entityIngMatch(s,name)) return;
    const t=new Date(s.scraped_at||s.created_at).getTime();
    if(t>=d7){curr++;if(s.severity==='high')high++;if(s.sentiment==='negative')neg++;if(s.sentiment==='positive')pos++;}
    else if(t>=d14) prev++;
  });
  const chg=curr-prev;
  const conf=curr>=5?'high':curr>=2?'medium':'low';
  const trend=chg>curr*.3?'rising':chg<-curr*.3?'declining':curr>0?'stable':'flat';
  return {curr,prev,high,neg,pos,chg,conf,trend};
}
function _ingWhyMatters(name,total,high,neg,trend) {
  if(trend==='rising'&&high>0) return `${high} high-severity signal${high>1?'s':''} with rising activity — regulatory attention or emerging safety concern.`;
  if(trend==='rising') return `Activity up this week (${total} signals) — emerging market interest or pending scrutiny. Flag for client briefing.`;
  if(high>2) return `${high} high-severity alerts — review labeling and claims for compliance exposure now.`;
  if(total>0&&neg>total*.6) return `Majority negative sentiment (${Math.round(neg/total*100)}%) — monitor for enforcement or claim challenge risk.`;
  return `${total} signal${total>1?'s':''} tracked across regulatory, research and adverse event sources.`;
}
function _ingAction(name,trend,high,neg) {
  if(trend==='rising'&&high>0) return 'Audit product range immediately; check authority enforcement calendar for upcoming decisions.';
  if(trend==='rising') return 'Flag for client briefing; review label claims against recent evidence base.';
  if(high>0) return 'Review high-severity signals; escalate to compliance team if product exposure confirmed.';
  if(neg>2) return 'Monitor claim language carefully; adverse event reports can precede regulatory action.';
  return 'No immediate action required — continue routine monitoring cadence.';
}

// === VMS OVERVIEW + LIVE SIGNALS RENDER ===

function signalCard(s) {
  const sevClass = s.severity==='high'?'high-sev':s.severity==='medium'?'medium-sev':'low-sev';
  const why = _uniqueWhy(s);
  const ing = s.ingredient_name&&s.ingredient_name!=='unknown'?s.ingredient_name:'';
  const pp = isPreprint(s.authority);
  const ingSafe = ing.replace(/'/g,"\\'");
  const authSafe = (s.authority||'').replace(/'/g,"\\'");
  const wlId = 'sig:'+s.id;
  const wlTitle = (s.title||'Signal').replace(/'/g,"\\'").slice(0,45);
  const wlActive = isWatched(wlId);
  return `<div class="signal-card ${sevClass}" onclick="openDrawer(${s.id})">
    <div class="card-top"><div class="card-title">${s.title||'Untitled Signal'}</div></div>
    <div class="card-badges">
      ${sevBadge(s.severity)}${sentBadge(s.sentiment)}
      <span class="badge badge-authority" style="cursor:pointer" onclick="event.stopPropagation();openEntityPanel('${authSafe}','authority')" title="View ${authLabel(s.authority)} intelligence">${authLabel(s.authority)}</span>
      ${pp?'<span class="badge badge-preprint">PREPRINT</span>':''}
      ${s.event_type&&s.event_type!=='other'?`<span class="badge badge-type">${s.event_type.replace(/_/g,' ')}</span>`:''}
      ${ing?`<span class="card-ingredient" onclick="event.stopPropagation();filterByIngredient('${ingSafe}')" title="Filter to ${ing}">${ing}</span><button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${ingSafe}','ingredient')" title="Open ${ing} detail">&#9432;</button>`:''}
    </div>
    ${s.summary?`<div class="card-summary">${trunc(s.summary,180)}</div>`:''}
    ${why?`<div class="card-why">${trunc(why,160)}</div>`:''}
    <div class="card-footer">
      <div class="card-meta">${fmt(s.scraped_at)}</div>
      <div class="card-actions">
        <button class="card-action card-action-primary" onclick="event.stopPropagation();openDrawer(${s.id})">View</button>
        <button class="copy-memo-btn" onclick="event.stopPropagation();copySignalMemo(${s.id},this)" title="Copy client memo">&#128203;</button>
        <button class="watchlist-btn${wlActive?' active':''}" onclick="event.stopPropagation();toggleWatchlistItem('${wlId}','${wlTitle}','signal',this)" title="Add to watchlist">&#9733;</button>
      </div>
    </div>
  </div>`;
}

function renderSignals() {
  const data = filteredSignals();
  if (activeClaimCat) console.log('[ClaimFilter] filtered results:', data.length, 'for', activeClaimCat);
  const pages = Math.ceil(data.length/PER_PAGE);
  currentPage = Math.min(currentPage, pages||1);
  const slice = data.slice((currentPage-1)*PER_PAGE, currentPage*PER_PAGE);
  const list = $('#signals-list');
  if (list) {
    if (slice.length) {
      list.innerHTML = slice.map(s=>signalCard(s)).join('');
    } else {
      const emptyTitle = activeClaimCat
        ? `No matching ${activeClaimCat} signals found for current filters`
        : 'No signals match this filter';
      const sug = activeClaimCat ? [] : _suggestSignals(3);
      list.innerHTML = '<div class="empty-filter-state">'
        + '<div class="empty-icon">&#128270;</div>'
        + `<div class="empty-title">${emptyTitle}</div>`
        + '<div class="empty-sub">Try broadening your search&nbsp;&nbsp;<button class="card-action card-action-primary" onclick="clearAllFilters()">Clear all filters</button></div>'
        + (sug.length ? '<div class="empty-sug-label">Closest matches:</div>' + sug.map(s=>signalCard(s)).join('') : '')
        + '</div>';
    }
  }
  const sc = $('#signal-count'); if (sc) sc.textContent = `${data.length} signal${data.length!==1?'s':''}`;
  const pg = $('#pagination'); if (!pg) return; pg.innerHTML='';
  if (pages>1) {
    const mkBtn=(l,p,a)=>{const b=document.createElement('button');b.className='page-btn'+(a?' active':'');b.textContent=l;b.onclick=()=>{currentPage=p;renderSignals();window.scrollTo(0,80)};return b};
    if(currentPage>1) pg.appendChild(mkBtn('‹',currentPage-1,false));
    for(let i=1;i<=pages;i++){if(i===1||i===pages||Math.abs(i-currentPage)<=2)pg.appendChild(mkBtn(i,i,i===currentPage));else if(Math.abs(i-currentPage)===3){const sp=document.createElement('span');sp.textContent='…';sp.style.color='var(--slate-dim)';pg.appendChild(sp);}}
    if(currentPage<pages) pg.appendChild(mkBtn('›',currentPage+1,false));
  }
}
function renderOverviewSignals() {
  const hasFilters = filters.sev.size||filters.auth.size||filters.sent.size||filters.type.size||filters.market.size||filters.ingredient||activeClaimCat||filters.query;
  let feed;
  if (hasFilters) {
    feed = filteredSignals().slice(0,12);
  } else {
    const regs=['tga','fda','artg','adverse_events','tga_consultations','efsa','cochrane'];
    feed=[...SIGNALS.filter(s=>s.severity==='high'),...SIGNALS.filter(s=>s.severity==='medium'&&regs.includes(s.authority))].slice(0,12);
  }
  const el=$('#overview-signals'); if(!el) return;
  if (feed.length) {
    el.innerHTML = feed.map(s=>signalCard(s)).join('');
  } else {
    el.innerHTML = '<div class="empty-filter-state"><div class="empty-icon">&#128270;</div><div class="empty-title">No signals match this filter</div><div class="empty-sub"><button class="card-action card-action-primary" onclick="clearAllFilters()">Clear all filters</button></div></div>';
  }
}

// ─── AI INSIGHTS RENDER ───────────────────────────────────────────────────────
function renderAiInsights() {
  const el=document.getElementById('ai-insights-body'); if(!el) return;
  // Each row: onclick on the whole row + more-specific ci links inside
  // Row fn must be a named function so it works from the onclick attr
  el.innerHTML = `
  <div class="insight-row insight-risk" onclick="filterByAuthorityAndSev('tga','high')" title="Filter to TGA high-severity signals">
    <span class="insight-dot"></span>
    <span class="insight-label">TGA Enforcement:</span>
    <span class="insight-text">Safety alert on <span class="ci" onclick="event.stopPropagation();filterByIngredient('artri ajo')">Artri Ajo King tablets</span> &mdash; herbal supplement scrutiny rising; review similar products in portfolio immediately.</span>
  </div>
  <div class="insight-row insight-enforcement" onclick="filterByIngredient('peptide')" title="Filter to peptide signals">
    <span class="insight-dot"></span>
    <span class="insight-label">Peptide Crackdown:</span>
    <span class="insight-text">TGA formal warning on <span class="ci" onclick="event.stopPropagation();filterByIngredient('bpc-157')">BPC-157</span>, <span class="ci" onclick="event.stopPropagation();filterByIngredient('tb-500')">TB-500</span>, <span class="ci" onclick="event.stopPropagation();filterByIngredient('ghk-cu')">GHK-Cu</span> &mdash; import bans expected within 90 days. Audit peptide exposure now.</span>
  </div>
  <div class="insight-row insight-risk" onclick="gotoSignalsFiltered('type','recall')" title="Filter to recall signals">
    <span class="insight-dot"></span>
    <span class="insight-label">Allergen Risk:</span>
    <span class="insight-text">3 recalls in 60 days for undeclared allergens (FDA) &mdash; category-wide review likely. Audit contract manufacturer allergen controls.</span>
  </div>
  <div class="insight-row insight-neutral" onclick="filterByAuthority('cochrane')" title="Filter to Cochrane signals">
    <span class="insight-dot"></span>
    <span class="insight-label">Claims Pressure:</span>
    <span class="insight-text">6 Cochrane reviews on <span class="ci" onclick="event.stopPropagation();filterByIngredient('melatonin')">melatonin</span>, <span class="ci" onclick="event.stopPropagation();filterByIngredient('probiotic')">probiotics</span>, <span class="ci" onclick="event.stopPropagation();filterByIngredient('omega-3')">omega-3</span> &mdash; &ldquo;inconclusive evidence&rdquo; conclusions can trigger health claim challenges.</span>
  </div>
  <div class="insight-row insight-opportunity" onclick="gotoSignalsFiltered('sent','positive')" title="Filter to positive research signals">
    <span class="insight-dot"></span>
    <span class="insight-label">Opportunity:</span>
    <span class="insight-text">115 new trial registrations + 78 positive signals available for efficacy dossiers and <span class="ci" onclick="event.stopPropagation();gotoSignalsFiltered('sent','positive')">marketing claim substantiation</span>.</span>
  </div>`;
}

// ─── ANALYTICS INSIGHTS ───────────────────────────────────────────────────────
function renderAnalyticsInsights() {
  const aimEl = document.getElementById('aim-bullets');
  if(aimEl) {
    const bullets = [
      {type:'risk', text:'<b>GMP violations</b> dominate at 26% of all citations — baseline compliance risk across every authority in scope', fn:"gotoEvidence('GMP violations','all')"},
      {type:'warn', text:'<b>Labelling & claims</b> remain second-highest — strong commercial and regulatory overlap for all VMS brands', fn:"gotoEvidence('Labelling & claims','all')"},
      {type:'warn', text:'<b>Data Integrity / CSV</b> signals are increasing — regulatory focus shifting toward execution quality, not just product formulation', fn:"gotoEvidence('CSV','all')"},
      {type:'info', text:'<b>Peptide enforcement</b> trajectory mirrors 2019 SARMs crackdown — 90-day window before TGA import restrictions likely', fn:"filterByIngredient('peptide')"},
      {type:'ok',   text:'<b>115 trials + 78 positive signals</b> available for efficacy dossiers and claim substantiation work', fn:"gotoSignalsFiltered('sent','positive')"},
    ];
    aimEl.innerHTML = bullets.map(b=>
      `<div class="aim-bullet" onclick="${b.fn}">
        <span class="aim-dot aim-dot-${b.type}"></span>
        <span class="aim-text">${b.text}</span>
      </div>`
    ).join('');
  }
  const srcEl = document.getElementById('acb-sources');
  if(srcEl) {
    const sources = [
      {name:'PubMed',    desc:'High volume research base — low direct regulatory impact',              tag:'research',    cls:'src-tag-low',  auth:'pubmed'},
      {name:'ClinTrials',desc:'115 active trials — substantiation pipeline for future claims',          tag:'pipeline',    cls:'src-tag-low',  auth:'clinical_trials'},
      {name:'FDA',       desc:'Lower volume, high enforcement relevance — recall and safety alert source',tag:'enforcement',cls:'src-tag-high', auth:'fda'},
      {name:'TGA',       desc:'Moderate volume, increasing activity — Australian regulatory focal point', tag:'enforcement',cls:'src-tag-high', auth:'tga'},
      {name:'Cochrane',  desc:'6 reviews — "inconclusive" conclusions can trigger claims challenges',     tag:'risk',        cls:'src-tag-med',  auth:'cochrane'},
      {name:'EFSA',      desc:'Research-driven signals with indirect EU enforcement impact',              tag:'evidence',    cls:'src-tag-med',  auth:'efsa'},
    ];
    srcEl.innerHTML = sources.map(s=>
      `<div class="src-row" onclick="filterByAuthority('${s.auth}')" title="Filter to ${s.name} signals">
        <div style="flex:1;min-width:0">
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:2px">
            <span class="src-name">${s.name}</span><span class="src-tag ${s.cls}">${s.tag}</span>
          </div>
          <div class="src-desc">${s.desc}</div>
        </div>
      </div>`
    ).join('');
  }
  const rkEl = document.getElementById('acb-risks');
  if(rkEl) {
    const risks = [
      {name:'GMP Violations',      ct:744, trend:'↑', impact:'high', fn:"gotoEvidence('GMP violations','all')"},
      {name:'Labelling & Claims',  ct:462, trend:'↑', impact:'high', fn:"gotoEvidence('Labelling & claims','all')"},
      {name:'Equipment',           ct:251, trend:'→', impact:'med',  fn:"gotoEvidence('Equipment','all')"},
      {name:'Contamination',       ct:210, trend:'→', impact:'high', fn:"gotoEvidence('Contamination','all')"},
      {name:'CSV / Data Integrity',ct:147, trend:'↑', impact:'med',  fn:"gotoEvidence('CSV','all')"},
    ];
    rkEl.innerHTML = risks.map((r,i)=>
      `<div class="rr-row" onclick="${r.fn}">
        <span class="rr-num">${i+1}</span>
        <span class="rr-name">${r.name}</span>
        <span class="rr-trend ${r.trend==='↑'?'rr-trend-up':'rr-trend-stable'}">${r.trend}</span>
        <span class="rr-ct">${r.ct.toLocaleString()}</span>
        <span class="rr-impact rr-impact-${r.impact}">${r.impact==='high'?'HIGH':'MED'}</span>
      </div>`
    ).join('');
  }
}

// ─── VMS WHAT CHANGED ─────────────────────────────────────────────────────────
function renderVmsWhatChanged() {
  const el=document.getElementById('vms-what-changed'); if(!el) return;
  // Use latest signal date as reference so weekly scrape batches fall within the window
  const latestMs=SIGNALS.reduce((m,s)=>Math.max(m,+new Date(s.scraped_at||s.created_at||0)),0);
  const refDate=latestMs||Date.now();
  const d7=refDate-7*864e5;
  const fromStr=new Date(d7).toLocaleDateString('en-AU',{day:'numeric',month:'short'});
  const toStr=new Date(refDate).toLocaleDateString('en-AU',{day:'numeric',month:'short'});
  const inWindow=SIGNALS.filter(s=>+new Date(s.scraped_at||s.created_at||0)>=d7).length;
  console.log('[WhatChanged] window:',fromStr,'→',toStr,'| signals in window:',inWindow);
  const ingTrends=computeIngredientTrends();
  const rising=ingTrends.filter(i=>i.curr>i.prev).slice(0,3);
  const falling=ingTrends.filter(i=>i.curr<i.prev).slice(0,2);
  const newHigh=SIGNALS.filter(s=>s.severity==='high'&&+new Date(s.scraped_at||s.created_at||0)>=d7).slice(0,3);
  const enfs=computeEnforcementTrends();
  const enfUp=enfs.filter(e=>e.change>0).slice(0,3);
  const emptyCol=(title)=>`<div style="padding:4px 0"><div style="font-size:11px;font-weight:600;color:#6B8FAF;margin-bottom:3px">${title}</div><div style="font-size:9px;color:#4a6a87">Baseline checked: ${fromStr} – ${toStr}</div></div>`;
  const actionStyle='font-size:10px;color:rgba(13,148,136,.82);padding-top:5px;margin-top:3px;border-top:1px solid rgba(30,70,110,.4)';
  const actionLbl='<b style="color:rgba(13,148,136,.95)">Action:</b> ';
  const ingCol=(rising.length||falling.length)?[
    ...rising.map(i=>{const pct=i.prev>0?Math.round((i.curr-i.prev)/i.prev*100):null;return`<div class="wc-vms-bullet"><div class="wc-vms-lbl">${i.name}</div><div class="wc-vms-txt wc-vms-rising">${i.curr} signals${pct!==null?' (+'+pct+'%)':''}</div>${i.high>0?`<div class="wc-vms-why">${i.high} high severity</div>`:''}</div>`;}),
    ...falling.map(i=>`<div class="wc-vms-bullet"><div class="wc-vms-lbl">${i.name}</div><div class="wc-vms-txt wc-vms-falling">${i.curr} signals (↓ from ${i.prev})</div></div>`)
  ].join(''):emptyCol('No major changes detected');
  const highCol=newHigh.length?newHigh.map(s=>`<div class="wc-vms-bullet"><div class="wc-vms-lbl" style="cursor:pointer;color:rgba(190,75,75,.65)" onclick="openDrawer(${s.id})">${trunc(s.title||'',52)}</div><div class="wc-vms-txt">${authLabel(s.authority)} · ${fmt(s.scraped_at)}</div></div>`).join(''):emptyCol('No new high-severity signals');
  const enfCol=enfUp.length===1
    ?`<div class="wc-vms-bullet"><div class="wc-vms-lbl">${enfUp[0].label} activity increasing</div><div class="wc-vms-txt wc-vms-rising">+${enfUp[0].change} vs prior period</div>${enfUp[0].high>0?`<div class="wc-vms-why">${enfUp[0].high} high severity</div>`:''}<div style="${actionStyle}">${actionLbl}Review current listings and claim exposure</div></div>`
    :enfUp.length?enfUp.map(e=>`<div class="wc-vms-bullet"><div class="wc-vms-lbl">${e.label}</div><div class="wc-vms-txt wc-vms-rising">+${e.change} vs prior week</div>${e.high>0?`<div class="wc-vms-why">${e.high} high severity</div>`:''}</div>`).join('')
    :emptyCol('No significant enforcement changes');
  const panelPad=(rising.length||falling.length||newHigh.length||enfUp.length)?'11px 14px':'8px 14px';
  el.innerHTML=`<div class="wc-vms-panel" style="padding:${panelPad}"><div class="wc-vms-hd">What Changed This Period</div><div class="wc-vms-cols"><div><div class="wc-vms-col-hd">Ingredient Activity</div>${ingCol}</div><div><div class="wc-vms-col-hd">New High-Severity</div>${highCol}</div><div><div class="wc-vms-col-hd">Enforcement Activity</div>${enfCol}</div></div></div>`;
}

// === VMS ENTITIES + EVIDENCE RENDER ===

function renderEntities() {
  const q=($('#entity-search')||{value:''}).value.toLowerCase();
  const map={};
  SIGNALS.forEach(s=>{
    let ing=(s.ingredient_name||'').trim().toLowerCase();
    if(!ing||ing==='unknown') ing = _extractIngFromTitle(s.title) || '';
    if(!ing) return;
    if(!map[ing])map[ing]={name:ing,total:0,high:0,neg:0,pos:0};
    map[ing].total++;
    if(s.severity==='high')map[ing].high++;
    if(s.sentiment==='negative')map[ing].neg++;
    if(s.sentiment==='positive')map[ing].pos++;
  });
  let items=Object.values(map).sort((a,b)=>b.total-a.total);
  if(q) items=items.filter(i=>i.name.includes(q));
  const maxT=items[0]?.total||1;
  const grid=$('#entity-grid'); if(!grid) return;
  grid.innerHTML=items.length?items.map(e=>{
    const nm=e.name.replace(/'/g,"\\'");
    return `<div class="entity-card" onclick="showEntitySignals('${nm}')">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:4px;margin-bottom:3px">
        <div class="entity-name" style="margin-bottom:0">${e.name}</div>
        <button class="ing-ep-btn" onclick="event.stopPropagation();openEntityPanel('${nm}','ingredient')" title="Open ${e.name} detail" style="font-size:13px;margin-top:1px">&#9432;</button>
      </div>
      <div class="entity-stats"><span class="entity-stat">${e.total} signals</span>${e.high?`<span class="entity-stat text-red">${e.high} high</span>`:''}${e.pos?`<span class="entity-stat text-green">${e.pos} pos</span>`:''}${e.neg?`<span class="entity-stat text-red">${e.neg} neg</span>`:''}</div>
      <div class="entity-bar"><div class="entity-bar-fill" style="width:${Math.round(e.total/maxT*100)}%"></div></div>
    </div>`;
  }).join(''):'<div class="empty"><div class="empty-icon">&#9762;</div><div class="empty-text">No entities found</div></div>';
  const ec=$('#entity-count'); if(ec) ec.textContent=`${items.length} ingredient${items.length!==1?'s':''}`;
}
function showEntitySignals(name) {
  const el = document.getElementById('entity-signals'); if(!el) return;
  // Use strict entity matching to avoid "vitamin d" matching "vitamin b"
  const matched = SIGNALS.filter(s => _entityIngMatch(s, name))
    .sort((a,b) => new Date(b.scraped_at)-new Date(a.scraped_at));
  // Group by authority
  const byAuth = {};
  matched.forEach(s => { const k=authLabel(s.authority); if(!byAuth[k]) byAuth[k]=[]; byAuth[k].push(s); });
  const disp = name.charAt(0).toUpperCase() + name.slice(1);
  el.innerHTML = `<div class="entity-sig-header">`
    + `<span class="breadcrumb">Entities &rsaquo; <b>${disp}</b> <span class="entity-sig-count">(${matched.length} signal${matched.length!==1?'s':''})</span></span>`
    + `<button class="card-action" onclick="clearEntitySignals()">&#8592; Back to all</button>`
    + `</div>`
    + (matched.length
      ? Object.entries(byAuth).map(([src,sigs]) =>
          `<details class="entity-src-group" open>`
          + `<summary class="entity-src-header">${src} <span class="badge badge-authority">${sigs.length}</span></summary>`
          + `<div class="entity-src-body">${sigs.map(s=>signalCard(s)).join('')}</div>`
          + `</details>`
        ).join('')
      : '<div class="empty"><div class="empty-icon">&#128270;</div><div class="empty-text">No signals found for &ldquo;'+disp+'&rdquo;</div></div>'
    );
  el.style.display='block';
  el.scrollIntoView({behavior:'smooth',block:'start'});
}
function clearEntitySignals() {
  const el=document.getElementById('entity-signals');
  if(el){el.style.display='none';el.innerHTML='';}
}

// ─── EVIDENCE ─────────────────────────────────────────────────────────────────
function filteredCitations() {
  return unifiedFilteredCitations({
    noiseFilter: false,
    auth: evFilters.auth, sev: evFilters.sev,
    query: evFilters.query, queryFields: ['summary', 'category'],
    sortCol: evSort.col, sortDir: evSort.dir,
  });
}
function renderEvidence() {
  const data=filteredCitations(),pages=Math.ceil(data.length/EV_PER_PAGE);
  evPage=Math.min(evPage,pages||1);
  const slice=data.slice((evPage-1)*EV_PER_PAGE,evPage*EV_PER_PAGE);
  const tb=$('#ev-tbody'); if(!tb) return;
  tb.innerHTML=slice.map(c=>`<tr><td><span class="badge badge-authority">${c.authority||'—'}</span></td><td>${trunc(c.category||'—',40)}</td><td style="color:var(--slate-dim);font-size:11px">${c.source_type||'—'}</td><td>${sevBadge(c.severity||'low')}</td><td style="white-space:nowrap;font-size:11px">${c.date?c.date.slice(0,10):'—'}</td><td style="max-width:320px;font-size:11px;color:var(--slate)">${trunc(c.summary||'—',120)}</td><td>${c.url?`<a class="ev-link" href="${c.url}" target="_blank">&#8599;</a>`:'—'}</td></tr>`).join('');
  const evc=$('#ev-count'); if(evc) evc.textContent=`${data.length} citations`;
  const pg=$('#ev-pagination'); if(!pg) return; pg.innerHTML='';
  if(pages>1){const mkB=(l,p,a)=>{const b=document.createElement('button');b.className='page-btn'+(a?' active':'');b.textContent=l;b.onclick=()=>{evPage=p;renderEvidence()};return b};
    if(evPage>1)pg.appendChild(mkB('‹',evPage-1,false));
    const s2=Math.max(1,evPage-2),e2=Math.min(pages,evPage+2);
    if(s2>1){pg.appendChild(mkB(1,1,false));if(s2>2){const x=document.createElement('span');x.textContent='…';x.style.color='var(--slate-dim)';pg.appendChild(x);}}
    for(let i=s2;i<=e2;i++)pg.appendChild(mkB(i,i,i===evPage));
    if(e2<pages){if(e2<pages-1){const x=document.createElement('span');x.textContent='…';x.style.color='var(--slate-dim)';pg.appendChild(x);}pg.appendChild(mkB(pages,pages,false));}
    if(evPage<pages)pg.appendChild(mkB('›',evPage+1,false));}
}


// === VMS ORCHESTRATION ===

// ─── VMS KPI CARD SYNC ────────────────────────────────────────────────────────
function syncVMSKpiCards() {
  const high=SIGNALS.filter(s=>(s.severity||'').toLowerCase()==='high').length;
  const recalls=SIGNALS.filter(s=>(s.event_type||'').toLowerCase()==='recall').length;
  const clinical=SIGNALS.filter(s=>s.authority==='clinical_trials').length;
  const positive=SIGNALS.filter(s=>(s.sentiment||'').toLowerCase()==='positive').length;
  const _setV=(id,val)=>{const e=document.getElementById(id);if(e){const v=e.querySelector('.kpi-val');if(v)v.textContent=Number.isInteger(val)&&val>=1000?val.toLocaleString():val;}};
  _setV('kpi-high-sev',high);
  _setV('kpi-recalls',recalls);
  _setV('kpi-total',SIGNALS.length);
  _setV('kpi-clinical',clinical);
  _setV('kpi-positive',positive);
  _setV('kpi-evidence',CITATIONS.length);
  // Update AI panel sub-line
  const subLine=document.querySelector('#page-overview .ai-sub');
  if(subLine){
    const auths=new Set(SIGNALS.map(s=>s.authority)).size;
    const lastUpd=SIGNALEX_META.lastUpdated
      ?new Date(SIGNALEX_META.lastUpdated).toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'})
      :'—';
    subLine.textContent=`${auths} sources · ${SIGNALS.length} signals · updated ${lastUpd} — click any insight to filter`;
  }
}

function syncVMSSidebarCounts() {
  // Authority counts from SIGNALS
  document.querySelectorAll('#auth-filters [data-filter="auth"]').forEach(el=>{
    const v=el.dataset.val,ct=el.querySelector('.filter-count');
    if(!ct) return;
    ct.textContent=v==='all'?SIGNALS.length:SIGNALS.filter(s=>s.authority===v).length;
  });
  // Severity counts
  document.querySelectorAll('#sev-filters [data-filter="sev"]').forEach(el=>{
    const v=el.dataset.val,ct=el.querySelector('.filter-count');
    if(!ct||v==='all') return;
    ct.textContent=SIGNALS.filter(s=>(s.severity||'').toLowerCase()===v).length;
  });
  // Sentiment counts
  document.querySelectorAll('#sent-filters [data-filter="sent"]').forEach(el=>{
    const v=el.dataset.val,ct=el.querySelector('.filter-count');
    if(!ct||v==='all') return;
    ct.textContent=SIGNALS.filter(s=>(s.sentiment||'').toLowerCase()===v).length;
  });
  // Event type counts — 'other' catches all unlisted types
  document.querySelectorAll('#type-filters [data-filter="type"]').forEach(el=>{
    const v=el.dataset.val,ct=el.querySelector('.filter-count');
    if(!ct||v==='all') return;
    const _known=['recall','safety_alert','warning','new_listing'];
    ct.textContent=v==='other'
      ?SIGNALS.filter(s=>!_known.includes((s.event_type||'').toLowerCase())).length
      :SIGNALS.filter(s=>(s.event_type||'').toLowerCase()===v).length;
  });
}

function _commit() {
  currentPage = 1;
  _syncSidebarStates();
  _updateChips();
  _syncKpiCards();
  renderSignals();
  renderOverviewSignals();
  renderTrendPanels();
}